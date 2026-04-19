"""
NBA Analytics Dashboard — Streamlit app.

Phase 1: Team season stats (W/L record, per-game averages, points trend chart)
Phase 2: Live scoreboard (stubbed, requires live pipeline completion)

Run:
  cd dashboard
  streamlit run app.py
"""

import os

import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
SF_ACCOUNT   = "sfedu02-unb02139"
SF_USER      = "MINNOW"
SF_DATABASE  = "MINNOW_DB"
SF_SCHEMA    = "PUBLIC"
SF_WAREHOUSE = "MINNOW_WH"
DEFAULT_PEM  = os.path.join(os.path.dirname(__file__), "../rsa_key.p8")


# ──────────────────────────────────────────────
# CONNECTION
# ──────────────────────────────────────────────

def _load_private_key(key_path: str) -> bytes:
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@st.cache_resource
def get_connection(pem_path: str):
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        private_key=_load_private_key(pem_path),
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
    )


def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    conn = get_connection(st.session_state.pem_path)
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        return pd.DataFrame.from_records(
            cur.fetchall(), columns=[d[0].lower() for d in cur.description]
        )


# ──────────────────────────────────────────────
# DATA LOADERS
# ──────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_teams() -> pd.DataFrame:
    return run_query("SELECT team_id, name, abbreviation FROM dim_team ORDER BY name")


@st.cache_data(ttl=3600)
def load_seasons() -> list[str]:
    df = run_query(
        "SELECT DISTINCT season FROM dim_date "
        "WHERE season IS NOT NULL AND season_type = 'Regular Season' "
        "ORDER BY season DESC"
    )
    return df["season"].tolist()


@st.cache_data(ttl=600)
def load_record(team_id: int, season: str) -> dict:
    df = run_query("""
        SELECT
            SUM(CASE
                WHEN fg.home_team_id = %(team_id)s AND fg.home_score > fg.away_score THEN 1
                WHEN fg.away_team_id = %(team_id)s AND fg.away_score > fg.home_score THEN 1
                ELSE 0
            END) AS wins,
            COUNT(*) AS games,
            AVG(CASE WHEN fg.home_team_id = %(team_id)s THEN fg.home_score
                     ELSE fg.away_score END) AS avg_pts_for,
            AVG(CASE WHEN fg.home_team_id = %(team_id)s THEN fg.away_score
                     ELSE fg.home_score END) AS avg_pts_against
        FROM fact_game fg
        JOIN dim_date dd ON fg.date = dd.date
        WHERE (fg.home_team_id = %(team_id)s OR fg.away_team_id = %(team_id)s)
          AND dd.season = %(season)s
          AND dd.season_type = 'Regular Season'
    """, {"team_id": team_id, "season": season})
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=600)
def load_roster_stats(team_id: int, season: str) -> pd.DataFrame:
    return run_query("""
        SELECT
            p.name,
            p.position,
            COUNT(fgps.game_id)  AS games,
            AVG(fgps.pts)        AS ppg,
            AVG(fgps.reb)        AS rpg,
            AVG(fgps.ast)        AS apg,
            AVG(fgps.stl)        AS spg,
            AVG(fgps.blk)        AS bpg,
            AVG(fgps.fg_pct)     AS fg_pct,
            AVG(fgps.fg3_pct)    AS fg3_pct,
            AVG(fgps.ft_pct)     AS ft_pct
        FROM fact_game_player_stats fgps
        JOIN (
            SELECT player_id, MAX(name) AS name, MAX(position) AS position
            FROM dim_player
            GROUP BY player_id
        ) p ON fgps.player_id = p.player_id
        JOIN fact_game fg   ON fgps.game_id  = fg.game_id
        JOIN dim_date dd    ON fg.date        = dd.date
        WHERE fgps.team_id        = %(team_id)s
          AND dd.season           = %(season)s
          AND dd.season_type      = 'Regular Season'
        GROUP BY fgps.player_id, p.name, p.position
        HAVING COUNT(fgps.game_id) >= 5
        ORDER BY ppg DESC
    """, {"team_id": team_id, "season": season})


@st.cache_data(ttl=600)
def load_game_stats(team_id: int, season: str) -> pd.DataFrame:
    return run_query("""
        SELECT
            fg.date,
            fgts.pts, fgts.fgm, fgts.fga, fgts.fg_pct,
            fgts.fg3m, fgts.fg3a, fgts.fg3_pct,
            fgts.ftm, fgts.fta, fgts.ft_pct,
            fgts.oreb, fgts.dreb, fgts.reb,
            fgts.ast, fgts.stl, fgts.blk, fgts.tov, fgts.pf
        FROM fact_game_team_stats fgts
        JOIN fact_game fg ON fgts.game_id = fg.game_id
        JOIN dim_date dd ON fg.date = dd.date
        WHERE fgts.team_id = %(team_id)s
          AND dd.season = %(season)s
          AND dd.season_type = 'Regular Season'
        ORDER BY fg.date
    """, {"team_id": team_id, "season": season})


# ──────────────────────────────────────────────
# UI COMPONENTS
# ──────────────────────────────────────────────

def show_summary(record: dict, avg_fg_pct: float):
    wins   = int(record.get("wins") or 0)
    games  = int(record.get("games") or 0)
    losses = games - wins
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Record", f"{wins}–{losses}")
    col2.metric("Avg Pts For",     f"{record.get('avg_pts_for') or 0:.1f}")
    col3.metric("Avg Pts Against", f"{record.get('avg_pts_against') or 0:.1f}")
    col4.metric("Avg FG%",         f"{avg_fg_pct * 100:.1f}%")


def show_points_chart(df: pd.DataFrame, team_name: str):
    if df.empty:
        st.info("No game data available for this selection.")
        return
    fig = px.line(
        df, x="date", y="pts",
        title=f"{team_name} — Points Per Game",
        labels={"date": "Date", "pts": "Points"},
        markers=True,
    )
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, width='stretch')


def show_stats_table(df: pd.DataFrame):
    if df.empty:
        return
    avg = df.drop(columns=["date"]).mean().to_frame(name="Per Game Avg").T
    avg["fg_pct"]  = avg["fg_pct"].map("{:.1%}".format)
    avg["fg3_pct"] = avg["fg3_pct"].map("{:.1%}".format)
    avg["ft_pct"]  = avg["ft_pct"].map("{:.1%}".format)
    for col in ["pts", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
                "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf"]:
        avg[col] = avg[col].map("{:.1f}".format)
    st.dataframe(avg, width='stretch')


def show_roster_table(df: pd.DataFrame):
    if df.empty:
        st.info("No player data available for this selection.")
        return
    display = df.copy()
    display.columns = ["Player", "Pos", "GP", "PPG", "RPG", "APG",
                       "SPG", "BPG", "FG%", "3P%", "FT%"]
    for col in ["PPG", "RPG", "APG", "SPG", "BPG"]:
        display[col] = display[col].map("{:.1f}".format)
    for col in ["FG%", "3P%", "FT%"]:
        display[col] = display[col].map(
            lambda v: f"{v * 100:.1f}%" if pd.notna(v) else "—"
        )
    st.dataframe(display, width='stretch', hide_index=True)


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    st.set_page_config(page_title="NBA Analytics", page_icon="🏀", layout="wide")
    st.title("NBA Analytics Dashboard")

    # PEM key path — allow override via env var or session state
    if "pem_path" not in st.session_state:
        st.session_state.pem_path = os.environ.get("NBA_PEM_KEY", DEFAULT_PEM)

    # ── Sidebar ──
    with st.sidebar:
        st.header("Filters")

        pem_input = st.text_input("PEM key path", value=st.session_state.pem_path)
        if pem_input != st.session_state.pem_path:
            st.session_state.pem_path = pem_input
            st.cache_resource.clear()

        try:
            teams   = load_teams()
            seasons = load_seasons()
        except Exception as e:
            st.error(f"Could not connect to Snowflake: {e}")
            st.stop()

        team_names = teams["name"].tolist()
        selected_team_name = st.selectbox("Team", team_names)
        selected_season    = st.selectbox("Season", seasons)

        team_row = teams[teams["name"] == selected_team_name].iloc[0]
        team_id  = int(team_row["team_id"])

    # ── Team Season Stats ──
    st.subheader(f"{selected_team_name} · {selected_season}")

    record    = load_record(team_id, selected_season)
    game_stats = load_game_stats(team_id, selected_season)

    avg_fg_pct = game_stats["fg_pct"].mean() if not game_stats.empty else 0.0
    show_summary(record, avg_fg_pct)

    st.divider()
    show_points_chart(game_stats, selected_team_name)

    st.subheader("Season Averages")
    show_stats_table(game_stats)

    st.subheader("Roster Stats")
    roster = load_roster_stats(team_id, selected_season)
    show_roster_table(roster)

    # ── Phase 2 stub ──
    st.divider()
    with st.expander("Live Scoreboard (coming soon)", expanded=False):
        st.info(
            "Live game data will appear here once the Kafka → Spark streaming "
            "pipeline is connected and LIVE_DATA is being populated."
        )


if __name__ == "__main__":
    main()
