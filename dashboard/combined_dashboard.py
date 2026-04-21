"""
NBA Analytics Dashboard — Combined Live + Historical
=====================================================
Combines the live scoreboard (live_dashboard.py) with the historical
team/player analytics (app.py) into a single unified dashboard.
 
Flow:
  1. Shows only LIVE (in-progress) games in a scoreboard header
  2. A dropdown lets the user pick one live game
  3. Once selected, reveals:
       - Live score card + score progression chart
       - Historical matchup data for the two teams
       - Team season stats & roster breakdowns
 
Run:
  streamlit run combined_dashboard.py
 
Env / secrets:
  NBA_PEM_KEY  — path to rsa_key.p8 used for the MINNOW_DB connection
                 (historical data).  Defaults to ../rsa_key.p8 relative
                 to this file.
  The DOLPHIN_DB connection credentials are hard-coded below (matching
  the original live_dashboard.py); update as needed.
"""
 
import os
import time
from datetime import datetime
 
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
 
# ──────────────────────────────────────────────
# PAGE CONFIG  (must be first Streamlit call)
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="NBA Dashboard",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="collapsed",
)
 
# ──────────────────────────────────────────────
# GLOBAL CSS
# ──────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;800&family=Barlow:wght@400;500&display=swap');
 
    html, body, [class*="css"] {
        font-family: 'Barlow', sans-serif;
        background-color: #0d0d0f;
        color: #f0f0f0;
    }
    .main { background-color: #0d0d0f; }
    h1, h2, h3 {
        font-family: 'Barlow Condensed', sans-serif;
        font-weight: 800;
        letter-spacing: 1px;
        text-transform: uppercase;
    }
 
    /* ── Live score cards ── */
    .game-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #2a2a4a;
        border-radius: 12px;
        padding: 18px 22px;
        margin-bottom: 12px;
        position: relative;
        overflow: hidden;
        cursor: pointer;
    }
    .game-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        background: linear-gradient(90deg, #00c853, #69f0ae);
    }
    .game-card.selected::before {
        background: linear-gradient(90deg, #e8500a, #f5a623);
    }
    .team-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin: 5px 0;
    }
    .team-name {
        font-family: 'Barlow Condensed', sans-serif;
        font-size: 1.8rem;
        font-weight: 800;
        letter-spacing: 2px;
        color: #ffffff;
    }
    .team-score {
        font-family: 'Barlow Condensed', sans-serif;
        font-size: 2.3rem;
        font-weight: 800;
        color: #f5a623;
    }
    .team-score.winning { color: #69f0ae; }
    .game-meta {
        font-size: 0.78rem;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 8px;
        border-top: 1px solid #2a2a4a;
        padding-top: 6px;
    }
    .live-badge {
        display: inline-block;
        background: #00c853;
        color: #000;
        font-size: 0.65rem;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 20px;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-right: 8px;
        animation: pulse 1.5s infinite;
    }
    @keyframes pulse {
        0%   { opacity: 1; }
        50%  { opacity: 0.5; }
        100% { opacity: 1; }
    }
 
    /* ── Section headings ── */
    .section-title {
        font-family: 'Barlow Condensed', sans-serif;
        font-size: 1.5rem;
        font-weight: 800;
        letter-spacing: 2px;
        text-transform: uppercase;
        color: #f5a623;
        border-bottom: 1px solid #2a2a4a;
        padding-bottom: 6px;
        margin-bottom: 14px;
    }
 
    /* ── Header bar ── */
    .header-bar {
        display: flex;
        justify-content: space-between;
        align-items: flex-end;
        margin-bottom: 24px;
        border-bottom: 2px solid #2a2a4a;
        padding-bottom: 14px;
    }
    .header-title {
        font-family: 'Barlow Condensed', sans-serif;
        font-size: 2.6rem;
        font-weight: 800;
        letter-spacing: 3px;
        text-transform: uppercase;
        color: #ffffff;
    }
    .header-sub {
        font-size: 0.78rem;
        color: #666;
        letter-spacing: 1px;
    }
 
    section[data-testid="stSidebar"] { background-color: #111122; }
    .stDataFrame { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)
 
 
# ──────────────────────────────────────────────
# PRIVATE KEY HELPER
# ──────────────────────────────────────────────
def _load_private_key_der(key_path: str, password=None) -> bytes:
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=password.encode() if password else None,
            backend=default_backend(),
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
 
 
# ──────────────────────────────────────────────
# SNOWFLAKE CONNECTIONS
# ──────────────────────────────────────────────
import snowflake.connector  # noqa: E402 — import after page config
 
DEFAULT_PEM = os.path.join(os.path.dirname(__file__), "../rsa_key.p8")
 
# ── Historical DB (MINNOW) ──
@st.cache_resource
def get_hist_conn(pem_path: str):
    return snowflake.connector.connect(
        account="sfedu02-unb02139",
        user="MINNOW",
        private_key=_load_private_key_der(pem_path),
        database="MINNOW_DB",
        schema="PUBLIC",
        warehouse="MINNOW_WH",
    )
 
 
def run_hist(sql: str, params=None) -> pd.DataFrame:
    conn = get_hist_conn(st.session_state.pem_path)
    with conn.cursor() as cur:
        cur.execute(sql, params or {})
        return pd.DataFrame.from_records(
            cur.fetchall(), columns=[d[0].lower() for d in cur.description]
        )
 
 
# ── Live DB (DOLPHIN) ──
@st.cache_resource
def get_live_conn(key="dolphin"):
    # Update key_path / password as needed
    key_path = os.environ.get(
        "NBA_LIVE_PEM",
        "/home/compute/fbonetta-misteli/.ssh/rsa_key.p8",
    )
    password = os.environ.get("NBA_LIVE_PEM_PASS", "JackAbah25")
    return snowflake.connector.connect(
        user="DOLPHIN",
        account="sfedu02-unb02139",
        database="DOLPHIN_DB",
        schema="PUBLIC",
        warehouse="DOLPHIN_WH",
        private_key=_load_private_key_der(key_path, password),
    )
 
 
def run_live(sql: str) -> pd.DataFrame:
    conn = get_live_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0].lower() for d in cur.description]
    cur.close()
    return pd.DataFrame(rows, columns=cols)
 
 
# ──────────────────────────────────────────────
# LIVE DATA LOADERS
# ──────────────────────────────────────────────
def fetch_live_games() -> pd.DataFrame:
    df = run_live("""
        SELECT game_id, h_team, a_team,
               h_points, a_points,
               quarter, clock, game_status, processed_time
        FROM LIVE_DATA
        QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY processed_time DESC) = 1
    """)
    if df.empty:
        return df
    mask = df["game_status"].str.strip() == "3"
    return df[mask].reset_index(drop=True)
 
 
def fetch_score_history(game_id: str) -> pd.DataFrame:
    return run_live(f"""
        SELECT processed_time, h_points, a_points, h_team, a_team, quarter, clock
        FROM LIVE_DATA
        WHERE game_id = '{game_id}'
        ORDER BY processed_time ASC
    """)
 
 
# ──────────────────────────────────────────────
# HISTORICAL DATA LOADERS
# ──────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_teams() -> pd.DataFrame:
    return run_hist("SELECT team_id, name, abbreviation FROM dim_team ORDER BY name")
 
 
@st.cache_data(ttl=3600)
def load_seasons() -> list[str]:
    df = run_hist(
        "SELECT DISTINCT season FROM dim_date "
        "WHERE season IS NOT NULL AND season_type = 'Regular Season' "
        "ORDER BY season DESC"
    )
    return df["season"].tolist()
 
 
@st.cache_data(ttl=600)
def load_record(team_id: int, season: str) -> dict:
    df = run_hist("""
        SELECT
            SUM(CASE
                WHEN fg.home_team_id = %(team_id)s AND fg.home_score > fg.away_score THEN 1
                WHEN fg.away_team_id = %(team_id)s AND fg.away_score > fg.home_score THEN 1
                ELSE 0 END) AS wins,
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
def load_game_stats(team_id: int, season: str) -> pd.DataFrame:
    return run_hist("""
        SELECT
            fg.date,
            fgts.pts, fgts.fgm, fgts.fga, fgts.fg_pct,
            fgts.fg3m, fgts.fg3a, fgts.fg3_pct,
            fgts.ftm, fgts.fta, fgts.ft_pct,
            fgts.oreb, fgts.dreb, fgts.reb,
            fgts.ast, fgts.stl, fgts.blk, fgts.tov, fgts.pf
        FROM fact_game_team_stats fgts
        JOIN fact_game fg ON fgts.game_id = fg.game_id
        JOIN dim_date  dd ON fg.date = dd.date
        WHERE fgts.team_id = %(team_id)s
          AND dd.season = %(season)s
          AND dd.season_type = 'Regular Season'
        ORDER BY fg.date
    """, {"team_id": team_id, "season": season})
 
 
@st.cache_data(ttl=600)
def load_roster_stats(team_id: int, season: str) -> pd.DataFrame:
    return run_hist("""
        SELECT
            p.name, p.position,
            COUNT(fgps.game_id) AS games,
            AVG(fgps.pts)       AS ppg,
            AVG(fgps.reb)       AS rpg,
            AVG(fgps.ast)       AS apg,
            AVG(fgps.stl)       AS spg,
            AVG(fgps.blk)       AS bpg,
            AVG(fgps.fg_pct)    AS fg_pct,
            AVG(fgps.fg3_pct)   AS fg3_pct,
            AVG(fgps.ft_pct)    AS ft_pct
        FROM fact_game_player_stats fgps
        JOIN (
            SELECT player_id, MAX(name) AS name, MAX(position) AS position
            FROM dim_player GROUP BY player_id
        ) p ON fgps.player_id = p.player_id
        JOIN fact_game fg ON fgps.game_id = fg.game_id
        JOIN dim_date  dd ON fg.date = dd.date
        WHERE fgps.team_id       = %(team_id)s
          AND dd.season          = %(season)s
          AND dd.season_type     = 'Regular Season'
        GROUP BY fgps.player_id, p.name, p.position
        HAVING COUNT(fgps.game_id) >= 5
        ORDER BY ppg DESC
    """, {"team_id": team_id, "season": season})
 
 
@st.cache_data(ttl=600)
def load_head_to_head(team_a_id: int, team_b_id: int, season: str) -> pd.DataFrame:
    """Recent games between two specific teams in a given season."""
    return run_hist("""
        SELECT
            fg.date,
            ht.abbreviation AS home_team,
            at.abbreviation AS away_team,
            fg.home_score,
            fg.away_score
        FROM fact_game fg
        JOIN dim_date  dd ON fg.date = dd.date
        JOIN dim_team  ht ON fg.home_team_id = ht.team_id
        JOIN dim_team  at ON fg.away_team_id = at.team_id
        WHERE dd.season = %(season)s
          AND dd.season_type = 'Regular Season'
          AND (
              (fg.home_team_id = %(a)s AND fg.away_team_id = %(b)s)
           OR (fg.home_team_id = %(b)s AND fg.away_team_id = %(a)s)
          )
        ORDER BY fg.date DESC
        LIMIT 10
    """, {"a": team_a_id, "b": team_b_id, "season": season})
 
 
# ──────────────────────────────────────────────
# UI HELPERS
# ──────────────────────────────────────────────
def render_live_card(game: pd.Series, selected: bool = False) -> None:
    card_class = "game-card selected" if selected else "game-card"
    h_pts = int(game.get("h_points", 0) or 0)
    a_pts = int(game.get("a_points", 0) or 0)
    h_cls = "winning" if h_pts > a_pts else ""
    a_cls = "winning" if a_pts > h_pts else ""
    qtr   = f"Q{game.get('quarter', '—')}" if game.get("quarter") else ""
    clk   = game.get("clock", "") or ""
    st.markdown(f"""
    <div class="{card_class}">
        <div class="team-row">
            <span class="team-name">{game.get('h_team', '—')}</span>
            <span class="team-score {h_cls}">{h_pts}</span>
        </div>
        <div class="team-row">
            <span class="team-name">{game.get('a_team', '—')}</span>
            <span class="team-score {a_cls}">{a_pts}</span>
        </div>
        <div class="game-meta">
            <span class="live-badge">● Live</span>
            {qtr}{"  ·  " + clk if clk else ""}
        </div>
    </div>
    """, unsafe_allow_html=True)
 
 
def render_score_progression(history: pd.DataFrame, h_team: str, a_team: str) -> None:
    if history.empty:
        st.info("No score history available yet for this game.")
        return

    history = history.copy()
    history["processed_time"] = pd.to_datetime(history["processed_time"])
    history = history.sort_values("processed_time").reset_index(drop=True)

    # Find where quarter changes for vertical markers
    quarter_changes = history[history["quarter"] != history["quarter"].shift()]

    fig = go.Figure()

    # Home team line
    fig.add_trace(go.Scatter(
        x=history["processed_time"],
        y=history["h_points"],
        name=h_team,
        line=dict(color="#f5a623", width=3),
        mode="lines+markers",
        marker=dict(size=6),
        hovertemplate=f"<b>{h_team}</b>: %{{y}} pts<br>%{{x|%I:%M %p}}<extra></extra>",
    ))

    # Away team line
    fig.add_trace(go.Scatter(
        x=history["processed_time"],
        y=history["a_points"],
        name=a_team,
        line=dict(color="#69f0ae", width=3),
        mode="lines+markers",
        marker=dict(size=6),
        hovertemplate=f"<b>{a_team}</b>: %{{y}} pts<br>%{{x|%I:%M %p}}<extra></extra>",
    ))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(22,22,46,0.6)",
        font=dict(family="Barlow Condensed", color="#cccccc", size=15),
        margin=dict(l=0, r=0, t=20, b=0),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=18, family="Barlow Condensed"),
        ),
        xaxis=dict(
            gridcolor="#2a2a4a",
            tickformat="%I:%M %p",
            title=dict(text="Time", font=dict(size=16)),
            tickfont=dict(size=14),
        ),
        yaxis=dict(
            gridcolor="#2a2a4a",
            title=dict(text="Points", font=dict(size=16)),
            tickfont=dict(size=14),
        ),
    )

    st.plotly_chart(fig, use_container_width=True)
 
 
def render_hist_summary(record: dict, avg_fg_pct: float, team_name: str) -> None:
    wins   = int(record.get("wins") or 0)
    games  = int(record.get("games") or 0)
    losses = games - wins
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Record",           f"{wins}–{losses}")
    c2.metric("Avg Pts For",      f"{record.get('avg_pts_for') or 0:.1f}")
    c3.metric("Avg Pts Against",  f"{record.get('avg_pts_against') or 0:.1f}")
    c4.metric("Avg FG%",          f"{avg_fg_pct * 100:.1f}%")
 
 
def render_pts_chart(df: pd.DataFrame, team_name: str) -> None:
    if df.empty:
        st.info("No game data available.")
        return
    fig = px.line(
        df, x="date", y="pts",
        title=f"{team_name} — Points Per Game (Season)",
        labels={"date": "Date", "pts": "Points"},
        markers=True,
        color_discrete_sequence=["#f5a623"],
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(22,22,46,0.6)",
        font=dict(family="Barlow Condensed", color="#aaa"),
        margin=dict(l=0, r=0, t=40, b=0),
        hovermode="x unified",
        xaxis=dict(gridcolor="#2a2a4a"),
        yaxis=dict(gridcolor="#2a2a4a"),
    )
    st.plotly_chart(fig, use_container_width=True)
 
 
def render_season_avgs(df: pd.DataFrame) -> None:
    if df.empty:
        return
    avg = df.drop(columns=["date"]).mean().to_frame(name="Per Game Avg").T
    for col in ["fg_pct", "fg3_pct", "ft_pct"]:
        avg[col] = avg[col].map("{:.1%}".format)
    for col in ["pts","fgm","fga","fg3m","fg3a","ftm","fta",
                "oreb","dreb","reb","ast","stl","blk","tov","pf"]:
        avg[col] = avg[col].map("{:.1f}".format)
    st.dataframe(avg, use_container_width=True)
 
 
def render_roster(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No player data for this selection.")
        return
    display = df.copy()
    display.columns = ["Player","Pos","GP","PPG","RPG","APG","SPG","BPG","FG%","3P%","FT%"]
    for col in ["PPG","RPG","APG","SPG","BPG"]:
        display[col] = display[col].map("{:.1f}".format)
    for col in ["FG%","3P%","FT%"]:
        display[col] = display[col].map(
            lambda v: f"{v*100:.1f}%" if pd.notna(v) else "—"
        )
    st.dataframe(display, use_container_width=True, hide_index=True)
 
 
def render_head_to_head(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No head-to-head data found for this season.")
        return
    display = df.rename(columns={
        "date": "Date", "home_team": "Home", "away_team": "Away",
        "home_score": "Home Pts", "away_score": "Away Pts",
    })
    st.dataframe(display, use_container_width=True, hide_index=True)
 
 
# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    # ── Session defaults ──────────────────────
    if "pem_path" not in st.session_state:
        st.session_state.pem_path = os.environ.get("NBA_PEM_KEY", DEFAULT_PEM)
 
    # ── Header ────────────────────────────────
    now_str = datetime.now().strftime("%a %b %d  ·  %I:%M:%S %p")
    st.markdown(f"""
    <div class="header-bar">
        <div>
            <div class="header-title">🏀 NBA Dashboard</div>
            <div class="header-sub">Live Scores · Historical Analytics · Powered by Snowflake</div>
        </div>
        <div class="header-sub">{now_str}</div>
    </div>
    """, unsafe_allow_html=True)
 
    # ── Sidebar controls ──────────────────────
    with st.sidebar:
        st.header("Settings")
        pem_input = st.text_input("Historical PEM key path", value=st.session_state.pem_path)
        if pem_input != st.session_state.pem_path:
            st.session_state.pem_path = pem_input
            st.cache_resource.clear()
 
        st.markdown("---")
        auto_refresh = st.toggle("Auto-refresh live data", value=True)
        refresh_interval = st.selectbox("Refresh interval (s)", [15, 30, 60], index=1)
 
    # ── Fetch live games ──────────────────────
    live_games = pd.DataFrame()
    live_error = None

    with st.spinner("Fetching live games..."):
        try:
            live_games = fetch_live_games()
        except Exception as e:
            live_error = str(e)
 
    # ── Section 1: Live scoreboard ────────────
    st.markdown('<div class="section-title">🟢 Live Games Right Now</div>', unsafe_allow_html=True)
 
    if live_error:
        st.error(f"Could not connect to live data source: {live_error}")
    elif live_games.empty:
        st.info("No games are currently in progress. Check back when games are live.")
        selected_game = None
        selected_game_id = None
    else:
        # Mini scorecards (up to 4 per row)
        cols = st.columns(min(len(live_games), 4))
        for i, (_, g) in enumerate(live_games.iterrows()):
            with cols[i % 4]:
                render_live_card(g)
 
        # ── Dropdown to pick one game ─────────
        st.markdown("---")
        game_labels = {
            f"{row['h_team']} vs {row['a_team']}  (Q{row['quarter']}  {row['clock'] or ''})": row["game_id"]
            for _, row in live_games.iterrows()
            if row.get("quarter")
        }
        if not game_labels:
            game_labels = {
                f"{row['h_team']} vs {row['a_team']}": row["game_id"]
                for _, row in live_games.iterrows()
            }
 
        chosen_label = st.selectbox(
            "Select a game for detailed stats →",
            ["— pick a game —"] + list(game_labels.keys()),
        )
 
        if chosen_label == "— pick a game —":
            selected_game = None
            selected_game_id = None
            st.info("Pick a game from the dropdown above to see live progression and historical data.")
        else:
            selected_game_id = game_labels[chosen_label]
            selected_game = live_games[live_games["game_id"] == selected_game_id].iloc[0]
 
    # ── Section 2: Selected game deep-dive ───
    if selected_game is not None:
        h_team_code = selected_game["h_team"]
        a_team_code = selected_game["a_team"]
 
        st.markdown(f'<div class="section-title">📊 {h_team_code} vs {a_team_code} — Game Detail</div>',
                    unsafe_allow_html=True)
 
        # ── 2a. Score progression ─────────────
        history_df = fetch_score_history(selected_game_id)
        render_score_progression(history_df, h_team_code, a_team_code)
 
        # ── 2b. Historical data ───────────────
        st.markdown("---")
        st.markdown('<div class="section-title">📜 Historical Team Stats</div>',
                    unsafe_allow_html=True)
 
        # Try to connect to historical DB
        hist_ok = True
        try:
            teams_df  = load_teams()
            seasons   = load_seasons()
        except Exception as e:
            st.error(f"Could not connect to historical Snowflake DB: {e}")
            hist_ok = False
 
        if hist_ok:
            # Season picker (sidebar-style inline)
            sel_season = st.selectbox("Season", seasons, key="hist_season")
 
            # Match live tricode → historical team_id
            def find_team(tricode: str) -> tuple:
                row = teams_df[teams_df["abbreviation"].str.upper() == tricode.upper()]
                if row.empty:
                    return None, tricode
                return int(row.iloc[0]["team_id"]), row.iloc[0]["name"]
 
            h_id, h_name = find_team(h_team_code)
            a_id, a_name = find_team(a_team_code)
 
            # Two-column layout: one column per team
            col_h, col_a = st.columns(2)
 
            for col, t_id, t_name, t_code in [
                (col_h, h_id, h_name, h_team_code),
                (col_a, a_id, a_name, a_team_code),
            ]:
                with col:
                    st.subheader(f"{t_name} ({t_code})")
                    if t_id is None:
                        st.warning(f"Team '{t_code}' not found in historical DB.")
                        continue
 
                    record     = load_record(t_id, sel_season)
                    game_stats = load_game_stats(t_id, sel_season)
                    avg_fg_pct = game_stats["fg_pct"].mean() if not game_stats.empty else 0.0
 
                    render_hist_summary(record, avg_fg_pct, t_name)
 
                    with st.expander("Season Points Trend", expanded=True):
                        render_pts_chart(game_stats, t_name)
 
                    with st.expander("Season Averages"):
                        render_season_avgs(game_stats)
 
                    with st.expander("Roster Stats"):
                        roster = load_roster_stats(t_id, sel_season)
                        render_roster(roster)
 
            # Head-to-head section
            if h_id and a_id:
                st.markdown("---")
                st.markdown(f'<div class="section-title">🆚 Head-to-Head — {h_name} vs {a_name}</div>',
                            unsafe_allow_html=True)
                h2h = load_head_to_head(h_id, a_id, sel_season)
                render_head_to_head(h2h)
 
    # ── Footer + auto-refresh ─────────────────
    if auto_refresh and not live_error:
        time.sleep(refresh_interval)
        st.rerun()
 
 
if __name__ == "__main__":
    main()