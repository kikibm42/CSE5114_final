"""
NBA Live Dashboard
------------------
Reads live game data from Snowflake (LIVE_DATA table written by Spark consumer)
and renders a real-time scoreboard dashboard using Streamlit.

Run:
    streamlit run dashboard.py

Requirements:
    pip install streamlit snowflake-connector-python pandas plotly
"""

import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go
import time
from datetime import datetime
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="NBA Live Dashboard",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
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

    /* Game card */
    .game-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #2a2a4a;
        border-radius: 12px;
        padding: 20px 24px;
        margin-bottom: 16px;
        position: relative;
        overflow: hidden;
    }
    .game-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        background: linear-gradient(90deg, #e8500a, #f5a623);
    }
    .game-card.final::before  { background: #555; }
    .game-card.live::before   { background: linear-gradient(90deg, #00c853, #69f0ae); }

    .team-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin: 6px 0;
    }
    .team-name {
        font-family: 'Barlow Condensed', sans-serif;
        font-size: 2rem;
        font-weight: 800;
        letter-spacing: 2px;
        color: #ffffff;
    }
    .team-score {
        font-family: 'Barlow Condensed', sans-serif;
        font-size: 2.6rem;
        font-weight: 800;
        color: #f5a623;
    }
    .team-score.winning { color: #69f0ae; }

    .game-meta {
        font-size: 0.8rem;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 10px;
        border-top: 1px solid #2a2a4a;
        padding-top: 8px;
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
    .final-badge {
        display: inline-block;
        background: #333;
        color: #aaa;
        font-size: 0.65rem;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 20px;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-right: 8px;
    }

    /* Header bar */
    .header-bar {
        display: flex;
        justify-content: space-between;
        align-items: flex-end;
        margin-bottom: 28px;
        border-bottom: 1px solid #2a2a4a;
        padding-bottom: 16px;
    }
    .header-title {
        font-family: 'Barlow Condensed', sans-serif;
        font-size: 2.4rem;
        font-weight: 800;
        letter-spacing: 3px;
        text-transform: uppercase;
        color: #ffffff;
    }
    .header-sub {
        font-size: 0.8rem;
        color: #666;
        letter-spacing: 1px;
    }

    /* Stats table */
    .stDataFrame { border-radius: 8px; overflow: hidden; }

    /* Sidebar toggle */
    section[data-testid="stSidebar"] {
        background-color: #111122;
    }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Snowflake connection (cached so it persists across reruns)
# ---------------------------------------------------------------------------
def get_private_key(key_path, password=None):
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=password.encode() if password else None,
            backend=default_backend()
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


@st.cache_resource
def get_snowflake_connection():
    """Create and cache a Snowflake connection for the session."""
    key_path = "/home/compute/fbonetta-misteli/.ssh/rsa_key.p8"
    private_key = get_private_key(key_path, password="JackAbah25")

    conn = snowflake.connector.connect(
        user="DOLPHIN",
        account="sfedu02-unb02139",
        database="DOLPHIN_DB",
        schema="PUBLIC",
        warehouse="DOLPHIN_WH",
        private_key=private_key,
    )
    return conn


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------
def fetch_live_games(conn) -> pd.DataFrame:
    """
    Fetch the most recent snapshot of each active game from LIVE_DATA.
    Uses MAX(processed_time) per game_id so we always show the latest row.
    """
    query = """
        SELECT
            l.game_id,
            l.h_team,
            l.a_team,
            l.h_points,
            l.a_points,
            l.quarter,
            l.clock,
            l.game_status,
            l.processed_time
        FROM LIVE_DATA l
        INNER JOIN (
            SELECT game_id, MAX(processed_time) AS latest
            FROM LIVE_DATA
            GROUP BY game_id
        ) latest_rows
          ON l.game_id = latest_rows.game_id
         AND l.processed_time = latest_rows.latest
        ORDER BY l.processed_time DESC
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [desc[0].lower() for desc in cur.description]
    cur.close()
    return pd.DataFrame(rows, columns=cols)


def fetch_score_history(conn, game_id: str) -> pd.DataFrame:
    """Fetch all score snapshots for a single game (for the sparkline chart)."""
    query = f"""
        SELECT
            processed_time,
            h_points,
            a_points,
            h_team,
            a_team,
            quarter,
            clock
        FROM LIVE_DATA
        WHERE game_id = '{game_id}'
        ORDER BY processed_time ASC
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [desc[0].lower() for desc in cur.description]
    cur.close()
    return pd.DataFrame(rows, columns=cols)


def fetch_recent_history(conn, limit: int = 50) -> pd.DataFrame:
    """Fetch the last N rows across all games for the raw log table."""
    query = f"""
        SELECT
            game_id,
            h_team,
            a_team,
            h_points,
            a_points,
            quarter,
            clock,
            game_status,
            processed_time
        FROM LIVE_DATA
        ORDER BY processed_time DESC
        LIMIT {limit}
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [desc[0].lower() for desc in cur.description]
    cur.close()
    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------------------------------
# UI components
# ---------------------------------------------------------------------------
def render_game_card(game: pd.Series):
    """Render a single game as an HTML card."""
    status = str(game.get("game_status", "")).strip().upper()
    is_live = status not in ("FINAL", "PPD", "")
    card_class = "live" if is_live else "final"

    h_pts = int(game.get("h_points", 0) or 0)
    a_pts = int(game.get("a_points", 0) or 0)
    h_winning = "winning" if h_pts > a_pts else ""
    a_winning = "winning" if a_pts > h_pts else ""

    badge = (
        f'<span class="live-badge">● Live</span>'
        if is_live else
        f'<span class="final-badge">Final</span>'
    )

    quarter_str = f"Q{game.get('quarter', '—')}" if game.get("quarter") else ""
    clock_str   = game.get("clock", "") or ""

    st.markdown(f"""
    <div class="game-card {card_class}">
        <div class="team-row">
            <span class="team-name">{game.get('h_team', '—')}</span>
            <span class="team-score {h_winning}">{h_pts}</span>
        </div>
        <div class="team-row">
            <span class="team-name">{game.get('a_team', '—')}</span>
            <span class="team-score {a_winning}">{a_pts}</span>
        </div>
        <div class="game-meta">
            {badge}
            {quarter_str}{"  ·  " + clock_str if clock_str else ""}
            &nbsp;&nbsp;·&nbsp;&nbsp;
            Game ID: {game.get('game_id', '—')}
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_score_chart(history_df: pd.DataFrame, h_team: str, a_team: str):
    """Render a Plotly line chart of score progression over time."""
    if history_df.empty:
        st.info("No historical data yet for this game.")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=history_df["processed_time"],
        y=history_df["h_points"],
        name=h_team,
        line=dict(color="#f5a623", width=2.5),
        mode="lines+markers",
        marker=dict(size=5),
    ))
    fig.add_trace(go.Scatter(
        x=history_df["processed_time"],
        y=history_df["a_points"],
        name=a_team,
        line=dict(color="#69f0ae", width=2.5),
        mode="lines+markers",
        marker=dict(size=5),
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(22,22,46,0.6)",
        font=dict(family="Barlow Condensed", color="#aaa", size=13),
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(gridcolor="#2a2a4a", showgrid=True),
        yaxis=dict(gridcolor="#2a2a4a", showgrid=True),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------
def main():
    # Header
    now_str = datetime.now().strftime("%a %b %d  ·  %I:%M:%S %p")
    st.markdown(f"""
    <div class="header-bar">
        <div>
            <div class="header-title">🏀 NBA Live Scores</div>
            <div class="header-sub">Powered by Snowflake · Spark Streaming</div>
        </div>
        <div class="header-sub">{now_str}</div>
    </div>
    """, unsafe_allow_html=True)

    # Auto-refresh control
    col_r, col_i = st.columns([3, 1])
    with col_r:
        auto_refresh = st.toggle("Auto-refresh", value=True)
    with col_i:
        refresh_interval = st.selectbox("Interval", [15, 30, 60], index=1, label_visibility="collapsed")

    # Connect to Snowflake
    try:
        conn = get_snowflake_connection()
    except Exception as e:
        st.error(f"Could not connect to Snowflake: {e}")
        st.stop()

    # ---- Live scoreboard ---------------------------------------------------
    st.markdown("### Today's Games")
    games_df = fetch_live_games(conn)

    if games_df.empty:
        st.info("No game data found in LIVE_DATA. Make sure spark_streaming.py is running.")
    else:
        # Lay out cards in a 3-column grid
        cols = st.columns(3)
        for i, (_, game) in enumerate(games_df.iterrows()):
            with cols[i % 3]:
                render_game_card(game)

    # ---- Score progression charts -----------------------------------------
    st.markdown("---")
    st.markdown("### Score Progression")

    if not games_df.empty:
        game_options = {
            f"{row['h_team']} vs {row['a_team']}": row["game_id"]
            for _, row in games_df.iterrows()
        }
        selected_label = st.selectbox("Select a game", list(game_options.keys()))
        selected_game_id = game_options[selected_label]
        selected_game    = games_df[games_df["game_id"] == selected_game_id].iloc[0]

        history_df = fetch_score_history(conn, selected_game_id)
        render_score_chart(
            history_df,
            h_team=selected_game["h_team"],
            a_team=selected_game["a_team"],
        )
    else:
        st.info("No games to chart yet.")

    # ---- Raw event log -----------------------------------------------------
    st.markdown("---")
    st.markdown("### Raw Event Log (last 50 rows)")

    log_df = fetch_recent_history(conn)
    if not log_df.empty:
        st.dataframe(
            log_df.rename(columns={
                "game_id": "Game ID", "h_team": "Home", "a_team": "Away",
                "h_points": "H Pts", "a_points": "A Pts", "quarter": "Qtr",
                "clock": "Clock", "game_status": "Status", "processed_time": "Last Updated"
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No log data yet.")

    # ---- Footer + auto-refresh --------------------------------------------
    st.markdown(f"""
    <div style="text-align:center; color:#444; font-size:0.75rem; margin-top:40px; letter-spacing:1px;">
        DATA SOURCE: SNOWFLAKE · LIVE_DATA TABLE · REFRESHES EVERY {refresh_interval}s
    </div>
    """, unsafe_allow_html=True)

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()