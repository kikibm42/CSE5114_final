"""
Pulls raw data from stats.nba.com for a given date range and writes
raw API responses to partitioned parquet files on disk.

No transformation — just raw data as-is from the API.
Transformation happens downstream in the Spark job.

Output structure:
  /data/nba/raw/games/date=YYYY-MM-DD/data.parquet
  /data/nba/raw/player_stats/date=YYYY-MM-DD/data.parquet
  /data/nba/raw/team_stats/date=YYYY-MM-DD/data.parquet

Usage:
  # Default: 2019-10-01 → today in 6-month chunks
  python extractor.py

  # Resume from chunk 5 (skip already-completed chunks)
  python extractor.py --start 2021-10-01

  # Skip chunks whose output folders already exist on disk
  python extractor.py --skip-existing

  # Adjust chunk size and sleep
  python extractor.py --start 2019-10-01 --end 2024-06-30 --chunk-months 6 --sleep 1.2
"""

import argparse
import logging
import os
import time
from datetime import date, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta
from nba_api.stats.endpoints import (
    boxscoretraditionalv2,
    boxscoretraditionalv3,
    leaguegamefinder,
)

# ──────────────────────────────────────────────
# DEFAULTS
# ──────────────────────────────────────────────
DEFAULT_START = date(2019, 10, 1)  # start of 2019-20 season
DEFAULT_END = date.today()
DEFAULT_CHUNK_MONTHS = 6
DEFAULT_SLEEP = 1.0  # seconds between box score requests
LONG_PAUSE_EVERY = 100  # games
LONG_PAUSE_SECONDS = 10
MAX_RETRIES = 3
RETRY_BACKOFF = 5  # extra seconds per retry attempt

OUTPUT_DIR = "./data/nba/raw"

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill raw NBA data from stats.nba.com")
    p.add_argument(
        "--start",
        default=DEFAULT_START.isoformat(),
        help="Start date (YYYY-MM-DD). Default: 2019-10-01",
    )
    p.add_argument(
        "--end",
        default=DEFAULT_END.isoformat(),
        help="End date (YYYY-MM-DD). Default: today",
    )
    p.add_argument(
        "--chunk-months",
        type=int,
        default=DEFAULT_CHUNK_MONTHS,
        help="Months per API chunk to avoid rate-limit timeouts. Default: 6",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help="Seconds between box score requests. Default: 1.0",
    )
    p.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip chunks whose output folder already exists on disk.",
    )
    return p.parse_args()


def date_chunks(start: date, end: date, months: int):
    """Yield (chunk_start, chunk_end) pairs covering [start, end]."""
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + relativedelta(months=months) - timedelta(days=1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def nba_date(d: date) -> str:
    """Format date as MM/DD/YYYY for nba_api."""
    return d.strftime("%m/%d/%Y")


def normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce mixed-type columns that cause PyArrow / Spark schema errors."""
    if "MIN" in df.columns:
        df["MIN"] = df["MIN"].astype(str)
    if "PLUS_MINUS" in df.columns:
        df["PLUS_MINUS"] = df["PLUS_MINUS"].astype("float64")
    return df


def save_parquet(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    log.info(f"  Saved {len(df)} rows → {path}")


_PLAYER_Vz_TO_V2 = {
    "gameId": "GAME_ID",
    "teamId": "TEAM_ID",
    "teamTricode": "TEAM_ABBREVIATION",
    "teamName": "TEAM_NAME",
    "personId": "PLAYER_ID",
    "position": "START_POSITION",
    "minutes": "MIN",
    "fieldGoalsMade": "FGM",
    "fieldGoalsAttempted": "FGA",
    "fieldGoalsPercentage": "FG_PCT",
    "threePointersMade": "FG3M",
    "threePointersAttempted": "FG3A",
    "threePointersPercentage": "FG3_PCT",
    "freeThrowsMade": "FTM",
    "freeThrowsAttempted": "FTA",
    "freeThrowsPercentage": "FT_PCT",
    "reboundsOffensive": "OREB",
    "reboundsDefensive": "DREB",
    "reboundsTotal": "REB",
    "assists": "AST",
    "steals": "STL",
    "blocks": "BLK",
    "turnovers": "TO",
    "foulsPersonal": "PF",
    "points": "PTS",
    "plusMinusPoints": "PLUS_MINUS",
}
_TEAM_V3_NUMERIC = [
    "fieldGoalsMade",
    "fieldGoalsAttempted",
    "threePointersMade",
    "threePointersAttempted",
    "freeThrowsMade",
    "freeThrowsAttempted",
    "reboundsOffensive",
    "reboundsDefensive",
    "reboundsTotal",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "foulsPersonal",
    "points",
]
_TEAM_V3_TO_V2 = {
    "gameId": "GAME_ID",
    "teamId": "TEAM_ID",
    "teamTricode": "TEAM_ABBREVIATION",
    "teamName": "TEAM_NAME",
    "fieldGoalsMade": "FGM",
    "fieldGoalsAttempted": "FGA",
    "threePointersMade": "FG3M",
    "threePointersAttempted": "FG3A",
    "freeThrowsMade": "FTM",
    "freeThrowsAttempted": "FTA",
    "reboundsOffensive": "OREB",
    "reboundsDefensive": "DREB",
    "reboundsTotal": "REB",
    "assists": "AST",
    "steals": "STL",
    "blocks": "BLK",
    "turnovers": "TO",
    "foulsPersonal": "PF",
    "points": "PTS",
}


def normalize_v3_to_v2(
    player_df: pd.DataFrame, team_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convert BoxScoreTraditionalV3 camelCase DataFrames to V2 UPPER_CASE format."""
    # ── Player stats ──
    player_df = player_df.rename(columns=_PLAYER_V3_TO_V2)
    player_df["PLAYER_NAME"] = (
        player_df["firstName"].fillna("") + " " + player_df["familyName"].fillna("")
    ).str.strip()
    for col in ("PLAYER_ID", "TEAM_ID"):
        if col in player_df.columns:
            player_df[col] = player_df[col].astype("Int64")
    _stat_cols = [
        "FGM",
        "FGA",
        "FG3M",
        "FG3A",
        "FTM",
        "FTA",
        "OREB",
        "DREB",
        "REB",
        "AST",
        "STL",
        "BLK",
        "TO",
        "PF",
        "PTS",
    ]
    for col in _stat_cols:
        if col in player_df.columns:
            player_df[col] = player_df[col].astype("float64")

    # ── Team stats: aggregate Starters + Bench into one row per (gameId, teamId) ──
    key = ["gameId", "teamId", "teamTricode", "teamName"]
    team_df = team_df.groupby(key, as_index=False)[_TEAM_V3_NUMERIC].sum()
    team_df = team_df.rename(columns=_TEAM_V3_TO_V2)
    team_df["FG_PCT"] = team_df["FGM"] / team_df["FGA"].replace(0, float("nan"))
    team_df["FG3_PCT"] = team_df["FG3M"] / team_df["FG3A"].replace(0, float("nan"))
    team_df["FT_PCT"] = team_df["FTM"] / team_df["FTA"].replace(0, float("nan"))
    team_df["PLUS_MINUS"] = float("nan")
    for col in ("TEAM_ID",):
        if col in team_df.columns:
            team_df[col] = team_df[col].astype("Int64")
    _stat_cols = [
        "FGM",
        "FGA",
        "FG3M",
        "FG3A",
        "FTM",
        "FTA",
        "OREB",
        "DREB",
        "REB",
        "AST",
        "STL",
        "BLK",
        "TO",
        "PF",
        "PTS",
    ]
    for col in _stat_cols:
        if col in team_df.columns:
            team_df[col] = team_df[col].astype("float64")

    return player_df, team_df


def get_box_score(game_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch player and team box score DataFrames for a game.

    Uses BoxScoreTraditionalV3 for 2025-26+ games (v2 no longer publishes
    data for those), v2 for all earlier seasons.
    Game ID format: 00SYYYY##### where YY (positions 3-4) is the 2-digit season year.
    """
    season_year = int(game_id[3:5])
    if season_year >= 25:
        box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
        dfs = box.get_data_frames()
        return normalize_v3_to_v2(dfs[0], dfs[1])
    else:
        box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        dfs = box.get_data_frames()
        return dfs[0], dfs[1]


def fetch_with_retry(
    game_id: str, sleep: float
) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Fetch box score with exponential backoff retries for rate-limit errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return get_box_score(game_id)
        except Exception as e:
            err = str(e)
            is_rate_limit = "Expecting value" in err or "Read timed out" in err
            if is_rate_limit and attempt < MAX_RETRIES:
                wait = sleep + RETRY_BACKOFF * attempt
                log.warning(
                    f"  Rate limited on game {game_id} (attempt {attempt}) — retrying in {wait:.0f}s"
                )
                time.sleep(wait)
            else:
                log.warning(f"  Failed for game {game_id}: {e}")
                return None
    return None


def extract_chunk(chunk_start: date, chunk_end: date, sleep: float):
    partition = chunk_start.isoformat()
    log.info(f"── Chunk {chunk_start} → {chunk_end} ──")

    # ── Step 1: Get all games in this chunk ──
    games_df = leaguegamefinder.LeagueGameFinder(
        date_from_nullable=nba_date(chunk_start),
        date_to_nullable=nba_date(chunk_end),
        league_id_nullable="00",  # NBA only
    ).get_data_frames()[0]

    if games_df.empty:
        log.warning("  No games found — skipping.")
        return

    game_ids = games_df["GAME_ID"].unique().tolist()
    log.info(f"  {len(game_ids)} games found")
    save_parquet(games_df, f"{OUTPUT_DIR}/games/date={partition}/data.parquet")

    # ── Step 2: Fetch box scores ──
    all_player_stats, all_team_stats = [], []

    for i, game_id in enumerate(game_ids):
        log.info(f"  [{i + 1}/{len(game_ids)}] game {game_id}")

        result = fetch_with_retry(game_id, sleep)
        if result is not None:
            all_player_stats.append(result[0])
            all_team_stats.append(result[1])

        time.sleep(sleep)

        # Longer pause every N games to avoid sustained rate-limiting
        if (i + 1) % LONG_PAUSE_EVERY == 0:
            log.info(f"  Pausing {LONG_PAUSE_SECONDS}s after {i + 1} games...")
            time.sleep(LONG_PAUSE_SECONDS)

    # ── Step 3: Save raw box score data ──
    if all_player_stats:
        save_parquet(
            normalize_dtypes(pd.concat(all_player_stats, ignore_index=True)),
            f"{OUTPUT_DIR}/player_stats/date={partition}/data.parquet",
        )
    if all_team_stats:
        save_parquet(
            normalize_dtypes(pd.concat(all_team_stats, ignore_index=True)),
            f"{OUTPUT_DIR}/team_stats/date={partition}/data.parquet",
        )


def main():
    args = parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    chunks = list(date_chunks(start, end, args.chunk_months))
    log.info("=" * 50)
    log.info(
        f"NBA Extractor: {start} → {end} | {len(chunks)} chunks | "
        f"{args.chunk_months}-month windows | sleep={args.sleep}s"
    )
    log.info("=" * 50)

    for idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
        partition = chunk_start.isoformat()
        existing_path = f"{OUTPUT_DIR}/games/date={partition}/data.parquet"

        if args.skip_existing and os.path.exists(existing_path):
            log.info(
                f"[Chunk {idx}/{len(chunks)}] Skipping {partition} — already exists."
            )
            continue

        log.info(f"[Chunk {idx}/{len(chunks)}]")
        extract_chunk(chunk_start, chunk_end, args.sleep)

    log.info("=" * 50)
    log.info("Backfill complete.")
    log.info("=" * 50)


if __name__ == "__main__":
    main()
