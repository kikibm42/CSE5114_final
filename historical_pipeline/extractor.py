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
  python extractor.py
  (date range hardcoded below for now)
"""

import logging
import os
import time

import pandas as pd
from nba_api.stats.endpoints import boxscoretraditionalv2, leaguegamefinder

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
START_DATE = "01/01/2024"
END_DATE = "01/01/2025"

OUTPUT_DIR = "./data/nba/raw"
SLEEP_BETWEEN_REQUESTS = 0.7  # seconds

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


def save_parquet(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    log.info(f"  Saved {len(df)} rows → {path}")


def main():
    log.info("=" * 50)
    log.info(f"NBA Extractor starting: {START_DATE} → {END_DATE}")
    log.info("=" * 50)

    partition = START_DATE.replace("/", "-")

    # ── Step 1: Get all games in date range ──
    log.info("Fetching game list...")
    games_df = leaguegamefinder.LeagueGameFinder(
        date_from_nullable=START_DATE,
        date_to_nullable=END_DATE,
        league_id_nullable="00",  # NBA only
    ).get_data_frames()[0]

    if games_df.empty:
        log.warning("No games found for this date range. Exiting.")
        return

    log.info(f"Found {games_df['GAME_ID'].nunique()} games")
    save_parquet(games_df, f"{OUTPUT_DIR}/games/date={partition}/data.parquet")

    # ── Step 2: Fetch raw box scores for each game ──
    game_ids = games_df["GAME_ID"].unique().tolist()
    log.info(f"Fetching box scores for {len(game_ids)} games...")

    all_player_stats = []
    all_team_stats = []

    for i, game_id in enumerate(game_ids):
        log.info(f"  [{i + 1}/{len(game_ids)}] game {game_id}")
        try:
            box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            dfs = box.get_data_frames()

            all_player_stats.append(dfs[0])  # raw PlayerStats
            all_team_stats.append(dfs[1])  # raw TeamStats

        except Exception as e:
            log.warning(f"  Failed for game {game_id}: {e}")

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # ── Step 3: Save raw box score data ──
    if all_player_stats:
        save_parquet(
            pd.concat(all_player_stats, ignore_index=True),
            f"{OUTPUT_DIR}/player_stats/date={partition}/data.parquet",
        )

    if all_team_stats:
        save_parquet(
            pd.concat(all_team_stats, ignore_index=True),
            f"{OUTPUT_DIR}/team_stats/date={partition}/data.parquet",
        )

    log.info("=" * 50)
    log.info("Extraction complete.")
    log.info("=" * 50)


if __name__ == "__main__":
    main()
