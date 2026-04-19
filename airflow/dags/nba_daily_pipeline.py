"""
Daily NBA pipeline DAG.

Schedule: runs at 6 AM every day, processing the previous day's games.

Tasks:
  1. extract  — runs extractor.py for the logical date (yesterday's games)
  2. transform — runs transform.py to load new Parquet data into Snowflake

Airflow's `ds` template variable is the logical execution date (YYYY-MM-DD),
which for a @daily schedule is always "yesterday" relative to wall-clock time.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

from airflow import DAG

# ──────────────────────────────────────────────
# CONFIG — update paths to match your environment
# ──────────────────────────────────────────────
REPO_DIR = (
    "/Users/anandparekh/Documents/GitHub/CSE5114_final"  # absolute path to repo root
)
PYTHON = (
    "/Users/anandparekh/anaconda3/bin/python"  # e.g. /home/user/anaconda3/bin/python
)
PEM_KEY = "/Users/anandparekh/Documents/GitHub/CSE5114_final/rsa_key.p8"
DATA_DIR = f"{REPO_DIR}/data/nba/raw"

default_args = {
    "owner": "nba-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="nba_daily_pipeline",
    description="Extract yesterday's NBA games → transform → load to Snowflake",
    schedule="0 6 * * *",  # 6 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,  # don't backfill missed runs
    default_args=default_args,
    tags=["nba"],
) as dag:
    # ── Task 1: Extract one day of raw data ──
    # `ds` = logical execution date = the day being processed (yesterday's games)
    extract = BashOperator(
        task_id="extract",
        bash_command=(
            f"{PYTHON} {REPO_DIR}/historical_pipeline/extractor.py "
            "--start {{ ds }} "
            "--end {{ ds }} "
            "--chunk-months 1 "  # one-day range, no chunking needed
            "--sleep 1.0"
        ),
    )

    # ── Task 2: Transform Parquet → Snowflake ──
    # dedup_and_write in transform.py ensures only new game_ids are inserted
    transform = BashOperator(
        task_id="transform",
        bash_command=(
            f"{PYTHON} {REPO_DIR}/historical_pipeline/transform.py "
            f"--data-dir {DATA_DIR} "
            f"--pem-key {PEM_KEY} "
            "--local"
        ),
    )

    extract >> transform
