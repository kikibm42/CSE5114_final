# NBA Game Intelligence Pipeline — CSE5114

End-to-end NBA analytics system: historical batch ingestion, a Snowflake star schema, a Streamlit dashboard, and a daily Airflow pipeline.

```
NBA API (stats.nba.com)
  └─ extractor.py ──► Parquet files ──► transform.py ──► Snowflake ──► Streamlit Dashboard
                                                              ▲
NBA CDN (live scoreboard)                                     │
  └─ kafka_ingestion.py ──► Kafka ──► spark_streaming_bones.py (LIVE_DATA table)
```

---

## Prerequisites

- Python 3.11 (Anaconda base env)
- **Java 17** (required for Spark — see note below)
- `rsa_key.p8` PEM private key in repo root (Snowflake auth)

```bash
pip install nba-api pyspark snowflake-connector-python cryptography \
            streamlit plotly apache-airflow kafka-python python-dateutil
```

**Java 17 is required.** Java 23 (macOS default) breaks Spark 3.4. Set before running any Spark or Airflow task:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.0.1.jdk/Contents/Home
```

Add to `~/.zshrc` to make permanent.

---

## Snowflake Setup

Run once to create the star schema:

```sql
-- In Snowflake worksheet:
-- snowflake/schema.sql
```

| Config | Value |
|--------|-------|
| Account | `sfedu02-unb02139` |
| Database | `MINNOW_DB` |
| Schema | `PUBLIC` |
| Warehouse | `MINNOW_WH` |
| User | `MINNOW` |

---

## Historical Pipeline

### 1. Extract

Fetches game results and box scores from `stats.nba.com` and writes partitioned Parquet files locally.

```bash
cd historical_pipeline

# Full backfill from 2019-10-01 to today
python extractor.py

# Resume from a specific date
python extractor.py --start 2023-10-01

# Skip chunks whose output folders already exist
python extractor.py --skip-existing

# Adjust rate-limit sleep between box score requests
python extractor.py --sleep 1.5
```

Output structure:
```
data/nba/raw/
  games/date=YYYY-MM-DD/data.parquet
  player_stats/date=YYYY-MM-DD/data.parquet
  team_stats/date=YYYY-MM-DD/data.parquet
```

Note: 2025-26+ seasons use `BoxScoreTraditionalV3` (NBA changed the API format); earlier seasons use V2. The extractor handles both automatically.

### 2. Repair (if needed)

If Spark throws type mismatch errors across parquet partitions, run this once to normalize dtypes:

```bash
python repair_parquet.py
```

### 3. Transform

Reads all Parquet files and loads them into Snowflake. Safe to re-run — `dedup_and_write` skips already-loaded game IDs.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.0.1.jdk/Contents/Home

python transform.py --local                        # Mac / local Spark
python transform.py --local --pem-key ../rsa_key.p8  # explicit key path
python transform.py --data-dir /path/to/data/nba/raw  # custom data dir
```

---

## Star Schema

```
dim_team       — team_id, abbreviation, name, city, conference, division
dim_player     — player_id, name, position, height, weight
dim_date       — date, season (e.g. "2023-24"), season_type ("Regular Season" | "Playoffs" | "Pre-Season")

fact_game               — game_id, date, home_team_id, away_team_id, home_score, away_score
fact_game_team_stats    — per-team box score stats per game (FGM/FGA, 3PM/3PA, REB, AST, STL, BLK, TOV, PTS, ...)
fact_game_player_stats  — per-player box score stats per game
LIVE_DATA               — live game snapshots from streaming pipeline
```

To inspect row counts:
```sql
-- snowflake/show_tables.sql
```

---

## Dashboard

```bash
export NBA_PEM_KEY=/path/to/rsa_key.p8  # or enter path in the sidebar
cd dashboard
streamlit run app.py
```

Features:
- Team W/L record, avg points for/against, avg FG% for any team + season
- Points-per-game trend chart
- Season averages table
- Per-player roster stats (GP, PPG, RPG, APG, SPG, BPG, FG%, 3P%, FT%)

---

## Airflow Daily Pipeline

The DAG `nba_daily_pipeline` runs at **6 AM daily**, extracting and loading the previous day's games into Snowflake automatically.

### One-time setup

```bash
airflow db migrate
ln -s /Users/anandparekh/Documents/GitHub/CSE5114_final/airflow/dags/nba_daily_pipeline.py ~/airflow/dags/
```

### Start Airflow

Airflow 3.x requires three separate processes:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.0.1.jdk/Contents/Home
airflow scheduler &
airflow dag-processor &
airflow webserver -p 8080 &
```

Web UI: http://localhost:8080

### Stop Airflow

```bash
pkill -f "airflow scheduler"
pkill -f "airflow dag-processor"
pkill -f "airflow webserver"
```

### Test a single task (dry run, no scheduling)

```bash
airflow tasks test nba_daily_pipeline extract 2024-11-15
airflow tasks test nba_daily_pipeline transform 2024-11-15
```

---

## Live Pipeline (incomplete)

| File | Status | Description |
|------|--------|-------------|
| `live_pipeline/kafka_ingestion.py` | Incomplete | Kafka producer polling NBA CDN live scoreboard — missing `producer.send()` and main loop |
| `live_pipeline/spark_streaming_bones.py` | Incomplete | Spark Streaming consumer writing to `LIVE_DATA` — missing Kafka bootstrap/topic options and PySpark type imports |

Requires Kafka running at `localhost:9092` and the `LIVE_DATA` table created via `snowflake/schema.sql`.

---

## Known Issues

- **Snowflake Spark connector writes by column position**, not name — DataFrame column order must exactly match the schema DDL order.
- **Mixed Parquet dtypes**: `spark.sql.parquet.enableVectorizedReader=false` is set in `transform.py` to handle stat columns stored as different numeric types across partition chunks.
- **dim_player duplicates**: A player can have multiple rows with different positions (G, F, blank). The roster stats query deduplicates via a `GROUP BY player_id` subquery before joining.
- **Airflow 3.x architecture change**: Unlike 2.x, the scheduler no longer parses DAG files directly — `airflow dag-processor` must run as a separate process.
