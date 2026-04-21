"""
NBA Live Streaming Pipeline (Kafka-free)
-----------------------------------------
Polls the NBA CDN scoreboard API directly inside Spark using a background
thread + shared queue. Every 30 seconds Spark's foreachBatch trigger fires,
drains the queue into a DataFrame, and appends it to Snowflake LIVE_DATA.

Architecture (no Kafka):
    NBA CDN API
        ↓  (polled every 30s by background thread)
    Python Queue
        ↓  (drained every 30s by Spark micro-batch)
    Spark DataFrame
        ↓
    Snowflake LIVE_DATA

Run:
    spark-submit \
      --packages net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 \
      spark_streaming.py

Requirements:
    pip install pyspark requests cryptography
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import requests
import threading
import queue
import time
import os

# ---------------------------------------------------------------------------
# Configuration — update key_path if running locally
# ---------------------------------------------------------------------------
NBA_SCOREBOARD_URL  = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
POLL_INTERVAL_SEC   = 5      # How often to hit the NBA API
SNOWFLAKE_KEY_PATH  = "/home/compute/fbonetta-misteli/.ssh/rsa_key.p8"  # ← update for local
CHECKPOINT_PATH     = "/tmp/nba_stream_checkpoint"

# Shared queue — the poller thread puts game dicts here,
# Spark's foreachBatch drains it each trigger interval
game_queue: queue.Queue = queue.Queue()


# ---------------------------------------------------------------------------
# Private key helper
# ---------------------------------------------------------------------------
def get_private_key_string(key_path: str, password: str = None) -> str:
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=password.encode() if password else None,
            backend=default_backend()
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    pkb_str = pkb.decode("utf-8")
    pkb_str = pkb_str.replace("-----BEGIN PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("-----END PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("\n", "")
    return pkb_str


# ---------------------------------------------------------------------------
# NBA API poller (runs in a background thread)
# ---------------------------------------------------------------------------
def fetch_games() -> list:
    """Fetch today's scoreboard and return a list of game dicts."""
    r = requests.get(NBA_SCOREBOARD_URL, timeout=10)
    r.raise_for_status()
    return r.json()["scoreboard"]["games"]


def poller_thread(stop_event: threading.Event):
    """
    Background thread: polls the NBA API every POLL_INTERVAL_SEC seconds
    and puts each game as a dict onto game_queue.
    Runs independently of Spark — keeps collecting data between batches.
    """
    print(f"[Poller] Starting. Polling every {POLL_INTERVAL_SEC}s...")
    while not stop_event.is_set():
        try:
            games = fetch_games()
            for game in games:
                event = {
                    "game_id":     str(game["gameId"]),
                    "h_team":      str(game["homeTeam"]["teamTricode"]),
                    "a_team":      str(game["awayTeam"]["teamTricode"]),
                    "h_points":    int(game["homeTeam"]["score"] or 0),
                    "a_points":    int(game["awayTeam"]["score"] or 0),
                    "quarter":     int(game["period"] or 0),
                    "clock":       str(game["gameClock"] or ""),
                    "game_status": str(game["gameStatus"] or ""),
                }
                game_queue.put(event)
                print(f"[Poller] Queued: {event['h_team']} vs {event['a_team']} | "
                      f"Q{event['quarter']} {event['clock']} | "
                      f"{event['h_points']}-{event['a_points']}")

        except requests.RequestException as e:
            print(f"[Poller] API error: {e}")
        except Exception as e:
            print(f"[Poller] Unexpected error: {e}")

        # Wait for next poll, but check stop_event frequently so we can exit cleanly
        for _ in range(POLL_INTERVAL_SEC):
            if stop_event.is_set():
                break
            time.sleep(1)

    print("[Poller] Stopped.")


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def create_spark_session(app_name: str = "NBA-Live-Streaming") -> SparkSession:
    try:
        node        = os.environ["SLURMD_NODENAME"]
        master_port = os.environ["SPARK_MASTER_PORT"]
        master_url  = f"spark://{node}.engr.wustl.edu:{master_port}"
        print(f"[Spark] Connecting to cluster: {master_url}")
    except KeyError:
        print("[Spark] Cluster env vars not found — falling back to local[*]")
        master_url = "local[*]"

    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master_url) \
        .config("spark.driver.memory", "4g") \
        .config(
            "spark.jars.packages",
            ",".join([
                "net.snowflake:snowflake-jdbc:3.13.30",
                "net.snowflake:spark-snowflake_2.13:2.12.0-spark_3.4"
            ])
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
NBA_SCHEMA = StructType() \
    .add("game_id",     StringType()) \
    .add("h_team",      StringType()) \
    .add("a_team",      StringType()) \
    .add("h_points",    IntegerType()) \
    .add("a_points",    IntegerType()) \
    .add("quarter",     IntegerType()) \
    .add("clock",       StringType()) \
    .add("game_status", StringType())


# ---------------------------------------------------------------------------
# Streaming pipeline
# ---------------------------------------------------------------------------
def build_and_run_stream(spark: SparkSession):
    """
    Uses Spark's 'rate' source as a reliable micro-batch clock.
    Each batch trigger drains the shared queue, builds a DataFrame,
    and writes it to Snowflake.
    """
    sf_options = {
        "sfURL":           "sfedu02-unb02139.snowflakecomputing.com",
        "sfUser":          "DOLPHIN",
        "sfDatabase":      "DOLPHIN_DB",
        "sfSchema":        "PUBLIC",
        "sfWarehouse":     "DOLPHIN_WH",
        "pem_private_key": get_private_key_string(SNOWFLAKE_KEY_PATH, password="JackAbah25"),
    }

    # 'rate' source emits one row per second — we use it purely as a
    # reliable trigger clock. The actual data comes from game_queue.
    rate_stream = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()

    def process_batch(batch_df, batch_id):
        """Drain the queue and write whatever accumulated since the last batch."""
        rows = []
        while not game_queue.empty():
            try:
                rows.append(game_queue.get_nowait())
            except queue.Empty:
                break

        if not rows:
            print(f"[Batch {batch_id}] No new game data — skipping Snowflake write.")
            return

        print(f"[Batch {batch_id}] Writing {len(rows)} rows to Snowflake...")

        # Build a proper DataFrame from the queued dicts
        game_df = spark.createDataFrame(rows, schema=NBA_SCHEMA) \
                       .withColumn("processed_time", current_timestamp())

        game_df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .option("dbtable", "LIVE_DATA") \
            .mode("append") \
            .save()

        print(f"[Batch {batch_id}] Done.")

    query = rate_stream.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime=f"{POLL_INTERVAL_SEC} seconds") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start()

    return query


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    spark = create_spark_session()

    # Start the NBA poller in a background thread
    stop_event = threading.Event()
    poller = threading.Thread(target=poller_thread, args=(stop_event,), daemon=True)
    poller.start()

    # Give the poller one cycle to collect data before Spark's first batch fires
    print(f"[Main] Waiting {POLL_INTERVAL_SEC}s for initial data collection...")
    time.sleep(POLL_INTERVAL_SEC)

    # Start the Spark streaming query
    print("[Main] Starting Spark streaming query...")
    query = build_and_run_stream(spark)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")
        stop_event.set()
        query.stop()
        spark.stop()
        print("[Main] Shutdown complete.")