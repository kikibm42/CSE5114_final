"""
Script to poll the NBA API and stream using spark to a live table in snowflake 

Run:
    python spark_streaming_noKafka.py

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
from nba_api.live.nba.endpoints import scoreboard

# NBA_SCOREBOARD_URL  = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
POLL_INTERVAL_SEC   = 5      # Used for the poller and spark ingestion
SNOWFLAKE_KEY_PATH  = "~/CSE5114_final/passkeys/dolphin_key.p8"  # unencrypted file for snowflake connection to dolphin database
CHECKPOINT_PATH     = "/tmp/nba_stream_checkpoint"

# Poller adds games to the game queue which spark will drain in micro-batches
game_queue: queue.Queue = queue.Queue()


# Helper function for private key
def get_private_key_string(key_path: str) -> str:
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_str = pkb.decode("utf-8")
    key_str = key_str.replace("-----BEGIN PRIVATE KEY-----", "")
    key_str = key_str.replace("-----END PRIVATE KEY-----", "")
    key_str = key_str.replace("\n", "")
    return key_str

# Collects games from the NBA endpoint -- one for the CDN and one using the API directly 
# def fetch_games() -> list:
#     """Fetch today's scoreboard and return a list of game dicts."""
#     r = requests.get(NBA_SCOREBOARD_URL, timeout=10)
#     r.raise_for_status()
#     return r.json()["scoreboard"]["games"]
def fetch_games() -> list:
    board = scoreboard.ScoreBoard()
    games = board.get_dict()["scoreboard"]["games"]
    return games

# Creates a background thread to poll the API endpoint every POLL_INTERVAL_SECONDS seconds
def poller_thread(stop_event: threading.Event):

    print(f"[Poller] Starting. Polling every {POLL_INTERVAL_SEC}s...")

    # Puts the fetched games into the game_queue
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

        # Pause between poll intervals and check if stop event is triggered to exit 
        for _ in range(POLL_INTERVAL_SEC):
            if stop_event.is_set():
                break
            time.sleep(1)

    print("[Poller] Stopped.")


# Creates spark session for draining the queue
def create_spark_session(app_name: str = "NBA-Live-Streaming") -> SparkSession:

    # WUSTL cluster info
    try:
        node        = os.environ["SLURMD_NODENAME"]
        master_port = os.environ["SPARK_MASTER_PORT"]
        master_url  = f"spark://{node}.engr.wustl.edu:{master_port}"
        print(f"[Spark] Connecting to cluster: {master_url}")
    except KeyError:
        print("[Spark] Cluster env vars not found — falling back to local[*]")
        master_url = "local[*]"

    # Build spark session
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


# Snowflake schema initialization 
NBA_SCHEMA = StructType() \
    .add("game_id",     StringType()) \
    .add("h_team",      StringType()) \
    .add("a_team",      StringType()) \
    .add("h_points",    IntegerType()) \
    .add("a_points",    IntegerType()) \
    .add("quarter",     IntegerType()) \
    .add("clock",       StringType()) \
    .add("game_status", StringType())


# Build and start spark stream to snowflake
def build_and_run_stream(spark: SparkSession):

    # Snowflake database connection options
    sf_options = {
        "sfURL":           "sfedu02-unb02139.snowflakecomputing.com",
        "sfUser":          "DOLPHIN",
        "sfDatabase":      "DOLPHIN_DB",
        "sfSchema":        "PUBLIC",
        "sfWarehouse":     "DOLPHIN_WH",
        "pem_private_key": get_private_key_string(SNOWFLAKE_KEY_PATH),
    }

    # rate source emits one row per second and is used as a clock for the spark stream
    rate_stream = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load()

    # Drains game_queue and writes to snowflake
    def process_batch(batch_df, batch_id):
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

        # Build a DataFrame from the queued dicts
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



if __name__ == "__main__":
    spark = create_spark_session()

    # Start the NBA poller in a background thread
    stop_event = threading.Event()
    poller = threading.Thread(target=poller_thread, args=(stop_event,), daemon=True)
    poller.start()

    # Give the poller one cycle to collect data before spark's first batch runs
    print(f"[Main] Waiting {POLL_INTERVAL_SEC}s for initial data collection...")
    time.sleep(POLL_INTERVAL_SEC)

    # Start the spark streaming query
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