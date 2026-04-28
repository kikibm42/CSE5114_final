""" Script to stream data using spark for a Kafka topic 

Run:
    python spark_fromKafka.py

Requirements:
    pip install pyspark cryptography

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StringType, IntegerType
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pyspark.sql.types import DoubleType


kafka_broker = "localhost:9092"
topic = "NBA-live-updates"
checkpoint = "/tmp/nba_stream_checkpoint"

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

def create_spark_session(app_name="NBA-live-updates"):
    try:
        # Dynamically map to the active LinuxLab node and port
        node = os.environ['SLURMD_NODENAME']
        master_port = os.environ['SPARK_MASTER_PORT']
        master_url = f"spark://{node}.engr.wustl.edu:{master_port}"
    except KeyError:
        print("Warning: LinuxLab environment variables not found.")
        print("Falling back to local mode for local testing...")
        master_url = "local[*]"

    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master_url) \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.13:2.12.0-spark_3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()
    return spark

def build_stream(spark, ckpt_path=checkpoint):
    key_path = "~/CSE5114_final/passkeys/dolphin_key.p8"
    # Snowflake options
    sfOptions = {
      "sfURL": "sfedu02-unb02139.snowflakecomputing.com",
      "sfUser": "DOLPHIN",
      "sfDatabase": "DOLPHIN_DB",
      "sfSchema": "PUBLIC",
      "sfWarehouse": "DOLPHIN_WH",
      "pem_private_key": get_private_key_string(key_path)
    }

    # schema outline
    nba_schema = StructType() \
            .add("game_id", StringType()) \
            .add("h_team", StringType()) \
            .add("a_team", StringType()) \
            .add("h_points", IntegerType()) \
            .add("a_points", IntegerType()) \
            .add("quarter", IntegerType()) \
            .add("clock", StringType()) \
            .add("game_status", StringType()) 

    # Read kafka stream 
    spark_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    processed_df = spark_stream.select(from_json(col("value").cast("string"), nba_schema).alias("data")) \
            .select("data.*").withColumn("processed_time", current_timestamp())


    def snowflake_write(batch_df, batch_id):
        if batch_df.count() == 0:
            print(f"Batch {batch_id}: empty, skipping.")
            return
        batch_df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**sfOptions) \
            .option("dbtable", "LIVE_DATA") \
            .mode("append") \
            .save()
       
    sf_query = processed_df.writeStream.foreachBatch(snowflake_write).trigger(processingTime="5 seconds").option("checkpointLocation", ckpt_path).start()

    return sf_query
    
if __name__ == "__main__":
    spark = create_spark_session()
    query = build_stream(spark)
    query.awaitTermination()