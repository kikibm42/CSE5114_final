from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.window import Window
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pyspark.sql.types import DoubleType

def get_private_key_string(key_path, password=None):
    """Reads a PEM private key and returns the string format required by PySpark."""
    with open(key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=password.encode() if password else None,
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # Spark requires the raw key string without headers, footers, or newlines
    pkb_str = pkb.decode("utf-8")
    pkb_str = pkb_str.replace("-----BEGIN PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("-----END PRIVATE KEY-----", "")
    pkb_str = pkb_str.replace("\n", "")
    return pkb_str

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
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.30,net.snowflake:spark-snowflake_2.13:2.12.0-spark_3.4") \
        .getOrCreate()
    return spark

def build_stream(spark, ckpt_path):
    # Snowflake options
    sfOptions = {
      "sfURL": "sfedu02-unb02139.snowflakecomputing.com",
      "sfUser": "DOLPHIN",
      "sfDatabase": "DOLPHIN_DB",
      "sfSchema": "PUBLIC",
      "sfWarehouse": "DOLPHIN_WH",
      "pem_private_key": "/home/compute/fbonetta-misteli/.ssh/rsa_key.p8"
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

    # Read kafka stream -- need to configure properly
    spark_stream = spark.readStream.format("kafka")

    processed_df = spark_stream.select(from_json(col("value").cast("string"), nba_schema).alias("data")) \
            .select("data.*").withColumn("processed_time", current_timestamp())


    def snowflake_write(batch_df, batch_id):
        batch_df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(sfOptions) \
            .option("dbtable", "LIVE_DATA") \
            .mode("append") \
            .save()
       
    # Write stream every 30s window -- can add more sophisticated window/watermark
    sf_query = processed_df.writeStream.foreachBatch(snowflake_write).trigger(processingTime="30 seconds").option("ChechpointLocation", ckpt_path).start()

    return sf_query
    
