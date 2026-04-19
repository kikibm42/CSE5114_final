"""
Reads raw NBA Parquet files produced by extractor.py and loads them
into the Snowflake star schema defined in snowflake/schema.sql.

Input:
  <data-dir>/games/**/*.parquet
  <data-dir>/player_stats/**/*.parquet
  <data-dir>/team_stats/**/*.parquet

Output (MINNOW_DB.PUBLIC):
  dim_team, dim_player, dim_date
  fact_game, fact_game_team_stats, fact_game_player_stats

Usage:
  python transform.py
  python transform.py --data-dir /path/to/data/nba/raw --pem-key /path/to/rsa_key.p8
"""

import argparse
import logging
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ──────────────────────────────────────────────
# SNOWFLAKE CONFIG
# ──────────────────────────────────────────────
SF_URL = "sfedu02-unb02139.snowflakecomputing.com"
SF_USER = "MINNOW"
SF_DATABASE = "MINNOW_DB"
SF_SCHEMA = "PUBLIC"
SF_WAREHOUSE = "MINNOW_WH"
DEFAULT_PEM = "./rsa_key.p8"

# ──────────────────────────────────────────────
# SEASON_ID TYPE → season_type label
# Leading digit: 1=Pre-Season, 2=Regular Season, 4=Playoffs
# ──────────────────────────────────────────────
SEASON_TYPE_MAP = {"1": "Pre-Season", "2": "Regular Season", "4": "Playoffs"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Transform raw NBA Parquet files into Snowflake"
    )
    p.add_argument(
        "--data-dir",
        default="./data/nba/raw",
        help="Root directory of raw Parquet files. Default: ./data/nba/raw",
    )
    p.add_argument(
        "--pem-key",
        default=DEFAULT_PEM,
        help="Path to PEM private key for Snowflake auth.",
    )
    p.add_argument(
        "--local",
        action="store_true",
        help="Force local[*] Spark mode, skipping LinuxLab detection.",
    )
    return p.parse_args()


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


def create_spark_session(force_local: bool = False) -> SparkSession:
    if force_local:
        master_url = "local[*]"
    else:
        try:
            node = os.environ["SLURMD_NODENAME"]
            master_port = os.environ["SPARK_MASTER_PORT"]
            master_url = f"spark://{node}.engr.wustl.edu:{master_port}"
        except KeyError:
            print("LinuxLab env vars not found — falling back to local[*]")
            master_url = "local[*]"

    builder = (
        SparkSession.builder.appName("NBA-historical-transform")
        .master(master_url)
        .config("spark.driver.memory", "4g")
    )

    return builder.config(
        "spark.jars.packages",
        "net.snowflake:snowflake-jdbc:3.13.30,"
        "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4",
    ).getOrCreate()


def snowflake_read(spark: SparkSession, table: str, sf_options: dict) -> DataFrame:
    return (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", table)
        .load()
    )


def dedup_and_write(
    spark: SparkSession, df: DataFrame, table: str, key_col: str, sf_options: dict
):
    """Anti-join against existing keys in Snowflake before appending."""
    try:
        existing_keys = snowflake_read(spark, table, sf_options).select(key_col)
        new_rows = df.join(existing_keys, on=key_col, how="left_anti")
    except Exception as e:
        log.warning(
            f"Could not read existing keys from {table} ({e}) — writing all rows"
        )
        new_rows = df

    count = new_rows.count()
    print(f"Writing {count} new rows → {SF_DATABASE}.{SF_SCHEMA}.{table}")
    if count > 0:
        (
            new_rows.write.format("net.snowflake.spark.snowflake")
            .options(**sf_options)
            .option("dbtable", table)
            .mode("append")
            .save()
        )


# ──────────────────────────────────────────────
# DIMENSION BUILDERS
# ──────────────────────────────────────────────


def build_dim_team(games_df: DataFrame) -> DataFrame:
    return (
        games_df.select("TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME")
        .distinct()
        .select(
            F.col("TEAM_ID").alias("team_id"),
            F.col("TEAM_ABBREVIATION").alias("abbreviation"),
            F.col("TEAM_NAME").alias("name"),
            F.lit(None).cast("string").alias("city"),
            F.lit(None).cast("string").alias("conference"),
            F.lit(None).cast("string").alias("division"),
        )
    )


def build_dim_player(player_df: DataFrame) -> DataFrame:
    return (
        player_df.filter(F.col("PLAYER_ID").isNotNull() & F.col("PLAYER_NAME").isNotNull())
        .select("PLAYER_ID", "PLAYER_NAME", "START_POSITION")
        .distinct()
        .select(
            F.col("PLAYER_ID").alias("player_id"),
            F.col("PLAYER_NAME").alias("name"),
            F.col("START_POSITION").alias("position"),
            F.lit(None).cast("string").alias("height"),
            F.lit(None).cast("int").alias("weight"),
        )
    )


def build_dim_date(games_df: DataFrame) -> DataFrame:
    # SEASON_ID e.g. "22019": first char = type, remaining chars = start year
    season_type_map = F.create_map(
        *[
            item
            for pair in SEASON_TYPE_MAP.items()
            for item in (F.lit(pair[0]), F.lit(pair[1]))
        ]
    )
    return (
        games_df.select("GAME_DATE", "SEASON_ID")
        .distinct()
        .select(
            F.to_date(F.col("GAME_DATE"), "yyyy-MM-dd").alias("date"),
            F.col("SEASON_ID").cast("string").alias("_sid"),
        )
        .withColumn("_type_char", F.col("_sid").substr(1, 1))
        .withColumn("_start_year", F.col("_sid").substr(2, 4).cast("int"))
        .withColumn(
            "season",
            F.concat(
                F.col("_start_year").cast("string"),
                F.lit("-"),
                F.lpad((F.col("_start_year") + 1 - 2000).cast("string"), 2, "0"),
            ),
        )
        .withColumn("season_type", season_type_map[F.col("_type_char")])
        .select("date", "season", "season_type")
        .distinct()
    )


# ──────────────────────────────────────────────
# FACT BUILDERS
# ──────────────────────────────────────────────


def build_fact_game(games_df: DataFrame) -> DataFrame:
    # LeagueGameFinder returns one row per team per game.
    # MATCHUP "X vs. Y" → X is home; "X @ Y" → X is away.
    home_df = games_df.filter(
        F.col("MATCHUP").contains(" vs. ") & F.col("GAME_ID").isNotNull() & F.col("PTS").isNotNull()
    ).select(
        F.col("GAME_ID").alias("game_id"),
        F.to_date(F.col("GAME_DATE"), "yyyy-MM-dd").alias("date"),
        F.col("TEAM_ID").alias("home_team_id"),
        F.col("PTS").alias("home_score"),
    )
    away_df = games_df.filter(
        F.col("MATCHUP").contains(" @ ") & F.col("GAME_ID").isNotNull() & F.col("PTS").isNotNull()
    ).select(
        F.col("GAME_ID").alias("game_id"),
        F.col("TEAM_ID").alias("away_team_id"),
        F.col("PTS").alias("away_score"),
    )
    return home_df.join(away_df, on="game_id", how="inner").select(
        "game_id", "date", "home_team_id", "away_team_id", "home_score", "away_score"
    )


def build_fact_game_team_stats(team_df: DataFrame) -> DataFrame:
    return team_df.filter(F.col("GAME_ID").isNotNull() & F.col("TEAM_ID").isNotNull()).select(
        F.col("GAME_ID").alias("game_id"),
        F.col("TEAM_ID").alias("team_id"),
        F.col("FGM").alias("fgm"),
        F.col("FGA").alias("fga"),
        F.col("FG_PCT").alias("fg_pct"),
        F.col("FG3M").alias("fg3m"),
        F.col("FG3A").alias("fg3a"),
        F.col("FG3_PCT").alias("fg3_pct"),
        F.col("FTM").alias("ftm"),
        F.col("FTA").alias("fta"),
        F.col("FT_PCT").alias("ft_pct"),
        F.col("OREB").alias("oreb"),
        F.col("DREB").alias("dreb"),
        F.col("REB").alias("reb"),
        F.col("AST").alias("ast"),
        F.col("STL").alias("stl"),
        F.col("BLK").alias("blk"),
        F.col("TO").alias("tov"),  # BoxScore uses TO, not TOV
        F.col("PF").alias("pf"),
        F.col("PTS").alias("pts"),
        F.col("PLUS_MINUS").cast("int").alias("plus_minus"),
    )


def build_fact_game_player_stats(player_df: DataFrame) -> DataFrame:
    return player_df.filter(
        F.col("PLAYER_ID").isNotNull() & F.col("GAME_ID").isNotNull() & F.col("TEAM_ID").isNotNull()
    ).select(
        F.col("GAME_ID").alias("game_id"),
        F.col("PLAYER_ID").alias("player_id"),
        F.col("TEAM_ID").alias("team_id"),
        F.col("MIN").alias("min"),  # kept as VARCHAR "MM:SS"
        F.col("PTS").alias("pts"),
        F.col("FGM").alias("fgm"),
        F.col("FGA").alias("fga"),
        F.col("FG_PCT").alias("fg_pct"),
        F.col("FG3M").alias("fg3m"),
        F.col("FG3A").alias("fg3a"),
        F.col("FG3_PCT").alias("fg3_pct"),
        F.col("FTM").alias("ftm"),
        F.col("FTA").alias("fta"),
        F.col("FT_PCT").alias("ft_pct"),
        F.col("OREB").alias("oreb"),
        F.col("DREB").alias("dreb"),
        F.col("REB").alias("reb"),
        F.col("AST").alias("ast"),
        F.col("STL").alias("stl"),
        F.col("BLK").alias("blk"),
        F.col("TO").alias("tov"),
        F.col("PF").alias("pf"),
        F.col("PLUS_MINUS").cast("int").alias("plus_minus"),
    )


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────


def main():
    args = parse_args()

    pem_key = get_private_key_string(args.pem_key)
    sf_options = {
        "sfURL": SF_URL,
        "sfUser": SF_USER,
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE,
        "pem_private_key": pem_key,
    }

    spark = create_spark_session(force_local=args.local)

    # Disable vectorized Parquet reader — it strictly enforces schema types across
    # partitions. The non-vectorized reader handles mixed types (e.g. PLUS_MINUS
    # stored as DOUBLE in some chunks and BIGINT in others) without erroring.
    # Also avoids the COLUMN_ALREADY_EXISTS clash from mergeSchema on case-variant
    # column names (e.g. COMMENT vs comment across player_stats chunks).
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    # ── Read raw Parquets (wildcards cover all date partitions) ──
    data_dir = args.data_dir
    games_df  = spark.read.parquet(f"{data_dir}/games/*/*.parquet")
    player_df = spark.read.parquet(f"{data_dir}/player_stats/*/*.parquet")
    team_df   = spark.read.parquet(f"{data_dir}/team_stats/*/*.parquet")

    # ── Dimensions first ──
    dedup_and_write(spark, build_dim_team(games_df), "dim_team", "team_id", sf_options)
    dedup_and_write(
        spark, build_dim_player(player_df), "dim_player", "player_id", sf_options
    )
    dedup_and_write(spark, build_dim_date(games_df), "dim_date", "date", sf_options)

    # ── Facts ──
    dedup_and_write(
        spark, build_fact_game(games_df), "fact_game", "game_id", sf_options
    )
    dedup_and_write(
        spark,
        build_fact_game_team_stats(team_df),
        "fact_game_team_stats",
        "game_id",
        sf_options,
    )
    dedup_and_write(
        spark,
        build_fact_game_player_stats(player_df),
        "fact_game_player_stats",
        "game_id",
        sf_options,
    )

    print("Transform complete.")
    spark.stop()


if __name__ == "__main__":
    main()
