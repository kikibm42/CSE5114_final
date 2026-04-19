"""
One-time repair: normalizes PLUS_MINUS to float64 across all team_stats and
player_stats parquet files so Spark reads them with a consistent schema.
"""
import glob
import os
import pandas as pd

FLOAT_COLS = [
    "PLUS_MINUS",
    # V3 writes these as int64 but V2 files have them as float64 — normalize to float64
    "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
    "OREB", "DREB", "REB", "AST", "STL", "BLK", "TO", "PF", "PTS",
]
INT_COLS   = ["PLAYER_ID", "TEAM_ID"]  # V3 API may return these as float64

patterns = [
    "./data/nba/raw/team_stats/*/*.parquet",
    "./data/nba/raw/player_stats/*/*.parquet",
]

fixed = 0
for pattern in patterns:
    for path in sorted(glob.glob(pattern)):
        df = pd.read_parquet(path)
        changed = False
        for col in FLOAT_COLS:
            if col in df.columns and df[col].dtype != "float64":
                df[col] = df[col].astype("float64")
                changed = True
        for col in INT_COLS:
            if col in df.columns and df[col].dtype == "float64":
                df[col] = df[col].astype("Int64")
                changed = True
        if changed:
            df.to_parquet(path, index=False)
            print(f"Fixed: {path}")
            fixed += 1

print(f"\nDone — {fixed} file(s) updated.")
