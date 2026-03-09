import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

GOLD_PATH = BASE_DIR / "data/gold/building_approvals_gold_batch.csv"


def run_quality_checks():

    print("Running data quality checks on batch gold dataset")

    df = pd.read_csv(GOLD_PATH)

    # check nulls
    if df["state"].isna().any():
        raise ValueError("Null values found in state column")

    if df["dwelling_type"].isna().any():
        raise ValueError("Null values found in dwelling_type column")

    # check duplicates
    duplicate_rows = df.duplicated().sum()

    print(f"Duplicate rows found: {duplicate_rows}")

    if duplicate_rows > 0:
        raise ValueError("Duplicate rows detected")

    print("Data quality checks passed successfully")


if __name__ == "__main__":

    run_quality_checks()
