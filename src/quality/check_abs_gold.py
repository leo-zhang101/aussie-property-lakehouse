from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
GOLD_PATH = BASE_DIR / "data" / "gold" / "building_approvals_gold.csv"


def run_quality_checks():
    print("Running data quality checks on ABS gold dataset")

    df = pd.read_csv(GOLD_PATH)

    # null checks
    if df["state"].isnull().any():
        raise ValueError("Null values found in state column")

    if df["dwelling_type"].isnull().any():
        raise ValueError("Null values found in dwelling_type column")

    if df["year"].isnull().any():
        raise ValueError("Null values found in year column")

    # duplicate checks
    duplicate_count = df.duplicated().sum()
    print(f"Duplicate rows found: {duplicate_count}")

    # range checks
    if (df["year"] < 1900).any():
        raise ValueError("Invalid year values found")

    if (df["year"] > 2100).any():
        raise ValueError("Future year values found")

    print("Data quality checks passed successfully")


if __name__ == "__main__":
    run_quality_checks()
