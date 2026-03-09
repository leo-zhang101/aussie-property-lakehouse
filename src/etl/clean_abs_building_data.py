import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

BRONZE_PATH = BASE_DIR / "data/bronze/building_approvals_bronze.csv"
SILVER_PATH = BASE_DIR / "data/silver/building_approvals_silver.csv"

SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)


def clean_abs_building_data():

    print("Cleaning ABS building approvals dataset")

    df = pd.read_csv(BRONZE_PATH)

    # Remove empty rows
    df = df.dropna(how="all")

    # Remove empty columns
    df = df.dropna(axis=1, how="all")

    # Standardize column names
    df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]

    # Keep important columns
    keep_cols = [
        "data_item_description",
        "series_id",
        "series_start",
        "series_end"
    ]

    df = df[[c for c in keep_cols if c in df.columns]]

    df.to_csv(SILVER_PATH, index=False)

    print("Silver dataset created")
    print(df.head())


if __name__ == "__main__":
    clean_abs_building_data()
