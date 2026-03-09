import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

SILVER_PATH = BASE_DIR / "data/silver/building_approvals_silver.csv"
GOLD_PATH = BASE_DIR / "data/gold/building_approvals_gold.csv"

GOLD_PATH.parent.mkdir(parents=True, exist_ok=True)


def build_gold_table():

    print("Building gold analytics table")

    df = pd.read_csv(SILVER_PATH)

    # extract state
    df["state"] = df["data_item_description"].str.split(";").str[1].str.strip()

    # extract dwelling type
    df["dwelling_type"] = df["data_item_description"].str.split(";").str[3].str.strip()

    # extract year
    df["year"] = pd.to_datetime(df["series_start"]).dt.year

    gold = df[[
        "state",
        "dwelling_type",
        "year"
    ]]

    # remove bad rows
    gold = gold.dropna()

    # remove duplicates
    gold = gold.drop_duplicates()

    gold.to_csv(GOLD_PATH, index=False)

    print("Gold analytics dataset created")
    print(gold.head())


if __name__ == "__main__":
    build_gold_table()
