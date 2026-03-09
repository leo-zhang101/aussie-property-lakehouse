import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

SILVER_PATH = BASE_DIR / "data/silver/building_approvals_silver_batch.csv"
GOLD_PATH = BASE_DIR / "data/gold/building_approvals_gold_batch.csv"

GOLD_PATH.parent.mkdir(parents=True, exist_ok=True)


def build_gold_table_batch():

    print("Building batch gold analytics table")

    df = pd.read_csv(SILVER_PATH)

    # 拆分 description
    desc_parts = df["data_item_description"].str.split(";", expand=True)

    if desc_parts.shape[1] >= 4:

        df["metric"] = desc_parts[0].str.strip()

        df["state"] = desc_parts[1].str.strip()

        df["dwelling_type"] = desc_parts[2].str.strip()

        df["sector"] = desc_parts[3].str.strip()

    else:

        raise ValueError("Unexpected ABS description format")

    # 提取年份
    df["year"] = pd.to_datetime(df["series_start"], errors="coerce").dt.year

    gold = df[
        [
            "metric",
            "state",
            "dwelling_type",
            "sector",
            "year",
            "source_file"
        ]
    ]

    gold = gold.dropna()

    gold = gold.drop_duplicates()

    gold.to_csv(GOLD_PATH, index=False)

    print("Gold batch dataset created")

    print(f"Rows: {len(gold)}")

    print(gold.head())


if __name__ == "__main__":

    build_gold_table_batch()
