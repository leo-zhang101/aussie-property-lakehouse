import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

BRONZE_PATH = BASE_DIR / "data/bronze/building_approvals_bronze_batch.csv"
SILVER_PATH = BASE_DIR / "data/silver/building_approvals_silver_batch.csv"

SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)


def clean_abs_building_data_batch():

    print("Cleaning batch ABS dataset")

    df = pd.read_csv(BRONZE_PATH)

    # 删除空行
    df = df.dropna(how="all")

    # 删除空列
    df = df.dropna(axis=1, how="all")

    # 标准化列名
    df.columns = [str(c).strip().replace(" ", "_").lower() for c in df.columns]

    # 保留关键字段
    keep_cols = [
        "data_item_description",
        "series_type",
        "series_id",
        "series_start",
        "series_end",
        "unit",
        "data_type",
        "freq.",
        "collection_month",
        "source_file"
    ]

    existing_cols = [c for c in keep_cols if c in df.columns]

    df = df[existing_cols]

    # 删除重复
    df = df.drop_duplicates()

    df.to_csv(SILVER_PATH, index=False)

    print("Silver batch dataset created")
    print(f"Rows: {len(df)}")

    print(df.head())


if __name__ == "__main__":
    clean_abs_building_data_batch()
