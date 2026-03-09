import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
UNZIP_DIR = RAW_DIR / "abs_unzipped"
BRONZE_DIR = BASE_DIR / "data" / "bronze"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def fetch_abs_building_data():
    print("Loading ABS building approvals dataset")

    excel_files = list(UNZIP_DIR.glob("*.xlsx"))

    if not excel_files:
        raise FileNotFoundError("No .xlsx file found in data/raw/abs_unzipped")

    file_path = excel_files[0]
    print(f"Using file: {file_path.name}")

    df = pd.read_excel(file_path, skiprows=9)
    df = df.dropna(how="all")

    df.to_csv(
        BRONZE_DIR / "building_approvals_bronze.csv",
        index=False
    )

    print("Building approvals dataset processed into bronze layer")


if __name__ == "__main__":
    fetch_abs_building_data()
