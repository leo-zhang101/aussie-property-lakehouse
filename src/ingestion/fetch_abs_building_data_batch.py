import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data/raw/abs_unzipped"
BRONZE_DIR = BASE_DIR / "data/bronze"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def fetch_all_abs_files():

    print("Starting batch ingestion of ABS building approvals files")

    excel_files = sorted(RAW_DIR.glob("*.xlsx"))

    if not excel_files:
        raise FileNotFoundError("No Excel files found in abs_unzipped folder")

    all_data = []

    for file_path in excel_files:

        print(f"Reading file: {file_path.name}")

        try:
            df = pd.read_excel(file_path, skiprows=9)

            df = df.dropna(how="all")
            df = df.dropna(axis=1, how="all")

            df["source_file"] = file_path.name

            all_data.append(df)

        except Exception as e:
            print(f"Skipping file {file_path.name} بسبب error: {e}")

    if not all_data:
        raise ValueError("No ABS files were processed")

    combined_df = pd.concat(all_data, ignore_index=True)

    output_path = BRONZE_DIR / "building_approvals_bronze_batch.csv"

    combined_df.to_csv(output_path, index=False)

    print("Batch bronze dataset created")

    print(f"Total rows: {len(combined_df)}")
    print(f"Total files processed: {len(all_data)}")


if __name__ == "__main__":
    fetch_all_abs_files()
