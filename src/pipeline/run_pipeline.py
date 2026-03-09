from src.ingestion.unzip_abs_data import unzip_abs_zip
from src.ingestion.fetch_abs_building_data import fetch_abs_building_data
from src.etl.bronze_layer import run as run_bronze
from src.etl.silver_layer import run as run_silver
from src.transform.build_gold_table import main as run_gold


def run_pipeline():
    print("Starting full lakehouse pipeline...")

    unzip_abs_zip()
    fetch_abs_building_data()
    run_bronze()
    run_silver()
    run_gold()

    print("Pipeline finished successfully.")


if __name__ == "__main__":
    run_pipeline()
