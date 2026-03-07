from src.etl.bronze_layer import run as run_bronze
from src.etl.silver_layer import run as run_silver
from src.transform.build_gold_table import main as run_gold


def run_pipeline():
    print("Starting full lakehouse pipeline...")
    run_bronze()
    run_silver()
    run_gold()
    print("Pipeline finished successfully.")


if __name__ == "__main__":
    run_pipeline()
