import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

raw = BASE_DIR / "data/raw"
bronze = BASE_DIR / "data/bronze"

def run():
    property_df = pd.read_csv(raw / "property_prices.csv")
    density_df = pd.read_csv(raw / "population_density.csv")

    property_df.to_csv(bronze / "property_prices_bronze.csv", index=False)
    density_df.to_csv(bronze / "population_density_bronze.csv", index=False)

    print("Bronze layer created")

if __name__ == "__main__":
    run()
