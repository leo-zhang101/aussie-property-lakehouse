import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

bronze = BASE_DIR / "data/bronze"
silver = BASE_DIR / "data/silver"

def run():
    property_df = pd.read_csv(bronze / "property_prices_bronze.csv")
    density_df = pd.read_csv(bronze / "population_density_bronze.csv")

    merged = property_df.merge(
        density_df,
        on=["city","suburb","month"]
    )

    merged.to_csv(silver / "property_density_silver.csv", index=False)

    print("Silver layer created")

if __name__ == "__main__":
    run()
