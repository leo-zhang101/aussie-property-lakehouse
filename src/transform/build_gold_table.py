from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
GOLD_DIR = BASE_DIR / "data" / "gold"

engine = create_engine(
    "postgresql://de_user:de_pass@localhost:5432/property_dw"
)

def main():
    property_df = pd.read_csv(RAW_DIR / "property_prices.csv")
    density_df = pd.read_csv(RAW_DIR / "population_density.csv")

    merged = property_df.merge(
        density_df,
        on=["city", "suburb", "month"],
        how="inner"
    )

    merged["price_per_density"] = (
        merged["median_price"] / merged["population_density"]
    ).round(2)

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_csv(
        GOLD_DIR / "suburb_property_density_analysis.csv",
        index=False
    )

    merged.to_sql(
        "suburb_property_analysis",
        engine,
        if_exists="replace",
        index=False
    )

    print("Data written to PostgreSQL")
    print(merged)

if __name__ == "__main__":
    main()
