import requests
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"

RAW_DIR.mkdir(parents=True, exist_ok=True)


def fetch_population_data():

    print("Fetching population data...")

    url = "https://raw.githubusercontent.com/owid/datasets/master/datasets/Population%20density%20by%20country/Population%20density%20by%20country.csv"

    df = pd.read_csv(url)

    df = df[df["Entity"].isin(["Australia"])]

    df = df.rename(columns={
        "Entity": "country",
        "Population density": "population_density"
    })

    df.to_csv(RAW_DIR / "population_density_real.csv", index=False)

    print("Population dataset saved.")


if __name__ == "__main__":
    fetch_population_data()
