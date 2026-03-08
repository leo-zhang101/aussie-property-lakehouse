from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parents[2]
GOLD_PATH = BASE_DIR / "data" / "gold" / "building_approvals_gold.csv"

engine = create_engine(
    "postgresql://de_user:de_pass@host.docker.internal:5432/property_dw"
)

def load_gold_to_postgres():
    print("Loading ABS gold dataset into PostgreSQL")

    df = pd.read_csv(GOLD_PATH)

    # convert year safely
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    # remove invalid rows
    df = df.dropna(subset=["year"])

    # convert to integer
    df["year"] = df["year"].astype(int)

    df.to_sql(
        "building_approvals_gold",
        engine,
        if_exists="replace",
        index=False
    )

    print("ABS gold dataset loaded into PostgreSQL")
    print(df.head())

if __name__ == "__main__":
    load_gold_to_postgres()
