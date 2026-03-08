import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql://de_user:de_pass@localhost:5432/property_dw"
)

GOLD_PATH = "data/gold/building_approvals_gold_batch.csv"


def build_star_schema():
    print("Building star schema from gold batch dataset")

    df = pd.read_csv(GOLD_PATH)

    # clean year
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # -------------------------
    # load dimension tables
    # -------------------------

    dim_year = df[["year"]].drop_duplicates().sort_values("year")
    dim_year.to_sql("dim_year", engine, if_exists="append", index=False)

    dim_state = df[["state"]].drop_duplicates()
    dim_state = dim_state.rename(columns={"state": "state_name"})
    dim_state.to_sql("dim_state", engine, if_exists="append", index=False)

    dim_dwelling_type = df[["dwelling_type"]].drop_duplicates()
    dim_dwelling_type.to_sql("dim_dwelling_type", engine, if_exists="append", index=False)

    dim_sector = df[["sector"]].drop_duplicates()
    dim_sector = dim_sector.rename(columns={"sector": "sector_name"})
    dim_sector.to_sql("dim_sector", engine, if_exists="append", index=False)

    # -------------------------
    # read dimension ids back
    # -------------------------

    with engine.connect() as conn:
        year_map = pd.read_sql(text("SELECT year_id, year FROM dim_year"), conn)
        state_map = pd.read_sql(text("SELECT state_id, state_name FROM dim_state"), conn)
        dwelling_type_map = pd.read_sql(
            text("SELECT dwelling_type_id, dwelling_type FROM dim_dwelling_type"), conn
        )
        sector_map = pd.read_sql(
            text("SELECT sector_id, sector_name FROM dim_sector"), conn
        )

    # -------------------------
    # build fact table
    # -------------------------

    fact_df = df.merge(year_map, on="year", how="left")
    fact_df = fact_df.merge(state_map, left_on="state", right_on="state_name", how="left")
    fact_df = fact_df.merge(
        dwelling_type_map, on="dwelling_type", how="left"
    )
    fact_df = fact_df.merge(
        sector_map, left_on="sector", right_on="sector_name", how="left"
    )

    fact_df = fact_df[
        [
            "year_id",
            "state_id",
            "dwelling_type_id",
            "sector_id",
            "metric",
            "source_file",
        ]
    ].copy()

    fact_df.to_sql("fact_building_approvals", engine, if_exists="append", index=False)

    print("Star schema loaded successfully")
    print(f"Rows loaded into fact_building_approvals: {len(fact_df)}")


if __name__ == "__main__":
    build_star_schema()
