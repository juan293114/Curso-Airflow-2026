import pandas as pd
from pathlib import Path


def build_fact_carts(silver_carts_path, output_path, **kwargs):
    """
    Crea la tabla de hechos desde carts silver y la guarda en output_path.
    Inputs: silver_carts_path (str o Path), output_path (str o Path).
    Output: None (escribe parquet en disco).
    """
    df = pd.read_parquet(silver_carts_path)

    fact_df = df[["cart_id", "user_id", "product_id", "quantity", "date"]].copy()
    fact_df = fact_df.rename(columns={
        "user_id": "user_key",
        "product_id": "product_key",
        "date": "cart_date"
    })

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fact_df.to_parquet(output_path, index=False)
