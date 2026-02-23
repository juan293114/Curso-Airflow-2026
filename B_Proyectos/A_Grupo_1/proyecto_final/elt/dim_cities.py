import pandas as pd
from pathlib import Path


def build_dim_cities(silver_users_path, output_path, **kwargs):
    """
    Crea la dimension de ciudades desde users silver y la guarda en output_path.
    Inputs: silver_users_path (str o Path), output_path (str o Path).
    Output: None (escribe parquet en disco).
    """
    df = pd.read_parquet(silver_users_path)

    dim_city = df[["city", "zipcode", "geo_lat", "geo_long"]].drop_duplicates().copy()
    dim_city = dim_city.rename(columns={"city": "city_name", "zipcode": "zip_code"})
    dim_city = dim_city.sort_values(["city_name", "zip_code"]).reset_index(drop=True)
    dim_city.insert(0, "city_key", range(1, len(dim_city) + 1))

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    dim_city.to_parquet(output_path, index=False)
