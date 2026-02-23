import pandas as pd
from pathlib import Path


def build_dim_users(silver_users_path, dim_cities_path, output_path, **kwargs):
    """
    Crea la dimension de usuarios y la relaciona con ciudades.
    Inputs: silver_users_path (str o Path), dim_cities_path (str o Path), output_path (str o Path).
    Output: None (escribe parquet en disco).
    """
    df = pd.read_parquet(silver_users_path)
    dim_city = pd.read_parquet(dim_cities_path)

    dim_users = df[
        ["id", "email", "username", "phone", "firstname", "lastname", "street", "number", "city", "zipcode", "geo_lat", "geo_long"]
    ].copy()

    dim_users = dim_users.merge(
        dim_city,
        left_on=["city", "zipcode", "geo_lat", "geo_long"],
        right_on=["city_name", "zip_code", "geo_lat", "geo_long"],
        how="left"
    )

    dim_users = dim_users.rename(columns={
        "id": "user_key",
        "firstname": "first_name",
        "lastname": "last_name"
    })

    dim_users = dim_users.drop(columns=["city", "zipcode", "city_name", "zip_code"])

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    dim_users.to_parquet(output_path, index=False)
