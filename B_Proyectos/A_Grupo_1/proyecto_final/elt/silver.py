import pandas as pd
from pathlib import Path


def transform_products_to_silver(bronze_path, silver_path, **kwargs):
    """
    Limpia productos (rating aplanado) y guarda Parquet en silver_path.
    Inputs: bronze_path (str o Path), silver_path (str o Path).
    Output: str con la ruta del parquet generado.
    """
    bronze_path = Path(bronze_path)
    if not bronze_path.exists():
        raise FileNotFoundError(f"No se encontro el archivo en {bronze_path}")

    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(bronze_path)

    if "rating" in df.columns:
        rating_df = df["rating"].apply(pd.Series)
        df = pd.concat([df.drop(columns=["rating"]), rating_df], axis=1)

    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    df.to_parquet(silver_path, index=False)
    return str(silver_path)


def transform_carts_to_silver(bronze_path, silver_path, **kwargs):
    """
    Explota items de carts y guarda Parquet en silver_path.
    Inputs: bronze_path (str o Path), silver_path (str o Path).
    Output: str con la ruta del parquet generado.
    """
    bronze_path = Path(bronze_path)
    if not bronze_path.exists():
        raise FileNotFoundError(f"No se encontro el archivo en {bronze_path}")

    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(bronze_path)

    df = df.explode("products").reset_index(drop=True)
    prod_df = df["products"].apply(pd.Series)
    df = pd.concat([df.drop(columns=["products"]), prod_df], axis=1)

    df = df.rename(columns={
        "id": "cart_id",
        "userId": "user_id",
        "productId": "product_id",
    })
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df.to_parquet(silver_path, index=False)
    return str(silver_path)


def transform_users_to_silver(bronze_path, silver_path, **kwargs):
    """
    Aplana name/address de users y guarda Parquet en silver_path.
    Inputs: bronze_path (str o Path), silver_path (str o Path).
    Output: str con la ruta del parquet generado.
    """
    bronze_path = Path(bronze_path)
    if not bronze_path.exists():
        raise FileNotFoundError(f"No se encontro el archivo en {bronze_path}")

    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(bronze_path)

    name_df = df["name"].apply(pd.Series)
    addr_df = df["address"].apply(pd.Series)
    geo_df = addr_df["geolocation"].apply(pd.Series).rename(columns={"lat": "geo_lat", "long": "geo_long"})
    addr_df = addr_df.drop(columns=["geolocation"])

    df = pd.concat([df.drop(columns=["name", "address"]), name_df, addr_df, geo_df], axis=1)

    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    df.to_parquet(silver_path, index=False)
    return str(silver_path)
