import pandas as pd
from pathlib import Path

def build_dim_products(silver_path, dim_categories_path, output_path, **kwargs):
    """
    Crea la dimension de productos y la relaciona con categorias.
    Inputs: silver_path (str o Path), dim_categories_path (str o Path), output_path (str o Path).
    Output: None (escribe parquet en disco).
    """
    df = pd.read_parquet(silver_path)
    dim_cat = pd.read_parquet(dim_categories_path)

    dim_prod = df[['id', 'title', 'description', 'image', 'category', 'price']].copy()
    dim_prod = dim_prod.rename(columns={'id': 'product_key'})

    dim_prod = dim_prod.merge(
        dim_cat,
        left_on='category',
        right_on='category_name',
        how='left'
    )
    dim_prod = dim_prod.drop(columns=['category', 'category_name'])
    dim_prod = dim_prod[['product_key', 'category_key', 'title', 'description', 'image', 'price']]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    dim_prod.to_parquet(output_path, index=False)
