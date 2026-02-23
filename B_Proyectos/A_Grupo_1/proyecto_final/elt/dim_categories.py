import pandas as pd
from pathlib import Path

def build_dim_categories(silver_path, output_path, **kwargs):
    """
    Crea la dimension de categorias desde silver y la guarda en output_path.
    Inputs: silver_path (str o Path), output_path (str o Path).
    Output: None (escribe parquet en disco).
    """
    df = pd.read_parquet(silver_path)
    
    # Extraer categorías únicas
    categories = sorted(df['category'].dropna().unique())
    dim_cat = pd.DataFrame({
        'category_key': range(1, len(categories) + 1),
        'category_name': categories
    })
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    dim_cat.to_parquet(output_path, index=False)
