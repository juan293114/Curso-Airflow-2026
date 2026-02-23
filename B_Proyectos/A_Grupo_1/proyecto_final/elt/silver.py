import pandas as pd
from pathlib import Path

def transform_bronze_to_silver(bronze_path, silver_path, **kwargs):
    """
    Limpia y normaliza el parquet bronze y lo guarda en silver_path.
    Inputs: bronze_path (str o Path), silver_path (str o Path).
    Output: str con la ruta del parquet generado.
    """
    bronze_path = Path(bronze_path)
    if not bronze_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo en {bronze_path}")
    
    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_parquet(bronze_path)
    
    # Aplanar el diccionario "rating" (rate y count)
    if 'rating' in df.columns:
        rating_df = df['rating'].apply(pd.Series)
        df = pd.concat([df.drop(columns=['rating']), rating_df], axis=1)
    
    # Limpieza básica: nombres de columnas y tipos
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    
    # Guardar en Silver
    df.to_parquet(silver_path, index=False)
    
    return str(silver_path)
