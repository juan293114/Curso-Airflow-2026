import pandas as pd
from pathlib import Path

def transform_raw_to_bronze(raw_path, bronze_path, **kwargs):
    """
    Convierte el JSON raw a Parquet en bronze_path.
    Inputs: raw_path (str o Path), bronze_path (str o Path).
    Output: str con la ruta del parquet generado.
    """
    raw_path = Path(raw_path)
    if not raw_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo en {raw_path}")

    bronze_path = Path(bronze_path)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    # Leer JSON y persistir en Parquet
    df = pd.read_json(raw_path)
    df.to_parquet(bronze_path, index=False, engine='pyarrow')
    
    return str(bronze_path)
