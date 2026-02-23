import pandas as pd
from pathlib import Path
from datetime import datetime

def build_dim_time(output_path, **kwargs):
    """
    Genera dimension tiempo del ano actual y la guarda en output_path.
    Inputs: output_path (str o Path).
    Output: None (escribe parquet en disco).
    """
    # Generamos un rango de fechas para el año actual
    curr_year = datetime.now().year
    dates = pd.date_range(start=f"{curr_year}-01-01", end=f"{curr_year}-12-31", freq='D')
    
    dim_time = pd.DataFrame({
        'date_key': dates.strftime('%Y%m%d').astype(int),
        'full_date': dates,
        'day': dates.day,
        'month': dates.month,
        'year': dates.year,
        'quarter': dates.quarter
    })
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    dim_time.to_parquet(output_path, index=False)
