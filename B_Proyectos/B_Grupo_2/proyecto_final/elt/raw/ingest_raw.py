from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests


# ===============================
# CONFIGURACIÓN
# ===============================

URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"


RAW_ROOT = Path("/opt/airflow/data_lake/raw")


# ===============================
# EXTRACT
# ===============================

def extract_json(url: str) -> dict:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


# ===============================
# LOAD RAW (JSON AS-IS)
# ===============================

def save_raw_json(data: dict, base_path: Path) -> str:
    ingestion_time = datetime.now(timezone.utc)
    ingestion_date = ingestion_time.date()

    # Crear partición por fecha de ingesta
    partition_path = base_path 
    partition_path.mkdir(parents=True, exist_ok=True)

    file_name = f"earthquakes_{ingestion_time.strftime('%Y%m%d_%H%M%S')}.json"
    full_path = partition_path / file_name

    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    logging.info(f"RAW guardado en: {full_path}")

    return str(full_path)


# ===============================
# FUNCIÓN PARA AIRFLOW
# ===============================

def run_raw() -> str:
    """
    Función ejecutable desde Airflow
    """
    data = extract_json(URL)
    return save_raw_json(data, RAW_ROOT)


# ===============================
# MAIN (para ejecución manual)
# ===============================

def main():
    path = run_raw()
    print(f"Archivo creado en: {path}")


if __name__ == "__main__":
    main()