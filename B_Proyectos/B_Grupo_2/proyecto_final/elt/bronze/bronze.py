import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


RAW_ROOT = Path("data_lake/raw")
BRONZE_ROOT = Path("data_lake/bronze")


def run_bronze():

    json_files = sorted(RAW_ROOT.glob("*.json"))

    if not json_files:
        raise FileNotFoundError("No se encontraron archivos RAW")

    for json_file in json_files:

        ingestion_date = json_file.parent.name.split("=")[-1]

        payload = json.loads(json_file.read_text(encoding="utf-8"))

        features = payload.get("features", [])
        if not features:
            continue

        df = pd.json_normalize(features)

        # Convertir timestamp
        df["event_time"] = pd.to_datetime(
            df["properties.time"], unit="ms", errors="coerce"
        )

        # Separar coordenadas
        coords_df = pd.DataFrame(
            df["geometry.coordinates"].tolist(),
            columns=["longitude", "latitude", "depth_km"]
        )
        df = pd.concat([df, coords_df], axis=1)

        df["ingestion_date"] = ingestion_date
        df["source_file"] = json_file.name

        # Crear carpeta bronze particionada
        output_dir = BRONZE_ROOT 
        output_dir.mkdir(parents=True, exist_ok=True)

        file_name = f"earthquakes_bronze_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
        output_path = output_dir / file_name

        df.to_parquet(output_path, index=False)

        print(f"Archivo BRONZE guardado en: {output_path}")


if __name__ == "__main__":
    run_bronze()