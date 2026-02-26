from __future__ import annotations

from pathlib import Path
import os
import pandas as pd



def _is_docker() -> bool:
    if Path("/.dockerenv").exists():
        return True
    if os.getenv("AIRFLOW_HOME") == "/opt/airflow":
        return True
    return False


def _project_root() -> Path:
    if _is_docker():
        return Path("/opt/airflow")
    return Path(__file__).resolve().parents[2]


ROOT = _project_root()

BRONZE_DIR = ROOT / "data_lake" / "bronze"
SILVER_DIR = ROOT / "data_lake" / "silver"


# ===============================
# HELPERS
# ===============================
def _clean_column_name(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
    )


# ===============================
# TRANSFORM BRONZE -> SILVER
# ===============================
def transform_bronze_to_silver_earthquakes(
    bronze_path: Path | str,
    silver_path: Path | str,
) -> str:
    bronze_path = Path(bronze_path)
    silver_path = Path(silver_path)
    silver_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(bronze_path)

    # Normalizar columnas
    df.columns = [_clean_column_name(c) for c in df.columns]

    # Renombrar columnas clave
    rename_map = {
        "id": "event_id",
        "event_time": "event_time",
        "properties_mag": "mag",
        "properties_place": "place",
        "properties_tsunami": "tsunami",
        "properties_sig": "sig",
        "properties_status": "status",
        "properties_type": "event_type",
        "properties_updated": "updated_ms",
        "latitude": "latitude",
        "longitude": "longitude",
        "depth_km": "depth_km",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Tipos
    if "event_time" in df.columns:
        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

    if "updated_ms" in df.columns:
        df["updated_ms"] = pd.to_numeric(df["updated_ms"], errors="coerce")

    for c in ["mag", "sig", "depth_km", "latitude", "longitude", "tsunami"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Calidad mínima
    if "event_time" in df.columns:
        df = df[df["event_time"].notna()]

    if "latitude" in df.columns:
        df = df[df["latitude"].between(-90, 90, inclusive="both")]

    if "longitude" in df.columns:
        df = df[df["longitude"].between(-180, 180, inclusive="both")]

    # Dedupe por event_id
    if "event_id" in df.columns:
        if "updated_ms" in df.columns and df["updated_ms"].notna().any():
            df = df.sort_values(["event_id", "updated_ms"], ascending=[True, False])
        else:
            df = df.sort_values(["event_id", "event_time"], ascending=[True, False])

        df = df.drop_duplicates(subset=["event_id"], keep="first")

    df.to_parquet(silver_path, index=False)
    return str(silver_path)


# ===============================
# RUNNER: latest bronze -> silver
# ===============================
def run_silver_latest() -> str:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    bronze_files = sorted(BRONZE_DIR.glob("earthquakes_bronze_*.parquet"))

    if not bronze_files:
        raise FileNotFoundError(
            f"No encontré parquets BRONZE en: {BRONZE_DIR}\n"
            f"Verifica que exista algo como:\n"
            f"  {BRONZE_DIR}/earthquakes_bronze_*.parquet\n"
            f"Primero ejecuta BRONZE."
        )

    latest_bronze = bronze_files[-1]

    run_ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    silver_path = SILVER_DIR / f"earthquakes_silver_{run_ts}.parquet"

    print(f"[SILVER] ROOT: {ROOT}")
    print(f"[SILVER] BRONZE: {latest_bronze}")
    print(f"[SILVER] OUT: {silver_path}")

    return transform_bronze_to_silver_earthquakes(
        bronze_path=latest_bronze,
        silver_path=silver_path,
    )


def main():
    out = run_silver_latest()
    print(f"[SILVER] OK -> {out}")


if __name__ == "__main__":
    main()