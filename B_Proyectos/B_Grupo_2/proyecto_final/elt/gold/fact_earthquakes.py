from pathlib import Path
import pandas as pd

SILVER_ROOT = Path("data_lake/silver")

DIM_DATE = Path("data_lake/gold/dim_date.parquet")
DIM_LOCATION = Path("data_lake/gold/dim_location.parquet")
DIM_EVENT_TYPE = Path("data_lake/gold/dim_event_type.parquet")
DIM_STATUS = Path("data_lake/gold/dim_status.parquet")

OUT_PATH = Path("data_lake/gold/fact_earthquakes.parquet")


def build_fact_earthquakes(
    silver_root: Path = SILVER_ROOT,
    out_path: Path = OUT_PATH,
) -> str:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) Leer Silver
    files = sorted(silver_root.glob("earthquakes_silver_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No hay silver parquets en {silver_root}")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    required = ["event_id", "event_time", "place", "latitude", "longitude"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Silver debe contener {c}")

    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df = df[df["event_time"].notna()].copy()

    # 2) Leer dims
    dim_date = pd.read_parquet(DIM_DATE)
    dim_loc = pd.read_parquet(DIM_LOCATION)
    dim_type = pd.read_parquet(DIM_EVENT_TYPE)
    dim_status = pd.read_parquet(DIM_STATUS)

    # 3) Preparar keys para joins
    df["date_key"] = df["event_time"].dt.strftime("%Y%m%d").astype(int)

    df["place"] = df["place"].fillna("unknown")
    df["latitude_r"] = pd.to_numeric(df["latitude"], errors="coerce").round(4)
    df["longitude_r"] = pd.to_numeric(df["longitude"], errors="coerce").round(4)

    df["event_type"] = df.get("event_type", "unknown").fillna("unknown")
    df["status"] = df.get("status", "unknown").fillna("unknown")

    # 4) Joins (left) para obtener surrogate keys
    df = df.merge(dim_date[["date_key"]], on="date_key", how="left")

    df = df.merge(
        dim_loc[["location_key", "place", "latitude_r", "longitude_r"]],
        on=["place", "latitude_r", "longitude_r"],
        how="left",
    )

    df = df.merge(
        dim_type[["event_type_key", "event_type"]],
        on="event_type",
        how="left",
    )

    df = df.merge(
        dim_status[["status_key", "status"]],
        on="status",
        how="left",
    )

    # 5) Construir FACT (una fila por evento)
    fact_cols = [
        "event_id",
        "date_key",
        "location_key",
        "event_type_key",
        "status_key",
        "event_time",
        "mag",
        "depth_km",
        "sig",
        "tsunami",
    ]

    for c in ["mag", "depth_km", "sig", "tsunami"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    fact = df[fact_cols].copy()

    # Asegurar dedupe final por event_id
    fact = fact.drop_duplicates(subset=["event_id"], keep="first")

    fact.to_parquet(out_path, index=False)
    return str(out_path)


if __name__ == "__main__":
    print(build_fact_earthquakes())