from pathlib import Path
import pandas as pd

SILVER_ROOT = Path("data_lake/silver")
OUT_PATH = Path("data_lake/gold/dim_location.parquet")

def build_dim_location(silver_root: Path = SILVER_ROOT, out_path: Path = OUT_PATH) -> str:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(silver_root.glob("earthquakes_silver_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No hay silver parquets en {silver_root}")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    for c in ["place", "latitude", "longitude"]:
        if c not in df.columns:
            raise ValueError(f"Silver debe contener {c}")

    dim = df[["place", "latitude", "longitude"]].copy()
    dim["place"] = dim["place"].fillna("unknown")

    # redondeo para estabilidad
    dim["latitude_r"] = pd.to_numeric(dim["latitude"], errors="coerce").round(4)
    dim["longitude_r"] = pd.to_numeric(dim["longitude"], errors="coerce").round(4)

    dim = dim.dropna(subset=["latitude_r", "longitude_r"]).drop_duplicates()
    dim = dim.sort_values(["place", "latitude_r", "longitude_r"]).reset_index(drop=True)

    dim["location_key"] = dim.index + 1

    dim_out = dim[["location_key", "place", "latitude_r", "longitude_r"]]
    dim_out.to_parquet(out_path, index=False)
    return str(out_path)

if __name__ == "__main__":
    print(build_dim_location())