from pathlib import Path
import pandas as pd

SILVER_ROOT = Path("data_lake/silver")
OUT_PATH = Path("data_lake/gold/dim_event_type.parquet")

def build_dim_event_type(silver_root: Path = SILVER_ROOT, out_path: Path = OUT_PATH) -> str:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(silver_root.glob("earthquakes_silver_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No hay silver parquets en {silver_root}")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    if "event_type" not in df.columns:
        df["event_type"] = "unknown"

    dim = (
        df[["event_type"]]
        .fillna("unknown")
        .drop_duplicates()
        .sort_values("event_type")
        .reset_index(drop=True)
    )
    dim["event_type_key"] = dim.index + 1

    dim.to_parquet(out_path, index=False)
    return str(out_path)

if __name__ == "__main__":
    print(build_dim_event_type())