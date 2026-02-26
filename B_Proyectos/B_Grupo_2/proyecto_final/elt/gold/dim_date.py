from pathlib import Path
import pandas as pd

SILVER_ROOT = Path("data_lake/silver")
OUT_PATH = Path("data_lake/gold/dim_date.parquet")

def build_dim_date(silver_root: Path = SILVER_ROOT, out_path: Path = OUT_PATH) -> str:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(silver_root.glob("earthquakes_silver_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No hay silver parquets en {silver_root}")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    if "event_time" not in df.columns:
        raise ValueError("Silver debe contener event_time")

    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df = df[df["event_time"].notna()]

    dates = pd.DataFrame({"date": df["event_time"].dt.date.astype(str)}).drop_duplicates()
    dates["date"] = pd.to_datetime(dates["date"])
    dates = dates.sort_values("date")

    dates["date_key"] = dates["date"].dt.strftime("%Y%m%d").astype(int)
    dates["year"] = dates["date"].dt.year
    dates["month"] = dates["date"].dt.month
    dates["day"] = dates["date"].dt.day
    dates["day_of_week"] = dates["date"].dt.dayofweek + 1  # 1=Lun ... 7=Dom
    dates["week_of_year"] = dates["date"].dt.isocalendar().week.astype(int)

    dates.to_parquet(out_path, index=False)
    return str(out_path)

if __name__ == "__main__":
    print(build_dim_date())