# stats/split_data.py
import pandas as pd
from pathlib import Path

def prompt_time(label):
    val = input(f"{label} (ISO 8601, e.g. 2025-01-15 13:00 or 2025-01-15T13:00): ").strip()
    return pd.to_datetime(val)

def main():
    csv_path = Path(__file__).resolve().parent / "download_stats.csv"
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)
    if 'start' not in df.columns or 'end' not in df.columns:
        print("CSV missing required columns: 'start' and/or 'end'")
        return

    # Ask which timestamp to filter by
    ts_col = "start"

    # Convert unix seconds to datetime
    df[f"{ts_col}_dt"] = pd.to_datetime(df[ts_col], unit='s', errors='coerce')
    df = df.dropna(subset=[f"{ts_col}_dt"])

    # After df[f"{ts_col}_dt"] is created and NaTs dropped
    min_dt = df[f"{ts_col}_dt"].min()
    max_dt = df[f"{ts_col}_dt"].max()
    print(f"Available {ts_col} time range: {min_dt} -> {max_dt}")

    # Get time range
    start_dt = prompt_time("Start time")
    end_dt = prompt_time("End time")

    # Filter
    mask = (df[f"{ts_col}_dt"] >= start_dt) & (df[f"{ts_col}_dt"] <= end_dt)
    filtered = df.loc[mask].copy()

    # Output path
    out_name = input("Output filename [download_stats_filtered.csv]: ").strip() or "download_stats_filtered.csv"
    out_path = csv_path.parent / out_name

    # Drop helper column before saving
    filtered = filtered.drop(columns=[f"{ts_col}_dt"])
    filtered.to_csv(out_path, index=False)

    print(f"Saved {len(filtered)} rows to {out_path}")

if __name__ == "__main__":
    main()