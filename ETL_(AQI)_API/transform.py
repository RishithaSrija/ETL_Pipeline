# transform.py
"""
Transform step for AtmosTrack Air Quality ETL.

Reads raw JSON files saved under data/raw/*_raw_*.json (either raw API JSON or wrapper with "body"),
flattens hourly arrays into tabular rows, computes derived features, filters empty rows,
and saves a combined CSV to data/staged/air_quality_transformed.csv.

Columns:
  city, time, pm10, pm2_5, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide, ozone, uv_index,
  aqi_category, severity, risk_class, hour

Usage:
  python transform.py
  python transform.py --raw-dir data/raw --out data/staged/air_quality_transformed.csv
"""

import os
import json
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging
import math
import pandas as pd

DEFAULT_RAW_DIR = os.path.join("data", "raw")
DEFAULT_OUT = os.path.join("data", "staged", "air_quality_transformed.csv")
os.makedirs(os.path.dirname(DEFAULT_OUT), exist_ok=True)

logger = logging.getLogger("transform")
if not logger.handlers:
    h = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    h.setFormatter(fmt)
    logger.addHandler(h)
logger.setLevel("INFO")

# Expected hourly keys from Open-Meteo request
HOURLY_KEYS = [
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "uv_index",
]

# ---------- Feature functions ----------
def aqi_from_pm25(pm25: Optional[float]) -> str:
    """Map pm2_5 numeric to categorical AQI band."""
    if pm25 is None or (isinstance(pm25, float) and math.isnan(pm25)):
        return "Unknown"
    # PM2.5 ranges (inclusive lower bound)
    # 0–50 → Good
    # 51–100 → Moderate
    # 101–200 → Unhealthy
    # 201–300 → Very Unhealthy
    # >300 → Hazardous
    try:
        v = float(pm25)
    except Exception:
        return "Unknown"
    if 0 <= v <= 50:
        return "Good"
    if 51 <= v <= 100:
        return "Moderate"
    if 101 <= v <= 200:
        return "Unhealthy"
    if 201 <= v <= 300:
        return "Very Unhealthy"
    if v > 300:
        return "Hazardous"
    return "Unknown"

def compute_severity(row: Dict[str, float]) -> float:
    """
    severity = (pm2_5 * 5) + (pm10 * 3) +
               (nitrogen_dioxide * 4) + (sulphur_dioxide * 4) +
               (carbon_monoxide * 2) + (ozone * 3)
    Missing values treated as 0 for severity calculation.
    """
    pm25 = 0.0 if pd_isna(row.get("pm2_5")) else float(row.get("pm2_5"))
    pm10 = 0.0 if pd_isna(row.get("pm10")) else float(row.get("pm10"))
    no2 = 0.0 if pd_isna(row.get("nitrogen_dioxide")) else float(row.get("nitrogen_dioxide"))
    so2 = 0.0 if pd_isna(row.get("sulphur_dioxide")) else float(row.get("sulphur_dioxide"))
    co = 0.0 if pd_isna(row.get("carbon_monoxide")) else float(row.get("carbon_monoxide"))
    o3 = 0.0 if pd_isna(row.get("ozone")) else float(row.get("ozone"))

    # Compute exactly using float arithmetic (careful about precision if needed)
    severity = (pm25 * 5.0) + (pm10 * 3.0) + (no2 * 4.0) + (so2 * 4.0) + (co * 2.0) + (o3 * 3.0)
    return float(severity)

def risk_from_severity(sev: float) -> str:
    """Risk classification from severity thresholds."""
    try:
        if sev > 400:
            return "High Risk"
        if sev > 200:
            return "Moderate Risk"
        return "Low Risk"
    except Exception:
        return "Unknown"

def pd_isna(x: Any) -> bool:
    """Helper to treat None or NaN as missing."""
    return x is None or (isinstance(x, float) and math.isnan(x))

# ---------- Parsing helpers ----------
def extract_city_from_filename(fname: str) -> str:
    """
    Try to infer city name from filename like 'Delhi_raw_2025...json'.
    Takes the part before '_raw_'.
    """
    base = os.path.basename(fname)
    if "_raw_" in base:
        return base.split("_raw_")[0]
    # fallback: remove extension
    return os.path.splitext(base)[0]

def load_raw_json(filepath: str) -> Optional[Dict[str, Any]]:
    """Load JSON from file and return the API body (respect wrapper with 'body' if present)."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        logger.error("Failed to read/parse JSON file %s: %s", filepath, e)
        return None

    # If this file is the wrapper used by extract scripts, body stored under "body"
    if isinstance(payload, dict) and "body" in payload:
        return payload["body"]
    return payload

# ---------- Core transform ----------
def transform_single_file(filepath: str) -> Optional[pd.DataFrame]:
    """
    Transform a single raw JSON file into a DataFrame with required columns.
    Returns None if transformation failed or no rows.
    """
    raw = load_raw_json(filepath)
    if raw is None:
        return None

    # The Open-Meteo air-quality response places hourly arrays under raw["hourly"]
    hourly = None
    if isinstance(raw, dict):
        hourly = raw.get("hourly") or raw.get("results") or None
    if hourly is None or not isinstance(hourly, dict):
        logger.warning("File %s: unexpected JSON shape; missing 'hourly' object. Skipping.", filepath)
        return None

    # Retrieve time array
    times = hourly.get("time") or hourly.get("times") or None
    if not isinstance(times, list):
        logger.warning("File %s: missing hourly time array. Skipping.", filepath)
        return None

    # For each expected pollutant, get the array (may be missing)
    arrays: Dict[str, Optional[List[Any]]] = {}
    for key in HOURLY_KEYS:
        # API might use 'pm2_5' or 'pm2_5' exactly — we used HOURLY_KEYS to match.
        arrays[key] = hourly.get(key)

    # Check that at least time exists
    n = len(times)
    # Prepare rows
    rows: List[Dict[str, Any]] = []
    city = extract_city_from_filename(filepath)

    for i in range(n):
        ts = times[i]
        row: Dict[str, Any] = {
            "city": city,
            "time": ts,
        }
        # for each pollutant, take ith value if array exists and index in range else None
        for key in HOURLY_KEYS:
            arr = arrays.get(key)
            val = None
            if isinstance(arr, list) and i < len(arr):
                val = arr[i]
            # convert to numeric where possible; leave None for missing
            try:
                if val is None:
                    row[key] = None
                else:
                    # Try parse number; some APIs may give strings
                    # Use float conversion (digit-by-digit arithmetic not necessary here)
                    row[key] = float(val)
            except Exception:
                # non-convertible -> treat as missing
                row[key] = None
        rows.append(row)

    if not rows:
        logger.warning("No hourly rows produced for file %s", filepath)
        return None

    # Build DataFrame
    import pandas as pd  # local import for clarity
    df = pd.DataFrame(rows)

    # Convert time column to datetime
    try:
        df["time"] = pd.to_datetime(df["time"], utc=True)
    except Exception:
        # As fallback, keep original strings but log
        logger.warning("Failed to convert 'time' to datetime in file %s; keeping original strings.", filepath)

    # Remove records where all pollutant readings are missing
    pollutant_cols = [k for k in HOURLY_KEYS]
    # consider all missing if each pollutant column is NA
    df = df[~df[pollutant_cols].isna().all(axis=1)].copy()
    if df.empty:
        logger.info("After dropping fully-missing pollutant rows, nothing remains for %s", filepath)
        return None

    # Derived features
    # AQI category based on pm2_5
    df["aqi_category"] = df["pm2_5"].apply(aqi_from_pm25)

    # Severity: use missing -> 0 for pollutants
    def _compute_row_severity(r):
        # r is a pandas Series
        vals = {
            "pm2_5": 0.0 if pd_isna(r.get("pm2_5")) else float(r.get("pm2_5")),
            "pm10": 0.0 if pd_isna(r.get("pm10")) else float(r.get("pm10")),
            "nitrogen_dioxide": 0.0 if pd_isna(r.get("nitrogen_dioxide")) else float(r.get("nitrogen_dioxide")),
            "sulphur_dioxide": 0.0 if pd_isna(r.get("sulphur_dioxide")) else float(r.get("sulphur_dioxide")),
            "carbon_monoxide": 0.0 if pd_isna(r.get("carbon_monoxide")) else float(r.get("carbon_monoxide")),
            "ozone": 0.0 if pd_isna(r.get("ozone")) else float(r.get("ozone")),
        }
        return compute_severity(vals)

    df["severity"] = df.apply(_compute_row_severity, axis=1)

    # Risk classification
    df["risk_class"] = df["severity"].apply(risk_from_severity)

    # Hour of day
    def extract_hour(x):
        try:
            if isinstance(x, (str,)):
                ts = pd.to_datetime(x, utc=True)
            else:
                ts = x
            return int(ts.hour)
        except Exception:
            return None

    df["hour"] = df["time"].apply(extract_hour)

    # Re-order columns to match spec + derived
    out_cols = [
        "city", "time",
        "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index",
        "aqi_category", "severity", "risk_class", "hour"
    ]
    # ensure all out_cols present (if uv_index missing, create column filled with NaN)
    for c in out_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[out_cols].copy()
    return df

def transform_all(raw_dir: str = DEFAULT_RAW_DIR, out_csv: str = DEFAULT_OUT) -> str:
    """
    Process all JSON files in raw_dir that look like '*_raw_*.json',
    concatenate them, and write out_csv. Returns path to out_csv.
    """
    files = [
        os.path.join(raw_dir, f) for f in os.listdir(raw_dir)
        if os.path.isfile(os.path.join(raw_dir, f)) and "_raw_" in f and f.lower().endswith(".json")
    ]
    if not files:
        raise RuntimeError(f"No raw files found in {raw_dir} (looking for '*_raw_*.json').")

    dfs = []
    for fp in sorted(files):
        logger.info("Transforming file: %s", fp)
        try:
            df = transform_single_file(fp)
            if df is not None and not df.empty:
                dfs.append(df)
                logger.info("Produced %d rows from %s", len(df), fp)
            else:
                logger.info("No rows produced from %s (skipping)", fp)
        except Exception as e:
            logger.exception("Error transforming %s: %s", fp, e)
            continue

    if not dfs:
        raise RuntimeError("No data transformed from raw files. Nothing to write.")

    final_df = pd.concat(dfs, ignore_index=True)
    # Optional: sort by city/time
    try:
        final_df = final_df.sort_values(["city", "time"]).reset_index(drop=True)
    except Exception:
        pass

    # Ensure staging dir exists
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    # Write CSV
    final_df.to_csv(out_csv, index=False)
    logger.info("Wrote transformed CSV with %d rows to %s", len(final_df), out_csv)
    return out_csv

# ---------- CLI ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform raw Open-Meteo air-quality JSON to tabular CSV")
    parser.add_argument("--raw-dir", type=str, default=DEFAULT_RAW_DIR, help="Directory containing raw JSON files")
    parser.add_argument("--out", type=str, default=DEFAULT_OUT, help="Output CSV file path")
    args = parser.parse_args()

    try:
        outpath = transform_all(raw_dir=args.raw_dir, out_csv=args.out)
        print("Transformed CSV written to:", outpath)
    except Exception as e:
        logger.exception("Transform failed: %s", e)
        raise
