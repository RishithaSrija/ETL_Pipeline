# Analysis (etl_analysis.py)
# Read the loaded data from Supabase and perform:
# A. KPI Metrics
# City with highest average PM2.5
# City with the highest severity score
# Percentage of High/Moderate/Low risk hours
# Hour of day with worst AQI
# B. City Pollution Trend Report
# For each city:
# time → pm2_5, pm10, ozone
# C. Export Outputs
# Save the following CSVs into data/processed/:
# summary_metrics.csv
# city_risk_distribution.csv
# pollution_trends.csv
# D. Visualizations
# Save the following PNG plots:
# Histogram of PM2.5
# Bar chart of risk flags per city
# Line chart of hourly PM2.5 trends
# Scatter: severity_score vs pm2_5
# etl_analysis.py
"""
Read `air_quality_data` from Supabase and produce:
  - CSVs in data/processed/: summary_metrics.csv, city_risk_distribution.csv, pollution_trends.csv
  - PNGs in data/processed/: pm25_histogram.png, risk_flags_per_city.png, hourly_pm25_trends.png, severity_vs_pm25_scatter.png

Usage:
  - Set SUPABASE_URL and SUPABASE_KEY in environment OR pass via CLI args.
  - Then run:
      python etl_analysis.py
  - Optional CLI:
      python etl_analysis.py --supabase-url "https://<proj>.supabase.co" --supabase-key "<KEY>"
"""

from __future__ import annotations

import os
import logging
import argparse
from typing import Dict, Any
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    from supabase import create_client
except Exception:
    create_client = None  # will raise a clear error at runtime

# Output directory
PROCESSED_DIR = os.path.join("data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Logging
logger = logging.getLogger("etl_analysis")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(h)
logger.setLevel(os.environ.get("ANALYSIS_LOG_LEVEL", "INFO"))


def fetch_table_as_df(supabase_url: str, supabase_key: str, table_name: str = "air_quality_data") -> pd.DataFrame:
    """
    Fetch entire table from Supabase and return a cleaned DataFrame.
    Converts numeric columns and parses time column.
    """
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY are required (env or CLI).")

    if create_client is None:
        raise RuntimeError("supabase client not installed. Install with: pip install supabase")

    client = create_client(supabase_url, supabase_key)
    logger.info("Querying Supabase table: %s", table_name)
    res = client.table(table_name).select("*").execute()

    # normalize response shapes
    if hasattr(res, "data"):
        data = res.data
    elif isinstance(res, dict) and "data" in res:
        data = res["data"]
    else:
        data = res

    df = pd.DataFrame(data)

    # parse time column if present
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")

    # Cast numeric columns
    numeric_cols = [
        "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
        "sulphur_dioxide", "ozone", "uv_index", "severity_score", "hour"
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Normalize risk column to consistent categories (if present)
    if "risk_flag" in df.columns:
        df["risk_flag"] = df["risk_flag"].astype("string").fillna("Unknown")

    return df


def compute_kpis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute KPIs:
      - city_highest_avg_pm2_5 (+ value)
      - city_highest_avg_severity (+ value)
      - risk_distribution_percent (High/Moderate/Low/Unknown %)
      - hour_with_worst_avg_pm2_5 (+ value)
    """
    kpis: Dict[str, Any] = {}

    # City with highest average PM2.5
    if "pm2_5" in df.columns and "city" in df.columns:
        avg_pm25 = df.groupby("city")["pm2_5"].mean().dropna()
        if not avg_pm25.empty:
            kpis["city_highest_avg_pm2_5"] = avg_pm25.idxmax()
            kpis["city_highest_avg_pm2_5_value"] = float(avg_pm25.max())
        else:
            kpis["city_highest_avg_pm2_5"] = None
            kpis["city_highest_avg_pm2_5_value"] = None
    else:
        kpis["city_highest_avg_pm2_5"] = None
        kpis["city_highest_avg_pm2_5_value"] = None

    # City with highest average severity_score
    if "severity_score" in df.columns and "city" in df.columns:
        avg_sev = df.groupby("city")["severity_score"].mean().dropna()
        if not avg_sev.empty:
            kpis["city_highest_avg_severity"] = avg_sev.idxmax()
            kpis["city_highest_avg_severity_value"] = float(avg_sev.max())
        else:
            kpis["city_highest_avg_severity"] = None
            kpis["city_highest_avg_severity_value"] = None
    else:
        kpis["city_highest_avg_severity"] = None
        kpis["city_highest_avg_severity_value"] = None

    # Percentage of High/Moderate/Low risk hours
    if "risk_flag" in df.columns:
        counts = df["risk_flag"].fillna("Unknown").value_counts()
        total = counts.sum() if counts.sum() > 0 else 1
        dist = (counts / total * 100).round(2).to_dict()
        # ensure keys High/Moderate/Low exist for clarity
        kpis["risk_distribution_percent"] = {
            "High": float(dist.get("High", 0.0)),
            "Moderate": float(dist.get("Moderate", 0.0)),
            "Low": float(dist.get("Low", 0.0)),
            "Unknown": float(dist.get("Unknown", 0.0)),
        }
    else:
        kpis["risk_distribution_percent"] = {"High": 0.0, "Moderate": 0.0, "Low": 0.0, "Unknown": 0.0}

    # Hour of day with worst average PM2.5
    if "pm2_5" in df.columns and "hour" in df.columns:
        hourly = df.groupby("hour")["pm2_5"].mean().dropna()
        if not hourly.empty:
            kpis["hour_with_worst_avg_pm2_5"] = int(hourly.idxmax())
            kpis["hour_with_worst_avg_pm2_5_value"] = float(hourly.max())
        else:
            kpis["hour_with_worst_avg_pm2_5"] = None
            kpis["hour_with_worst_avg_pm2_5_value"] = None
    else:
        kpis["hour_with_worst_avg_pm2_5"] = None
        kpis["hour_with_worst_avg_pm2_5_value"] = None

    return kpis


def build_city_trends(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build per-city trends DataFrame with columns: city, time, pm2_5, pm10, ozone
    One row per timestamp per city (sorted).
    """
    cols = ["city", "time", "pm2_5", "pm10", "ozone"]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    trends = df[cols].copy()
    trends = trends.sort_values(["city", "time"]).reset_index(drop=True)
    return trends


def save_csvs_and_plots(df: pd.DataFrame, kpis: Dict[str, Any]) -> None:
    # 1) summary_metrics.csv
    summary = {
        "city_highest_avg_pm2_5": [kpis.get("city_highest_avg_pm2_5")],
        "city_highest_avg_pm2_5_value": [kpis.get("city_highest_avg_pm2_5_value")],
        "city_highest_avg_severity": [kpis.get("city_highest_avg_severity")],
        "city_highest_avg_severity_value": [kpis.get("city_highest_avg_severity_value")],
        "hour_with_worst_avg_pm2_5": [kpis.get("hour_with_worst_avg_pm2_5")],
        "hour_with_worst_avg_pm2_5_value": [kpis.get("hour_with_worst_avg_pm2_5_value")],
    }
    summary_df = pd.DataFrame(summary)
    summary_path = os.path.join(PROCESSED_DIR, "summary_metrics.csv")
    summary_df.to_csv(summary_path, index=False)
    logger.info("Wrote %s", summary_path)

    # 2) city_risk_distribution.csv
    if "risk_flag" in df.columns:
        risk_df = df.groupby(["city", "risk_flag"]).size().reset_index(name="count")
        total_per_city = risk_df.groupby("city")["count"].transform("sum")
        risk_df["percent"] = (risk_df["count"] / total_per_city * 100).round(2)
    else:
        risk_df = pd.DataFrame(columns=["city", "risk_flag", "count", "percent"])
    risk_path = os.path.join(PROCESSED_DIR, "city_risk_distribution.csv")
    risk_df.to_csv(risk_path, index=False)
    logger.info("Wrote %s", risk_path)

    # 3) pollution_trends.csv
    trends_df = build_city_trends(df)
    trends_path = os.path.join(PROCESSED_DIR, "pollution_trends.csv")
    trends_df.to_csv(trends_path, index=False)
    logger.info("Wrote %s", trends_path)

    # ---- Plots ----
    try:
        # Histogram of PM2.5
        if "pm2_5" in df.columns and not df["pm2_5"].dropna().empty:
            plt.figure(figsize=(8, 5))
            plt.hist(df["pm2_5"].dropna(), bins=30)
            plt.title("Histogram of PM2.5")
            plt.xlabel("PM2.5 (µg/m³)")
            plt.ylabel("Frequency")
            out = os.path.join(PROCESSED_DIR, "pm25_histogram.png")
            plt.savefig(out, dpi=150, bbox_inches="tight")
            plt.close()
            logger.info("Saved %s", out)
        else:
            logger.warning("pm2_5 missing or empty — histogram skipped.")

        # Bar chart: risk flags per city
        if "risk_flag" in df.columns and not df["risk_flag"].dropna().empty:
            bar_df = df.groupby(["city", "risk_flag"]).size().unstack(fill_value=0)
            plt.figure(figsize=(10, 6))
            bar_df.plot(kind="bar")
            plt.title("Risk Flags per City")
            plt.ylabel("Count")
            plt.xlabel("City")
            plt.xticks(rotation=45)
            out = os.path.join(PROCESSED_DIR, "risk_flags_per_city.png")
            plt.savefig(out, dpi=150, bbox_inches="tight")
            plt.close()
            logger.info("Saved %s", out)
        else:
            logger.warning("risk_flag missing or empty — bar chart skipped.")

        # Line chart of hourly PM2.5 trends (aggregate)
        if "pm2_5" in df.columns and "hour" in df.columns and not df["pm2_5"].dropna().empty:
            hourly_avg = df.groupby("hour")["pm2_5"].mean().reset_index()
            plt.figure(figsize=(10, 5))
            plt.plot(hourly_avg["hour"], hourly_avg["pm2_5"], marker="o")
            plt.title("Hourly Average PM2.5 (All Cities)")
            plt.xlabel("Hour of Day")
            plt.ylabel("Average PM2.5 (µg/m³)")
            plt.grid(True)
            plt.xticks(range(0, 24))
            out = os.path.join(PROCESSED_DIR, "hourly_pm25_trends.png")
            plt.savefig(out, dpi=150, bbox_inches="tight")
            plt.close()
            logger.info("Saved %s", out)
        else:
            logger.warning("pm2_5/hour missing or empty — hourly trend skipped.")

        # Scatter: severity_score vs pm2_5
        if "severity_score" in df.columns and "pm2_5" in df.columns and not df[["severity_score", "pm2_5"]].dropna().empty:
            plt.figure(figsize=(8, 6))
            plt.scatter(df["pm2_5"], df["severity_score"], alpha=0.6)
            plt.title("Severity Score vs PM2.5")
            plt.xlabel("PM2.5 (µg/m³)")
            plt.ylabel("Severity Score")
            plt.grid(True)
            out = os.path.join(PROCESSED_DIR, "severity_vs_pm25_scatter.png")
            plt.savefig(out, dpi=150, bbox_inches="tight")
            plt.close()
            logger.info("Saved %s", out)
        else:
            logger.warning("severity_score/pm2_5 missing or empty — scatter skipped.")
    except Exception as e:
        logger.exception("Plotting failed: %s", e)


def run_analysis(supabase_url: str | None = None, supabase_key: str | None = None) -> None:
    """
    Top-level: fetch table, compute KPIs, write CSVs and plots.
    """
    supabase_url = supabase_url or os.environ.get("SUPABASE_URL")
    supabase_key = supabase_key or os.environ.get("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be provided via env or CLI args.")

    df = fetch_table_as_df(supabase_url, supabase_key)

    if df.empty:
        logger.error("No data fetched from air_quality_data. Aborting analysis.")
        return

    kpis = compute_kpis(df)
    logger.info("Computed KPIs: %s", kpis)

    save_csvs_and_plots(df, kpis)
    logger.info("Analysis finished. Outputs written to %s", PROCESSED_DIR)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL analysis on air_quality_data from Supabase")
    parser.add_argument("--supabase-url", type=str, help="Supabase URL (overrides env)")
    parser.add_argument("--supabase-key", type=str, help="Supabase key (overrides env)")
    args = parser.parse_args()

    try:
        run_analysis(supabase_url=args.supabase_url, supabase_key=args.supabase_key)
    except Exception as exc:
        logger.exception("Analysis failed: %s", exc)
        raise
