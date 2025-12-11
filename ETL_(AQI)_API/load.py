# Load (load.py) – Supabase
# Create table:
# air_quality_data (
#     id BIGSERIAL PRIMARY KEY,
#     city TEXT,
#     time TIMESTAMP,
#     pm10 FLOAT,
#     pm2_5 FLOAT,
#     carbon_monoxide FLOAT,
#     nitrogen_dioxide FLOAT,
#     sulphur_dioxide FLOAT,
#     ozone FLOAT,
#     uv_index FLOAT,
#     aqi_category TEXT,
#     severity_score FLOAT,
#     risk_flag TEXT,
#     hour INTEGER)
# Load Requirements
# Batch insert records (batch size = 200)
# Auto-convert NaN → NULL
# Convert datetime to ISO formatted strings
# Retry failed batches (2 retries)
# Print summary of inserted rows


# load.py
"""
Load transformed air-quality CSV into Supabase.

Features implemented (per requirements):
- CREATE_TABLE_SQL provided (run manually in Supabase SQL editor or use --create-table to attempt programmatic creation via DATABASE_URL)
- Batch insert records (default batch_size=200)
- Auto-convert NaN -> NULL
- Convert datetime to ISO formatted strings
- Retry failed batches (default 2 retries)
- Print summary of inserted rows

Usage examples:
# Load (provide Supabase credentials via CLI)
python load.py --input data/staged/air_quality_transformed.csv --batch 200 --retries 2 \
  --supabase-url "https://<project>.supabase.co" --supabase-key "<SUPABASE_KEY>"

# Create table programmatically (requires DATABASE_URL env var and psycopg2 installed)
python load.py --create-table

# Or set SUPABASE_URL and SUPABASE_KEY env vars and run without CLI creds.
"""
from __future__ import annotations

import os
import time
import argparse
import logging
from typing import List, Dict, Any, Iterable

import pandas as pd

# attempt to import supabase client; raise at runtime with clear message if missing
try:
    from supabase import create_client
except Exception:
    create_client = None

# ---------- DDL ----------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.air_quality_data (
  id BIGSERIAL PRIMARY KEY,
  city TEXT,
  time TIMESTAMP,
  pm10 FLOAT,
  pm2_5 FLOAT,
  carbon_monoxide FLOAT,
  nitrogen_dioxide FLOAT,
  sulphur_dioxide FLOAT,
  ozone FLOAT,
  uv_index FLOAT,
  aqi_category TEXT,
  severity_score FLOAT,
  risk_flag TEXT,
  hour INTEGER
);
"""

# ---------- Defaults ----------
DEFAULT_INPUT = os.path.join("data", "staged", "air_quality_transformed.csv")
DEFAULT_BATCH_SIZE = 200
DEFAULT_RETRIES = 2  # retries in addition to the initial attempt
RETRY_BACKOFF = 2.0  # exponential backoff base in seconds

# ---------- Logging ----------
logger = logging.getLogger("load")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(os.environ.get("LOAD_LOG_LEVEL", "INFO"))


# ---------- Helpers ----------
def _chunks(iterable: Iterable, n: int):
    lst = list(iterable)
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def pd_isna(x) -> bool:
    try:
        return pd.isna(x)
    except Exception:
        if x is None:
            return True
        try:
            return float(x) != float(x)
        except Exception:
            return False


def _sanitize_value(x):
    """Convert pandas/numpy NA to Python None and numpy scalars to native Python."""
    if pd_isna(x):
        return None
    try:
        # numpy scalar -> native
        if hasattr(x, "item"):
            return x.item()
    except Exception:
        pass
    return x


def _row_to_insertable(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a row (dict) to JSON-friendly types:
    - pandas.Timestamp / datetime -> ISO string
    - NaN/NA -> None
    """
    out: Dict[str, Any] = {}
    for k, v in record.items():
        if pd_isna(v):
            out[k] = None
            continue
        # pandas.Timestamp
        try:
            if isinstance(v, pd.Timestamp):
                out[k] = v.isoformat()
                continue
        except Exception:
            pass
        # datetime.datetime
        try:
            import datetime

            if isinstance(v, datetime.datetime):
                out[k] = v.isoformat()
                continue
        except Exception:
            pass
        out[k] = _sanitize_value(v)
    return out


# ---------- Table creation helper ----------
def create_table_if_not_exists() -> bool:
    """
    Attempt to create the target table using DATABASE_URL (psycopg2 required).
    If DATABASE_URL missing or psycopg2 not installed, print CREATE_TABLE_SQL and return False.
    Returns True on success.
    """
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.warning("DATABASE_URL not set; cannot create table programmatically.")
        print("\nPlease create the table in Supabase SQL editor using this DDL:\n")
        print(CREATE_TABLE_SQL)
        return False

    try:
        import psycopg2
    except Exception:
        logger.warning("psycopg2 not installed. Install with: pip install psycopg2-binary")
        print("\nPlease create the table in Supabase SQL editor using this DDL:\n")
        print(CREATE_TABLE_SQL)
        return False

    try:
        logger.info("Connecting to DB to run CREATE TABLE...")
        conn = psycopg2.connect(database_url)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_SQL)
        cur.close()
        conn.close()
        logger.info("CREATE TABLE executed (table created or already existed).")
        return True
    except Exception as e:
        logger.exception("Failed to run CREATE TABLE: %s", e)
        print("\nFailed to create table programmatically. Please run the following DDL in Supabase SQL editor:\n")
        print(CREATE_TABLE_SQL)
        return False


# ---------- Core loader ----------
def load_csv_to_supabase(
    input_csv: str = DEFAULT_INPUT,
    batch_size: int = DEFAULT_BATCH_SIZE,
    retries: int = DEFAULT_RETRIES,
    supabase_url: str | None = None,
    supabase_key: str | None = None,
) -> Dict[str, Any]:
    """
    Load CSV into Supabase table `air_quality_data`.
    Returns a summary dict: {inserted, failed_estimate, batch_errors_count, batch_errors_sample}
    """
    url = supabase_url or os.environ.get("SUPABASE_URL")
    key = supabase_key or os.environ.get("SUPABASE_KEY")

    if not url or not key:
        logger.error("SUPABASE_URL and SUPABASE_KEY are required (env or CLI flags).")
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY environment variables are required.")

    if create_client is None:
        raise RuntimeError("supabase client not installed. Install with: pip install supabase")

    client = create_client(url, key)
    logger.info("Connected to Supabase at %s", url)

    if not os.path.exists(input_csv):
        logger.error("Input CSV not found: %s", input_csv)
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    # Load CSV; parse time column into datetimes
    df = pd.read_csv(input_csv, parse_dates=["time"], keep_default_na=True, na_values=["", "NaN", "nan"])
    logger.info("Loaded %d rows from %s", len(df), input_csv)

    # Accept alternate column names and normalize
    if "severity_score" in df.columns and "severity" not in df.columns:
        df["severity"] = df["severity_score"]
    if "risk_flag" in df.columns and "risk_class" not in df.columns:
        df["risk_class"] = df["risk_flag"]
    df = df.rename(columns={"severity": "severity_score", "risk_class": "risk_flag"})

    # Ensure DB columns present
    db_cols = [
        "city",
        "time",
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "uv_index",
        "aqi_category",
        "severity_score",
        "risk_flag",
        "hour",
    ]
    for c in db_cols:
        if c not in df.columns:
            df[c] = None

    # Reorder to DB schema
    df = df[db_cols]

    # Convert each row to JSON-friendly dict
    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        records.append(_row_to_insertable(row.to_dict()))

    total_inserted = 0
    total_failed = 0
    batch_errors: List[Dict[str, Any]] = []

    attempts_allowed = retries + 1  # initial attempt + retries

    for batch_idx, batch in enumerate(_chunks(records, batch_size), start=1):
        attempt = 0
        success = False
        while attempt < attempts_allowed and not success:
            attempt += 1
            try:
                logger.info("Inserting batch %d (size=%d), attempt %d/%d", batch_idx, len(batch), attempt, attempts_allowed)
                res = client.table("air_quality_data").insert(batch).execute()
                # Try to infer inserted_count from different response shapes
                inserted_count = None
                try:
                    if hasattr(res, "data") and res.data is not None:
                        inserted_count = len(res.data) if isinstance(res.data, list) else 1
                    elif isinstance(res, dict) and "data" in res and res["data"] is not None:
                        inserted_count = len(res["data"]) if isinstance(res["data"], list) else 1
                except Exception:
                    inserted_count = None

                if inserted_count is None:
                    # assume success for entire batch if no exception thrown
                    inserted_count = len(batch)

                total_inserted += inserted_count
                logger.info("Batch %d inserted %d rows", batch_idx, inserted_count)
                success = True
                # small pause
                time.sleep(0.05)
            except Exception as exc:
                logger.warning("Batch %d attempt %d failed: %s", batch_idx, attempt, exc)
                batch_errors.append({"batch": batch_idx, "attempt": attempt, "error": str(exc)})
                if attempt < attempts_allowed:
                    wait = RETRY_BACKOFF ** attempt
                    logger.info("Retrying batch %d after %.1fs", batch_idx, wait)
                    time.sleep(wait)
                else:
                    logger.error("Batch %d failed after %d attempts. Skipping batch.", batch_idx, attempt)
                    total_failed += len(batch)
                    break

    logger.info(
        "Load complete. Inserted (approx): %d. Failed rows (approx): %d. Batches with errors: %d",
        total_inserted,
        total_failed,
        len(batch_errors),
    )
    if batch_errors:
        logger.info("Sample batch errors: %s", batch_errors[:5])

    return {
        "inserted": total_inserted,
        "failed_estimate": total_failed,
        "batch_errors_count": len(batch_errors),
        "batch_errors_sample": batch_errors[:5],
    }


# ---------- CLI ----------
def _cli():
    parser = argparse.ArgumentParser(description="Load transformed CSV into Supabase (air_quality_data).")
    parser.add_argument("--input", type=str, default=DEFAULT_INPUT, help="Transformed CSV input file")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size for inserts")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES, help="Retry attempts per batch (in addition to initial attempt)")
    parser.add_argument("--supabase-url", type=str, help="Supabase URL (overrides SUPABASE_URL env)")
    parser.add_argument("--supabase-key", type=str, help="Supabase key (overrides SUPABASE_KEY env)")
    parser.add_argument("--create-table", action="store_true", help="Attempt to create table using DATABASE_URL (requires psycopg2)")
    args = parser.parse_args()

    # inject CLI creds into environment (downstream convenience)
    if args.supabase_url:
        os.environ["SUPABASE_URL"] = args.supabase_url
    if args.supabase_key:
        os.environ["SUPABASE_KEY"] = args.supabase_key

    if args.create_table:
        ok = create_table_if_not_exists()
        if not ok:
            logger.error("Table creation did not complete. Please create the table and re-run.")
            return

    result = load_csv_to_supabase(
        input_csv=args.input,
        batch_size=args.batch,
        retries=args.retries,
        supabase_url=args.supabase_url,
        supabase_key=args.supabase_key,
    )
    print("Summary:", result)


if __name__ == "__main__":
    _cli()
