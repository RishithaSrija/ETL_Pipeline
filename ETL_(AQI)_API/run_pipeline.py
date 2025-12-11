# Build a Combined ETL Runner
# Write run_pipeline.py:
# extract
# transform
# load
# analysis
# Automate it as:
# python run_pipeline.py

# run_pipeline.py
from __future__ import annotations

import os
import sys
import time
import logging
import argparse
from typing import List

# Logging setup
logger = logging.getLogger("pipeline")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(h)
logger.setLevel(os.environ.get("PIPELINE_LOG_LEVEL", "INFO"))

# Import local modules (fail early with friendly message)
try:
    import extract
except Exception as e:
    logger.exception("Failed to import extract module: %s", e)
    raise

try:
    import transform
except Exception as e:
    logger.exception("Failed to import transform module: %s", e)
    raise

try:
    from load import load_csv_to_supabase, create_table_if_not_exists  # load.create_table_if_not_exists should exist
except Exception as e:
    logger.exception("Failed to import load module or required functions: %s", e)
    raise

# etl_analysis may expose run_analysis() or main()
try:
    import etl_analysis
except Exception as e:
    logger.exception("Failed to import etl_analysis module: %s", e)
    raise


def safe_call(func, /, *args, **kwargs):
    """
    Try to call func with (args, kwargs). If TypeError because of signature mismatch,
    try calling func() with no args. Re-raise other exceptions.
    Returns func(...) return value.
    """
    try:
        return func(*args, **kwargs)
    except TypeError as te:
        # try no-arg call as fallback
        logger.debug("Function %s rejected provided args; trying no-arg call (reason: %s)", getattr(func, "__name__", func), te)
        try:
            return func()
        except Exception:
            logger.exception("Fallback no-arg call to %s also failed.", getattr(func, "__name__", func))
            raise
    except Exception:
        logger.exception("Call to %s failed.", getattr(func, "__name__", func))
        raise


def run_full_pipeline(raw_dir: str,
                      staged_csv: str,
                      batch_size: int,
                      supabase_url: str | None,
                      supabase_key: str | None,
                      create_table: bool):
    # inject Supabase CLI creds into environment so downstream modules can pick them up
    if supabase_url:
        os.environ["SUPABASE_URL"] = supabase_url
    if supabase_key:
        os.environ["SUPABASE_KEY"] = supabase_key

    logger.info("Starting pipeline. raw_dir=%s, staged_csv=%s, batch_size=%d", raw_dir, staged_csv, batch_size)

    # 1) Extract
    logger.info("Running extraction...")
    try:
        # expected: extract.extract_all_cities(raw_dir=...) or extract_all_cities()
        saved = safe_call(extract.extract_all_cities, raw_dir)
        # If the function returns a single path, unify to list
        if isinstance(saved, str):
            saved_files: List[str] = [saved]
        elif isinstance(saved, list):
            saved_files = saved
        else:
            logger.warning("extract_all_cities returned unexpected type %s. Proceeding.", type(saved))
            saved_files = list(saved) if saved is not None else []
    except Exception as e:
        logger.exception("Extraction failed: %s", e)
        raise

    if not saved_files:
        logger.warning("Extraction produced no raw files. Aborting pipeline.")
        raise RuntimeError("No raw files produced by extraction.")

    logger.info("Extraction saved %d file(s). Sample: %s", len(saved_files), saved_files[:3])

    # small pause
    time.sleep(0.5)

    # 2) Transform
    logger.info("Running transformation...")
    try:
        # transform.transform_all(raw_dir=..., out_csv=...) or transform_all(list_of_files) or transform_all()
        # Try common signatures in order:
        out_csv = None
        # 1) transform_all(raw_dir=..., out_csv=...)
        try:
            out_csv = transform.transform_all(raw_dir=raw_dir, out_csv=staged_csv)
        except TypeError:
            # 2) transform_all(list_of_files)
            try:
                out_csv = transform.transform_all(saved_files)
            except TypeError:
                # 3) no-arg
                out_csv = transform.transform_all()
    except Exception as e:
        logger.exception("Transformation failed: %s", e)
        raise

    # If transform returned None, assume staged_csv path
    if out_csv is None:
        out_csv = staged_csv

    if not os.path.exists(out_csv):
        logger.error("Transformed CSV not found at expected path: %s", out_csv)
        raise FileNotFoundError(out_csv)

    logger.info("Transformation completed. Staged CSV: %s", out_csv)

    # 3) Create table (optional)
    if create_table:
        logger.info("Attempting to create table (create_table_if_not_exists)...")
        try:
            ok = create_table_if_not_exists()
            if not ok:
                logger.warning("create_table_if_not_exists reported failure or asked for manual creation. Please ensure table exists.")
        except Exception as e:
            logger.exception("Table creation attempt failed: %s", e)
            raise

    # 4) Load
    logger.info("Running loader to Supabase...")
    try:
        load_result = load_csv_to_supabase(input_csv=out_csv, batch_size=batch_size, retries=2,
                                           supabase_url=os.environ.get("SUPABASE_URL"),
                                           supabase_key=os.environ.get("SUPABASE_KEY"))
        logger.info("Load result: %s", load_result)
    except Exception as e:
        logger.exception("Loading failed: %s", e)
        raise

    # 5) Analysis
    logger.info("Running analysis...")
    try:
        # prefer explicit run_analysis in module
        if hasattr(etl_analysis, "run_analysis"):
            safe_call(etl_analysis.run_analysis)
        elif hasattr(etl_analysis, "main"):
            safe_call(etl_analysis.main)
        elif hasattr(etl_analysis, "run"):
            safe_call(etl_analysis.run)
        else:
            # try calling top-level function as fallback
            logger.warning("etl_analysis has no run_analysis/main/run. Attempting to call module-level run_analysis name anyway.")
            safe_call(getattr(etl_analysis, "run_analysis"))
    except Exception as e:
        logger.exception("Analysis failed: %s", e)
        raise

    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run full ETL pipeline: extract -> transform -> load -> analysis")
    parser.add_argument("--raw-dir", type=str, default=os.path.join("data", "raw"), help="raw directory for extraction and transform")
    parser.add_argument("--staged", type=str, default=os.path.join("data", "staged", "air_quality_transformed.csv"), help="staged CSV path (transform output)")
    parser.add_argument("--batch", type=int, default=200, help="batch size for loading")
    parser.add_argument("--supabase-url", type=str, help="Supabase URL (overrides env)")
    parser.add_argument("--supabase-key", type=str, help="Supabase Key (overrides env)")
    parser.add_argument("--create-table", action="store_true", help="Attempt to create the target table before loading (requires DATABASE_URL/psycopg2 support in load.py)")
    args = parser.parse_args()

    # inject cred env for downstream modules
    if args.supabase_url:
        os.environ["SUPABASE_URL"] = args.supabase_url
    if args.supabase_key:
        os.environ["SUPABASE_KEY"] = args.supabase_key

    try:
        run_full_pipeline(raw_dir=args.raw_dir,
                          staged_csv=args.staged,
                          batch_size=args.batch,
                          supabase_url=args.supabase_url,
                          supabase_key=args.supabase_key,
                          create_table=args.create_table)
    except Exception as exc:
        logger.exception("Pipeline terminated with error: %s", exc)
        sys.exit(1)