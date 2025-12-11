# Urban Air Quality Monitoring – Multi-City ETL Pipeline
# A government environmental agency wants to build an automated analytics system that monitors air quality across multiple Indian cities. The agency provides an open, unauthenticated API (no token required) that returns Air Quality Index (AQI) and pollutant information.
# You are required to build a complete ETL pipeline (Extract → Transform → Load → Analyze) using Python and Supabase.
# 🟦 1️⃣ Extract (extract.py)
# Use the following public API:
# API Endpoint (No Token Needed):
# OpenAQ API (Public Open Data):
# https://api.openaq.org/v2/latest
# Your task
# Write code that:
# Fetches AQI readings for 5 cities:
# Delhi, Bengaluru, Hyderabad, Mumbai, Kolkata
# For each city, call the API with a query like:
# Save each API response separately inside:
# Implement:
# Retry logic (3 attempts)
# Graceful failure handling
# Logging of errors and empty responses
# Return list of all saved file paths.
# data/raw/cityname_raw_timestamp.json
# https://api.openaq.org/v2/latest?city=Delhi

# extract.py
import os
import json
import time
import logging
import traceback
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
import requests

# ----- Configuration -----
BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
RAW_DIR = os.path.join("data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

CITIES = {
    "Delhi": {"latitude": 28.7041, "longitude": 77.1025},
    "Mumbai": {"latitude": 19.0760, "longitude": 72.8777},
    "Bengaluru": {"latitude": 12.9716, "longitude": 77.5946},
    "Hyderabad": {"latitude": 17.3850, "longitude": 78.4867},
    "Kolkata": {"latitude": 22.5726, "longitude": 88.3639},
}

HOURLY_FIELDS = "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,uv_index"

MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0  # seconds
TIMEOUT = 15  # seconds

# ----- Logging -----
logger = logging.getLogger("extract")
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
logger.setLevel(os.environ.get("EXTRACT_LOG_LEVEL", "INFO"))

# ----- Helpers -----
def _now_ts() -> str:
    """Compact UTC timestamp safe for filenames."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _safe_city(city: str) -> str:
    """Make city name filename-safe."""
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in city)[:100]


def _filepath_for_city(city: str) -> str:
    safe = _safe_city(city)
    return os.path.join(RAW_DIR, f"{safe}_raw_{_now_ts()}.json")


def _save_raw(filepath: str, url: str, status_code: Optional[int], body: Any, extra: Optional[Dict[str, Any]] = None) -> None:
    payload: Dict[str, Any] = {
        "ts": _now_ts(),
        "url": url,
        "status_code": status_code,
        "body": body,
    }
    if extra:
        payload["extra"] = extra
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Saved raw response to %s", filepath)


def _request_with_retries(url: str, params: Dict[str, Any]) -> requests.Response:
    backoff = INITIAL_BACKOFF
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.debug("Attempt %d: GET %s params=%s", attempt, url, params)
            resp = requests.get(url, params=params, timeout=TIMEOUT)
            # Try to capture body for debugging even on non-200
            try:
                body = resp.json()
            except ValueError:
                body = resp.text
            # Save attempt-specific file for audit
            attempt_fp = _filepath_for_city(params.get("city", "unknown"))
            _save_raw(attempt_fp, resp.url, resp.status_code, body, extra={"attempt": attempt})
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning("Request attempt %d failed for city=%s: %s", attempt, params.get("city"), exc)
            # Save the exception metadata too
            try:
                err_fp = _filepath_for_city(params.get("city", "unknown"))
                _save_raw(err_fp, url, None, None, extra={
                    "attempt": attempt,
                    "exception": str(exc),
                    "traceback": traceback.format_exc(),
                    "params": params
                })
            except Exception as save_err:
                logger.debug("Failed to save error metadata: %s", save_err)
            if attempt == MAX_RETRIES:
                logger.error("All %d attempts failed for city=%s", MAX_RETRIES, params.get("city"))
                raise
            time.sleep(backoff)
            backoff *= 2.0
    # Should not reach here
    if last_exc:
        raise last_exc
    raise RuntimeError("Unexpected error in _request_with_retries")


# ----- Public extraction function -----
def extract_all_cities() -> List[str]:
    """
    Fetch hourly air-quality data for all configured cities and save raw responses.
    Returns list of saved file paths (skips cities that permanently failed).
    """
    saved_files: List[str] = []
    for city, coords in CITIES.items():
        logger.info("Fetching air-quality for city: %s", city)
        params = {
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "hourly": HOURLY_FIELDS,
        }
        try:
            resp = _request_with_retries(BASE_URL, params=params)
        except Exception as exc:
            logger.exception("Failed to fetch data for city=%s: %s", city, exc)
            # Graceful: continue to next city
            continue

        # Save final file (labelled final)
        try:
            body = resp.json()
        except ValueError:
            body = resp.text

        filepath = _filepath_for_city(city)
        try:
            _save_raw(filepath, resp.url, resp.status_code, body)
            saved_files.append(filepath)
        except Exception as exc:
            logger.exception("Failed to save final raw file for city=%s: %s", city, exc)
            # do not append; continue others

    logger.info("Extraction finished. %d files saved.", len(saved_files))
    return saved_files


# ----- CLI / quick test -----
if __name__ == "__main__":
    # set debug for CLI
    logging.getLogger().setLevel(logging.DEBUG)
    files = extract_all_cities()
    print("Saved files:")
    for p in files:
        print(" -", p)