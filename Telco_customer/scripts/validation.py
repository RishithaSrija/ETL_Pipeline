# VALIDATION SCRIPT (validate.py)
# After load, write a script that checks:
# No missing values in:
# tenure, MonthlyCharges, TotalCharges
# Unique count of rows = original dataset
# Row count matches Supabase table
# All segments (tenure_group, monthly_charge_segment) exist
# Contract codes are only {0,1,2}
# Print a validation summary.


import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

def get_supabase_client() -> Client:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ SUPABASE_URL or SUPABASE_KEY not set in environment/.env file")

    return create_client(url, key)


def load_local_staged_csv() -> pd.DataFrame:
    """Load the transformed CSV from data/staged/churn_transformed.csv"""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_path = os.path.join(base_dir, "data", "staged", "churn_transformed.csv")

    if not os.path.exists(staged_path):
        raise FileNotFoundError(f"❌ Staged file not found at: {staged_path}")

    df = pd.read_csv(staged_path)
    print(f"📁 Loaded local staged data: {staged_path} ({len(df)} rows)")
    return df


def load_supabase_table(table_name: str = "telcocust_data") -> pd.DataFrame:
    """Load all rows from Supabase table into a DataFrame."""
    supabase = get_supabase_client()

    # Fetch everything (for small dataset like Telco this is okay)
    res = supabase.table(table_name).select("*").execute()

    if hasattr(res, "data"):
        data = res.data
    else:
        data = res.get("data", [])

    df_sb = pd.DataFrame(data)
    print(f"🌐 Loaded Supabase table '{table_name}': {len(df_sb)} rows")
    return df_sb


# ---------- Validation Logic ----------

def validate_data():
    print("🔍 Starting validation...\n")

    # 1. Load local & Supabase data
    df_local = load_local_staged_csv()
    df_sb = load_supabase_table("telcocust_data")

    # 2. Check no missing values in tenure, MonthlyCharges, TotalCharges
    numeric_cols = ["tenure", "MonthlyCharges", "TotalCharges"]

    print("\n📌 Missing value check (local staged data):")
    for col in numeric_cols:
        if col not in df_local.columns:
            print(f"  ⚠️ Column '{col}' not found in local data!")
            continue
        missing = df_local[col].isna().sum()
        print(f"  {col}: {missing} missing")

    print("\n📌 Missing value check (Supabase data):")
    for col in numeric_cols:
        if col not in df_sb.columns:
            print(f"  ⚠️ Column '{col}' not found in Supabase table!")
            continue
        missing = df_sb[col].isna().sum()
        print(f"  {col}: {missing} missing")

    # 3. Unique count of rows = original dataset (local)
    local_row_count = len(df_local)
    local_unique_rows = len(df_local.drop_duplicates())

    print("\n📌 Row uniqueness (local staged data):")
    print(f"  Total rows       : {local_row_count}")
    print(f"  Unique rows      : {local_unique_rows}")
    if local_row_count == local_unique_rows:
        print("  ✅ No duplicate rows in local data.")
    else:
        print("  ⚠️ There are duplicate rows in local data.")

    # 4. Row count matches Supabase table
    sb_row_count = len(df_sb)

    print("\n📌 Row count comparison:")
    print(f"  Local staged rows : {local_row_count}")
    print(f"  Supabase rows     : {sb_row_count}")
    if local_row_count == sb_row_count:
        print("  ✅ Row counts match between local and Supabase.")
    else:
        print("  ⚠️ Row counts DO NOT match!")

    # 5. All segments exist (tenure_group, monthly_charge_segment)
    expected_tenure_groups = {"New", "Regular", "Loyal", "Champion"}
    expected_charge_segments = {"Low", "Medium", "High"}

    print("\n📌 Segment coverage (local staged data):")

    if "tenure_group" in df_local.columns:
        actual_tenure_groups = set(df_local["tenure_group"].dropna().unique())
        missing_tenure = expected_tenure_groups - actual_tenure_groups
        print(f"  tenure_group values present  : {actual_tenure_groups}")
        if not missing_tenure:
            print("  ✅ All expected tenure_group segments exist.")
        else:
            print(f"  ⚠️ Missing tenure_group segments: {missing_tenure}")
    else:
        print("  ⚠️ Column 'tenure_group' not found in local data!")

    if "monthly_charge_segment" in df_local.columns:
        actual_charge_segments = set(df_local["monthly_charge_segment"].dropna().unique())
        missing_charge = expected_charge_segments - actual_charge_segments
        print(f"  monthly_charge_segment values present: {actual_charge_segments}")
        if not missing_charge:
            print("  ✅ All expected monthly_charge_segment segments exist.")
        else:
            print(f"  ⚠️ Missing monthly_charge_segment segments: {missing_charge}")
    else:
        print("  ⚠️ Column 'monthly_charge_segment' not found in local data!")

    # 6. Contract codes are only {0,1,2}
    print("\n📌 Contract code validation (local staged data):")
    if "contract_type_code" in df_local.columns:
        unique_codes_local = set(df_local["contract_type_code"].dropna().unique())
        print(f"  contract_type_code (local) unique values: {unique_codes_local}")
        if unique_codes_local.issubset({0, 1, 2}):
            print("  ✅ Local contract_type_code values are valid (subset of {0,1,2}).")
        else:
            print("  ⚠️ Local contract_type_code has invalid values!")

    else:
        print("  ⚠️ Column 'contract_type_code' not found in local data!")

    print("\n📌 Contract code validation (Supabase data):")
    if "contract_type_code" in df_sb.columns:
        unique_codes_sb = set(pd.Series(df_sb["contract_type_code"]).dropna().unique())
        print(f"  contract_type_code (Supabase) unique values: {unique_codes_sb}")
        if unique_codes_sb.issubset({0, 1, 2}):
            print("  ✅ Supabase contract_type_code values are valid (subset of {0,1,2}).")
        else:
            print("  ⚠️ Supabase contract_type_code has invalid values!")
    else:
        print("  ⚠️ Column 'contract_type_code' not found in Supabase data!")

    print("\n🎯 Validation complete.\n")


if __name__ == "__main__":
    validate_data()
