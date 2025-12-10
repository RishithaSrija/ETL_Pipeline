# ANALYSIS REPORT (etl_analysis.py)
# Read table from Supabase and generate:
# 📊 Metrics
# Churn percentage
# Average monthly charges per contract
# Count of new, regular, loyal, champion customers
# Internet service distribution
# Pivot table: Churn vs Tenure Group
# Optional visualizations:
# Churn rate by Monthly Charge Segment
# Histogram of TotalCharges
# Bar plot of Contract types
# Save output CSV into:
# data/processed/analysis_summary.csv
 
# etl_analysis.py
# Purpose: Read Telco churn data from Supabase, compute analysis metrics,
#          generate optional plots, and save summary CSV to data/processed.

import os
import pandas as pd
import matplotlib.pyplot as plt
from supabase import create_client, Client
from dotenv import load_dotenv


# ----------------- Helpers -----------------

def get_supabase_client() -> Client:
    """Initialize and return Supabase client using .env (SUPABASE_URL, SUPABASE_KEY)."""
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ SUPABASE_URL or SUPABASE_KEY not set in environment/.env file")

    return create_client(url, key)


def load_supabase_table(table_name: str = "telco_customer_data") -> pd.DataFrame:
    """Load full table from Supabase into a pandas DataFrame."""
    supabase = get_supabase_client()
    res = supabase.table(table_name).select("*").execute()

    if hasattr(res, "data"):
        data = res.data
    else:
        data = res.get("data", [])

    df = pd.DataFrame(data)
    print(f"🌐 Loaded {len(df)} rows from Supabase table '{table_name}'")
    return df


def find_first_existing(df: pd.DataFrame, candidates):
    """Return the first column name from 'candidates' that exists in df, else None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ----------------- Metric Computations -----------------

def compute_analysis_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute:
    - Churn percentage
    - Avg MonthlyCharges per Contract
    - Count of tenure_group segments
    - InternetService distribution
    - Pivot: Churn vs tenure_group
    Return a long-format DataFrame (metric, dimension, category, value).
    """

    summary_rows = []

    # ---- Identify important columns (handle different casing if needed) ----
    churn_col = find_first_existing(df, ["Churn", "churn"])
    monthly_col = find_first_existing(df, ["MonthlyCharges", "monthlycharges"])
    total_col = find_first_existing(df, ["TotalCharges", "totalcharges"])
    contract_col = find_first_existing(df, ["Contract", "contract"])
    tenure_group_col = find_first_existing(df, ["tenure_group"])
    charge_segment_col = find_first_existing(df, ["monthly_charge_segment"])
    internet_col = find_first_existing(df, ["InternetService", "internetservice"])

    # ------------------------------
    # 1. Churn percentage
    # ------------------------------
    if churn_col and churn_col in df.columns:
        churn_series = df[churn_col]

        # Normalize to string for safety
        s = churn_series.astype(str).str.strip().str.upper()
        churned = s.isin(["YES", "Y", "1", "TRUE"])
        churn_rate = churned.mean() * 100 if len(s) > 0 else 0.0

        summary_rows.append({
            "metric": "churn_rate_percent",
            "dimension": "overall",
            "category": "all",
            "value": round(churn_rate, 2)
        })

        print(f"📊 Churn rate: {churn_rate:.2f}%")
    else:
        print("⚠️  Churn column not found. Skipping churn rate metric.")

    # ------------------------------
    # 2. Average monthly charges per contract
    # ------------------------------
    if monthly_col and contract_col:
        df[monthly_col] = pd.to_numeric(df[monthly_col], errors="coerce")
        avg_by_contract = df.groupby(contract_col)[monthly_col].mean()

        for contract_type, avg_val in avg_by_contract.items():
            summary_rows.append({
                "metric": "avg_monthly_charges",
                "dimension": "contract",
                "category": str(contract_type),
                "value": round(float(avg_val), 2) if pd.notnull(avg_val) else None
            })

        print("\n📊 Average MonthlyCharges by Contract:")
        print(avg_by_contract)
    else:
        print("⚠️  MonthlyCharges or Contract column not found. Skipping avg by contract metric.")

    # ------------------------------
    # 3. Count of New / Regular / Loyal / Champion (tenure_group)
    # ------------------------------
    if tenure_group_col and tenure_group_col in df.columns:
        counts_tenure_group = df[tenure_group_col].value_counts(dropna=False)
        for grp, cnt in counts_tenure_group.items():
            summary_rows.append({
                "metric": "customer_count",
                "dimension": "tenure_group",
                "category": str(grp),
                "value": int(cnt)
            })

        print("\n📊 Tenure group distribution:")
        print(counts_tenure_group)
    else:
        print("⚠️  tenure_group column not found. Skipping tenure group counts.")

    # ------------------------------
    # 4. Internet service distribution
    # ------------------------------
    if internet_col and internet_col in df.columns:
        dist_internet = df[internet_col].value_counts(dropna=False)
        for service, cnt in dist_internet.items():
            summary_rows.append({
                "metric": "internet_service_count",
                "dimension": "internet_service",
                "category": str(service),
                "value": int(cnt)
            })

        print("\n📊 InternetService distribution:")
        print(dist_internet)
    else:
        print("⚠️  InternetService column not found. Skipping internet distribution.")

    # ------------------------------
    # 5. Pivot: Churn vs Tenure Group
    # ------------------------------
    if churn_col and tenure_group_col and (churn_col in df.columns) and (tenure_group_col in df.columns):
        pivot_ct = pd.crosstab(df[tenure_group_col], df[churn_col])

        # Add as separate metrics to summary_rows
        for tg in pivot_ct.index:
            for ch_val in pivot_ct.columns:
                summary_rows.append({
                    "metric": "churn_vs_tenure_group_count",
                    "dimension": str(tg),
                    "category": str(ch_val),
                    "value": int(pivot_ct.loc[tg, ch_val])
                })

        print("\n📊 Pivot table: Churn vs Tenure_group")
        print(pivot_ct)
    else:
        print("⚠️  Cannot compute pivot: missing churn or tenure_group column.")

    # Turn summary_rows into DataFrame
    summary_df = pd.DataFrame(summary_rows)
    return summary_df


# ----------------- Optional Plots -----------------

def generate_plots(df: pd.DataFrame, output_dir: str):
    """
    Optional visualizations:
    - Churn rate by Monthly Charge Segment
    - Histogram of TotalCharges
    - Bar plot of Contract types
    """

    os.makedirs(output_dir, exist_ok=True)

    churn_col = find_first_existing(df, ["Churn", "churn"])
    charge_segment_col = find_first_existing(df, ["monthly_charge_segment"])
    total_col = find_first_existing(df, ["TotalCharges", "totalcharges"])
    contract_col = find_first_existing(df, ["Contract", "contract"])

    # 1) Churn rate by Monthly Charge Segment
    if churn_col and charge_segment_col:
        s = df[churn_col].astype(str).str.strip().str.upper()
        churned = s.isin(["YES", "Y", "1", "TRUE"]).astype(int)

        df_temp = df.copy()
        df_temp["churn_flag"] = churned

        churn_rate_by_seg = df_temp.groupby(charge_segment_col)["churn_flag"].mean()

        plt.figure()
        churn_rate_by_seg.plot(kind="bar")
        plt.title("Churn Rate by Monthly Charge Segment")
        plt.xlabel("Monthly Charge Segment")
        plt.ylabel("Churn Rate")
        plt.tight_layout()
        plot_path = os.path.join(output_dir, "churn_rate_by_charge_segment.png")
        plt.savefig(plot_path)
        plt.close()
        print(f"📈 Saved plot: {plot_path}")
    else:
        print("⚠️  Skipping 'Churn Rate by Monthly Charge Segment' plot (missing columns).")

    # 2) Histogram of TotalCharges
    if total_col and total_col in df.columns:
        df[total_col] = pd.to_numeric(df[total_col], errors="coerce")

        plt.figure()
        df[total_col].dropna().hist(bins=30)
        plt.title("Histogram of TotalCharges")
        plt.xlabel("TotalCharges")
        plt.ylabel("Frequency")
        plt.tight_layout()
        plot_path = os.path.join(output_dir, "hist_total_charges.png")
        plt.savefig(plot_path)
        plt.close()
        print(f"📈 Saved plot: {plot_path}")
    else:
        print("⚠️  Skipping 'Histogram of TotalCharges' plot (missing column).")

    # 3) Bar plot of Contract types
    if contract_col and contract_col in df.columns:
        plt.figure()
        df[contract_col].value_counts().plot(kind="bar")
        plt.title("Contract Type Distribution")
        plt.xlabel("Contract Type")
        plt.ylabel("Count")
        plt.tight_layout()
        plot_path = os.path.join(output_dir, "bar_contract_types.png")
        plt.savefig(plot_path)
        plt.close()
        print(f"📈 Saved plot: {plot_path}")
    else:
        print("⚠️  Skipping 'Contract types' bar plot (missing column).")


# ----------------- Main -----------------

def main():
    # Base directory (project root)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed_dir = os.path.join(base_dir, "data", "processed")
    os.makedirs(processed_dir, exist_ok=True)

    # 1. Load data from Supabase
    df = load_supabase_table("telco_customer_data")

    # 2. Compute metrics
    summary_df = compute_analysis_metrics(df)

    # 3. Save summary CSV
    summary_path = os.path.join(processed_dir, "analysis_summary.csv")
    summary_df.to_csv(summary_path, index=False)
    print(f"\n💾 Analysis summary saved to: {summary_path}")

    # 4. Optional plots
    generate_plots(df, processed_dir)

    print("\n✅ ETL analysis finished.\n")


if __name__ == "__main__":
    main()
