# Customer Churn Prediction Analytics for a Telecom Company
# A telecom operator named TeleConnect is facing customer churn (customers leaving the service).
# Their data team wants to analyze customer behavior and produce a high-quality cleaned dataset for future ML modeling.
# You are assigned to build a full ETL pipeline:
# Extract → Transform → Load → Validate → Generate Analytics Summary.

# EXTRACT (extract.py)
# Write a script that:
# Creates folder structure:
# data/raw
# data/staged
# data/processed
# import opendatasets as od
# load the dataset
# data/raw/churn_raw.csv
# Save raw CSV as:data/raw/churn_raw.csv


import os
import pandas as pd

def extract_data():
    # Base folder: project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Create raw/staged/processed folders
    raw_dir = os.path.join(base_dir, "data", "raw")
    staged_dir = os.path.join(base_dir, "data", "staged")
    processed_dir = os.path.join(base_dir, "data", "processed")

    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(staged_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    # ----------- LOAD THE CHURN DATASET ---------------
    # Correct local path
    file_path = r"C:\Users\RISHITHA SRIJA\OneDrive\Documents\23B81A05AS (AI&DS)\Telco_customer\archive (1)\WA_Fn-UseC_-Telco-Customer-Churn.csv"

    df = pd.read_csv(file_path)

    # ----------- SAVE INTO RAW FOLDER -----------------
    raw_output_path = os.path.join(raw_dir, "churn_raw.csv")
    df.to_csv(raw_output_path, index=False)

    print(f"✔ Data extracted and saved at: {raw_output_path}")
    return raw_output_path


if __name__ == "__main__":
    extract_data()
