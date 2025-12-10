# TRANSFORM (transform.py)
# must perform advanced transformations, not just cleaning.
# ✔ Cleaning Tasks
# Convert "TotalCharges" to numeric (dataset has spaces → become NaN).
# Fill missing numeric values using:
# Median for tenure, MonthlyCharges, TotalCharges.
# Replace missing categorical values with "Unknown".
# ✔ Feature Engineering
# Create the following new columns:
# 1. tenure_group
# Based on tenure months:
# 0–12   → "New"
# 13–36  → "Regular"
# 37–60  → "Loyal"
# 60+    → "Champion"
# 2. monthly_charge_segment
# MonthlyCharges < 30  → "Low"
# 30–70              → "Medium"
# > 70                 → "High"
# 3. has_internet_service
# Convert InternetService column:
# "DSL" / "Fiber optic" → 1
# "No" → 0
# 4. is_multi_line_user
# 1 if MultipleLines == "Yes"
# 0 otherwise
# 5. contract_type_code
# Map:
# Month-to-month → 0
# One year      → 1
# Two year      → 2
# ✔ Drop unnecessary fields
# Remove:
# customerID, gender
# ✔ Save output to:
# data/staged/churn_transformed.csv
import pandas as pd
import os
import numpy as np

import os
import pandas as pd

# BASE DIRECTORY (your project folder)
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
raw_path = os.path.join(base_dir, "data", "raw", "churn_raw.csv")

# Read CSV
df = pd.read_csv(raw_path)
print("✔ File loaded from:", raw_path)

df['TotalCharges']=pd.to_numeric(df['TotalCharges'],errors='coerce')

num_col=['tenure','MonthlyCharges','TotalCharges']
for col in num_col:
    df[col]=df[col].fillna(df[col].median)

cat_cols = ['gender','Partner','Dependents','PhoneService','MultipleLines',
            'InternetService','OnlineSecurity','OnlineBackup','DeviceProtection',
            'TechSupport','StreamingTV','StreamingMovies','Contract',
            'PaperlessBilling','PaymentMethod','Churn']

for col in cat_cols:
    df[col] = df[col].fillna("Unknown")
# df.tail()


df['tenure'] = pd.to_numeric(df['tenure'], errors='coerce')

df['tenure_group'] = pd.cut(
    df['tenure'],
    bins=[-0.1, 12, 36, 60, np.inf],         # -0.1 so that 0 is included
    labels=['New', 'Regular', 'Loyal', 'Champion']
)


df['MonthlyCharges']=pd.to_numeric(df['MonthlyCharges'],errors='coerce')

df['monthly_charge_segment']=pd.cut(
    df['MonthlyCharges'],
    bins=[-0.1,30,70,np.inf],
    labels=['Low','Medium','High']
)

df['InternetService']=df['InternetService'].astype(str).str.strip()
df['has_internet_service']=df['InternetService'].map({
    'DSL':1,
    'Fiber optic':1,
    'No':0

}).fillna(0).astype(int)

df['MultipleLines'] = df['MultipleLines'].astype(str).str.strip()

df['is_multi_line_user'] = (df['MultipleLines'] == 'Yes').astype(int)


df['Contract'] = df['Contract'].astype(str).str.strip()

df['contract_type_code'] = df['Contract'].map({
    'Month-to-month': 0,
    'One year': 1,
    'Two year': 2
})
# print(df[['tenure','tenure_group',
#           'MonthlyCharges','monthly_charge_segment',
#           'InternetService','has_internet_service',
#           'MultipleLines','is_multi_line_user',
#           'Contract','contract_type_code']].head())
cols_to_drop = ['customerID', 'gender']

df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

# ✔ Save output to:
# data/staged/churn_transformed.csv
# Save output to data/staged/churn_transformed.csv
staged_dir = os.path.join(base_dir, "data", "staged")
os.makedirs(staged_dir, exist_ok=True)
staged_path=os.path.join(staged_dir,'churn_transformed.csv')
df.to_csv(staged_path,index=False)
print(f"✔ File saved successfully at: {staged_path}")