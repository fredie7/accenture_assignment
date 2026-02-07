import sys
from pathlib import Path
import pandas as pd

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent / "silver"))

from transform_transactiions_data import transform_transactions_data

transactions_df = transform_transactions_data()

# Create a dimension table for the date for time series analysis
dim_date = pd.DataFrame({
    "date": transactions_df["timestamp"].dt.date.unique()
})

dim_date["date_key"] = range(1, len(dim_date) + 1)
dim_date["transaction_day"] = pd.to_datetime(dim_date["date"]).dt.day
dim_date["transaction_month"] = pd.to_datetime(dim_date["date"]).dt.month
dim_date["transaction_year"] = pd.to_datetime(dim_date["date"]).dt.year
dim_date["transaction_weekday"] = pd.to_datetime(dim_date["date"]).dt.strftime("%a")
dim_date["transaction_timestamp"] = transactions_df["timestamp"].iloc[0] if not transactions_df.empty else None

# Include the transction surrogate key for future merging with fact table
dim_date["transaction_key"] = transactions_df["transaction_key"]

dim_date = dim_date[["date_key","transaction_key", "date", "transaction_day", "transaction_month", "transaction_year", "transaction_weekday", "transaction_timestamp"]]
# print(dim_date["transaction_day"].head())