import sys
from pathlib import Path
import pandas as pd


def build_dim_date():
    # Add parent directory to sys.path
    sys.path.append(str(Path(__file__).resolve().parent.parent / "silver"))

    from transform_transactiions_data import transform_transactions_data

    output_path = (
        Path(__file__).resolve().parent.parent.parent
        / "processed_data"
        / "dim_dates.csv"
    )

    # Load and prepare transactions
    transactions_df = transform_transactions_data()
    if transactions_df.empty:
        return pd.DataFrame(
            columns=[
                "date_key",
                "transaction_key",
                "date",
                "transaction_day",
                "transaction_month",
                "transaction_year",
                "transaction_weekday",
                "transaction_timestamp",
            ]
        )

    # Create dimension table for date for time series analysis
    dim_date = pd.DataFrame({
        "date": transactions_df["timestamp"]
    })

    dim_date["date_key"] = range(1, len(dim_date) + 1)
    dim_date["transaction_day"] = pd.to_datetime(dim_date["date"]).dt.day
    dim_date["transaction_month"] = pd.to_datetime(dim_date["date"]).dt.month
    dim_date["transaction_year"] = pd.to_datetime(dim_date["date"]).dt.year
    dim_date["transaction_weekday"] = pd.to_datetime(dim_date["date"]).dt.strftime("%a")

    # Propagate transaction timestamp and key
    first_timestamp = (
        transactions_df["timestamp"]
    )
    dim_date["transaction_timestamp"] = first_timestamp
    dim_date["transaction_key"] = (
        transactions_df["transaction_key"]
    )

    # Final column order
    dim_date = dim_date[
        [
            "date_key",
            "transaction_key",
            "date",
            "transaction_day",
            "transaction_month",
            "transaction_year",
            "transaction_weekday",
            "transaction_timestamp",
        ]
    ]

    # Save dimension table to CSV
    dim_date.to_csv(output_path, index=False)

    # Return the dimension date table
    
    return dim_date


# Usage
dim_date = build_dim_date()
print(dim_date.head())
print(len(dim_date))