"""
Loads and prepares business data from the data warehouse.

Responsibilities:
- Read dimension & fact tables
- Apply SCD Type-2 joins
- Filter valid records
- Return analytics-ready dataset
"""

from pathlib import Path
import pandas as pd

def load_business_data():
    # Resolve project root directory
    root_dir = Path(__file__).resolve().parent.parent.parent.parent

    # Load dimension tables
    dim_categories = pd.read_csv(
        root_dir / "data_warehouse/processed_data/dim_categories.csv"
    )
    dim_customers = pd.read_csv(
        root_dir / "data_warehouse/processed_data/dim_customers.csv"
    )
    dim_dates = pd.read_csv(
        root_dir / "data_warehouse/processed_data/dim_dates.csv"
    )
    dim_currencies = pd.read_csv(
        root_dir / "data_warehouse/processed_data/dim_currencies.csv"
    )

    # Load fact table
    fact_transactions = pd.read_csv(
        root_dir / "data_warehouse/processed_data/fact_transactions.csv"
    )

    # Join static dimensions to fact table
    fact = (
        fact_transactions
        .merge(dim_categories, on="category_key", how="left")
        .merge(dim_currencies, on="currency_key", how="left")
        .merge(dim_dates, on="date_key", how="left")
    )

    # Enforce datetime types
    fact["date"] = pd.to_datetime(fact["date"])
    dim_customers["effective_from"] = pd.to_datetime(dim_customers["effective_from"])
    dim_customers["effective_to"] = pd.to_datetime(dim_customers["effective_to"])

    # Join SCD Type-2 customer dimension
    fact = fact.merge(dim_customers, on="customer_id", how="left")

    # Handle open-ended SCD records
    fact["effective_to"] = fact["effective_to"].fillna(
        pd.Timestamp("2099-12-31")
    )

    # Filter records valid at transaction date
    fact = fact[
        (fact["date"] >= fact["effective_from"]) &
        (fact["date"] <= fact["effective_to"])
    ]

    # Clean duplicated / technical columns
    return (
        fact
        .drop(columns=[
            "transaction_timestamp",
            "transaction_timestamp_y",
            "customer_key_x"
        ])
        .rename(columns={
            "transaction_timestamp_x": "transaction_timestamp",
            "customer_key_y": "customer_key"
        })
    )
