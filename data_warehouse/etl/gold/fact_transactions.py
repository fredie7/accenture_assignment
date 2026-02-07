import sys
from pathlib import Path
import pandas as pd


def build_fact_transactions():
    # Add parent directory to sys.path
    sys.path.append(str(Path(__file__).resolve().parent.parent / "silver"))

    # Output path for the fact table
    output_path = (
        Path(__file__).resolve().parent.parent.parent
        / "processed_data"
        / "fact_transactions.csv"
    )

    # Load transformed data
    from transform_transactiions_data import transform_transactions_data
    from transform_customers_data import transform_customers_data

    transactions_df = transform_transactions_data()
    customers_df = transform_customers_data()

    # Import dimension tables
    from dim_customers import dim_customer
    from dim_currency import dim_currency
    from dim_category import dim_category
    from dim_dates import dim_date

    # Ensure timestamp is datetime
    transactions_df["timestamp"] = pd.to_datetime(transactions_df["timestamp"])

    # Rename columns for clarity
    transactions_df = transactions_df.rename(columns={
        "timestamp": "transaction_timestamp",
        "amount_eur": "transaction_amount_eur",
        "exchange_rate": "current_exchange_rate",
    })

    # Add a transaction date column
    transactions_df["transaction_date"] = transactions_df["transaction_timestamp"].dt.date

    # High‑value transaction flag (> 500 EUR)
    transactions_df["is_high_value_transaction"] = (
        transactions_df["transaction_amount_eur"] > 500
    ).astype(int)

    # Merge dimension keys into the fact table
    transactions_df = (
        transactions_df
        .merge(
            dim_customer[["customer_id", "customer_key"]],
            on="customer_id",
            how="left",
        )
        .merge(
            dim_currency[["transaction_key", "currency_key"]],
            on="transaction_key",
            how="left",
        )
        .merge(
            dim_category[["transaction_key", "category_key"]],
            on="transaction_key",
            how="left",
        )
        .merge(
            dim_date[["transaction_key", "date_key"]],
            on="transaction_key",
            how="left",
        )
    )

    # Final fact table
    fact_transactions = transactions_df[[
        "transaction_id",
        "transaction_key",
        "customer_id",
        "customer_key",
        "currency_key",
        "category_key",
        "date_key",
        "transaction_timestamp",
        "transaction_amount_eur",
        "current_exchange_rate",
        "is_high_value_transaction",
    ]]

    # Store data as CSV in the processed_data folder for downstream analytics and AI applications
    fact_transactions.to_csv(output_path, index=False)

    return fact_transactions


# Usage & Quality checks
fact_transactions = build_fact_transactions()
print(fact_transactions.head())
print(fact_transactions.columns.tolist())
print(len(fact_transactions))