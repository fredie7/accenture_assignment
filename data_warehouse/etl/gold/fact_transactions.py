import sys
from pathlib import Path
import pandas as pd


# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent / "silver"))


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


# Rename early so the rest of the code uses the new name
transactions_df = transactions_df.rename(columns={
    "timestamp": "transaction_timestamp",
    "amount_eur": "transaction_amount_eur",
    "exchange_rate": "current_exchange_rate"
})


transactions_df["transaction_date"] = transactions_df["transaction_timestamp"].dt.date

# Merge dimension keys into the fact table
transactions_df = (
    transactions_df
    .merge(
        dim_customer[["customer_id", "customer_key"]],
        on="customer_id",
        how="left"
    )
    .merge(
        dim_currency[["transaction_key", "currency_key"]],
        on="transaction_key",
        how="left"
    )
    .merge(
        dim_category[["transaction_key", "category_key"]],
        on="transaction_key",
        how="left"
    )
    .merge(
        dim_date[["transaction_key", "date_key"]],
        on="transaction_key",
        how="left"
    )
)


# High‑value transaction flag (example > 500 EUR)
transactions_df["is_high_value_transaction"] = (
    transactions_df["transaction_amount_eur"] > 500
).astype(int)


# Time‑based features
transactions_df["transaction_hour"] = transactions_df["transaction_timestamp"].dt.hour
transactions_df["transaction_day_of_week"] = transactions_df["transaction_timestamp"].dt.dayofweek
transactions_df["transaction_is_weekend"] = transactions_df["transaction_day_of_week"] >= 5


# Fact table
fact_transactions = transactions_df[[
    "transaction_id",
    "customer_id",
    "customer_key",
    "currency_key",
    "category_key",
    "date_key",
    "transaction_timestamp",
    "transaction_amount_eur",
    "current_exchange_rate",
    "is_high_value_transaction"
]]


print(fact_transactions.head())
