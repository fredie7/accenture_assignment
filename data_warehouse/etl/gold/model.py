import sys
from pathlib import Path

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent / "silver"))

import logging
from transform_transactiions_data import transform_transactions_data
from transform_customers_data import transform_customers_data

import pandas as pd

logger = logging.getLogger(__name__)

transactions_df = transform_transactions_data()
customers_df = transform_customers_data()

# Create the fact table
# High-value transaction flag (example > 500 EUR)
transactions_df['high_value_transactions'] = (transactions_df['amount_eur'] > 500).astype(int)

# Time-based features
transactions_df['transaction_hour'] = transactions_df['timestamp'].dt.hour
transactions_df['transaction_day_of_week'] = transactions_df['timestamp'].dt.dayofweek
transactions_df['transaction_is_weekend'] = transactions_df['transaction_day_of_week'] >= 5

# Fact table
fact_transactions = transactions_df[[
    'transaction_id',
    'customer_id',
    'timestamp',
    'amount_eur',
    'exchange_rate',
    'high_value_transactions',
    'transaction_hour',
    'transaction_day_of_week',
    'transaction_is_weekend'
]].copy()

# Create a dimension table for the date for time series analysis
dim_date = pd.DataFrame({
    "date": transactions_df["timestamp"].dt.date.unique()
})

dim_date["date_key"] = range(1, len(dim_date) + 1)
dim_date["day"] = pd.to_datetime(dim_date["date"]).dt.day
dim_date["month"] = pd.to_datetime(dim_date["date"]).dt.month
dim_date["year"] = pd.to_datetime(dim_date["date"]).dt.year
dim_date["weekday"] = pd.to_datetime(dim_date["date"]).dt.weekday

# Create dimension table for currency
dim_currency = (
    transactions_df[["currency"]]
)

dim_currency["currency_key"] = range(1, len(dim_currency) + 1)
dim_currency["currency_imputed"] = transactions_df["currency_imputed"]
dim_currency = dim_currency[["currency_key", "currency","currency_imputed"]]


# Create dimension table for category
dim_category = (
    transactions_df[["category"]]
)

dim_category["category_key"] = range(1, len(dim_category) + 1)
dim_category = dim_category[["category_key", "category"]]


# Create dimension table for customer
dim_customer = pd.DataFrame(columns=[
    "customer_key",
    "customer_id",
    "email",
    "country",
    "signup_date",
    "effective_from",
    "effective_to",
    "is_current"
])

def scd2_upsert_customer(dim_customer, stg_customers):
    today = pd.Timestamp.today().normalize()

    # -------------------------
    # CASE 1: Empty dimension → all rows are new
    # -------------------------
    if dim_customer.empty:
        inserts = stg_customers.copy()

        inserts = inserts.assign(
            customer_key=range(1, len(inserts) + 1),
            effective_from=today,
            effective_to=pd.NaT,
            is_current=True
        )

        return inserts[[
            "customer_key",
            "customer_id",
            "email",
            "country",
            "signup_date",
            "effective_from",
            "effective_to",
            "is_current"
        ]]

    # -------------------------
    # CASE 2: Normal SCD2 logic
    # -------------------------
    dim_current = dim_customer[dim_customer["is_current"]].copy()

    merged = stg_customers.merge(
        dim_current,
        on="customer_id",
        how="left",
        suffixes=("_stg", "_dim")
    )

    is_new = merged["customer_key"].isna()

    is_changed = (
        ~is_new &
        (
            (merged["email_stg"] != merged["email_dim"]) |
            (merged["country_stg"] != merged["country_dim"])
        )
    )

    # Expire changed records
    dim_customer.loc[
        dim_customer["customer_key"].isin(merged.loc[is_changed, "customer_key"]),
        ["effective_to", "is_current"]
    ] = [today, False]

    # Prepare inserts
    inserts = merged.loc[is_new | is_changed, [
        "customer_id",
        "email_stg",
        "country_stg",
        "signup_date"
    ]].rename(columns={
        "email_stg": "email",
        "country_stg": "country"
    })

    if not inserts.empty:
        next_key = dim_customer["customer_key"].max() + 1

        inserts = inserts.assign(
            customer_key=range(next_key, next_key + len(inserts)),
            effective_from=today,
            effective_to=pd.NaT,
            is_current=True
        )

        dim_customer = pd.concat([dim_customer, inserts], ignore_index=True)

    return dim_customer


dim_customer = scd2_upsert_customer(dim_customer, customers_df)

# print(dim_customer.shape)
# print(dim_customer.head())
# print("=*30")
# print(dim_customer.loc[dim_customer["customer_id"] == 2386])

# 2️⃣ Then put your test code below
dim_customer = pd.DataFrame(columns=[
    "customer_key",
    "customer_id",
    "email",
    "country",
    "signup_date",
    "effective_from",
    "effective_to",
    "is_current"
])

stg_day1 = pd.DataFrame({
    "customer_id": [1, 2],
    "email": ["a@gmail.com", "b@gmail.com"],
    "country": ["FI", "SE"],
    "signup_date": ["2024-01-01", "2024-01-02"]
})




dim_customer = scd2_upsert_customer(dim_customer, stg_day1)
print("==>",dim_customer)

expired = dim_customer[~dim_customer["is_current"]]
assert expired["effective_to"].notna().all()



# print(dim_customer.columns)
