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


def scd2_upsert_customer(dim_customer: pd.DataFrame,
                         stg_customers: pd.DataFrame) -> pd.DataFrame:
    """
    Perform SCD Type 2 upsert for customer dimension.
    """

    today = pd.Timestamp.today().normalize()

    # Ensure staging has unique business keys
    stg_customers = stg_customers.drop_duplicates(subset=["customer_id"])

    # ------------------------------------------------------------------
    # CASE 1: Empty dimension → insert all staging rows
    # ------------------------------------------------------------------
    if dim_customer.empty:
        inserts = stg_customers.copy()

        inserts["customer_key"] = range(1, len(inserts) + 1)
        inserts["effective_from"] = today
        inserts["effective_to"] = pd.NaT
        inserts["is_current"] = True

        result = inserts[[
            "customer_key",
            "customer_id",
            "email",
            "country",
            "signup_date",
            "effective_from",
            "effective_to",
            "is_current"
        ]]

        # ------------------ TESTS ------------------
        assert result["customer_key"].is_unique, "Customer keys are not unique"
        assert result["is_current"].all(), "Initial rows must be current"
        assert result["effective_to"].isna().all(), "Initial rows must be open-ended"
        # -------------------------------------------

        return result

    # ------------------------------------------------------------------
    # CASE 2: Incremental SCD2 logic
    # ------------------------------------------------------------------
    dim_current = dim_customer[dim_customer["is_current"]].copy()

    merged = stg_customers.merge(
        dim_current,
        on="customer_id",
        how="left",
        suffixes=("_stg", "_dim")
    )

    # Identify new customers
    is_new = merged["customer_key"].isna()

    # NULL-safe change detection
    email_changed = (
        merged["email_stg"].fillna("") != merged["email_dim"].fillna("")
    )

    country_changed = (
        merged["country_stg"].fillna("") != merged["country_dim"].fillna("")
    )

    is_changed = ~is_new & (email_changed | country_changed)

    # ------------------------------------------------------------------
    # Expire existing records
    # ------------------------------------------------------------------
    keys_to_expire = merged.loc[is_changed, "customer_key"]

    if not keys_to_expire.empty:
        dim_customer.loc[
            dim_customer["customer_key"].isin(keys_to_expire),
            ["effective_to", "is_current"]
        ] = [today, False]

        # ------------------ TESTS ------------------
        expired = dim_customer.loc[
            dim_customer["customer_key"].isin(keys_to_expire)
        ]
        assert not expired["is_current"].any(), "Expired rows still marked current"
        assert expired["effective_to"].notna().all(), "Expired rows missing end date"
        # -------------------------------------------

    # ------------------------------------------------------------------
    # Prepare inserts (new + changed)
    # ------------------------------------------------------------------
    inserts = merged.loc[is_new | is_changed, [
        "customer_id",
        "email_stg",
        "country_stg",
        "signup_date_stg"
    ]].rename(columns={
        "email_stg": "email",
        "country_stg": "country",
        "signup_date_stg": "signup_date"
    })
    assert "signup_date_stg" in merged.columns, "signup_date missing after merge"

    if inserts.empty:
        return dim_customer

    next_key = dim_customer["customer_key"].max() + 1

    inserts["customer_key"] = range(next_key, next_key + len(inserts))
    inserts["effective_from"] = today
    inserts["effective_to"] = pd.NaT
    inserts["is_current"] = True

    dim_customer = pd.concat([dim_customer, inserts], ignore_index=True)

    # ------------------ TESTS ------------------
    current_rows = dim_customer[dim_customer["is_current"]]

    assert current_rows["customer_id"].is_unique, \
        "More than one current row per customer detected"

    assert dim_customer["customer_key"].is_unique, \
        "Customer key uniqueness violated"

    assert dim_customer.loc[
        dim_customer["is_current"], "effective_to"
    ].isna().all(), "Current rows must have NULL effective_to"
    # -------------------------------------------

    return dim_customer



try:
    dim_customer = scd2_upsert_customer(dim_customer, customers_df)
    print("✅ SCD Type 2 tests passed")
    print(dim_customer.sort_values(
    ["customer_id", "effective_from"]
))

except AssertionError as e:
    print("❌ SCD Type 2 test failed:")
    print(e)



