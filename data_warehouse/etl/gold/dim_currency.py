import sys
from pathlib import Path

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent / "silver"))

from transform_transactiions_data import transform_transactions_data

transactions_df = transform_transactions_data()

# Create dimension table for currency
dim_currency = (
    transactions_df[["currency"]]
)

dim_currency["currency_key"] = range(1, len(dim_currency) + 1)
dim_currency["base_currency"] = "EUR"
dim_currency["currency_imputed"] = transactions_df["currency_imputed"].notna()
dim_currency['conversion_type'] = dim_currency['currency_imputed'].map(
    lambda x: 'imputed' if x else 'actual'
)
dim_currency["transaction_timestamp"] = transactions_df["timestamp"].iloc[0] if not transactions_df.empty else None
# Keep the original currency as transaction_currency
dim_currency = dim_currency.rename(columns={"currency": "transaction_currency"})

dim_currency["exchange_rate_source"] = "https://www.oanda.com/currency-converter"


# Include the transction surrogate key for future merging with fact table

dim_currency["transaction_key"] = transactions_df["transaction_key"]

dim_currency = dim_currency[
    [
        "currency_key",
        "transaction_key",
        "base_currency",
        "transaction_currency",
        "currency_imputed",
        "conversion_type",
        "transaction_timestamp",
        "exchange_rate_source",
    ]
]
# print(dim_currency.columns.tolist())