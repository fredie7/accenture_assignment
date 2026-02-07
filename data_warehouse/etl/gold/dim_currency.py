import sys
from pathlib import Path
import pandas as pd


# Add parent directory to sys.path at module level
sys.path.append(str(Path(__file__).resolve().parent.parent / "silver"))


# Output path for the dimension table
output_path = (
    Path(__file__).resolve().parent.parent.parent
    / "processed_data"
    / "dim_currencies.csv"
)


def build_dim_currency(transform_transactions_fn):
    # Prepare transactions
    transactions_df = transform_transactions_fn()
    if transactions_df.empty:
        return pd.DataFrame(
            columns=[
                "currency_key",
                # "transaction_key",
                "base_currency",
                "transaction_currency",
                "currency_imputed",
                "conversion_type",
                "transaction_timestamp",
                "exchange_rate_source",
            ]
        )

    # Create dimension table for currency
    dim_currency = (
        transactions_df[["transaction_currency"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim_currency["currency_key"] = range(1, len(dim_currency) + 1)
    dim_currency["base_currency"] = "EUR"
    dim_currency["currency_imputed"] = transactions_df["currency_imputed"].notna()

    dim_currency["conversion_type"] = dim_currency["currency_imputed"].map(
        lambda x: "imputed" if x else "actual"
    )

    # Propagate transaction timestamp and key
    first_timestamp = transactions_df["transaction_timestamp"].iloc[0] if not transactions_df.empty else None
    dim_currency["transaction_timestamp"] = first_timestamp
    # dim_currency["transaction_key"] = transactions_df["transaction_key"].iloc[0] if not transactions_df.empty else None

    # Keep the original currency as transaction_currency
    # dim_currency = dim_currency.rename(columns={"currency": "transaction_currency"})

    dim_currency["exchange_rate_source"] = "https://www.oanda.com/currency-converter"

    # Final column order
    dim_currency = dim_currency[
        [
            "currency_key",
            # "transaction_key",
            "base_currency",
            "transaction_currency",
            "currency_imputed",
            "conversion_type",
            "transaction_timestamp",
            "exchange_rate_source",
        ]
    ]

    # Save dimension table to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dim_currency.to_csv(output_path, index=False)
    # assert dim_currency["transaction_currency"].is_unique
    return dim_currency


# Usage & Quality checks
from transform_transactiions_data import transform_transactions_data

dim_currency = build_dim_currency(transform_transactions_data)
print(dim_currency.head())
print(len(dim_currency))
print(dim_currency.info())
