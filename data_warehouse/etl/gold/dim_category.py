import sys
from pathlib import Path
import pandas as pd


def build_dim_category():
    # Add parent directory to sys.path
    sys.path.append(str(Path(__file__).resolve().parent.parent / "silver"))

    # Load transactions
    from transform_transactiions_data import transform_transactions_data

    # Output path for the fact table
    output_path = (
        Path(__file__).resolve().parent.parent.parent
        / "processed_data"
        / "dim_categories.csv"
    )

    # Prepare transactions
    transactions_df = transform_transactions_data()
    if transactions_df.empty:
        return pd.DataFrame(
            columns=[
                "category_key",
                "category",
                "is_refundable",
                "return_window_days",
                "transaction_timestamp",
                "transaction_key",
            ]
        )

    # Create dimension table for category
    dim_category = (
        transactions_df[["category"]]
    )

    dim_category["category_key"] = range(1, len(dim_category) + 1)

    # Refundability rules
    dim_category["is_refundable"] = dim_category["category"].map(
        {
            "Electronics": True,
            "Food": False,
        }
    ).fillna(False)

    # Return window rules
    dim_category["return_window_days"] = dim_category["category"].map(
        {
            "Electronics": 30,
            "Food": 0,
        }
    ).fillna(0)

    # Propagate transaction timestamp and key for later merging
    first_timestamp = (
        transactions_df["timestamp"].iloc[0] if not transactions_df.empty else None
    )
    dim_category["transaction_timestamp"] = first_timestamp
    dim_category["transaction_key"] = (
        transactions_df["transaction_key"]
    )

    # Final column order
    dim_category = dim_category[
        [
            "category_key",
            "transaction_key",
            "category",
            "is_refundable",
            "return_window_days",
            "transaction_timestamp",
        ]
    ]

    # Save dimension table to CSV
    dim_category.to_csv(output_path, index=False)
    
    # Return the dimension category table
    return dim_category


# Quality checks
dim_category = build_dim_category()
print(dim_category.head(2))
print(len(dim_category))
print(dim_category["transaction_key"].nunique())
