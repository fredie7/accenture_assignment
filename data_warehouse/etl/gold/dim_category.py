import sys
from pathlib import Path

# Add parent directory to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent / "silver"))

from transform_transactiions_data import transform_transactions_data

transactions_df = transform_transactions_data()

# Create dimension table for category
dim_category = (
    transactions_df[["category"]]
)

dim_category["category_key"] = range(1, len(dim_category) + 1)
dim_category['is_refundable'] = dim_category['category'].map({
    'Electronics': True,
    'Food': False
}).fillna(False)

dim_category['return_window_days'] = dim_category['category'].map({
    'Electronics': 30,
    'Food': 0
}).fillna(0)
dim_category["transaction_timestamp"] = transactions_df["timestamp"].iloc[0] if not transactions_df.empty else None
dim_category = dim_category[["category_key", "category","is_refundable","return_window_days", "transaction_timestamp"]]
# Include the transction surrogate key for future merging with fact table
dim_category["transaction_key"] = transactions_df["transaction_key"]
# print(dim_category.head())