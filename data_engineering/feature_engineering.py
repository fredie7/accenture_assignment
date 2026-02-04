import logging
from transform_customers_data import transform_customers_data
from transform_transactiions_data import transform_transactions_data

import pandas as pd

logger = logging.getLogger(__name__)

transactions = transform_transactions_data()
customers = transform_customers_data()

# Left join to not lose track of any transactions for analyzing customer behaviour, fraud detection, churn rate
business_data = transactions.merge(
    customers,
    on="customer_id",
    how="left"
)
# print(business_data.head())
print(business_data["country"].value_counts())

transactions_data = transactions

logger.info("Retrieving transaction frequency per customer...")

# Calculate customer life time value for New vs Loyal customers, personalized marketing campaigns & fraud detection
business_data["days_since_signup"] = (
    business_data["signup_date"] - business_data["timestamp"]
).dt.days.abs()
business_data_clv = business_data.sort_values("days_since_signup", ascending=False)
clv = business_data_clv[["customer_id", "days_since_signup"]]
print(clv.head())

# Customer spend analysis for targeted marketing campaigns and loyalty programs
customer_spend = (
    business_data
    .groupby("customer_id")
    .agg(
        transacton_count = ("transaction_id", "count"),
        avg_transaction_value_eur = ("amount_eur", "mean"),
        maximum_transaction_value_eur = ("amount_eur", "max"),
    )
    .reset_index()
    .round(2)
)
print(customer_spend.head())

# Cross-border transaction analysis for compliance review, fraud detection and risk assessment
cross_border_transactions = business_data[
    business_data["country"].notna() &
    (business_data["country"] != "unknown")
]
cross_border_summary = (
    cross_border_transactions
    .groupby("country")
    .agg(
        total_transactions = ("transaction_id", "count"),
        total_amount_eur = ("amount_eur", "sum"),
    )
    .reset_index()
    .round(2)
    .sort_values("total_amount_eur", ascending=False)
)
print(cross_border_summary.head())