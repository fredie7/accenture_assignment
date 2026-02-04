import logging
import pandas as pd

from extract_data import extract_data
from utils.helper_functions import (
    standardize_columns,
    EXCHANGE_RATES,
    DuplicateDataError,
)

logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)


def transform_transactions_data() -> pd.DataFrame:
    """
    Extract and transform transactions data:
    - Standardize columns
    - Deduplicate transactions
    - Clean and impute missing values
    - Normalize currencies to EUR
    """

    logger.info("Retrieving transactions data...")
    _, transactions_df = extract_data()
    logger.info(f"Initial transactions shape: {transactions_df.shape}")
    
    logger.info("Standardizing column names...")
    transactions_df = standardize_columns(transactions_df)

    logger.info("Converting timestamp to datetime...")
    transactions_df["timestamp"] = pd.to_datetime(
        transactions_df["timestamp"],
        errors="coerce"
    )

    logger.info("Converting customer_id to Int64...")
    transactions_df["customer_id"] = transactions_df["customer_id"].astype("Int64")

    # --------------------------------------------------
    # Duplicate handling
    # --------------------------------------------------
    logger.info("Checking for duplicate transaction_id values...")
    duplicate_count = transactions_df.duplicated("transaction_id").sum()

    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate transaction_id records")

        dup_df = transactions_df[transactions_df.duplicated("transaction_id", keep=False)]

        identical_timestamps = (
            dup_df
            .groupby("transaction_id")["timestamp"]
            .nunique()
            .eq(1)
            .any()
        )

        if identical_timestamps:
            logger.info(
                "Some duplicate transaction_id entries share identical timestamps "
                "(likely pipeline retries)"
            )

        logger.info(
            "Dropping duplicate transaction_id records, keeping the latest timestamp"
        )
        transactions_df = (
            transactions_df
            .sort_values("timestamp")
            .drop_duplicates(subset=["transaction_id"], keep="last")
            .reset_index(drop=True)
        )

    remaining_duplicates = transactions_df.duplicated("transaction_id").sum()
    if remaining_duplicates > 0:
        raise DuplicateDataError(
            f"Duplicate transaction_id values remain after deduplication: "
            f"{remaining_duplicates}"
        )

    # --------------------------------------------------
    # Null handling & imputations
    # --------------------------------------------------
    logger.info("Dropping records with null customer_id...")
    transactions_df = transactions_df.dropna(subset=["customer_id"])

    logger.info("Imputing missing currency values...")
    transactions_df["currency_imputed"] = transactions_df["currency"].isna()
    transactions_df["currency"] = (
        transactions_df["currency"]
        .fillna("EUR")
        .str.strip()
        .str.upper()
    )

    logger.info("Imputing missing category values...")
    transactions_df["category"] = transactions_df["category"].fillna("unknown")

    # --------------------------------------------------
    # Currency normalization
    # --------------------------------------------------
    logger.info("Mapping exchange rates...")
    transactions_df["exchange_rate"] = transactions_df["currency"].map(EXCHANGE_RATES)

    logger.info("Calculating amount_eur...")
    transactions_df["amount_eur"] = (
        transactions_df["amount"] * transactions_df["exchange_rate"]
    ).round(2)

    null_summary = transactions_df.isnull().sum()
    logger.info(f"Final null value summary:\n{null_summary}")

    logger.info(f"Final transactions shape: {transactions_df.shape}")
    logger.info("Transactions data transformation completed successfully.")
    
    return transactions_df


if __name__ == "__main__":
    transformed_transactions = transform_transactions_data()
