import logging
from typing import Tuple

import pandas as pd

from extract_data import extract_data
from utils.helper_functions import standardize_columns, DuplicateDataError


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def transform_customer_data() -> pd.DataFrame:
    """
    Extract and transform customer data:
    - Standardize column names
    - Convert signup_date to datetime
    - Remove duplicate customers, keeping the latest signup_date
    """

    logger.info("Retrieving customer data...")
    customers_df, _ = extract_data()

    logger.info("Standardizing customer data columns...")
    customers_df = standardize_columns(customers_df)

    logger.info("Converting signup_date to datetime...")
    customers_df["signup_date"] = pd.to_datetime(
        customers_df["signup_date"],
        errors="coerce"
    )

    logger.info("Checking for null values...")
    null_counts = customers_df.isnull().sum()
    logger.info(f"Null value summary:\n{null_counts}")

    logger.info("Removing duplicate customer records...")
    customers_df = (
        customers_df
        .sort_values("signup_date")
        .drop_duplicates(subset=["customer_id"], keep="last")
        .reset_index(drop=True)
    )

    duplicate_count = customers_df.duplicated(subset=["customer_id"]).sum()
    if duplicate_count > 0:
        raise DuplicateDataError(
            f"Duplicate customer_id values found after deduplication: {duplicate_count}"
        )

    logger.info("Customer data transformation completed successfully.")
    return customers_df


if __name__ == "__main__":
    transformed_customers = transform_customer_data()
