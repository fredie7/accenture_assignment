# Import dependencies
from data_extraction import extract_data
from utils.helper import standardize_columns, EXCHANGE_RATES, DuplicateDataError

import pandas as pd

def transform_transactions_data():
    """Transform customer data"""

    print("Retrieving transactions data...")
    _, transactions_df =  extract_data()
    print("Displaying shape of transactions_df:", transactions_df.shape)
    # customer_info = customers_df.info()
    print("=====Processing Transactions Data=====")

    
    print("=====Standardizing transactions data column=====")
    transactions_df = standardize_columns(transactions_df)
    
    print("=====Displaying Transactions data info=====")
    print(transactions_df.info())
    
    print("=====Converting timestamp to datetime=====")
    transactions_df["timestamp"] = pd.to_datetime(transactions_df["timestamp"])

    print("=====Converting customer_id to Int64=====")
    transactions_df["customer_id"] = transactions_df["customer_id"].astype("Int64")

    print("=====Double-checking data type for timestamp and customer_id=====")
    print(transactions_df.info())
    # print(transactions_df.head())

    print("=====Checking for duplicate records on transaction_id=====")
    duplicate_transactions = transactions_df.duplicated(subset=["transaction_id"]).sum()
    if(duplicate_transactions == 0):
        print("=====No duplicate records on transaction_id=====")
    else:
        print(f"=====Found {duplicate_transactions} duplicate records on transaction_id=====")
        print("=====Investigating duplicate records on transaction_id for repeat entries due to pipeline retrys=====")
        duplicate_transactions = transactions_df[transactions_df.duplicated("transaction_id", keep=False)]
        print(duplicate_transactions)
        print("=====Check if any duplicates have identical timestamps (possible retried rows)=====")
        if (
            duplicate_transactions
            .groupby("transaction_id")["timestamp"]
            .nunique()
            .eq(1)
            .any()
        ):
            print("=====Some duplicate transaction_id entries have identical timestamps (possible pipeline retries)=====")
        else:
            print("=====Duplicate transaction_id entries have different timestamps (likely not simple retries)=====")

            print("=====Dropping duplicate records on transaction_id while retaining the latest timestamp due to late arrivals=====")
            transactions_df = (
                transactions_df
                .sort_values("timestamp")
                .drop_duplicates(subset=["transaction_id"], keep="last")
            )

    
    try:
        print("=====Double Checking for duplicate records on transaction_id after dropping duplicates=====")
        duplicate_transactions = transactions_df.duplicated(subset=["transaction_id"]).sum()

        if duplicate_transactions == 0:
            print("=====No duplicate records on transaction_id=====")
        else:
            print(f"=====Found {duplicate_transactions} duplicate records on transaction_id=====")
            raise DuplicateDataError(f"Duplicate records found on transaction_id after dropping duplicates: {duplicate_transactions}")
    except DuplicateDataError as e:
        print(f"Data validation error: {e}")
   

    
    print("=====Double-checking for null values=====")
    print(transactions_df.isnull().sum())
    
    print("=====Dropping records with null customer_id=====")
    transactions_df = transactions_df.dropna(subset=["customer_id"])
    
    print("=====Imputing missing currency with 'EUR' and creating currency_imputed flag for lineage tracking=====")
    transactions_df["currency_imputed"] = transactions_df["currency"].isna()
    transactions_df["currency"] = transactions_df["currency"].fillna("EUR")
    
    print("=====Imputing missing category with 'unknown'=====")
    transactions_df["category"] = transactions_df["category"].fillna("unknown")

    print("=====Checking currency denominations=====")
    print(transactions_df["currency"].value_counts())
    
    print("=====Standardizing currency values to uppercase and stripping them of white spaces=====")
    transactions_df["currency"] = transactions_df["currency"].str.strip().str.upper()
    print(transactions_df["currency"].value_counts())

    print("=====Creating exchange rate column for lineage tracking=====")
    transactions_df["exchange_rate"] = transactions_df["currency"].map(EXCHANGE_RATES)
    
    print("=====Creating amount_eur column to standardize all currency values to 'EUR'=====")
    transactions_df["amount_eur"] = round(transactions_df["amount"] * transactions_df["exchange_rate"], 2)
    
    
    print("=====Double-checking for null values=====")
    print(transactions_df.isnull().sum())
    

    print(transactions_df.head())
    
    print("Displaying shape of transactions_df:", transactions_df.shape)
    
    print("=====Completed transactions data transformation=====")
    return transactions_df

transform_transactions_data()