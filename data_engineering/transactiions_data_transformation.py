# Import dependencies
from data_extraction import extract_data
from utils.helper import standardize_columns, EXCHANGE_RATES

import pandas as pd

def transform_transactions_data():
    """Transform customer data"""

    print("Retrieving transactions data...")
    _, transactions_df =  extract_data()
    # customer_info = customers_df.info()
    print("=====Processing Transactions Data=====")
    print("=====Displaying Transactions data info=====")

    print(transactions_df.info())
    
    print("=====Standardizing transactions data column=====")
    transactions_df = standardize_columns(transactions_df)
    
    print("=====Converting timestamp to datetime=====")
    transactions_df["timestamp"] = pd.to_datetime(transactions_df["timestamp"])

    transactions_df["customer_id"] = transactions_df["customer_id"].astype("Int64")

    print(transactions_df.info())
    print(transactions_df.head())

    print("=====Checking for duplicate records on transaction_id=====")
    print(transactions_df.duplicated(subset=["transaction_id"]).sum())
    
    print("=====Investigave duplicate records on transaction_id for repeat entries due to pipeline retrys=====")
    duplicate_transactions = transactions_df[transactions_df.duplicated("transaction_id", keep=False)]
    print(duplicate_transactions)

    print("=====No repeat entries due to retries notices among duplicate records=====")

    print("=====Dropping duplicate records on transaction_id while retaining the latest timestamp due to late arrivals=====")
    transactions_df = (
        transactions_df
        .sort_values("timestamp")
        .drop_duplicates(subset=["transaction_id"], keep="last")
    )


    
    # transactions_df = transactions_df.drop_duplicates(subset=["transaction_id"])
    
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
    
    
    print("=====Checking for null values=====")
    print(transactions_df.isnull().sum())
    


    # print(transactions_df["currency"].value_counts())
    # print(transactions_df["currency_imputed"].value_counts())
    print(transactions_df.head())
    # print(transactions_df["exchange_rate"].isna().sum())
    
    # print("=====Completed transactions data transformation=====")
    # return transactions_df

transform_transactions_data()