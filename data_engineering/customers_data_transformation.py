# Import dependencies
from data_extraction import extract_data
from utils.helper import standardize_columns

import pandas as pd

def transform_customer_data():
    """Transform customer data"""

    print("Retrieving customer data...")
    customers_df, _ =  extract_data()
    # customer_info = customers_df.info()
    print("=====Processing Customer Data=====")
    print("=====Displaying Customer data info=====")

    print(customers_df.info())
    
    print("=====Standardizing customer data column=====")
    customers_df = standardize_columns(customers_df)
    
    print("=====Converting signup_date to datetime=====")
    customers_df["signup_date"] = pd.to_datetime(customers_df["signup_date"])

    print("=====Double-checking data type for signup_date=====")
    print(customers_df.info())
    
    print("=====Checking for null values=====")
    print(customers_df.isnull().sum())

    print("=====Checking for duplicate records on customer_id=====")
    print(customers_df.duplicated(subset=["customer_id"]).sum())
    
    print("=====No duplicate records on customer_id=====")
    
    
    print("=====Completed customer data transformation=====")
    return customers_df

transform_customer_data()