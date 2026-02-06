# =========================================
# Load the data from the data directory
# =========================================

# Import dependencies

import os
import pandas as pd
import numpy as np
import logging
from sklearn.preprocessing import StandardScaler

# Define directory containing csv files
DATA_DIR = "./data"

logger = logging.getLogger(__name__)

# Function to extract data
def extract_data():
    """Load structured CSVs."""
    logger.info("=====Loading customers.csv and transactions.csv=====")

    # Load CSV files
    customers = pd.read_csv(os.path.join(DATA_DIR, "customers.csv"))
    transactions = pd.read_csv(os.path.join(DATA_DIR, "transactions.csv"))
    
    # Log the shape of the loaded data
    logger.info(f"=====Loaded {len(customers)} customers and {len(transactions)} transactions=====")
    
    # Return the loaded data
    return customers, transactions

extract_data()