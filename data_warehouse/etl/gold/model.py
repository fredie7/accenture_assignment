import logging
from ..silver.transform_transactiions_data import transform_transactions_data
from ..silver.transform_customers_data import transform_customers_data

import pandas as pd

logger = logging.getLogger(__name__)

transactions = transform_transactions_data()
customers = transform_customers_data()

print(transactions.head())