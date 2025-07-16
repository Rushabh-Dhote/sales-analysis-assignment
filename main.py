import os
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import hashlib
import sqlite3

SALES_DB_PATH = '/Users/rushabhdhote/PycharmProjects/Python/66Degrees/.venv/sqlite_db'
SALES_DB_NAME = 'sales_db.db'

def get_data(dataset, path):
    """
    Function to download file from kaggle using kaggle API
    """
    try:
        api = KaggleApi()
        api.authenticate()

        if not os.path.isdir(path):
            os.makedirs(path)
        api.dataset_download_files(dataset, path, unzip=True)
        print(f"Dataset {dataset} downloaded successfully.")
        data_file = f'{path}/{os.listdir(path)[0]}'
        return data_file
    except Exception as e:
        print(f"Download failed: {e}")
        return None

def generate_hash_key(row, columns):
    """Generating unique surrogate key based on column values"""
    key_string = '|'.join(str(row[col]) for col in columns)
    hashed_key = hashlib.sha256(key_string.encode('utf-8')).hexdigest()
    return hashed_key

file = get_data(dataset='faresashraf1001/supermarket-sales', path='./data')
if file:
    # Loading File Into a DF
    df = pd.read_csv(file)

    # Standardizing column names for consistency and database compatibility
    df.columns = (
        df.columns
        .str.strip()
        .str.replace(" ", "_")
        .str.lower()
    )

    # Creating the customer dimension table
    # Selecting relevant columns and generating surrogate key
    customer_dim = df[['customer_type', 'gender']].drop_duplicates().reset_index(drop=True)
    customer_dim['customer_key'] = customer_dim.apply(lambda row: generate_hash_key(row, ['customer_type', 'gender']), axis=1)
    customer_dim = customer_dim[['customer_key', 'customer_type', 'gender']]

    # Creating branch dimension table
    # Selecting relevant columns and generating surrogate key
    branch_dim = df[['branch', 'city']].drop_duplicates().reset_index(drop=True)
    branch_dim['branch_key'] = branch_dim.apply(lambda row: generate_hash_key(row, ['branch', 'city']), axis=1)
    branch_dim = branch_dim[['branch_key', 'branch', 'city']]

    # Creating sales fact table
    # Adding 'customer_key' and 'branch_key' to the sales_fact table
    # These act as foreign keys linking sales records to their respective dimension tables
    sales_fact = (
        df
        .merge(customer_dim, on=['customer_type', 'gender'], how='left')
        .merge(branch_dim, on=['branch', 'city'])
    )
    sales_fact['sales_key'] = sales_fact.apply(lambda row: generate_hash_key(row, ['invoice_id']), axis=1)
    sales_fact = sales_fact[['sales_key', 'customer_key', 'branch_key', 'invoice_id', 'product_line',
                             'unit_price', 'quantity', 'tax_5%', 'sales', 'date', 'time',
                             'payment', 'cogs', 'gross_margin_percentage', 'gross_income', 'rating']]

    # Loading data into sqlite database
    if not os.path.isdir(SALES_DB_PATH):
        os.makedirs(SALES_DB_PATH)

    with sqlite3.connect(f'{SALES_DB_PATH}/{SALES_DB_NAME}') as conn:
        customer_dim.to_sql('customer_dim', conn, if_exists='replace', index=False)
        branch_dim.to_sql('branch_dim', conn, if_exists='replace', index=False)
        sales_fact.to_sql('sales_fact', conn, if_exists='replace', index=False)
        print('Data Load Successful')

else:
    print('File Not Found')