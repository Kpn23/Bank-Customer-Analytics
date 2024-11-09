from airflow import DAG
from airflow.decorators import task
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import logging
import sys
import sqlite3


logging.basicConfig(level=logging.INFO)

load_dotenv()
folder_directory = os.getenv("folder_path")

sys.path.append(folder_directory)


from scripts.realtime_data_generation import *

database_db_path = f"{folder_directory}/data/bank_customer_data.db"

unique_id_columns = {
    "customers": "customer_id",
    "addresses": "address_id",
    "accounts": "account_id",
    "transactions": "transaction_id",
    "loans": "loan_id",
    "merchants": "merchant_id",
    "account_types": "account_type_id",
}


default_args = {
    "owner": "Paul",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 5),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag1 = DAG(
    "realtime_data_elt_pipeline",
    default_args=default_args,
    description="generate data every interval to simulate realtime data ingestion",
    schedule=timedelta(seconds=20),
    catchup=False,
)

"------------Fake Realtime Data Generation-------------"


@task(dag=dag1)
def get_new_data(database_db_path):
    dataframes = load_data_from_database(database_db_path)

    # Convert Timestamp objects to strings for JSON serialization
    for key, df in dataframes.items():
        if isinstance(df, pd.DataFrame):
            for column in df.select_dtypes(include=["datetime64[ns]"]).columns:
                df[column] = df[column].astype(
                    str
                )  # Convert datetime columns to strings

    logging.info(
        f"DataFrames loaded: {list(dataframes.keys())}"
    )  # Log keys of the DataFrames
    return dataframes


@task(dag=dag1)
def load_realtime_transaction(dataframes, database_db_path):
    accounts_df = pd.DataFrame(dataframes["accounts"])
    transactions_df = pd.DataFrame(dataframes["transactions"])
    if accounts_df.empty or transactions_df.empty:
        logging.error("One of the DataFrames is empty: Accounts or Transactions.")
    else:
        generate_new_transactions(accounts_df, transactions_df, database_db_path)


@task(dag=dag1)
def load_realtime_recipient(dataframes, database_db_path):
    campaigns_df = pd.DataFrame(dataframes["campaigns"])
    recipients_df = pd.DataFrame(dataframes["recipients"])
    if campaigns_df.empty or recipients_df.empty:
        logging.error("One of the DataFrames is empty: Campaigns or Recipients.")
    else:
        generate_new_recipient(campaigns_df, recipients_df, database_db_path)


# _______DAG_____________
get_df_task = get_new_data(database_db_path)

# Load tasks
save_realtime_transaction_task = load_realtime_transaction(
    get_df_task, database_db_path
)
save_realtime_recipient_task = load_realtime_recipient(get_df_task, database_db_path)

# Set up dependencies
get_df_task >> save_realtime_transaction_task

get_df_task >> save_realtime_recipient_task
