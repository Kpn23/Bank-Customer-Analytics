import joblib
from airflow import DAG
from airflow.decorators import task
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import logging
import sys

# Set up logging configuration
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()
folder_directory = os.getenv("folder_path")

if not folder_directory:
    raise ValueError("The 'folder_path' environment variable is not set.")

# Add folder directory to system path
sys.path.append(folder_directory)

from scripts.realtime_data_generation import load_data_from_database, generate_new_transactions, generate_new_recipient

# Database path
database_db_path = os.path.join(folder_directory, "data", "bank_customer_data.db")

# Default arguments for the DAG
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
    description="Generate data every interval to simulate real-time data ingestion",
    schedule_interval=timedelta(seconds=20),
    catchup=False,
)

@task(dag=dag1)
def get_new_data(db_path):
    """Load new data from the database."""
    try:
        dataframes = load_data_from_database(db_path)

        # Convert Timestamp objects to strings for JSON serialization
        for key, df in dataframes.items():
            if isinstance(df, pd.DataFrame):
                for column in df.select_dtypes(include=["datetime64[ns]"]).columns:
                    df[column] = df[column].astype(str)  # Convert datetime columns to strings

        logging.info(f"DataFrames loaded: {list(dataframes.keys())}")  # Log keys of the DataFrames
        return dataframes

    except Exception as e:
        logging.error(f"Error loading new data: {e}")
        raise

@task(dag=dag1)
def load_realtime_transaction(dataframes, db_path):
    """Load new transactions into the database."""
    try:
        accounts_df = pd.DataFrame(dataframes["accounts"])
        transactions_df = pd.DataFrame(dataframes["transactions"])

        if accounts_df.empty or transactions_df.empty:
            logging.error("One of the DataFrames is empty: Accounts or Transactions.")
            return
        
        generate_new_transactions(accounts_df, transactions_df, db_path)
        logging.info("New transactions generated successfully.")

    except Exception as e:
        logging.error(f"Error loading real-time transactions: {e}")
        raise

@task(dag=dag1)
def load_realtime_recipient(dataframes, db_path):
    """Load new recipients into the database."""
    try:
        campaigns_df = pd.DataFrame(dataframes["campaigns"])
        recipients_df = pd.DataFrame(dataframes["recipients"])

        if campaigns_df.empty or recipients_df.empty:
            logging.error("One of the DataFrames is empty: Campaigns or Recipients.")
            return
        
        generate_new_recipient(campaigns_df, recipients_df, db_path)
        logging.info("New recipients generated successfully.")

    except Exception as e:
        logging.error(f"Error loading real-time recipients: {e}")
        raise

# _______DAG_____________
get_df_task = get_new_data(database_db_path)

# Load tasks
save_realtime_transaction_task = load_realtime_transaction(get_df_task, database_db_path)
save_realtime_recipient_task = load_realtime_recipient(get_df_task, database_db_path)

# Set up dependencies
get_df_task >> [save_realtime_transaction_task, save_realtime_recipient_task]

if __name__ == "__main__":
    pass  # The main block is not needed for Airflow DAGs.