from airflow import DAG
from airflow.decorators import task
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import logging
import sqlite3

# Set up logging configuration
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()
folder_directory = os.getenv("folder_path")

# Define database paths and source file path
database_db_path = f"{folder_directory}/data/bank_customer_data.db"
analytics_data_warehouse_db_path = f"{folder_directory}/data/analytics_data_warehouse.db"
source_file_path = f"{folder_directory}/dashboard/source_file"

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

dag5 = DAG(
    "dashboard_pipeline",
    default_args=default_args,
    description="Export tables from database and data warehouse as CSV to update dashboard",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)

def get_db_connection(db_path):
    """Create a new database connection."""
    return sqlite3.connect(db_path)

@task(dag=dag5)
def export_tables_to_csv(db_path, source_file_path):
    """Export all tables from the specified database to CSV files."""
    
    try:
        # Connect to the database
        with get_db_connection(db_path) as conn:
            # Retrieve table names
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            # Export each table to a CSV file
            for table_name in tables:
                table_name_str = table_name[0]  # Extract table name from tuple
                df = pd.read_sql_query(f"SELECT * FROM {table_name_str}", conn)
                
                # Define CSV file path
                csv_file_path = os.path.join(source_file_path, f"{table_name_str}.csv")
                
                # Export DataFrame to CSV
                df.to_csv(csv_file_path, index=False)
                logging.info(f"Exported {table_name_str} to {csv_file_path}")

    except Exception as e:
        logging.error(f"Error exporting tables to CSV: {e}")
        raise

# Define tasks for exporting database and data warehouse tables
export_database_table_task = export_tables_to_csv(database_db_path, source_file_path)
export_datawarehouse_table_task = export_tables_to_csv(analytics_data_warehouse_db_path, source_file_path)

# Set task dependencies
export_database_table_task >> export_datawarehouse_table_task