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

database_db_path = f"{folder_directory}/data/bank_customer_data.db"
analytics_data_warehouse_db_path = f"{folder_directory}/data/analytics_data_warehouse.db"
source_file_path = f"{folder_directory}/dashboard/source_file"

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
    description="Export tables from database and datawarehouse as csv to update dashboard ",
    schedule=timedelta(minutes=1),
    catchup=False,
)

@task(dag=dag5)
def export_tables_to_csv(db_path, source_file_path):
    # Connect to the database
    conn = sqlite3.connect(db_path)
    
    # Create a cursor object
    cursor = conn.cursor()
    
    # Retrieve table names
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
        print(f"Exported {table_name_str} to {csv_file_path}")
    
    # Close the connection
    conn.close()




export_database_table_task = export_tables_to_csv(database_db_path, source_file_path)

export_datawarehouse_table_task = export_tables_to_csv(analytics_data_warehouse_db_path, source_file_path)

export_database_table_task >> export_datawarehouse_table_task
