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

# Database paths
database_db_path = f"{folder_directory}/data/bank_customer_data.db"
analytics_data_warehouse_db_path = f"{folder_directory}/data/Analytics_Data_Warehouse.db"

# Unique ID columns for various tables
unique_id_columns = {
    "customers": "customer_id",
    "addresses": "address_id",
    "accounts": "account_id",
    "transactions": "transaction_id",
    "loans": "loan_id",
    "merchants": "merchant_id",
    "account_types": "account_type_id",
}

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

dag2 = DAG(
    "analytics_data_etl_pipeline",
    default_args=default_args,
    description="ETL data from database to data warehouse for data mining",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)

def get_db_connection(db_path):
    """Create a new database connection with error handling."""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")
    try:
        return sqlite3.connect(db_path)
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database: {e}")
        raise

def extract_cohort_data(db_path):
    """Extract necessary data for cohort analysis from the SQLite database."""
    query = """
        SELECT r.customer_id, c.start_date AS combined_date
        FROM campaigns c
        JOIN recipients r ON c.campaign_id = r.campaign_id
        WHERE c.campaign_id = 3

        UNION

        SELECT r.customer_id, r.response_date AS combined_date
        FROM campaigns c
        JOIN recipients r ON c.campaign_id = r.campaign_id
        WHERE c.campaign_id = 3;
    """
    
    with get_db_connection(db_path) as conn:
        cohort_df = pd.read_sql_query(query, conn)

    return cohort_df

def transform_cohort_data(cohort_df):
    """Transform the cohort data to calculate retention rates."""
    cohort_df["combined_date"] = pd.to_datetime(cohort_df["combined_date"], format="ISO8601")
    
    cohort_df["InvoiceMonth"] = cohort_df["combined_date"].dt.to_period("M").dt.to_timestamp()
    
    cohort_df["CohortMonth"] = (
        cohort_df.groupby("customer_id")["combined_date"]
        .transform("min")
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    cohort_df["CohortIndex"] = cohort_df.apply(lambda row: (
        (row["InvoiceMonth"].year - row["CohortMonth"].year) * 12 +
        (row["InvoiceMonth"].month - row["CohortMonth"].month) + 1) if pd.notna(row["InvoiceMonth"]) and pd.notna(row["CohortMonth"]) else 0,
        axis=1
    )
    
    cohort_counts = (
        cohort_df.dropna(subset=["CohortIndex"])
        .groupby(["CohortMonth", "CohortIndex"])["customer_id"]
        .nunique()
        .reset_index(name="customer_count")
    )

    cohort_counts_pivot = cohort_counts.pivot(index="CohortMonth", columns="CohortIndex", values="customer_count")
    
    cohort_sizes = cohort_counts.groupby("CohortMonth")["customer_count"].sum()
    
    retention = cohort_counts_pivot.divide(cohort_sizes, axis=0).fillna(0)
    
    return retention.round(3) * 100  # Return retention percentage

@task(dag=dag2)
def extract_RFM_data(db_path):
    """Extract necessary data for RFM analysis from the SQLite database."""
    query = """
        SELECT 
            c.customer_id,
            a.open_date,
            t.transaction_date,
            t.transaction_amount
        FROM 
            CUSTOMERS c
        JOIN 
            ACCOUNTS a ON c.customer_id = a.customer_id
        JOIN 
            TRANSACTIONS t ON a.account_id = t.account_id;
    """
    
    with get_db_connection(db_path) as conn:
        RFM_df1 = pd.read_sql_query(query, conn)
        
    return RFM_df1.to_dict(orient="records")

@task(dag=dag2)
def transform_RFM_data(RFM):
    """Transform the RFM data to calculate Recency, Frequency, and Monetary Value."""
    RFM_df2 = pd.DataFrame(RFM)
    
    RFM_df2["open_date"] = pd.to_datetime(RFM_df2["open_date"], format="ISO8601")
    RFM_df2["transaction_date"] = pd.to_datetime(RFM_df2["transaction_date"], format="ISO8601")

    snapshot_date = datetime.now()
    
    RFM_df2["Recency"] = (snapshot_date - RFM_df2["transaction_date"]).dt.days
    
    RFM_transformed = (
        RFM_df2.groupby("customer_id")
        .agg(
            Recency=("Recency", "min"),
            Frequency=("transaction_date", "count"),
            MonetaryValue=("transaction_amount", "sum"),
        )
        .reset_index()
        .to_dict(orient="records")
    )
    
    return RFM_transformed

@task(dag=dag2)
def load_to_warehouse(db_path, dataframes_to_save, table_names_to_save):
    """Load transformed data into the analytics data warehouse with validation and error handling."""
    try:
        # Validate inputs
        if not isinstance(dataframes_to_save, (pd.DataFrame, dict)):
            raise ValueError("dataframes_to_save must be a DataFrame or dictionary")
        if not isinstance(table_names_to_save, str):
            raise ValueError("table_names_to_save must be a string")
            
        # Convert to DataFrame if needed
        df_to_save = pd.DataFrame(dataframes_to_save) if isinstance(dataframes_to_save, dict) else dataframes_to_save
        
        logging.info(f"DataFrame to be saved: {df_to_save.shape}")
        
        # Validate DataFrame is not empty
        if df_to_save.empty:
            raise ValueError("DataFrame is empty")
            
        with get_db_connection(db_path) as conn:
            df_to_save.to_sql(
                table_names_to_save.lower(),
                conn,
                if_exists="replace",
                index=False,
                method='multi',  # More efficient for large datasets
                chunksize=1000   # Process in chunks to avoid memory issues
            )
            
        logging.info(f"Successfully saved DataFrame to table {table_names_to_save}")
        
    except Exception as e:
        logging.error(f"Error in load_to_warehouse: {str(e)}")
        raise

# DAG task dependencies
extract_RFM_data_task = extract_RFM_data(database_db_path)
transform_RFM_data_task = transform_RFM_data(extract_RFM_data_task)
load_RFM_to_warehouse_task = load_to_warehouse(
    analytics_data_warehouse_db_path, transform_RFM_data_task, "RFM_analysis"
)

extract_RFM_data_task >> transform_RFM_data_task >> load_RFM_to_warehouse_task

if __name__ == "__main__":
    # # cohort analysis etl pipeline
    # cohort_df = extract_cohort_data(database_db_path)
    # retention_percentage = transform_cohort_data(cohort_df)
    # print(retention_percentage)
    # load_to_warehouse(
    #     analytics_data_warehouse_db_path,
    #     retention_percentage,
    #     "cohort_analysis",
    # )
    # RFM analysis etl pipeline
    # RFM_df = extract_RFM_data(database_db_path)
    # RFM_transformed_df = transform_RFM_data(RFM_df)
    # load_to_warehouse(
    #     analytics_data_warehouse_db_path,
    #     RFM_transformed_df,
    #     "RFM_analysis",
    # )
    pass
