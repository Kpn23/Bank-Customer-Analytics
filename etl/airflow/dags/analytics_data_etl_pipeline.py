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
analytics_data_warehouse_db_path = (
    f"{folder_directory}/data/analytics_data_warehouse.db"
)

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

dag2 = DAG(
    "analytics_data_etl_pipeline",
    default_args=default_args,
    description="ETL data from database to datawarehouse for data mining",
    schedule=timedelta(seconds=20),
    catchup=False,
)

"------------For Cohort Analysis-------------"


def extract_cohort_data(db_path):
    """
    Extracts necessary data for cohort analysis from the SQLite database.

    Parameters:
    db_path (str): Path to the SQLite database file.

    Returns:
    pd.DataFrame: DataFrame containing customer_id, open_date, and transaction_date.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)

    # SQL query to extract relevant data
    query = """
    SELECT
        r.customer_id,
        c.start_date AS combined_date
    FROM
        campaigns c
    JOIN
        recipients r ON c.campaign_id = r.campaign_id
    WHERE
        c.campaign_id = 3

    UNION

    SELECT
        r.customer_id,
        r.response_date AS combined_date
    FROM
        campaigns c
    JOIN
        recipients r ON c.campaign_id = r.campaign_id
    WHERE
        c.campaign_id = 3;
    """

    # Execute the query and load data into a DataFrame
    cohort_df = pd.read_sql_query(query, conn)

    # Close the database connection
    conn.close()

    return cohort_df


def transform_cohort_data(cohort_df):
    # Convert date columns to datetime format
    cohort_df["combined_date"] = pd.to_datetime(
        cohort_df["combined_date"], format="ISO8601"
    )

    # Create InvoiceMonth and CohortMonth
    cohort_df["InvoiceMonth"] = (
        cohort_df["combined_date"].dt.to_period("M").dt.to_timestamp()
    )
    cohort_df["CohortMonth"] = (
        cohort_df.groupby("customer_id")["combined_date"]
        .transform("min")
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    # Calculate CohortIndex
    def calculate_cohort_index(row):
        if pd.isna(row["InvoiceMonth"]) or pd.isna(row["CohortMonth"]):
            return 0
        return (
            (row["InvoiceMonth"].year - row["CohortMonth"].year) * 12
            + (row["InvoiceMonth"].month - row["CohortMonth"].month)
            + 1
        )

    cohort_df["CohortIndex"] = cohort_df.apply(calculate_cohort_index, axis=1)
    cohort_data = cohort_df.dropna(subset=["CohortIndex"])

    # Grouping by CohortMonth and CohortIndex to get unique customer_id counts
    cohort_counts = (
        cohort_data.groupby(["CohortMonth", "CohortIndex"])["customer_id"]
        .nunique()
        .reset_index(name="customer_count")
    )

    # Pivoting the DataFrame
    cohort_counts_pivot = cohort_counts.pivot(
        index="CohortMonth", columns="CohortIndex", values="customer_count"
    )

    # Ensure cohort_sizes is a Series with matching index type
    cohort_sizes = cohort_counts.groupby("CohortMonth")["customer_count"].sum()

    # Align indices for division
    retention = cohort_counts_pivot.divide(cohort_sizes, axis=0).fillna(0)

    # Review retention table
    retention_percentage = retention.round(3) * 100

    return retention_percentage  # Return the retention percentage instead of an undefined variable


"------------For RFM Analysis-------------"


@task(dag=dag2)
def extract_RFM_data(db_path):
    """
    Extracts necessary data for RFM analysis from the SQLite database.

    Parameters:
    db_path (str): Path to the SQLite database file.

    Returns:
    pd.DataFrame: DataFrame containing customer_id, recency, frequency, and monetary value.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)

    # SQL query to extract relevant data
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
        TRANSACTIONS t ON a.account_id = t.account_id
    """

    # Execute the query and load data into a DataFrame
    RFM_df1 = pd.read_sql_query(query, conn)
    RFM = RFM_df1.to_dict(orient="records")

    # Close the database connection
    conn.close()
    return RFM


@task(dag=dag2)
def transform_RFM_data(RFM):
    RFM_df2 = pd.DataFrame(RFM)
    # Convert date columns to datetime format
    RFM_df2["open_date"] = pd.to_datetime(RFM_df2["open_date"], format="ISO8601")
    RFM_df2["transaction_date"] = pd.to_datetime(
        RFM_df2["transaction_date"], format="ISO8601"
    )

    # Calculate Recency (days since last transaction)
    snapshot_date = datetime.now()
    RFM_df2["Recency"] = (snapshot_date - RFM_df2["transaction_date"]).dt.days

    # Calculate Frequency and Monetary Value
    RFM_transformed_df1 = (
        RFM_df2.groupby("customer_id")
        .agg(
            Recency=("Recency", "min"),  # Minimum recency for each customer
            Frequency=("transaction_date", "count"),  # Count of transactions
            MonetaryValue=("transaction_amount", "sum"),  # Sum of transaction amounts
        )
        .reset_index()
    )
    RFM_transformed = RFM_transformed_df1.to_dict(orient="records")
    return RFM_transformed


"------------Load to the warehouse-------------"


@task(dag=dag2)
def load_to_warehouse(db_path, dataframes_to_save, table_names_to_save):
    df = pd.DataFrame(dataframes_to_save)
    logging.info(f"dataframe to be saved: {df}")
    def save_to_database(db_name, dataframe_df, table_names_to_save):
        logging.info(f"Saving DataFrames to {db_name}...")

        conn = sqlite3.connect(db_name)

        dataframe_df.to_sql(
            table_names_to_save.lower(), conn, if_exists="replace", index=False
        )

        conn.close()
        logging.info("Finished saving DataFrames to database.")

    save_to_database(
        db_path,
        df,
        table_names_to_save,
    )


"------------DAG2 task-------------"
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
