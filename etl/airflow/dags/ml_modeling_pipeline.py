import joblib
from airflow import DAG
from airflow.decorators import task
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import logging
import sqlite3
import sys

# Set up logging configuration
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()
folder_directory = os.getenv("folder_path")

# Ensure the folder path is valid
if not folder_directory:
    raise ValueError("The 'folder_path' environment variable is not set.")

# Add folder directory to system path
sys.path.append(folder_directory)

from ml.k_means import load_data, preprocess_data, elbow_method, run_kmeans, describe_segments

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

dag4 = DAG(
    "modeling_pipeline",
    default_args=default_args,
    description="Train ML model for customer segmentation",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)

model_directory = os.path.join(folder_directory, "ml/models")
analytics_data_warehouse_db_path = os.path.join(folder_directory, "data", "analytics_data_warehouse.db")

def get_db_connection(db_path):
    """Create a new database connection."""
    return sqlite3.connect(db_path)

@task(dag=dag4)
def get_rfm_data(analytics_data_warehouse_db_path):
    """Retrieve RFM data from the analytics data warehouse."""
    try:
        datamart_rfm = load_data(analytics_data_warehouse_db_path)
        logging.info(f"Retrieved {len(datamart_rfm)} records from RFM data.")
        return datamart_rfm.to_dict(orient="records")
    except Exception as e:
        logging.error(f"Error loading RFM data: {e}")
        raise

@task(dag=dag4)
def preprocess_rfm_data(datamart_rfm):
    """Preprocess RFM data for modeling."""
    try:
        datamart_rfm_df = pd.DataFrame(datamart_rfm)
        selected_columns = ["Recency", "Frequency", "MonetaryValue"]
        rfm_df = datamart_rfm_df[selected_columns]

        # Normalize the data
        datamart_normalized, scaler = preprocess_data(rfm_df)
        logging.info(f"Normalized data shape: {datamart_normalized.shape}")

        result = {
            "normalized": datamart_normalized.tolist(),
            "scaler_mean": scaler.mean_.tolist(),
            "scaler_scale": scaler.scale_.tolist(),
        }
        
        return result
    except Exception as e:
        logging.error(f"Error preprocessing RFM data: {e}")
        raise

@task(dag=dag4)
def train_model(datamart_rfm, preprocess_result):
    """Train K-Means model on the preprocessed RFM data."""
    
    try:
        datamart_rfm_df = pd.DataFrame(datamart_rfm)
        selected_columns = ["Recency", "Frequency", "MonetaryValue"]
        datamart_rfm_k_selected_column = datamart_rfm_df[selected_columns]

        # Extract normalized data and scaler parameters
        normalized_data = preprocess_result["normalized"]
        scaler_mean = preprocess_result["scaler_mean"]
        scaler_scale = preprocess_result["scaler_scale"]

        # Set up the scaler based on provided means and scales
        from sklearn.preprocessing import StandardScaler

        scaler = StandardScaler()
        scaler.mean_ = np.array(scaler_mean)
        scaler.scale_ = np.array(scaler_scale)

        # Determine optimal number of clusters using elbow method
        optimal_k = elbow_method(normalized_data)
        
        # Run K-Means clustering
        kmeans_model = run_kmeans(normalized_data, optimal_k)

        # Assign cluster labels to the original DataFrame
        cluster_labels = kmeans_model.labels_
        datamart_rfm_k = datamart_rfm_k_selected_column.assign(Cluster=cluster_labels)

         # Generate descriptions for each segment
        descriptions_dict = describe_segments(datamart_rfm_k)

        # Add customer IDs and descriptions to the DataFrame
        datamart_rfm_k["customer_id"] = datamart_rfm_df["customer_id"]
        datamart_rfm_k['Description'] = datamart_rfm_k['Cluster'].map(descriptions_dict)

        logging.info(f"Customer segment data: {datamart_rfm_k}")

        # Save results to SQLite database
        with get_db_connection(analytics_data_warehouse_db_path) as db_connection:
            datamart_rfm_k.to_sql("customer_segment", db_connection, if_exists="replace", index=False)
            logging.info("DataFrame saved to SQLite database as 'customer_segment' table.")

            # Save trained models using joblib
            joblib.dump(kmeans_model, os.path.join(model_directory, "kmeans_model.pkl"))
            joblib.dump(scaler, os.path.join(model_directory, "scaler.pkl"))

            logging.info("Model training completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during model training: {e}")
    
# ------------DAG task-------------
get_rfm_data_task = get_rfm_data(analytics_data_warehouse_db_path)
preprocess_result = preprocess_rfm_data(get_rfm_data_task)
train_model_task = train_model(get_rfm_data_task, preprocess_result)

get_rfm_data_task >> preprocess_result >> train_model_task