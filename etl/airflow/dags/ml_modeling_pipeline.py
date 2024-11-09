import joblib
from airflow import DAG
from airflow.decorators import task
import pandas as pd
import numpy as np
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

from ml.k_means import *

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
    description="train ml model for customer segmentation",
    schedule=timedelta(seconds=20),
    catchup=False,
)

model_directory = os.path.join(folder_directory, "ml/models")
analytics_data_warehouse_db_path = (
    f"{folder_directory}/data/analytics_data_warehouse.db"
)


@task(dag=dag4)
def get_rfm_data(analytics_data_warehouse_db_path):
    try:
        datamart_rfm  = load_data(analytics_data_warehouse_db_path)
        datamart_rfm_dict = datamart_rfm.to_dict(orient="records")
        logging.info(f"Retrieved {len(datamart_rfm_dict)} records from RFM data.")
        return datamart_rfm
    except Exception as e:
        logging.error(f"Error loading RFM data: {e}")
        raise


@task(dag=dag4)
def preprocess_rfm_data(datamart_rfm):

    try:
        datamart_rfm_df = pd.DataFrame(datamart_rfm)
        selected_columns = ["Recency", "Frequency", "MonetaryValue"]
        rfm_df = datamart_rfm_df[selected_columns]
        logging.info(rfm_df)
        datamart_normalized, scaler = preprocess_data(rfm_df)
        logging.info(f"Normalized data shape: {datamart_normalized.shape}")
        result = {
            "normalized": datamart_normalized.tolist(),
            "scaler_mean": scaler.mean_.tolist(),
            "scaler_scale": scaler.scale_.tolist(),
        }
        logging.info(f"Preprocessing result: {result}")
        return result
    except Exception as e:
        logging.error(f"Error preprocessing RFM data: {e}")
        raise


@task(dag=dag4)
def train_model(
    datamart_rfm,
    preprocess_result,
    model_directory,
    analytics_data_warehouse_db_path,
):
    
    datamart_rfm_df = pd.DataFrame(datamart_rfm)
    selected_columns = ["Recency", "Frequency", "MonetaryValue"]
    datamart_rfm_k_selected_column = datamart_rfm_df[selected_columns]

    try:
        # Extract the results from the input dictionary
        normalized_data = preprocess_result["normalized"]
        scaler_mean = preprocess_result["scaler_mean"]
        scaler_scale = preprocess_result["scaler_scale"]

        logging.info(f"Scaler mean: {normalized_data}")
        logging.info(f"Scaler mean: {scaler_mean}")
        logging.info(f"Scaler scale: {scaler_scale}")

        # Create and set up the scaler based on provided means and scales
        from sklearn.preprocessing import StandardScaler

        scaler = StandardScaler()
        scaler.mean_ = np.array(scaler_mean)
        scaler.scale_ = np.array(scaler_scale)

        optimal_k = elbow_method(normalized_data)
        kmeans_model = run_kmeans(normalized_data, optimal_k)

        cluster_labels = kmeans_model.labels_
        datamart_rfm_k = datamart_rfm_k_selected_column.assign(Cluster=cluster_labels)


         # Generate descriptions for each segment
        descriptions_dict = describe_segments(datamart_rfm_k)

        datamart_rfm_k["customer_id"] = datamart_rfm_df["customer_id"]
        datamart_rfm_k['Description'] = datamart_rfm_k['Cluster'].map(descriptions_dict)

        logging.info(f"customer_segment data: {datamart_rfm_k}")

        # Save datamart_rfm_k as "customer_segment" table in analytics_data_warehouse.db
        db_connection = sqlite3.connect(analytics_data_warehouse_db_path)

        # Use if_exists='replace' to overwrite if it already exists, or 'append' to add new data
        datamart_rfm_k.to_sql(
            "customer_segment", db_connection, if_exists="replace", index=False
        )
        logging.info("DataFrame saved to SQLite database as 'customer_segment' table.")

        kmeans_file_path = os.path.join(model_directory, "kmeans_model.pkl")
        scaler_file_path = os.path.join(model_directory, "scaler.pkl")
        joblib.dump(kmeans_model, kmeans_file_path)
        joblib.dump(scaler, scaler_file_path)

        logging.info("Model training completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        db_connection.close()


# ------------DAG task-------------
get_rfm_data_task = get_rfm_data(analytics_data_warehouse_db_path)

preprocess_result = preprocess_rfm_data(get_rfm_data_task)

train_model_task = train_model(
    get_rfm_data_task,
    preprocess_result,
    model_directory,
    analytics_data_warehouse_db_path,
)

get_rfm_data_task >> preprocess_result >> train_model_task


