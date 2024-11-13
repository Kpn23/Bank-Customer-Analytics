import matplotlib
matplotlib.use('TkAgg') 
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, davies_bouldin_score
import numpy as np
import sqlite3
import os
from dotenv import load_dotenv
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
from yellowbrick.cluster import KElbowVisualizer
import logging


logging.basicConfig(level=logging.INFO)


load_dotenv()
folder_directory = os.getenv("folder_path")
sys.path.append(folder_directory)

analytics_data_warehouse_db_path = (
    f"{folder_directory}/data/analytics_data_warehouse.db"
)
model_directory = os.path.join(folder_directory, "ml/models")
os.makedirs(model_directory, exist_ok=True)


def load_data(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = "SELECT customer_id, recency, frequency, monetaryvalue FROM rfm_analysis;"
    cursor.execute(query)
    result_1 = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    datamart_rfm = pd.DataFrame(result_1, columns=column_names)
    return datamart_rfm

def preprocess_data(datamart_rfm):
    if (datamart_rfm <= 0).any().any():
        print("Warning: Non-positive values found in the DataFrame. Adjusting...")
        datamart_rfm = datamart_rfm.clip(lower=1e-5)

    datamart_log = np.log(datamart_rfm)
    scaler = StandardScaler()
    scaler.fit(datamart_log)

    # Ensure datamart_normalized is a 2D array
    datamart_normalized = scaler.transform(datamart_log)
    return np.array(datamart_normalized), scaler


def elbow_method(datamart_normalized):
    # Check if it's a list and convert it to a NumPy array
    if isinstance(datamart_normalized, list):
        datamart_normalized = np.array(datamart_normalized)

    # Ensure it is 2D
    if len(datamart_normalized.shape) == 1:
        datamart_normalized = datamart_normalized.reshape(-1, 1)

    model = KMeans()
    visualizer = KElbowVisualizer(model, k=(1, 10))

    logging.info(f"Type of normalized data: {type(datamart_normalized)}")
    logging.info(f"Shape of normalized data: {datamart_normalized.shape}")

    visualizer.fit(datamart_normalized)
    
    # Show the plot
    visualizer.show()  # This line will display the elbow plot

    return visualizer.elbow_value_

def run_kmeans(datamart_normalized, optimal_k):
    kmeans = KMeans(n_clusters=optimal_k, random_state=1)
    kmeans.fit(datamart_normalized)
    logging.info(f"kmean info: {kmeans}")
    return kmeans


def evaluate_model(datamart_normalized, cluster_labels):
    silhouette_avg = silhouette_score(datamart_normalized, cluster_labels)
    davies_bouldin_avg = davies_bouldin_score(datamart_normalized, cluster_labels)
    print(f"Silhouette Score: {silhouette_avg:.4f}")
    print(f"Davies-Bouldin Index: {davies_bouldin_avg:.4f}")


def analyze_clusters(datamart_rfm_k):
    cluster_analysis = (
        datamart_rfm_k.groupby(["Cluster"])
        .agg(
            {
                "Recency": "mean",
                "Frequency": "mean",
                "MonetaryValue": ["mean", "count"],
            }
        )
        .round(0)
    )
    print(cluster_analysis)


def visualize_clusters(datamart_rfm_k, datamart_normalized):
    datamart_normalized_df = pd.DataFrame(
        datamart_normalized,
        index=datamart_rfm_k.index,
        columns=datamart_rfm_k.columns[:-1],
    )
    datamart_normalized_df["Cluster"] = datamart_rfm_k["Cluster"]
    datamart_melt = pd.melt(
        datamart_normalized_df.reset_index(),
        id_vars=["Cluster"],
        value_vars=["Recency", "Frequency", "MonetaryValue"],
        var_name="Attribute",
        value_name="Value",
    )
    
    plt.figure(figsize=(10, 6))
    plt.title("Cluster Analysis: Snake Plot of Standardized Variables by Customer Segments")  # Add a descriptive title
    sns.lineplot(x="Attribute", y="Value", hue="Cluster", data=datamart_melt)
    plt.xlabel("Attributes")
    plt.ylabel("Standardized Values")
    plt.legend(title='Customer Segment')
    plt.show()


def relative_importance(datamart_rfm_k):
    cluster_avg = datamart_rfm_k.groupby(["Cluster"]).mean()
    population_avg = datamart_rfm_k.drop(columns="Cluster").mean()
    relative_imp = cluster_avg / population_avg - 1
    print(relative_imp.round(2))
    plt.figure(figsize=(8, 2))
    plt.title("Relative Importance of Attributes")
    sns.heatmap(data=relative_imp, annot=True, fmt=".2f", cmap="RdYlGn")
    plt.show()


def save_model(model_directory, kmeans, scaler):
    kmeans_file_path = os.path.join(model_directory, "kmeans_model.pkl")
    scaler_file_path = os.path.join(model_directory, "scaler.pkl")
    joblib.dump(kmeans, kmeans_file_path)
    joblib.dump(scaler, scaler_file_path)


def load_model():
    kmeans_file_path = os.path.join(model_directory, "kmeans_model.pkl")
    scaler_file_path = os.path.join(model_directory, "scaler.pkl")
    kmeans = joblib.load(kmeans_file_path)
    scaler = joblib.load(scaler_file_path)
    return kmeans, scaler


def categorize_new_customer(new_customer_data):
    kmeans, scaler = load_model()

    if (new_customer_data <= 0).any().any():
        print(
            "Warning: Non-positive values found in the new customer data. Adjusting..."
        )
        new_customer_data = new_customer_data.clip(lower=1e-5)

    new_customer_df = pd.DataFrame(
        new_customer_data.values.reshape(1, -1),
        columns=["Recency", "Frequency", "MonetaryValue"],
    )
    new_customer_log = np.log(new_customer_df)
    new_customer_normalized = scaler.transform(new_customer_log)

    segment_label = kmeans.predict(new_customer_normalized)

    return segment_label[0]


def describe_segments(datamart_rfm_k):
    """Generate text descriptions for each customer segment based on average RFM values."""
    # Calculate average RFM values for each cluster
    segment_description = (
        datamart_rfm_k.groupby("Cluster")
        .agg({"Recency": "mean", "Frequency": "mean", "MonetaryValue": "mean"})
        .round(2)
    )

    descriptions = {}

    recency_thresholds = segment_description["Recency"].quantile([0.25, 0.5, 0.75])
    frequency_thresholds = segment_description["Frequency"].quantile([0.25, 0.5, 0.75])
    monetary_thresholds = segment_description["MonetaryValue"].quantile([0.25, 0.5, 0.75])

    for segment, values in segment_description.iterrows():
        recency = values["Recency"]
        frequency = values["Frequency"]
        monetary_value = values["MonetaryValue"]

        # Define recency level based on percentiles
        if recency <= recency_thresholds[0.25]:
            recency_level = "recently active"
        elif recency <= recency_thresholds[0.5]:
            recency_level = "inactive"
        else:
            recency_level = "very inactive"

        # Define frequency level based on percentiles
        if frequency > frequency_thresholds[0.75]:
            frequency_level = "frequently"
        elif frequency > frequency_thresholds[0.5]:
            frequency_level = "occasionally"
        else:
            frequency_level = "rarely"

        # Define monetary level based on percentiles
        if monetary_value > monetary_thresholds[0.75]:
            monetary_level = "highly"
        elif monetary_value > monetary_thresholds[0.5]:
            monetary_level = "moderately"
        else:
            monetary_level = "low"

        description = (
            f"Segment {segment}: "
            f"Customers in this segment have an average Recency of {recency}, "
            f"an average Frequency of {frequency}, "
            f"and an average Monetary Value of {monetary_value}. "
            f"This indicates that they are {recency_level} customers, "
            f"who purchase {frequency_level}, "
            f"and spend {monetary_level}."
        )

        descriptions[segment] = description

    return descriptions


__all__ = [
    "load_data",
    "preprocess_data",
    "elbow_method",
    "run_kmeans",
    "evaluate_model",
    "analyze_clusters",
    "visualize_clusters",
    "relative_importance",
    "save_model",
    "load_model",
    "categorize_new_customer",
    "describe_segments",
]


def main():
    """
    Main function to execute the customer segmentation workflow using KMeans clustering.

    Steps involved:
    1. Load environment variables to access the database path.
    2. Connect to the SQLite database and load RFM (Recency, Frequency, Monetary) data into a DataFrame.
    3. Preprocess the data by handling non-positive values and applying log transformation.
    4. Determine the optimal number of clusters using the Elbow Method.
    5. Run KMeans clustering with the specified number of clusters.
    6. Add cluster labels to the original DataFrame for analysis.
    7. Evaluate the clustering model using silhouette score and Davies-Bouldin index.
    8. Analyze average RFM values for each cluster to understand customer segments.
    9. Visualize cluster attributes using snake plots and heatmaps.
    10. Calculate and visualize the relative importance of segment attributes.

    The results provide insights into customer behavior, enabling targeted marketing strategies and improved customer relationship management.
    """
    datamart_rfm = load_data(analytics_data_warehouse_db_path)

    datamart_normalized, scaler = preprocess_data(datamart_rfm)

    optimal_k = elbow_method(datamart_normalized)

    kmeans_model = run_kmeans(datamart_normalized, optimal_k)

    save_model(model_directory, kmeans_model, scaler)

    cluster_labels = kmeans_model.labels_

    datamart_rfm_k = datamart_rfm.assign(Cluster=cluster_labels)

    evaluate_model(datamart_normalized, cluster_labels)

    analyze_clusters(datamart_rfm_k)

    visualize_clusters(datamart_rfm_k, datamart_normalized)

    relative_importance(datamart_rfm_k)


if __name__ == "__main__":
    main()
