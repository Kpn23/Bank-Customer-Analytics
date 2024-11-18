# Bank Customer Analytics System

This README provides a brief overview of the Bank Customer Analytics System project. For a comprehensive understanding of the system, please refer to my article [here](https://medium.com/@kpn23/bank-customer-analytics-system-a-data-engineering-and-ml-project-610d7e157f39).

## Project Description

The Bank Customer Analytics System is designed to transform how banks analyze and engage with their customers. It addresses challenges such as real-time insights, complex customer segmentation, and user-friendly access to data.

## Getting Started

These instructions will help you set up and run the project on your local machine for development and testing purposes.

### Prerequisites

- Python 3.8
- Apache Airflow
- FastAPI
- SQLite

### Setting Up the Environment

1. **Set up a virtual environment:**
   ```bash
   cd <your_path_to_directory>/p_AnalyticsPlatform
   python3 -m venv airflow_venv
   source airflow_venv/bin/activate

2. **Set the Airflow home directory:**
    ```bash
    export AIRFLOW_HOME=<your_path_to_directory>/p_AnalyticsPlatform/etl/airflow
    cd $AIRFLOW_HOME

3. **Install Apache Airflow and initialize the database:**
    ```bash
    pip install apache-airflow==2.10.2
    airflow db init

### Starting Airflow
1. **Create an admin user:**
    ```bash
    airflow users create --username 
    admin --firstname 
    FIRST_NAME --lastname 
    LAST_NAME --role 
    Admin --email admin@example.com

2. **Access the UI:**
Open your browser and navigate to http://localhost:8080.

### Running FastAPI

Access the API at http://127.0.0.1:8000 and its documentation at http://127.0.0.1:8000/docs.

## Directory Structure
```text
Banking_Customer_Insights_Dashboard/
├── api/
│   └── fastapi_app.py
│
├── dashboard/
│   ├── source file               
│   │   ├── account_types.csv
│   │   ├── accounts.csv
│   │   ├── addresses.csv  
│   │   ├── campaigns.csv
│   │   ├── cohort_analysis.csv  
│   │   ├── customer_segment.csv
│   │   ├── customers.csv
│   │   ├── loans.csv
│   │   ├── recipients.csv
│   │   ├── rfm_analysis.csv
│   │   └── transactions.csv
│   └── insight_dashboard.pbix
│
├── data/
│   ├── analytics_data_warehouse.db
│   └── bank_customer_data.db
│
├── etl/
│   ├── airflow/                  
│   │   ├── dags/
│   │   │   ├── analytics_data_etl_pipeline.py
│   │   │   ├── dashboard_pipeline.py
│   │   │   ├── ml_modeling_pipeline.py                
│   │   │   └── realtime_data_etl_pipeline.py
│   │   ├── logs/
│   │   ├── airflow.cfg           
│   │   └── webserver_config.py
│
├── gui/
│   └── main_window.py
│
├── ml/
│   ├── models/                   
│   │   ├── kmeans_model.pkl
│   │   └── scaler.pkl
│   └── k-means.py              
│
├── notebook/
│   └── EDA.ipynb
│
├── scripts/                     
│   ├── data_generation.py        
│   └── realtime_data_generation.py         
│
├── tmp/                     
│   ├── airflow.lock
│   └── fastapi.lock
│
├── .env.sample  
├── .gitignore
├── main.py                       
├── README.md
└── requirements.txt
```
