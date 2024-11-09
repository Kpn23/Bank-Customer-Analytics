# set up virtual env 
cd /Users/superdayuanjingzhi/Documents/JDE-python/p_AnalyticsPlatform
python3 -m venv airflow_venv
### activate
source /Users/superdayuanjingzhi/Documents/JDE-python/p_AnalyticsPlatform/airflow_venv/bin/activate
### set pythonpath environment variable 
export PYTHONPATH=/Users/superdayuanjingzhi/Documents/JDE-python/p_AnalyticsPlatform

# location to store  configuration files and logs, install, initiate db
### why it need to be set everytime?
export AIRFLOW_HOME=/Users/superdayuanjingzhi/Documents/JDE-python/p_AnalyticsPlatform/etl/airflow
### check current HOME
cd $AIRFLOW_HOME
### Create the Directory Structure
pip install apache-airflow==2.10.2
airflow db init

# start airflow
###create user
airflow users create \
    --username admin \
    --firstname FIRST_NAME \
    --lastname LAST_NAME \
    --role Admin \
    --email admin@example.com
### check user
airflow users list
### delete user 
airflow users delete -u __username__
### initiate for development purposes only
airflow standalone
### initiate this will initiate the HOME airflow
airflow webserver --port 8080
airflow scheduler
### UI
http://localhost:8080
### airflow XCom return always strings
XCom Serialization:
By default, Airflow serializes data as strings when storing it in XComs. This means that if your extract_realtime_data function returns a complex object (like a DataFrame), it gets converted to a string representation when pushed to XCom.
# dags
airflow dags list
airflow dags list-import-errors

# FastAPI
### run 
uvicorn main:app --reload
### call api
http://127.0.0.1:8000
http://127.0.0.1:8000/docs

# module import
### run as module from the root
python -m etl.airflow.dags.etl_pipeline

# search for current schema
SELECT name FROM sqlite_master WHERE type='table';
PRAGMA table_info(transactions);


# build native hadoop libraries
1. 
git clone https://github.com/apache/hadoop.git
cd hadoop
2. 
git checkout branch-<VERSION>  # Replace <VERSION> with your desired version
3. Use Maven to build the native libraries:
mvn package -Pdist,native -DskipTests -Dtar
4. After building, copy the newly created libraries to your Hadoop installation directory:
cp -R hadoop-dist/target/hadoop-<VERSION>/lib/native $HADOOP_HOME/lib/native
5. 
nano ~/.zshrc
6. 
After building, copy the newly created libraries to your Hadoop installation directory:
7. Check Native Library Availability
hadoop checknative -a

### Start Hadoop DFS:
 Run the following command to start the Hadoop Distributed File System:
bash
start-dfs.sh
### Verify Services Are Running
After starting the services, check again with:
bash
jps
You should see entries for NameNode, DataNode, SecondaryNameNode, and possibly others like ResourceManager.
### stop running Hadoop Processes
bash
hdfs --daemon stop
stop-dfs.sh
###  If you want to stop all HDFS-related services at once, you can use:
bash
stop-dfs.sh



```text
Banking_Customer_Insights_Dashboard/
│
├── data/
│   ├── analytics_data_warehouse.db
│   └── bank_customer_data.db
│
├── etl/
│   ├── airflow/                  
│   │   ├── dags/                 
│   │   │   └── etl_pipeline.py   # Main ETL pipeline script
│   │   ├── logs/
│   │   └── airflow.cfg           # Airflow configuration file
│
├── ml/
│   ├── models/                   
│   │   ├── kmeans_model.pkl
│   │   └── scaler.pkl
│   └── k-means.py                # Script for customer segmentation model
│
├── notebook/
│   └── EDA.ipynb
│
├── scripts/                     
│   ├── data_generation.py        
│   └── realtime_data_generation.py
│
├── api/
│   ├── __init__.py
│   └── fastapi_app.py            # New file for FastAPI application
│
├── tmp/                          # New folder for lock files
│   ├── airflow.lock
│   └── fastapi.lock
│
├── .env.sample  
├── .gitignore
├── main.py                       # Your main script to start both Airflow and FastAPI
├── README.md
└── requirements.txt
```