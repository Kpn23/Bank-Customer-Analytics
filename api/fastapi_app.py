from fastapi import FastAPI, HTTPException
import asyncio
import subprocess
from filelock import FileLock
from dotenv import load_dotenv
import os
import logging
import sys
import sqlite3
from datetime import datetime


load_dotenv()
folder_directory = os.getenv("folder_path")

sys.path.append(folder_directory)


app = FastAPI()
logger = logging.getLogger(__name__)

database_db_path = f"{folder_directory}/data/bank_customer_data.db"
analytics_data_warehouse_db_path = (
    f"{folder_directory}/data/analytics_data_warehouse.db"
)
# Define lock file paths
FASTAPI_LOCK = f"{folder_directory}/tmp/fastapi.lock"


def is_process_running(process_name):
    try:
        subprocess.check_output(["pgrep", "-f", process_name])
        return True
    except subprocess.CalledProcessError:
        return False


def is_lock_file_valid(lock_file):
    if not os.path.exists(lock_file):
        return False
    with open(lock_file, "r") as f:
        content = f.read().strip()
    return bool(content) and all(pid.isdigit() for pid in content.split(","))


def remove_lock_file(lock_file):
    try:
        os.remove(lock_file)
        logger.info(f"Removed invalid lock file: {lock_file}")
    except OSError as e:
        logger.error(f"Error removing lock file {lock_file}: {e}")


async def start_fastapi():
    logger.info("Attempting to start FastAPI")

    if is_process_running("uvicorn api.fastapi_app:app"):
        logger.info("FastAPI is already running")
        return None

    with FileLock(FASTAPI_LOCK):
        if os.path.exists(FASTAPI_LOCK):
            if not is_lock_file_valid(FASTAPI_LOCK):
                remove_lock_file(FASTAPI_LOCK)
            else:
                logger.info(
                    f"Valid lock file {FASTAPI_LOCK} exists, FastAPI might be running"
                )
                return None

        logger.info("Starting FastAPI")
        fastapi = await asyncio.create_subprocess_exec(
            "uvicorn", "api.fastapi_app:app", "--host", "0.0.0.0", "--port", "8000"
        )
        pid = str(fastapi.pid)
        logger.info(f"FastAPI started with PID: {pid}")

        logger.info(f"Writing PID to lock file: {pid}")
        with open(FASTAPI_LOCK, "w") as f:
            f.write(pid)

        logger.info(f"FastAPI started successfully. PID: {pid}")
        return fastapi



@app.get("/customers/")
def get_customer(customer_name: str, id_number: str):
    conn = sqlite3.connect(database_db_path)
    cursor = conn.cursor()

    # Query the CUSTOMERS and ADDRESSES table
    cursor.execute("""
        SELECT c.customer_id, c.customer_name, c.id_number, c.age, c.credit_score, c.loans_taken, a.state, a.city, a.street
        FROM CUSTOMERS c
        LEFT JOIN ADDRESSES a ON c.customer_id = a.customer_id
        WHERE c.customer_name = ? AND c.id_number = ?;
    """, (customer_name, id_number))
    
    customer_data = cursor.fetchone()
    if not customer_data:
        conn.close()
        raise HTTPException(status_code=404, detail="Customer not found")

    # Query the ACCOUNTS table for the customer's accounts
    cursor.execute("""
        SELECT at.account_type, ac.balance, ac.open_date
        FROM ACCOUNTS ac
        JOIN ACCOUNT_TYPES at ON ac.account_type_id = at.account_type_id
        WHERE ac.customer_id = ?;
    """, (customer_data[0],))
    
    accounts_data = cursor.fetchall()

    # Query the TRANSACTIONS table for recent transactions of each account
    cursor.execute("""
        SELECT t.transaction_date, t.transaction_amount, m.merchant_name 
        FROM TRANSACTIONS t
        JOIN MERCHANTS m ON t.merchant_id = m.merchant_id
        WHERE t.account_id IN (SELECT account_id FROM ACCOUNTS WHERE customer_id = ?)
        ORDER BY t.transaction_date DESC LIMIT 10;
    """, (customer_data[0],))

    transactions_data = cursor.fetchall()

    conn.close()

    # Query the CUSTOMER_SEGMENT table from another database
    conn = sqlite3.connect(analytics_data_warehouse_db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT customer_id, Description
        FROM CUSTOMER_SEGMENT
        WHERE customer_id = ?;
    """, (customer_data[0],))
    
    segment_data = cursor.fetchone()
    
    conn.close()

    if not segment_data:
        raise HTTPException(status_code=404, detail="Segment data not found")


    # Format open_date and transaction_date to remove microseconds
    formatted_accounts_data = [
        {
            "Account Type": acc[0],
            "Balance": acc[1],
            "Open Date": datetime.strptime(acc[2], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
            if acc[2] else None  # Handle null dates if necessary
        }
        for acc in accounts_data
    ]

    formatted_transactions_data = [
        {
            "Date": datetime.strptime(trans[0], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
            if trans[0] else None,
            "Amount": trans[1],
            "Description": trans[2]
        }
        for trans in transactions_data
    ]

    # Prepare response data structure
    response_data = {
        "Basic Information": {
            "Customer ID": customer_data[0],
            "Name": customer_data[1],
            "ID Number": customer_data[2],
            "Age": customer_data[3]
        },
        "Address Information": {
            "State": customer_data[6],
            "City": customer_data[7],
            "Street": customer_data[8]
        },
        "Financial Information": {
            "Credit Score": customer_data[4],
            "Loans Taken": customer_data[5]
        },
        "Customer Segment": {
            "Description": segment_data[1]
        },
        "Accounts": formatted_accounts_data,
        "Transactions": formatted_transactions_data,
    }

    return response_data

@app.get("/")
async def root():
    return {"message": "Welcome to Banking Customer data API endpoints"}


__all__ = [
    "app",
    "start_fastapi",
    "is_process_running",
    "is_lock_file_valid",
    "remove_lock_file",
]



if __name__ == "__main__":
    asyncio.run(start_fastapi())