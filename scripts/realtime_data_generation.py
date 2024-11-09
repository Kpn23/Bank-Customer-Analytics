from datetime import datetime
import random
import sqlite3
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)


def load_data_from_database(database_db_path):
    """Load data from the specified SQLite database into Pandas DataFrames."""
    conn = sqlite3.connect(database_db_path)
    logging.info(f"Successfully connected to database at {database_db_path}")

    # Define the tables to retrieve
    tables = ["accounts", "transactions", "recipients", "campaigns"]
    dataframes = {}

    for table in tables:
        try:
            # Load each table into a DataFrame
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            if table == "campaigns":
                # Convert end_date to datetime
                df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
            dataframes[table] = df
            logging.info(f"Successfully retrieved data from {table}.")
        except Exception as e:
            logging.error(f"Error retrieving data from {table}: {e}")

    conn.close()
    return dataframes


def get_max_ids(accounts, transactions, recipients):
    """Get the maximum IDs for accounts, transactions, and recipients."""
    max_account_id = accounts["account_id"].max() if not accounts.empty else 0
    max_transaction_id = (
        transactions["transaction_id"].max() if not transactions.empty else 0
    )
    max_recipient_id = recipients["recipient_id"].max() if not recipients.empty else 0
    return max_account_id, max_transaction_id, max_recipient_id


def generate_new_transactions(
    accounts, transactions, database_path, num_updates=3, NUM_MERCHANTS=100
):
    """Generate new transactions for multiple accounts and update their balances."""
    max_account_id, max_transaction_id, _ = get_max_ids(
        accounts, transactions, pd.DataFrame()
    )

    # Select multiple accounts to update
    accounts_to_update = accounts.sample(n=num_updates)

    transaction_list = []

    for _, account_to_update in accounts_to_update.iterrows():
        transaction_amount = round(random.uniform(-5000, 5000), 2)

        # Check if account_type_id is 1 and ensure balance does not go below 0
        if account_to_update["account_type_id"] == 1:
            if account_to_update["balance"] + transaction_amount < 0:
                # Adjust transaction amount to prevent negative balance
                transaction_amount = -account_to_update["balance"]

        # Update account balance
        updated_balance = account_to_update["balance"] + transaction_amount

        accounts.loc[
            accounts["account_id"] == account_to_update["account_id"], "balance"
        ] = updated_balance

        # Create new transaction record
        new_transaction = {
            "transaction_id": max_transaction_id + 1,
            "account_id": account_to_update["account_id"],
            "transaction_amount": transaction_amount,
            "transaction_date": datetime.now().isoformat(),
            "transaction_type": "Deposit" if transaction_amount > 0 else "Withdraw",
            "merchant_id": random.randint(1, NUM_MERCHANTS),
        }

        transaction_list.append(new_transaction)
        max_transaction_id += 1  # Increment transaction ID for next entry

    # Save all new transactions to database at once
    save_to_database("transactions", transaction_list, database_path)
    # Save updated accounts back to the database
    save_to_database("accounts", accounts, database_path)

    return transaction_list, accounts


def generate_new_recipient(campaigns, recipients, database_path):
    campaigns["end_date"] = pd.to_datetime(campaigns["end_date"], format="ISO8601")
    # campaigns["end_date"] = pd.to_datetime(campaigns["end_date"], errors="coerce")
    """Generate new recipients based on ongoing campaigns."""
    ongoing_campaigns = campaigns[campaigns["end_date"] > datetime.now()]

    recipient_list = []  # Initialize recipient list

    for _, campaign in ongoing_campaigns.iterrows():
        signed_up_customers = recipients[
            (recipients["campaign_id"] == campaign["campaign_id"])
            & (recipients["response_status"] == "Signed up")
            & (
                ~recipients["customer_id"].isin(
                    recipients[recipients["response_status"] != "Unsubscribed"]
                )
            )
        ]

        percentage = 0.01
        num_customers_to_select = int(len(signed_up_customers) * percentage)

        if num_customers_to_select > 0:
            # Randomly sample customers
            random_customers = signed_up_customers.sample(n=num_customers_to_select)

            for _, customer in random_customers.iterrows():
                new_recipient = {
                    "recipient_id": len(recipients) + 1,
                    "customer_id": customer["customer_id"],
                    "campaign_id": campaign["campaign_id"],
                    "response_status": "Signed up",
                    "response_date": datetime.now().isoformat(),
                }
                recipient_list.append(new_recipient)

    # Save updated recipients to database only if there are new entries
    if recipient_list:
        save_to_database("recipients", recipient_list, database_path)
    return recipient_list


def save_to_database(table_name, dataframe, database_path):
    """Append DataFrame records to the specified SQLite table."""
    conn = sqlite3.connect(database_path)

    try:
        # Ensure dataframe is a DataFrame before saving
        if isinstance(dataframe, list):
            dataframe = pd.DataFrame(dataframe)

        dataframe.to_sql(table_name, conn, if_exists="append", index=False)
        logging.info(f"Successfully appended records to {table_name}.")

        # If saving accounts, use 'replace' instead of 'append'
        if table_name == "accounts":
            dataframe.to_sql(table_name, conn, if_exists="replace", index=False)
            logging.info(f"Successfully updated records in {table_name}.")

    except Exception as e:
        logging.error(f"Error appending records to {table_name}: {e}")

    conn.close()


__all__ = [
    "load_data_from_database",
    "get_max_ids",
    "generate_new_transactions",
    "generate_new_recipient",
    "save_to_database",
]


# if __name__ == "__main__":
#     from dotenv import load_dotenv
#     import os
#     import logging
#     import sys

#     logging.basicConfig(level=logging.INFO)

#     load_dotenv()
#     folder_directory = os.getenv("folder_path")

#     # Load data from database
#     database_path = f"{folder_directory}/data/bank_customer_data.db"
#     dataframes = load_data_from_database(database_path)  # Correctly load dataframes

#     # Generate new transactions and update balances
#     transactions, updated_accounts = generate_new_transactions(
#         dataframes["accounts"], dataframes["transactions"], database_path
#     )

#     # Generate new recipients based on ongoing campaigns
#     new_recipients = generate_new_recipient(
#         dataframes["campaigns"], dataframes["recipients"], database_path
#     )

#     print("New Transactions:", transactions)
#     print("Updated Accounts:", updated_accounts)
#     print("New Recipients:", new_recipients)
