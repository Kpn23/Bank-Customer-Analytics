from faker import Faker
import random
import datetime
import sqlite3
import pandas as pd
import logging
from dotenv import load_dotenv
import os
import sys

load_dotenv()
folder_directory = os.getenv("folder_path")

sys.path.append(folder_directory)

# Set up logging
logging.basicConfig(level=logging.INFO)

# Step 1: Initialize Faker and Constants
fake = Faker()
NUM_CUSTOMERS = 1000
NUM_MERCHANTS = 100
NUM_ACCOUNT_TYPES = ["Savings", "Checking", "Business", "Investment"]
NUM_LOAN_TYPES = [
    "Personal",
    "Home",
    "Auto",
    "Education",
    "Business",
    "Debt Consolidation",
    "Medical",
    "Vacation",
    "Home Improvement",
    "Green Energy",
    "Emergency",
]
NUM_CAMPAIGNS = 10

# Step 2: Data Storage Initialization
customers = []
addresses = []
accounts = []
transactions = []
loans = []
merchants = []
campaigns = []
recipients = []
account_types = []

# Step 3: Generate Account Types
for i, account_type in enumerate(NUM_ACCOUNT_TYPES):
    account_types.append(
        {
            "account_type_id": i + 1,
            "account_type": account_type,
            "description": f"{account_type} account type",
        }
    )

# Step 4: Generate Merchants
for i in range(NUM_MERCHANTS):
    merchants.append(
        {
            "merchant_id": i + 1,
            "merchant_name": fake.company(),
            "merchant_category": random.choice(
                ["Retail", "Food", "Services", "Entertainment"]
            ),
        }
    )

# Step 5: Generate Customers and Related Data
for i in range(NUM_CUSTOMERS):
    customer_id = i + 1

    # Generate loans taken
    loans_taken = random.randint(0, 7)

    # Calculate credit score based on loans taken with some noise
    base_credit_score = loans_taken * 100
    noise = random.randint(-400, 400)  # Adding noise between -50 and 50
    credit_score = max(
        100, min(850, base_credit_score + noise)
    )  # Ensure credit score is within valid range

    customers.append(
        {
            "customer_id": customer_id,
            "customer_name": fake.name(),  # Generate a random customer name
            "id_number": fake.ssn(),  # Generate a random ID number (SSN in this case)
            "age": random.randint(18, 70),
            "gender": random.choice(["Male", "Female"]),
            "num_products": random.randint(1, 5),
            "credit_score": credit_score,
            "loans_taken": loans_taken,
        }
    )

    # Addresses for each Customer
    addresses.append(
        {
            "address_id": customer_id,
            "customer_id": customer_id,
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state(),
            "zipcode": fake.zipcode(),
        }
    )

    # Generate Accounts for each Customer with open date before transactions
    MIN_SAVINGS_BALANCE = 500
    num_accounts = random.randint(1, 3)
    for _ in range(num_accounts):
        open_date = fake.date_time_between(start_date="-5y", end_date="now")

        # Set an initial balance based on account type
        if account_type == "Savings":
            initial_balance = round(
                random.uniform(MIN_SAVINGS_BALANCE, 5000), 2
            )  # Ensure it starts above minimum
        else:
            initial_balance = round(
                random.uniform(0, 5000), 2
            )  # Other account types can start from $0

        account_data = {
            "account_id": len(accounts) + 1,
            "customer_id": customer_id,
            "account_type_id": random.randint(1, len(NUM_ACCOUNT_TYPES)),
            "balance": initial_balance,  # Set initial balance here
            "open_date": open_date,
        }

        accounts.append(account_data)

# Step 6: Generate Loans for Customers
for customer in customers:
    num_loans = customer["loans_taken"]
    for _ in range(num_loans):
        loan_data = {
            "loan_id": len(loans) + 1,
            "customer_id": customer["customer_id"],
            "loan_amount": round(random.uniform(1000, 50000), 2),
            "loan_type": random.choice(NUM_LOAN_TYPES),
            "loan_status": random.choice(["Active", "Paid Off"]),
        }
        loans.append(loan_data)

# Step 7: Generate Transactions Linked to Accounts with Constraints
MIN_SAVINGS_BALANCE = 100.00  # Minimum balance for Savings accounts

for account in accounts:
    num_transactions = random.randint(5, 20)
    transaction_sum = 0

    for _ in range(num_transactions):
        transaction_type = random.choice(["Deposit", "Withdraw"])
        if (
            account["account_type_id"] == 1
        ):  # Assuming Savings is the first account type
            # For Savings accounts, ensure balance doesn't go below MIN_SAVINGS_BALANCE
            if transaction_type == "Withdraw":
                max_withdrawal = max(0, account["balance"] - MIN_SAVINGS_BALANCE)
                transaction_amount = round(random.uniform(0, max_withdrawal), 2)
            else:
                transaction_amount = round(
                    random.uniform(0, 5000), 2
                )  # Deposits can be any amount
        else:
            # For other account types, allow full range of transactions
            transaction_amount = round(random.uniform(-5000, 5000), 2)

        transaction_date = fake.date_time_between(
            start_date=account["open_date"], end_date="now"
        )

        transactions.append(
            {
                "transaction_id": len(transactions) + 1,
                "account_id": account["account_id"],
                "transaction_amount": transaction_amount,
                "transaction_date": transaction_date,
                "transaction_type": transaction_type,
                "merchant_id": random.randint(1, NUM_MERCHANTS),
            }
        )

        transaction_sum += transaction_amount

    # Update account balance after all transactions are generated for that account
    account["balance"] = round(transaction_sum, 2)

    # Ensure the balance does not fall below the minimum for Savings accounts
    if account["account_type_id"] == 1 and account["balance"] < MIN_SAVINGS_BALANCE:
        logging.warning(
            f"Balance for Savings Account ID {account['account_id']} is below minimum."
        )
        account["balance"] = MIN_SAVINGS_BALANCE

# Step 8: Generate Campaigns and Recipients with Constraints on Dates and Response Statuses
for i in range(NUM_CAMPAIGNS):
    start_date = fake.date_time_between(start_date="-5y", end_date="now")
    end_date = start_date + datetime.timedelta(
        days=180
    )  # Campaign lasts for half a year

    campaigns.append(
        {
            "campaign_id": i + 1,
            "campaign_name": f"Campaign {i + 1}",
            "start_date": start_date,
            "end_date": end_date,
            "budget": round(random.uniform(1000, 10000), 2),
            "description": fake.text(max_nb_chars=200),
        }
    )

    selected_customers = random.sample(
        customers,
        random.randint(round(NUM_CUSTOMERS * 0.05), round(NUM_CUSTOMERS * 0.3)),
    )

    # Generate Recipients for each Campaign with response status constraints
    for customer in selected_customers:
        first_response_date = fake.date_time_between(
            start_date=start_date, end_date=end_date
        )

        recipients.append(
            {
                "recipient_id": len(recipients) + 1,
                "customer_id": customer["customer_id"],
                "campaign_id": campaigns[i]["campaign_id"],
                "response_status": "Signed up",
                "response_date": first_response_date,
            }
        )

        num_subsequent_responses = random.randint(0, 50)
        last_response_date = first_response_date

        for _ in range(num_subsequent_responses):
            next_response_date = fake.date_time_between(
                start_date=last_response_date, end_date=end_date
            )
            last_response_status = random.choice(["Participated", "Unsubscribed"])

            recipients.append(
                {
                    "recipient_id": len(recipients) + 1,
                    "customer_id": customer["customer_id"],
                    "campaign_id": campaigns[i]["campaign_id"],
                    "response_status": last_response_status,
                    "response_date": next_response_date,
                }
            )

            last_response_date = next_response_date

            if last_response_status == "Unsubscribed":
                break


# Step 9: Store Data in SQLite Database Using Pandas
def save_to_database(database_db_path):
    conn = sqlite3.connect(database_db_path)
    logging.info(f"Successfully connected to database at {database_db_path}")

    # Create DataFrames from generated data
    df_merchants = pd.DataFrame(merchants)
    df_customers = pd.DataFrame(customers)
    df_addresses = pd.DataFrame(addresses)
    df_accounts = pd.DataFrame(accounts)
    df_transactions = pd.DataFrame(transactions)
    df_loans = pd.DataFrame(loans)
    df_account_types = pd.DataFrame(account_types)
    df_campaigns = pd.DataFrame(campaigns)
    df_recipients = pd.DataFrame(recipients)

    dataframes = [
        df_merchants,
        df_customers,
        df_addresses,
        df_accounts,
        df_transactions,
        df_loans,
        df_account_types,
        df_campaigns,
        df_recipients,
    ]

    unique_id_columns = {
        "merchants": "merchant_id",
        "customers": "customer_id",
        "addresses": "address_id",
        "accounts": "account_id",
        "transactions": "transaction_id",
        "loans": "loan_id",
        "account_types": "account_type_id",
        "campaigns": "campaign_id",
        "recipients": "recipient_id",
    }

    for table_name, df in zip(unique_id_columns.keys(), dataframes):
        if isinstance(df, pd.DataFrame):
            try:
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                logging.info(
                    f"Successfully inserted data into {table_name} in the database."
                )
            except Exception as e:
                logging.error(f"Error saving {table_name} to the database: {e}")

    conn.close()


__all__ = [
    "generate_account_types",
    "generate_merchants",
    "generate_customers",
    "generate_accounts",
    "generate_loans",
    "generate_transactions",
    "generate_campaigns",
    "save_to_database",
]

# Main execution flow to save data to the database
database_file_path = f"{folder_directory}/data/bank_customer_data.db"
save_to_database(database_file_path)

# # Example Output of Generated Data (first few records)
# print("Customers:", customers[:5])
# print("Addresses:", addresses[:5])
# print("Accounts:", accounts[:5])
# print("Transactions:", transactions[:5])
# print("Loans:", loans[:5])
# print("Merchants:", merchants[:5])
# print("Account Types:", account_types[:5])
# print("Campaigns:", campaigns[:5])
# print("Recipients:", recipients[:5])
