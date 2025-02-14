import pandas as pd
import numpy as np
import os
import time
import random
import logging
from faker import Faker
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Faker and set random seed
fake = Faker()
np.random.seed(42)

# Load configurations from environment variables
DATA_DIR = os.getenv("DATA_DIR", "/data")
TRANSACTIONS_FILE = os.path.join(DATA_DIR, "transactions.csv")
ACCOUNTS_FILE = os.path.join(DATA_DIR, "accounts.csv")
MAX_TRANSACTIONS_PER_BATCH = int(os.getenv("MAX_TRANSACTIONS_PER_BATCH", 1000))
MIN_SLEEP_TIME = int(os.getenv("MIN_SLEEP_TIME", 30))  # Min seconds between writes
MAX_SLEEP_TIME = int(os.getenv("MAX_SLEEP_TIME", 300))  # Max seconds between writes

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Function to generate transactions
def generate_transactions():
    if not os.path.exists(ACCOUNTS_FILE):
        logging.error("⚠️ Accounts file missing! Ensure accounts dataset is generated first.")
        return pd.DataFrame()

    accounts_df = pd.read_csv(ACCOUNTS_FILE)
    account_ids = accounts_df["AccountID"].tolist()

    transaction_count = random.randint(1, MAX_TRANSACTIONS_PER_BATCH)  # Random batch size
    transaction_ids = np.arange(1000000, 1000000 + transaction_count)

    transactions = pd.DataFrame({
        "TransactionID": transaction_ids,
        "AccountID": np.random.choice(account_ids, transaction_count),
        "TransactionType": np.random.choice(["Deposit", "Withdrawal", "Transfer", "Payment", "Investment"], transaction_count),
        "Amount": np.round(np.random.uniform(10, 5000, transaction_count), 2),
        "TransactionDate": [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * transaction_count,
        "Status": np.random.choice(["Completed", "Pending", "Failed"], transaction_count, p=[0.9, 0.08, 0.02])
    })

    return transactions

# Function to continuously generate transactions at random intervals
def run_transaction_generator():
    logging.info("🚀 Transaction generator started on Raspberry Pi. Writing transactions at random intervals...")

    while True:
        sleep_time = random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME)  # Random sleep duration

        # Generate new transactions
        new_transactions = generate_transactions()

        if not new_transactions.empty:
            # Append to the existing transactions file
            if os.path.exists(TRANSACTIONS_FILE):
                existing_transactions = pd.read_csv(TRANSACTIONS_FILE)
                updated_transactions = pd.concat([existing_transactions, new_transactions], ignore_index=True)
            else:
                updated_transactions = new_transactions

            updated_transactions.to_csv(TRANSACTIONS_FILE, index=False)

            logging.info(f"✅ Added {len(new_transactions)} transactions. Sleeping for {sleep_time} seconds...")

        # Sleep before next batch
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_transaction_generator()
