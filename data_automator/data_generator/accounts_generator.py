import os
import random
import time

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from logger import log

from ..config import fake

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR", "../../data")
ACCOUNTS_FILE = os.path.join(DATA_DIR, "accounts.csv")
CUSTOMERS_FILE = os.path.join(DATA_DIR, "customers.csv")
MIN_SLEEP_TIME_ACCOUNTS = int(os.getenv("MIN_SLEEP_TIME_ACCOUNTS", 180))
MAX_SLEEP_TIME_ACCOUNTS = int(os.getenv("MAX_SLEEP_TIME_ACCOUNTS", 300))
MAX_BATCH_ACCOUNTS = int(os.getenv("MAX_BATCH_ACCOUNTS", 50))

def run_accounts_generator():
    while True:
        sleep_time = random.randint(MIN_SLEEP_TIME_ACCOUNTS, MAX_SLEEP_TIME_ACCOUNTS)
        batch_size = random.randint(1, MAX_BATCH_ACCOUNTS)
        if not os.path.exists(CUSTOMERS_FILE):
            log.error("Customers file missing! Generate customers first.")
            time.sleep(sleep_time)
            continue
        customers_df = pd.read_csv(CUSTOMERS_FILE)
        customer_ids = customers_df["CustomerID"].tolist()
        if os.path.exists(ACCOUNTS_FILE):
            accounts_df = pd.read_csv(ACCOUNTS_FILE)
            max_id = accounts_df["AccountID"].max()
        else:
            max_id = 200000 - 1
        new_ids = np.arange(max_id + 1, max_id + 1 + batch_size)
        opened_dates = [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d') for _ in range(batch_size)]
        new_accounts = pd.DataFrame({
            "AccountID": new_ids,
            "CustomerID": np.random.choice(customer_ids, batch_size),
            "AccountType": np.random.choice(["Savings", "Checking", "Business", "Investment"], batch_size),
            "Balance": np.round(np.random.uniform(-23000, 991234, batch_size), 2),
            "OpenedDate": opened_dates,
            "Status": np.random.choice(["Active", "Inactive", "Closed"], batch_size),
            "Currency": np.random.choice(["USD", "CAD", "EUR", "GBP"], batch_size),
            "BranchCode": [f"BR{random.randint(100, 999)}" for _ in range(batch_size)],
            "InterestRate": np.round(np.random.uniform(0.1, 5.0, batch_size), 2),
            "AccountSubType": np.random.choice(["Basic", "Premium", "Gold", "Platinum"], batch_size),
            "OverdraftLimit": np.round(np.random.uniform(0, 5000, batch_size), 2),
            "LastTransactionDate": [fake.date_between(start_date=d, end_date='today').strftime('%Y-%m-%d') for d in opened_dates]
        })
        log.debug("New accounts batch shape: %s", new_accounts.shape)
        if os.path.exists(ACCOUNTS_FILE):
            existing = pd.read_csv(ACCOUNTS_FILE)
            updated = pd.concat([existing, new_accounts], ignore_index=True)
        else:
            updated = new_accounts
        updated.to_csv(ACCOUNTS_FILE, index=False)
        log.info("Added %d new accounts. Sleeping for %d seconds.", batch_size, sleep_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_accounts_generator()
