import os
import random
import time

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from faker import Faker
from logger import log

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR", "../../../data")
LOANS_FILE = os.path.join(DATA_DIR, "loans.csv")
CUSTOMERS_FILE = os.path.join(DATA_DIR, "customers.csv")
MIN_SLEEP_TIME_LOANS = int(os.getenv("MIN_SLEEP_TIME_LOANS", 600))
MAX_SLEEP_TIME_LOANS = int(os.getenv("MAX_SLEEP_TIME_LOANS", 900))
MAX_BATCH_LOANS = int(os.getenv("MAX_BATCH_LOANS", 20))

fake = Faker()
np.random.seed(42)
os.makedirs(DATA_DIR, exist_ok=True)

def run_loans_generator():
    while True:
        sleep_time = random.randint(MIN_SLEEP_TIME_LOANS, MAX_SLEEP_TIME_LOANS)
        batch_size = random.randint(1, MAX_BATCH_LOANS)
        if not os.path.exists(CUSTOMERS_FILE):
            log.error("Customers file missing! Generate customers first.")
            time.sleep(sleep_time)
            continue
        customers_df = pd.read_csv(CUSTOMERS_FILE)
        customer_ids = customers_df["CustomerID"].tolist()
        if os.path.exists(LOANS_FILE):
            loans_df = pd.read_csv(LOANS_FILE)
            max_id = loans_df["LoanID"].max()
        else:
            max_id = 400000 - 1
        new_ids = np.arange(max_id + 1, max_id + 1 + batch_size)
        approval_dates = [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d') for _ in range(batch_size)]
        new_loans = pd.DataFrame({
            "LoanID": new_ids,
            "CustomerID": np.random.choice(customer_ids, batch_size),
            "LoanType": np.random.choice(["Personal", "Mortgage", "Auto", "Business"], batch_size),
            "LoanAmount": np.round(np.random.uniform(5000, 500000, batch_size), 2),
            "InterestRate": np.round(np.random.uniform(2.5, 10.5, batch_size), 2),
            "LoanTermYears": np.random.choice([5, 10, 15, 20, 25, 30], batch_size),
            "ApprovalDate": approval_dates,
            "Status": np.random.choice(["Active", "Closed", "Default"], batch_size),
            "CollateralType": np.random.choice(["Property", "Vehicle", "Equipment", "None"], batch_size)
        })
        new_loans["CollateralValue"] = new_loans["CollateralType"].apply(
            lambda x: np.round(np.random.uniform(1000, 300000), 2) if x != "None" else 0.0
        )
        new_loans["LoanProductSubType"] = new_loans["LoanType"].apply(
            lambda lt: random.choice(["Fixed", "Adjustable"]) if lt == "Mortgage" else
            random.choice(["Secured", "Unsecured"]) if lt == "Personal" else
            random.choice(["New", "Used"]) if lt == "Auto" else
            random.choice(["Small Business", "Corporate"])
        )
        new_loans["MonthlyPayment"] = np.round(new_loans["LoanAmount"] / (new_loans["LoanTermYears"] * 12) * np.random.uniform(0.9, 1.1, batch_size), 2)
        new_loans["OutstandingBalance"] = new_loans.apply(lambda row: np.round(np.random.uniform(0, row["LoanAmount"]), 2), axis=1)
        new_loans["LoanStartDate"] = pd.to_datetime(new_loans["ApprovalDate"]) + pd.to_timedelta(np.random.randint(0, 30, batch_size), unit='d')
        new_loans["LoanEndDate"] = new_loans.apply(lambda row: row["LoanStartDate"] + pd.DateOffset(years=int(row["LoanTermYears"])), axis=1)
        new_loans["LoanStartDate"] = new_loans["LoanStartDate"].dt.strftime('%Y-%m-%d')
        new_loans["LoanEndDate"] = new_loans["LoanEndDate"].dt.strftime('%Y-%m-%d')
        log.debug("New loans batch shape: %s", new_loans.shape)
        if os.path.exists(LOANS_FILE):
            existing = pd.read_csv(LOANS_FILE)
            updated = pd.concat([existing, new_loans], ignore_index=True)
        else:
            updated = new_loans
        updated.to_csv(LOANS_FILE, index=False)
        log.info("Added %d new loans. Sleeping for %d seconds.", batch_size, sleep_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_loans_generator()
