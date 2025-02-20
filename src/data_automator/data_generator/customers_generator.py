import os
import random
import time

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from logger import log

from src.config.config import fake

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR", "../../../data")
CUSTOMERS_FILE = os.path.join(DATA_DIR, "customers.csv")
MIN_SLEEP_TIME_CUSTOMERS = int(os.getenv("MIN_SLEEP_TIME_CUSTOMERS", 300))
MAX_SLEEP_TIME_CUSTOMERS = int(os.getenv("MAX_SLEEP_TIME_CUSTOMERS", 600))
MAX_BATCH_CUSTOMERS = int(os.getenv("MAX_BATCH_CUSTOMERS", 100))


def run_customers_generator():
    while True:
        sleep_time = random.randint(MIN_SLEEP_TIME_CUSTOMERS, MAX_SLEEP_TIME_CUSTOMERS)
        batch_size = random.randint(1, MAX_BATCH_CUSTOMERS)
        if os.path.exists(CUSTOMERS_FILE):
            df = pd.read_csv(CUSTOMERS_FILE)
            max_id = df["CustomerID"].max()
        else:
            max_id = 100000 - 1
        new_ids = np.arange(max_id + 1, max_id + 1 + batch_size)
        new_customers = pd.DataFrame({
            "CustomerID": new_ids,
            "FirstName": [fake.first_name() for _ in range(batch_size)],
            "LastName": [fake.last_name() for _ in range(batch_size)],
            "SSN": [fake.ssn() for _ in range(batch_size)],
            "Gender": np.random.choice(["Male", "Female"], batch_size),
            "DateOfBirth": [fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d') for _ in range(batch_size)],
            "Email": [fake.email() for _ in range(batch_size)],
            "PhoneNumber": [fake.phone_number() for _ in range(batch_size)],
            "StreetAddress": [fake.street_address() for _ in range(batch_size)],
            "City": [fake.city() for _ in range(batch_size)],
            "State": [fake.state() for _ in range(batch_size)],
            "ZipCode": [fake.zipcode() for _ in range(batch_size)],
            "AccountCreated": [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d') for _ in range(batch_size)],
            "EmploymentStatus": np.random.choice(["Employed", "Unemployed", "Retired", "Student", "Self-Employed"], batch_size),
            "Occupation": [fake.job() for _ in range(batch_size)],
            "Employer": [fake.company() for _ in range(batch_size)],
            "AnnualIncome": np.random.choice(["<25K", "25K-50K", "50K-100K", "100K-250K", ">250K"], batch_size),
            "MaritalStatus": np.random.choice(["Single", "Married", "Divorced", "Widowed"], batch_size),
            "Nationality": np.random.choice(["USA", "Canada", "UK", "Germany", "India", "China", "France", "Australia"], batch_size),
            "CreditScore": np.random.randint(300, 851, batch_size),
            "RiskRating": np.random.randint(1, 6, batch_size),
            "CustomerSegment": np.random.choice(["Retail", "SME", "Corporate", "High Net Worth"], batch_size),
            "KYCStatus": np.random.choice(["Verified", "Pending", "Not Verified"], batch_size),
            "AMLFlag": np.random.choice([True, False], batch_size),
            "LoyaltyProgramStatus": np.random.choice(["Active", "Inactive", "Not Enrolled"], batch_size),
            "RewardPoints": np.random.randint(0, 10000, batch_size),
            "ChurnProbability": np.round(np.random.uniform(0, 1, batch_size), 2),
            "PreferredBranch": [f"BR{random.randint(100, 999)}" for _ in range(batch_size)],
            "LastLoginDate": [fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d') for _ in range(batch_size)],
            "CustomerRating": np.round(np.random.uniform(1, 5, batch_size), 2)
        })
        log.debug("New customers batch shape: %s", new_customers.shape)
        if os.path.exists(CUSTOMERS_FILE):
            existing = pd.read_csv(CUSTOMERS_FILE)
            updated = pd.concat([existing, new_customers], ignore_index=True)
        else:
            updated = new_customers
        updated.to_csv(CUSTOMERS_FILE, index=False)
        log.info("Added %d new customers. Sleeping for %d seconds.", batch_size, sleep_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_customers_generator()
