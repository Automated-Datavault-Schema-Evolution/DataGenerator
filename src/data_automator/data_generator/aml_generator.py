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
AML_FILE = os.path.join(DATA_DIR, "aml_compliance.csv")
CUSTOMERS_FILE = os.path.join(DATA_DIR, "customers.csv")
MIN_SLEEP_TIME_AML = int(os.getenv("MIN_SLEEP_TIME_AML", 900))
MAX_SLEEP_TIME_AML = int(os.getenv("MAX_SLEEP_TIME_AML", 1800))
MAX_BATCH_AML = int(os.getenv("MAX_BATCH_AML", 30))


def run_aml_generator():
    while True:
        sleep_time = random.randint(MIN_SLEEP_TIME_AML, MAX_SLEEP_TIME_AML)
        batch_size = random.randint(1, MAX_BATCH_AML)
        if not os.path.exists(CUSTOMERS_FILE):
            log.error("Customers file missing! Generate customers first.")
            time.sleep(sleep_time)
            continue
        customers_df = pd.read_csv(CUSTOMERS_FILE)
        customer_ids = customers_df["CustomerID"].tolist()
        if os.path.exists(AML_FILE):
            aml_df = pd.read_csv(AML_FILE)
            max_id = aml_df["AMLRecordID"].max()
        else:
            max_id = 1100000 - 1
        new_ids = np.arange(max_id + 1, max_id + 1 + batch_size)
        report_filed = np.random.choice([True, False], batch_size)
        filing_dates = [fake.date_between(start_date='-3y', end_date='today').strftime('%Y-%m-%d') if filed else "" for filed in report_filed]
        new_aml = pd.DataFrame({
            "AMLRecordID": new_ids,
            "CustomerID": np.random.choice(customer_ids, batch_size),
            "Regulation": np.random.choice(["IFRS", "FATCA", "CRS"], batch_size),
            "ComplianceStatus": np.random.choice(["Compliant", "Non-Compliant", "Under Review"], batch_size),
            "InvestigationStatus": np.random.choice(["Cleared", "Investigating", "Escalated", "Not Applicable"], batch_size),
            "SuspicionScore": np.round(np.random.uniform(0, 100, batch_size), 2),
            "ReportFiled": report_filed,
            "FilingDate": filing_dates,
            "HighRiskJurisdiction": np.random.choice([True, False], batch_size),
            "OffshoreAccountFlag": np.random.choice([True, False], batch_size),
            "Comments": [fake.sentence(nb_words=6) for _ in range(batch_size)],
            "LastUpdated": [fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d') for _ in range(batch_size)]
        })
        log.debug("New AML compliance batch shape: %s", new_aml.shape)
        if os.path.exists(AML_FILE):
            existing = pd.read_csv(AML_FILE)
            updated = pd.concat([existing, new_aml], ignore_index=True)
        else:
            updated = new_aml
        updated.to_csv(AML_FILE, index=False)
        log.info("Added %d new AML compliance records. Sleeping for %d seconds.", batch_size, sleep_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_aml_generator()
