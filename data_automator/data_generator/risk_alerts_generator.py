import os
import random
import time

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from logger import log

from config import fake

load_dotenv()

DATA_DIR = os.getenv("DATA_DIR", "../../data")
RISK_ALERTS_FILE = os.path.join(DATA_DIR, "risk_alerts.csv")
CUSTOMERS_FILE = os.path.join(DATA_DIR, "customers.csv")
MIN_SLEEP_TIME_RISK_ALERTS = int(os.getenv("MIN_SLEEP_TIME_RISK_ALERTS", 300))
MAX_SLEEP_TIME_RISK_ALERTS = int(os.getenv("MAX_SLEEP_TIME_RISK_ALERTS", 600))
MAX_BATCH_RISK_ALERTS = int(os.getenv("MAX_BATCH_RISK_ALERTS", 50))


def run_risk_alerts_generator():
    while True:
        sleep_time = random.randint(MIN_SLEEP_TIME_RISK_ALERTS, MAX_SLEEP_TIME_RISK_ALERTS)
        batch_size = random.randint(1, MAX_BATCH_RISK_ALERTS)
        if not os.path.exists(CUSTOMERS_FILE):
            log.error("Customers file missing! Generate customers first.")
            time.sleep(sleep_time)
            continue
        customers_df = pd.read_csv(CUSTOMERS_FILE)
        customer_ids = customers_df["CustomerID"].tolist()
        if os.path.exists(RISK_ALERTS_FILE):
            risk_df = pd.read_csv(RISK_ALERTS_FILE)
            max_id = risk_df["AlertID"].max()
        else:
            max_id = 800000 - 1
        new_ids = np.arange(max_id + 1, max_id + 1 + batch_size)
        new_risk = pd.DataFrame({
            "AlertID": new_ids,
            "CustomerID": np.random.choice(customer_ids, batch_size),
            "RiskType": np.random.choice(["Fraud", "Money Laundering", "Cyber Attack", "Regulatory"], batch_size),
            "RiskScore": np.random.randint(1, 101, batch_size),
            "AlertDate": [fake.date_between(start_date='-3y', end_date='today').strftime('%Y-%m-%d') for _ in range(batch_size)],
            "ActionTaken": np.random.choice(["Investigated", "Resolved", "Pending", "Escalated"], batch_size),
            "ComplianceStatus": np.random.choice(["Compliant", "Non-Compliant", "Under Review"], batch_size)
        })
        log.debug("New risk alerts batch shape: %s", new_risk.shape)
        if os.path.exists(RISK_ALERTS_FILE):
            existing = pd.read_csv(RISK_ALERTS_FILE)
            updated = pd.concat([existing, new_risk], ignore_index=True)
        else:
            updated = new_risk
        updated.to_csv(RISK_ALERTS_FILE, index=False)
        log.info("Added %d new risk alerts. Sleeping for %d seconds.", batch_size, sleep_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_risk_alerts_generator()
