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
DIGITAL_FILE = os.path.join(DATA_DIR, "digital_interactions.csv")
CUSTOMERS_FILE = os.path.join(DATA_DIR, "customers.csv")
MIN_SLEEP_TIME_DIGITAL = int(os.getenv("MIN_SLEEP_TIME_DIGITAL", 30))
MAX_SLEEP_TIME_DIGITAL = int(os.getenv("MAX_SLEEP_TIME_DIGITAL", 120))
MAX_BATCH_DIGITAL = int(os.getenv("MAX_BATCH_DIGITAL", 200))


def run_digital_generator():
    while True:
        sleep_time = random.randint(MIN_SLEEP_TIME_DIGITAL, MAX_SLEEP_TIME_DIGITAL)
        batch_size = random.randint(1, MAX_BATCH_DIGITAL)
        if not os.path.exists(CUSTOMERS_FILE):
            log.error("Customers file missing! Generate customers first.")
            time.sleep(sleep_time)
            continue
        customers_df = pd.read_csv(CUSTOMERS_FILE)
        customer_ids = customers_df["CustomerID"].tolist()
        session_ids = np.arange(600000, 600000 + batch_size)
        new_digital = pd.DataFrame({
            "SessionID": session_ids,
            "CustomerID": np.random.choice(customer_ids, batch_size),
            "LoginTime": [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(batch_size)],
            "DeviceType": np.random.choice(["Desktop", "Mobile", "Tablet"], batch_size),
            "Browser": np.random.choice(["Chrome", "Firefox", "Safari", "Edge", "Opera"], batch_size),
            "IPAddress": [fake.ipv4() for _ in range(batch_size)]
        })
        new_digital["LogoutTime"] = new_digital["LoginTime"].apply(
            lambda x: (x + pd.Timedelta(minutes=random.randint(5, 120))).strftime('%Y-%m-%d %H:%M:%S')
        )
        new_digital["LoginTime"] = new_digital["LoginTime"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        log.debug("New digital interactions batch shape: %s", new_digital.shape)
        if os.path.exists(DIGITAL_FILE):
            existing = pd.read_csv(DIGITAL_FILE)
            updated = pd.concat([existing, new_digital], ignore_index=True)
        else:
            updated = new_digital
        updated.to_csv(DIGITAL_FILE, index=False)
        log.info("Added %d new digital interaction sessions. Sleeping for %d seconds.", batch_size, sleep_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_digital_generator()
