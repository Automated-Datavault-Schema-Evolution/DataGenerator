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
DEPOTS_FILE = os.path.join(DATA_DIR, "depots.csv")
CUSTOMERS_FILE = os.path.join(DATA_DIR, "customers.csv")
MIN_SLEEP_TIME_DEPOTS = int(os.getenv("MIN_SLEEP_TIME_DEPOTS", 600))
MAX_SLEEP_TIME_DEPOTS = int(os.getenv("MAX_SLEEP_TIME_DEPOTS", 900))
MAX_BATCH_DEPOTS = int(os.getenv("MAX_BATCH_DEPOTS", 20))



def run_depots_generator():
    while True:
        sleep_time = random.randint(MIN_SLEEP_TIME_DEPOTS, MAX_SLEEP_TIME_DEPOTS)
        batch_size = random.randint(1, MAX_BATCH_DEPOTS)
        if not os.path.exists(CUSTOMERS_FILE):
            log.error("Customers file missing! Generate customers first.")
            time.sleep(sleep_time)
            continue
        customers_df = pd.read_csv(CUSTOMERS_FILE)
        customer_ids = customers_df["CustomerID"].tolist()
        if os.path.exists(DEPOTS_FILE):
            depots_df = pd.read_csv(DEPOTS_FILE)
            max_id = depots_df["DepotID"].max()
        else:
            max_id = 1000000 - 1
        new_ids = np.arange(max_id + 1, max_id + 1 + batch_size)
        new_depots = pd.DataFrame({
            "DepotID": new_ids,
            "CustomerID": np.random.choice(customer_ids, batch_size),
            "DepotType": np.random.choice(["Standard", "Premium", "Gold"], batch_size),
            "OpeningDate": [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d') for _ in range(batch_size)],
            "Status": np.random.choice(["Active", "Inactive", "Closed"], batch_size),
            "TotalValue": np.round(np.random.uniform(1000, 500000, batch_size), 2),
            "Custodian": [fake.company() for _ in range(batch_size)],
            "NumberOfSecurities": np.random.randint(1, 50, batch_size)
        })
        new_depots["ValuePerSecurity"] = new_depots.apply(lambda row: row["TotalValue"] / row["NumberOfSecurities"]
                                                            if row["NumberOfSecurities"] > 0 else np.nan, axis=1)
        log.debug("New depots batch shape: %s", new_depots.shape)
        if os.path.exists(DEPOTS_FILE):
            existing = pd.read_csv(DEPOTS_FILE)
            updated = pd.concat([existing, new_depots], ignore_index=True)
        else:
            updated = new_depots
        updated.to_csv(DEPOTS_FILE, index=False)
        log.info("Added %d new depot records. Sleeping for %d seconds.", batch_size, sleep_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_depots_generator()
