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
MARKETING_FILE = os.path.join(DATA_DIR, "marketing.csv")
CUSTOMERS_FILE = os.path.join(DATA_DIR, "customers.csv")
MIN_SLEEP_TIME_MARKETING = int(os.getenv("MIN_SLEEP_TIME_MARKETING", 120))
MAX_SLEEP_TIME_MARKETING = int(os.getenv("MAX_SLEEP_TIME_MARKETING", 240))
MAX_BATCH_MARKETING = int(os.getenv("MAX_BATCH_MARKETING", 10))


def run_marketing_generator():
    while True:
        sleep_time = random.randint(MIN_SLEEP_TIME_MARKETING, MAX_SLEEP_TIME_MARKETING)
        batch_size = random.randint(1, MAX_BATCH_MARKETING)
        if not os.path.exists(CUSTOMERS_FILE):
            log.error("Customers file missing! Generate customers first.")
            time.sleep(sleep_time)
            continue
        customers_df = pd.read_csv(CUSTOMERS_FILE)
        customer_ids = customers_df["CustomerID"].tolist()
        if os.path.exists(MARKETING_FILE):
            marketing_df = pd.read_csv(MARKETING_FILE)
            max_id = marketing_df["CampaignID"].max()
        else:
            max_id = 500000 - 1
        new_ids = np.arange(max_id + 1, max_id + 1 + batch_size)
        new_marketing = pd.DataFrame({
            "CampaignID": new_ids,
            "CustomerID": np.random.choice(customer_ids, batch_size),
            "CampaignType": np.random.choice(["Email", "SMS", "Social Media", "Direct Mail"], batch_size),
            "CampaignDate": [fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d') for _ in range(batch_size)],
            "Response": np.random.choice(["Positive", "Negative", "Neutral", "No Response"], batch_size),
            "OfferAccepted": np.random.choice([True, False], batch_size),
            "CampaignName": [f"Campaign {random.randint(1000, 9999)}" for _ in range(batch_size)],
            "CampaignBudget": np.round(np.random.uniform(1000, 10000, batch_size), 2),
            "Impressions": np.random.randint(1000, 100000, batch_size),
            "Clicks": np.random.randint(10, 10000, batch_size)
        })
        new_marketing["ConversionRate"] = np.round(np.random.uniform(0, 1, batch_size), 2)
        new_marketing["Cost"] = np.round(np.random.uniform(100, 1000, batch_size), 2)
        log.debug("New marketing batch shape: %s", new_marketing.shape)
        if os.path.exists(MARKETING_FILE):
            existing = pd.read_csv(MARKETING_FILE)
            updated = pd.concat([existing, new_marketing], ignore_index=True)
        else:
            updated = new_marketing
        updated.to_csv(MARKETING_FILE, index=False)
        log.info("Added %d new marketing campaigns. Sleeping for %d seconds.", batch_size, sleep_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_marketing_generator()
