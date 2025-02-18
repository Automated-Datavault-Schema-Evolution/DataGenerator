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
SHARES_FILE = os.path.join(DATA_DIR, "shares.csv")
CUSTOMERS_FILE = os.path.join(DATA_DIR, "customers.csv")
MIN_SLEEP_TIME_SHARES = int(os.getenv("MIN_SLEEP_TIME_SHARES", 60))
MAX_SLEEP_TIME_SHARES = int(os.getenv("MAX_SLEEP_TIME_SHARES", 180))
MAX_BATCH_SHARES = int(os.getenv("MAX_BATCH_SHARES", 100))


def run_shares_generator():
    while True:
        sleep_time = random.randint(MIN_SLEEP_TIME_SHARES, MAX_SLEEP_TIME_SHARES)
        batch_size = random.randint(1, MAX_BATCH_SHARES)
        if not os.path.exists(CUSTOMERS_FILE):
            log.error("Customers file missing! Generate customers first.")
            time.sleep(sleep_time)
            continue
        customers_df = pd.read_csv(CUSTOMERS_FILE)
        customer_ids = customers_df["CustomerID"].tolist()
        if os.path.exists(SHARES_FILE):
            shares_df = pd.read_csv(SHARES_FILE)
            max_id = shares_df["ShareID"].max()
        else:
            max_id = 900000 - 1
        new_ids = np.arange(max_id + 1, max_id + 1 + batch_size)
        stocks = [
            ("AAPL", "Apple Inc.", "Technology", "NASDAQ"),
            ("GOOGL", "Alphabet Inc.", "Technology", "NASDAQ"),
            ("MSFT", "Microsoft Corp.", "Technology", "NASDAQ"),
            ("AMZN", "Amazon.com Inc.", "Consumer Discretionary", "NASDAQ"),
            ("TSLA", "Tesla Inc.", "Consumer Discretionary", "NASDAQ"),
            ("NFLX", "Netflix Inc.", "Communication Services", "NASDAQ"),
            ("FB", "Meta Platforms", "Communication Services", "NASDAQ"),
            ("NVDA", "NVIDIA Corp.", "Technology", "NASDAQ"),
            ("BABA", "Alibaba Group", "Consumer Discretionary", "NYSE"),
            ("ORCL", "Oracle Corp.", "Technology", "NYSE")
        ]
        stock_details = [random.choice(stocks) for _ in range(batch_size)]
        new_shares = pd.DataFrame({
            "ShareID": new_ids,
            "CustomerID": np.random.choice(np.array(customer_ids).flatten(), batch_size),
            "StockSymbol": [s[0] for s in stock_details],
            "StockName": [s[1] for s in stock_details],
            "Sector": [s[2] for s in stock_details],
            "Exchange": [s[3] for s in stock_details],
            "Quantity": np.random.randint(1, 1000, batch_size),
            "PurchasePrice": np.round(np.random.uniform(10, 500, batch_size), 2),
            "CurrentPrice": np.round(np.random.uniform(10, 500, batch_size), 2),
            "PurchaseDate": [fake.date_between(start_date='-3y', end_date='today').strftime('%Y-%m-%d') for _ in range(batch_size)]
        })
        new_shares["TotalValue"] = new_shares["Quantity"] * new_shares["CurrentPrice"]
        log.debug("New shares batch shape: %s", new_shares.shape)
        if os.path.exists(SHARES_FILE):
            existing = pd.read_csv(SHARES_FILE)
            updated = pd.concat([existing, new_shares], ignore_index=True)
        else:
            updated = new_shares
        updated.to_csv(SHARES_FILE, index=False)
        log.info("Added %d new share records. Sleeping for %d seconds.", batch_size, sleep_time)
        time.sleep(sleep_time)

if __name__ == "__main__":
    run_shares_generator()
