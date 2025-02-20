import random
import time

import numpy as np
import pandas as pd
from logger import log

from src.config.config import fake
from src.initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_shares(num_shares, customer_ids):
    log.info("Start generating shares")
    start_time = time.time()
    try:
        share_ids = np.arange(900000, 900000 + num_shares)
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
        # Use Python's random.choice to select stock details for each record
        stock_details = [random.choice(stocks) for _ in range(num_shares)]
        shares = pd.DataFrame({
            "ShareID": share_ids,
            "CustomerID": np.random.choice(np.array(customer_ids).flatten(), num_shares),
            "StockSymbol": [s[0] for s in stock_details],
            "StockName": [s[1] for s in stock_details],
            "Sector": [s[2] for s in stock_details],
            "Exchange": [s[3] for s in stock_details],
            "Quantity": np.random.randint(1, 1000, num_shares),
            "PurchasePrice": np.round(np.random.uniform(10, 500, num_shares), 2),
            "CurrentPrice": np.round(np.random.uniform(10, 500, num_shares), 2),
            "PurchaseDate": [fake.date_between(start_date='-3y', end_date='today').strftime('%Y-%m-%d')
                             for _ in range(num_shares)]
        })
        log.debug("Shares dataframe shape: %s", shares.shape)
        save_dataframe_in_chunks(shares, "shares", CHUNK_SIZE)
        log.info("Shares dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_shares: %s", e)