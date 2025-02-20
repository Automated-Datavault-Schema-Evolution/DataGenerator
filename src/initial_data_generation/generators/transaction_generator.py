import time

import numpy as np
import pandas as pd
from logger import log

from src.config.config import fake
from src.initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_transactions(num_transactions_large, account_ids):
    log.info("Start generating transactions")
    start_time = time.time()
    try:
        transaction_ids = np.arange(700000, 700000 + num_transactions_large)
        amounts = np.round(np.random.uniform(10, 10000, num_transactions_large), 2)
        exchange_rates = np.round(np.random.uniform(0.8, 1.2, num_transactions_large), 2)
        transactions = pd.DataFrame({
            "TransactionID": transaction_ids,
            "AccountID": np.random.choice(account_ids, num_transactions_large),
            "TransactionType": np.random.choice(["Deposit", "Withdrawal", "Transfer", "Payment", "Investment"],
                                                num_transactions_large),
            "Amount": amounts,
            "TransactionDate": [fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d')
                                for _ in range(num_transactions_large)],
            "Status": np.random.choice(["Completed", "Pending", "Failed"], num_transactions_large),
            "Channel": np.random.choice(["ATM", "Online", "Branch", "Mobile"], num_transactions_large),
            "MerchantName": [fake.company() for _ in range(num_transactions_large)],
            "MerchantLocation": [fake.city() for _ in range(num_transactions_large)],
            "TransactionTime": [fake.time() for _ in range(num_transactions_large)],
            # Additional attributes
            "Fee": np.round(np.random.uniform(0, 50, num_transactions_large), 2),
            "ExchangeRate": exchange_rates,
            "OriginalAmount": np.round(amounts / exchange_rates, 2),
            "CardUsed": np.random.choice(["Visa", "MasterCard", "Amex", "Discover", "None"], num_transactions_large),
            "POSID": [f"POS{np.random.randint(1000, 9999)}" for _ in range(num_transactions_large)]
        })
        transactions["Tax"] = np.round(transactions["Amount"] * np.random.uniform(0, 0.1, num_transactions_large), 2)
        log.debug("Transactions dataframe shape: %s", transactions.shape)
        save_dataframe_in_chunks(transactions, "transactions", CHUNK_SIZE)
        log.info("Transactions dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_transactions: %s", e)