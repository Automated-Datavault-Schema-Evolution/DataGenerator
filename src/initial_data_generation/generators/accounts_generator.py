import time

import numpy as np
import pandas as pd
from logger import log

from src.config.config import fake
from src.initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_accounts(num_accounts_large, customer_ids):
    log.info("Start generating accounts")
    start_time = time.time()
    try:
        account_ids = np.arange(200000, 200000 + num_accounts_large)
        opened_dates = [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d')
                        for _ in range(num_accounts_large)]
        accounts = pd.DataFrame({
            "AccountID": account_ids,
            "CustomerID": np.random.choice(customer_ids, num_accounts_large),
            "AccountType": np.random.choice(["Savings", "Checking", "Business", "Investment"],
                                            num_accounts_large),
            "Balance": np.round(np.random.uniform(-23000, 991234, num_accounts_large), 2),
            "OpenedDate": opened_dates,
            "Status": np.random.choice(["Active", "Inactive", "Closed"], num_accounts_large),
            "Currency": np.random.choice(["USD", "CAD", "EUR", "GBP"], num_accounts_large),
            "BranchCode": [f"BR{np.random.randint(100, 999)}" for _ in range(num_accounts_large)],
            "InterestRate": np.round(np.random.uniform(0.1, 5.0, num_accounts_large), 2),
            # Additional attributes
            "AccountSubType": np.random.choice(["Basic", "Premium", "Gold", "Platinum"],
                                               num_accounts_large),
            "OverdraftLimit": np.round(np.random.uniform(0, 5000, num_accounts_large), 2)
        })
        last_transaction_dates = []
        for date_str in opened_dates:
            try:
                last_date = fake.date_between(start_date=date_str, end_date='today').strftime('%Y-%m-%d')
            except Exception:
                last_date = date_str
            last_transaction_dates.append(last_date)
        accounts["LastTransactionDate"] = last_transaction_dates
        log.debug("Accounts dataframe shape: %s", accounts.shape)
        save_dataframe_in_chunks(accounts, "accounts", CHUNK_SIZE)
        log.info("Accounts dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_accounts: %s", e)