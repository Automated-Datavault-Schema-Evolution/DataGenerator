import time

import numpy as np
import pandas as pd
from logger import log

from src.config.config import fake
from src.initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_branches(num_branches):
    log.info("Start generating branches")
    start_time = time.time()
    try:
        branch_ids = np.arange(300000, 300000 + num_branches)
        branches = pd.DataFrame({
            "BranchID": branch_ids,
            "BranchName": [f"Branch {np.random.randint(1000, 9999)}" for _ in range(num_branches)],
            "StreetAddress": [fake.street_address() for _ in range(num_branches)],
            "City": [fake.city() for _ in range(num_branches)],
            "State": [fake.state() for _ in range(num_branches)],
            "ZipCode": [fake.zipcode() for _ in range(num_branches)],
            "Country": np.random.choice(["USA", "Canada", "UK", "Germany", "France", "Australia"], num_branches),
            "OperationalHours": ["9:00-17:00" for _ in range(num_branches)],
            "TransactionVolume": np.random.randint(1000, 10000, num_branches),
            "ManagerName": [fake.name() for _ in range(num_branches)],
            "OpeningDate": [fake.date_between(start_date='-30y', end_date='-5y').strftime('%Y-%m-%d')
                            for _ in range(num_branches)],
            "ContactNumber": [fake.phone_number() for _ in range(num_branches)],
            "NumberOfEmployees": np.random.randint(10, 100, num_branches),
            "ATMCount": np.random.randint(1, 20, num_branches)
        })
        log.debug("Branches dataframe shape: %s", branches.shape)
        save_dataframe_in_chunks(branches, "branches", CHUNK_SIZE)
        log.info("Branches dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_branches: %s", e)
