import time

import numpy as np
import pandas as pd
from logger import log

from config import fake
from initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_depots(num_depots, customer_ids):
    log.info("Start generating depots")
    start_time = time.time()
    try:
        depot_ids = np.arange(1000000, 1000000 + num_depots)
        depots = pd.DataFrame({
            "DepotID": depot_ids,
            "CustomerID": np.random.choice(customer_ids, num_depots),
            "DepotType": np.random.choice(["Standard", "Premium", "Gold"], num_depots),
            "OpeningDate": [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d')
                            for _ in range(num_depots)],
            "Status": np.random.choice(["Active", "Inactive", "Closed"], num_depots),
            "TotalValue": np.round(np.random.uniform(1000, 500000, num_depots), 2),
            "Custodian": [fake.company() for _ in range(num_depots)],
            "NumberOfSecurities": np.random.randint(1, 50, num_depots)
        })
        log.debug("Depots dataframe shape: %s", depots.shape)
        save_dataframe_in_chunks(depots, "depots", CHUNK_SIZE)
        log.info("Depots dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_depots: %s", e)