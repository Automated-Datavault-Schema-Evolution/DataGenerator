import time
from datetime import timedelta

import numpy as np
import pandas as pd
from logger import log

from config import fake
from initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_digital_interactions(num_sessions, customer_ids):
    log.info("Start generating digital interactions")
    start_time = time.time()
    try:
        session_ids = np.arange(600000, 600000 + num_sessions)
        digital = pd.DataFrame({
            "SessionID": session_ids,
            "CustomerID": np.random.choice(customer_ids, num_sessions),
            "LoginTime": [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(num_sessions)],
            "DeviceType": np.random.choice(["Desktop", "Mobile", "Tablet"], num_sessions),
            "Browser": np.random.choice(["Chrome", "Firefox", "Safari", "Edge", "Opera"], num_sessions),
            "IPAddress": [fake.ipv4() for _ in range(num_sessions)]
        })
        digital["LogoutTime"] = digital["LoginTime"].apply(
            lambda x: (x + timedelta(minutes=np.random.randint(5, 120))).strftime('%Y-%m-%d %H:%M:%S'))
        digital["LoginTime"] = digital["LoginTime"].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        log.debug("Digital interactions dataframe shape: %s", digital.shape)
        save_dataframe_in_chunks(digital, "digital_interactions", CHUNK_SIZE)
        log.info("Digital interactions dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_digital_interactions: %s", e)