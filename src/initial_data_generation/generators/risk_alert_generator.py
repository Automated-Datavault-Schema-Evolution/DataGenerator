import time

import numpy as np
import pandas as pd
from logger import log

from src.config.config import fake
from src.initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_risk_alerts(num_alerts, customer_ids):
    log.info("Start generating risk alerts")
    start_time = time.time()
    try:
        alert_ids = np.arange(800000, 800000 + num_alerts)
        risk = pd.DataFrame({
            "AlertID": alert_ids,
            "CustomerID": np.random.choice(customer_ids, num_alerts),
            "RiskType": np.random.choice(["Fraud", "Money Laundering", "Cyber Attack", "Regulatory"], num_alerts),
            "RiskScore": np.random.randint(1, 101, num_alerts),
            "AlertDate": [fake.date_between(start_date='-3y', end_date='today').strftime('%Y-%m-%d')
                          for _ in range(num_alerts)],
            "ActionTaken": np.random.choice(["Investigated", "Resolved", "Pending", "Escalated"], num_alerts),
            "ComplianceStatus": np.random.choice(["Compliant", "Non-Compliant", "Under Review"], num_alerts)
        })
        log.debug("Risk alerts dataframe shape: %s", risk.shape)
        save_dataframe_in_chunks(risk, "risk_alerts", CHUNK_SIZE)
        log.info("Risk alerts dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_risk_alerts: %s", e)