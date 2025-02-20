import time

import numpy as np
import pandas as pd
from logger import log

from src.config.config import fake
from src.initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_marketing(num_marketing, customer_ids):
    log.info("Start generating marketing")
    start_time = time.time()
    try:
        campaign_ids = np.arange(500000, 500000 + num_marketing)
        marketing = pd.DataFrame({
            "CampaignID": campaign_ids,
            "CustomerID": np.random.choice(customer_ids, num_marketing),
            "CampaignType": np.random.choice(["Email", "SMS", "Social Media", "Direct Mail"], num_marketing),
            "CampaignDate": [fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
                             for _ in range(num_marketing)],
            "Response": np.random.choice(["Positive", "Negative", "Neutral", "No Response"], num_marketing),
            "OfferAccepted": np.random.choice([True, False], num_marketing),
            "CampaignName": [f"Campaign {np.random.randint(1000, 9999)}" for _ in range(num_marketing)],
            "CampaignBudget": np.round(np.random.uniform(1000, 10000, num_marketing), 2),
            "Impressions": np.random.randint(1000, 100000, num_marketing),
            "Clicks": np.random.randint(10, 10000, num_marketing),
            "ConversionRate": np.round(np.random.uniform(0, 1, num_marketing), 2),
            "Cost": np.round(np.random.uniform(100, 1000, num_marketing), 2)
        })
        log.debug("Marketing dataframe shape: %s", marketing.shape)
        save_dataframe_in_chunks(marketing, "marketing", CHUNK_SIZE)
        log.info("Marketing dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_marketing: %s", e)
