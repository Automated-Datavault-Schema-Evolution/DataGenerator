import time

import numpy as np
import pandas as pd
from logger import log

from config import fake
from initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_aml_compliance(num_aml, customer_ids):
    log.info("Start generating AML compliance")
    start_time = time.time()
    try:
        aml_ids = np.arange(1100000, 1100000 + num_aml)
        report_filed = np.random.choice([True, False], num_aml)
        filing_dates = []
        for filed in report_filed:
            filing_dates.append(
                fake.date_between(start_date='-3y', end_date='today').strftime('%Y-%m-%d') if filed else "")
        aml = pd.DataFrame({
            "AMLRecordID": aml_ids,
            "CustomerID": np.random.choice(customer_ids, num_aml),
            "Regulation": np.random.choice(["IFRS", "FATCA", "CRS"], num_aml),
            "ComplianceStatus": np.random.choice(["Compliant", "Non-Compliant", "Under Review"], num_aml),
            "InvestigationStatus": np.random.choice(["Cleared", "Investigating", "Escalated", "Not Applicable"],
                                                    num_aml),
            "SuspicionScore": np.round(np.random.uniform(0, 100, num_aml), 2),
            "ReportFiled": report_filed,
            "FilingDate": filing_dates,
            "HighRiskJurisdiction": np.random.choice([True, False], num_aml),
            "OffshoreAccountFlag": np.random.choice([True, False], num_aml),
            "Comments": [fake.sentence(nb_words=6) for _ in range(num_aml)],
            "LastUpdated": [fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
                            for _ in range(num_aml)]
        })
        log.debug("AML compliance dataframe shape: %s", aml.shape)
        save_dataframe_in_chunks(aml, "aml_compliance", CHUNK_SIZE)
        log.info("AML compliance dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_aml_compliance: %s", e)