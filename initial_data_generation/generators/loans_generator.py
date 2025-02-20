import time

import numpy as np
import pandas as pd
from logger import log

from config import fake
from initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_loans(num_loans_large, customer_ids):
    log.info("Start generating loans")
    start_time = time.time()
    try:
        loan_ids = np.arange(400000, 400000 + num_loans_large)
        loan_amounts = np.round(np.random.uniform(5000, 500000, num_loans_large), 2)
        loan_terms = np.random.choice([5, 10, 15, 20, 25, 30], num_loans_large)
        loans = pd.DataFrame({
            "LoanID": loan_ids,
            "CustomerID": np.random.choice(customer_ids, num_loans_large),
            "LoanType": np.random.choice(["Personal", "Mortgage", "Auto", "Business"], num_loans_large),
            "LoanAmount": loan_amounts,
            "InterestRate": np.round(np.random.uniform(2.5, 10.5, num_loans_large), 2),
            "LoanTermYears": loan_terms,
            "ApprovalDate": [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d')
                             for _ in range(num_loans_large)],
            "Status": np.random.choice(["Active", "Closed", "Default"], num_loans_large),
            "CollateralType": np.random.choice(["Property", "Vehicle", "Equipment", "None"], num_loans_large)
        })
        loans["CollateralValue"] = loans["CollateralType"].apply(
            lambda x: np.round(np.random.uniform(1000, 300000), 2) if x != "None" else 0.0
        )

        def get_subtype(loan_type):
            if loan_type == "Mortgage":
                return np.random.choice(["Fixed", "Adjustable"])
            elif loan_type == "Personal":
                return np.random.choice(["Secured", "Unsecured"])
            elif loan_type == "Auto":
                return np.random.choice(["New", "Used"])
            elif loan_type == "Business":
                return np.random.choice(["Small Business", "Corporate"])
            else:
                return "Standard"

        loans["LoanProductSubType"] = loans["LoanType"].apply(get_subtype)
        loans["MonthlyPayment"] = np.round(
            loan_amounts / (loan_terms * 12) * np.random.uniform(0.9, 1.1, num_loans_large), 2)
        loans["OutstandingBalance"] = loans.apply(lambda row: np.round(np.random.uniform(0, row["LoanAmount"]), 2),
                                                  axis=1)
        loans["LoanPurpose"] = np.random.choice(
            ["Home Improvement", "Debt Consolidation", "Business Expansion", "Car Purchase", "Education", "Medical",
             "Vacation"],
            num_loans_large
        )
        loans["LoanStartDate"] = pd.to_datetime(loans["ApprovalDate"]) + pd.to_timedelta(
            np.random.randint(0, 30, num_loans_large), unit='d')
        loans["LoanEndDate"] = loans.apply(
            lambda row: row["LoanStartDate"] + pd.DateOffset(years=int(row["LoanTermYears"])), axis=1)
        loans["LoanStartDate"] = loans["LoanStartDate"].dt.strftime('%Y-%m-%d')
        loans["LoanEndDate"] = loans["LoanEndDate"].dt.strftime('%Y-%m-%d')
        log.debug("Loans dataframe shape: %s", loans.shape)
        save_dataframe_in_chunks(loans, "loans", CHUNK_SIZE)
        log.info("Loans dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_loans: %s", e)