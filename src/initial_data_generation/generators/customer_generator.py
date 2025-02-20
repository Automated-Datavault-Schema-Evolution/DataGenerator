import time

import numpy as np
import pandas as pd
from logger import log

from src.config.config import fake
from src.initial_data_generation.utils import save_dataframe_in_chunks, CHUNK_SIZE


def generate_customers(num_customers_large):
    log.info("Start generating customers")
    start_time = time.time()
    try:
        customer_ids = np.arange(100000, 100000 + num_customers_large)
        customers = pd.DataFrame({
            "CustomerID": customer_ids,
            "FirstName": [fake.first_name() for _ in range(num_customers_large)],
            "LastName": [fake.last_name() for _ in range(num_customers_large)],
            "SSN": [fake.ssn() for _ in range(num_customers_large)],
            "Gender": np.random.choice(["Male", "Female"], num_customers_large),
            "DateOfBirth": [fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d')
                            for _ in range(num_customers_large)],
            "Email": [fake.email() for _ in range(num_customers_large)],
            "PhoneNumber": [fake.phone_number() for _ in range(num_customers_large)],
            "StreetAddress": [fake.street_address() for _ in range(num_customers_large)],
            "City": [fake.city() for _ in range(num_customers_large)],
            "State": [fake.state() for _ in range(num_customers_large)],
            "ZipCode": [fake.zipcode() for _ in range(num_customers_large)],
            "AccountCreated": [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d')
                               for _ in range(num_customers_large)],
            "EmploymentStatus": np.random.choice(["Employed", "Unemployed", "Retired", "Student", "Self-Employed"],
                                                 num_customers_large),
            "Occupation": [fake.job() for _ in range(num_customers_large)],
            "Employer": [fake.company() for _ in range(num_customers_large)],
            "AnnualIncome": np.random.choice(["<25K", "25K-50K", "50K-100K", "100K-250K", ">250K"],
                                             num_customers_large),
            "MaritalStatus": np.random.choice(["Single", "Married", "Divorced", "Widowed"],
                                              num_customers_large),
            "Nationality": np.random.choice(["USA", "Canada", "UK", "Germany", "India", "China", "France", "Australia"],
                                            num_customers_large),
            "CreditScore": np.random.randint(300, 851, num_customers_large),
            "RiskRating": np.random.randint(1, 6, num_customers_large),
            "CustomerSegment": np.random.choice(["Retail", "SME", "Corporate", "High Net Worth"],
                                                num_customers_large),
            "KYCStatus": np.random.choice(["Verified", "Pending", "Not Verified"], num_customers_large),
            "AMLFlag": np.random.choice([True, False], num_customers_large),
            # Additional attributes
            "LoyaltyProgramStatus": np.random.choice(["Active", "Inactive", "Not Enrolled"], num_customers_large),
            "RewardPoints": np.random.randint(0, 10000, num_customers_large),
            "ChurnProbability": np.round(np.random.uniform(0, 1, num_customers_large), 2),
            "PreferredBranch": [f"BR{np.random.randint(100, 999)}" for _ in range(num_customers_large)],
            "LastLoginDate": [fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d')
                              for _ in range(num_customers_large)],
            "CustomerRating": np.round(np.random.uniform(1, 5, num_customers_large), 2)
        })
        log.debug("Customers dataframe shape: %s", customers.shape)
        save_dataframe_in_chunks(customers, "customers", CHUNK_SIZE)
        log.info("Customers dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.critical("Error in generate_customers: %s", e)
