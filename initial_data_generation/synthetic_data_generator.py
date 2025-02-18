import multiprocessing as mp
import os
import random
import time
from datetime import timedelta

import numpy as np
import pandas as pd
from faker import Faker
from logger import log

# Initialize Faker and set random seed
fake = Faker(['it_IT', 'en_US', 'de_AT', 'de_DE', 'de_CH'])
np.random.seed(42)

# Define directories for output
output_dir = "synthetic_bank_data"
final_output_dir = r"..\data"
os.makedirs(output_dir, exist_ok=True)
os.makedirs(final_output_dir, exist_ok=True)

# Define chunk size and scaling factor
chunk_size = 100000  # Number of rows per chunk
scale_factor = 50  # Increase dataset size 50x
num_customers = 2500  # Base number of customers


# Function to save data in chunks
def save_dataframe_in_chunks(df, filename, chunk_size):
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)
    log.debug("Saving %d chunks for %s", num_chunks, filename)
    for i in range(num_chunks):
        chunk = df[i * chunk_size: (i + 1) * chunk_size]
        chunk.to_csv(f"{output_dir}/{filename}_part{i + 1}.csv", index=False)


# Function to combine chunks into final dataset
def combine_chunks(filename):
    files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.startswith(filename)]
    log.debug("Found files for %s: %s", filename, files)
    if not files:
        log.warning(f"No files found for '{filename}'. Skipping combination.")
        return
    combined_df = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
    combined_df.to_csv(f"{final_output_dir}/{filename}.csv", index=False)
    log.info(f"Final dataset '{filename}.csv' created successfully.")


# ---------------------------
# Dataset generation functions
# ---------------------------
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
        save_dataframe_in_chunks(customers, "customers", chunk_size)
        log.info("Customers dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.critical("Error in generate_customers: %s", e)


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
        save_dataframe_in_chunks(accounts, "accounts", chunk_size)
        log.info("Accounts dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_accounts: %s", e)


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
        save_dataframe_in_chunks(transactions, "transactions", chunk_size)
        log.info("Transactions dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_transactions: %s", e)


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
        save_dataframe_in_chunks(loans, "loans", chunk_size)
        log.info("Loans dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_loans: %s", e)


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
        save_dataframe_in_chunks(branches, "branches", chunk_size)
        log.info("Branches dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_branches: %s", e)


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
        save_dataframe_in_chunks(marketing, "marketing", chunk_size)
        log.info("Marketing dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_marketing: %s", e)


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
        save_dataframe_in_chunks(digital, "digital_interactions", chunk_size)
        log.info("Digital interactions dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_digital_interactions: %s", e)


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
        save_dataframe_in_chunks(risk, "risk_alerts", chunk_size)
        log.info("Risk alerts dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_risk_alerts: %s", e)


def generate_shares(num_shares, customer_ids):
    log.info("Start generating shares")
    start_time = time.time()
    try:
        share_ids = np.arange(900000, 900000 + num_shares)
        stocks = [
            ("AAPL", "Apple Inc.", "Technology", "NASDAQ"),
            ("GOOGL", "Alphabet Inc.", "Technology", "NASDAQ"),
            ("MSFT", "Microsoft Corp.", "Technology", "NASDAQ"),
            ("AMZN", "Amazon.com Inc.", "Consumer Discretionary", "NASDAQ"),
            ("TSLA", "Tesla Inc.", "Consumer Discretionary", "NASDAQ"),
            ("NFLX", "Netflix Inc.", "Communication Services", "NASDAQ"),
            ("FB", "Meta Platforms", "Communication Services", "NASDAQ"),
            ("NVDA", "NVIDIA Corp.", "Technology", "NASDAQ"),
            ("BABA", "Alibaba Group", "Consumer Discretionary", "NYSE"),
            ("ORCL", "Oracle Corp.", "Technology", "NYSE")
        ]
        # Use Python's random.choice to select stock details for each record
        stock_details = [random.choice(stocks) for _ in range(num_shares)]
        shares = pd.DataFrame({
            "ShareID": share_ids,
            "CustomerID": np.random.choice(np.array(customer_ids).flatten(), num_shares),
            "StockSymbol": [s[0] for s in stock_details],
            "StockName": [s[1] for s in stock_details],
            "Sector": [s[2] for s in stock_details],
            "Exchange": [s[3] for s in stock_details],
            "Quantity": np.random.randint(1, 1000, num_shares),
            "PurchasePrice": np.round(np.random.uniform(10, 500, num_shares), 2),
            "CurrentPrice": np.round(np.random.uniform(10, 500, num_shares), 2),
            "PurchaseDate": [fake.date_between(start_date='-3y', end_date='today').strftime('%Y-%m-%d')
                             for _ in range(num_shares)]
        })
        log.debug("Shares dataframe shape: %s", shares.shape)
        save_dataframe_in_chunks(shares, "shares", chunk_size)
        log.info("Shares dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_shares: %s", e)


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
        save_dataframe_in_chunks(depots, "depots", chunk_size)
        log.info("Depots dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_depots: %s", e)


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
        save_dataframe_in_chunks(aml, "aml_compliance", chunk_size)
        log.info("AML compliance dataset generated in %.2f seconds.", time.time() - start_time)
    except Exception as e:
        log.error("Error in generate_aml_compliance: %s", e)


# ---------------------------
# Main execution
# ---------------------------
if __name__ == "__main__":
    total_start_time = time.time()

    # Generate customers synchronously
    num_customers_large = num_customers * scale_factor
    generate_customers(num_customers_large)
    combine_chunks("customers")

    # Use the known range from customer generation
    customer_ids = np.arange(100000, 100000 + num_customers_large)

    # Define remaining parameters
    num_accounts_large = num_customers_large * 2
    account_ids = np.arange(200000, 200000 + num_accounts_large)
    num_transactions_large = num_accounts_large * 5
    num_loans_large = num_customers_large // 2
    num_branches = 100
    num_marketing = num_customers_large * 2
    num_digital_sessions = num_customers_large * 3
    num_risk_alerts = num_customers_large // 10
    num_shares = num_customers_large * 3
    num_depots = int(num_customers_large * 0.5)
    num_aml = num_customers_large // 20

    # Generate remaining datasets concurrently (they all link to the same customer IDs)
    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.apply_async(generate_accounts, (num_accounts_large, customer_ids))
        pool.apply_async(generate_transactions, (num_transactions_large, account_ids))
        pool.apply_async(generate_loans, (num_loans_large, customer_ids))
        pool.apply_async(generate_branches, (num_branches,))
        pool.apply_async(generate_marketing, (num_marketing, customer_ids))
        pool.apply_async(generate_digital_interactions, (num_digital_sessions, customer_ids))
        pool.apply_async(generate_risk_alerts, (num_risk_alerts, customer_ids))
        pool.apply_async(generate_shares, (num_shares, customer_ids))
        pool.apply_async(generate_depots, (num_depots, customer_ids))
        pool.apply_async(generate_aml_compliance, (num_aml, customer_ids))
        pool.close()
        pool.join()

    # Combine all chunked datasets into final datasets
    for dataset in ["accounts", "transactions", "loans", "branches", "marketing", "digital_interactions",
                    "risk_alerts", "shares", "depots", "aml_compliance"]:
        combine_chunks(dataset)

    log.info("Multi-core data generation completed in %.2f seconds.", time.time() - total_start_time)
