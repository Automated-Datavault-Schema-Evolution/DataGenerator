import multiprocessing as mp
import os
import time

import numpy as np
from logger import log

from config import DATA_DIR, SCALE_FACTOR, NUM_CUSTOMER
from generators.accounts_generator import generate_accounts
from generators.aml_compliance_generator import generate_aml_compliance
from generators.branches_generator import generate_branches
# Import generator functions
from generators.customer_generator import generate_customers
from generators.depots_generator import generate_depots
from generators.digital_interaction_generator import generate_digital_interactions
from generators.loans_generator import generate_loans
from generators.marketing_generator import generate_marketing
from generators.risk_alert_generator import generate_risk_alerts
from generators.shares_generator import generate_shares
from generators.transaction_generator import generate_transactions
from utils import combine_chunks

# Configuration variables
scale_factor = SCALE_FACTOR  # Increase dataset size 50x
num_customers = NUM_CUSTOMER  # Base number of customers

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

    # Generate remaining datasets concurrently
    pool = mp.Pool(processes=mp.cpu_count())
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
    datasets = ["accounts", "transactions", "loans", "branches", "marketing",
                "digital_interactions", "risk_alerts", "shares", "depots", "aml_compliance"]
    for dataset in datasets:
        combine_chunks(dataset)

    marker = os.path.join(DATA_DIR, "initial_complete.flag")
    with open(marker, "w") as f:
        f.write("Initial data generation completed successfully.\n")
    log.info("Initial data generation completed. Marker file created.")

    log.info("Multi-core data generation completed in %.2f seconds.", time.time() - total_start_time)
