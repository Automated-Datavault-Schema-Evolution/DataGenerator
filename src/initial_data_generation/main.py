import os
import shutil
import sys
import time

import numpy as np
import pandas as pd
from logger import log

from generators.accounts_generator import generate_accounts
from generators.aml_compliance_generator import generate_aml_compliance
from generators.branches_generator import generate_branches
from generators.customer_generator import generate_customers
from generators.depots_generator import generate_depots
from generators.digital_interaction_generator import generate_digital_interactions
from generators.loans_generator import generate_loans
from generators.marketing_generator import generate_marketing
from generators.risk_alert_generator import generate_risk_alerts
from generators.shares_generator import generate_shares
from src.config.config import DATA_DIR, SCALE_FACTOR, NUM_CUSTOMER, TEMP_DATA_DIR
from utils import combine_chunks

# Derived parameters
num_customers_large = SCALE_FACTOR * NUM_CUSTOMER
num_accounts_large = num_customers_large * 2
num_transactions_large = num_accounts_large * 5
num_loans_large = num_customers_large // 2
num_branches = 100
num_marketing = num_customers_large * 2
num_digital_sessions = num_customers_large * 3
num_risk_alerts = num_customers_large // 10
num_shares = num_customers_large * 3
num_depots = int(num_customers_large * 0.5)
num_aml = num_customers_large // 20

# List of required datasets and corresponding generator functions with arguments.
# Note: For datasets that require customer IDs, we assume that after customers are generated,
# customer_ids is defined as below.
REQUIRED_DATASETS = [
    ("customers", generate_customers, (num_customers_large,)),
    # The rest require customer_ids, which we'll compute after ensuring customers exist.
    ("accounts", generate_accounts, (num_accounts_large,)),
    ("loans", generate_loans, (num_loans_large,)),
    ("branches", generate_branches, (num_branches,)),
    ("marketing", generate_marketing, (num_marketing,)),
    ("digital_interactions", generate_digital_interactions, (num_digital_sessions,)),
    ("risk_alerts", generate_risk_alerts, (num_risk_alerts,)),
    ("shares", generate_shares, (num_shares,)),
    ("depots", generate_depots, (num_depots,)),
    ("aml_compliance", generate_aml_compliance, (num_aml,))
]


def file_exists_and_has_data(filename):
    file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        return False
    try:
        df = pd.read_csv(file_path)
        # We assume the file has a header; at least one data row means shape[0] >= 1.
        return df.shape[0] >= 1
    except Exception as e:
        log.error("Error reading %s: %s", filename, e)
        return False


def generate_missing_data(customer_ids):
    # For each dataset, if the file is missing or empty, call its generator once.
    for dataset_name, generator_func, args in REQUIRED_DATASETS:
        log.debug(f"Dataset name: {dataset_name}")
        filename = dataset_name + ".csv"
        if not file_exists_and_has_data(filename):
            log.info("File %s is missing or empty. Generating initial data...", filename)
            # For generators that require customer_ids, append customer_ids to the args.
            if dataset_name == "customers" or dataset_name == "branches":
                generator_func(*args)
                # generator_func(*(args + (customer_ids,)))
            else:
                generator_func(*(args + (customer_ids,)))
                # generator_func(*args)
            combine_chunks(dataset_name)
        else:
            log.info("File %s exists and has data.", filename)


def remove_temp_data():
    if os.path.exists(TEMP_DATA_DIR):
        for filename in os.listdir(TEMP_DATA_DIR):
            file_path = os.path.join(TEMP_DATA_DIR, filename)
            try:
                os.remove(file_path)
                log.debug("Removed temporary file: %s", file_path)
            except Exception as e:
                log.error("Failed to remove file %s: %s", file_path, e)
        try:
            shutil.rmtree(TEMP_DATA_DIR)
            log.debug("Removed temporary folder: %s", TEMP_DATA_DIR)
        except Exception as e:
            log.error("Failed to remove temporary folder: %s", e)
    else:
        log.warning("Temporary directory %s does not exist.", TEMP_DATA_DIR)


def main():
    total_start_time = time.time()

    # Step 1: Ensure customers data is present.
    if not file_exists_and_has_data("customers.csv"):
        log.info("Customers file missing or empty. Generating customers...")
        generate_customers(num_customers_large)
        combine_chunks("customers")
    else:
        log.info("Customers file exists and has data.")

    # Define customer_ids from the customers file.
    # Here we assume the customers file is created with IDs ranging from 100000 to (100000 + num_customers_large - 1)
    customer_ids = np.arange(100000, 100000 + num_customers_large)

    # Step 2: Check and generate any missing datasets.
    generate_missing_data(customer_ids)

    # Final check: ensure all files exist and have data.
    missing_files = [name + ".csv" for name, _, _ in REQUIRED_DATASETS if not file_exists_and_has_data(name + ".csv")]
    if missing_files:
        log.critical("Initial data generation failed. The following files are missing or empty: %s", missing_files)
        sys.exit(1)

    # Create marker file to indicate initial data generation is complete.
    marker_path = os.path.join(DATA_DIR, "initial_complete.flag")
    with open(marker_path, "w") as f:
        f.write("Initial data generation completed successfully.\n")
    log.info("Initial data generation completed. Marker file created at %s", marker_path)

    # Remove temporary files in synthetic_bank_data folder.
    remove_temp_data()

    log.info("Multi-core initial data generation completed in %.2f seconds.", time.time() - total_start_time)


if __name__ == "__main__":
    main()
