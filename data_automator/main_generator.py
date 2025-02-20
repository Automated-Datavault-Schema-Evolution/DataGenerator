import os
import threading
import time

from logger import log

from data_automator.data_generator.accounts_generator import run_accounts_generator
from data_automator.data_generator.aml_generator import run_aml_generator
from data_automator.data_generator.customers_generator import run_customers_generator
from data_automator.data_generator.depots_generator import run_depots_generator
from data_automator.data_generator.digital_generator import run_digital_generator
from data_automator.data_generator.loans_generator import run_loans_generator
from data_automator.data_generator.marketing_generator import run_marketing_generator
from data_automator.data_generator.risk_alerts_generator import run_risk_alerts_generator
from data_automator.data_generator.shares_generator import run_shares_generator


def start_generator(generator_func, name):
    thread = threading.Thread(target=generator_func, name=name, daemon=True)
    thread.start()
    log.info("Started thread for %s", name)
    return thread

def main():
    threads = []
    threads.append(start_generator(run_customers_generator, "CustomersGenerator"))
    threads.append(start_generator(run_accounts_generator, "AccountsGenerator"))
    threads.append(start_generator(run_loans_generator, "LoansGenerator"))
    threads.append(start_generator(run_marketing_generator, "MarketingGenerator"))
    threads.append(start_generator(run_digital_generator, "DigitalGenerator"))
    threads.append(start_generator(run_risk_alerts_generator, "RiskAlertsGenerator"))
    threads.append(start_generator(run_shares_generator, "SharesGenerator"))
    threads.append(start_generator(run_depots_generator, "DepotsGenerator"))
    threads.append(start_generator(run_aml_generator, "AMLGenerator"))

    # Keep the main thread alive indefinitely.
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Main generator interrupted by user. Exiting...")


def wait_for_initial_data(data_dir, marker_file, poll_interval=5):
    marker_path = os.path.join(data_dir, marker_file)
    log.info("Waiting for initial data marker: %s", marker_path)
    while not os.path.exists(marker_path):
        log.info("Initial data not ready yet. Sleeping for %d seconds...", poll_interval)
        time.sleep(poll_interval)
    log.info("Initial data detected. Proceeding with continuous data automator.")


if __name__ == "__main__":
    DATA_DIR = os.getenv("DATA_DIR", "/app/data")
    wait_for_initial_data(DATA_DIR, "initial_complete.flag")
    # Start the automator. Using os.execv or os.system is acceptable.
    log.info("Starting main automator...")
    main()