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

if __name__ == "__main__":
    main()