import pandas as pd
import numpy as np
import os
import multiprocessing as mp
import time
from faker import Faker

# Initialize Faker and set random seed
fake = Faker()
np.random.seed(42)

# Define directories for output
output_dir = "synthetic_bank_data"
final_output_dir = "/data"
os.makedirs(output_dir, exist_ok=True)
os.makedirs(final_output_dir, exist_ok=True)

# Define chunk size and scaling factor
chunk_size = 100000  # Number of rows per chunk
scale_factor = 50  # Increase dataset size 50x
num_customers = 2500  # Base number of customers


# Function to save data in chunks
def save_dataframe_in_chunks(df, filename, chunk_size):
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)
    for i in range(num_chunks):
        chunk = df[i * chunk_size: (i + 1) * chunk_size]
        chunk.to_csv(f"{output_dir}/{filename}_part{i + 1}.csv", index=False)


# Function to combine chunks into final dataset
def combine_chunks(filename):
    combined_df = pd.concat(
        [pd.read_csv(os.path.join(output_dir, f)) for f in os.listdir(output_dir) if f.startswith(filename)],
        ignore_index=True
    )
    combined_df.to_csv(f"{final_output_dir}/{filename}.csv", index=False)
    print(f"✅ Final dataset '{filename}.csv' created successfully.")


# Function to generate customers
def generate_customers(num_customers_large):
    start_time = time.time()

    customer_ids_large = np.arange(100000, 100000 + num_customers_large)

    customers_large = pd.DataFrame({
        "CustomerID": customer_ids_large,
        "FirstName": [fake.first_name() for _ in range(num_customers_large)],
        "LastName": [fake.last_name() for _ in range(num_customers_large)],
        "Email": [fake.email() for _ in range(num_customers_large)],
        "PhoneNumber": [fake.phone_number() for _ in range(num_customers_large)],
        "Address": [fake.address().replace("\n", ", ") for _ in range(num_customers_large)],
        "DateOfBirth": [fake.date_of_birth(minimum_age=18, maximum_age=80).strftime('%Y-%m-%d') for _ in
                        range(num_customers_large)],
        "AccountCreated": [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d') for _ in
                           range(num_customers_large)],
        "EmploymentStatus": np.random.choice(["Employed", "Unemployed", "Retired", "Student", "Self-Employed"],
                                             num_customers_large),
        "AnnualIncome": np.random.choice(["<25K", "25K-50K", "50K-100K", "100K-250K", ">250K"], num_customers_large),
        "MaritalStatus": np.random.choice(["Single", "Married", "Divorced", "Widowed"], num_customers_large),
        "Nationality": np.random.choice(["USA", "Canada", "UK", "Germany", "India", "China", "France", "Australia"],
                                        num_customers_large)
    })

    save_dataframe_in_chunks(customers_large, "customers", chunk_size)
    print(f"✅ Customers dataset generated in {time.time() - start_time:.2f} seconds.")


# Function to generate accounts
def generate_accounts(num_accounts_large, customer_ids_large):
    start_time = time.time()

    account_ids_large = np.arange(200000, 200000 + num_accounts_large)

    accounts_large = pd.DataFrame({
        "AccountID": account_ids_large,
        "CustomerID": np.random.choice(customer_ids_large, num_accounts_large),
        "AccountType": np.random.choice(["Savings", "Checking", "Business", "Investment"], num_accounts_large),
        "Balance": np.round(np.random.uniform(-23000, 991234, num_accounts_large), 2),
        "OpenedDate": [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d') for _ in
                       range(num_accounts_large)],
        "Status": np.random.choice(["Active", "Inactive", "Closed"], num_accounts_large)
    })

    save_dataframe_in_chunks(accounts_large, "accounts", chunk_size)
    print(f"✅ Accounts dataset generated in {time.time() - start_time:.2f} seconds.")


# Function to generate transactions
def generate_transactions(num_transactions_large, account_ids_large):
    start_time = time.time()

    transaction_ids_large = np.arange(700000, 700000 + num_transactions_large)

    transactions_large = pd.DataFrame({
        "TransactionID": transaction_ids_large,
        "AccountID": np.random.choice(account_ids_large, num_transactions_large),
        "TransactionType": np.random.choice(["Deposit", "Withdrawal", "Transfer", "Payment", "Investment"],
                                            num_transactions_large),
        "Amount": np.round(np.random.uniform(10, 10000, num_transactions_large), 2),
        "TransactionDate": [fake.date_between(start_date='-5y', end_date='today').strftime('%Y-%m-%d') for _ in
                            range(num_transactions_large)],
        "Status": np.random.choice(["Completed", "Pending", "Failed"], num_transactions_large)
    })

    save_dataframe_in_chunks(transactions_large, "transactions", chunk_size)
    print(f"✅ Transactions dataset generated in {time.time() - start_time:.2f} seconds.")


# Function to generate loans
def generate_loans(num_loans_large, customer_ids_large):
    start_time = time.time()

    loan_ids_large = np.arange(400000, 400000 + num_loans_large)

    loans_large = pd.DataFrame({
        "LoanID": loan_ids_large,
        "CustomerID": np.random.choice(customer_ids_large, num_loans_large),
        "LoanType": np.random.choice(["Personal", "Mortgage", "Auto", "Business"], num_loans_large),
        "LoanAmount": np.round(np.random.uniform(5000, 500000, num_loans_large), 2),
        "InterestRate": np.round(np.random.uniform(2.5, 10.5, num_loans_large), 2),
        "LoanTermYears": np.random.choice([5, 10, 15, 20, 25, 30], num_loans_large),
        "ApprovalDate": [fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d') for _ in
                         range(num_loans_large)],
        "Status": np.random.choice(["Active", "Closed", "Default"], num_loans_large)
    })

    save_dataframe_in_chunks(loans_large, "loans", chunk_size)
    print(f"✅ Loans dataset generated in {time.time() - start_time:.2f} seconds.")


# Parallel processing for dataset generation
if __name__ == "__main__":
    total_start_time = time.time()

    num_customers_large = num_customers * scale_factor
    customer_ids_large = np.arange(100000, 100000 + num_customers_large)
    num_accounts_large = num_customers_large * 2
    account_ids_large = np.arange(200000, 200000 + num_accounts_large)
    num_transactions_large = num_accounts_large * 5
    num_loans_large = num_customers_large // 2

    # Create a process pool
    with mp.Pool(processes=mp.cpu_count()) as pool:
        pool.apply_async(generate_customers, (num_customers_large,))
        pool.apply_async(generate_accounts, (num_accounts_large, customer_ids_large))
        pool.apply_async(generate_transactions, (num_transactions_large, account_ids_large))
        pool.apply_async(generate_loans, (num_loans_large, customer_ids_large))
        pool.close()
        pool.join()

    # Combine all chunked datasets into final datasets
    for dataset in ["customers", "accounts", "transactions", "loans"]:
        combine_chunks(dataset)

    print(f"🚀 Multi-core data generation completed in {time.time() - total_start_time:.2f} seconds.")
