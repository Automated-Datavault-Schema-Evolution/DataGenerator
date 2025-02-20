import os

import pandas as pd
from logger import log

from config import DATA_DIR

# Directories for output
OUTPUT_DIR = "synthetic_bank_data"
FINAL_OUTPUT_DIR = DATA_DIR

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)

# Define a default chunk size for saving files
CHUNK_SIZE = 100000

def save_dataframe_in_chunks(df, filename, chunk_size=CHUNK_SIZE):
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)
    log.debug("Saving %d chunks for %s", num_chunks, filename)
    for i in range(num_chunks):
        chunk = df[i * chunk_size: (i + 1) * chunk_size]
        chunk.to_csv(f"{OUTPUT_DIR}/{filename}_part{i + 1}.csv", index=False)

def combine_chunks(filename):
    files = [os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR) if f.startswith(filename)]
    log.debug("Found files for %s: %s", filename, files)
    if not files:
        log.warning("No files found for '%s'. Skipping combination.", filename)
        return
    combined_df = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
    combined_df.to_csv(f"{FINAL_OUTPUT_DIR}/{filename}.csv", index=False)
    log.info("Final dataset '%s.csv' created successfully.", filename)