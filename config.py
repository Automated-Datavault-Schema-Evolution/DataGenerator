import os

import numpy as np
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

SCALE_FACTOR = os.getenv('SCALE_FACTOR', 50)
NUM_CUSTOMER = os.getenv('NUM_CUSTOMER', 2500)
CHUNK_SIZE = os.getenv('CHUNK_SIZE', 1000)

DATA_DIR = os.getenv("DATA_DIR", "../data")
os.makedirs(DATA_DIR, exist_ok=True)

# Create a shared Faker instance if desired.
fake = Faker(['it_IT', 'en_US', 'de_AT', 'de_DE', 'de_CH'])

# Set a common random seed
np.random.seed(42)
