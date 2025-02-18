import os

import numpy as np
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
DATA_DIR = os.getenv("DATA_DIR", "../data")
os.makedirs(DATA_DIR, exist_ok=True)

# Create a shared Faker instance if desired.
fake = Faker(['it_IT', 'en_US', 'de_AT', 'de_DE', 'de_CH'])

# Set a common random seed
np.random.seed(42)
