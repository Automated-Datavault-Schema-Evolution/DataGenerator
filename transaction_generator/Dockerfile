# Use a compatible Python image for Raspberry Pi 3 B+
FROM python:3.9-slim-bullseye

# Set working directory
WORKDIR /app

# Copy project files
COPY requirements.txt .
COPY transaction_generator.py .

# Install necessary system dependencies
RUN apt update && apt install -y --no-install-recommends gcc \
 && pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && apt remove -y gcc \
 && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV DATA_DIR="/data"
ENV MAX_TRANSACTIONS_PER_BATCH=1000
ENV MIN_SLEEP_TIME=30
ENV MAX_SLEEP_TIME=300

# The actual data folder will be mounted at runtime
VOLUME ["/data"]

# Run the script
CMD ["python", "transaction_generator.py"]
