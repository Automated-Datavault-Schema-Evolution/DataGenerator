# Use a compatible Python image for Raspberry Pi 3 B+
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY requirements.txt .
COPY synthetic_data_generator.py .

RUN apt update && apt install -y --no-install-recommends gcc \
 && pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && apt remove -y gcc \
 && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV DATA_DIR="/data"

# Create a directory for data storage
VOLUME ["/data"]

# Run the script
CMD ["python", "synthetic_data_generator.py"]
