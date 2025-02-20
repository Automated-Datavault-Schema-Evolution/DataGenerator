# Data Automator & Initial Data Generator

This project generates synthetic data for a banking data warehouse system. It consists of two main components:

1. **Initial Data Generator:** Generates the bulk initial synthetic data.
2. **Continuous Data Automator:** Continuously generates new records to simulate ongoing updates.

Both components are containerized using Docker and orchestrated with Docker Compose. The project uses a custom logger package (installed via pip from a private Git repository using SSH forwarding) and centralized configuration via a `.env` file.

## .env File Template

Create a file named `.env` in the project root with content similar to the following. Adjust values as needed:

```dotenv
# Host data directory on your local machine (absolute path)
HOST_DATA_DIR=/mnt/nas/data

# Data directory inside the container (this is fixed to /app/data)
DATA_DIR=/app/data

# Customers generator thresholds (customers are updated infrequently)
MIN_SLEEP_TIME_CUSTOMERS=300
MAX_SLEEP_TIME_CUSTOMERS=600
MAX_BATCH_CUSTOMERS=100

# Accounts generator thresholds
MIN_SLEEP_TIME_ACCOUNTS=180
MAX_SLEEP_TIME_ACCOUNTS=300
MAX_BATCH_ACCOUNTS=50

# Loans generator thresholds
MIN_SLEEP_TIME_LOANS=600
MAX_SLEEP_TIME_LOANS=900
MAX_BATCH_LOANS=20

# Marketing generator thresholds
MIN_SLEEP_TIME_MARKETING=120
MAX_SLEEP_TIME_MARKETING=240
MAX_BATCH_MARKETING=10

# Digital interactions thresholds (sessions occur frequently)
MIN_SLEEP_TIME_DIGITAL=30
MAX_SLEEP_TIME_DIGITAL=120
MAX_BATCH_DIGITAL=200

# Risk alerts generator thresholds
MIN_SLEEP_TIME_RISK_ALERTS=300
MAX_SLEEP_TIME_RISK_ALERTS=600
MAX_BATCH_RISK_ALERTS=50

# Shares generator thresholds
MIN_SLEEP_TIME_SHARES=60
MAX_SLEEP_TIME_SHARES=180
MAX_BATCH_SHARES=100

# Depots generator thresholds
MIN_SLEEP_TIME_DEPOTS=600
MAX_SLEEP_TIME_DEPOTS=900
MAX_BATCH_DEPOTS=20

# AML generator thresholds
MIN_SLEEP_TIME_AML=900
MAX_SLEEP_TIME_AML=1800
MAX_BATCH_AML=30

# Base number of customers and scaling factor
NUM_CUSTOMER=2500
SCALE_FACTOR=50
```

## SSH Key Configuration

### macOS / Linux

1. **Ensure Your SSH Agent Is Running:**

Open a terminal and run:
```bash
ssh-add -l
```
If your key is not listed, add it with:
```bash
ssh-add ~/.ssh/id_ed25519
```

2. **(Optional) Configure SSH:**
Create or edit your SSH configuration file at `~/.ssh/config*` and add:
```ssh
Host github.com/Automated-Datavault-Schema-Evolution
    User git
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
```
This ensures that when Docker BuildKit forwards your SSH agent during the build process, the correct key is used for accessing your private repositories.

### Windows
1. **Open your WSL terminal and run:**
```bash
ssh-add -l
ssh-add ~/.ssh/id_ed25519

```
2. **Ensure your ~/.ssh/config in WSL includes:**
```ssh
Host github.com/Automated-Datavault-Schema-Evolution
    User git
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
```
**Note**: Before building your Docker images, always confirm that your SSH key is unlocked and listed by `ssh-add -l` so that Docker BuildKit can securely access your private repositories.

# Docker Compose Configuration
The ``docker-compose.yaml`` is set up to use an environment variable (HOST_DATA_DIR) so that the data folder can be mapped from an absolute host path. This makes the solution cross-platform. For example:
```yaml
services:
  initial_data:
    build:
      context: ..
      dockerfile: docker/Dockerfile.initial
      ssh:
        - default
    container_name: initial_data_generator
    restart: "no"
    environment:
      - DATA_DIR=/app/data
    volumes:
      - ${HOST_DATA_DIR}:/app/data
    healthcheck:
      test: ["CMD-SHELL", "test -f /app/data/customers.csv && test -f /app/data/accounts.csv"]
      interval: 10s
      timeout: 5s
      retries: 5

  automator:
    build:
      context: ..
      dockerfile: docker/Dockerfile.automator
      ssh:
        - default
    container_name: data_automator
    depends_on:
      initial_data:
        condition: service_completed_successfully
    environment:
      - DATA_DIR=/app/data
    volumes:
      - ${HOST_DATA_DIR}:/app/data
```
In your ``.env`` file, set ``HOST_DATA_DIR`` to an absolute path appropriate for your host:
- On macOS/Linux, for example: ``/mnt/shared/data``
- On Windows (with Docker Desktop in WSL mode), for example: ``/c/Data``
## Building and Running
1. **Ensure your SSH key is unlocked** (see SSH Key Configuration above).
2. **Build and Run Using Docker Compose:** From the project directory, run:
````bash
docker-compose -f docker/docker-compose.yaml -p data_generator up --build -d
````
This command will:
- Build the initial data generator image using SSH forwarding.
- Run the initial data generator container, which generates the initial bulk data and writes a marker file.
- Once the initial data generator completes successfully (as verified by the marker file and health check), the continuous data automator container starts.

## Accessing Generated Data
The data directory is mapped using the ``HOST_DATA_DIR`` variable. Any files written to ``/app/data`` inside the container will appear in the folder specified by ``HOST_DATA_DIR`` on your host system.

# Troubleshooting
- **SSH Access Issues:**
If you encounter errors such as "Host key verification failed," ensure your SSH key is added to your agent and that your known_hosts file in the container is properly populated (e.g., by adding a command like 
``
ssh-keyscan github.com/Automated-Datavault-Schema-Evolution >> ~/.ssh/known_hosts 
``
in your Dockerfile).

