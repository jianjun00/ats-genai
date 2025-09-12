# PostgreSQL Database Connection Solution

This document describes the solution implemented to fix PostgreSQL database connection issues in Kubernetes environments.

## Problem

The application was experiencing database connection issues in Kubernetes environments (`dev` and `intg` namespaces) due to:

1. Incorrect database host resolution
2. SSL handshake failures
3. Missing connection timeout parameters
4. Incorrect database name selection per environment
5. No retry mechanism for transient connection issues

## Solution

### 1. Environment-Aware Database Configuration

The `Database` class in `src/config/database.py` was updated to:

- Detect the current environment from the `ENVIRONMENT` environment variable
- Select the appropriate database host based on the environment
- Use simplified service names in Kubernetes environments (`timescaledb` instead of fully qualified DNS names)
- Select the correct database name per environment:
  - `dev_db` for `dev` environment
  - `intg_db` for `intg` environment
  - `trading_db` for other environments

```python
# Set database name based on environment
db_name = os.getenv("DB_NAME")
if not db_name:
    if env_type == "dev":
        db_name = "dev_db"
        print(f"[DATABASE] Using dev_db for dev environment")
    elif env_type == "intg":
        db_name = "intg_db"
        print(f"[DATABASE] Using intg_db for intg environment")
    else:
        db_name = database or 'trading_db'
```

### 2. Kubernetes-Specific Connection Parameters

For Kubernetes environments, the connection string was updated to:

- Explicitly disable SSL (`sslmode=disable`) to avoid SSL handshake errors
- Add connection timeout parameters (`connect_timeout=10`) to fail fast on connection issues

```python
# For Kubernetes dev environment
if self.env_type == "dev" and is_kubernetes_host(self.host):
    print(f"[DATABASE] Kubernetes dev environment detected, using special connection for {self.host}")
    # Try with connection timeout and disable SSL
    return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?connect_timeout=10&sslmode=disable"
```

### 3. Connection Retry Mechanism

A retry mechanism was implemented in `src/config/db_retry.py` to handle transient connection issues:

- Exponential backoff between retry attempts
- Detailed error logging for better diagnostics
- Configurable retry parameters (attempts, delay, backoff factor)

```python
async def create_pool_with_retry(self, asyncpg, max_retries=3, initial_delay=1.0):
    """Create a connection pool with retry logic."""
    # Define the connection function to retry
    async def connect_to_db():
        connection_url = self.get_database_url()
        return await asyncpg.create_pool(connection_url)

    # Use retry logic
    return await retry_async(
        connect_to_db,
        retries=max_retries,
        delay=initial_delay,
        backoff_factor=2.0,
        exceptions=(asyncio.TimeoutError, ConnectionRefusedError, OSError)
    )
```

### 4. Enhanced Error Logging

The error logging was enhanced to provide detailed information about connection failures:

- Exception type
- Exception message
- Full exception details
- Connection parameters (with sensitive information masked)

```python
logging.warning(f"Connection attempt {attempt + 1} failed with exception type: {type(e).__name__}")
logging.warning(f"Exception message: {str(e)}")
logging.warning(f"Exception details: {repr(e)}")
```

### 5. Kubernetes Job Configuration

The Kubernetes job YAML files were updated to use the correct database name for each environment:

```yaml
env:
- name: ENVIRONMENT
  value: "dev"
- name: DB_USER
  value: "postgres"
- name: DB_PASSWORD
  value: "postgres"
- name: DB_NAME
  value: "dev_db"  # Changed from trading_db to dev_db for dev environment
```

## Testing

A test script (`test_db_connection_local.py`) was created to verify the database connection logic locally:

- Tests connections for different environments (`dev`, `intg`, `prod`, `local`)
- Uses the retry mechanism to attempt multiple connections
- Provides detailed error logging for connection failures

## Deployment

The solution was deployed by:

1. Updating the Docker image with the new database connection logic
2. Updating the Kubernetes job YAML files to use the correct database name
3. Recreating the Kubernetes jobs with the updated configuration

## Troubleshooting

If database connection issues persist:

1. Check if the PostgreSQL service is accessible from the Kubernetes pod:
   ```bash
   kubectl exec -n <namespace> <pod-name> -- nc -zv timescaledb 5432
   ```

2. Verify the database name is correct for the environment:
   ```bash
   kubectl exec -n <namespace> <pod-name> -- env | grep DB_NAME
   ```

3. Check the database logs for authentication or connection issues:
   ```bash
   kubectl logs -n <namespace> <timescaledb-pod-name>
   ```

4. Use the debug pod to test database connectivity:
   ```bash
   kubectl apply -f k8s/dev/db-debug-pod.yaml
   kubectl exec -it -n ats-dev db-debug-pod -- bash
   ```
