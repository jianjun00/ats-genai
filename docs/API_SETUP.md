# API Setup and Testing Documentation

This document outlines the process for setting up and testing the ATS API in the Kubernetes cluster.

## Overview

The ATS API provides RESTful endpoints to interact with the trading database, including:
- Health checks
- Database connectivity verification
- Instrument data retrieval
- Cross-reference information

## API Deployment Options

### 1. Job-Based Approach (Recommended for Testing)

We've implemented a Job-based approach for the API, which is ideal for testing and development purposes. This approach:
- Uses the standard Python image
- Installs required dependencies at runtime
- Creates and runs a FastAPI application
- Keeps the container running for a specified duration for testing

#### Deployment Steps

1. **Create the API Test Job YAML**:
   - Located at: `/home/jianjun/ats-genai/k8s/dev/api-test-job.yaml`
   - This job creates a Python FastAPI application with endpoints for database connectivity testing

2. **Deploy the API Test Job**:
   ```bash
   kubectl apply -f k8s/dev/api-test-job.yaml
   ```

3. **Verify Deployment**:
   ```bash
   kubectl get jobs -n ats-dev
   kubectl get pods -n ats-dev
   ```

### 2. Standard Deployment (For Production)

For production use, a standard Deployment should be used with:
- A properly built Docker image
- Appropriate resource limits
- Service for network access
- Ingress for external access (if needed)

## API Access

### Port Forwarding

To access the API locally:

```bash
# Get the pod name
kubectl get pods -n ats-dev

# Set up port forwarding
kubectl port-forward pod/api-test-job-[pod-suffix] 8080:8080 -n ats-dev
```

### Testing API Connectivity

#### Using curl

```bash
# Health check
curl http://localhost:8080/health

# Database connectivity check
curl http://localhost:8080/api/v1/db-check

# Get all instruments
curl http://localhost:8080/api/v1/instruments

# Get specific instrument
curl http://localhost:8080/api/v1/instrument/AAPL
```

#### Using Python Script

We've created a Python script to test API connectivity:
- Located at: `/home/jianjun/ats-genai/test_api_connectivity.py`
- Tests all major endpoints
- Provides formatted JSON output

To run the script:
```bash
uv run python test_api_connectivity.py
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Root endpoint, basic API information |
| `/health` | GET | Health check endpoint for Kubernetes probes |
| `/api/v1/db-check` | GET | Database connectivity check with table information |
| `/api/v1/instruments` | GET | List all instruments in the database |
| `/api/v1/instrument/{symbol}` | GET | Get details for a specific instrument by symbol |

## Database Verification Scripts

We have several scripts to verify database connectivity and check instrument data:

### 1. `check_trading_db.py`

This script checks the trading database tables and data, with support for both local and Kubernetes database access:

```bash
# For local database
uv run python check_trading_db.py

# For Kubernetes database with port-forwarding
uv run python check_trading_db.py --k8s
```

The script will:
- Set up port-forwarding to the PostgreSQL pod if using Kubernetes
- Connect to the database
- List all tables
- Show instrument-related tables with row counts
- Display sample data from instrument tables
- Show cross-references by vendor

### 2. `check_instrument_count.py`

This script provides a focused view of instrument counts and cross-references:

```bash
# For local database
uv run python check_instrument_count.py

# For Kubernetes database with port-forwarding
uv run python check_instrument_count.py --k8s

# Using DAOs instead of direct database access
uv run python check_instrument_count.py --use-dao
```

The script will:
- Connect to the database (with port-forwarding if using Kubernetes)
- Count instruments
- Show a sample of instruments
- Display cross-references by vendor/provider with counts
- Show vendor names when available

## Troubleshooting

### Common Issues

1. **Port Forwarding Issues**:
   - Ensure the pod is running (`kubectl get pods -n ats-dev`)
   - Check if another process is using port 8080
   - Try a different local port: `kubectl port-forward pod/api-test-job-[pod-suffix] 8081:8080 -n ats-dev`
   - If you see `address already in use` errors, use a different port number
   - For database port-forwarding, our scripts now use port 5434 instead of 5433 to avoid conflicts

2. **Database Connectivity Issues**:
   - Verify PostgreSQL pod is running (`kubectl get pods -n ats-dev | grep postgres`)
   - Check database credentials in the API script
   - Ensure database initialization was completed successfully
   - For Kubernetes database access, check that port-forwarding is established successfully
   - Verify table prefixes are correct (Kubernetes database uses `dev_` prefix)

3. **API Not Responding**:
   - Check pod logs: `kubectl logs pod/api-test-job-[pod-suffix] -n ats-dev`
   - Verify the API server started successfully
   - Check for Python dependency issues in the logs
   - Ensure the API pod has network access to the PostgreSQL pod

## Monitoring Tools

### API Job Monitoring Script

We've created a Python script to monitor the API test job in Kubernetes:
- Located at: `/home/jianjun/ats-genai/monitor_api_job.py`
- Provides real-time status of the API job and pods
- Can display logs from the API pods
- Supports port-forwarding setup

#### Usage Examples

```bash
# Basic status check
uv run python monitor_api_job.py

# Show logs from the API pods
uv run python monitor_api_job.py --logs

# Set up port-forwarding to the API
uv run python monitor_api_job.py --port-forward

# Watch job status continuously
uv run python monitor_api_job.py --watch --interval 10

# Combine options
uv run python monitor_api_job.py --logs --port-forward --local-port 8081
```

#### Available Options

| Option | Description |
|--------|-------------|
| `--namespace` | Kubernetes namespace (default: ats-dev) |
| `--job-name` | Job name (default: api-test-job) |
| `--logs` | Show logs from the job pods |
| `--tail` | Number of log lines to show (default: 50) |
| `--port-forward` | Set up port-forwarding to the API pod |
| `--local-port` | Local port for port-forwarding (default: 8080) |
| `--watch` | Watch job status continuously |
| `--interval` | Watch interval in seconds (default: 5) |

## Next Steps

1. **Production Deployment**:
   - Build a proper Docker image for the API
   - Create a Deployment with appropriate resource limits
   - Set up a Service for network access
   - Configure Ingress for external access (if needed)

2. **API Enhancement**:
   - Add authentication and authorization
   - Implement rate limiting
   - Add more comprehensive endpoints for data access
   - Implement proper error handling and logging

3. **Integration with Frontend**:
   - Develop frontend components to interact with the API
   - Implement data visualization for instrument data
   - Create dashboards for monitoring

## Conclusion

The API is now successfully deployed and tested in the Kubernetes cluster. It provides access to the instrument data in the PostgreSQL database and can be used for further development and testing.
