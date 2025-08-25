# API and Database Verification Script

This document describes the usage and features of the `verify_api_and_db.py` script, which is designed to verify the connectivity and data consistency between the ATS API and PostgreSQL database in a Kubernetes environment.

## Overview

The verification script performs the following checks:
- API health endpoint verification
- API database connectivity check
- API instruments endpoint verification
- Direct database connectivity verification
- Database schema and table verification
- Data consistency comparison between API and database

## Prerequisites

- Kubernetes cluster with the ATS API and PostgreSQL database running
- `kubectl` CLI configured to access the cluster
- Python 3.8+ with the following packages:
  - aiohttp
  - asyncpg
  - asyncio

## Usage

```bash
uv run python verify_api_and_db.py [options]
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--namespace` | `ats-dev` | Kubernetes namespace where the API and database are running |
| `--api-job` | `api-test-job` | Name of the API job or deployment |
| `--api-port` | `8090` | Local port for API port-forwarding |
| `--db-port` | `5440` | Local port for database port-forwarding |
| `--skip-api` | `False` | Skip API verification |
| `--skip-db` | `False` | Skip database verification |
| `--skip-compare` | `False` | Skip API and database comparison |
| `--db-user` | `postgres` | Database username |
| `--db-password` | `postgres` | Database password |
| `--db-name` | `ats_dev` | Database name |

## Features

### Port-Forwarding Management

The script automatically:
- Discovers the API pod and PostgreSQL pod in the specified namespace
- Sets up port-forwarding to access these services locally
- Cleans up port-forwarding processes when the script completes or encounters an error

### API Verification

The script verifies the following API endpoints:
- `/health` - Checks if the API service is healthy
- `/api/v1/db-check` - Verifies the API's connection to the database
- `/api/v1/instruments` - Lists all instruments from the API
- `/api/v1/instrument/{symbol}` - Gets details for a specific instrument (e.g., AAPL)

### Database Verification

The script performs the following database checks:
- Connects directly to the PostgreSQL database
- Lists all tables in the database
- Queries the instruments table to verify data access
- Retrieves cross-references for specific instruments

### Data Consistency Comparison

The script compares data between the API and database:
- Compares the count of instruments
- Compares specific instrument details (symbol, name, exchange)
- Handles different response structures between API and database
- Provides detailed output for any mismatches

## Error Handling

The script includes robust error handling:
- Graceful handling of empty instrument lists
- Proper cleanup of resources even if errors occur
- Detailed error messages for troubleshooting
- Fallback checks with alternative symbols if primary checks fail

## Example Output

```
=== Verifying API Endpoints ===
Health check successful: { "status": "healthy", "service": "api-test" }
Database check successful: { "status": "success", "message": "Database connection successful" }
Found 11 instruments
Found instrument: { "status": "success", "instrument": { "id": 1, "symbol": "AAPL", ... } }

=== Verifying Database ===
Found 5 tables: dev_instrument_aliases, dev_instrument_metadata, ...
Found 11 instruments
Found instrument: { "id": 1, "symbol": "AAPL", "name": "Apple Inc.", ... }
Found 1 cross-references: polygon: AAPL

=== Comparing API and Database Data ===
API instruments count: 11
Database instruments count: 11
Instrument AAPL data matches between API and database

=== Verification Complete ===
All checks passed successfully!
```

## Troubleshooting

If the verification fails, check the following:
1. Ensure the API job is running (`kubectl get pods -n ats-dev`)
2. Verify the PostgreSQL pod is running (`kubectl get pods -n ats-dev | grep postgres`)
3. Check for port conflicts on your local machine
4. Examine the API logs (`kubectl logs -n ats-dev <api-pod-name>`)
5. Check the database logs (`kubectl logs -n ats-dev <postgres-pod-name>`)
