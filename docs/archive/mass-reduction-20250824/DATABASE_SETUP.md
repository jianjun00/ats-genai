# Database Setup and Verification in Kubernetes

This document outlines the process for setting up and verifying the PostgreSQL database in the Kubernetes cluster for the instrument-agent.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Database Deployment](#database-deployment)
- [Database Schema Initialization](#database-schema-initialization)
- [Database Access](#database-access)
- [Instrument Data Management](#instrument-data-management)
- [Verification Scripts](#verification-scripts)
- [Troubleshooting](#troubleshooting)

## Prerequisites

- Kubernetes cluster with `kubectl` configured
- Access to the `ats-dev` namespace
- Environment variables in `.env.dev` file

## Database Deployment

The PostgreSQL database is deployed as a StatefulSet in the `ats-dev` namespace using the TimescaleDB image.

### Deployment Manifest

The deployment manifest is located at `k8s/dev/postgres-deployment.yaml` and includes:

- ConfigMap with PostgreSQL configuration
- Service for database access
- Deployment with TimescaleDB image and resource limits

### Deployment Steps

1. Apply the deployment manifest:
   ```bash
   kubectl apply -f k8s/dev/postgres-deployment.yaml
   ```

2. Verify the deployment:
   ```bash
   kubectl get pods -n ats-dev
   kubectl get services -n ats-dev
   ```

## Database Schema Initialization

The database schema is initialized using a Kubernetes Job that runs SQL commands to create the necessary tables.

### Schema Initialization Manifest

The initialization job manifest is located at `k8s/dev/init-db-job.yaml` and includes:

- Job that runs SQL commands to create tables
- Tables created: `dev_instruments`, `dev_instrument_aliases`, `dev_instrument_metadata`, `dev_vendors`, `dev_instrument_xrefs`

### Initialization Steps

1. Apply the initialization job manifest:
   ```bash
   kubectl apply -f k8s/dev/init-db-job.yaml
   ```

2. Verify the job completion:
   ```bash
   kubectl get jobs -n ats-dev
   kubectl logs job/init-db-job -n ats-dev
   ```

## Database Access

There are multiple ways to access the database:

### Port-Forwarding

Port-forwarding allows access to the database from the local machine:

```bash
kubectl port-forward service/postgres 5432:5432 -n ats-dev
```

### Database Client Pod

A database client pod provides direct access to the database from within the cluster:

1. Apply the client pod manifest:
   ```bash
   kubectl apply -f k8s/dev/db-client-pod.yaml
   ```

2. Access the database using `psql`:
   ```bash
   kubectl exec -it db-client -n ats-dev -- psql
   ```

## Instrument Data Management

### Adding Instruments

Instruments can be added to the database using the `add_instruments.py` script:

```bash
uv run python add_instruments.py
```

The script adds instruments and their cross-references to vendors (polygon, tiingo).

### Instrument Data Structure

- `dev_instruments`: Stores basic instrument information (symbol, name, exchange)
- `dev_vendors`: Stores vendor information (polygon, tiingo)
- `dev_instrument_xrefs`: Maps instruments to vendors with vendor-specific symbols

## Verification Scripts

Several scripts are available to verify the database setup and data:

### Database Connection Check

The `check_ats_dev_k8s_db.py` script checks if the database is accessible via port-forwarding:

```bash
uv run python check_ats_dev_k8s_db.py
```

### Direct Database Check

The `check_k8s_db.py` script checks the database directly from within the cluster:

```bash
uv run python check_k8s_db.py
```

### Instrument Data Verification

The `check_db_direct.py` script verifies the instrument data in the database:

```bash
uv run python check_db_direct.py
```

## Troubleshooting

### Port-Forwarding Issues

If port-forwarding is not working:

1. Check if port-forwarding is active:
   ```bash
   netstat -tuln | grep 5432
   ```

2. Restart port-forwarding:
   ```bash
   pkill -f "kubectl port-forward"
   kubectl port-forward service/postgres 5432:5432 -n ats-dev
   ```

### Database Connection Issues

If unable to connect to the database:

1. Check if the PostgreSQL pod is running:
   ```bash
   kubectl get pods -n ats-dev
   ```

2. Check PostgreSQL logs:
   ```bash
   kubectl logs deployment/postgres -n ats-dev
   ```

3. Verify the database service:
   ```bash
   kubectl get services -n ats-dev
   ```

### Data Integrity Issues

If there are issues with instrument data:

1. Check if tables exist:
   ```bash
   kubectl exec db-client -n ats-dev -- psql -c '\dt dev_*'
   ```

2. Check instrument counts:
   ```bash
   kubectl exec db-client -n ats-dev -- psql -c 'SELECT COUNT(*) FROM dev_instruments;'
   ```

3. Check xref counts:
   ```bash
   kubectl exec db-client -n ats-dev -- psql -c 'SELECT COUNT(*) FROM dev_instrument_xrefs;'
   ```

4. Run the verification script:
   ```bash
   uv run python check_db_direct.py
   ```
