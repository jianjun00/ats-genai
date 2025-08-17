# Environment and Testing Guide
**ATS-GenAI Development and Deployment Environments**

## Overview

This document defines the different environments, execution contexts, and the proper ways to test and run applications across local development and Kubernetes deployments.

## Environment Matrix

### 1. Execution Contexts

| Context | Description | Use Case | Database Access |
|---------|-------------|----------|----------------|
| **Local Dev** | Running on developer machine | Development, debugging, unit tests | Local PostgreSQL or port-forwarded K8s DB |
| **K8s ats-dev** | Kubernetes development environment | Integration testing, data pipeline development | TimescaleDB in ats-dev namespace |
| **K8s ats-intg** | Kubernetes integration environment | Model training, stable data environment | TimescaleDB in ats-intg namespace |
| **K8s ats-prod** | Kubernetes production environment | Live customer recommendations | TimescaleDB in ats-prod namespace |

### 2. Database Environments

| Environment | Table Prefix | Purpose | Data Scope |
|-------------|--------------|---------|------------|
| **test** | `test_` | Unit tests, isolated testing | Minimal test data |
| **dev** | `dev_` | Development and debugging | 6-month subset, 3000 stocks |
| **intg** | `intg_` | Integration and model training | Production snapshot + incrementals |
| **prod** | `prod_` | Production customer data | Complete historical data |

## Testing and Execution Patterns

### 1. Local Development

#### Unit Tests
```bash
# Run unit tests with isolated test database
export PYTHONPATH=src
uv run pytest tests/ -v

# Run specific test with debug
uv run pytest tests/dao/test_instruments_dao.py -v -s
```

#### Local Development Against Local DB
```bash
# Set up local PostgreSQL
export ENVIRONMENT=test
export PYTHONPATH=src
uv run python src/db/migration_manager.py

# Run scripts locally
uv run python src/secmaster/populate_instrument_polygon.py --environment test
```

#### Local Development Against K8s DB (Port-Forward)
```bash
# 1. Port-forward the target K8s database
kubectl port-forward -n ats-dev service/timescaledb 5432:5432

# 2. Run local scripts against K8s database
export PYTHONPATH=src
export ENVIRONMENT=dev
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=postgres
export DB_PASSWORD=postgres
export DB_NAME=ats_dev

# 3. Execute data population scripts
uv run python src/secmaster/populate_instrument_polygon.py \
  --environment dev \
  --db_host localhost \
  --db_port 5432 \
  --db_user postgres \
  --db_password postgres \
  --db_name ats_dev
```

### 2. Kubernetes Execution

#### K8s Job Execution
```bash
# Apply pre-built K8s jobs
kubectl apply -f k8s/dev/db-migrate-job.yaml
kubectl apply -f k8s/dev/instrument-agent-job.yaml

# Monitor job progress
kubectl get jobs -n ats-dev
kubectl logs job/instrument-agent-job -n ats-dev
```

#### Flyte Dynamic Execution
```bash
# Run Python code dynamically in K8s without image rebuilds
pyflyte run --remote \
  scripts/flyte/flyte_instrument_polygon_workflow.py \
  instrument_polygon_workflow \
  --job_type backfill

# Monitor Flyte execution
flytectl get execution -p ats-genai
```

#### Direct Pod Execution (Debug)
```bash
# Create debug pod for interactive testing
kubectl apply -f k8s/dev/debug-pod.yaml
kubectl exec -it debug-pod -n ats-dev -- bash

# Inside pod: run scripts with proper environment
export PYTHONPATH=src
export ENVIRONMENT=dev
python src/secmaster/populate_instrument_polygon.py --environment dev
```

## Environment-Specific Configuration

### 1. Local Development (.env files)
```bash
# .env.local
ENVIRONMENT=test
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=trading_db
POLYGON_API_KEY=your_key_here
```

### 2. K8s Secrets Management
```bash
# Check existing secrets
kubectl get secrets -n ats-dev
kubectl describe secret db-credentials-dev -n ats-dev

# Create new secrets (if needed)
kubectl create secret generic api-keys-dev \
  --from-literal=POLYGON_API_KEY=your_key \
  --from-literal=TIINGO_API_KEY=your_key \
  -n ats-dev
```

### 3. Gin Configuration Files
```bash
# Local development
config/app.gin          # Test environment
config/app_docker.gin    # Docker/local containers

# K8s environments  
config/app_intg.gin      # Integration environment
config/app_prod.gin      # Production environment
```

## Data Population Workflows

### 1. ats-dev Setup (Development)

#### Step 1: Database Setup
```bash
# Option A: Use K8s job
kubectl apply -f k8s/dev/db-migrate-job.yaml

# Option B: Port-forward + local execution
kubectl port-forward -n ats-dev service/timescaledb 5432:5432 &
export PYTHONPATH=src
uv run python src/db/migration_manager.py --environment dev --db_host localhost
```

#### Step 2: Instrument Population (3000 stocks)
```bash
# Port-forward approach (recommended for development)
kubectl port-forward -n ats-dev service/timescaledb 5432:5432 &

# Run existing instrument population with dev environment
PYTHONPATH=src uv run python src/secmaster/populate_instrument_polygon.py \
  --environment dev \
  --db_host localhost \
  --db_port 5432 \
  --db_user postgres \
  --db_password postgres \
  --db_name ats_dev
```

#### Step 3: Daily Price Backfill (5 years)
```bash
# Continue with port-forward active
PYTHONPATH=src uv run python src/market_data/eod/daily_price_polygon.py \
  --environment dev \
  --start-date 2020-01-01 \
  --end-date 2025-01-16 \
  --db_host localhost \
  --db_port 5432
```

### 2. ats-intg Setup (Integration)

#### Weekly Snapshot Refresh
```bash
# Use K8s CronJob for automated snapshots
kubectl apply -f k8s/intg/weekly-snapshot-cronjob.yaml

# Manual snapshot trigger
kubectl create job --from=cronjob/weekly-snapshot manual-snapshot-$(date +%Y%m%d) -n ats-intg
```

#### Model Training Data Preparation
```bash
# Use Flyte for large-scale data processing
pyflyte run --remote \
  scripts/flyte/prepare_training_data_workflow.py \
  prepare_training_data \
  --start_date 2020-01-01 \
  --end_date 2025-01-16 \
  --symbols_count 3000
```

### 3. ats-prod Setup (Production)

#### Blue-Green Deployment
```bash
# Deploy to staging first
kubectl apply -f k8s/prod/staging-deployment.yaml

# Validate and promote to production
scripts/deployment/validate_and_promote.sh
```

## Testing Strategies

### 1. Unit Testing (Isolated)
- **Environment**: Local with `test_` prefix tables
- **Database**: Temporary test database per test
- **Scope**: Individual functions, DAOs, business logic
- **Command**: `PYTHONPATH=src uv run pytest tests/`

### 2. Integration Testing (K8s ats-dev)
- **Environment**: ats-dev namespace
- **Database**: Persistent dev database with `dev_` prefix
- **Scope**: End-to-end workflows, API testing
- **Command**: Port-forward + integration test scripts

### 3. Performance Testing (K8s ats-intg)
- **Environment**: ats-intg namespace
- **Database**: Production-like data volume
- **Scope**: Load testing, performance validation
- **Command**: Flyte workflows for scale testing

### 4. Production Validation (K8s ats-prod)
- **Environment**: ats-prod namespace
- **Database**: Live production data
- **Scope**: Smoke tests, health checks
- **Command**: Automated monitoring and alerting

## Best Practices

### 1. Environment Isolation
- ✅ **Always use environment-aware table prefixes** (`env.get_table_name()`)
- ✅ **Never hardcode database names or connection strings**
- ✅ **Use secrets for sensitive configuration in K8s**
- ✅ **Validate environment before running destructive operations**

### 2. Development Workflow
- ✅ **Start with unit tests locally**
- ✅ **Use port-forward for K8s database development**
- ✅ **Test scripts in ats-dev before promoting to ats-intg**
- ✅ **Use Flyte for large-scale data processing**

### 3. Data Safety
- ✅ **Always backup before major operations**
- ✅ **Use read-only connections for analysis**
- ✅ **Validate data quality after population**
- ✅ **Monitor resource usage during large operations**

## Troubleshooting

### Common Issues

#### Port-Forward Connection Issues
```bash
# Check if port-forward is active
netstat -tuln | grep 5432

# Restart port-forward
pkill -f "kubectl port-forward"
kubectl port-forward -n ats-dev service/timescaledb 5432:5432 &
```

#### Environment Configuration Issues
```bash
# Check current environment settings
env | grep -E "(ENVIRONMENT|DB_|PYTHONPATH)"

# Verify database connectivity
PYTHONPATH=src uv run python -c "
from config.environment import Environment
env = Environment()
print(f'Environment: {env.env_type}')
print(f'DB URL: {env.get_database_url()}')
"
```

#### K8s Resource Issues
```bash
# Check pod resources
kubectl top pods -n ats-dev

# Check pod logs
kubectl logs -f deployment/your-app -n ats-dev

# Check service connectivity
kubectl exec -it debug-pod -n ats-dev -- ping timescaledb
```

## Quick Reference Commands

### Setup ats-dev with 3000 instruments
```bash
# 1. Port-forward database
kubectl port-forward -n ats-dev service/timescaledb 5432:5432 &

# 2. Run migrations
PYTHONPATH=src uv run python src/db/migration_manager.py

# 3. Populate instruments
PYTHONPATH=src uv run python src/secmaster/populate_instrument_polygon.py --environment dev

# 4. Populate unified instruments  
PYTHONPATH=src uv run python src/secmaster/populate_unified_instruments.py

# 5. Backfill daily prices
PYTHONPATH=src uv run python src/market_data/eod/daily_price_polygon.py --start-date 2020-01-01

# 6. Reconcile data
PYTHONPATH=src uv run python src/market_data/eod/unify_daily_prices.py
```

This guide ensures clear separation between local development and K8s environments while providing efficient workflows for each context.