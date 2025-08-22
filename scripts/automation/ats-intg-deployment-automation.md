# ATS-INTG Deployment Automation

## Overview
Complete automation pipeline for deploying ats-intg environment with:
- Kubernetes cluster creation
- Database setup with persistent storage
- Real vendor data import (Polygon, Tiingo, etc.)
- Universe creation and backtesting infrastructure

## Automation Architecture

### Phase 1: Infrastructure Setup
```bash
# 1. Cluster Creation
scripts/automation/create-k8s-cluster.sh ats-intg

# 2. Database Deployment
kubectl apply -f k8s/environments/ats-intg/persistent-database.yaml

# 3. Secret Management
kubectl apply -f k8s/environments/ats-intg/api-secrets.yaml
```

### Phase 2: Database Schema & Vendors
```bash
# 4. Run migrations
kubectl apply -f k8s/environments/ats-intg/database-migration-job.yaml

# 5. Setup vendor integrations
kubectl apply -f k8s/environments/ats-intg/setup-vendors-job.yaml
```

### Phase 3: Real Data Import
```bash
# 6. Import instruments from Polygon
kubectl apply -f k8s/environments/ats-intg/import-polygon-instruments-job.yaml

# 7. Import historical prices (5 years)
kubectl apply -f k8s/environments/ats-intg/import-historical-prices-job.yaml

# 8. Import market cap data
kubectl apply -f k8s/environments/ats-intg/import-market-cap-job.yaml
```

### Phase 4: Universe Creation
```bash
# 9. Create modeling universes
kubectl apply -f k8s/environments/ats-intg/create-universes-job.yaml

# 10. Validate data quality
kubectl apply -f k8s/environments/ats-intg/data-validation-job.yaml
```

## Implementation Files

### 1. Cluster Creation Script
**`scripts/automation/create-k8s-cluster.sh`**
```bash
#!/bin/bash
set -e

ENVIRONMENT=$1
if [ -z "$ENVIRONMENT" ]; then
    echo "Usage: $0 <environment>"
    exit 1
fi

echo "🚀 Creating ATS-${ENVIRONMENT} Kubernetes cluster..."

# Create namespace
kubectl create namespace ats-${ENVIRONMENT} --dry-run=client -o yaml | kubectl apply -f -

# Setup persistent volumes
kubectl apply -f k8s/environments/${ENVIRONMENT}/pvc-setup.yaml

# Deploy TimescaleDB with persistence
kubectl apply -f k8s/environments/${ENVIRONMENT}/timescaledb-deployment.yaml

# Wait for database
kubectl wait --for=condition=available deployment/timescaledb -n ats-${ENVIRONMENT} --timeout=300s

echo "✅ Cluster ats-${ENVIRONMENT} ready"
```

### 2. Real Data Import Jobs

**`k8s/environments/ats-intg/import-polygon-instruments-job.yaml`**
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: import-polygon-instruments
  namespace: ats-intg
spec:
  template:
    spec:
      containers:
      - name: import-instruments
        image: ats-data-importer:latest
        env:
        - name: POLYGON_API_KEY
          valueFrom:
            secretKeyRef:
              name: api-secrets
              key: polygon-api-key
        - name: DB_HOST
          value: "timescaledb-service"
        - name: ENVIRONMENT
          value: "intg"
        command: ["/bin/bash"]
        args:
          - -c
          - |
            echo "📦 Importing real instruments from Polygon..."
            PYTHONPATH=src python src/secmaster/populate_instrument_polygon.py \
              --environment intg \
              --gin_config config/app_intg.gin \
              --full-universe \
              --limit 10000
        volumeMounts:
        - name: app-config
          mountPath: /app/config
      volumes:
      - name: app-config
        configMap:
          name: app-config-intg
      restartPolicy: Never
```

**`k8s/environments/ats-intg/import-historical-prices-job.yaml`**
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: import-historical-prices
  namespace: ats-intg
spec:
  template:
    spec:
      containers:
      - name: import-prices
        image: ats-data-importer:latest
        env:
        - name: POLYGON_API_KEY
          valueFrom:
            secretKeyRef:
              name: api-secrets
              key: polygon-api-key
        - name: TIINGO_API_KEY
          valueFrom:
            secretKeyRef:
              name: api-secrets
              key: tiingo-api-key
        command: ["/bin/bash"]
        args:
          - -c
          - |
            echo "📈 Importing 5 years of historical price data..."
            
            # Import Polygon daily data
            PYTHONPATH=src python scripts/backfill/run_unified_5year_backfill.py \
              --mode full \
              --start-date 2019-01-01 \
              --end-date 2024-12-31 \
              --chunk-days 90 \
              --batch-size 50
            
            # Import Tiingo data for cross-validation
            PYTHONPATH=src python src/market_data/eod/daily_price_tiingo.py \
              --environment intg \
              --backfill-years 5
        volumeMounts:
        - name: app-config
          mountPath: /app/config
      volumes:
      - name: app-config
        configMap:
          name: app-config-intg
      restartPolicy: Never
```

### 3. Configuration Management

**`k8s/environments/ats-intg/api-secrets.yaml`**
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: api-secrets
  namespace: ats-intg
type: Opaque
data:
  polygon-api-key: <base64-encoded-key>
  tiingo-api-key: <base64-encoded-key>
  finnhub-api-key: <base64-encoded-key>
  alpha-vantage-api-key: <base64-encoded-key>
```

**`config/app_intg.gin`**
```python
# Integration environment configuration
Environment.table_prefix = "intg_"
Environment.database_url = "postgresql://postgres:intg_password@timescaledb-service:5432/ats_intg"

# API Configuration
PolygonConfig.api_key = %POLYGON_API_KEY
PolygonConfig.rate_limit = 5  # requests per minute for paid plan
PolygonConfig.batch_size = 1000

TiingoConfig.api_key = %TIINGO_API_KEY  
TiingoConfig.rate_limit = 50  # requests per hour

# Data Import Settings
DataImportConfig.historical_years = 5
DataImportConfig.universe_size = 10000
DataImportConfig.enable_cross_validation = True
```

### 4. Master Automation Script

**`scripts/automation/deploy-ats-intg.sh`**
```bash
#!/bin/bash
set -e

echo "🚀 Starting ATS-INTG Full Deployment Automation..."

# Phase 1: Infrastructure
echo "📋 Phase 1: Infrastructure Setup"
./scripts/automation/create-k8s-cluster.sh intg
kubectl apply -f k8s/environments/ats-intg/persistent-database.yaml
kubectl apply -f k8s/environments/ats-intg/api-secrets.yaml

# Phase 2: Schema Setup
echo "📋 Phase 2: Database Schema"
kubectl apply -f k8s/environments/ats-intg/database-migration-job.yaml
kubectl wait --for=condition=complete job/database-migration -n ats-intg --timeout=300s

# Phase 3: Data Import
echo "📋 Phase 3: Real Data Import"
kubectl apply -f k8s/environments/ats-intg/import-polygon-instruments-job.yaml
kubectl wait --for=condition=complete job/import-polygon-instruments -n ats-intg --timeout=1800s

kubectl apply -f k8s/environments/ats-intg/import-historical-prices-job.yaml
kubectl wait --for=condition=complete job/import-historical-prices -n ats-intg --timeout=7200s  # 2 hours

kubectl apply -f k8s/environments/ats-intg/import-market-cap-job.yaml
kubectl wait --for=condition=complete job/import-market-cap -n ats-intg --timeout=600s

# Phase 4: Universe Creation
echo "📋 Phase 4: Universe Creation"
kubectl apply -f k8s/environments/ats-intg/create-universes-job.yaml
kubectl wait --for=condition=complete job/create-universes -n ats-intg --timeout=600s

# Phase 5: Validation
echo "📋 Phase 5: Data Validation"
kubectl apply -f k8s/environments/ats-intg/data-validation-job.yaml
kubectl wait --for=condition=complete job/data-validation -n ats-intg --timeout=300s

echo "🎉 ATS-INTG deployment completed successfully!"
echo "📊 Environment: ats-intg"
echo "🗄️ Database: TimescaleDB with 5 years historical data"
echo "📈 Universe: 10,000 real instruments with market data"
echo "✅ Ready for backtesting and analysis"

# Display connection info
kubectl get services -n ats-intg
echo ""
echo "🔗 Access database:"
echo "kubectl port-forward svc/timescaledb-service 5432:5432 -n ats-intg"
```

### 5. Data Validation and Quality Checks

**`k8s/environments/ats-intg/data-validation-job.yaml`**
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: data-validation
  namespace: ats-intg
spec:
  template:
    spec:
      containers:
      - name: validate-data
        image: ats-data-importer:latest
        command: ["/bin/bash"]
        args:
          - -c
          - |
            echo "🔍 Running comprehensive data validation..."
            
            PYTHONPATH=src python scripts/validation/validate_data_quality.py \
              --environment intg \
              --check-completeness \
              --check-consistency \
              --generate-report \
              --output-file /tmp/data-quality-report.json
            
            echo "📊 Data quality validation completed"
            cat /tmp/data-quality-report.json
      restartPolicy: Never
```

## Usage

### Quick Start (One Command)
```bash
# Deploy complete ats-intg environment
./scripts/automation/deploy-ats-intg.sh
```

### Step-by-Step (For debugging)
```bash
# 1. Infrastructure only
./scripts/automation/create-k8s-cluster.sh intg

# 2. Schema setup
kubectl apply -f k8s/environments/ats-intg/database-migration-job.yaml

# 3. Import instruments (real data)
kubectl apply -f k8s/environments/ats-intg/import-polygon-instruments-job.yaml

# 4. Import historical prices (5 years)
kubectl apply -f k8s/environments/ats-intg/import-historical-prices-job.yaml

# 5. Create universes
kubectl apply -f k8s/environments/ats-intg/create-universes-job.yaml
```

### Monitoring and Troubleshooting
```bash
# Check job status
kubectl get jobs -n ats-intg

# View logs
kubectl logs job/import-polygon-instruments -n ats-intg

# Check data quality
kubectl logs job/data-validation -n ats-intg

# Database access
kubectl port-forward svc/timescaledb-service 5432:5432 -n ats-intg
PGPASSWORD=intg_password psql -h localhost -p 5432 -U postgres -d ats_intg
```

## Key Benefits

1. **Automated Infrastructure**: One command creates entire environment
2. **Real Data Import**: Uses actual Polygon/Tiingo APIs instead of mock data
3. **Environment Isolation**: Separate ats-dev, ats-intg, ats-prod namespaces
4. **Persistent Storage**: Data survives pod restarts/cluster reboots
5. **Scalable**: Handles 10k+ instruments with 5 years of data
6. **Validated**: Built-in data quality checks and validation
7. **Reproducible**: Git-tracked configuration and scripts

## Prerequisites

1. API keys for Polygon, Tiingo, Finnhub
2. Kubernetes cluster access
3. Docker images built for data importers
4. Persistent volume storage configured

This automation framework allows you to deploy a complete, production-like ats-intg environment with real market data instead of mock data, fully automated from cluster creation to universe validation.