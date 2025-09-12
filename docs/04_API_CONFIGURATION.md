# 🔑 ATS API & Configuration Reference

**Complete API keys, vendor monitoring, endpoint reference, and environment configuration for the ATS platform.**

---

## 🚨 API Key Management - Single Source of Truth

### CRITICAL: All API keys MUST use these exact values from .env.test (git commit 5168e8e83)

**✅ VERIFIED WORKING API KEYS (Last validated: 2025-09-11):**
```bash
# These are the ONLY valid API keys - use these exact values everywhere
POLYGON_API_KEY="wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"      # ✅ VERIFIED WORKING
TIINGO_API_KEY="5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"    # ✅ VERIFIED WORKING
EODHD_API_KEY="68aa0c7d2fe831.67386369"                   # ✅ VERIFIED WORKING
FMP_API_KEY="Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr"            # ✅ Available
ALPHA_VANTAGE_API_KEY="9GI0NZ3V4VNFX271"                  # ✅ Available
```

### API Key Validation - MANDATORY Before Service Deployment

**ALWAYS validate API keys before deploying services:**
```bash
# Validate all API keys (uses correct keys automatically)
python3 scripts/validate_api_keys.py

# Expected output: ✅ All API keys are valid!
```

### Service Integration - Centralized Management

**All services automatically use correct API keys via run_intg.py and run_dev.py:**

#### Integration Environment Services:
```bash
# All integration services use centralized API key management
python3 scripts/run_intg.py start --service realtime-minute-collector
python3 scripts/run_intg.py start --service analytics
python3 scripts/run_intg.py start --service news-realtime

# API keys automatically configured with correct values
```

#### Development Environment Services:
```bash
# All dev services use centralized API key management
python3 scripts/run_dev.py start --service analytics
python3 scripts/run_dev.py start --service postgres

# API keys automatically configured with correct values
```

### Service-Specific API Key Requirements

| Service | Required Keys | Verification Command |
|---------|---------------|---------------------|
| **realtime-minute-collector** | POLYGON, TIINGO, EODHD | `docker logs ats-intg-realtime-minute-collector` |
| **news-realtime** | POLYGON, TIINGO, EODHD | `docker logs ats-intg-news-realtime` |
| **analytics** | All keys (fallback) | `curl http://localhost:4000/health` |

### Troubleshooting API Key Issues

#### Step 1: Validate Current Keys
```bash
# Test all API keys
python3 scripts/validate_api_keys.py

# Test specific vendor directly
curl -s "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2025-09-11/2025-09-11?adjusted=true&sort=asc&limit=1&apikey=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"
```

#### Step 2: Check Service Configuration
```bash
# Verify service has correct API keys
docker inspect <service_name> | grep -A 10 "Env"

# Should show: POLYGON_API_KEY=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD
# Should show: EODHD_API_KEY=68aa0c7d2fe831.67386369
# Should show: TIINGO_API_KEY=5f40b4f36e171405746304ec0e5a6f3aa9ca77e5
```

#### Step 3: Fix Authentication Failures
```bash
# If service shows 401/403 errors, restart with correct keys
docker stop <service_name>
docker rm <service_name>

# Use run_intg.py or run_dev.py to ensure correct configuration
python3 scripts/run_intg.py start --service <service_name>
```

---

## 🌐 API Endpoints Reference

### ATS-DEV Environment (Development)
```bash
# Analytics Service
http://localhost:3000/health          # Health check endpoint
http://localhost:3000/eda            # EDA Dashboard interface
http://localhost:3000/api/           # Analytics API endpoints

# Core Analytics APIs
http://localhost:3000/api/datasets              # List all training datasets
http://localhost:3000/api/datasets/{id}         # Get specific dataset details
http://localhost:3000/api/sequences/{id}        # Get dataset sequences
http://localhost:3000/api/features/{dataset_id} # Get feature metadata

# Data Query APIs
http://localhost:3000/api/instruments            # List instruments
http://localhost:3000/api/daily_prices          # Daily price data
http://localhost:3000/api/minute_bars           # Minute bar data
http://localhost:3000/api/universe_state        # Universe membership data

# API Service
http://localhost:8000/health          # API health check
http://localhost:8000/api/           # Main API endpoints
http://localhost:8000/api/backtest   # Backtesting APIs
http://localhost:8000/api/models     # Model registry APIs

# Database Direct Access
postgresql://postgres:dev_password@localhost:3432/dev_db
```

### ATS-INTG Environment (Integration Testing)
```bash
# Analytics Service
http://localhost:4000/health          # Health check endpoint
http://localhost:4000/eda            # EDA Dashboard interface
http://localhost:4000/api/           # Analytics API endpoints

# Monitoring & Metrics
http://localhost:4080/health          # Prometheus metrics health
http://localhost:4080/metrics        # Prometheus metrics endpoint
http://localhost:4080/api/v1/query   # Prometheus query API

# Grafana Dashboards
http://localhost:4002/               # Grafana dashboards (admin/admin)
http://localhost:4002/api/health     # Grafana health check
http://localhost:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f  # Vendor monitoring

# Prometheus Server
http://localhost:4091/-/ready        # Prometheus server ready check
http://localhost:4091/-/healthy      # Prometheus server health

# Database Direct Access
postgresql://postgres:intg_password@localhost:4432/intg_db
```

### Service Discovery Commands
```bash
# Check running services and their ports
python scripts/run_dev.py status                    # ATS-DEV services
python scripts/run_intg.py status                   # ATS-INTG services
docker ps | grep -E "(ats-dev|intg)"               # Container status with ports

# Get external access info for any service
./scripts/get_external_access.sh service-name      # Service endpoint discovery script

# Test endpoints are working
curl -f http://localhost:3000/health               # ATS-DEV analytics
curl -f http://localhost:4000/health               # ATS-INTG analytics
curl -f http://localhost:4080/health               # ATS-INTG metrics
```

### API Usage Examples
```bash
# Health checks (always test these first)
curl -s http://localhost:3000/health | jq
curl -s http://localhost:4000/health | jq

# Analytics API examples
curl -s http://localhost:3000/api/datasets | jq
curl -s http://localhost:3000/api/datasets/1 | jq
curl -s http://localhost:3000/api/instruments?limit=10 | jq

# Metrics collection
curl -s http://localhost:4080/metrics | grep "ats_"

# Prometheus queries
curl -s "http://localhost:4080/api/v1/query?query=up" | jq

# Dashboard access (open in browser)
open http://localhost:3000/eda                     # EDA interface
open http://localhost:4002/                        # Grafana dashboards (admin/admin)

# Real-time minute bar monitoring
open http://localhost:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f/ats-vendor-monitoring-dashboard-fixed
```

---

## 📊 Training Data API

### Dataset Management APIs
```bash
# List all training datasets
GET http://localhost:3000/api/datasets
# Response: [{"id": 1, "dataset_name": "aapl_tsla_2024", "symbols": ["AAPL", "TSLA"], ...}]

# Get specific dataset details
GET http://localhost:3000/api/datasets/{id}
# Response: {"id": 1, "dataset_name": "...", "total_sequences": 1234, ...}

# Get dataset sequences for EDA
GET http://localhost:3000/api/sequences/{dataset_id}?limit=100&offset=0
# Response: {"sequences": [...], "total_count": 1234, "has_more": true}

# Get feature metadata
GET http://localhost:3000/api/features/{dataset_id}
# Response: {"feature_names": [...], "feature_types": {...}, "statistics": {...}}
```

### Data Query APIs
```bash
# Get instruments
GET http://localhost:3000/api/instruments?limit=100&search=AAPL
# Response: [{"symbol": "AAPL", "company_name": "Apple Inc.", ...}]

# Get daily prices
GET http://localhost:3000/api/daily_prices?symbol=AAPL&start_date=2024-01-01&end_date=2024-12-31
# Response: [{"date": "2024-01-01", "open": 185.64, "high": 186.95, ...}]

# Get minute bars
GET http://localhost:3000/api/minute_bars?symbol=AAPL&date=2024-01-01&timeframe=5m
# Response: [{"timestamp": "2024-01-01T09:30:00Z", "open": 185.64, ...}]

# Get universe state
GET http://localhost:3000/api/universe_state?date=2024-01-01&universe=large_cap
# Response: [{"symbol": "AAPL", "in_universe": true, "market_cap": 3000000000000, ...}]
```

---

## ⚙️ Environment Configuration

### Environment Variable Patterns

**ATS-DEV Environment:**
```bash
# Database Configuration
ENVIRONMENT=dev
DB_HOST=ats-dev-postgres              # Container name for inter-service communication
DB_PORT=5432                         # Internal container port
DB_USER=postgres
DB_PASSWORD=dev_password
DB_NAME=dev_db
DB_URL=postgresql://postgres:dev_password@ats-dev-postgres:5432/dev_db

# File System Paths (Container Perspective)
ATS_DATA_PATH=/data                  # Maps to /mnt/d/ats-data on host
ATS_BACKUP_PATH=/backup             # Maps to /mnt/d/ats-backup on host
ATS_LOGS_PATH=/logs                 # Maps to /mnt/d/ats-logs on host
PYTHONPATH=/workspace/src           # Critical for Python module imports

# Service Configuration
ANALYTICS_SERVICE_PORT=3000
API_SERVICE_PORT=8000
LOG_LEVEL=INFO
DEBUG_MODE=true

# API Keys (Centrally Managed)
POLYGON_API_KEY=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD
TIINGO_API_KEY=5f40b4f36e171405746304ec0e5a6f3aa9ca77e5
EODHD_API_KEY=68aa0c7d2fe831.67386369
```

**ATS-INTG Environment:**
```bash
# Database Configuration
ENVIRONMENT=intg
DB_HOST=ats-intg-postgres            # Container name for inter-service communication
DB_PORT=5432                        # Internal container port
DB_USER=postgres
DB_PASSWORD=intg_password
DB_NAME=intg_db
DB_URL=postgresql://postgres:intg_password@ats-intg-postgres:5432/intg_db

# File System Paths (Same as DEV)
ATS_DATA_PATH=/data
ATS_BACKUP_PATH=/backup
ATS_LOGS_PATH=/logs
PYTHONPATH=/workspace/src

# Service Configuration
ANALYTICS_SERVICE_PORT=3000          # Internal port (exposed as 4000 externally)
API_SERVICE_PORT=8000               # Internal port (exposed as 8001 externally)
PROMETHEUS_PORT=9090                # Internal port (exposed as 4080 externally)
GRAFANA_PORT=3000                   # Internal port (exposed as 4002 externally)
LOG_LEVEL=INFO
DEBUG_MODE=false

# Monitoring Configuration
PROMETHEUS_ENABLED=true
GRAFANA_ENABLED=true
METRICS_COLLECTION_INTERVAL=30s
ALERT_WEBHOOK_URL=https://hooks.slack.com/...

# API Keys (Same as DEV)
POLYGON_API_KEY=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD
TIINGO_API_KEY=5f40b4f36e171405746304ec0e5a6f3aa9ca77e5
EODHD_API_KEY=68aa0c7d2fe831.67386369
```

### Container Configuration Patterns

**Standard Container Environment Variables:**
```bash
# Docker Compose Environment Section
environment:
  - ENVIRONMENT=dev
  - DB_HOST=ats-dev-postgres
  - DB_PORT=5432
  - DB_USER=postgres
  - DB_PASSWORD=dev_password
  - DB_NAME=dev_db
  - PYTHONPATH=/workspace/src
  - ATS_DATA_PATH=/data
  - POLYGON_API_KEY=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD
  - TIINGO_API_KEY=5f40b4f36e171405746304ec0e5a6f3aa9ca77e5
  - EODHD_API_KEY=68aa0c7d2fe831.67386369

# Docker Run Environment Variables
docker run -e ENVIRONMENT=dev \
           -e DB_HOST=ats-dev-postgres \
           -e DB_PORT=5432 \
           -e PYTHONPATH=/workspace/src \
           -e POLYGON_API_KEY=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD \
           your-image
```

### Configuration Validation

**Environment Validation Script:**
```bash
# Validate all environment variables are set correctly
python scripts/validate_env.py

# Check specific environment
python scripts/validate_env.py --environment dev
python scripts/validate_env.py --environment intg

# Expected output: ✅ All environment variables valid
```

**Configuration Health Check:**
```bash
# Test database connectivity
python scripts/run_dev.py query --query "SELECT current_database(), current_user"

# Test API key functionality
python scripts/validate_api_keys.py

# Test file system access
ls -la /data /backup /logs

# Test Python path
python -c "import src.core.config; print('✅ Python imports working')"
```

---

## 🔐 Security Configuration

### API Key Security Best Practices

**✅ SECURE API Key Management:**
- Use environment variables to override defaults when needed
- Never commit new API keys to version control
- Use the validation script before any service deployment
- Monitor logs for authentication failures
- Rotate keys proactively if possible

**❌ SECURITY VIOLATIONS:**
- Hard-coding different API keys in source code
- Using untested/invalid API keys in production
- Ignoring authentication failures without root cause analysis
- Creating manual workarounds that bypass centralized management

### Database Security

**Connection Security:**
```bash
# Use strong passwords (already configured)
dev_password=dev_password      # Development only
intg_password=intg_password    # Integration testing only

# Network isolation
# - Dev database only accessible from ats-network
# - Intg database only accessible from ats-intg-network
# - No external access except via localhost ports
```

**Access Control:**
```bash
# Database user permissions (read-only for analytics)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO analytics_user;

# Admin access only for migrations
GRANT ALL PRIVILEGES ON DATABASE dev_db TO migration_user;
```

---

## 📈 Monitoring & Metrics Configuration

### Prometheus Configuration
```yaml
# /config/prometheus.yml
global:
  scrape_interval: 30s
  evaluation_interval: 30s

scrape_configs:
  - job_name: 'ats-analytics'
    static_configs:
      - targets: ['ats-intg-analytics:3000']
    scrape_interval: 15s

  - job_name: 'ats-postgres'
    static_configs:
      - targets: ['ats-intg-postgres:5432']
    scrape_interval: 60s
```

### Grafana Data Sources
```json
{
  "name": "ATS-INTG-PostgreSQL",
  "type": "postgres",
  "url": "ats-intg-postgres:5432",
  "database": "intg_db",
  "user": "grafana_reader",
  "password": "grafana_password",
  "sslmode": "disable"
}
```

### Custom Metrics Collection
```bash
# Custom application metrics
curl -s http://localhost:4080/metrics | grep -E "ats_|app_"

# Database metrics
curl -s http://localhost:4080/metrics | grep -E "pg_|postgres_"

# System metrics
curl -s http://localhost:4080/metrics | grep -E "node_|system_"
```

---

**🎯 This API and configuration guide provides comprehensive reference for integrating with and configuring all ATS platform services and APIs.**