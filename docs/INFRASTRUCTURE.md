# 🏗️ ATS Infrastructure Guide

**Database connections, Docker networking, service architecture, and persistent storage for the ATS platform.**

---

## 📊 Database Connection Reference

### Two-Environment Docker Architecture

**ATS-DEV (Development):**
- **Purpose**: Primary development, unit testing, feature development
- **Database**: PostgreSQL 13 on `localhost:3432`
- **Container**: `ats-dev-postgres` (postgres:13 image)
- **Connection**: `dev_db` database, `postgres` user, `dev_password`
- **Table Prefix**: `dev_*` (e.g., `dev_instruments`, `dev_daily_prices`)
- **Usage**: `python3 scripts/run_dev.py --environment dev query --query "..."`

**ATS-INTG (Integration):**
- **Purpose**: CI/CD integration testing, pre-production validation
- **Database**: TimescaleDB (PostgreSQL 13.15) on `localhost:4432`
- **Container**: `ats-intg-postgres` (timescale/timescaledb:latest-pg13 image)
- **Connection**: `intg_db` database, `postgres` user, `intg_password`
- **Table Prefix**: `intg_*` (e.g., `intg_instruments`, `intg_daily_prices`)
- **Usage**: `python3 scripts/run_dev.py --environment intg query --query "..."`

### Database Connection Examples

**Auto-Detection (Recommended):**
```bash
# Automatically detects available environment based on running containers
python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instruments"     # → dev env
python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM intg_instruments"    # → intg env
```

**Explicit Environment Selection:**
```bash
# Force specific environment
python3 scripts/run_dev.py --environment dev query --query "SELECT current_database()"
python3 scripts/run_dev.py --environment intg query --query "SELECT current_database()"
```

**Direct Database Connections:**
```bash
# ATS-DEV (password required)
PGPASSWORD=dev_password psql -h localhost -p 3432 -U postgres -d dev_db -c "SELECT version()"

# ATS-INTG (password required)
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT version()"
```

### Quick Reference - Database Connections

| Environment | Host | Port | Database | Username | Password | Connection String |
|-------------|------|------|----------|----------|----------|-------------------|
| **ATS-DEV** | localhost | 3432 | dev_db | postgres | dev_password | `postgresql://postgres:dev_password@localhost:3432/dev_db` |
| **ATS-INTG** | localhost | 4432 | intg_db | postgres | intg_password | `postgresql://postgres:intg_password@localhost:4432/intg_db` |

---

## 🐳 Docker Network Architecture

### CRITICAL: Docker Network Connectivity Patterns

**Docker Container Communication Requirements:**
- ✅ **Containers must be on same network** to communicate via container names
- ✅ **Database host references** must match actual container names
- ✅ **Network connections** must be established for multi-container services
- ❌ **Different networks** prevent container-to-container communication

**ATS Network Architecture:**
- **`ats-network`**: ATS-DEV services (ats-dev-postgres, ats-dev-analytics)
- **`ats-intg-network`**: ATS-INTG services (ats-intg-scheduler, ats-intg-analytics)
- **Cross-network**: ats-intg-postgres connected to both networks for compatibility

**Critical Connection Pattern:**
```bash
# ✅ CORRECT: Connect database to service network
docker network connect ats-intg-network ats-intg-postgres

# ✅ CORRECT: Use container name as hostname
DB_HOST=ats-intg-postgres  # Not localhost in container configs

# ❌ WRONG: Missing network connection causes "host not found"
# ❌ WRONG: Using localhost:4432 inside containers (use container:5432)
```

### Database Connectivity Checklist
1. **Container on correct network**: `docker network inspect network-name`
2. **Database host matches container name**: `DB_HOST=ats-intg-postgres`
3. **Use internal port (5432)** not external port (4432) in containers
4. **Test connectivity**: `docker exec container psycopg2.connect()` test

### Troubleshooting Network Issues
```bash
# Check container networks
docker network ls | grep ats
docker network inspect ats-intg-network --format "{{range .Containers}}{{.Name}} {{end}}"

# Connect missing containers to networks
docker network connect ats-intg-network container-name

# Verify database reachability from container
docker exec scheduler-container python3 -c "import psycopg2; psycopg2.connect(host='db-container', port=5432, user='postgres')"
```

---

## 💾 Persistent Storage Architecture

### ATS Persistent Storage (D: Drive)

**Docker Volume Configuration:**
- **📁 Data**: `/mnt/d/ats-data` → `/data` (in containers)
- **📁 Backup**: `/mnt/d/ats-backup` → `/backup` (in containers)
- **📁 Logs**: `/mnt/d/ats-logs` → `/logs` (in containers)

**PostgreSQL Database Storage:**
- **🗄️ ATS-DEV**: Docker volume `postgres-data-new`
- **🗄️ ATS-INTG**: Docker volume `postgres-intg-data`
- **📍 Location**: Managed by Docker in `/var/snap/docker/common/var-lib-docker/volumes/`

**Environment Variables Available in Containers:**
- `ATS_DATA_PATH=/data`
- `ATS_BACKUP_PATH=/backup`
- `ATS_LOGS_PATH=/logs`

**Usage in Code:**
```python
import os
data_path = os.getenv('ATS_DATA_PATH', '/data')
backup_path = os.getenv('ATS_BACKUP_PATH', '/backup')
log_path = os.getenv('ATS_LOGS_PATH', '/logs')
```

---

## 📊 Two-Stream Data Architecture

### CRITICAL: Two-Stream Data Storage Architecture

#### Real-Time Database Storage (Intraday Trading):
- **📊 Polygon Real-Time**: Database table `dev_daily_prices_polygon` (partial day data, every 30min)
- **📊 Tiingo Real-Time**: Database table `dev_daily_prices_tiingo` (partial day data, every 30min)
- **Purpose**: Fast SQL queries for live trading systems, alerts, real-time analytics
- **Update Frequency**: Every 30 minutes during market hours (9:30 AM - 4:00 PM EST)

#### Historical Parquet Storage (Analysis & Research):
- **📊 Polygon Complete**: `/mnt/d/ats-data/minute-bars/polygon/` (complete daily minute bars)
- **📊 Tiingo Complete**: `/mnt/d/ats-data/minute-bars/tiingo/` (complete daily minute bars)
- **📊 FirstRate Direct**: `/mnt/d/ats-data/minute-bars/firstrate/` (52,796 parquet files, direct download)
- **Purpose**: ML training, backtesting, historical analysis, research
- **Update Frequency**: Daily after 7:00 PM EST (complete settlement data)

**⚠️ DESIGN RATIONALE:**
- **Database**: Optimized for real-time queries during trading hours
- **Parquet**: Optimized for large-scale historical analysis and ML training
- **Two streams prevent**: Trading system slowdowns from large historical queries

---

## 🔧 Service Startup Dependencies

### Service Startup Dependencies:
- **Database first**: Start PostgreSQL before dependent services
- **Network connectivity**: Ensure containers can resolve each other's hostnames
- **Health checks**: Wait for database readiness before application startup
- **Missing files**: Create required startup scripts when containers expect them

### CRITICAL: Database Connection Compatibility Fix

**Issue:** Scripts running inside Docker containers were failing with Docker networking errors: "container not attached to default bridge network".

**Root Cause:** Docker containers were using deprecated `--link` instead of proper custom network connectivity:
- ❌ **Wrong**: `--link ats-dev-postgres:postgres` (deprecated, requires same network)
- ✅ **Correct**: `--network ats-network` (modern Docker networking)

**Database Connection Pattern for Container Scripts:**
- ❌ **Wrong**: `host='localhost', port=5432` (tries to connect outside container)
- ✅ **Correct**: `host='ats-dev-postgres', port=5432` (connects to container via name)

**Solution Applied:**
1. **Fixed `run_dev.py`**: Updated to use `--network ats-network` instead of deprecated `--link`
2. **Container Scripts**: Use `host='ats-dev-postgres'` for database connections
3. **Result**: Docker job containers now properly connect to PostgreSQL on same network

```python
# Correct Docker-compatible database connection for scripts
conn = await asyncpg.connect(
    host='ats-dev-postgres',  # PostgreSQL container name on ats-network
    port=5432,                # Internal Docker port
    user='postgres',
    password='dev_password',
    database='dev_db'
)
```

---

## 📊 Market Data Collection Architecture

### CRITICAL: Market Data Collection Architecture

**TWO-STREAM DATA COLLECTION STRATEGY:**

#### Real-Time Intraday Collection (Database Storage)
**Polygon & Tiingo:** Every 30 minutes during market hours
- **Purpose**: Real-time trading signals, live analytics
- **Storage**: Database tables (`dev_daily_prices_polygon`, `dev_daily_prices_tiingo`)
- **Schedule**: 9:30 AM - 4:00 PM EST, every 30 minutes
- **Data**: Current day's minute bars (partial day data)
- **Use Case**: Live trading systems, real-time alerts

#### End-of-Day Complete Collection (Parquet Files)
**Polygon & Tiingo:** After 7:00 PM daily
- **Purpose**: Complete historical analysis, backtesting
- **Storage**: Monthly parquet files (`/mnt/d/ats-data/minute-bars/polygon/`, `/mnt/d/ats-data/minute-bars/tiingo/`)
- **Schedule**: 7:30 PM EST daily (after markets close + settlement)
- **Data**: Complete daily minute bars (full day data)
- **Use Case**: ML training, historical analysis, research

#### FirstRate Collection (Direct Parquet)
**FirstRate:** Single daily download at 2:30 AM
- **Purpose**: Premium minute bar data for analysis
- **Storage**: Direct to parquet files (`/mnt/d/ats-data/minute-bars/firstrate/`)
- **Schedule**: 2:30 AM EST daily (data available after midnight)
- **Data**: Previous trading day's complete minute bars
- **Use Case**: High-quality backtesting, research, ML training

**⚠️ CRITICAL DESIGN RATIONALE:**
- **Database**: Fast queries for real-time trading (partial day)
- **Parquet**: Optimized storage for historical analysis (complete day)
- **Two streams ensure**: Live trading AND historical research capabilities

---

## 🚀 Daily 1-Minute Bar Backfill System

### System Overview

The Daily 1-Minute Bar Backfill System provides comprehensive intraday market data processing for all stocks and critical ETFs with automated scheduling, monitoring, and notifications.

**Key Features:**
- **18,331+ Instrument Coverage**: All US exchange stocks and critical ETFs
- **7-Day Rolling Backfill**: Processes last 7 trading days with overwrite capability
- **Organized File Storage**: `/mnt/d/ats-data/firstrate-data/daily/yyyy/mm/dd/<first_letter>/<symbol>_YYYYMMDD.parquet`
- **Prometheus Metrics**: Real-time tracking of symbols per instrument type and minute bars per day
- **Slack Notifications**: Daily and weekly processing summary reports
- **Container Orchestration**: Three-service Docker architecture with health monitoring

### Service Management

**Start Complete System:**
```bash
# Start all three services (scheduler, metrics, notifications)
docker-compose -f docker-compose.minute-bars-jobs.yml up -d

# Verify services are running
docker ps | grep "ats-intg.*minute"
# Expected: ats-intg-minute-bars-scheduler, ats-intg-prometheus-metrics, ats-intg-slack-notifier
```

### Monitoring & Metrics

**Prometheus Metrics (http://localhost:4080/metrics):**
```bash
# Key metrics to monitor
curl -s http://localhost:4080/metrics | grep -E "(ats_daily_minute_backfill|minute_bars)"

# Specific metric examples:
# ats_daily_minute_backfill_instruments_processed_total{instrument_type="stock"} 15234
# ats_daily_minute_backfill_total_minute_bars 45678901
# ats_daily_minute_backfill_symbols_by_type{instrument_type="critical_etf"} 25
# ats_daily_minute_backfill_processing_duration_seconds 1247.5
```

---

## 📊 Vendor Monitoring & Dashboards

### Comprehensive vendor API and data collection monitoring via Grafana:

**🎯 Primary Dashboard**
```bash
# Professional Grafana dashboards (recommended)
http://localhost:4002/d/cb0f07fd-9f56-486e-8cd6-7c9893e63116/ats-vendor-monitoring-dashboard-postgresql  # Main vendor dashboard
http://localhost:4002                                                                                        # Grafana home (admin/admin)
```

### Monitoring Capabilities
- **Minute Bar Collection per Vendor**: Real-time collection rates by vendor/symbol
- **API Calls per Vendor with Status Codes**: 200, 429, 500 response breakdown
- **Vendor Health Monitoring**: Success rates, response times, rate limits
- **Error Tracking**: Recent API failures with detailed error messages
- **Data Quality Metrics**: Collection success rates and data quality scores

### Backend Services
```bash
# Prometheus metrics (feeds Grafana)
http://localhost:8091/metrics                    # Vendor performance metrics

# Database tables for direct queries
intg_api_calls                                   # API call tracking
intg_minute_bar_collection_metrics               # Collection performance
intg_vendor_api_health                          # Periodic health summaries
```

---

## 🏗️ Directory Structure

### Consolidated Directory Structure

**Aggressive consolidation completed - 50% file reduction, 70% duplicate code eliminated:**

#### Analytics Services (Unified)
- **`src/analytics/unified_analytics_service.py`** - Single consolidated analytics service
- **Combines**: Type-aware analysis, universe analytics, Ray computing, EDA capabilities
- **Replaces**: 5 separate analytics services (7,270+ lines → 1 unified service)

#### ML/Training Data (Organized)
- **`src/ml/training_data/generators/`** - Core training data generators
- **`src/ml/training_data/legacy_scripts/`** - Reference legacy training scripts
- **Organized**: Proper ML pipeline structure with clear separation

#### Scripts Organization (Clean Structure)
- **`scripts/deployment/`** - Deployment automation scripts
- **`scripts/infrastructure/`** - System setup and infrastructure scripts
- **`scripts/validation/`** - Testing and validation scripts
- **`scripts/monitoring/`** - Monitoring utilities and health checks

#### Data Ingestion (Consolidated)
- **`src/data_ingestion/legacy_backfill_scripts/`** - Vendor backfill scripts (reference)
- **Consolidated**: 10 vendor scripts → organized legacy reference system

#### Tests (Properly Organized)
- **`tests/browser_tests/`** - UI/browser testing scripts moved from scripts/
- **`tests/integration/gin_refactoring/`** - Gin refactoring tests organized
- **Clean separation**: Test files in proper test directories, not scattered in scripts/

---

## 🚨 Critical Anti-Patterns

- ❌ **DO NOT** use `docker run` for ATS-INTG services (use Docker Compose)
- ❌ **DO NOT** use Docker Compose for ATS-DEV services (use `run_dev.py`)
- ❌ **DO NOT** mix `run_dev.py` commands with `docker-compose` commands
- ❌ **DO NOT** start containers without ensuring network connectivity
- ❌ **DO NOT** use localhost:port in container DB_HOST configs (use container-name:5432)
- ❌ **DO NOT** assume containers can communicate across different networks

---

## 🎯 Infrastructure Success Criteria

**Service Deployment Checklist:**
- [ ] Database containers started first with correct ports (3432 for DEV, 4432 for INTG)
- [ ] Containers connected to appropriate networks (ats-network, ats-intg-network)
- [ ] DB_HOST configured with container names (ats-dev-postgres, ats-intg-postgres)
- [ ] Required startup scripts created when containers expect them
- [ ] Network connectivity verified between dependent containers
- [ ] Health checks confirm database readiness before application startup

**You're using infrastructure correctly when:**
- [ ] Database connections use correct container names and internal ports
- [ ] Docker containers can communicate via network names
- [ ] Persistent volumes maintain data across container restarts
- [ ] Service health endpoints respond correctly
- [ ] Monitoring metrics are being collected and displayed
- [ ] Automated backups are running and completing successfully

---

**📋 For operational procedures and daily maintenance, see OPERATIONS.md**
**📋 For development workflows and testing procedures, see DEVELOPMENT.md**

*This infrastructure guide covers the technical foundation for reliable, scalable ATS platform operations.*