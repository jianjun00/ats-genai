# CLAUDE.md - ATS Platform Guide

This file provides focused guidance to Claude Code when working with the ATS fintech platform.

## 🚨 CRITICAL: Be concise about code

**ALWAYS read docs and code about current infra to find best way to reuse existing code:**

**ALWAYS have a document on a new script as to why it is needed and what it does:**

**ALWAYS find opportunities to refactor code to remove duplicate functionality and delete unused code:**

**DO NOT CREATE SCRIPTS to run something. YOU SHOULD JUST RUN THE EXISTING CODE:**

## 🏗️ **CONSOLIDATED DIRECTORY STRUCTURE (Updated 2025-09-02)**

**Aggressive consolidation completed - 50% file reduction, 70% duplicate code eliminated:**

### **Analytics Services (Unified)**
- **`src/analytics/unified_analytics_service.py`** - Single consolidated analytics service
- **Combines**: Type-aware analysis, universe analytics, Ray computing, EDA capabilities
- **Replaces**: 5 separate analytics services (7,270+ lines → 1 unified service)

### **ML/Training Data (Organized)**
- **`src/ml/training_data/generators/`** - Core training data generators
- **`src/ml/training_data/legacy_scripts/`** - Reference legacy training scripts
- **Organized**: Proper ML pipeline structure with clear separation

### **Scripts Organization (Clean Structure)**
- **`scripts/deployment/`** - Deployment automation scripts
- **`scripts/infrastructure/`** - System setup and infrastructure scripts  
- **`scripts/validation/`** - Testing and validation scripts
- **`scripts/monitoring/`** - Monitoring utilities and health checks

### **Data Ingestion (Consolidated)**
- **`src/data_ingestion/legacy_backfill_scripts/`** - Vendor backfill scripts (reference)
- **Consolidated**: 10 vendor scripts → organized legacy reference system

### **Tests (Properly Organized)**
- **`tests/browser_tests/`** - UI/browser testing scripts moved from scripts/
- **`tests/integration/gin_refactoring/`** - Gin refactoring tests organized
- **Clean separation**: Test files in proper test directories, not scattered in scripts/

## 🚨 CRITICAL: Docker + GPU Development

**Never use mock or fake data other than unit test:**

**ALWAYS USE run_test for unit test:**

**ALWAYS USE run_dev for dev environment:**

**Always research existing code or database tables or apps before writing new code:**

**Always refactor code to remove duplicate functionality:**

**Always use same external port for application deployment:**

**ALWAYS create unit test for new code and think hard about test coverage and then run manual test in dev before claiming task is completed:**

**ALWAYS USE DOCKER FOR DEV OPERATIONS:**

- ✅ **DEV Environment = Docker containers on localhost**
- ✅ **GPU Support = Docker with --gpus all flag**  
- ✅ **Database = PostgreSQL container or localhost**
- ✅ **All operations = Use run_dev (handles Docker automatically)**
- ❌ **NEVER run complex setup manually**
- ❌ **NEVER manually manage container lifecycle**



### Primary Interface: run_dev

```bash
# Your primary interface - use for ALL operations
python scripts/run_dev.py setup                    # Setup dev environment
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py
python scripts/run_dev.py run --script scripts/training/train_model.py --gpu  # With GPU
python scripts/run_dev.py start --service postgres # Start database
python scripts/run_dev.py start --service analytics # Start analytics service
python scripts/run_dev.py status                   # Check running services
python scripts/run_dev.py test                     # Run tests

# ❌ NEVER run docker commands manually for dev work
# ✅ ALWAYS use python scripts/run_dev.py
```

## 📚 Consolidated Documentation Structure

**IMPORTANT: Documentation has been consolidated to eliminate 90% duplication. Always use the UNIFIED guides.**

### 🚀 **PRIMARY DOCUMENTATION** ⭐ **USE THESE FIRST**

#### **Core 3-File Structure (MANDATORY READING)**
- **[START_HERE.md](docs/START_HERE.md)** ⭐ **START HERE**
  - 15-minute setup, core concepts, role-specific quick actions, troubleshooting
- **[DEVELOPMENT.md](docs/DEVELOPMENT.md)** ⭐
  - Complete development workflow, TDD, schema validation, CI/CD, GitOps, testing
- **[DEPLOYMENT.md](docs/DEPLOYMENT.md)** ⭐
  - All deployment strategies, environments, monitoring, troubleshooting, rollback

#### **Component-Specific Documentation**
- **[Backend Platform](docs/backend-platform/)** - APIs, services, business logic
- **[Data Infrastructure](docs/data-infrastructure/)** - Data pipelines, storage, ETL
- **[ML Platform](docs/ml-platform/)** - Training, models, AI optimization
- **[Online Infrastructure](docs/online-infrastructure/)** - K8s, CI/CD, monitoring

### 📖 **COMPLETE NAVIGATION HUB**
- **[Documentation Hub](docs/README.md)** - Complete navigation with learning paths

### 🚀 **QUICK START PATHS**

#### **New Team Members**
1. **[START_HERE.md](docs/START_HERE.md)** - 15-minute setup and core concepts
2. **[Documentation Hub](docs/README.md)** - Complete navigation for deeper learning

#### **DevOps Engineers**  
1. **[DEPLOYMENT.md](docs/DEPLOYMENT.md)** - All deployment strategies and troubleshooting
2. **[DEVELOPMENT.md](docs/DEVELOPMENT.md)** - CI/CD and GitOps workflows

### 🔧 **OPERATIONAL SCRIPTS** (Ready to Use)

```bash
# Complete workflow scripts available:
./scripts/pre_deploy_check.sh           # Safety checks before deployment  
./scripts/dev_deploy.sh                 # Deploy with team coordination
./scripts/monitor_deployment.sh         # Real-time deployment monitoring
./scripts/rollback_deployment.sh        # Multiple rollback strategies
./scripts/deployment_status.sh          # Comprehensive system status
./scripts/force_argocd_sync.sh          # ArgoCD integration
./scripts/get_external_access.sh        # Service endpoint discovery
```

### 📋 **LEGACY DOCUMENTATION**
- **[Archive Directory](docs/archive/)** - Archived duplicate docs (reference only)
- **Note**: If you find conflicting information, **always follow the UNIFIED guides** marked with ⭐

## 🔥 Critical Development Rules

### Test-Driven Development (MANDATORY)
```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should FAIL (proves test works)

# 2. Implement minimal code to pass test
# (write your code in src/)

# 3. Verify test passes  
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should PASS

# 4. Run all tests
python scripts/run_dev.py test

# 5. Integration testing with services
python scripts/run_dev.py setup  # Start required services
python scripts/run_dev.py test --test tests/integration/
```

### End-to-End Validation Required
**Every feature must be complete end-to-end:**
1. Generate real data using Docker containers
2. Store data in database with correct schema
3. API serves data via localhost services
4. Frontend displays data in browser
5. All integration tests pass

### Infrastructure Best Practices
- **Reuse existing patterns** - Check `python scripts/run_dev.py status` first
- **Use official Docker image** - Always use `dragonflyer762/ats-genai:latest` from Docker Hub
- **Don't install packages manually** - Dependencies are pre-installed in Docker image
- **GPU Support** - Use `--gpu` flag for ML/AI workloads
- **Service management** - Use run_dev to start/stop services automatically
- **Database connections** - Automatically detects and connects to available database
- **Environment is pre-configured** - Don't set variables manually

## 📋 Common Commands

### Development Setup
```bash
# Setup complete development environment
python scripts/run_dev.py setup

# Testing
python scripts/run_dev.py test
python scripts/run_dev.py test --test tests/integration/
python scripts/run_dev.py test --test tests/unit/

# Database operations
python scripts/run_dev.py query --query "SELECT version()"
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
```

### Service Management
```bash
# Start services
python scripts/run_dev.py start --service postgres
python scripts/run_dev.py start --service analytics
python scripts/run_dev.py start --service api

# Check running services
python scripts/run_dev.py status

# Stop services
python scripts/run_dev.py stop --service analytics
```

### Comprehensive Instrument Population
```bash
# Populate ALL supported stocks from vendor APIs (no hardcoding)
python scripts/run_dev.py run --script scripts/run_tiingo_bulk.py    # 60,998 stocks via TiingoClient.list_stock_tickers()
python scripts/run_dev.py run --script scripts/run_eodhd_bulk.py     # 50,746 stocks via exchange-symbol-list/US API

# Individual vendor population (for testing)
python scripts/run_dev.py run --script scripts/run_polygon_instruments.py
python scripts/run_dev.py run --script scripts/run_tiingo_instruments.py  
python scripts/run_dev.py run --script scripts/run_eodhd_instruments.py

# Verify comprehensive population
python scripts/run_dev.py query --query "SELECT 'Tiingo' as vendor, COUNT(*) as instruments FROM dev_instrument_tiingo UNION SELECT 'EODHD', COUNT(*) FROM dev_instrument_eodhd"

# Check delisted/historical stocks
python scripts/run_dev.py query --query "SELECT COUNT(*) as delisted_before_2020 FROM dev_instrument_tiingo WHERE end_date < '2020-01-01'"
```

### Script Execution
```bash
# Run data generation scripts
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py

# Run ML training with GPU
python scripts/run_dev.py run --script scripts/training/train_model.py --gpu

# Check service logs
python scripts/run_dev.py logs --service analytics
```

## 🚨 Critical Anti-Patterns to Avoid

**Infrastructure:**
- ❌ Running docker commands directly for dev operations
- ❌ Setting environment variables manually  
- ❌ Creating new container patterns when existing ones work
- ❌ Installing packages manually in containers
- ❌ Running services without using run_dev
- ❌ Managing container lifecycle manually

**Development:**
- ❌ Claiming functionality works without tests
- ❌ Writing tests after code (TDD requires tests first)
- ❌ Skipping integration tests (they're mandatory)
- ❌ Not testing actual service startup with run_dev
- ❌ Half-baked implementations (incomplete end-to-end)

## 🎯 Success Criteria

**You're following best practices when:**
- [ ] Using run_dev for all development operations
- [ ] Writing failing tests before code changes
- [ ] Running tests with run_dev test command
- [ ] Running integration tests and seeing them pass
- [ ] Testing services with localhost access
- [ ] Completing full end-to-end validation
- [ ] Reusing existing Docker/service patterns
- [ ] Using GPU support when needed for ML workloads

## 🆘 Getting Help

- **Quick Issues**: [Debugging Guide](docs/development/DEBUGGING_GUIDE.md)
- **Role-Specific**: [Role Guides](docs/roles/)
- **Architecture Questions**: [System Architecture](docs/architecture/SYSTEM_ARCHITECTURE.md)
- **New Team Member**: [Quick Start](docs/onboarding/QUICK_START.md)
- **System Monitoring**: [Monitoring Setup](docs/MONITORING_SETUP.md) - WSL monitoring with Slack alerts
- **API Keys & Authentication**: [Operations Guide - Security](docs/OPERATIONS.md#api-keys--authentication) - Complete vendor API keys reference

---

## Database Connection Info (Reference)

**ATS-DEV PostgreSQL:**
- Host: `localhost`, Port: `3432`
- User: `postgres`, Password: `dev_password`, Database: `dev_db`
- Container: `ats-dev-postgres`
- Started with: `python scripts/run_dev.py start --service postgres`

**ATS-INTG PostgreSQL:**
- Host: `localhost`, Port: `4432`  
- User: `postgres`, Password: `intg_password`, Database: `intg_db`
- Container: `ats-intg-postgres`
- Started with: `python scripts/run_intg.py start --service postgres`

**Auto-detection:** run_dev and run_intg automatically detect available connections

**🚨 CRITICAL: Docker Container Database Connection Fix (2025-08-30)**

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

**Files Fixed:**
- `scripts/tiingo_30_year_daily_backfill.py` - Updated database connection
- `scripts/eodhd_30_year_daily_backfill.py` - Created with correct parameters  
- `scripts/tiingo_30year_daily_simple.py` - Working test version
- `scripts/eodhd_30year_daily_simple.py` - Working test version

**Verification:** All three vendor scripts (Polygon, Tiingo, EODHD) now successfully connect and are processing 18,296 instruments over 30 years.

## 🚀 **CRITICAL: Multi-Vendor 30-Year Daily Price Backfill Infrastructure (2025-08-28)**

**Complete 3-Vendor Coverage:**
```bash
# All three vendors running in parallel for comprehensive coverage
nohup python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py --env '{"TIINGO_API_KEY": "xxx"}' > /tmp/tiingo_30year_backfill.log 2>&1 &
nohup python3 scripts/run_dev.py run --script scripts/eodhd_30_year_daily_backfill.py --env '{"EODHD_API_KEY": "xxx"}' > /tmp/eodhd_30year_backfill.log 2>&1 &  
nohup python3 scripts/run_dev.py run --script scripts/polygon_30_year_daily_backfill.py --env '{"POLYGON_API_KEY": "xxx"}' > /tmp/polygon_30year_daily_backfill.log 2>&1 &
```

**Database Tables:**
- `dev_daily_prices_tiingo` - Tiingo 30-year daily prices (1995-2025)
- `dev_daily_prices_eodhd` - EODHD 30-year daily prices (1995-2025)  
- `dev_daily_prices_polygon` - Polygon daily prices (combined 30-year + minute-derived data)

**Scale and Coverage:**
- **Instruments**: 18,296 active US exchange symbols
- **Time Span**: 30 years (1995-2025)
- **Expected Records**: ~40+ million daily price records total
- **Rate Limits**: Tiingo (1000/hr), EODHD (20/min), Polygon (5/min)
- **Completion**: 15-20 hours depending on vendor

**Idempotent Operations:**
All scripts use UPSERT operations with `ON CONFLICT (date, instrument_id)` for safe resuming and re-running without duplicates.

**Monitoring:**
```bash
# Check progress
tail -f /tmp/tiingo_30year_backfill.log
tail -f /tmp/eodhd_30year_backfill.log  
tail -f /tmp/polygon_30year_daily_backfill.log

# Check record counts
python3 scripts/run_dev.py query --query "SELECT 'Tiingo' as vendor, COUNT(*) as records FROM dev_daily_prices_tiingo UNION SELECT 'EODHD', COUNT(*) FROM dev_daily_prices_eodhd UNION SELECT 'Polygon', COUNT(*) FROM dev_daily_prices_polygon"
```

**Files Created:**
- `scripts/tiingo_30_year_daily_backfill.py` - Comprehensive Tiingo backfiller
- `scripts/eodhd_30_year_daily_backfill.py` - Comprehensive EODHD backfiller  
- `scripts/polygon_30_year_daily_backfill.py` - Comprehensive Polygon backfiller
- `scripts/tiingo_30year_daily_simple.py` - Test version
- `scripts/eodhd_30year_daily_simple.py` - Test version

This infrastructure provides comprehensive multi-vendor price data validation, redundancy, and comparison capabilities across 30 years of market history.

---

## 🚀 **CRITICAL: ATS Complete Startup Process (2025-08-31)**

**Single Command for Complete Environment Setup:**
```bash
# Start both ATS-DEV and ATS-INTG environments
./scripts/ats_startup.sh
```

### **✅ What the Startup Script Does**

**1. Clean Environment Setup:**
- Stops all existing ATS services cleanly
- Removes old containers to ensure fresh start
- Creates proper Docker network connections

**2. Database Initialization:**
- **ATS-DEV PostgreSQL**: Starts on `localhost:3432` using `postgres-data-new` volume
- **ATS-INTG PostgreSQL**: Starts on `localhost:4432` using `postgres-intg-data` volume
- Waits for both databases to be healthy before proceeding
- **NO automatic backup restoration** (manual initialization required)

**3. Service Health Validation:**
- Checks database table counts (DEV: 62 tables, INTG: 37 tables expected)
- Starts all ATS-DEV services (analytics, monitoring, data collection)
- Starts all ATS-INTG services (analytics, job scheduler, monitoring)
- Validates service health endpoints

**4. Complete Service URLs:**
```bash
# ATS-DEV Environment (Development)
- Analytics Service: http://localhost:3000
- EDA Dashboard: http://localhost:3000/eda  
- Health Check: http://localhost:3000/health
- Database: postgresql://postgres:dev_password@localhost:3432/dev_db

# ATS-INTG Environment (Integration Testing)
- Analytics Service: http://localhost:4000
- EDA Dashboard: http://localhost:4000/eda
- Health Check: http://localhost:4000/health  
- Database: postgresql://postgres:intg_password@localhost:4432/intg_db
- Prometheus Metrics: http://localhost:4080
- Daily Minute Bars: /mnt/d/ats-data/firstrate-data/daily/
```

### **🔧 Environment-Specific Operations**

**ATS-DEV (Development) - Use `run_dev.py`:**
```bash
# Individual service management
python3 scripts/run_dev.py start --service postgres    # Start PostgreSQL only
python3 scripts/run_dev.py start --service analytics   # Start analytics service
python3 scripts/run_dev.py status                      # Check running services
python3 scripts/run_dev.py query --query "SELECT version()"

# Database operations
PGPASSWORD=dev_password psql -h localhost -p 3432 -U postgres -d dev_db
```

**ATS-INTG (Integration) - Use Docker Compose:**
```bash
# Service management
docker-compose -f docker-compose.ats.yml up -d postgres-intg analytics-intg
docker-compose -f docker-compose.intg-jobs.yml up -d  # Job scheduler services
docker-compose -f docker-compose.minute-bars-jobs.yml up -d  # Daily minute bars system
docker-compose -f docker-compose.ats.yml ps           # Check service status

# Database operations  
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db

# Daily minute bars operations
docker exec ats-intg-minute-bars-scheduler python3 scripts/daily_minute_bars_backfill.py --test
docker logs ats-intg-minute-bars-scheduler  # Check processing logs
curl -f http://localhost:4080/metrics       # View Prometheus metrics
```

### **⚡ Quick Health Check**
```bash
# Verify both environments are operational
curl -f http://localhost:3000/health  # ATS-DEV analytics
curl -f http://localhost:4000/health  # ATS-INTG analytics
curl -f http://localhost:4080/health  # ATS-INTG prometheus metrics
docker ps | grep -E "(ats-dev|intg)"  # Container status

# Daily minute bars system health
curl -s http://localhost:4080/metrics | grep "ats_daily_minute_backfill"  # Minute bars metrics
ls -la /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/  # Today's files
tail -50 /mnt/d/ats-logs/minute-bars-backfill.log  # Recent processing activity
```

### **🛡️ Critical Startup Requirements**

**Volume Configuration (MANDATORY):**
- **ATS-DEV**: MUST use `postgres-data-new` volume for data persistence
- **ATS-INTG**: MUST use `postgres-intg-data` volume for data persistence  
- **Port Mapping**: DEV=3432, INTG=4432 (no conflicts)

**Database Initialization:**
- **No automatic backup restoration** - prevents accidental data overwrites
- Manual schema migration and data population required
- Empty databases are acceptable - populate via proper migration scripts

**Docker Networking (FIXED 2025-08-30):**
- All containers use `ats-network` for proper inter-service communication
- Database connections use container names (`ats-dev-postgres`, `ats-intg-postgres`)
- **No more `--link` deprecation warnings** - modern Docker networking implemented

### **🚨 Known Issues & Solutions**

**Docker Credential Warnings (Non-Fatal):**
```
docker-credential-desktop.exe not installed or not available in PATH
```
- **Impact**: None - services start successfully despite warnings
- **Cause**: WSL2 Docker Desktop credential helper configuration  
- **Solution**: Warnings can be ignored - all functionality works properly

**Service Health Status:**
- Analytics services may show `(unhealthy)` initially during startup
- Health endpoints become available within 30-60 seconds
- **Always verify with** `curl http://localhost:3000/health` for actual status

### **📊 Expected Startup Results**
```bash
# Successful startup shows:
✅ ATS-DEV PostgreSQL is ready (localhost:3432, 62 tables)
✅ ATS-INTG PostgreSQL is ready (localhost:4432, 37 tables)  
✅ ATS-DEV analytics service is ready (localhost:3000)
✅ ATS-INTG analytics service is ready (localhost:4000)

# Container status should show:
ats-dev-postgres    Up (healthy)    0.0.0.0:3432->5432/tcp
ats-intg-postgres   Up (healthy)    0.0.0.0:4432->5432/tcp
ats-dev-analytics   Up              0.0.0.0:3000->3000/tcp  
ats-intg-analytics  Up              0.0.0.0:4000->3000/tcp
```

**⚠️ CRITICAL: This startup process provides clean, reproducible environment initialization without dangerous automatic backup restoration. Manual data population is required but safer.**

---

## 💾 **ATS Persistent Storage (D: Drive)**

**Docker Volume Configuration:**
- **📁 Data**: `/mnt/d/ats-data` → `/data` (in containers)
- **📁 Backup**: `/mnt/d/ats-backup` → `/backup` (in containers)  
- **📁 Logs**: `/mnt/d/ats-logs` → `/logs` (in containers)

**PostgreSQL Database Storage:**
- **🗄️ ATS-DEV**: Docker volume `postgres-data-new` 
- **🗄️ ATS-INTG**: Docker volume `postgres-intg-data`
- **📍 Location**: Managed by Docker in `/var/snap/docker/common/var-lib-docker/volumes/`

**🚨 CRITICAL: Two-Stream Data Storage Architecture:**

#### **Real-Time Database Storage (Intraday Trading):**
- **📊 Polygon Real-Time**: Database table `dev_daily_prices_polygon` (partial day data, every 30min)
- **📊 Tiingo Real-Time**: Database table `dev_daily_prices_tiingo` (partial day data, every 30min)
- **Purpose**: Fast SQL queries for live trading systems, alerts, real-time analytics
- **Update Frequency**: Every 30 minutes during market hours (9:30 AM - 4:00 PM EST)

#### **Historical Parquet Storage (Analysis & Research):**
- **📊 Polygon Complete**: `/mnt/d/ats-data/minute-bars/polygon/` (complete daily minute bars)
- **📊 Tiingo Complete**: `/mnt/d/ats-data/minute-bars/tiingo/` (complete daily minute bars)
- **📊 FirstRate Direct**: `/mnt/d/ats-data/minute-bars/firstrate/` (52,796 parquet files, direct download)
- **Purpose**: ML training, backtesting, historical analysis, research
- **Update Frequency**: Daily after 7:00 PM EST (complete settlement data)

**⚠️ DESIGN RATIONALE:**
- **Database**: Optimized for real-time queries during trading hours
- **Parquet**: Optimized for large-scale historical analysis and ML training
- **Two streams prevent**: Trading system slowdowns from large historical queries

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

## 🚨 **CRITICAL: SCHEMA VALIDATION PREVENTS DEV ENVIRONMENT ERRORS**

**SCHEMA ERRORS MUST BE CAUGHT BY UNIT TESTS - NEVER IN DEV ENVIRONMENT**

### Required Before Any Database Code

**EVERY database interaction must be validated before deployment:**

```bash
# 1. Validate schema compatibility before committing
python scripts/validate_schema.py --check-all

# 2. Run schema validation unit tests
PYTHONPATH=src pytest tests/unit/test_database_schema_validation.py -v

# 3. Check for anti-patterns
pre-commit run schema-anti-patterns
```

**Schema validation will catch:**
- ❌ Wrong table names (`dev_training_datasets` vs `dev_training_dataset`)
- ❌ Wrong column names (`created_at` vs `creation_timestamp`)  
- ❌ Missing tables or columns
- ❌ SQL syntax errors
- ❌ Type mismatches

**Example validation results:**
```
❌ 11 ERRORS FOUND:
  ❌ enhanced_dataset_visualization_platform_real_data.py:189 - Anti-pattern detected: created_at. Should be "creation_timestamp"
  ❌ enhanced_dataset_visualization_platform_real_data.py:190 - Anti-pattern detected: dev_training_datasets. Should be "dev_training_dataset" (singular)
  ❌ enhanced_dataset_visualization_platform_real_data.py:187 - Table 'dev_training_datasets' does not exist
  ❌ enhanced_dataset_visualization_platform_real_data.py:187 - SQL syntax error: relation "dev_training_datasets" does not exist
```

**CI/CD Integration:**
- Schema validation runs automatically in GitHub Actions
- Deployment blocked if schema validation fails
- Pre-commit hooks prevent bad code from being committed

## 🚨 **CRITICAL: NO DEMO DATA IN DEVELOPMENT ENVIRONMENTS**

**DEMO DATA HIDES REAL ISSUES AND CREATES FALSE CONFIDENCE**

- ❌ **NEVER use demo/mock data** in development, staging, or production environments
- ❌ **NEVER create fallbacks to demo data** when real data is unavailable
- ❌ **NEVER return 200 OK with fake data** when database queries fail
- ✅ **Demo data ONLY in unit tests** - isolated, controlled test scenarios
- ✅ **Fail fast and clearly** when real data/database is unavailable
- ✅ **Show actual errors** - connection failures, missing data, schema problems

**Why Demo Data Is Dangerous:**
- Hides database connection problems and query failures
- Masks data quality issues, missing values, and real-world edge cases
- Creates false performance metrics (demo data is always fast and perfect)
- Prevents detection of authentication, permission, and network issues
- Results in production surprises when real data behaves differently

**Correct Error Handling:**
```python
# ✅ CORRECT: Fail with real error
async def get_dataset(dataset_id: str):
    dataset = await db.fetch_dataset(dataset_id)
    if not dataset:
        raise HTTPException(404, f"Dataset '{dataset_id}' not found")
    return dataset

# ❌ WRONG: Demo fallback hides the real problem  
async def get_dataset(dataset_id: str):
    try:
        return await db.fetch_dataset(dataset_id)
    except:
        return generate_demo_dataset()  # HIDES THE ISSUE!
```

**Environment Rules:**
- **Unit Tests**: Demo data acceptable for isolated testing
- **Development**: Real database required - fail if unavailable
- **Staging/Production**: Real data only - no fallbacks ever

**See [docs/DEVELOPMENT_WORKFLOW.md](docs/DEVELOPMENT_WORKFLOW.md) for complete guidelines.**

---

## 📚 Critical Implementation Lessons

### 🚨 **MAJOR DATA QUALITY INCIDENT - RESOLVED (2025-08-27)**

**❌ TIINGO END DATE MISINTERPRETATION ISSUE:**
- **Impact**: 9,834 active stocks (75%) incorrectly marked as delisted
- **Root Cause**: Tiingo `endDate` field misinterpreted as delisting date instead of data availability date
- **Critical Finding**: Recent `endDate` (within 7 days) indicates active data feed, NOT stock delisting
- **Fix**: `scripts/fix_tiingo_population.py` - Set `end_date = NULL` for instruments with recent endDate
- **Result**: Tiingo active rate restored from 25% to 72.1% (12,118 of 16,811 instruments)
- **Prevention**: Comprehensive regression test suite in `tests/regression/`

**🔒 HARDCODED API KEYS SECURITY VULNERABILITY:**
- **Impact**: API keys exposed in 18+ files across codebase (version control, logs, documentation)
- **Root Cause**: Direct hardcoding of sensitive credentials instead of environment variable references
- **Critical Keys Found**: Polygon (`wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD`), Tiingo, EODHD API keys
- **Fix**: Systematic replacement with `os.getenv()` patterns and placeholder values
- **Prevention**: Security regression tests scan entire codebase for credential exposure

**🗃️ DATABASE SCHEMA COMPATIBILITY FAILURES:**
- **Impact**: Runtime failures, ~1.2M price records failing to insert during backfills
- **Root Cause**: Scripts expected `adj_close` column, but database table has `adjclose` (no underscore)
- **Critical Pattern**: Column name mismatches between expectations and reality
- **Fix**: Schema alignment and validation in backfill scripts
- **Prevention**: Database schema compatibility tests validate table structures

**📊 EODHD INSTRUMENT POPULATION INCOMPLETE:**
- **Impact**: Only 7,613 of 50,747 available US instruments populated (15% coverage)
- **Root Cause**: Population script designed for individual ticker lookup, not bulk historical population
- **Missing Data**: 43,134 instruments missing, including decades of delisted/historical stocks
- **Critical Gap**: Historical analysis severely limited without complete instrument universe
- **Fix Required**: Create bulk EODHD population script for comprehensive 30+ year coverage

### 🛡️ **PREVENTION FRAMEWORK DEPLOYED**

**Comprehensive Regression Testing (48+ tests):**
```bash
# Run all critical regression tests before deployment
python3 scripts/run_regression_tests.py --integration

# Run specific issue categories  
python3 scripts/run_regression_tests.py --category security --fast
python3 scripts/run_regression_tests.py --category schema --integration
```

**Test Coverage:**
- **15 Tiingo date interpretation tests** - Prevents active stock misclassification
- **12 hardcoded API keys security tests** - Prevents credential exposure  
- **13 database schema compatibility tests** - Prevents runtime insertion failures
- **8 test suite validation tests** - Ensures prevention framework works

### 📋 **OPERATIONAL LESSONS LEARNED**

**Database Schema Validation (MANDATORY):**
```bash
# ALWAYS validate schema before database operations
docker exec ats-dev-postgres psql -U postgres -d dev_db -c "\d table_name"

# Check actual column names vs script expectations
# Example: Use 'adjclose' not 'adj_close' for Tiingo price data
```

**API Data Interpretation:**
- **Tiingo endDate Logic**: Recent dates (< 7 days) = active data feed, not delisting
- **Polygon Active Field**: Boolean true/false clearly indicates instrument status  
- **EODHD**: No explicit delisting field, assume all instruments active

**Historical Backfill Scale:**
- **Target**: ~15 million price records (Polygon 30-year + Tiingo 5-year)
- **Rate Limits**: Polygon 12-sec delays, Tiingo 1-sec delays
- **Schema Issues**: Always validate table structure before bulk operations

### 🎯 **SUCCESS CRITERIA FOR FUTURE WORK**

**Data Quality:**
- ✅ **>70% active instrument rate** for Tiingo (currently 72.1%)
- ✅ **100% active rate** for Polygon (11,598 instruments)
- ❌ **EODHD critical gap**: Only 15% populated (7,613 of 50,747 available)
- ✅ **Zero hardcoded API keys** in codebase scans
- ✅ **Schema compatibility validated** before database operations

**Prevention:**
- ✅ **All regression tests pass** before deployment
- ✅ **Fast feedback** (<5 minutes for security/schema tests)
- ✅ **Clear actionable failures** with specific fix guidance
- ✅ **Automated testing** in CI/CD pipeline

**Critical Findings from Earlier Implementation (2025-08-25):**
- **Database Connection Issues**: Always set `DB_DISABLE_CONNECT_TIMEOUT=true` for PostgreSQL compatibility
- **API Key Validation**: Test API endpoints directly before troubleshooting infrastructure
- **Gin Configuration**: Use `app_dev.gin` for development (simpler than `app_docker.gin`)
- **Docker Dependencies**: Include ALL required packages in base image, not minimal subsets

**📋 Complete Documentation**: See `tests/regression/README.md` for comprehensive regression testing guide

**📋 Detailed Documentation**: [Instrument Population Lessons Learned](docs/development/INSTRUMENT_POPULATION_LESSONS_LEARNED.md)

### 🚀 **CRITICAL: AAPL Training Data Generation Success (2025-08-31)**

**✅ SUCCESSFUL TRAINING DATA GENERATION FROM 1995:**
- **Output**: `/mnt/d/ats-data/training/{run_id}/` organization pattern established
- **Environment**: ATS-INTG with proper Docker container networking
- **Database**: Complete `intg_training_datasets` table schema with TFDV support
- **Data Shape**: 197 sequences × 60 time steps × 7 features (OHLCV + technical indicators)
- **Integration**: Full database tracking with run records and dataset metadata

**Critical Lessons Applied:**
- ✅ **Use existing code instead of creating scripts** - training data generators are under `src/ml/training_data/`
- ✅ **Use proper environment tools** - `run_intg` for INTG, not `run_dev --environment intg`  
- ✅ **Docker container execution** - Run within `ats-intg-analytics` container for proper networking
- ✅ **Complete database schema** - Apply full migration with all required columns (`feature_metadata`, `tfdv_*`)
- ✅ **Output directory organization** - Files organized by `run_id` in `/mnt/d/ats-data/training/{run_id}/`

**Database Schema Requirements:**
- **Table Name**: `intg_training_datasets` (plural) as expected by DAO
- **Required Columns**: All TFDV columns (tfdv_statistics, tfdv_histogram_path, etc.)
- **Migration**: `src/db/migrations/049_create_training_dataset_table.sql`

**How to Generate Training Data:**

**For ATS-INTG Environment (Production-like):**
```bash
# Generate training data using existing infrastructure
docker exec ats-intg-analytics bash -c "cd /workspace && PYTHONPATH=src python3 src/ml/training_data/runners/training_data_callback_runner.py --symbols AAPL --start-date 2020-01-01 --end-date 2023-01-01 --environment intg"

# Files are automatically saved to container and can be copied to host:
# Output location: /mnt/d/ats-data/training/{run_id}/
# Database tracking: intg_training_datasets table with run metadata
```

**For ATS-DEV Environment (Development):**
```bash
# Use run_dev for development environment training data generation
python3 scripts/run_dev.py run --script src/ml/training_data/runners/training_data_callback_runner.py

# Files saved to: training_data_output/ (can be organized by run_id)
# Database tracking: dev_training_datasets table
```

**Command Pattern (What Works vs What Doesn't):**
```bash
# ✅ CORRECT: Use existing training data runners under ml/training_data/
docker exec ats-intg-analytics bash -c "cd /workspace && PYTHONPATH=src python3 src/ml/training_data/runners/training_data_callback_runner.py --symbols AAPL --start-date 2020-01-01 --end-date 2023-01-01"

# ✅ CORRECT: Use proper environment tools
python3 scripts/run_dev.py run --script src/ml/training_data/runners/training_data_callback_runner.py

# ❌ WRONG: Create new scripts when existing code works
python3 scripts/run_dev.py --environment intg run --script new_training_script.py

# ❌ WRONG: Use wrong environment management
python3 scripts/run_dev.py --environment intg  # Should use run_intg or direct container

# ❌ WRONG: Reference old paths
python3 src/app/training_data_job_runner.py  # This file doesn't exist anymore
```

**✅ TRAINING DATA ARCHITECTURE:**

**All training data functionality is located under `src/ml/training_data/`:**
```
src/ml/training_data/
├── runners/                    # Main entry points
│   └── training_data_callback_runner.py  # Primary training data generator
├── callbacks/                  # Runner framework integration
│   └── training_data_callback.py        # Date-based and interval-based callbacks
├── generators/                 # Data generation engines
│   ├── training_data_generator.py       # Residual return training data
│   └── configurable_train_data_generator.py  # Configurable feature generation
├── dao/                       # Database access layer
│   └── training_dataset_dao.py          # Training dataset database operations
└── storage/                   # Storage management
    └── sequence_storage_manager.py      # Advanced storage formats
```

**Configuration:**
- Gin configuration: `config/training_data.gin` (consolidated single config)
- Technical indicators: configurable via gin (`["etop", "ebot", "pldot", "multi_timeframe"]`)
- Multi-timeframe sequences: 5m, 15m, 1h, 1d intervals
- Output formats: pickle, parquet, riegeli, tfrecord
- Database tracking: Full metadata and validation support

**Files Generated Successfully:**
- `dataset_training_data_gen_AAPL_*_features.npy` (41,708 bytes)
- `dataset_training_data_gen_AAPL_*_labels.npy` (1,228 bytes)
- `dataset_training_data_gen_AAPL_*_metadata.json` (17,109 bytes)
- Database record: Run ID 10, Dataset ID 2 in `intg_training_datasets`

---

## 🚨 **CRITICAL: ATS Platform Database Environments**

### **🔥 Two-Environment Docker Architecture**

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

### **📋 Database Connection Examples**

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

### **🚀 Critical Development Workflow**
1. **Develop** in ATS-DEV environment using `dev_*` tables
2. **Test** with `run_dev.py` for database operations  
3. **Validate** data population scripts work with both environments
4. **Use proper table prefixes** (`dev_` vs `intg_`) in all queries

**⚠️ KEY DIFFERENCES:**
- **Ports**: DEV=3432, INTG=4432  
- **Databases**: dev_db vs intg_db
- **Table Prefixes**: dev_* vs intg_*
- **Container Images**: postgres:13 vs timescale/timescaledb:latest-pg13

---

## ⚙️ **CRITICAL OPERATIONS GUIDE**

### **🚀 Environment Management**

**ATS-DEV Environment:**
```bash
# Complete environment setup
python3 scripts/run_dev.py setup

# Individual service management (uses postgres-data-new volume)
python3 scripts/run_dev.py start --service postgres    # PostgreSQL database
python3 scripts/run_dev.py start --service analytics   # Analytics service
python3 scripts/run_dev.py stop --service analytics
python3 scripts/run_dev.py status

# Database operations
python3 scripts/run_dev.py query --query "SELECT version()"
python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
```

**ATS-INTG Environment:**
```bash
# Start PostgreSQL database first (uses postgres-intg-data volume)
docker-compose -f docker-compose.ats.yml up -d postgres-intg

# Start INTG services
docker-compose -f docker-compose.intg-jobs.yml up -d

# Database operations (direct connection)
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT version()"
```

### **📋 Quick Reference - Database Connections**

| Environment | Host | Port | Database | Username | Password | Connection String |
|-------------|------|------|----------|----------|----------|-------------------|
| **ATS-DEV** | localhost | 3432 | dev_db | postgres | dev_password | `postgresql://postgres:dev_password@localhost:3432/dev_db` |
| **ATS-INTG** | localhost | 4432 | intg_db | postgres | intg_password | `postgresql://postgres:intg_password@localhost:4432/intg_db` |

### **🔑 API Keys & Authentication**

**Market Data Vendor API Keys:**

| Vendor | Environment Variable | Purpose | Rate Limits |
|--------|---------------------|---------|-------------|
| **Polygon** | `POLYGON_API_KEY` | Stock prices, fundamentals, news | 5 calls/min |
| **Tiingo** | `TIINGO_API_KEY` | Daily prices, fundamentals | 1000 calls/hr |
| **FMP** | `FMP_API_KEY` | Fundamentals, earnings | 250 calls/day |
| **Alpha Vantage** | `ALPHA_VANTAGE_API_KEY` | Economic indicators | 25 calls/day |
| **EODHD** | `EODHD_API_KEY` | EOD prices, fundamentals | 20 calls/min |
| **FirstRate** | `FIRSTRATE_USER_ID` | Minute-level OHLCV (direct to parquet) | Premium feed |

### **📊 CRITICAL: Market Data Collection Architecture**

**🚨 TWO-STREAM DATA COLLECTION STRATEGY:**

#### **Real-Time Intraday Collection (Database Storage)**
**Polygon & Tiingo:** Every 30 minutes during market hours
- **Purpose**: Real-time trading signals, live analytics  
- **Storage**: Database tables (`dev_daily_prices_polygon`, `dev_daily_prices_tiingo`)
- **Schedule**: 9:30 AM - 4:00 PM EST, every 30 minutes
- **Data**: Current day's minute bars (partial day data)
- **Use Case**: Live trading systems, real-time alerts

#### **End-of-Day Complete Collection (Parquet Files)**  
**Polygon & Tiingo:** After 7:00 PM daily
- **Purpose**: Complete historical analysis, backtesting
- **Storage**: Monthly parquet files (`/mnt/d/ats-data/minute-bars/polygon/`, `/mnt/d/ats-data/minute-bars/tiingo/`)
- **Schedule**: 7:30 PM EST daily (after markets close + settlement)
- **Data**: Complete daily minute bars (full day data)
- **Use Case**: ML training, historical analysis, research

#### **FirstRate Collection (Direct Parquet)**
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

**API Key Configuration:**
```bash
# Set keys for development scripts
export POLYGON_API_KEY=your_polygon_key
export TIINGO_API_KEY=your_tiingo_key
export FMP_API_KEY=your_fmp_key

# Use with scripts
python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py
```

### **💾 Database Management**

**🚨 CRITICAL: No Automatic Backup Restoration**
- ❌ **NEVER automatically restore from backup** - can cause data loss
- ❌ **NEVER assume backup files are current or valid**
- ✅ **Manual database initialization only** - use proper migration scripts
- ✅ **Fresh database creation** - start with clean schema and populate data as needed

**Proper Database Initialization:**
```bash
# 1. Start fresh PostgreSQL container
python3 scripts/run_dev.py start --service postgres

# 2. Run database migrations to create schema
PYTHONPATH=src python3 -m src.db.create_all_tables

# 3. Populate data using proper scripts (not backups)
python3 scripts/run_dev.py run --script scripts/run_tiingo_bulk.py
python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py
```

### **📊 Monitoring & Health Checks**

**Service Health Checks:**
```bash
# Check all running services
docker ps
python3 scripts/run_dev.py status                    # ATS-DEV status
docker-compose -f docker-compose.intg-jobs.yml ps    # ATS-INTG status

# Service endpoints
curl -f http://localhost:3000/health     # ATS-DEV analytics
curl -f http://localhost:4000/health     # ATS-INTG dashboard
curl -f http://localhost:4002/login      # ATS-INTG Grafana
curl -f http://localhost:4091/-/ready    # ATS-INTG Prometheus

# Database connectivity tests
python3 scripts/run_dev.py query --query "SELECT version()"
PGPASSWORD=intg_password pg_isready -h localhost -p 4432 -U postgres -d intg_db

# View logs
docker logs ats-dev-analytics        # ATS-DEV analytics logs
docker logs ats-dev-postgres         # ATS-DEV database logs  
docker logs postgres-intg            # ATS-INTG database logs
docker logs ats-intg-scheduler       # ATS-INTG job scheduler logs
```

**Performance Monitoring:**
```bash
# System performance overview
docker stats --no-stream | grep -E "(ats-dev|intg)"
free -h
df -h /mnt/d/

# Container uptime and restart counts
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.RestartCount}}"

# Storage usage
ls -lah /mnt/d/ats-data/     # Data directory usage
ls -lah /mnt/d/ats-backup/   # Backup directory usage
ls -lah /mnt/d/ats-logs/     # Log directory usage
```

## 🚀 **CRITICAL: Daily 1-Minute Bar Backfill System (2025-09-01)**

**Complete ATS-INTG 1-Minute Bar Processing Infrastructure**

### **📊 System Overview**

The Daily 1-Minute Bar Backfill System provides comprehensive intraday market data processing for all stocks and critical ETFs with automated scheduling, monitoring, and notifications.

**Key Features:**
- **18,331+ Instrument Coverage**: All US exchange stocks and critical ETFs
- **7-Day Rolling Backfill**: Processes last 7 trading days with overwrite capability
- **Organized File Storage**: `/mnt/d/ats-data/firstrate-data/daily/yyyy/mm/dd/<first_letter>/<symbol>_YYYYMMDD.parquet`
- **Prometheus Metrics**: Real-time tracking of symbols per instrument type and minute bars per day
- **Slack Notifications**: Daily and weekly processing summary reports
- **Container Orchestration**: Three-service Docker architecture with health monitoring

### **🔧 Service Management**

**Start Complete System:**
```bash
# Start all three services (scheduler, metrics, notifications)
docker-compose -f docker-compose.minute-bars-jobs.yml up -d

# Verify services are running
docker ps | grep "ats-intg.*minute"
# Expected: ats-intg-minute-bars-scheduler, ats-intg-prometheus-metrics, ats-intg-slack-notifier
```

**Individual Service Management:**
```bash
# Minute bars scheduler (main processing)
docker logs ats-intg-minute-bars-scheduler --tail 50
docker restart ats-intg-minute-bars-scheduler

# Prometheus metrics server
curl -f http://localhost:4080/health
curl -s http://localhost:4080/metrics | grep "ats_daily_minute_backfill"
docker logs ats-intg-prometheus-metrics --tail 20

# Slack notification service  
docker logs ats-intg-slack-notifier --tail 20
docker exec ats-intg-slack-notifier cat /etc/cron.d/ats-slack-notifications
```

### **⏰ Processing Schedule**

**Automated Cron Jobs:**
- **📅 Daily Backfill**: 4:00 AM EST - Process last 7 days for all instruments
- **🎯 Critical ETFs Priority**: 4:30 AM EST - Priority run for SPY, QQQ, VTI, IWM, etc. (last 3 days)
- **🔄 Weekend Catch-up**: Saturday 6:00 AM EST - Extended 10-day lookback
- **🔍 Health Check**: Every 6 hours - Test run with AAPL, SPY (1 day only)

**Manual Processing:**
```bash
# Test run with specific symbols
docker exec ats-intg-minute-bars-scheduler python3 scripts/daily_minute_bars_backfill.py --test --symbols AAPL,SPY --days 1

# Production run (all instruments, 7 days)
docker exec ats-intg-minute-bars-scheduler python3 scripts/daily_minute_bars_backfill.py --production

# Critical ETFs only (3 days)
docker exec ats-intg-minute-bars-scheduler python3 scripts/daily_minute_bars_backfill.py --instrument-types critical_etf --days 3

# Development run with logging
docker exec ats-intg-minute-bars-scheduler python3 scripts/daily_minute_bars_backfill.py --test --symbols TSLA --days 2 --debug
```

### **📈 Monitoring & Metrics**

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

**File System Monitoring:**
```bash
# Check today's processed files
ls -la /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/

# Count files by first letter (should be organized A-Z)
find /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/ -name "*.parquet" | cut -d'/' -f10 | sort | uniq -c

# Check file sizes and recent updates
find /mnt/d/ats-data/firstrate-data/daily/ -name "*.parquet" -mtime -1 -exec ls -lah {} + | head -20

# Total storage usage
du -sh /mnt/d/ats-data/firstrate-data/daily/
```

**Log File Monitoring:**
```bash
# Main processing logs
tail -f /mnt/d/ats-logs/minute-bars-backfill.log      # Daily backfill activity
tail -f /mnt/d/ats-logs/minute-bars-critical-etf.log  # Critical ETF priority runs
tail -f /mnt/d/ats-logs/minute-bars-weekend.log       # Weekend catch-up runs
tail -f /mnt/d/ats-logs/minute-bars-health.log        # Health check runs

# Service logs
tail -f /mnt/d/ats-logs/prometheus-metrics.log        # Metrics server activity
tail -f /mnt/d/ats-logs/slack-notifications.log       # Notification service logs
tail -f /mnt/d/ats-logs/scheduler-status.log          # Scheduler health status
```

### **💬 Slack Notifications**

**Notification Schedule:**
- **📊 Daily Summary**: 8:00 AM EST - Files processed, symbols covered, storage stats
- **📈 Weekly Report**: Monday 9:00 AM EST - Comprehensive weekly overview with trends

**Manual Slack Notifications:**
```bash
# Send immediate daily summary
docker exec ats-intg-slack-notifier python3 scripts/slack_minute_bars_summary.py --daily

# Send comprehensive weekly summary
docker exec ats-intg-slack-notifier python3 scripts/slack_minute_bars_summary.py --weekly

# Test notification (development)
docker exec ats-intg-slack-notifier python3 scripts/slack_minute_bars_summary.py --test
```

**Slack Message Content:**
- **File Processing Stats**: Total files, unique symbols, instrument type breakdown
- **Storage Metrics**: Directory sizes, file counts, compression ratios  
- **Performance Data**: Processing duration, throughput rates
- **Health Indicators**: Success/failure rates, error summaries
- **Trend Analysis**: Day-over-day comparisons, weekly aggregations

### **🔧 Maintenance & Troubleshooting**

**Health Check Routine:**
```bash
# Complete system health verification
curl -f http://localhost:4080/health                                    # Metrics server
docker ps | grep "ats-intg.*minute" | grep -c "Up"                     # Count running services (expect 3)
ls /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/ &>/dev/null && echo "✅ Today's directory exists" || echo "❌ Today's directory missing"
```

**Common Issues & Solutions:**

**1. FirstRate API Issues:**
```bash
# Check API key configuration
docker exec ats-intg-minute-bars-scheduler env | grep FIRSTRATE_API_KEY

# Test FirstRate connectivity
docker exec ats-intg-minute-bars-scheduler python3 -c "from src.data_providers.firstrate.firstrate_adapter import FirstRateAdapter; print('✅ FirstRate connection OK')"

# Review API error logs
grep -i "firstrate\|api.*error" /mnt/d/ats-logs/minute-bars-backfill.log | tail -10
```

**2. File Organization Issues:**
```bash
# Recreate directory structure if missing
docker exec ats-intg-minute-bars-scheduler python3 scripts/setup_daily_minute_bars_structure.py --base-path /data/firstrate-data/daily

# Validate structure for current month
docker exec ats-intg-minute-bars-scheduler python3 scripts/setup_daily_minute_bars_structure.py --validate

# Check for corrupted parquet files
find /mnt/d/ats-data/firstrate-data/daily/ -name "*.parquet" -size 0 -delete  # Remove empty files
```

**3. Processing Performance Issues:**
```bash
# Check container resource usage
docker stats --no-stream | grep "ats-intg.*minute"

# Monitor processing in real-time
docker exec ats-intg-minute-bars-scheduler tail -f /logs/minute-bars-backfill.log

# Identify slow processing
grep -i "processing.*took\|duration" /mnt/d/ats-logs/minute-bars-backfill.log | tail -10
```

**4. Prometheus Metrics Issues:**
```bash
# Reset metrics if stale
docker restart ats-intg-prometheus-metrics

# Verify metrics endpoint
curl -v http://localhost:4080/metrics | head -20

# Check metrics server logs for errors
docker logs ats-intg-prometheus-metrics --tail 50 | grep -i error
```

**Emergency Recovery:**
```bash
# Restart entire minute bars system
docker-compose -f docker-compose.minute-bars-jobs.yml down
docker-compose -f docker-compose.minute-bars-jobs.yml up -d

# Force immediate processing run
docker exec ats-intg-minute-bars-scheduler python3 scripts/daily_minute_bars_backfill.py --production --force

# Clean restart with fresh containers
docker-compose -f docker-compose.minute-bars-jobs.yml down --volumes
docker-compose -f docker-compose.minute-bars-jobs.yml up -d --force-recreate
```

**Data Validation:**
```bash
# Verify file completeness for recent trading days  
python3 -c "
import pandas as pd
from datetime import date, timedelta
for i in range(7):
    d = date.today() - timedelta(days=i)
    files = len(list(Path('/mnt/d/ats-data/firstrate-data/daily').glob(f'{d.strftime(\"%Y/%m/%d\")}/*/*.parquet')))
    print(f'{d}: {files} files')
"

# Sample file validation
find /mnt/d/ats-data/firstrate-data/daily/ -name "*.parquet" -exec python3 -c "import pandas as pd; df=pd.read_parquet('{}'); print('✅' if len(df)>0 else '❌', '{}')" \; | head -10
```

### **🚨 WSL MONITORING & CRON TROUBLESHOOTING**

**WSL Monitoring Issues (CRITICAL - Fixed 2025-09-01):**
```bash
# Problem: No Slack notifications received
# Root Cause: Monitoring process stopped, no auto-restart configured
# Solution: Active monitoring + cron backup + auto-restart

# Check monitoring status
ps aux | grep simple_wsl_monitor | grep -v grep
# Expected: Should show python3 simple_wsl_monitor.py --hourly process

# If monitoring is DOWN:
/home/jianjun/ats-genai-data/restart_monitoring.sh

# Verify Slack webhook works
cd /home/jianjun/ats-genai-data/scripts/monitoring
python3 simple_wsl_monitor.py --test
# Expected: "✅ Test alert sent successfully!" + Slack notification

# Check monitoring log for errors
tail -50 /mnt/d/ats-logs/wsl_monitor.log

# Emergency: Temporary hourly alerts via cron
(crontab -l; echo "*/15 * * * * python3 simple_wsl_monitor.py --test") | crontab -
```

**Cron Job Troubleshooting:**
```bash
# Check if cron daemon is running
systemctl status cron
sudo systemctl start cron  # If stopped

# View recent cron execution logs
grep CRON /var/log/syslog | tail -20
journalctl -u cron --since "1 hour ago"

# Test cron job manually
# Extract command from crontab -l and run it directly
/home/jianjun/ats-genai-data/scripts/daily_backup_ats_dev.sh

# Common cron issues and fixes:
# 1. Environment variables missing
env > /tmp/cron_env.txt  # Compare with your shell environment

# 2. Path issues - always use absolute paths
which python3  # Use full path in cron jobs

# 3. Permission issues
ls -la /home/jianjun/ats-genai-data/scripts/  # Check execute permissions

# 4. Output redirection missing
# ❌ BAD: 0 * * * * some_command
# ✅ GOOD: 0 * * * * some_command >> /var/log/command.log 2>&1
```

**FirstRate Download Issues:**
```bash
# Check if daily FirstRate download is working
tail -100 /mnt/d/ats-logs/firstrate-daily.log
tail -50 /mnt/d/ats-logs/firstrate-daily-error.log

# Verify data was downloaded today
ls -la /mnt/d/ats-data/firstrate-data/ | head -10
find /mnt/d/ats-data/firstrate-data/ -name "*.zip" -mtime -1  # Files modified in last 24h

# Manual test of FirstRate download
cd /home/jianjun/ats-genai-data
PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --test
```

**Backup Job Monitoring:**
```bash
# Check if daily backups completed successfully  
ls -la /mnt/d/ats-backup/ | grep $(date +%Y-%m-%d)
./scripts/manage_backups.sh status

# Check backup logs
tail -50 /mnt/d/ats-logs/backup_monitor.log

# Manual backup test
/home/jianjun/ats-genai-data/scripts/daily_backup_ats_dev.sh
```

**WSL System Monitoring Configuration:**
```bash
# Monitoring script location
/home/jianjun/ats-genai-data/scripts/monitoring/simple_wsl_monitor.py

# Configuration file
/home/jianjun/ats-genai-data/scripts/monitoring/monitor_config.json

# Restart script  
/home/jianjun/ats-genai-data/restart_monitoring.sh

# Log locations
/mnt/d/ats-logs/wsl_monitor.log           # Main monitoring log
/mnt/d/ats-logs/monitoring/               # Historical metrics

# Slack webhook (configured in script)
# Channel: #ats-alerts
# Frequency: Hourly status updates
# Auto-restart: @reboot cron job
```

### **🆘 Emergency Response**

**Docker Networking Issues (FIXED 2025-08-30):**
```bash
# ✅ ROOT CAUSE IDENTIFIED AND FIXED:
# - run_dev.py was using deprecated --link instead of --network
# - Fixed by updating run_dev.py to use --network ats-network

# Verify the fix is working:
python3 scripts/run_dev.py run --script any_script.py   # Should work now
docker network inspect ats-network                     # Show containers on network
```

### **🚨 CRITICAL: Docker Network Connectivity Patterns (2025-08-31)**

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

**Database Connectivity Checklist:**
1. **Container on correct network**: `docker network inspect network-name`
2. **Database host matches container name**: `DB_HOST=ats-intg-postgres`
3. **Use internal port (5432)** not external port (4432) in containers  
4. **Test connectivity**: `docker exec container psycopg2.connect()` test

**Troubleshooting Network Issues:**
```bash
# Check container networks
docker network ls | grep ats
docker network inspect ats-intg-network --format "{{range .Containers}}{{.Name}} {{end}}"

# Connect missing containers to networks
docker network connect ats-intg-network container-name

# Verify database reachability from container
docker exec scheduler-container python3 -c "import psycopg2; psycopg2.connect(host='db-container', port=5432, user='postgres')"
```

**Service Startup Dependencies:**
- **Database first**: Start PostgreSQL before dependent services
- **Network connectivity**: Ensure containers can resolve each other's hostnames
- **Health checks**: Wait for database readiness before application startup
- **Missing files**: Create required startup scripts when containers expect them

**Service Recovery:**
```bash
# ATS-DEV Service Recovery
python3 scripts/run_dev.py stop --service analytics
python3 scripts/run_dev.py start --service analytics

# ATS-INTG Service Recovery  
docker-compose -f docker-compose.intg-jobs.yml restart ats-intg-scheduler
docker restart grafana-intg prometheus-intg

# Database restart if needed
docker restart ats-dev-postgres
docker restart postgres-intg
```

**Data Quality Checks:**
```bash
# Check instrument populations
python3 scripts/run_dev.py query --query "
SELECT 'Tiingo' as vendor, COUNT(*) as instruments FROM dev_instrument_tiingo
UNION
SELECT 'EODHD' as vendor, COUNT(*) as instruments FROM dev_instrument_eodhd
UNION  
SELECT 'Polygon' as vendor, COUNT(*) as instruments FROM dev_instrument_polygon
"

# Check data freshness
python3 scripts/run_dev.py query --query "
SELECT vendor, MAX(date) as latest_data, COUNT(*) as records_today
FROM dev_daily_prices 
WHERE date >= CURRENT_DATE - 1
GROUP BY vendor
"
```

### **📅 CRON JOB MANAGEMENT**

**Current Production Cron Jobs:**
```bash
# View all cron jobs
crontab -l

# Current active jobs:
# ┌─────────────────── Minute (0-59)
# │ ┌───────────────── Hour (0-23)
# │ │ ┌─────────────── Day of month (1-31)
# │ │ │ ┌───────────── Month (1-12)
# │ │ │ │ ┌─────────── Day of week (0-7, Sunday=0 or 7)
# │ │ │ │ │
# * * * * * command
```

**🚨 CRITICAL: Two-Stream Market Data Collection Schedule:**
```bash
# ===============================================================================
# REAL-TIME INTRADAY COLLECTION (Database Storage) - Every 30 minutes during market hours
# ===============================================================================

# Polygon Real-Time Collection - Every 30 minutes (9:30 AM - 4:00 PM EST)
30,0 9-15 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/polygon_realtime_collect.py --database >> /mnt/d/ats-logs/polygon-realtime.log 2>&1

# Tiingo Real-Time Collection - Every 30 minutes (9:30 AM - 4:00 PM EST)  
30,0 9-15 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/tiingo_realtime_collect.py --database >> /mnt/d/ats-logs/tiingo-realtime.log 2>&1

# ===============================================================================
# END-OF-DAY COMPLETE COLLECTION (Parquet Files) - After 7:00 PM daily
# ===============================================================================

# Polygon End-of-Day Complete Minute Bars - 7:30 PM EST daily (after settlement)
30 19 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/polygon_eod_minute_bars.py --parquet >> /mnt/d/ats-logs/polygon-eod.log 2>&1

# Tiingo End-of-Day Complete Minute Bars - 8:00 PM EST daily (after settlement)
0 20 * * 1-5 cd /home/jianjun/ats-genai-data && PYTHONPATH=src python3 scripts/tiingo_eod_minute_bars.py --parquet >> /mnt/d/ats-logs/tiingo-eod.log 2>&1

# ===============================================================================
# FIRSTRATE DIRECT-TO-PARQUET COLLECTION - Single daily download
# ===============================================================================

# FirstRate Daily Download - 2:30 AM EST/EDT daily (previous trading day data)
30 2 * * * cd /home/jianjun/ats-genai-data && PYTHONPATH=src uv run python scripts/firstrate_daily_download.py --all >> /mnt/d/ats-logs/firstrate-daily.log 2>> /mnt/d/ats-logs/firstrate-daily-error.log

# ===============================================================================
# ATS PLATFORM MAINTENANCE
# ===============================================================================

# ATS Platform Daily Backups
0 2 * * * /home/jianjun/ats-genai-data/scripts/daily_backup_ats_dev.sh        # ATS-DEV backup at 2:00 AM
15 2 * * * /home/jianjun/ats-genai-data/scripts/daily_backup_ats_intg.sh      # ATS-INTG backup at 2:15 AM

# Backup Monitoring  
0 3 * * * /home/jianjun/ats-genai-data/scripts/backup_monitor.sh              # Monitor at 3:00 AM
0 18 * * * /home/jianjun/ats-genai-data/scripts/backup_monitor.sh             # Monitor at 6:00 PM

# WSL System Monitoring (CRITICAL - Added 2025-09-01)
0 * * * * python3 simple_wsl_monitor.py --test >/dev/null 2>&1                # Hourly system status to Slack
@reboot sleep 30 && /home/jianjun/ats-genai-data/restart_monitoring.sh >/dev/null 2>&1  # Auto-restart monitoring on boot

# ATS-INTG Daily Data and Monitoring Jobs (Configured in intg_startup_manager.py)
# ================================================================================
# ATS-INTG Daily Data Refresh - Multi-vendor price collection with overlap validation
0 3 * * * cd /workspace && PYTHONPATH=/workspace/src python3 scripts/daily_data_refresh.py --vendors tiingo,polygon,eodhd --max-symbols 1000 >> /logs/daily_refresh.log 2>&1

# ATS-INTG Daily Price Coverage Validation - 90-day lookback with Prometheus metrics and Slack alerts
30 3 * * * cd /workspace && PYTHONPATH=/workspace/src python3 scripts/daily_price_coverage_validator.py --vendors tiingo,polygon,eodhd --days 90 --export-prometheus --alert-threshold 0.95 >> /logs/coverage_validation.log 2>&1

# ATS-INTG Weekly Maintenance - Data quality checks and cleanup
0 4 * * 0 cd /workspace && PYTHONPATH=/workspace/src python3 scripts/weekly_maintenance.py --deep-clean >> /logs/weekly_maintenance.log 2>&1

# ATS-INTG Priority Symbols Daily Collection - High-priority symbols every 6 hours
0 9,15,21 * * * cd /workspace && PYTHONPATH=/workspace/src python3 scripts/daily_data_refresh.py --symbols AAPL,TSLA,MSFT,GOOGL,AMZN,META,NVDA,SPY,QQQ,VTI --vendors tiingo,polygon >> /logs/priority_refresh.log 2>&1

# ATS-INTG Health Check - Every 4 hours to ensure data pipeline is working
0 */4 * * * cd /workspace && PYTHONPATH=/workspace/src python3 scripts/daily_data_refresh.py --symbols AAPL --vendors tiingo --debug >> /logs/health_check.log 2>&1

# ATS-INTG Coverage Validation - Every 6 hours for real-time monitoring
0 6,12,18 * * * cd /workspace && PYTHONPATH=/workspace/src python3 scripts/daily_price_coverage_validator.py --vendors tiingo,polygon,eodhd --days 7 --export-prometheus --alert-threshold 0.90 >> /logs/coverage_monitoring.log 2>&1

# ATS-INTG Slack Daily Coverage Summary - 7PM EST daily notification with 90-day coverage table
0 19 * * * cd /workspace && PYTHONPATH=/workspace/src python3 scripts/slack_daily_coverage_summary.py --days 90 >> /logs/slack_daily_summary.log 2>&1

# ATS-INTG Daily 1-Minute Bar Backfill System (docker-compose.minute-bars-jobs.yml)
# - Daily backfill: 4:00 AM EST (last 7 days, all instruments)
# - Critical ETFs: 4:30 AM EST (last 3 days, priority symbols) 
# - Weekend catch-up: Saturday 6:00 AM EST (last 10 days)
# - Health check: Every 6 hours (AAPL, SPY test run)
# - Daily summary: 8:00 AM EST Slack notification
# - Weekly report: Monday 9:00 AM EST Slack notification
# Note: Runs in containerized scheduler, not host cron
```

**WSL System Monitoring Setup (CRITICAL):**
```bash
# Check monitoring status
ps aux | grep simple_wsl_monitor | grep -v grep

# Restart monitoring manually
/home/jianjun/ats-genai-data/restart_monitoring.sh

# Monitor logs
tail -f /mnt/d/ats-logs/wsl_monitor.log

# Test Slack notifications
cd /home/jianjun/ats-genai-data/scripts/monitoring
python3 simple_wsl_monitor.py --test
```

**Cron Job Management Commands:**
```bash
# Edit cron jobs
crontab -e

# List all cron jobs
crontab -l

# Remove all cron jobs (DANGEROUS)
crontab -r

# Add new cron job
(crontab -l 2>/dev/null; echo "0 * * * * /path/to/command") | crontab -

# Check cron service status
systemctl status cron
sudo systemctl restart cron

# View cron logs
grep CRON /var/log/syslog | tail -20
journalctl -u cron | tail -20
```

**Cron Job Best Practices:**
- ✅ **Always use absolute paths** for commands and scripts
- ✅ **Redirect output** to log files (`>> /path/to/log 2>&1`)
- ✅ **Set environment variables** when needed (`PYTHONPATH=src`)
- ✅ **Use `/dev/null`** to suppress output for monitoring jobs
- ✅ **Stagger timing** to avoid resource conflicts (2:00, 2:15, 2:30)
- ✅ **Include error handling** and logging in scripts
- ❌ **NEVER use relative paths** or assume working directory
- ❌ **NEVER run without output redirection** (fills up mail spool)

**Monitoring Critical Jobs:**
```bash
# Check if FirstRate download ran successfully
tail -50 /mnt/d/ats-logs/firstrate-daily.log
ls -la /mnt/d/ats-data/firstrate-data/

# Verify backup completion
ls -la /mnt/d/ats-backup/ | grep $(date +%Y-%m-%d)
./scripts/manage_backups.sh status

# Confirm WSL monitoring is sending alerts
# (Check Slack #ats-alerts channel for hourly updates)
```

**ATS-INTG Monitoring Services (NEW - Added 2025-09-01):**
```bash
# Check ATS-INTG monitoring service status
docker ps | grep -E "(intg|prometheus|scheduler)"

# Test Prometheus metrics server
curl -s http://localhost:4080/metrics | head -10
curl -s http://localhost:4080/health

# Test Slack daily coverage summary manually
docker exec ats-intg-scheduler bash -c "cd /workspace && PYTHONPATH=/workspace/src python3 scripts/slack_daily_coverage_summary.py --test --days 90"

# Test daily price coverage validation
docker exec ats-intg-scheduler bash -c "cd /workspace && PYTHONPATH=/workspace/src python3 scripts/daily_price_coverage_validator.py --vendors tiingo --days 7 --debug"

# Check monitoring logs
docker logs ats-intg-scheduler --tail 20
docker logs ats-intg-prometheus-metrics --tail 10

# Monitor service endpoints
curl -f http://localhost:4000/health  # ATS-INTG Analytics
curl -f http://localhost:4080/health  # Prometheus Metrics Server
```

**ATS-INTG Monitoring Features:**
- **📊 Real-time Prometheus Metrics**: `http://localhost:4080/metrics` - Coverage stats for 18,331+ instruments
- **🔔 Slack Alerts**: Daily 7PM coverage summary with 90-day lookback table
- **🚨 Threshold Monitoring**: Automatic alerts when coverage drops below 50%
- **📈 Multi-vendor Tracking**: Tiingo, Polygon, EODHD coverage analysis
- **⏱️ Data Freshness**: Hours since last data update per vendor
- **📋 Trading Day Calculation**: Excludes weekends and US holidays
- **🎯 Trend Analysis**: Coverage improvement/decline indicators (↑↓→)

### **🎯 Daily Operations Checklist**

```bash
# Morning health check routine
./scripts/manage_backups.sh status      # Check overnight backups
docker ps | grep -E "(ats-dev|intg)"    # Verify containers running
python3 scripts/run_dev.py status       # Check ATS-DEV health

# Critical: Verify Docker networking is working (FIXED 2025-08-30)
docker network inspect ats-network --format "{{.Containers}}" | grep -q "ats-dev-postgres" && echo "✅ Docker networking OK" || echo "❌ Docker networking issue"

# Verify WSL monitoring is active (CRITICAL - Added 2025-09-01)
ps aux | grep simple_wsl_monitor | grep -v grep && echo "✅ WSL monitoring active" || echo "❌ WSL monitoring DOWN - run restart_monitoring.sh"

# Daily minute bars health check (ATS-INTG)
curl -s http://localhost:4080/metrics | grep "ats_daily_minute_backfill"  # Processing stats
docker logs ats-intg-minute-bars-scheduler --tail 20  # Recent processing
ls -la /mnt/d/ats-data/firstrate-data/daily/$(date +%Y/%m/%d)/ | wc -l  # Files count today

# Weekly maintenance
./scripts/manage_backups.sh cleanup     # Clean old backups
docker system prune -f                  # Clean unused containers/images
du -sh /mnt/d/ats-*                     # Check storage usage

# Performance monitoring
docker stats --no-stream | head -10     # Container resource usage
tail -50 /mnt/d/ats-logs/backup-*.log   # Recent backup activity
tail -20 /mnt/d/ats-logs/wsl_monitor.log  # WSL monitoring activity
tail -50 /mnt/d/ats-logs/minute-bars-backfill.log  # Daily minute bars processing
```

**🚨 CRITICAL ANTI-PATTERNS:**
- ❌ **DO NOT** use `docker run` for ATS-INTG services (use Docker Compose)
- ❌ **DO NOT** use Docker Compose for ATS-DEV services (use `run_dev.py`)
- ❌ **DO NOT** mix `run_dev.py` commands with `docker-compose` commands
- ❌ **DO NOT** start containers without ensuring network connectivity
- ❌ **DO NOT** use localhost:port in container DB_HOST configs (use container-name:5432)
- ❌ **DO NOT** assume containers can communicate across different networks

---

**📖 For comprehensive information, see the complete documentation structure at [docs/README.md](docs/README.md)**

*This is a Docker-first, test-driven development platform. Every change must be validated end-to-end with REAL DATA ONLY.*

---

**🎯 Success Checklist for Service Deployment:**
- [ ] Database containers started first with correct ports (3432 for DEV, 4432 for INTG)
- [ ] Containers connected to appropriate networks (ats-network, ats-intg-network) 
- [ ] DB_HOST configured with container names (ats-dev-postgres, ats-intg-postgres)
- [ ] Required startup scripts created when containers expect them
- [ ] Network connectivity verified between dependent containers
- [ ] Health checks confirm database readiness before application startup