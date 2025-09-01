# CLAUDE.md - ATS Platform Guide

This file provides focused guidance to Claude Code when working with the ATS fintech platform.

## 🚨 CRITICAL: Be concise about code

**ALWAYS read docs and code about current infra to find best way to reuse existing code:**

**ALWAYS have a document on a new script as to why it is needed and what it does:**

**ALWAYS find opportunities to refactor code to remove duplicate functionality and delete unused code:**

**DO NOT CREATE SCRIPTS to run something. YOU SHOULD JUST RUN THE EXISTING CODE:**

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
docker-compose -f docker-compose.ats.yml ps           # Check service status

# Database operations  
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db
```

### **⚡ Quick Health Check**
```bash
# Verify both environments are operational
curl -f http://localhost:3000/health  # ATS-DEV analytics
curl -f http://localhost:4000/health  # ATS-INTG analytics
docker ps | grep -E "(ats-dev|intg)"  # Container status
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

**Vendor-Specific Data Organization (Parquet Files on Disk):**
- **📊 Polygon**: `/mnt/d/ats-data/minute-bars/polygon/` (minute OHLCV parquet files)
- **📊 FirstRate**: `/mnt/d/ats-data/minute-bars/firstrate/` (6,289 parquet files, 2020-2025)  
- **📊 Tiingo**: `/mnt/d/ats-data/minute-bars/tiingo/` (future implementation)
- **📊 EODHD**: `/mnt/d/ats-data/minute-bars/eodhd/` (future implementation)

**⚠️ CRITICAL: Minute-bar data is stored as parquet files on disk, NOT in database tables**

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
- ✅ **Use existing code instead of creating scripts** - `training_data_job_runner.py` had all functionality
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
docker exec ats-intg-analytics bash -c "cd /workspace && PYTHONPATH=src python3 src/app/training_data_job_runner.py"

# Files are automatically saved to container and can be copied to host:
# Output location: /mnt/d/ats-data/training/{run_id}/
# Database tracking: intg_training_datasets table with run metadata
```

**For ATS-DEV Environment (Development):**
```bash
# Use run_dev for development environment training data generation
python3 scripts/run_dev.py run --script src/app/training_data_job_runner.py

# Files saved to: training_data_output/ (can be organized by run_id)
# Database tracking: dev_training_datasets table
```

**Command Pattern (What Works vs What Doesn't):**
```bash
# ✅ CORRECT: Use existing training job runner
docker exec ats-intg-analytics bash -c "cd /workspace && PYTHONPATH=src python3 src/app/training_data_job_runner.py"

# ✅ CORRECT: Use proper environment tools
python3 scripts/run_dev.py run --script src/app/training_data_job_runner.py

# ❌ WRONG: Create new scripts when existing code works
python3 scripts/run_dev.py --environment intg run --script new_training_script.py

# ❌ WRONG: Use wrong environment management
python3 scripts/run_dev.py --environment intg  # Should use run_intg or direct container
```

**Training Data Configuration:**
- Default generates synthetic OHLCV data for AAPL, MSFT, GOOGL
- Enhanced features include: etop, ebot, pldot, oneonedot technical indicators  
- Output: 60-day sequences with 5-day prediction horizon
- Customize symbols, date ranges, and features in `TrainingDataJobConfig`

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
| **FirstRate** | `FIRSTRATE_USER_ID` | Minute-level OHLCV (parquet files) | Premium feed |

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

### **🎯 Daily Operations Checklist**

```bash
# Morning health check routine
./scripts/manage_backups.sh status      # Check overnight backups
docker ps | grep -E "(ats-dev|intg)"    # Verify containers running
python3 scripts/run_dev.py status       # Check ATS-DEV health

# Critical: Verify Docker networking is working (FIXED 2025-08-30)
docker network inspect ats-network --format "{{.Containers}}" | grep -q "ats-dev-postgres" && echo "✅ Docker networking OK" || echo "❌ Docker networking issue"

# Weekly maintenance
./scripts/manage_backups.sh cleanup     # Clean old backups
docker system prune -f                  # Clean unused containers/images
du -sh /mnt/d/ats-*                     # Check storage usage

# Performance monitoring
docker stats --no-stream | head -10     # Container resource usage
tail -50 /mnt/d/ats-logs/backup-*.log   # Recent backup activity
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