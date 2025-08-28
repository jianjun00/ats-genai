# CLAUDE.md - ATS Platform Guide

This file provides focused guidance to Claude Code when working with the ATS fintech platform.

## 🚨 CRITICAL: Be concise about code

**ALWAYS read docs and code about current infra to find best way to reuse existing code:**

**ALWAYS have a document on a new script as to why it is needed and what it does:**

**ALWAYS find opportunities to refactor code to remove duplicate functionality and delete unused code:**

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

---

## Database Connection Info (Reference)

**Docker PostgreSQL (primary):**
- Host: `localhost`, Port: `5432`
- User: `postgres`, Password: `dev_password`, Database: `dev_db`
- Started with: `python scripts/run_dev.py start --service postgres`

**Integration PostgreSQL:**
- Host: `localhost`, Port: `5433`  
- User: `postgres`, Password: `intg_password`, Database: `intg_db`
- Started with: `python scripts/run_intg.py start --service postgres`

**Auto-detection:** run_dev and run_intg automatically detect available connections

---

## 💾 **ATS Persistent Storage (D: Drive)**

**Automatic Volume Mounting:**
- **📁 Data**: `/mnt/d/ats-data` → `/data` (in containers)
- **📁 Backup**: `/mnt/d/ats-backup` → `/backup` (in containers)  
- **📁 Logs**: `/mnt/d/ats-logs` → `/logs` (in containers)
- **🔄 Database**: PostgreSQL data persisted to `D:\ats-data\db`

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

---

**📖 For comprehensive information, see the complete documentation structure at [docs/README.md](docs/README.md)**

*This is a Docker-first, test-driven development platform. Every change must be validated end-to-end with REAL DATA ONLY.*