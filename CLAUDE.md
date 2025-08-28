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

---

## Database Connection Info (Reference)

**ATS-DEV PostgreSQL (primary):**
- Host: `localhost`, Port: `5432`
- User: `postgres`, Password: `postgres` (no password), Database: `dev_db`
- Container: `ats-dev-postgres` (PostgreSQL 13)
- Started with: `python scripts/run_dev.py start --service postgres`

**ATS-INTG PostgreSQL (integration):**
- Host: `localhost`, Port: `5433`  
- User: `postgres`, Password: `intg_password`, Database: `intg_db`
- Container: `ats-intg-postgres` (TimescaleDB latest-pg13)
- Started with: `python scripts/run_intg.py start --service postgres`

**Auto-detection:** run_dev and run_intg automatically detect available connections

**Backup Commands:**
```bash
# Dev backup
docker exec ats-dev-postgres pg_dump -U postgres dev_db > /mnt/d/ats-backup/dev/backup_$(date +%Y%m%d_%H%M%S).sql

# Integration backup  
PGPASSWORD=intg_password pg_dump -h localhost -p 5433 -U postgres intg_db > /mnt/d/ats-backup/intg/backup_$(date +%Y%m%d_%H%M%S).sql
```

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

## 📚 Recent Implementation Lessons

**Critical Findings from Instrument Population Implementation (2025-08-25):**
- **Database Connection Issues**: Always set `DB_DISABLE_CONNECT_TIMEOUT=true` for PostgreSQL compatibility
- **API Key Validation**: Test API endpoints directly before troubleshooting infrastructure
- **Gin Configuration**: Use `app_dev.gin` for development (simpler than `app_docker.gin`)
- **Docker Dependencies**: Include ALL required packages in base image, not minimal subsets

**📋 Detailed Documentation**: [Instrument Population Lessons Learned](docs/development/INSTRUMENT_POPULATION_LESSONS_LEARNED.md)

---

---

## 🚨 **CRITICAL: ATS Platform Environments**

### **🔥 Two-Environment Development Architecture**

**ATS-DEV (Development):**
- **Purpose**: Primary development, unit testing, feature development
- **Database**: PostgreSQL 13 on port 5432 (no password)
- **Container**: `ats-dev-postgres`
- **Usage**: `python scripts/run_dev.py setup`

**ATS-INTG (Integration):**
- **Purpose**: CI/CD integration testing, pre-production validation
- **Database**: TimescaleDB (PostgreSQL 13.15 + TimescaleDB 2.15.3) on port 5433
- **Container**: `ats-intg-postgres`
- **Password**: `intg_password` 
- **Usage**: `python scripts/run_intg.py setup`
- **CI/CD**: Automatic deployment on main branch push when unit tests pass

### **🚀 Critical Workflow**
1. **Develop** in ATS-DEV environment (`run_dev.py`)
2. **Test** unit tests locally
3. **Push** to main branch
4. **CI/CD** automatically deploys to ATS-INTG if tests pass
5. **Integration tests** run against live ATS-INTG environment

**⚠️ NEVER confuse the environments - they have different ports, passwords, and purposes!**

---

**📖 For comprehensive information, see the complete documentation structure at [docs/README.md](docs/README.md)**

*This is a Docker + GPU-enabled, test-driven development platform. Every change must be validated end-to-end in both ATS-DEV and ATS-INTG environments with REAL DATA ONLY.*