# CLAUDE.md - ATS Platform Guide

This file provides focused guidance to Claude Code when working with the ATS fintech platform.

## 🚨 **CRITICAL DEVELOPMENT PRINCIPLES** ⚡

### **🔍 DEBUG-FIRST: NO WORKAROUNDS WITHOUT ROOT CAUSE ANALYSIS**
- **❌ NEVER use workarounds** without understanding the underlying issue
- **❌ NEVER restart services** without investigating why they failed
- **❌ NEVER switch environments** when current environment has problems
- **❌ NEVER use manual SQL** when migration manager fails - fix migration manager
- **❌ NEVER create new infrastructure** when existing infrastructure is broken
- **✅ ALWAYS investigate logs** when commands fail
- **✅ ALWAYS understand WHY** before implementing solutions
- **✅ ALWAYS fix root causes** - not symptoms
- **✅ ALWAYS document findings** in issues/commits for future reference

**Debugging Requirements:**
- Read error messages completely and understand them
- Check service logs before restarting services
- Investigate git history when encountering similar issues
- Review related documentation before trying alternative approaches
- Test hypotheses systematically rather than trying random fixes

### **📋 MANDATORY ROOT CAUSE ANALYSIS PROCESS**

**When ANY command/service fails, follow this exact sequence:**

#### 1. **🔍 GATHER EVIDENCE** 
```bash
# NEVER restart or switch environments first - investigate!
# Check service status
python scripts/run_dev.py status
docker ps -a | grep -E "(ats|postgres)"

# Get detailed logs (MANDATORY before any fixes)
python scripts/run_dev.py logs --service <failed_service>
docker logs <container_id> --tail 100

# Check system resources
df -h                                    # Disk space
docker system df                         # Docker space usage
free -h                                 # Memory usage
```

#### 2. **📖 READ DOCUMENTATION & CODE**
```bash
# Check related documentation FIRST
grep -r "error_message" docs/
git log --grep="similar_issue" --oneline -10

# Examine relevant source code
find src/ -name "*.py" -exec grep -l "error_pattern" {} \;
grep -n "failed_function" src/path/to/relevant/file.py
```

#### 3. **🕵️ SYSTEMATIC INVESTIGATION**
```bash
# Test isolated components
python scripts/run_dev.py query --query "SELECT version()"  # DB connectivity
curl -f http://localhost:3000/health                        # Service health
docker exec <container> ps aux                              # Process status inside container

# Check configuration consistency
diff config/dev.yaml config/current.yaml                    # Config drift
env | grep -E "(POSTGRES|DOCKER)"                          # Environment variables
```

#### 4. **💡 HYPOTHESIS-DRIVEN DEBUGGING**
- **Form specific hypothesis** about the root cause
- **Test hypothesis** with minimal reproduction case  
- **Document findings** - what worked, what didn't, why
- **Implement targeted fix** based on understanding
- **Verify fix** solves root cause, not just symptoms

#### 5. **📝 DOCUMENT ROOT CAUSE**
```bash
# Document findings in commit/issue
git commit -m "fix: resolve postgres connection issue

Root cause: Connection pool exhaustion due to unclosed connections in analytics service
Investigation: Checked docker logs, found 'too many connections' error  
Solution: Added proper connection cleanup in analytics_service.py:245
Verification: Service now maintains <10 connections vs previous 100+

Refs: #123"
```

### **🚫 NO MOCK/SYNTHETIC DATA IN NON-TEST CODE**
- **❌ NEVER use mock data, fake data, synthetic data, demo data** outside of unit tests
- **❌ NEVER create fallbacks to demo data** when real data is unavailable  
- **❌ NEVER return 200 OK with fake data** when database queries fail
- **✅ Demo data ONLY in unit tests** - isolated, controlled test scenarios
- **✅ Fail fast and clearly** when real data/database is unavailable
- **✅ Show actual errors** - connection failures, missing data, schema problems

**Why Mock Data Is Dangerous:**
- Hides database connection problems and query failures
- Masks data quality issues, missing values, and real-world edge cases
- Creates false performance metrics (demo data is always fast and perfect)
- Prevents detection of authentication, permission, and network issues
- Results in production surprises when real data behaves differently

### **🔄 ENHANCE EXISTING BEFORE CREATING NEW**
- **❌ NEVER create new files** without checking if existing files can be enhanced
- **❌ NEVER create new services** without checking if existing services can be extended
- **❌ NEVER add new containers/ports** without checking if existing infrastructure can handle it
- **✅ ALWAYS enhance existing services** - add features to current code
- **✅ ALWAYS reuse existing ports/endpoints** - extend current APIs
- **✅ ALWAYS consolidate functionality** - reduce complexity, don't add it

### **📁 MODIFY EXISTING FILES FIRST**
- **❌ NEVER create new scripts** when existing scripts can be modified
- **❌ NEVER duplicate functionality** in new files
- **✅ ALWAYS extend existing files** - add methods, enhance classes
- **✅ ALWAYS look for similar functionality** before writing new code
- **✅ ALWAYS refactor and consolidate** - remove duplication

## 🐳 **Docker-First Development**

**ALWAYS USE DOCKER FOR ALL OPERATIONS:**
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
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instruments"
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py
python scripts/run_dev.py run --script scripts/training/train_model.py --gpu  # With GPU
python scripts/run_dev.py start --service postgres # Start database
python scripts/run_dev.py start --service analytics # Start analytics service
python scripts/run_dev.py status                   # Check running services
python scripts/run_dev.py test                     # Run tests

# ❌ NEVER run docker commands manually for dev work
# ✅ ALWAYS use python scripts/run_dev.py
```

## 🧪 **Test-Driven Development (MANDATORY)**

### MANDATORY sequence for ALL code changes:
```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should FAIL (proves test works)

# 2. Write minimal code to make test pass
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

## 🎭 **Playwright UX Testing (MANDATORY for Frontend Changes)**

### ⚠️ **CRITICAL:** ALWAYS test UX changes with Playwright BEFORE claiming success

```bash
# 1. Start services for testing
python scripts/run_dev.py start --service analytics
python scripts/run_dev.py start --service postgres

# 2. MANDATORY: Test complete user flows 
PYTHONPATH=src python3 -m pytest tests/browser_tests/test_eda_playwright.py -v --tb=short

# 3. Test specific features (dataset visualization, sequence selection, etc.)
PYTHONPATH=src python3 -m pytest tests/browser_tests/ -k "training_dataset" -v

# 4. Create new Playwright test for new UX features
touch tests/browser_tests/test_new_ux_feature.py
# Write test that exercises complete user workflow

# 5. Run test to verify end-to-end functionality
PYTHONPATH=src python3 -m pytest tests/browser_tests/test_new_ux_feature.py -v
```

### UX Testing Requirements:
- **✅ REQUIRED:** Test complete user workflow from UI interaction to data display
- **✅ REQUIRED:** Verify API endpoints return expected data structure  
- **✅ REQUIRED:** Test error cases and edge conditions in UI
- **❌ FORBIDDEN:** Claiming UX changes work without Playwright verification

## 📋 **Common Commands**

### Development Setup
```bash
# Setup complete development environment
python scripts/run_dev.py setup

# Testing (MANDATORY for UX/API changes)
python scripts/run_dev.py test
python scripts/run_dev.py test --test tests/integration/
python scripts/run_dev.py test --test tests/unit/

# Playwright Testing (REQUIRED for all UX changes)
PYTHONPATH=src python3 -m pytest tests/browser_tests/ -v
PYTHONPATH=src python3 -m pytest tests/ui/ -v --tb=short

# Database operations
python scripts/run_dev.py query --query "SELECT version()"
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instruments"

# Run tracking and metadata
python scripts/run_dev.py get --run-id <run_id>             # Get run details with gin config tracking
python scripts/run_dev.py query --query "SELECT id, run_type, status, command_line FROM dev_runs ORDER BY id DESC LIMIT 10"

# Training dataset management
python scripts/run_dev.py training_dataset get <dataset_id> # Get training dataset details
python scripts/run_dev.py query --query "SELECT id, dataset_name, symbols, creation_timestamp FROM dev_training_dataset ORDER BY id DESC LIMIT 10"
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

### Script Execution
```bash
# Run data generation scripts
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py

# Run ML training with GPU
python scripts/run_dev.py run --script scripts/training/train_model.py --gpu

# Check service logs
python scripts/run_dev.py logs --service analytics
```

## 🤖 **Training Data & Run Management**

### **Run Tracking and Gin Configuration**
```bash
# Track training data generation runs with gin config
python scripts/run_dev.py get --run-id 35    # Shows: command_line, gin config, git hash, environment
# Example output: 
# command_line: training_data_callback_runner.py --gin-config config/training_data.gin --symbols AAPL TSLA
# git_commit_hash: f35265c1242abea4509aab15214e5eb9516d7227
# environment: dev

# List recent runs by type  
python scripts/run_dev.py query --query "SELECT id, run_type, status, LEFT(command_line, 50) as command FROM dev_runs WHERE run_type = 'training_data_generation' ORDER BY id DESC LIMIT 5"

# View all runs with gin config tracking
python scripts/run_dev.py query --query "SELECT id, run_type, status, created_at FROM dev_runs ORDER BY id DESC LIMIT 10"
```

### **Training Dataset Management**
```bash
# Get comprehensive training dataset details
python scripts/run_dev.py training_dataset get 1
# Shows: dataset_name, symbols, date ranges, sequence info, quality metrics, technical indicators, etc.

# List training datasets with key metrics
python scripts/run_dev.py query --query "SELECT id, dataset_name, symbols, data_quality_score, file_size_mb, total_sequences FROM dev_training_dataset ORDER BY creation_timestamp DESC"

# Find datasets by symbol
python scripts/run_dev.py query --query "SELECT id, dataset_name, symbols, creation_timestamp FROM dev_training_dataset WHERE symbols LIKE '%AAPL%'"

# Check dataset quality metrics
python scripts/run_dev.py query --query "SELECT dataset_name, data_quality_score, feature_completeness, label_completeness FROM dev_training_dataset WHERE data_quality_score > 0.9"
```

### **Training Data Generation Workflow**
```bash
# 1. Generate training data using gin config (tracks metadata automatically)
python scripts/run_dev.py run --script src/domains/ml/services/training_data/runners/training_data_callback_runner.py

# 2. Check the run was tracked  
python scripts/run_dev.py query --query "SELECT MAX(id) as latest_run_id FROM dev_runs WHERE run_type = 'training_data_generation'"

# 3. Get run details including gin config used
python scripts/run_dev.py get --run-id <latest_run_id>

# 4. Check generated training datasets
python scripts/run_dev.py query --query "SELECT id, dataset_name, creation_timestamp FROM dev_training_dataset ORDER BY creation_timestamp DESC LIMIT 5"

# 5. Get dataset details
python scripts/run_dev.py training_dataset get <dataset_id>
```

### **Training Data Structure (ArrayRecord Format Only)**
```bash
# Training data is organized in multi-timeframe structure using ArrayRecord format:
# /mnt/d/ats-data/training-data/{dataset_id}/SYMBOL_STARTDATE_STARTTIME_ENDDATE_ENDTIME/{timeframe}/SYMBOL_STARTDATE_STARTTIME_ENDDATE_ENDTIME.arrayrecord
# 
# Example structure:
# /mnt/d/ats-data/training-data/89/TSLA_20250701_000000_20250906_000000/5m/TSLA_20250701_000000_20250906_000000.arrayrecord
# /mnt/d/ats-data/training-data/89/TSLA_20250701_000000_20250906_000000/15m/TSLA_20250701_000000_20250906_000000.arrayrecord
# /mnt/d/ats-data/training-data/89/TSLA_20250701_000000_20250906_000000/1h/TSLA_20250701_000000_20250906_000000.arrayrecord
# /mnt/d/ats-data/training-data/89/TSLA_20250701_000000_20250906_000000/1d/TSLA_20250701_000000_20250906_000000.arrayrecord

# Container path mapping: /data/training_data (container) = /mnt/d/ats-data/training-data (host)

# Check training data files for a specific dataset
ls -la /mnt/d/ats-data/training-data/89/*/

# Verify training data structure  
python scripts/run_dev.py query --query "SELECT run_type, parameters, command_line FROM dev_runs WHERE id = 35"
```

## 🚨 **Critical Anti-Patterns to Avoid**

**Infrastructure:**
- ❌ Running docker commands directly for dev operations
- ❌ Setting environment variables manually  
- ❌ Creating new container patterns when existing ones work
- ❌ Installing packages manually in containers
- ❌ Running services without using run_dev
- ❌ Managing container lifecycle manually
- ❌ **Creating new services/ports when existing ones can be enhanced**
- ❌ **Adding new containers when existing infrastructure can handle the requirement**

**Development:**
- ❌ Claiming functionality works without tests
- ❌ Writing tests after code (TDD requires tests first)
- ❌ Skipping integration tests (they're mandatory)
- ❌ Not testing actual service startup with run_dev
- ❌ Half-baked implementations (incomplete end-to-end)
- ❌ **Using mock/synthetic data outside of unit tests**
- ❌ **Creating new files when existing files can be enhanced**
- ❌ **Not tracking training data generation runs in dev_runs table**
- ❌ **Generating training data without gin config tracking**
- ❌ **CRITICAL: Claiming UX/frontend changes work without Playwright testing**
- ❌ **Modifying APIs without testing complete user workflow end-to-end**

**Debugging & Problem Solving:**
- ❌ **Using workarounds without understanding root cause**
- ❌ **Restarting services without checking logs first**
- ❌ **Switching environments when current environment fails**
- ❌ **Manual SQL when migration manager has issues**
- ❌ **Creating new infrastructure when existing is broken**
- ❌ **Ignoring error messages and trying random fixes**
- ❌ **Not documenting investigation findings**
- ❌ **Fixing symptoms instead of root causes**

## 🎯 **Success Criteria**

**You're following best practices when:**
- [ ] Using run_dev for all development operations
- [ ] Writing failing tests before code changes
- [ ] Running tests with run_dev test command
- [ ] Running integration tests and seeing them pass
- [ ] Testing services with localhost access
- [ ] Completing full end-to-end validation
- [ ] Reusing existing Docker/service patterns
- [ ] Using GPU support when needed for ML workloads
- [ ] **Enhancing existing services instead of creating new ones**
- [ ] **Using real data only - no mock/synthetic data outside tests**
- [ ] **Modifying existing files instead of creating new files**
- [ ] **Consolidating functionality to reduce infrastructure complexity**
- [ ] **Tracking all training data generation in dev_runs table**
- [ ] **Using gin config with proper run metadata tracking**
- [ ] **Verifying training dataset quality with run_dev training_dataset get command**
- [ ] **CRITICAL: Testing ALL UX changes with Playwright before claiming success**
- [ ] **Verifying complete user workflows work end-to-end via browser automation**

**Debugging & Problem Solving:**
- [ ] **Investigating logs before restarting services**
- [ ] **Understanding root causes before implementing fixes**
- [ ] **Reading documentation and code when encountering issues**
- [ ] **Following systematic debugging process when commands fail**
- [ ] **Documenting investigation findings in commits/issues**
- [ ] **Testing hypotheses systematically rather than random fixes**
- [ ] **Fixing broken infrastructure instead of creating new infrastructure**
- [ ] **Using proper debugging tools instead of switching environments**

## 🚨 **CRITICAL: Training Data Generation Flow**

**❌ DO NOT use `dev_daily_prices` - This table is NOT used for training data**

### **Data Flow: Minute Bars → Training Data Generator → Training Datasets**

```
1. Minute Bar Files (INPUT - Raw Data)
   ↓
2. FileBasedMinuteManager (Reads parquet files)
   ↓  
3. FileBasedMinuteMarketDataManager (Aggregates timeframes)
   ↓
4. Training Data Generator (Creates sequences, features, labels)
   ↓
5. Training Datasets (OUTPUT - ML-ready numpy arrays)
```

**🔹 INPUT: Minute Bar Files (Raw OHLCV Data)**
- **Location**: `/mnt/d/ats-data/minute-bars/firstrate/` 
- **Structure**: `{first_letter}/{SYMBOL}/{YYYY}/{MM}/{SYMBOL}_{YYYY}_{MM}.parquet`
- **Example**: `/mnt/d/ats-data/minute-bars/firstrate/A/AAPL/2025/07/AAPL_2025_07.parquet`
- **Content**: Raw minute-level OHLCV data from market

**🔹 PROCESSOR: Training Data Infrastructure**
- **Data Reader**: `FileBasedMinuteManager` - Reads parquet files from disk
- **Data Manager**: `FileBasedMinuteMarketDataManager` - Provides aggregated timeframes  
- **Generator**: `src/domains/ml/services/training_data/runners/training_data_callback_runner.py`
- **Callback**: `DateBasedTrainingDataCallback` - Processes intervals into sequences

**🔹 OUTPUT: Training Datasets (ML-Ready Sequences)**
- **Location**: `/data/training_data/` (container) = `/mnt/d/ats-data/training-data/` (host)
- **Format**: ArrayRecord format only (.arrayrecord files)
- **Structure**: `{dataset_id}/SYMBOL_STARTDATE_STARTTIME_ENDDATE_ENDTIME/{timeframe}/SYMBOL_STARTDATE_STARTTIME_ENDDATE_ENDTIME.arrayrecord`
- **Content**: Sequences with features (OHLCV + indicators) and labels (future returns)
- **Database**: Registered in `dev_training_dataset` table
- **Tracking**: All runs logged in `dev_runs` table with command_line, git_commit_hash

**❌ Common Mistakes:**
- **NOT daily prices**: `dev_daily_prices` is not involved
- **NOT firstrate-data/daily/**: Wrong location - use `minute-bars/firstrate/`
- **Minute bars ≠ Training data**: Minute bars are INPUT, training data is OUTPUT

## 📚 **Detailed Documentation**

For comprehensive operational procedures, infrastructure details, and development workflows, see:

- **[DEVELOPMENT_WORKFLOW.md](docs/DEVELOPMENT_WORKFLOW.md)** - Complete development processes, TDD, CI/CD
- **[OPERATIONS.md](docs/OPERATIONS.md)** - Daily operations, monitoring, troubleshooting, cron jobs
- **[INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md)** - Database connections, Docker networking, service architecture
- **[START_HERE.md](docs/START_HERE.md)** - 15-minute setup and core concepts
- **[DEVELOPMENT.md](docs/DEVELOPMENT.md)** - Complete development guide and best practices

---

**🔥 This is a Docker-first, test-driven, DEBUG-FIRST development platform. Every change must be validated end-to-end with REAL DATA ONLY. When systems fail, investigate and understand before implementing workarounds.**