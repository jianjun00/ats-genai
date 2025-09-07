# CLAUDE.md - ATS Platform Guide

This file provides focused guidance to Claude Code when working with the ATS fintech platform.

## 🚨 **CRITICAL DEVELOPMENT PRINCIPLES** ⚡

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
python scripts/run_dev.py run --script src/ml/training_data/runners/training_data_callback_runner.py

# 2. Check the run was tracked  
python scripts/run_dev.py query --query "SELECT MAX(id) as latest_run_id FROM dev_runs WHERE run_type = 'training_data_generation'"

# 3. Get run details including gin config used
python scripts/run_dev.py get --run-id <latest_run_id>

# 4. Check generated training datasets
python scripts/run_dev.py query --query "SELECT id, dataset_name, creation_timestamp FROM dev_training_dataset ORDER BY creation_timestamp DESC LIMIT 5"

# 5. Get dataset details
python scripts/run_dev.py training_dataset get <dataset_id>
```

### **Multi-Timeframe Training Data Structure**
```bash
# Training data is organized in multi-timeframe structure:
# /mnt/d/ats-data/training/{run_id}/5m/SYMBOL_START_END.riegeli
# /mnt/d/ats-data/training/{run_id}/15m/SYMBOL_START_END.riegeli  
# /mnt/d/ats-data/training/{run_id}/1h/SYMBOL_START_END.riegeli
# /mnt/d/ats-data/training/{run_id}/1d/SYMBOL_START_END.riegeli

# Check training data files for a specific run
ls -la /mnt/d/ats-data/training/35/*/

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

## 🚨 **MANDATORY DEVELOPMENT PROTOCOLS**

### **🔍 ROOT CAUSE ANALYSIS BEFORE FILE CREATION**

**❌ FORBIDDEN: Creating new files as first response to problems**

**✅ MANDATORY SEQUENCE:**
1. **INVESTIGATE existing files first** - Why don't they work?
2. **IDENTIFY root cause** - Configuration? Missing dependency? Logic error?
3. **ATTEMPT to fix existing files** - Enhance, refactor, debug
4. **ONLY create new files** if existing files are fundamentally incompatible

**❌ ANTI-PATTERN EXAMPLES:**
```bash
# WRONG: Creating new simple script because existing one is "complex"
touch scripts/simple_data_loader.py
# This hides the real issue in the existing loader

# WRONG: Creating new test file because existing tests fail
touch tests/test_simplified_api.py  
# This masks the real API problems
```

**✅ CORRECT APPROACH:**
```bash
# RIGHT: Debug why existing file fails
python scripts/run_dev.py run --script scripts/data_loader.py --verbose
# Analyze error messages, fix dependencies, enhance logic

# RIGHT: Fix failing tests to understand real issues
python scripts/run_dev.py test --test tests/test_api.py -v
# Fix the actual bugs revealed by test failures
```

**🔥 ENFORCEMENT RULES:**
- **MUST** read and understand existing relevant files BEFORE creating new ones
- **MUST** document why existing files cannot be enhanced
- **MUST** show attempt to fix existing files in commit messages

### **🔄 MANDATORY CODE REUSE ANALYSIS**

**❌ FORBIDDEN: Duplicating logic with slight variations**

**✅ MANDATORY PROCESS:**
1. **SEARCH codebase** for similar functionality before writing
2. **ANALYZE existing patterns** - Can they be parameterized?
3. **REFACTOR existing code** to handle new requirements
4. **CONSOLIDATE duplicated logic** into reusable components

**❌ ANTI-PATTERN EXAMPLES:**
```python
# WRONG: Creating similar but slightly different functions
def process_daily_data(data): ...
def process_hourly_data(data): ...  # 90% same logic
def process_minute_data(data): ...  # 95% same logic

# WRONG: Copy-pasting with minor changes
class DailyAnalyzer: ...
class HourlyAnalyzer: ...  # Nearly identical class
```

**✅ CORRECT APPROACH:**
```python
# RIGHT: Parameterized reusable function
def process_market_data(data, timeframe): ...

# RIGHT: Base class with specialization
class BaseAnalyzer:
    def analyze(self, timeframe): ...
```

**🔥 ENFORCEMENT RULES:**
- **MUST** use Grep/Glob tools to find similar functionality BEFORE writing
- **MUST** refactor existing code instead of duplicating
- **MUST** consolidate duplicate logic found during development
- **MUST** justify why new similar code cannot reuse existing patterns

### **📊 COMPREHENSIVE END-TO-END TESTING REQUIREMENTS**

**❌ FORBIDDEN: Superficial testing (logs, status codes, mocks only)**

**✅ MANDATORY TESTING HIERARCHY:**
1. **DATA VALIDATION** - Verify actual data is produced correctly
2. **BUSINESS LOGIC TESTING** - Test real calculations, transformations
3. **INTEGRATION TESTING** - Test complete workflows with real data
4. **USER WORKFLOW TESTING** - Test actual user interactions (Playwright)

**❌ SUPERFICIAL TEST EXAMPLES:**
```python
# WRONG: Only testing logs
def test_data_processing():
    process_data()
    assert "Processing complete" in logs  # Meaningless

# WRONG: Only testing status codes
def test_api_endpoint():
    response = client.get("/data")
    assert response.status_code == 200  # No data validation

# WRONG: Mock everything, test nothing real
def test_calculation():
    with patch('get_real_data') as mock_data:
        mock_data.return_value = [1,2,3]
        result = calculate()
        assert result == "mocked"  # No real logic tested
```

**✅ COMPREHENSIVE TEST EXAMPLES:**
```python
# RIGHT: Test actual data and business logic
def test_data_processing():
    # Use real test data
    input_data = load_real_test_data()
    result = process_data(input_data)
    
    # Validate actual output structure and values
    assert len(result) > 0
    assert all(isinstance(r.price, float) for r in result)
    assert all(r.timestamp is not None for r in result)
    
    # Test business logic with known inputs/outputs
    known_input = create_known_test_case()
    expected_output = calculate_expected_result()
    actual_output = process_data(known_input)
    assert actual_output == expected_output

# RIGHT: Test complete workflow with real services
def test_complete_data_pipeline():
    # Start real services
    start_database()
    start_api_server()
    
    # Test entire flow
    raw_data = fetch_market_data()
    processed_data = transform_data(raw_data)
    stored_data = save_to_database(processed_data)
    api_response = query_api_endpoint()
    
    # Validate end-to-end data integrity
    assert api_response.data == stored_data
    assert len(api_response.data) == len(processed_data)
```

**🔥 ENFORCEMENT RULES:**
- **MUST** test actual data production and consumption
- **MUST** validate business logic with known inputs/outputs
- **MUST** test complete workflows, not isolated units only
- **MUST** use real data/services in integration tests

### **✅ MANDATORY VERIFICATION CHECKPOINTS**

**❌ FORBIDDEN: Stopping work without complete verification**

**✅ MANDATORY END-TO-END VERIFICATION SEQUENCE:**

**For API/Backend Changes:**
1. **DATA VERIFICATION** - Query database to verify data is stored correctly
2. **API TESTING** - Test endpoints return correct data structure
3. **INTEGRATION TESTING** - Test complete request/response cycle
4. **PERFORMANCE TESTING** - Verify performance meets requirements

**For UI/Frontend Changes:**
1. **PLAYWRIGHT TESTING** - Verify UI interactions work completely
2. **DATA FLOW TESTING** - Verify data reaches UI from backend
3. **USER WORKFLOW TESTING** - Complete user journey works end-to-end
4. **ERROR HANDLING TESTING** - Verify error states display correctly

**For Data/Analytics Changes:**
1. **OUTPUT VALIDATION** - Verify correct files/tables are created
2. **DATA QUALITY TESTING** - Verify data accuracy and completeness
3. **CALCULATION VERIFICATION** - Verify mathematical correctness
4. **INTEGRATION TESTING** - Verify data flows to downstream systems

**❌ INCOMPLETE VERIFICATION EXAMPLES:**
```bash
# WRONG: Stopping after implementation without testing
git commit -m "Added new API endpoint"  # No verification it works

# WRONG: Only testing logs, not functionality
echo "API returns 200" # No actual data validation

# WRONG: Testing in isolation only
pytest tests/unit/ # No integration or end-to-end testing
```

**✅ COMPLETE VERIFICATION EXAMPLES:**
```bash
# RIGHT: Full API verification
python scripts/run_dev.py start --service api
curl http://localhost:8000/new-endpoint  # Test actual response
python scripts/run_dev.py query --query "SELECT * FROM new_table"  # Verify data
python scripts/run_dev.py test --test tests/integration/test_new_endpoint.py

# RIGHT: Full UI verification  
python scripts/run_dev.py start --service analytics
PYTHONPATH=src python3 -m pytest tests/browser_tests/test_new_feature.py -v
# Manually verify: Click through UI, verify data appears correctly

# RIGHT: Full data pipeline verification
python scripts/run_dev.py run --script scripts/data_processor.py
ls -la /output/directory/  # Verify files created
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM processed_data"  # Verify database
```

**🔥 ENFORCEMENT RULES:**
- **MUST** complete ALL verification steps before claiming success
- **MUST** test actual user interactions for UI changes
- **MUST** verify data is produced and consumable correctly
- **MUST** provide evidence of working functionality in commit messages

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

**MANDATORY DEVELOPMENT PROTOCOLS COMPLIANCE:**
- [ ] **ROOT CAUSE ANALYSIS: Investigated existing files before creating new ones**
- [ ] **CODE REUSE: Searched codebase and refactored existing logic instead of duplicating**
- [ ] **COMPREHENSIVE TESTING: Tested actual data and business logic, not just logs/status**
- [ ] **COMPLETE VERIFICATION: Verified end-to-end functionality actually works**
- [ ] **EVIDENCE PROVIDED: Commit messages show verification steps were completed**

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
- **Generator**: `src/ml/training_data/runners/training_data_callback_runner.py`
- **Callback**: `DateBasedTrainingDataCallback` - Processes intervals into sequences

**🔹 OUTPUT: Training Datasets (ML-Ready Sequences)**
- **Location**: `/data/training/` (container) = `/mnt/d/ats-data/training/` (host)
- **Format**: Numpy arrays (.npy), Riegeli files, with metadata
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

**🔥 This is a Docker-first, test-driven development platform. Every change must be validated end-to-end with REAL DATA ONLY.**