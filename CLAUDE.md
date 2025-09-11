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

### **🚫 AGGRESSIVE ANTI-FILE-CREATION POLICIES**

**ZERO TOLERANCE for file proliferation under "simple", "test", "demo", "quick" excuses:**

#### **🔍 MANDATORY FILE CREATION CHECKLIST - ALL must be ✅ before creating ANY new file:**

```bash
# 1. EXHAUSTIVE SEARCH for existing functionality (MANDATORY)
find src/ -name "*.py" -exec grep -l "similar_function_name" {} \;
grep -r "similar_class_name" src/
rg "similar_feature" src/ --type py

# 2. PROVE existing files cannot be extended (MANDATORY documentation)
echo "❌ BLOCKING: Cannot extend existing_file.py because:"
echo "   - Specific technical limitation: [detailed reason]"  
echo "   - Attempted refactor: [what was tried]"
echo "   - Alternative approaches considered: [list 3+ options]"

# 3. MEASURE complexity reduction (NEW file must REDUCE total codebase complexity)
wc -l src/**/*.py                    # Before: total lines
# After new file creation:
wc -l src/**/*.py                    # After: must be FEWER total lines
echo "REQUIRED: Net reduction in total codebase size and complexity"
```

#### **❌ BANNED JUSTIFICATIONS for New Files:**
- **"It's simpler"** - REFACTOR existing code instead
- **"It's just a test"** - Use existing test files
- **"It's a quick demo"** - Extend existing examples  
- **"It's cleaner separation"** - CONSOLIDATE, don't separate
- **"It's better organized"** - REORGANIZE existing files
- **"It's more modular"** - MERGE modules, reduce complexity
- **"It's temporary"** - NO temporary files allowed
- **"It's just a POC"** - Prototype IN existing files

#### **✅ ONLY ACCEPTABLE Reasons for New Files:**
1. **Replacing MULTIPLE existing files** (net reduction in file count)
2. **Framework requirement** (e.g., required by Django, pytest, etc.)
3. **External integration** (new vendor API requiring separate authentication)
4. **Performance isolation** (proven bottleneck requiring separate process)

#### **🔧 MANDATORY REFACTORING BEFORE FILE CREATION:**

```bash
# STEP 1: Identify consolidation opportunities
grep -r "class.*Test" tests/                    # Find test classes to merge
grep -r "def test_" tests/ | wc -l              # Count total test methods
echo "TARGET: Merge into fewer, comprehensive test files"

# STEP 2: Measure current complexity  
find src/ -name "*.py" | wc -l                  # File count (must decrease)
find src/ -name "*.py" -exec wc -l {} \; | awk '{sum+=$1} END {print sum}'  # Line count

# STEP 3: Aggressive consolidation
echo "REQUIRED ACTIONS:"
echo "1. Merge similar test files into single comprehensive files"
echo "2. Consolidate duplicate utilities and helpers"
echo "3. Combine related classes into single modules"
echo "4. Remove abandoned/unused files"

# STEP 4: Prove improvement
echo "MANDATORY METRICS IMPROVEMENT:"
echo "- Fewer total files"
echo "- Fewer total lines" 
echo "- Fewer duplicate functions"
echo "- Higher test coverage per file"
```

### **🧹 CODE CONSOLIDATION REQUIREMENTS**

**Every development session MUST include consolidation:**

```bash
# MANDATORY: Find and eliminate duplication BEFORE adding new code
rg "def " src/ --type py | sort | uniq -d       # Find duplicate function names
rg "class " src/ --type py | sort | uniq -d     # Find duplicate class names

# REQUIRED: Merge duplicate test files
find tests/ -name "*test*.py" | xargs grep -l "class.*Test" | head -10
echo "ACTION: Merge these test classes into comprehensive test files"

# CONSOLIDATE: Remove single-use files
find src/ -name "*.py" -exec sh -c 'if [ $(grep -c "def \|class " "$1") -eq 1 ]; then echo "MERGE: $1"; fi' _ {} \;
```

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

## 🐳 **CRITICAL DEPLOYMENT ARCHITECTURE**

### **🚨 Docker Network Architecture - CRITICAL FOR SERVICE COMMUNICATION**

**ALL services MUST use `ats-network` for inter-service communication:**

```bash
# Create network (done automatically by run_dev/run_intg)
docker network create ats-network

# CRITICAL: All containers must be on same network
docker run --network ats-network ...   # ✅ CORRECT
docker run ...                         # ❌ WRONG - uses bridge network, can't communicate
```

**Network Troubleshooting:**
```bash
# Check container networks (MANDATORY when services can't communicate)
docker inspect <container_name> | grep NetworkMode
docker network ls
docker network inspect ats-network    # See which containers are connected

# Fix network issues
docker stop <container>
docker rm <container>
# Restart with correct network via run_dev/run_intg scripts
```

### **🔌 Port Architecture - Environment Isolation**

| Service | DEV Environment | INTG Environment | Internal Port | 
|---------|----------------|------------------|---------------|
| **Analytics** | `localhost:3000` | `localhost:4000` | `3000` |
| **PostgreSQL** | `localhost:5432` | `localhost:4432` | `5432` |
| **API** | `localhost:8000` | `localhost:8001` | `8000` |
| **Grafana** | `localhost:3001` | `localhost:4002` | `3000` |

**Critical Port Rules:**
- **External ports differ** between environments to avoid conflicts
- **Internal container ports stay same** (analytics always uses 3000 internally)
- **Database connections use internal ports** (ats-dev-postgres:5432, ats-intg-postgres:5432)

### **📦 Container Architecture - Naming & Dependencies**

**Container Naming Pattern:**
- **DEV**: `ats-dev-{service}` (e.g., `ats-dev-analytics`, `ats-dev-postgres`)  
- **INTG**: `ats-intg-{service}` (e.g., `ats-intg-analytics`, `ats-intg-postgres`)

**Service Dependencies (Start Order Critical):**
```bash
# 1. Database FIRST (other services depend on it)
python scripts/run_dev.py start --service postgres
python scripts/run_intg.py start --service postgres

# 2. Analytics service (depends on database)  
python scripts/run_dev.py start --service analytics
python scripts/run_intg.py start --service analytics

# 3. API service (depends on database)
python scripts/run_dev.py start --service api
python scripts/run_intg.py start --service api
```

### **💾 Volume Architecture - Data Persistence**

**Critical Volume Mounts (NEVER change these):**
```bash
# Core application volumes
-v /home/jianjun/ats-genai-admin:/workspace                    # Source code
-v /mnt/d/ats-data:/data                                       # Training data, minute bars
-v /mnt/d/ats-backup:/backup                                   # Database backups  
-v /mnt/d/ats-logs:/logs                                       # Service logs

# Database volumes (persistent data)
-v postgres-dev-data:/var/lib/postgresql/data                  # DEV database
-v postgres-intg-data:/var/lib/postgresql/data                 # INTG database

# Working directory (MANDATORY)
-w /workspace                                                  # All containers work from here
```

**Data Structure (READ-ONLY - Do not modify):**
```
/mnt/d/ats-data/
├── minute-bars/firstrate/           # Raw OHLCV data INPUT (parquet files)
├── training-data/                   # ML-ready datasets OUTPUT (arrayrecord)
├── checkpoints/                     # API rate limiting checkpoints
└── temp/                           # Temporary processing files
```

### **🔧 Environment Variables - Service Configuration**

**DEV Environment Variables:**
```bash
# Database connection (internal docker network)
DB_HOST=ats-dev-postgres             # Container name, NOT localhost
DB_PORT=5432                        # Internal port, NOT external 5432
DB_USER=postgres
DB_PASSWORD=dev_password  
DB_NAME=dev_db
ENVIRONMENT=dev

# File paths (container perspective)
ATS_DATA_PATH=/data                 # Maps to /mnt/d/ats-data
ATS_BACKUP_PATH=/backup            # Maps to /mnt/d/ats-backup
ATS_LOGS_PATH=/logs                # Maps to /mnt/d/ats-logs
PYTHONPATH=/workspace/src          # Critical for Python imports
```

**INTG Environment Variables:**
```bash
# Database connection (internal docker network)  
DB_HOST=ats-intg-postgres           # Container name, NOT localhost
DB_PORT=5432                       # Internal port, NOT external 4432
DB_USER=postgres
DB_PASSWORD=intg_password
DB_NAME=intg_db
ENVIRONMENT=intg

# Same file paths as DEV (same volume mounts)
ATS_DATA_PATH=/data
ATS_BACKUP_PATH=/backup
ATS_LOGS_PATH=/logs
PYTHONPATH=/workspace/src
```

### **⚡ Service Commands - Exact Startup Commands**

**Analytics Service:**
```bash
# DEV
python src/services/analytics_service.py    # Port 3000 internally
# INTG  
python src/services/analytics_service.py    # Port 3000 internally, exposed as 4000
```

**Database Service:**
```bash
# Both environments use same image
timescale/timescaledb:latest-pg13
```

**API Service:**
```bash
# Both environments
python src/api/main.py               # Port 8000 internally
```

### **🚨 COMMON DEPLOYMENT ISSUES & FIXES**

**Issue 1: "Connection refused" errors**
```bash
# Symptom: Services can't reach database
# Root Cause: Containers on different networks
# Fix: Ensure both containers use --network ats-network

# Debug:
docker inspect <container> | grep NetworkMode
# Should show "ats-network", not "bridge"
```

**Issue 2: "Loading database tables..." (dummy content)**
```bash
# Symptom: Analytics shows loading screens instead of data
# Root Cause: Database connection misconfigured  
# Fix: Check DB_HOST uses container name, not localhost

# Debug:
docker logs ats-intg-analytics --tail 20
# Look for connection errors to wrong host/port
```

**Issue 3: Character encoding issues (�� instead of emojis)**  
```bash
# Symptom: "ðŸš€" instead of "🚀" 
# Root Cause: Missing charset=utf-8 in HTTP headers
# Fix: Add charset to Content-Type header in analytics service
```

**Issue 4: Port conflicts**
```bash
# Symptom: "Port already in use" errors
# Root Cause: Dev and intg services using same external ports
# Fix: Use correct port mappings (3000 vs 4000, 5432 vs 4432)

# Debug:
docker ps | grep -E "(3000|4000|5432|4432)"
netstat -tulpn | grep -E "(3000|4000|5432|4432)"
```

### **🔍 MANDATORY DEPLOYMENT VERIFICATION**

**After starting any service, ALWAYS verify:**
```bash
# 1. Container is running on correct network
docker inspect <container> | grep -A 5 NetworkMode

# 2. Database connectivity works  
curl -f http://localhost:<port>/health

# 3. Service logs show no connection errors
docker logs <container> --tail 10

# 4. Port mappings are correct
docker ps | grep <service_name>
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

### **🚫 BANNED: Superficial Testing (Testing Theater)**

**These tests are MEANINGLESS and provide false confidence:**

#### **❌ SUPERFICIAL TESTS (FORBIDDEN):**
```python
# MEANINGLESS tests that check nothing useful:
def test_file_exists():
    assert os.path.exists('/path/to/file.txt')           # File exists ≠ content correct

def test_function_runs():
    result = my_function()                               # Runs ≠ produces correct output
    assert result is not None                            # Not None ≠ meaningful result

def test_api_returns_200():
    response = requests.get('/api/endpoint')             # 200 OK ≠ correct data
    assert response.status_code == 200                   # HTTP success ≠ business logic works

def test_database_table_exists():
    cursor.execute("SELECT * FROM table_name LIMIT 1")  # Table exists ≠ data is valid
    assert cursor.fetchone() is not None                # Has rows ≠ correct rows

def test_service_starts():
    service.start()                                      # Starts ≠ functions correctly
    assert service.is_running()                          # Running ≠ working properly
```

#### **✅ MANDATORY REAL TESTING:**
```python
# MEANINGFUL tests that verify actual functionality and data quality:

def test_data_processing_accuracy():
    """Test actual data transformation with known input/output pairs."""
    input_data = pd.DataFrame({
        'price': [100.0, 105.0, 95.0],
        'volume': [1000, 1500, 800]
    })
    
    result = process_market_data(input_data)
    
    # Verify ACTUAL calculations and transformations
    assert result['sma_3'].iloc[-1] == pytest.approx(100.0, abs=0.01)  # Specific calculation
    assert len(result) == len(input_data), "No data loss during processing"
    assert result['volume'].sum() == input_data['volume'].sum(), "Volume conservation"
    assert not result.isnull().any().any(), "No missing values introduced"

def test_api_returns_correct_training_data():
    """Test API returns structurally and semantically correct training data."""
    response = requests.get('/api/training-data/AAPL/2024-01-01/2024-01-31')
    
    # Verify HTTP success AND content structure AND data quality
    assert response.status_code == 200, "HTTP request successful"
    
    data = response.json()
    assert 'timeframe_features' in data, "Expected data structure present"
    assert '5m' in data['timeframe_features'], "Required timeframes included"
    
    # Verify ACTUAL data content and quality
    ohlcv = data['timeframe_features']['5m']
    assert ohlcv['open'] > 0, "Realistic price data"
    assert ohlcv['high'] >= ohlcv['open'], "Price relationship logical"
    assert ohlcv['low'] <= ohlcv['open'], "Price relationship logical"
    assert ohlcv['volume'] > 0, "Non-zero volume data"

def test_database_data_integrity():
    """Test database contains correct, complete, and consistent data."""
    conn = get_database_connection()
    
    # Verify data QUALITY, not just existence
    result = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as unique_symbols,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            AVG(CASE WHEN close > 0 THEN 1 ELSE 0 END) as valid_price_pct
        FROM daily_prices 
        WHERE date >= '2024-01-01'
    """).fetchone()
    
    assert result.total_records > 1000, f"Expected >1000 records, got {result.total_records}"
    assert result.unique_symbols >= 100, f"Expected >=100 symbols, got {result.unique_symbols}"
    assert result.valid_price_pct > 0.99, f"Expected >99% valid prices, got {result.valid_price_pct*100:.1f}%"
    
    # Verify data CONSISTENCY across tables
    count_check = conn.execute("""
        SELECT COUNT(*) FROM daily_prices dp
        LEFT JOIN instruments i ON dp.symbol = i.symbol  
        WHERE i.symbol IS NULL
    """).fetchone()[0]
    
    assert count_check == 0, f"Found {count_check} orphaned price records"

def test_ml_training_produces_valid_model():
    """Test ML training produces functional model with measurable performance."""
    # Use REAL market data for training
    training_data = load_real_training_data('AAPL', '2024-01-01', '2024-06-30')
    test_data = load_real_training_data('AAPL', '2024-07-01', '2024-12-31')
    
    model = train_model(training_data)
    
    # Verify model FUNCTIONALITY, not just creation
    predictions = model.predict(test_data['features'])
    
    # Test ACTUAL performance metrics
    mse = mean_squared_error(test_data['labels'], predictions)
    r2 = r2_score(test_data['labels'], predictions)
    
    assert mse < 0.01, f"MSE too high: {mse:.6f}"
    assert r2 > 0.3, f"R² too low: {r2:.3f}"
    assert not np.isnan(predictions).any(), "Model produced NaN predictions"
    assert len(predictions) == len(test_data['labels']), "Prediction count mismatch"
```

### **🔬 COMPREHENSIVE TESTING REQUIREMENTS**

**Every test MUST verify multiple aspects:**

#### **📊 DATA INTEGRITY TESTING:**
```python
def test_comprehensive_data_pipeline():
    """Test complete data pipeline with real data validation."""
    
    # 1. INPUT VALIDATION - Verify source data quality
    raw_data = load_minute_bars('AAPL', '2024-01-01')
    assert len(raw_data) > 0, "Source data not empty"
    assert raw_data['volume'].min() >= 0, "No negative volume"
    assert raw_data['high'].ge(raw_data['low']).all(), "High >= Low always"
    
    # 2. PROCESSING VALIDATION - Verify transformations
    processed_data = aggregate_to_timeframes(raw_data)
    assert '5m' in processed_data, "5-minute aggregation exists"
    assert len(processed_data['5m']) == len(raw_data) // 5, "Correct aggregation ratio"
    
    # 3. OUTPUT VALIDATION - Verify final quality
    final_features = extract_features(processed_data)
    assert not final_features.isnull().any().any(), "No missing features"
    assert final_features.dtypes.eq('float64').all(), "Correct data types"
    
    # 4. PERSISTENCE VALIDATION - Verify storage integrity
    save_training_data(final_features, 'test_dataset')
    reloaded = load_training_data('test_dataset')
    pd.testing.assert_frame_equal(final_features, reloaded, "Data persisted correctly")
```

#### **🚀 PERFORMANCE TESTING:**
```python
def test_performance_benchmarks():
    """Test actual performance meets requirements."""
    import time
    
    start_time = time.time()
    result = process_large_dataset(10000)  # Process 10k records
    processing_time = time.time() - start_time
    
    # Verify ACTUAL performance requirements
    assert processing_time < 60, f"Processing too slow: {processing_time:.1f}s"
    assert len(result) == 10000, "No data loss during processing"
    
    # Memory usage validation
    import psutil
    memory_usage = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    assert memory_usage < 1000, f"Memory usage too high: {memory_usage:.1f}MB"
```

#### **🔧 ERROR HANDLING TESTING:**
```python
def test_comprehensive_error_handling():
    """Test system handles all error scenarios gracefully."""
    
    # Test malformed input data
    with pytest.raises(ValidationError, match="Invalid price data"):
        process_market_data(pd.DataFrame({'price': [-100, None, 'invalid']}))
    
    # Test network failures
    with patch('requests.get', side_effect=ConnectionError):
        result = fetch_market_data_with_retry('AAPL')
        assert result is None, "Graceful failure on network error"
    
    # Test database failures  
    with patch('database.connect', side_effect=DatabaseError):
        success = save_data_with_fallback(test_data)
        assert success is False, "Graceful failure on database error"
        
    # Test resource exhaustion
    with patch('psutil.virtual_memory', return_value=Mock(available=1024)):  # Low memory
        result = process_with_memory_check(large_dataset)
        assert result is None, "Graceful failure on low memory"
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

## 🔍 **THOROUGH VALIDATION REQUIREMENTS**

### **🚫 BANNED: Premature Claims of Completion**

**NEVER claim work is "done" based on superficial checks:**

#### **❌ SUPERFICIAL VALIDATION (FORBIDDEN):**
```bash
# These checks are MEANINGLESS and give false confidence:
ls /path/file.txt                           # File exists ≠ content is correct  
echo "✅ Database table created"             # Table exists ≠ data is valid
curl -I http://localhost:3000               # 200 OK ≠ functionality works
grep "column_name" schema.sql               # Column exists ≠ queries work
docker ps | grep container_name             # Container running ≠ service works
```

#### **✅ MANDATORY THOROUGH VALIDATION:**
```bash
# REAL validation - verify ACTUAL functionality and data:

# 1. DATA VALIDATION - Check actual content, not just existence
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM table_name WHERE valid_data = true"
head -20 /data/output_file.csv              # Inspect actual data content
python -c "import pandas as pd; df = pd.read_csv('file.csv'); print(df.dtypes); print(df.head())"

# 2. FUNCTIONAL VALIDATION - Test complete workflows end-to-end  
curl -X POST http://localhost:3000/api/endpoint -d '{"real": "data"}' | jq '.'
python scripts/run_dev.py run --script test_complete_workflow.py

# 3. PERFORMANCE VALIDATION - Measure actual speed and resource usage
time python your_script.py                  # Real execution time
docker stats container_name                 # Actual resource usage
python -m cProfile your_script.py          # Performance profiling

# 4. ERROR HANDLING VALIDATION - Test failure scenarios
python scripts/run_dev.py query --query "INVALID SQL"      # Verify graceful error handling
curl http://localhost:3000/nonexistent                     # Test 404 responses
python test_with_invalid_data.py                          # Test bad input handling
```

### **📊 MANDATORY RESULT VERIFICATION**

**EVERY claim must be backed by measurable evidence:**

#### **🔢 QUANTITATIVE VALIDATION REQUIREMENTS:**
```bash
# DATA PROCESSING - Prove actual data was processed correctly
echo "INPUT: $(wc -l input_file.csv) records"
echo "OUTPUT: $(wc -l output_file.csv) records"  
echo "PROCESSING RATE: $((output_count / input_count * 100))% success rate"
echo "ERRORS: $(grep ERROR log_file.txt | wc -l) failures"

# DATABASE OPERATIONS - Verify actual database state changes
echo "BEFORE: $(python scripts/run_dev.py query --query 'SELECT COUNT(*) FROM table')"
# Run your operation
echo "AFTER: $(python scripts/run_dev.py query --query 'SELECT COUNT(*) FROM table')"
echo "CHANGED: $((after - before)) records affected"

# API ENDPOINTS - Test real request/response cycles
curl -s http://localhost:3000/api/data | jq '.items | length'    # Count actual results
curl -w "Response time: %{time_total}s\n" http://localhost:3000/health

# ML/TRAINING - Verify actual model output and quality
python -c "
import numpy as np
data = np.load('training_data.npy')
print(f'Shape: {data.shape}')
print(f'Data type: {data.dtype}')  
print(f'Value range: {data.min()} to {data.max()}')
print(f'NaN values: {np.isnan(data).sum()}')
"
```

#### **🎯 QUALITY VALIDATION REQUIREMENTS:**
```bash
# DATA QUALITY - Verify actual data integrity
python -c "
import pandas as pd
df = pd.read_csv('output.csv')
print(f'Completeness: {df.notna().sum().sum() / df.size * 100:.1f}%')
print(f'Unique records: {len(df.drop_duplicates())} / {len(df)}')
print(f'Data types correct: {df.dtypes}')
"

# SERVICE QUALITY - Test actual service behavior under load
for i in {1..100}; do curl -s http://localhost:3000/api/test > /dev/null; done
echo "100 requests completed - check logs for errors"

# CODE QUALITY - Measure actual improvements
pylint src/your_module.py                    # Code quality score
python -m pytest tests/ --cov=src           # Test coverage percentage
bandit -r src/                              # Security issues
```

### **⚠️ COMPLETION CRITERIA ENFORCEMENT**

**Work is NOT complete until ALL criteria are ✅:**

#### **🔍 COMPLETION VERIFICATION CHECKLIST:**
```bash
# 1. FUNCTIONAL VERIFICATION (MANDATORY)
[ ] End-to-end workflow tested and passes
[ ] Error cases tested and handled gracefully  
[ ] Performance measured and documented
[ ] Resource usage within acceptable limits

# 2. DATA VERIFICATION (MANDATORY)
[ ] Input data validated and processed correctly
[ ] Output data inspected and meets quality standards
[ ] No data corruption or loss during processing
[ ] Edge cases (empty data, large data) handled

# 3. INTEGRATION VERIFICATION (MANDATORY)
[ ] Service integration tested with real services
[ ] Database queries tested with real database
[ ] API endpoints tested with real requests/responses
[ ] UI tested with real user interactions (Playwright)

# 4. REGRESSION VERIFICATION (MANDATORY)
[ ] Existing functionality still works (full test suite passes)
[ ] Performance not degraded (benchmarks maintained)
[ ] No new security vulnerabilities introduced
[ ] Documentation updated and accurate
```

### **🚨 ZERO TOLERANCE FOR SUPERFICIAL COMPLETION**

**Immediate RED FLAGS that indicate incomplete work:**

- **"Files created successfully"** - What about the CONTENT?
- **"Service started"** - What about FUNCTIONALITY?
- **"No errors in logs"** - What about ACTUAL RESULTS?
- **"Tests pass"** - What do the tests actually VERIFY?
- **"Database table exists"** - What about DATA QUALITY?
- **"API returns 200 OK"** - What about RESPONSE CONTENT?
- **"Script runs without errors"** - What OUTPUTS were produced?

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
# /data/training_data/{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord
# 
# Example structure:
# /data/training_data/dataset_20250701_120000/TSLA_20250701_000000_20250701_235959/5m/TSLA_20250701_000000_20250701_235959.arrayrecord
# /data/training_data/dataset_20250701_120000/TSLA_20250701_000000_20250701_235959/15m/TSLA_20250701_000000_20250701_235959.arrayrecord
# /data/training_data/dataset_20250701_120000/TSLA_20250701_000000_20250701_235959/1h/TSLA_20250701_000000_20250701_235959.arrayrecord
# /data/training_data/dataset_20250701_120000/TSLA_20250701_000000_20250701_235959/1d/TSLA_20250701_000000_20250701_235959.arrayrecord

# Container path mapping: /data/training_data (container) = /mnt/d/ats-data/training-data (host)

# Check training data files for a specific dataset
ls -la /data/training_data/dataset_20250701_120000/*/

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
- ❌ **CRITICAL: Creating files under "simple", "test", "demo" excuses**
- ❌ **CRITICAL: Superficial testing (file exists, service starts, 200 OK)**
- ❌ **CRITICAL: Claiming completion without thorough validation**
- ❌ **CRITICAL: Premature claims of "done" based on existence checks**
- ❌ **CRITICAL: Writing tests that don't verify actual functionality**

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

**AGGRESSIVE ANTI-FILE-CREATION & VALIDATION:**
- [ ] **MANDATORY: Exhaustive search for existing functionality before creating files**
- [ ] **MANDATORY: Prove existing files cannot be extended with detailed documentation**
- [ ] **MANDATORY: Net reduction in total codebase complexity when creating files**
- [ ] **MANDATORY: Thorough validation of actual results, not superficial checks**
- [ ] **MANDATORY: Real testing that verifies functionality and data quality**
- [ ] **MANDATORY: Quantitative validation with measurable evidence**
- [ ] **MANDATORY: Complete end-to-end workflow testing with real data**
- [ ] **MANDATORY: Performance and error handling validation**
- [ ] **FORBIDDEN: Claims of completion based on file existence or 200 OK responses**
- [ ] **FORBIDDEN: Tests that only check existence without verifying content/functionality**

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
- **Location**: `/data/training_data/` (container path)
- **Format**: ArrayRecord format only (.arrayrecord files)
- **Structure**: `{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord`
- **✅ VERIFIED EXAMPLE**: `dataset_20250909_080134/TSLA_20250701_000000_20250701_235959/5m/TSLA_20250701_000000_20250701_235959.arrayrecord`
- **Content**: QR4-compliant scalar data (timestamp, symbol, open, high, low, close, volume, vwap)
- **Timeframes**: 5m, 15m, 1h, 1d (each gets separate ArrayRecord file)
- **File Size**: ~131KB per timeframe (contains real TSLA minute bar data)
- **Database**: Registered in `dev_training_dataset` table
- **Tracking**: All runs logged in `dev_runs` table with command_line, git_commit_hash

**❌ Common Mistakes:**
- **NOT daily prices**: `dev_daily_prices` is not involved
- **NOT firstrate-data/daily/**: Wrong location - use `minute-bars/firstrate/`
- **Minute bars ≠ Training data**: Minute bars are INPUT, training data is OUTPUT

## 📚 **Detailed Documentation**

For comprehensive operational procedures, infrastructure details, and development workflows, see:

- **[ATS_AUTOSTART_SERVICES.md](docs/ATS_AUTOSTART_SERVICES.md)** - Complete autostart service configuration and troubleshooting
- **[DEVELOPMENT_WORKFLOW.md](docs/DEVELOPMENT_WORKFLOW.md)** - Complete development processes, TDD, CI/CD
- **[OPERATIONS.md](docs/OPERATIONS.md)** - Daily operations, monitoring, troubleshooting, cron jobs
- **[INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md)** - Database connections, Docker networking, service architecture
- **[START_HERE.md](docs/START_HERE.md)** - 15-minute setup and core concepts
- **[DEVELOPMENT.md](docs/DEVELOPMENT.md)** - Complete development guide and best practices

---

## 🚨 **ZERO TOLERANCE DEVELOPMENT ENFORCEMENT**

### **📊 MANDATORY DEVELOPMENT METRICS**

**Track and improve these metrics every development session:**

```bash
# FILE PROLIFERATION CONTROL (must decrease over time)
echo "Current file count: $(find src/ tests/ -name '*.py' | wc -l)"
echo "Duplicate functions: $(rg 'def ' src/ --type py | cut -d':' -f2 | sort | uniq -d | wc -l)"
echo "Single-purpose files: $(find src/ -name '*.py' -exec sh -c 'if [ $(grep -c "def \|class " "$1") -eq 1 ]; then echo "$1"; fi' _ {} \; | wc -l)"

# CODE QUALITY ENFORCEMENT (must improve over time)
pylint src/ --score=y | grep "Your code has been rated"
python -m pytest tests/ --cov=src --cov-report=term-missing | grep "TOTAL"
bandit -r src/ -f json | jq '.metrics.CONFIDENCE.HIGH // 0'

# REAL TESTING VERIFICATION (must cover actual functionality)
echo "Superficial tests: $(grep -r 'assert.*exists\|assert.*is not None\|assert.*status_code.*200' tests/ | wc -l)"
echo "Performance tests: $(grep -r '@pytest.mark.performance\|processing_time\|memory_usage' tests/ | wc -l)"
echo "Data validation tests: $(grep -r 'data_quality\|assert.*sum\|assert.*count' tests/ | wc -l)"
```

### **⚡ IMMEDIATE RED FLAGS**

**STOP ALL WORK if you see any of these patterns:**

1. **File Creation Without Justification:**
   - New files created without exhaustive search for existing functionality
   - "Simple", "test", "demo", "quick" files being created
   - File count increasing without corresponding reduction elsewhere

2. **Superficial Validation:**
   - Claims of "done" without actual result verification
   - Tests checking only existence, not functionality
   - Missing quantitative validation of outputs

3. **Testing Theater:**
   - Tests that always pass regardless of functionality
   - No performance, error handling, or data quality tests
   - Mock data being used outside unit test scenarios

### **🎯 EXCELLENCE ENFORCEMENT**

**Development is only acceptable when ALL these conditions are met:**

- **✅ ZERO new files** created without removing multiple existing files
- **✅ ZERO superficial tests** - all tests verify actual functionality and data
- **✅ ZERO completion claims** without thorough quantitative validation
- **✅ MEASURABLE improvement** in codebase metrics (files, lines, duplicates)
- **✅ REAL DATA validation** throughout entire development cycle
- **✅ END-TO-END testing** of complete user workflows

---

**🔥 This is a Docker-first, test-driven, DEBUG-FIRST development platform. Every change must be validated end-to-end with REAL DATA ONLY. When systems fail, investigate and understand before implementing workarounds.**

**🚫 ZERO TOLERANCE for file proliferation, superficial testing, and premature claims of completion. Excellence is non-negotiable.**