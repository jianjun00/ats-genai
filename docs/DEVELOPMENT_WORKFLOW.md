# 💻 ATS Development Workflow

**Complete development processes, TDD, CI/CD, and quality standards for the ATS platform.**

---

## 🚨 **Critical Development Rules**

### **🚫 NO MOCK/SYNTHETIC DATA IN DEVELOPMENT ENVIRONMENTS**

**DEMO DATA HIDES REAL ISSUES AND CREATES FALSE CONFIDENCE**

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

### **🔍 SCHEMA VALIDATION PREVENTS DEV ENVIRONMENT ERRORS**

**SCHEMA ERRORS MUST BE CAUGHT BY UNIT TESTS - NEVER IN DEV ENVIRONMENT**

**EVERY database interaction must be validated before deployment:**

```bash
# 1. Validate schema compatibility before coding
python scripts/validate_schema.py --check-all

# 2. Run schema validation unit tests
PYTHONPATH=src pytest tests/unit/test_database_schema_validation.py -v

# 3. Check for anti-patterns
pre-commit run schema-anti-patterns
```

### **🚨 SINGLE SOURCE OF TRUTH - NO DUPLICATE LOGIC**

**DUPLICATE LOGIC WITH VARIATIONS IS FORBIDDEN**

**Every critical path, file structure, or business logic must have exactly ONE canonical implementation:**

**❌ BAD: Duplicate logic with variations**
```python
# File 1: Inconsistent path generation
def get_training_path_v1(symbol, date_range):
    return f"/data/training_data/{symbol}_{date_range}"

# File 2: Slightly different path generation
def get_training_path_v2(symbol, start, end):
    return f"/mnt/d/ats-data/training/{symbol}_{start}_{end}"

# File 3: Another variation
def create_dataset_dir(symbol, dates):
    return f"/data/datasets/{symbol}-{dates}"
```

**✅ CORRECT: Single canonical implementation**
```python
# Single source of truth in src/core/paths/training_dataset_paths.py
@staticmethod
def get_training_dataset_basedir(run_id: str, symbol: str, date_range: str) -> str:
    """Canonical training dataset directory structure per PRD/DRD requirements"""
    return f"/data/training_data/{run_id}/{symbol}_{date_range}"

def get_timeframe_file_path(basedir: str, timeframe: str, symbol: str) -> str:
    """Canonical ArrayRecord file path per QR4 requirements"""
    return f"{basedir}/{timeframe}/{symbol}.arrayrecord"
```

**Rules:**
- ✅ **One function per responsibility** - no variations
- ✅ **Import and reuse** - never reimplement
- ✅ **Document the canonical location** in code comments
- ❌ **Never create "similar but different" functions**
- ❌ **Never copy-paste with modifications**

**Schema validation will catch:**
- ❌ Wrong table names (`dev_training_datasets` vs `dev_training_dataset`)
- ❌ Wrong column names (`created_at` vs `creation_timestamp`)
- ❌ Missing tables or columns
- ❌ SQL syntax errors
- ❌ Type mismatches

## 🧪 **Test-Driven Development Framework**

### **Mandatory Development Workflow**

**EVERY code change must follow this exact process:**

#### 1. 🎫 GitHub Issue Management
- **🚫 NEVER start work without a GitHub Issue**
- Create detailed issue with acceptance criteria
- Link all commits and PRs to GitHub Issue ID

```bash
# Create issue first
gh issue create --title "feat: add new trading signal algorithm" --body "
## Description
Brief description of the feature

## Acceptance Criteria
- [ ] Criterion 1
- [ ] Criterion 2

## Definition of Done
- [ ] Tests written and passing
- [ ] Schema validation completed
- [ ] End-to-end validation successful
"
```

#### 2. 🌿 Git Branching Workflow
```bash
# Start from latest main
git checkout main && git pull origin main

# Create feature branch with Issue ID
git checkout -b issue-123/feature-description

# OR use GitHub CLI to create branch from issue
gh issue develop 123 --checkout
```

#### 3. 🗄️ Schema Validation FIRST
```bash
# Validate database schema before coding
python scripts/validate_schema.py --check-all
python scripts/run_dev.py test --test tests/unit/test_database_schema_validation.py

# Get current schema
python scripts/run_dev.py query --query "\d+ table_name"
```

#### 4. 🧪 Test-Driven Development (TDD)
```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should FAIL (proves test works)

# 2. Write minimal code to make test pass
# 3. Verify test passes
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should PASS

# 4. Run full test suite
python scripts/run_dev.py test
```

#### 5. 🔄 End-to-End Validation
**Features must complete entire pipeline:**
1. Generate real data using Docker containers
2. Store data in database with correct schema
3. API serves data to external clients
4. Frontend displays data in browser
5. All integration tests pass

#### 6. 📋 Pull Request Process
```bash
# Push feature branch
git push origin issue-123/feature-description

# Create PR with comprehensive details
gh pr create --title "feat: description (closes #123)" --body "
## Summary
- Detailed description of changes
- Why this change was needed

## Related Issue
Closes #123

## Testing
- ✅ Schema validation tests pass
- ✅ All unit tests pass
- ✅ All integration tests pass
- ✅ End-to-end functionality verified

## Verification Checklist
- [x] Tests written first (TDD followed)
- [x] Schema validation completed
- [x] Integration tests pass
- [x] External access tested
- [x] No breaking changes
"
```

---

## 🧪 **Testing Classification System**

### Unit Tests (Isolated, Fast)
```bash
# Pure function tests, mocked dependencies
PYTHONPATH=src pytest tests/unit/ -v --tb=short

# Schema validation tests
PYTHONPATH=src pytest tests/unit/test_database_schema_validation.py -v
```

### Integration Tests (Real Dependencies)
```bash
# Database connectivity, services, APIs
PYTHONPATH=src pytest tests/integration/ -v --tb=short

# Test categories:
# - Database connectivity tests
# - Cross-service communication tests
# - End-to-end API tests
```

### System Tests (Full Environment)
```bash
# Complete workflows in deployed environment
PYTHONPATH=src pytest tests/system/ -v --tb=short

# Test categories:
# - Complete workflow tests
# - Performance tests
# - Load tests
# - Deployment verification tests
```

---

## 🚨 **Critical Testing Standards & Practices**

### **⚡ MANDATORY: Thorough Testing Over Superficial Validation**

**CRITICAL PRINCIPLE: Test actual computed values, not just structural relationships.**

#### ❌ **Superficial Testing (DANGEROUS)**
```python
# BAD: Only checking relationships
def test_ohlc_data():
    result = get_market_data()
    assert result['high'] >= result['low']  # Structural only
    assert result['close'] > 0  # Basic validation
    # ❌ PROBLEM: Passes with ANY structurally valid data
```

#### ✅ **Thorough Value Testing (REQUIRED)**
```python
# GOOD: Checking exact computed values
def test_ohlc_data():
    # Use deterministic test data with known outcomes
    test_data = create_deterministic_minute_bars()
    result = aggregate_to_5m(test_data)

    # Validate EXACT computed values with mathematical precision
    assert abs(result['open'] - 100.25) < 0.01
    assert abs(result['high'] - 102.75) < 0.01
    assert abs(result['close'] - 101.50) < 0.01
    # ✅ RESULT: Catches real aggregation bugs
```

### **🎯 Lead vs Lag Price Testing Example**

**The user correctly identified that both `get_lead_prices` and `get_lag_prices` call the same `get_ohlcv_data` method with the same `cur_datetime`. This required comprehensive validation that they return DIFFERENT data.**

#### Critical Test Requirements:
```python
def test_lead_vs_lag_return_different_data():
    # SAME reference point
    reference_datetime = datetime(2025, 9, 6, 14, 30, 0)

    # Different directions should yield different data
    lag_result = get_lag_prices(ref_datetime, periods=5)    # backward
    lead_result = get_lead_prices(ref_datetime, periods=5)   # forward

    # CRITICAL: Must return different values (different time periods)
    lag_closes = lag_result['close'].tolist()
    lead_closes = lead_result['close'].tolist()

    assert lag_closes != lead_closes, "MUST return different time periods"
    assert avg(lead_closes) > avg(lag_closes), "Future > historical (with trend)"

    # Validate EXACT expected values from deterministic test data
    assert lag_closes == [98.50, 99.25, 100.10], "Historical data"
    assert lead_closes == [102.80, 103.45, 104.20], "Future data"
```

### **🔧 Interface Consistency Testing**

**CRITICAL: Async/Sync interface mismatches cause runtime failures.**

```python
# Base interface defines synchronous method
class MarketDataManager:
    def get_ohlcv_data(self, instrument_id, reference_datetime, periods):
        # Synchronous interface

# Implementation MUST match interface
class FileBasedMinuteMarketDataManager(MarketDataManager):
    def get_ohlcv_data(self, instrument_id, reference_datetime, periods):
        # ✅ CORRECT: Synchronous (matches base class)
        return asyncio.run(self._get_ohlcv_data_async(...))

    # ❌ WRONG: async def get_ohlcv_data(...)  # Interface violation!
```

**Test interface consistency:**
```python
def test_interface_consistency():
    # Verify UniverseStateManager can call without await
    manager = FileBasedMinuteMarketDataManager(env, path)
    universe_manager = UniverseStateManager(env)
    universe_manager.market_data_manager = manager

    # This must work synchronously (no await needed)
    result = universe_manager.get_lag_prices(instrument_id, datetime, periods=3)
    assert not result.empty
```

---

## 🚨 **Critical Testing Lessons Learned**

**Based on real failures and hard-learned lessons from UI interface testing incidents.**

### The Fundamental Testing Failure (2025-01-09)

**Incident**: Developer claimed to fix UI interface issues but made multiple critical testing errors:

#### What Went Wrong:
1. **❌ Tested Wrong Interface** - Fixed `/dataset-detail` when user was using `/eda`
2. **❌ API-Only Testing** - Used only `curl` commands, never saw actual UI behavior
3. **❌ No Browser Verification** - Never opened the interface in browser
4. **❌ False Success Claims** - Made statements like "All fixes verified" without real validation
5. **❌ Ignored User Feedback** - User said "nothing changed" but developer didn't investigate properly

### 🎯 **Mandatory Testing Protocol**

**EVERY UI/Interface change MUST follow this exact process:**

#### 1. Identify the ACTUAL Interface
```bash
# ❌ WRONG: Assume which interface user is using
# ✅ CORRECT: Find all possible interfaces
find /workspace -name "*.html" -type f | grep -E "(interface|dashboard|eda)"
docker exec container find /workspace -name "*.html" -type f

# ❌ WRONG: Fix first interface found
# ✅ CORRECT: Ask user which URL they're using
echo "Which URL are you accessing? http://localhost:4000/???"
```

#### 2. Browser-First Testing (MANDATORY)
```bash
# ❌ WRONG: Test only with curl
curl -s http://localhost:4000/interface

# ✅ CORRECT: Test in actual browser
# 1. Open browser to the EXACT URL user provided
# 2. Reproduce the EXACT issue user described
# 3. Verify the problem exists BEFORE claiming to fix it
```

#### 3. End-to-End Validation Protocol
```bash
# Create comprehensive test script for EVERY interface
# Example: scripts/test_eda_interface.py

# Test script MUST verify:
# 1. Interface accessibility
# 2. Actual DOM elements exist/removed
# 3. JavaScript functionality works
# 4. API endpoints return correct data
# 5. User workflows complete successfully
```

#### 4. Before/After Verification
```bash
# ❌ WRONG: Implement fix and assume it works
# ✅ CORRECT: Screenshot/document BEFORE state
# ✅ CORRECT: Implement fix
# ✅ CORRECT: Screenshot/document AFTER state
# ✅ CORRECT: Compare and verify specific changes
```

### 📋 **Testing Checklist (MANDATORY)**

#### Interface/UI Testing:
- [ ] **Identified Correct Interface** - Confirmed exact URL user is accessing
- [ ] **Reproduced Original Issue** - Saw the problem with my own eyes
- [ ] **Browser Tested** - Opened interface in actual browser
- [ ] **Element Verification** - Confirmed specific DOM elements added/removed
- [ ] **Functionality Testing** - Tested interactive features work
- [ ] **API Integration** - Verified APIs return proper data structures
- [ ] **User Workflow** - Completed full user workflow successfully
- [ ] **Automated Test Created** - Created test script to prevent regression
- [ ] **Before/After Documentation** - Clear evidence of what changed

#### Data/Algorithm Testing:
- [ ] **Exact Value Validation** - Test computed values, not just relationships
- [ ] **Deterministic Test Data** - Use predictable inputs with known outcomes
- [ ] **Edge Case Coverage** - Test boundary conditions and error cases
- [ ] **Mathematical Precision** - Validate aggregations within tolerance (±0.01)
- [ ] **Direction/Parameter Testing** - Verify different inputs yield different outputs
- [ ] **Interface Consistency** - Ensure async/sync signatures match base classes
- [ ] **Integration Testing** - Test with real dependencies, not just mocks
- [ ] **Performance Validation** - Verify operations complete in reasonable time

#### System Integration Testing:
- [ ] **End-to-End Workflow** - Complete user journey from start to finish
- [ ] **Database Schema Validation** - Confirm table/column names match code
- [ ] **External Service Integration** - APIs, file systems, message queues
- [ ] **Error Handling Coverage** - Test failure scenarios and recovery
- [ ] **Performance Under Load** - Test with realistic data volumes
- [ ] **Security Validation** - No secrets logged, proper authentication
- [ ] **Regression Prevention** - New tests prevent previously fixed bugs

---

## 📊 **Code Quality Standards**

### Infrastructure Best Practices
- **✅ Reuse existing patterns** - Check existing services first
- **✅ Use official Docker image** - `dragonflyer762/ats-genai:latest`
- **✅ Test external access** - Not just internal connectivity
- **❌ Don't install packages in containers** - Pre-installed in Docker image
- **❌ No embedded code in configurations** - Keep logic separate

### Quality Requirements
```bash
# Schema validation (before any DB code)
python scripts/validate_schema.py --check-all
pre-commit run schema-anti-patterns

# Security scanning
pre-commit run security-checks

# Performance testing
PYTHONPATH=src pytest tests/performance/ -v

# Documentation updates
# Update relevant docs for new features
```

### Testing Anti-Patterns to Avoid
- **❌ Superficial relationship testing** instead of exact value validation
- **❌ Testing with random data** that makes validation unpredictable
- **❌ Assuming methods work differently** without thorough validation
- **❌ Interface signature mismatches** (async implementation with sync base class)
- **❌ Testing only success paths** without error conditions
- **❌ Mock-only testing** without real integration verification
- **❌ Claiming functionality works** without user scenario validation

### Development Anti-Patterns to Avoid
- **❌ Using manual operations** for dev work
- **❌ Setting environment variables manually**
- **❌ Creating new deployment patterns** when existing work
- **❌ Testing only via limited tools**
- **❌ Claiming functionality works without comprehensive tests**
- **❌ Writing tests after code (violates TDD)**
- **❌ Skipping integration tests**
- **❌ Half-baked implementations**
- **❌ Using demo/mock data in dev/staging/prod**

---

## 🔧 **Development Environment Matrix**

| Environment | Tables | Purpose | Database Access |
|-------------|--------|---------|-----------------|
| **test** | `test_*` | Unit tests | Local PostgreSQL |
| **dev** | `dev_*` | Development | Docker PostgreSQL |
| **intg** | `intg_*` | Integration | Docker PostgreSQL |
| **prod** | `prod_*` | Production | Docker PostgreSQL |

### Testing Commands
```bash
# Local unit tests
export PYTHONPATH=src
uv run pytest tests/ -v

# Local dev using Docker PostgreSQL
python scripts/run_dev.py start --service postgres
export ENVIRONMENT=dev DB_HOST=localhost
uv run python src/script.py
```

---

## 🎯 **Success Criteria**

**Development workflow is complete when:**
- [ ] GitHub Issue created and linked
- [ ] Feature branch created from main
- [ ] Schema validation passes
- [ ] TDD cycle completed (failing test → code → passing test)
- [ ] Integration tests pass
- [ ] End-to-end validation completed
- [ ] External access tested
- [ ] PR reviewed and approved
- [ ] GitHub Issue automatically closed via PR merge

**You're testing correctly when:**
- You can reproduce user's exact issue before fixing it
- You test in the same environment/interface user is using
- You verify specific measurable changes (element counts, data types, etc.)
- You create automated tests that catch regressions
- You document what was actually observed, not what should happen
- **You validate computed values with mathematical precision**
- **You test that different inputs produce different outputs**
- **You verify interface contracts are correctly implemented**

**Testing is complete when:**
- User confirms the issue is resolved
- Automated tests prevent regression
- Other team members can verify the fix using your test instructions
- The solution works in the real user environment, not just development
- **Exact computed values match expected mathematical outcomes**
- **Edge cases and error conditions are covered**
- **System integration works with real dependencies**

---

## 💡 **Key Development Insights**

### UI/Interface Testing
1. **Browser is Truth** - If it doesn't work in browser, it doesn't work
2. **User Experience is Truth** - If user says it doesn't work, it doesn't work
3. **APIs ≠ UI** - API returning 200 doesn't mean UI displays correctly
4. **Files ≠ Function** - File existing doesn't mean code executes
5. **Tools Have Limits** - curl can't test JavaScript, DOM manipulation, or user workflows

### Algorithm/Data Testing
6. **Computed Values Matter** - Test exact mathematical outcomes, not just relationships
7. **Different Inputs Must Yield Different Outputs** - Validate that methods actually behave differently
8. **Interface Contracts Are Critical** - Async/sync mismatches cause runtime failures
9. **Deterministic Data Enables Validation** - Random test data makes thorough testing impossible
10. **Integration Reveals Interface Issues** - Mock-only testing misses critical bugs

### General Principles
11. **Real Data Reveals Truth** - Mock data hides production problems
12. **Tests First Prevent Issues** - TDD catches problems before they reach users
13. **Question Assumptions** - "Both methods call the same function with same parameters - do they really return different data?"
14. **Mathematical Precision Matters** - ±0.01 tolerance for financial calculations

**Remember**: The user is the ultimate test. If they say it doesn't work, it doesn't work—regardless of what your tests show. When they question your implementation logic, listen carefully—they might be identifying critical gaps in your validation.

---

**🔥 This workflow ensures reliable, verified solutions rather than assumptions that may fail in production environments.**