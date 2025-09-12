# Multi-Run Training Dataset Path Resolution Test Strategy

## 🐛 **The Bug That Escaped**

**Issue**: EDA showed "No sequence data available" because analytics service returned only 1 sequence instead of expected 3,216 sequences.

**Root Cause**: Analytics service was finding the first matching file across ALL runs instead of using the specific `run_id` from the database.

**Example**: Dataset 58 had `run_id=76` in database, but analytics service found AAPL files from `run_id=60` instead of the correct `run_id=76` files.

## 🧪 **Why Prior Tests Missed This Bug**

### **Critical Test Coverage Gaps**

1. **Single-Run Testing Bias**
   - Most tests created only one training run per test scenario
   - Bug only manifests when multiple runs with same symbols exist simultaneously
   - Cross-contamination impossible with single runs

2. **Missing Database-Filesystem Integration**
   - Tests validated API responses OR file creation, but not the critical linkage
   - No verification: "When database says `run_id=76`, does API use `run_id=76` files?"

3. **No Sequence Count Validation**
   - Tests checked for "some sequences returned" but not expected vs actual counts
   - Bug was quantitative: Expected 3,216, got 1

4. **Missing Run Isolation Testing**
   - No tests verified that each dataset gets data from its own run, not other runs
   - No cross-contamination detection

## 🛡️ **Comprehensive Test Suite Design**

### **1. Multi-Run Integration Tests**
**File**: `tests/integration/test_multi_run_training_dataset_path_resolution.py`

**Key Tests**:
- `test_multi_run_path_resolution_isolation()` - **CRITICAL**: Each dataset gets correct run's data
- `test_sequence_count_accuracy_validation()` - Validates API count matches training data count
- `test_database_filesystem_linkage_validation()` - Verifies database `run_id` → filesystem path linkage
- `test_wrong_run_detection()` - Regression test with "decoy" runs to catch cross-contamination

**Test Scenario Example**:
```python
# Create Run 60: AAPL with 250 sequences
# Create Run 76: AAPL + TSLA with 2000 sequences
# Dataset A points to run_id=60 → Should get 250 sequences
# Dataset B points to run_id=76 → Should get 2000 sequences
# CRITICAL: Dataset B must NOT get 250 sequences from run 60
```

### **2. Path Resolution Algorithm Unit Tests**
**File**: `tests/unit/test_training_dataset_path_resolution_algorithm.py`

**Key Tests**:
- `test_path_resolution_uses_correct_run_id()` - Unit test of path construction logic
- `test_run_specific_directory_search()` - Verifies run-first search strategy
- `test_fallback_search_when_run_directory_missing()` - Tests graceful degradation
- `test_symbol_matching_case_insensitive()` - Edge case validation
- `test_multiple_base_paths_search_order()` - Multi-path resolution testing

### **3. Sequence Count Validation Pipeline Tests**
**File**: `tests/integration/test_sequence_count_validation_pipeline.py`

**Key Tests**:
- `test_single_symbol_sequence_count_accuracy()` - **CRITICAL**: Exact sequence count matching
- `test_multi_symbol_sequence_count_distribution()` - Multiple symbols with different counts
- `test_sequence_count_with_missing_timeframes()` - Handles incomplete data gracefully
- `test_sequence_count_edge_cases()` - Empty, single, and large sequence counts
- `test_sequence_count_regression_protection()` - **Exact recreation of original bug scenario**

## 🎯 **Critical Test Cases That Would Have Caught The Bug**

### **Test Case 1: Multi-Run Cross-Contamination**
```python
# Setup
create_training_run(run_id=60, symbols=["AAPL"], sequences=250)
create_training_run(run_id=76, symbols=["AAPL", "TSLA"], sequences=3216)
create_dataset(dataset_id=58, run_id=76, expected_sequences=3216)

# Test
api_response = get_sequences(dataset_id=58)
actual_sequences = api_response['total_count']

# CRITICAL ASSERTION (would have failed with bug)
assert actual_sequences == 3216, f"Expected 3216, got {actual_sequences} (wrong run used)"
```

### **Test Case 2: Database-Filesystem Linkage Validation**
```python
# Verify database says run_id=76
db_run_id = query_database("SELECT run_id FROM datasets WHERE id = 58")
assert db_run_id == 76

# Verify API uses run_id=76 files
with mock_filesystem_access_tracking():
    api_response = get_sequences(dataset_id=58)
    accessed_paths = get_accessed_paths()

# CRITICAL ASSERTION (would have failed with bug)
assert any("/76/" in path for path in accessed_paths), "API didn't use run_id=76 directory"
```

### **Test Case 3: Exact Sequence Count Validation**
```python
# Known training data: 643 sequences × 5 timeframes × 2 symbols = 6430 total
create_known_training_data(sequences=6430)
create_dataset(expected_sequences=6430)

api_response = get_sequences()
actual_count = api_response['total_count']

# CRITICAL ASSERTION (would have caught 6430 → 1 bug)
assert actual_count == 6430, f"Sequence count mismatch: {actual_count} != 6430"
```

## 🚀 **Test Execution Strategy**

### **Continuous Integration Requirements**
```yaml
# Add to CI pipeline
- name: Multi-Run Path Resolution Tests
  run: |
    pytest tests/integration/test_multi_run_training_dataset_path_resolution.py -v
    pytest tests/unit/test_training_dataset_path_resolution_algorithm.py -v
    pytest tests/integration/test_sequence_count_validation_pipeline.py -v
```

### **Pre-Deployment Validation**
```bash
# Before any EDA deployment, run the regression test
pytest tests/integration/test_sequence_count_validation_pipeline.py::TestSequenceCountValidationPipeline::test_sequence_count_regression_protection -v
```

### **Development Workflow Integration**
```bash
# When making changes to analytics service path resolution:
pytest tests/unit/test_training_dataset_path_resolution_algorithm.py -v

# When making changes to training data generation:
pytest tests/integration/test_multi_run_training_dataset_path_resolution.py -v
```

## 📊 **Test Coverage Metrics**

### **Before (Prior Tests)**
- ❌ Single-run scenarios only
- ❌ No database-filesystem linkage validation
- ❌ No sequence count accuracy validation
- ❌ No cross-contamination detection
- **Result**: Critical path resolution bug escaped to production

### **After (New Test Suite)**
- ✅ Multi-run scenarios with cross-contamination detection
- ✅ Database-filesystem linkage validation at unit and integration levels
- ✅ Precise sequence count validation across entire pipeline
- ✅ Regression protection for specific bug scenario
- ✅ Path resolution algorithm unit tests
- ✅ Edge case and error condition coverage
- **Result**: Comprehensive coverage prevents similar bugs

## 🛠️ **Implementation Guidelines**

### **For New Features**
1. **Always test multi-run scenarios** - Never test with single runs only
2. **Validate sequence counts** - Always check expected vs actual counts
3. **Test database-filesystem integration** - Mock filesystem, verify path usage
4. **Include cross-contamination tests** - Verify run isolation

### **For Bug Fixes**
1. **Write failing test first** - Reproduce the exact bug scenario
2. **Test the fix** - Ensure test passes with fix applied
3. **Add regression protection** - Include test in CI suite permanently

### **For Code Reviews**
1. **Check for multi-run test coverage** - Reject PRs without multi-run tests
2. **Verify sequence count validation** - Ensure quantitative validation exists
3. **Confirm integration test coverage** - Unit tests alone are insufficient

## 🔍 **Future Test Enhancements**

### **Property-Based Testing**
```python
@given(
    run_ids=lists(integers(min_value=1, max_value=1000), min_size=2, max_size=5),
    symbols=lists(text(min_size=3, max_size=5), min_size=1, max_size=3),
    sequences_per_timeframe=integers(min_value=1, max_value=1000)
)
def test_path_resolution_property_based(run_ids, symbols, sequences_per_timeframe):
    """Property-based test: Any combination of runs/symbols should isolate correctly."""
```

### **Performance Testing**
```python
def test_path_resolution_performance_with_many_runs():
    """Test performance doesn't degrade with 100+ training runs."""
```

### **Chaos Testing**
```python
def test_path_resolution_with_filesystem_inconsistencies():
    """Test resilience to missing directories, corrupted files, permission issues."""
```

---

## 📝 **Summary**

This comprehensive test suite addresses the root causes that allowed the path resolution bug to escape:

1. **Multi-Run Scenarios**: Tests realistic production conditions with multiple runs
2. **Database-Filesystem Integration**: Validates the critical linkage between metadata and files
3. **Quantitative Validation**: Ensures sequence counts match across the entire pipeline
4. **Regression Protection**: Prevents the specific 3,216 → 1 bug from recurring

**Bottom Line**: These tests would have caught the bug during development, preventing the "No sequence data available" issue in production.