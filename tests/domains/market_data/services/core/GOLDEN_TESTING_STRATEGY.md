# UnifiedMarketDataManager Golden File Testing Strategy

## **🎯 ULTRA-COMPREHENSIVE TESTING APPROACH**

This directory implements a **sophisticated golden file testing strategy** for `UnifiedMarketDataManager` using:

1. **Real FirstRate minute bar data** (not mocks)
2. **Proto serialization** for deterministic comparison
3. **Golden file regression testing** with automated management
4. **Comprehensive test scenarios** covering edge cases

## **🏗️ ARCHITECTURE OVERVIEW**

```
Real MinuteBar Data → UnifiedMarketDataManager → Proto Serialization → Golden File Comparison
     ↓                        ↓                         ↓                      ↓
FirstRate Parquet     get_ohlcv() Output        Deterministic JSON      Pass/Fail + Update
```

### **Key Components:**

1. **Proto Schema** (`proto/market_data_test.proto`)
   - Deterministic OHLCV data representation
   - Micro-dollar precision for exact comparisons
   - Test metadata for traceability

2. **Test Data** (`test_data/`)
   - Real 2-day subset of AAPL/TSLA minute bars
   - Covers market open, intraday, close scenarios
   - Isolated from production data

3. **Golden Files** (`test_data/golden_files/`)
   - Expected test outputs in JSON format
   - Auto-generated on first run
   - Version-controlled for regression detection

4. **Test Suite** (`test_unified_market_data_manager_golden.py`)
   - Comprehensive scenarios testing all functionality
   - Automated golden file management
   - Clear pass/fail reporting

## **🧪 TEST SCENARIOS COVERED**

### **Core Functionality Tests:**

| Test | Purpose | Data | Validation |
|------|---------|------|------------|
| `test_get_ohlcv_1m_single_symbol_golden` | Basic 1-minute OHLCV retrieval | AAPL, 1 hour | Full OHLCV accuracy |
| `test_get_ohlcv_5m_aggregation_golden` | Timeframe aggregation | AAPL, 1.5 hours | Aggregation logic |
| `test_get_ohlcv_multiple_symbols_golden` | Multi-symbol handling | AAPL+TSLA, 1 hour | Cross-symbol consistency |
| `test_get_minute_ohlc_batch_compatibility_golden` | Legacy compatibility | AAPL, 30 minutes | Interface compatibility |

### **Edge Cases Covered:**
- Market open/close boundaries
- Timeframe aggregation accuracy (1m → 5m)
- Multiple symbol synchronization
- Data gaps and missing values
- Volume and VWAP calculations

## **🚀 USAGE INSTRUCTIONS**

### **Quick Start:**
```bash
# Run all tests (auto-setup if needed)
./run_golden_tests.sh

# Setup test data first
./run_golden_tests.sh --setup

# Update golden files when behavior changes
./run_golden_tests.sh --update-golden
```

### **Detailed Commands:**

**1️⃣ Initial Setup:**
```bash
cd tests/domains/market_data/services/core/
./run_golden_tests.sh --setup
```

**2️⃣ Run Tests:**
```bash
# Run with existing golden files
./run_golden_tests.sh

# Run with pytest directly  
PYTHONPATH=src python -m pytest test_unified_market_data_manager_golden.py -v
```

**3️⃣ Update Golden Files (when expected behavior changes):**
```bash
./run_golden_tests.sh --update-golden
```

**4️⃣ Check Status:**
```bash
./run_golden_tests.sh --status
```

**5️⃣ Clean Artifacts:**
```bash
./run_golden_tests.sh --clean
```

## **📊 GOLDEN FILE FORMAT**

Golden files use deterministic JSON format:

```json
{
  "test_name": "test_get_ohlcv_1m_single_symbol", 
  "timeframe": "1m",
  "symbols": ["AAPL"],
  "start_datetime": "2024-08-01T09:30:00",
  "end_datetime": "2024-08-01T10:30:00",
  "vendor": "firstrate",
  "data_points": [
    {
      "symbol": "AAPL",
      "timestamp_utc_micros": 1722513000000000,
      "open_micro_dollars": 150250000,     // $150.25 * 1,000,000
      "high_micro_dollars": 150750000,     // $150.75 * 1,000,000  
      "low_micro_dollars": 150000000,      // $150.00 * 1,000,000
      "close_micro_dollars": 150500000,    // $150.50 * 1,000,000
      "volume": 125000,
      "vwap_micro_dollars": 150375000      // $150.375 * 1,000,000
    }
  ],
  "test_data_hash": "abc123...",
  "generated_at_utc_micros": 1722513000000000
}
```

## **🗂️ ORGANIZED GOLDEN FILE STRUCTURE**

### **Directory Organization:**
```
test_data/golden_files/
└── test_unified_market_data_manager_golden/    # Per test file
    ├── test_get_ohlcv_1m_single_symbol_golden.json        # Per test method
    ├── test_get_ohlcv_5m_aggregation_golden.json
    ├── test_get_ohlcv_multiple_symbols_golden.json
    └── test_get_minute_ohlc_batch_compatibility_golden.json
```

### **Naming Convention:**
- **Test file subdirectory**: `{test_file_name_without_py}/`
- **Golden file naming**: `{test_method_name}.json`
- **Organized by test suite**: Easy to find and manage

## **🔧 DETERMINISTIC TESTING FEATURES**

### **Precision Control:**
- **Micro-dollar precision**: Eliminates floating-point comparison issues
- **Timestamp microseconds**: Exact time representation
- **Sorted output**: Consistent data ordering

### **Data Integrity:**
- **Test data checksums**: Validates input data hasn't changed
- **Hash verification**: Ensures test parameters match
- **Version tracking**: Links golden files to code versions

### **Automated Management:**
- **Auto-creation**: Creates golden files on first run
- **Regression detection**: Fails on unexpected changes
- **Easy updates**: Simple golden file regeneration
- **Organized structure**: Golden files grouped by test file

## **🚨 CRITICAL SUCCESS CRITERIA**

### **✅ PASSING TESTS INDICATE:**
- UnifiedMarketDataManager produces exact expected outputs
- All vendor adapters work correctly
- Timeframe aggregation is mathematically accurate
- Data serialization is deterministic
- No regressions from previous versions

### **❌ FAILING TESTS INDICATE:**
- Logic changes affecting output
- Data source changes
- Floating-point precision issues
- Implementation bugs
- Environmental differences

## **🔄 WHEN TO UPDATE GOLDEN FILES**

**Update golden files when:**
- ✅ **Intentional behavior changes** (new features, bug fixes)
- ✅ **Data source improvements** (better accuracy, new fields)
- ✅ **Performance optimizations** (same output, different path)

**DO NOT update golden files for:**
- ❌ **Failing tests due to bugs** (fix the bug instead)
- ❌ **Environmental issues** (fix the environment)
- ❌ **Random test failures** (investigate root cause)

## **🐛 DEBUGGING FAILED TESTS**

### **Common Issues:**

**1. Missing Test Data:**
```bash
# Re-setup test data
./run_golden_tests.sh --setup
```

**2. Golden File Mismatch:**
```bash
# Check what changed
git diff tests/domains/market_data/services/core/test_data/golden_files/

# If change is expected, update golden files
./run_golden_tests.sh --update-golden
```

**3. Import Errors:**
```bash
# Verify PYTHONPATH
export PYTHONPATH=src
python -c "from domains.market_data.services.core.unified_market_data_manager import UnifiedMarketDataManager"
```

**4. Data Source Issues:**
```bash
# Check FirstRate data availability
ls -la /mnt/d/ats-data/minute-bars/firstrate/A/AAPL/2024/08/
```

## **🎯 INTEGRATION WITH CI/CD**

### **Automated Testing:**
```yaml
# CI/CD pipeline integration
test_unified_market_data_manager:
  script:
    - cd tests/domains/market_data/services/core/
    - ./run_golden_tests.sh --setup
    - ./run_golden_tests.sh
  artifacts:
    when: on_failure
    paths:
      - tests/domains/market_data/services/core/test_data/golden_files/
```

### **Regression Protection:**
- Golden files committed to version control
- CI fails on unexpected changes
- Requires explicit golden file updates for changes
- Clear audit trail of behavioral changes

## **📈 BENEFITS OF THIS APPROACH**

### **Immediate Benefits:**
- **100% real data testing** (no mocks)
- **Exact regression detection** (deterministic comparisons)
- **Comprehensive coverage** (all major scenarios)
- **Easy maintenance** (automated golden file management)

### **Long-term Benefits:**
- **Confidence in changes** (know exactly what changed)
- **Fast debugging** (clear failure modes)
- **Documentation through tests** (golden files show expected behavior)
- **Regression prevention** (impossible to accidentally break functionality)

---

**🔥 This golden file testing strategy provides ULTRA-COMPREHENSIVE validation of UnifiedMarketDataManager with real data and deterministic comparisons, ensuring absolute confidence in correctness and preventing regressions.**