# Timestamp-Based Multi-Timeframe Navigation Test Suite

**Date**: September 7, 2025
**Purpose**: Comprehensive testing for the new timestamp-based multi-timeframe navigation architecture

## 🎯 Overview

This test suite validates the new timestamp-based multi-timeframe navigation system with comprehensive coverage across all testing levels:

### **Architecture Under Test**

**NEW: Timestamp-Based Navigation Flow**
1. **1-Hour Navigation**: `GET /api/v1/training-datasets/{id}/sequences/{seq}/1h?row_index={pos}`
2. **Multi-Timeframe Coordination**: `GET /api/v1/training-datasets/{id}/sequences/{seq}/multi-timeframe?timestamp={ts}`
3. **UI Navigation**: Next/Previous buttons drive 1-hour navigation → timestamp extraction → multi-timeframe chart updates

## 📋 Test Suite Structure

### 1. **Unit Tests** (`tests/unit/test_timestamp_based_navigation.py`)
**Purpose**: Isolated component testing with mocks
- ✅ 1-hour navigation functionality
- ✅ 21-bar context window calculation
- ✅ Multi-timeframe data retrieval by timestamp
- ✅ Timestamp synchronization logic
- ✅ Edge cases and error handling
- ✅ API response format compliance

**Run**: `python scripts/run_timestamp_navigation_tests.py --type unit`

### 2. **Integration Tests** (`tests/integration/test_timestamp_navigation_integration.py`)
**Purpose**: API endpoint testing with real analytics service
- ✅ 1-hour navigation endpoint integration
- ✅ Multi-timeframe by timestamp integration
- ✅ Complete navigation workflow (1h → multi-timeframe)
- ✅ Navigation edge cases with real data
- ✅ API performance integration

**Prerequisites**: Analytics service running (`python scripts/run_dev.py start --service analytics`)
**Run**: `python scripts/run_timestamp_navigation_tests.py --type integration`

### 3. **API Contract Tests** (`tests/integration/test_api_contract_timestamp_synchronization.py`)
**Purpose**: Validate API contracts and timestamp synchronization
- ✅ 1-hour navigation endpoint contract compliance
- ✅ Multi-timeframe endpoint contract compliance
- ✅ Timestamp synchronization consistency
- ✅ Navigation workflow integrity
- ✅ API response time contract
- ✅ Error handling contract

**Prerequisites**: Analytics service running
**Run**: `python scripts/run_timestamp_navigation_tests.py --type contract`

### 4. **Performance Tests** (`tests/performance/test_timestamp_navigation_performance.py`)
**Purpose**: Load testing and performance validation
- ✅ Single request performance
- ✅ Sequential navigation performance
- ✅ Concurrent navigation performance
- ✅ Memory usage during navigation
- ✅ Rapid navigation stress testing

**Prerequisites**: Analytics service running, `pip install aiohttp psutil`
**Run**: `python scripts/run_timestamp_navigation_tests.py --type performance`

### 5. **Playwright Tests** (`tests/browser_tests/test_timestamp_navigation_playwright.py`)
**Purpose**: End-to-end browser automation testing
- ✅ Complete navigation workflow in browser
- ✅ API contract compliance in browser context
- ✅ Navigation error handling in browser
- ✅ UI updates verification
- ✅ Multi-timeframe chart synchronization

**Prerequisites**: Analytics service running, Playwright installed
**Run**: `python scripts/run_timestamp_navigation_tests.py --type playwright`

## 🚀 Quick Start

### **Run All Tests**
```bash
# Comprehensive test suite (recommended)
python scripts/run_timestamp_navigation_tests.py

# Or run specific test types
python scripts/run_timestamp_navigation_tests.py --type unit
python scripts/run_timestamp_navigation_tests.py --type integration
python scripts/run_timestamp_navigation_tests.py --type contract
python scripts/run_timestamp_navigation_tests.py --type performance
python scripts/run_timestamp_navigation_tests.py --type playwright
```

### **Manual Test Execution**
```bash
# Unit tests (no service required)
PYTHONPATH=src python -m pytest tests/unit/test_timestamp_based_navigation.py -v

# Integration tests (service required)
python scripts/run_dev.py start --service analytics
PYTHONPATH=src python -m pytest tests/integration/test_timestamp_navigation_integration.py -v

# API contract tests
PYTHONPATH=src python -m pytest tests/integration/test_api_contract_timestamp_synchronization.py -v

# Performance tests
pip install aiohttp psutil
PYTHONPATH=src python -m pytest tests/performance/test_timestamp_navigation_performance.py -v

# Playwright tests
pip install playwright
playwright install chromium
PYTHONPATH=src python -m pytest tests/browser_tests/test_timestamp_navigation_playwright.py -v
```

## 📊 Test Coverage Areas

### **API Endpoint Testing**
- **1-Hour Navigation**: `/api/v1/training-datasets/{id}/sequences/{seq}/1h`
  - Row index parameter handling
  - 21-bar context window selection
  - Timestamp extraction and return
  - Edge case handling (negative index, beyond bounds)
- **Multi-Timeframe**: `/api/v1/training-datasets/{id}/sequences/{seq}/multi-timeframe`
  - Timestamp parameter handling
  - Multi-timeframe data retrieval (5m, 15m, 1d, 1w)
  - 21-bar synchronization around timestamp
  - Data consistency validation

### **Data Flow Validation**
- **Timestamp Synchronization**: Verify exact timestamp matching between endpoints
- **21-Bar Logic**: Validate 10 before + 1 current + 10 after logic across all timeframes
- **Table Display**: Confirm 1-hour data always shown in table
- **Chart Updates**: Verify multi-timeframe charts synchronized to 1-hour timestamp

### **Performance Requirements**
- **Response Time**: < 3s for 1-hour navigation, < 5s for multi-timeframe
- **Memory Usage**: < 100MB growth during navigation session
- **Concurrent Handling**: ≥ 70% success rate under 10 concurrent requests
- **Stress Testing**: Handle 20 rapid requests with ≥ 70% success rate

### **User Experience Testing**
- **Browser Navigation**: Next/Previous buttons work correctly
- **Visual Updates**: Table and charts update with different data after navigation
- **Error Handling**: Graceful degradation when data unavailable
- **Performance**: UI remains responsive during navigation

## 🎯 Success Criteria

### **✅ All Tests Must Pass**
1. **Unit Tests**: 100% pass rate (isolated functionality)
2. **Integration Tests**: 100% pass rate (API endpoints)
3. **Contract Tests**: 100% pass rate (API compliance)
4. **Performance Tests**: Meet all performance thresholds
5. **Playwright Tests**: End-to-end workflow successful

### **✅ Key Validations**
- **Timestamp Coordination**: 1-hour timestamp correctly drives multi-timeframe data
- **Data Accuracy**: Different positions return different data (no caching issues)
- **UI Responsiveness**: Navigation updates both table and charts
- **Error Resilience**: System handles edge cases gracefully
- **Performance**: Meets response time and memory requirements

## 🔧 Troubleshooting

### **Common Issues**

**Analytics Service Not Running**
```bash
# Start the service
python scripts/run_dev.py start --service analytics

# Check service health
curl http://localhost:3001/health
```

**No Training Datasets Available**
```bash
# Check available datasets
curl http://localhost:3001/api/v1/training-datasets

# Generate test data if needed (see CLAUDE.md)
```

**Playwright Installation Issues**
```bash
pip install playwright
playwright install chromium

# If still failing, try system-wide install
sudo playwright install chromium
```

**Performance Test Dependencies**
```bash
pip install aiohttp psutil
```

### **Test Debugging**

**Enable Verbose Output**
```bash
python scripts/run_timestamp_navigation_tests.py --type integration
# Uses -v --tb=short -s flags automatically for detailed output
```

**Run Individual Test Methods**
```bash
PYTHONPATH=src python -m pytest tests/unit/test_timestamp_based_navigation.py::TestTimestampBasedNavigation::test_1h_navigation_basic_functionality -v -s
```

**Check Service Logs**
```bash
python scripts/run_dev.py logs --service analytics
```

## 📈 Continuous Integration

### **Pre-Commit Testing**
```bash
# Run core tests before committing
python scripts/run_timestamp_navigation_tests.py --type unit
python scripts/run_timestamp_navigation_tests.py --type integration
```

### **Full Validation**
```bash
# Complete test suite for releases
python scripts/run_timestamp_navigation_tests.py
```

## 📚 Related Documentation

- **[PRD_ATS_EDA_Tool.md](projects/ats-eda-tool/PRD_ATS_EDA_Tool.md)**: Updated PRD with timestamp-based architecture
- **[CLAUDE.md](../CLAUDE.md)**: Development workflow and principles
- **[DEVELOPMENT_WORKFLOW.md](DEVELOPMENT_WORKFLOW.md)**: Complete development processes

---

**🎉 This comprehensive test suite ensures the timestamp-based multi-timeframe navigation system works correctly across all levels: unit, integration, contract, performance, and end-to-end user experience.**