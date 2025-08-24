# Analytics Service Test Suite

This comprehensive test suite prevents the critical issues that were discovered during the analytics service implementation and fixes the following specific problems:

## 🚨 Issues These Tests Prevent

### 1. **Jobs Stats vs Jobs List Inconsistency**
- **Problem**: `/api/v1/jobs/stats` showed `{"total_jobs":1,"running_jobs":1}` but `/api/v1/jobs` showed `{"jobs":[],"total":0}`
- **Root Cause**: Wrong column names (`job_type` vs `run_type`, `started_at` vs `start_time`, `symbol` vs `symbols`)  
- **Test**: `test_jobs_stats_vs_jobs_list_consistency` - Catches this exact scenario

### 2. **Coverage Showing Zero When Data Exists**
- **Problem**: Database had 500K+ price records but `/api/v1/coverage/summary` showed `{"total_combinations":0}`
- **Root Cause**: Wrong timestamp column names (`created_at` vs `collected_at` for different tables)
- **Test**: `test_coverage_summary_has_data` - Validates coverage reflects actual data

### 3. **Database Connection Pool Issues**  
- **Problem**: "Error: cannot perform operation: another operation is in progress"
- **Root Cause**: Single database connection couldn't handle concurrent requests
- **Test**: `test_concurrent_endpoint_access` - Tests multiple simultaneous requests

### 4. **SQL Syntax Errors in UNION Queries**
- **Problem**: Complex UNION queries causing syntax errors
- **Root Cause**: Different table schemas causing incompatible UNION operations
- **Test**: `test_analytics_queries_syntax` - Validates all queries execute successfully

### 5. **Wrong Table Names**
- **Problem**: Code referencing `dev_training_datasets` (plural) when table is `dev_training_dataset` (singular)
- **Root Cause**: Inconsistent naming conventions
- **Test**: `test_dev_training_dataset_table_schema` - Validates correct table names

## 📁 Test Files

### `test_database_schema_validation.py`
- **Purpose**: Validates database schema matches code expectations
- **Key Tests**:
  - `test_dev_runs_table_schema` - Validates jobs table columns
  - `test_price_tables_timestamp_columns` - Validates timestamp columns across price tables
  - `test_analytics_queries_syntax` - Tests all SQL queries for syntax errors

### `test_analytics_endpoints.py` 
- **Purpose**: Integration tests for all analytics service endpoints
- **Key Tests**:
  - `test_job_stats_consistency` - Validates API data matches database
  - `test_coverage_summary_has_data` - Ensures coverage shows real data when it exists
  - `test_concurrent_endpoint_access` - Tests connection pool handles concurrent requests

### `test_query_performance.py`
- **Purpose**: Performance tests to prevent slow queries and timeouts
- **Key Tests**:
  - `test_coverage_queries_performance` - Ensures coverage queries complete quickly
  - `test_concurrent_query_performance` - Tests performance under load

### Regression Tests (`TestAnalyticsServiceRegression`)
- **Purpose**: Specific tests for bugs we previously encountered
- **Key Tests**:
  - `test_jobs_stats_vs_jobs_list_consistency` - The exact bug we had
  - `test_coverage_zero_when_data_exists` - Coverage showing 0 when data exists
  - `test_connection_pool_fixes_concurrent_errors` - Connection pool issues

## 🚀 Running the Tests

### Quick Test (Requires running analytics service)
```bash
# Start analytics service first
kubectl port-forward service/ats-analytics-service 3001:3000 -n ats-dev

# Run tests
ANALYTICS_SERVICE_URL=http://localhost:3001 pytest tests/analytics/ -v
```

### Full Test Suite
```bash
./tests/analytics/run_analytics_tests.sh
```

### Individual Test Categories
```bash
# Schema validation tests
PYTHONPATH=src pytest tests/analytics/test_database_schema_validation.py -v

# Endpoint integration tests  
ANALYTICS_SERVICE_URL=http://localhost:3001 pytest tests/analytics/test_analytics_endpoints.py -v

# Performance tests
PYTHONPATH=src pytest tests/analytics/test_query_performance.py -v
```

## 📊 Test Coverage

The test suite covers:

- ✅ **Database Schema Validation** - Prevents wrong table/column names
- ✅ **Query Syntax Validation** - Prevents SQL syntax errors
- ✅ **Data Consistency Tests** - Ensures API reflects database state
- ✅ **Connection Pool Tests** - Prevents concurrent request errors
- ✅ **Performance Tests** - Prevents slow queries and timeouts
- ✅ **Regression Tests** - Prevents specific bugs we encountered

## 🛠️ CI/CD Integration

Add to GitHub Actions workflow:
```yaml
- name: Run Analytics Tests
  run: |
    # Start analytics service (in CI environment)
    kubectl port-forward service/ats-analytics-service 3001:3000 -n ats-dev &
    sleep 10
    
    # Run test suite
    ./tests/analytics/run_analytics_tests.sh
```

## 📋 Test Requirements

```bash
pip install -r tests/analytics/requirements.txt
```

Required packages:
- `pytest>=7.0.0` - Test framework
- `pytest-asyncio>=0.21.0` - Async test support  
- `httpx>=0.24.0` - HTTP client for API tests
- `asyncpg>=0.28.0` - PostgreSQL async driver

## 🏗️ Architecture Benefits

This test suite provides:

1. **Early Detection** - Catches schema issues before deployment
2. **Regression Prevention** - Prevents re-introduction of fixed bugs
3. **Performance Monitoring** - Ensures queries remain fast
4. **Documentation** - Tests serve as executable documentation of expected behavior
5. **Confidence** - Safe refactoring and feature additions

## 🔧 Environment Variables

The tests use these environment variables:

```bash
# Database connection (for schema tests)
DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres  
DB_PASSWORD=dev_password
DB_NAME=dev_db

# Analytics service URL (for endpoint tests)
ANALYTICS_SERVICE_URL=http://localhost:3001
```

## 📈 Success Metrics

A successful test run indicates:
- ✅ All database schemas match code expectations
- ✅ All API endpoints return consistent data
- ✅ No connection pool or concurrency issues
- ✅ All queries complete within performance thresholds
- ✅ No regression of previously fixed bugs

This test suite ensures the analytics service remains reliable and prevents the specific issues that caused failures during implementation.