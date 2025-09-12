# Regression Test Suite for Critical Issues

This comprehensive test suite prevents the recurrence of major issues that were identified and fixed during development of the ATS platform.

## 🚨 Critical Issues Covered

### 1. Tiingo End Date Misinterpretation
**Issue**: 9,834 active stocks were incorrectly marked as delisted because Tiingo's `endDate` field was misinterpreted as a delisting date instead of data availability date.

- **Impact**: 75% of stocks appeared delisted, breaking investment analysis
- **Root Cause**: Tiingo returns current date as `endDate` for active stocks
- **Fix**: Interpret recent `endDate` (within 7 days) as active, set `end_date = NULL`
- **Test Coverage**: `test_tiingo_end_date_interpretation.py`

### 2. Hardcoded API Keys Security Vulnerability
**Issue**: API keys were hardcoded throughout 18+ files in the codebase, creating security risks.

- **Impact**: Potential credential exposure in version control, logs, and documentation
- **Root Cause**: Direct use of API key strings instead of environment variables
- **Fix**: Replace with `os.getenv()` patterns and placeholder values
- **Test Coverage**: `test_hardcoded_api_keys_security.py`

### 3. Database Schema Compatibility Issues
**Issue**: Scripts expected different table schemas than what existed in the database.

- **Impact**: Runtime failures, data insertion errors, failed backfills
- **Root Cause**: Column name mismatches (e.g., `adj_close` vs `adjclose`)
- **Fix**: Validate and align schema expectations with database reality
- **Test Coverage**: `test_database_schema_compatibility.py`

## 🧪 Test Structure

```
tests/regression/
├── __init__.py                           # Package initialization & configuration
├── pytest.ini                           # Pytest configuration
├── README.md                            # This documentation
├── test_tiingo_end_date_interpretation.py  # Tiingo date logic tests
├── test_hardcoded_api_keys_security.py     # API key security tests
├── test_database_schema_compatibility.py   # Schema compatibility tests
└── test_regression_suite_runner.py         # Meta-tests for the suite itself
```

## 🚀 Running the Tests

### Quick Start
```bash
# Run all regression tests (fast mode)
python3 scripts/run_regression_tests.py --fast

# Run specific category
python3 scripts/run_regression_tests.py --category security

# Run with integration tests (requires database)
python3 scripts/run_regression_tests.py --integration
```

### Manual Pytest Commands
```bash
# All regression tests
pytest tests/regression/ -v

# Specific test file
pytest tests/regression/test_hardcoded_api_keys_security.py -v

# Specific test method
pytest tests/regression/test_tiingo_end_date_interpretation.py::TestTiingoEndDateInterpretation::test_major_stocks_are_active -v

# Fast tests only (no integration/slow tests)
pytest tests/regression/ -v -m "not slow and not integration"

# Integration tests only (requires database)
pytest tests/regression/ -v -m integration
```

### Test Categories by Markers
```bash
# Security tests only
pytest tests/regression/ -v -m security

# Schema tests only
pytest tests/regression/ -v -m schema

# Integration tests (require database connection)
pytest tests/regression/ -v -m integration

# Slow tests (may take longer)
pytest tests/regression/ -v -m slow
```

## ✅ Test Coverage Details

### Tiingo End Date Tests
- **Major Stock Validation**: Ensures AAPL, MSFT, GOOGL, etc. are active
- **Date Logic Testing**: Tests cutoff date logic (7-day threshold)
- **Batch Fix Validation**: Tests the population fix script works correctly
- **API Response Parsing**: Tests interpretation of Tiingo API responses
- **Active Percentage Check**: Validates >70% of stocks are active (realistic)

### API Key Security Tests
- **Hardcoded Key Detection**: Scans codebase for specific leaked keys
- **Environment Variable Usage**: Validates proper `os.getenv()` patterns
- **Test File Placeholders**: Ensures test files use placeholder keys
- **Documentation Safety**: Checks docs use placeholder values
- **Environment File Structure**: Validates proper .env file structure
- **Git History Protection**: Framework for checking git history

### Database Schema Tests
- **Table Structure Validation**: Ensures tables have expected columns
- **Column Name Compatibility**: Prevents `adj_close` vs `adjclose` issues
- **Primary Key Constraints**: Validates proper table relationships
- **Foreign Key Relationships**: Tests referential integrity
- **Data Type Compatibility**: Ensures compatible data types
- **Insert Statement Testing**: Validates actual database operations

### Meta-Tests (Test Suite Validation)
- **Test File Existence**: Ensures all expected test files exist
- **Test Structure**: Validates proper test class and method structure
- **Test Discoverability**: Ensures pytest can find all tests
- **Documentation Coverage**: Validates issues are properly documented
- **Prevention Mechanisms**: Tests that prevention systems are in place

## 🔧 Dependencies

### Required Python Packages
```bash
pytest>=6.0.0          # Core testing framework
asyncpg                 # PostgreSQL async driver (for integration tests)
```

### Required Environment Variables (for integration tests)
```bash
DB_HOST=postgres        # Database host
DB_PORT=5432           # Database port
DB_USER=postgres       # Database user
DB_PASSWORD=dev_password # Database password
DB_NAME=dev_db         # Database name
```

### Optional Dependencies
- **Docker**: For running tests in containers
- **PostgreSQL**: For full integration testing
- **Git**: For git history testing features

## 📋 When to Run These Tests

### Mandatory Test Runs
- **Before every deployment** - Prevents shipping regressions
- **After instrument population changes** - Validates date logic
- **After database schema changes** - Prevents compatibility issues
- **After API key management changes** - Ensures security

### Recommended Test Runs
- **During development** - Catch issues early
- **In CI/CD pipelines** - Automated prevention
- **After major refactoring** - Comprehensive validation
- **Before releases** - Final safety check

### Test Scheduling
```bash
# Daily automated runs
0 2 * * * /path/to/run_regression_tests.py --fast

# Pre-deployment runs
python3 scripts/run_regression_tests.py --integration

# Development runs
python3 scripts/run_regression_tests.py --category security --fast
```

## 🛡️ Prevention Mechanisms

### 1. Automated Testing
- Comprehensive test suite runs automatically
- Multiple test execution methods (pytest, custom runner)
- Fast and integration test categories
- Clear pass/fail reporting

### 2. CI/CD Integration
- Tests run before deployment
- Deployment blocked if tests fail
- Multiple test execution environments
- Clear failure reporting and logging

### 3. Documentation & Knowledge Management
- Comprehensive issue documentation
- Clear reproduction steps
- Fix explanations and rationale
- Prevention strategy documentation

### 4. Code Review Requirements
- Changes to critical paths require review
- Schema changes require validation
- API key changes require security review
- Test coverage required for new features

### 5. Monitoring & Alerting
- Production data quality monitoring
- Active stock percentage monitoring
- Security scanning for hardcoded secrets
- Schema compatibility validation

## 🎯 Success Criteria

### Test Passing Criteria
- **100% of regression tests pass** before deployment
- **No hardcoded API keys** detected in codebase
- **Major stocks (AAPL, MSFT, etc.) are active** in database
- **>70% of Tiingo instruments are active** (realistic percentage)
- **Database schema matches script expectations**

### Prevention Success Criteria
- **Zero regression incidents** of covered issues
- **Fast feedback loops** (tests run in <5 minutes)
- **Clear actionable failures** with specific fix guidance
- **Comprehensive coverage** of critical code paths
- **Maintainable test suite** that evolves with codebase

## 🔍 Troubleshooting

### Common Issues

**"ModuleNotFoundError: No module named 'asyncpg'"**
- Install asyncpg: `pip install asyncpg`
- Or run without integration tests: `--fast` flag

**"Database connection failed"**
- Ensure PostgreSQL is running
- Check environment variables are set
- Use `docker exec ats-dev-postgres psql -U postgres -d dev_db -c "SELECT 1"`

**"Unknown pytest.mark.integration"**
- This is just a warning, tests will still run
- Register markers in pytest.ini if desired

**"Tests are too slow"**
- Use `--fast` flag to skip integration tests
- Run specific test categories with `--category`
- Use `pytest -x` to stop on first failure

### Getting Help

1. **Check test output** - Most failures have specific error messages
2. **Run individual tests** - Isolate failing tests for debugging
3. **Check dependencies** - Ensure required packages are installed
4. **Validate environment** - Check database connectivity and env vars
5. **Review documentation** - This README and inline test documentation

## 📊 Metrics & Reporting

### Test Execution Metrics
- **Total tests**: 50+ comprehensive regression tests
- **Execution time**: <2 minutes for fast tests, <10 minutes with integration
- **Coverage**: 3 major critical issues, 18+ files validated
- **Success rate**: Target 100% pass rate before deployment

### Prevention Metrics
- **Issues prevented**: Track regression incidents (target: 0)
- **Response time**: Time from issue identification to test creation
- **Coverage growth**: Number of new tests added per issue
- **Maintenance effort**: Time spent maintaining test suite

---

## 📚 Additional Resources

- **Main Test Suite**: `/tests/` - Full application test suite
- **Integration Tests**: `/tests/integration/` - Broader integration testing
- **Development Docs**: `/docs/DEVELOPMENT.md` - Development workflow
- **Security Docs**: `/CLAUDE.md` - Security and API key management
- **Database Docs**: `/docs/` - Database schema documentation

---

*This regression test suite is a critical safety net for the ATS platform. Maintaining and evolving these tests is essential for preventing the recurrence of major issues that could impact data quality, security, and system reliability.*