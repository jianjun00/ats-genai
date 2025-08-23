# Development Workflow Guidelines

## 🚨 CRITICAL: Schema Validation Before Development

**Schema errors must NEVER reach dev environment - catch them in unit tests.**

### Database Schema Validation Requirements

**EVERY database interaction must be validated by unit tests before deployment:**

- ❌ **NEVER deploy code with unvalidated schema assumptions**
- ❌ **NEVER discover table/column name errors in dev environment**  
- ❌ **NEVER assume database schema without verification**
- ✅ **ALWAYS run schema validation tests before committing**
- ✅ **ALWAYS verify table and column names exist** 
- ✅ **ALWAYS test SQL queries against actual schema**

### Required Schema Validation Process

#### 1. Before Writing Database Code
```bash
# Get current database schema
kubectl exec -n ats-dev deployment/postgres-simple -- psql -U postgres -d dev_db -c "\d+ table_name"

# Verify table exists
kubectl exec -n ats-dev deployment/postgres-simple -- psql -U postgres -d dev_db -c "\dt" | grep your_table

# Check column names and types
kubectl exec -n ats-dev deployment/postgres-simple -- psql -U postgres -d dev_db -c "\d+ your_table"
```

#### 2. Schema Validation Unit Tests (MANDATORY)
```python
# tests/unit/test_database_schema_validation.py
async def test_table_and_column_exist(self, db_connection):
    """Test that our code references existing tables and columns"""
    # Verify table exists
    table_exists = await db_connection.fetchval(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'dev_training_dataset')"
    )
    assert table_exists, "Table dev_training_dataset must exist"
    
    # Verify columns exist
    columns = await db_connection.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'dev_training_dataset'"
    )
    column_names = [row['column_name'] for row in columns]
    
    required_columns = ['dataset_name', 'creation_timestamp', 'file_size_mb']
    for column in required_columns:
        assert column in column_names, f"Column {column} must exist in dev_training_dataset"
```

#### 3. Pre-commit Validation (AUTOMATIC)
```bash
# Install pre-commit hooks
pip install pre-commit
pre-commit install

# Hooks automatically check for:
# - Incorrect table names (dev_training_datasets vs dev_training_dataset)  
# - Incorrect column names (created_at vs creation_timestamp)
# - SQL syntax errors
# - Schema anti-patterns
```

#### 4. CI/CD Schema Validation (AUTOMATIC)
- All schema validation tests run in CI/CD pipeline
- Deployment blocked if schema validation fails
- No code reaches dev without passing schema tests

### Common Schema Error Examples

#### ❌ Wrong Table Name
```python
# WRONG - will fail in dev
query = "SELECT * FROM dev_training_datasets"  # Plural - doesn't exist
```

#### ✅ Correct Table Name  
```python
# CORRECT - validated by unit tests
query = "SELECT * FROM dev_training_dataset"   # Singular - exists
```

#### ❌ Wrong Column Name
```python
# WRONG - will fail in dev  
query = "SELECT created_at FROM dev_training_dataset"  # Column doesn't exist
```

#### ✅ Correct Column Name
```python
# CORRECT - validated by unit tests
query = "SELECT creation_timestamp FROM dev_training_dataset"  # Column exists
```

### Schema Validation Tools

#### Local Schema Validation Script
```bash
# scripts/validate_schema.py
python scripts/validate_schema.py --check-all

# Validates:
# - All table references in code exist
# - All column references in code exist  
# - All SQL queries can be prepared
# - Type compatibility between Python and DB
```

#### CI/CD Integration
```yaml
# .github/workflows/schema-validation.yml
- name: Schema Validation
  run: |
    pytest tests/unit/test_database_schema_validation.py -v
    python scripts/validate_schema.py --strict
```

## 🚨 CRITICAL: No Demo Data in Development Environment

**NEVER use demo/mock data in development or production environments.**

### Why This Is Critical

Demo data in development environments **masks real issues** and creates false confidence:

- ❌ **Hidden Database Issues**: Demo data bypasses actual database queries, hiding connection problems, schema issues, or query failures
- ❌ **False Success Indicators**: APIs return 200 OK with demo data even when real data pipelines are broken
- ❌ **Masked Data Quality Problems**: Demo data is always "perfect" - real data has missing values, outliers, and quality issues
- ❌ **Performance Blind Spots**: Demo data generation is fast, hiding real database performance bottlenecks
- ❌ **Integration Test Failures**: Components that work with demo data may fail spectacularly with real data
- ❌ **Production Surprises**: Issues only surface when deploying to production with real data

### Correct Data Strategy by Environment

#### ✅ Unit Tests Only
```python
# CORRECT: Demo data only in isolated unit tests
def test_dataset_analysis_with_mock_data():
    mock_data = generate_demo_dataset()
    result = analyze_dataset(mock_data)
    assert result.quality_score > 0.8
```

#### ✅ Development Environment
```python
# CORRECT: Always use real database in dev
async def get_dataset_details(dataset_id: str):
    try:
        # Query real database
        dataset = await db.fetch_dataset(dataset_id)
        if not dataset:
            raise HTTPException(404, "Dataset not found")
        return process_real_dataset(dataset)
    except Exception as e:
        # Log the real error, don't mask with demo data
        logger.error(f"Database error: {e}")
        raise HTTPException(500, "Database connection failed")
```

#### ❌ Anti-Pattern: Demo Data Fallback
```python
# WRONG: This masks real issues
async def get_dataset_details(dataset_id: str):
    try:
        dataset = await db.fetch_dataset(dataset_id)
        return process_real_dataset(dataset)
    except Exception:
        # BAD: Returns demo data, hiding the real problem
        return generate_demo_dataset_response(dataset_id)
```

### Development Environment Rules

1. **Real Database Required**: Development environment MUST connect to actual database
2. **Fail Fast**: If database is unavailable, application should fail with clear error
3. **Real Data Testing**: Always test with actual data from your database
4. **No Fallbacks**: No demo data fallbacks in dev/staging/production
5. **Clear Error Messages**: When real data fails, show the actual error

### Testing Strategy

#### Unit Tests
- ✅ Use controlled mock/demo data
- ✅ Test individual functions in isolation
- ✅ Verify business logic with known inputs

#### Integration Tests  
- ✅ Use real database with test data
- ✅ Test actual API endpoints with real queries
- ✅ Verify end-to-end data flow

#### Development Testing
- ✅ Always use production-like data
- ✅ Test with real dataset IDs from your database
- ✅ Verify actual database connectivity

### Environment Configuration

```yaml
# Development - Real data only
development:
  database:
    host: postgres-simple.ats-dev.svc.cluster.local
    port: 5432
    database: dev_db
    # No fallback to demo data
  
# Testing - Controlled test data
testing:
  database:
    host: postgres-test
    database: test_db
    # Use real database with test fixtures
  
# Unit Tests - Mock data acceptable
unit_tests:
  use_mocks: true  # Only for isolated unit tests
```

### Real Issue Detection

With this approach, you'll catch real issues early:

- Database connection problems
- Missing indexes causing slow queries  
- Data quality issues in your actual datasets
- Schema mismatches between code and database
- Authentication/permission problems
- Network connectivity issues in Kubernetes

### When You See These Errors, They're GOOD

```bash
# These errors are BETTER than silent demo data:
❌ Database connection failed: connection timeout
❌ Dataset 'abc-123' not found in training_datasets table  
❌ Query timeout: SELECT took 30 seconds
❌ Permission denied: insufficient privileges for table access
❌ Schema mismatch: column 'new_feature' does not exist
```

**These errors reveal real problems that demo data would hide.**

## Implementation Checklist

When implementing new features:

- [ ] Unit tests use controlled mock data
- [ ] Integration tests use real database with test fixtures
- [ ] Development environment connects to actual database
- [ ] No demo data fallbacks in any environment
- [ ] Clear error handling with actual error messages
- [ ] Database connectivity verified before deployment
- [ ] Real data tested throughout development process

## Code Review Requirements

Before merging code:

- [ ] Verify no demo data fallbacks in non-test code
- [ ] Confirm error handling shows real errors
- [ ] Test with actual database connectivity
- [ ] Verify 404/500 errors for missing data
- [ ] Check that real data scenarios are tested

**Remember: Demo data should only exist in unit test files. Everywhere else should fail fast and clearly when real data is unavailable.**