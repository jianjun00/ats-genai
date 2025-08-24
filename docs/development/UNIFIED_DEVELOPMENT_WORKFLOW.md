# Unified Development Workflow

## 🚨 Critical Development Rules

**EVERY code change must follow this exact workflow:**

1. **JIRA Issue Management** - Create JIRA ticket before any work
2. **Feature Branch Development** - NEVER commit directly to main
3. **Schema Validation First** - Validate database changes before coding
4. **Test-Driven Development (TDD)** - Write failing test first
5. **Kubernetes-First Development** - Use K8s for all operations  
6. **End-to-End Validation** - Verify complete pipelines work
7. **Integration Testing** - Test actual service startup
8. **Pull Request Review** - Always merge through PR after review
9. **Issue Verification** - Verify resolution before closing JIRA ticket

## 🎫 JIRA Issue Management - MANDATORY

### 🚫 NEVER Start Work Without JIRA Ticket

**All development work must be tracked through JIRA tickets.**

#### Step 1: Create JIRA Ticket First

**Before any code changes:**
1. Identify the issue (bug, feature, technical debt)
2. Go to JIRA project dashboard
3. Click "Create Issue"
4. Use appropriate template from `docs/templates/JIRA_TICKET_TEMPLATE.md`

**Required JIRA Ticket Information:**
- **Summary:** Clear, actionable title
- **Description:** Detailed problem/requirement description  
- **Acceptance Criteria:** Specific, testable conditions for completion
- **Priority:** Critical/High/Medium/Low based on impact
- **Components:** Affected system areas
- **Labels:** Categorization (bug, feature, technical-debt, etc.)

#### Step 2: Branch Naming with JIRA Integration

```bash
# ALWAYS include JIRA ticket number in branch name
git checkout -b PGPT-1234/fix-workflow-dependencies
git checkout -b PGPT-1235/feature-dataset-filtering  
git checkout -b PGPT-1236/docs-api-documentation
```

## 🌿 Git Branching Workflow - MANDATORY

### 🚫 NEVER Commit Directly to Main Branch

**All changes must go through feature branches and pull requests.**

#### Step 1: Create Feature Branch

```bash
# Always start from latest main
git checkout main
git pull origin main

# Create descriptive feature branch with JIRA ticket
git checkout -b PGPT-1234/fix-workflow-dependencies
```

#### Step 2: Follow TDD Process (See TDD Section Below)

#### Step 3: Push Feature Branch

```bash
# Push feature branch to remote
git push origin PGPT-1234/fix-workflow-dependencies
```

#### Step 4: Create Pull Request

```bash
# Create PR via GitHub CLI
gh pr create --title "fix: resolve workflow dependency issues [PGPT-1234]" --body "
## Summary
- Fix missing Python dependencies causing workflow failures
- Resolve gin configuration path issues in tests
- All workflow tests now pass successfully

## JIRA Ticket
- [PGPT-1234](https://your-company.atlassian.net/browse/PGPT-1234)

## Testing
- ✅ Schema validation tests pass
- ✅ All unit tests pass
- ✅ All integration tests pass  
- ✅ Kubernetes deployment verified

## Verification Checklist
- [x] Tests written first (TDD followed)
- [x] Schema validation completed
- [x] Integration tests pass
- [x] End-to-end functionality verified
- [x] No breaking changes
"
```

## 🗄️ Schema Validation - CRITICAL FIRST STEP

**Schema errors must NEVER reach dev environment - catch them in unit tests.**

### Before Writing Any Database Code

```bash
# 1. Get current database schema
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "\d+ table_name"

# 2. Verify table exists
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "\dt" | grep your_table

# 3. Check column names and types
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "\d+ your_table"
```

### Schema Validation Unit Tests (MANDATORY)

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

### Common Schema Errors to Avoid

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

## 🧪 Test-Driven Development (TDD) - MANDATORY

### TDD Workflow - Red, Green, Refactor

#### Step 1: Write Failing Test First (RED)

```bash
# 1. Create test file first
touch tests/integration/test_new_feature.py

# 2. Write failing test
cat > tests/integration/test_new_feature.py << 'EOF'
import pytest

async def test_dataset_filtering_functionality():
    """Test that dataset filtering works correctly"""
    # This should FAIL initially
    result = await filter_datasets_by_criteria({'size': 'large'})
    assert result is not None
    assert len(result) > 0
EOF

# 3. Run test - should FAIL
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should FAIL (proves test works)
```

#### Step 2: Write Minimal Implementation (GREEN)

```python
# Implement minimal code to make test pass
async def filter_datasets_by_criteria(criteria):
    """Minimal implementation to pass test"""
    return [{'id': 1, 'name': 'test'}]  # Minimal working code
```

```bash
# 4. Run test - should PASS
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should PASS
```

#### Step 3: Refactor and Improve (REFACTOR)

```python
# Now implement full functionality
async def filter_datasets_by_criteria(criteria):
    """Full implementation with proper database queries"""
    query = "SELECT * FROM dev_training_dataset WHERE"
    conditions = []
    
    if 'size' in criteria:
        conditions.append("file_size_mb > 100" if criteria['size'] == 'large' else "file_size_mb <= 100")
    
    if conditions:
        query += " " + " AND ".join(conditions)
    
    return await db.fetch(query)
```

#### Step 4: Run Full Test Suite

```bash
# Run all tests to ensure no regressions
PYTHONPATH=src pytest tests/integration/ -v --tb=short
PYTHONPATH=src pytest tests/unit/ -v
```

### TDD Rules

1. **🔴 RED**: Write failing test first
2. **🟢 GREEN**: Write minimal code to pass
3. **🔵 REFACTOR**: Improve code while keeping tests green
4. **🔄 REPEAT**: For each new feature/requirement

### Integration Testing Requirements

**Every feature must have end-to-end validation:**

```bash
# Test actual Kubernetes deployment
kubectl apply -f k8s/your-service-deployment.yaml
kubectl rollout status deployment/your-service -n ats-dev

# Test service endpoints
curl -s http://NODE_IP:NODE_PORT/your-endpoint

# Test database integration  
PYTHONPATH=src pytest tests/integration/test_database_integration.py -v
```

## 🚢 Kubernetes-First Development

### Use Dev CLI for All Operations

```bash
# ✅ CORRECT - Use run_dev
run_dev query "SELECT COUNT(*) FROM dev_daily_prices"
run_dev job price-unification --symbols AAPL,MSFT
run_dev logs job-name

# ❌ WRONG - Never use kubectl directly for dev work
kubectl exec -it pod-name -- bash
```

### K8s Script Organization and Best Practices

**Script Extraction and Management:**
- All K8s YAML files use **external scripts** only (no embedded code)
- Extracted scripts located in `scripts/k8s-extracted/`
- Each script is independently testable and maintainable
- YAML files focus purely on Kubernetes configuration

```bash
# Directory structure for K8s scripts
scripts/k8s-extracted/
├── app.py                    # Web application logic
├── environment.py            # Environment configuration
├── migration.sql            # Database migrations
├── training_*.py            # ML training scripts
├── backfill_*.py            # Data backfill operations
└── monitoring_*.py          # Monitoring and alerting
```

**Development Workflow for K8s Scripts:**
1. **Edit scripts** in `scripts/k8s-extracted/` directory
2. **Unit test scripts** independently before K8s deployment
3. **Validate YAML** references correct script paths
4. **Deploy and test** in K8s environment

```bash
# Testing extracted K8s scripts
PYTHONPATH=src python scripts/k8s-extracted/environment.py
python -m pytest scripts/k8s-extracted/ -v

# Validating YAML references
./scripts/validate_deployment.sh k8s/your-service.yaml
```

### Development Environment Rules

1. **Real Database Required**: Always connect to K8s database
2. **No Local Scripts**: Use K8s jobs for data processing  
3. **External Script References**: K8s YAML must reference external scripts only
4. **Script Testing**: Test extracted scripts independently before K8s deployment
5. **External Access Testing**: Test actual NodePort/LoadBalancer URLs
6. **Service Integration**: Verify services can communicate
7. **Docker Image**: All K8s deployments must use `dragonflyer762/ats-genai:latest` from Docker Hub

## 🚨 No Demo Data in Development

### Critical Rule: Real Data Only

- ✅ **Unit Tests**: Demo data acceptable for isolated testing
- ✅ **Development**: Real database required - fail if unavailable  
- ✅ **Staging/Production**: Real data only - no fallbacks

### Correct Error Handling

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

## 🔄 Complete Development Cycle

### Full Implementation Checklist

For every feature/bug fix:

- [ ] **JIRA Ticket Created** - Before any coding
- [ ] **Feature Branch Created** - With JIRA ticket number  
- [ ] **Schema Validation** - Database changes tested first
- [ ] **Failing Test Written** - TDD red phase
- [ ] **Minimal Implementation** - TDD green phase
- [ ] **Full Implementation** - TDD refactor phase
- [ ] **Integration Tests Pass** - End-to-end validation
- [ ] **Kubernetes Deployment** - Service runs in K8s
- [ ] **External Access Verified** - Real URL testing
- [ ] **Pull Request Created** - With proper description
- [ ] **Code Review Completed** - Team approval received
- [ ] **CI/CD Pipeline Passes** - All automated checks
- [ ] **Production Deployment** - If applicable
- [ ] **JIRA Ticket Closed** - After verification

### Post-Deployment Verification

```bash
# 1. Verify deployment health
kubectl get deployments -n ats-dev
kubectl rollout status deployment/your-service -n ats-dev

# 2. Test service endpoints
curl -s http://NODE_IP:NODE_PORT/health | jq

# 3. Check logs for errors
kubectl logs -n ats-dev -l app=your-service --tail=50

# 4. Run integration tests against live service
PYTHONPATH=src pytest tests/integration/test_live_service.py -v
```

## 🚨 Critical Anti-Patterns to Avoid

### Development Anti-Patterns
- ❌ Committing directly to main branch
- ❌ Starting work without JIRA ticket  
- ❌ Skipping schema validation
- ❌ Writing code before tests (non-TDD)
- ❌ Using demo data in dev/staging/production
- ❌ Testing only via port-forwarding
- ❌ Skipping integration tests
- ❌ **Moving to different approach when facing issues instead of debugging**
- ❌ **Creating duplicate code instead of reusing existing functionality**
- ❌ **Bypassing errors with hacky workarounds instead of fixing root causes**
- ❌ **Not following explicit instructions and directions strictly**

### Infrastructure Anti-Patterns  
- ❌ Using kubectl directly for dev operations
- ❌ Running scripts locally instead of in K8s
- ❌ Setting environment variables manually
- ❌ Creating new deployment patterns unnecessarily
- ❌ Embedding code directly in K8s YAML files
- ❌ Creating inline scripts in ConfigMaps or args sections

## 📋 Workflow Commands Reference

### Git Workflow
```bash
# Start new work
git checkout main && git pull origin main
git checkout -b PGPT-1234/feature-description

# Development cycle (repeat)
# 1. Write failing test
# 2. Implement feature
# 3. Run tests
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v

# Commit and push
git add . && git commit -m "descriptive message"
git push origin PGPT-1234/feature-description

# Create PR
gh pr create --title "Title [PGPT-1234]" --body "Description"
```

### Testing Commands
```bash
# Schema validation
python scripts/validate_schema.py --check-all

# Unit tests
PYTHONPATH=src pytest tests/unit/ -v

# Integration tests  
PYTHONPATH=src pytest tests/integration/ -v --tb=short

# K8s extracted script tests
python -m pytest scripts/k8s-extracted/ -v
PYTHONPATH=src python scripts/k8s-extracted/environment.py

# Full test suite
PYTHONPATH=src pytest tests/ -v
```

### Debugging Commands
```bash
# Check job logs when things fail
kubectl logs job/job-name -n ats-dev

# Check pod status and events
kubectl get pods -n ats-dev
kubectl describe pod pod-name -n ats-dev

# Check what migration names are implemented
grep -n 'elif.*migration_name' scripts/run_dev.py

# Debug database connections
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "SELECT version();"
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "\dt"

# Database setup and migration debugging
# 1. run_dev migrate only works with specific implemented migration names
# 2. Available migrations: "training-dataset", "enhanced-training-dataset"  
# 3. migration_manager.py requires: migrate --environment dev --db-url "postgresql://postgres:dev_password@postgres:5432/dev_db"
# 4. Docker image structure: working directory is /workspace, not /scripts
# 5. Migration dependencies: dev_runs table must exist before training-dataset migration
# 6. Always check logs for actual errors instead of assuming what went wrong

# 🚨 CRITICAL DEBUGGING PRINCIPLES
# 1. Always debug the current approach before switching to a different one
# 2. Read error messages carefully and address the root cause
# 3. Follow explicit instructions and directions strictly
# 4. Avoid creating duplicate code - reuse existing functionality
# 5. Fix actual problems instead of creating workarounds
# 6. When told to "fix X", focus on fixing X, not replacing it with Y
```

### Kubernetes Operations
```bash
# Database operations
run_dev query "SELECT version()"

# Job management
run_dev job price-unification --symbols AAPL
run_dev logs job-name

# Service verification
kubectl get services -n ats-dev
curl -s http://NODE_IP:NODE_PORT/health
```

---

**This workflow ensures:**
- ✅ Complete traceability through JIRA tickets
- ✅ Code quality through TDD and reviews  
- ✅ System reliability through real data testing
- ✅ Team coordination through feature branches
- ✅ Production readiness through K8s-first development