# 💻 ATS Development Guide

**Complete development workflow, testing, CI/CD, and GitOps processes for the ATS platform.**

---

## 🚨 Mandatory Development Workflow

**EVERY code change must follow this exact process:**

### 1. 🎫 GitHub Issue Management
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

### 2. 🌿 Git Branching Workflow  
```bash
# Start from latest main
git checkout main && git pull origin main

# Create feature branch with Issue ID
git checkout -b issue-123/feature-description

# OR use GitHub CLI to create branch from issue
gh issue develop 123 --checkout
```

### 3. 🗄️ Schema Validation FIRST
```bash
# Validate database schema before coding
python scripts/validate_schema.py --check-all
python scripts/run_dev.py test --test tests/unit/test_database_schema_validation.py

# Get current schema
python scripts/run_dev.py query --query "\d+ table_name"
```

### 4. 🧪 Test-Driven Development (TDD)
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

### 5. 🐳 Docker-First Development
```bash
# Use python scripts/run_dev.py for ALL operations
python scripts/run_dev.py setup                    # Setup dev environment
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py
python scripts/run_dev.py start --service postgres # Start services
python scripts/run_dev.py status                   # Check running services

# NEVER run docker commands directly for dev work
# NEVER manage container lifecycle manually
# NEVER manually set environment variables
```

### 6. 🔄 End-to-End Validation
**Features must complete entire pipeline:**
1. Generate real data using Docker containers
2. Store data in database with correct schema
3. API serves data to external clients  
4. Frontend displays data in browser
5. All integration tests pass

### 7. 🔍 Integration Testing
```bash
# Test actual service startup
PYTHONPATH=src pytest tests/integration/ -v --tb=short

# Test K8s extracted scripts
python -m pytest scripts/k8s-extracted/ -v

# Test end-to-end workflows
PYTHONPATH=src pytest tests/system/ -v
```

### 8. 📋 Pull Request Process
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
- ✅ Kubernetes deployment verified
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

## 🧪 Testing Framework

### Test Classification System

#### Unit Tests (Isolated, Fast)
```bash
# Pure function tests, mocked dependencies
PYTHONPATH=src pytest tests/unit/ -v --tb=short

# K8s extracted script tests  
python -m pytest scripts/k8s-extracted/ -v

# Schema validation tests
PYTHONPATH=src pytest tests/unit/test_database_schema_validation.py -v
```

#### Integration Tests (Real Dependencies)
```bash
# Database connectivity, K8s services, APIs
PYTHONPATH=src pytest tests/integration/ -v --tb=short

# Test categories:
# - Database connectivity tests
# - Kubernetes service tests  
# - Cross-service communication tests
# - End-to-end API tests
```

#### System Tests (Full Environment)
```bash
# Complete workflows in deployed environment
PYTHONPATH=src pytest tests/system/ -v --tb=short

# Test categories:
# - Complete workflow tests
# - Performance tests
# - Load tests
# - Deployment verification tests
```

### Environment Matrix

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

# K8s job execution
kubectl apply -f k8s/job.yaml
kubectl logs -f job/job-name -n ats-dev
```

---

## 🚀 CI/CD and GitOps

### CI Pipeline (GitHub Actions)
**Triggers**: Push to any branch, PR creation

**Stages**:
1. **Unit Tests**: Fast isolated tests  
2. **Integration Tests**: Real dependencies
3. **Security Scanning**: Trivy, CodeQL, dependency audits
4. **Build**: Docker images with multi-arch support
5. **Publish**: Images to GitHub Container Registry

### GitOps Workflow (Option 2: Direct Service Replacement)

#### Development Cycle
```bash
# 1. Safety checks
./scripts/pre_deploy_check.sh

# 2. Make changes
vim k8s/service/deployment.yaml
vim scripts/k8s-extracted/app.py

# 3. Deploy and test
./scripts/dev_deploy.sh
./scripts/monitor_deployment.sh service-name
curl http://$(./scripts/get_external_access.sh service-name)/endpoint

# 4. Iterate quickly
git commit -m "feat: update service"
./scripts/dev_deploy.sh
```

#### Production Deployment
```bash
# 1. Final integration testing
./scripts/validate_deployment.sh k8s/**/*.yaml
PYTHONPATH=src pytest tests/system/ -v

# 2. Merge to main
gh pr merge --squash

# 3. ArgoCD handles deployment
# - Automatic sync to environments
# - Rolling updates with zero downtime
# - Health checks and rollback on failure
```

### Deployment Environments

| Environment | Purpose | Update Frequency | Auto-Sync |
|-------------|---------|------------------|-----------|
| **dev** | Development & Testing | Continuous | ✅ Yes |
| **intg** | Weekly Integration Testing | Weekly | ✅ Yes |
| **prod** | Live System | Monthly | ❌ Manual |

---

## 🔧 Development Best Practices

### Infrastructure Patterns
- **✅ Reuse existing patterns** - Check `kubectl get all -n ats-dev`
- **✅ Use official Docker image** - `dragonflyer762/ats-genai:latest`
- **✅ External script references** - K8s YAML → `scripts/k8s-extracted/`
- **✅ Test external access** - Not just port-forwarding
- **❌ Don't install packages in jobs** - Pre-installed in Docker image
- **❌ No embedded code in YAML** - Keep logic separate

### Code Quality Standards
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

### Anti-Patterns to Avoid
- **❌ Using kubectl directly** for dev operations
- **❌ Setting environment variables manually**
- **❌ Creating new deployment patterns** when existing work
- **❌ Installing packages in K8s containers**
- **❌ Embedding code in K8s YAML**
- **❌ Testing only via port-forwarding**
- **❌ Claiming functionality works without tests**
- **❌ Writing tests after code (violates TDD)**
- **❌ Skipping integration tests**
- **❌ Half-baked implementations**
- **❌ Using demo/mock data in dev/staging/prod**

---

## 🆘 Troubleshooting

### Common Development Issues

#### "Schema validation failing"
```bash
# Check actual database schema
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "\d+ table_name"

# Common fixes:
# - Wrong table names (dev_training_datasets vs dev_training_dataset)
# - Wrong column names (created_at vs creation_timestamp)
# - Missing tables or columns
```

#### "Tests failing in CI but pass locally"
```bash
# Ensure consistent environment
export PYTHONPATH=src
export ENVIRONMENT=test

# Run tests exactly as CI does
uv run pytest tests/integration/ -v --tb=short

# Check for:
# - Missing environment variables
# - Database connection issues
# - Dependency conflicts
```

#### "K8s job failing"
```bash
# Check job logs
kubectl logs -f job/job-name -n ats-dev

# Check resource allocation
kubectl describe job job-name -n ats-dev

# Common issues:
# - Resource limits too low
# - Missing secrets or config maps
# - Script path issues
# - Database connectivity problems
```

#### "ArgoCD sync issues"
```bash
# Force ArgoCD sync
./scripts/force_argocd_sync.sh

# Check ArgoCD status
kubectl get applications -n argocd

# Debug sync problems
kubectl describe application ats-dev -n argocd
```

### Debugging Commands
```bash
# System status
./scripts/deployment_status.sh

# External access info
./scripts/get_external_access.sh all

# Resource conflicts
python scripts/detect_k8s_conflicts.py k8s/

# Rollback deployment
./scripts/rollback_deployment.sh service-name [k8s|git|argocd]
```

---

## 🎯 Success Criteria

**You're following best practices when:**
- [ ] Using run_dev for all K8s operations
- [ ] Writing failing tests before code changes
- [ ] Running schema validation before DB changes
- [ ] All integration tests passing
- [ ] Testing external access (not port-forwarding)
- [ ] Completing full end-to-end validation
- [ ] Reusing existing infrastructure patterns
- [ ] Keeping K8s YAML free of embedded code

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

---

## 📚 Critical Lessons Learned

### 🚨 **MAJOR: Training Data System Cleanup (2025-08-31)**

**Issue**: Training data generation system contained hardcoded symbols, synthetic data, and infrastructure problems that prevented clean, parameterized execution.

**Root Causes Identified:**
- **Hardcoded Symbols**: Default `symbols = ['AAPL', 'MSFT', 'GOOGL']` in `create_sample_job_config()` 
- **Hardcoded Argument Parser**: `--symbol` with `default='AAPL'` instead of `required=True`
- **Missing Environment Attribute**: `Environment` class missing `table_prefix` property
- **Dataset Naming Collisions**: Dataset IDs without run_id causing database duplicate key violations

**Solution Applied:**
```python
# ❌ WRONG: Hardcoded defaults hide parameterization issues
def create_sample_job_config(symbols: List[str] = None):
    if symbols is None:
        symbols = ['AAPL', 'MSFT', 'GOOGL']  # Hardcoded!

# ✅ CORRECT: Explicit requirements enforce proper usage  
def create_sample_job_config(symbols: List[str] = None):
    if symbols is None:
        raise ValueError("symbols parameter is required - no default symbols provided")

# ❌ WRONG: Optional arguments with defaults mask configuration issues
parser.add_argument('--symbol', type=str, default='AAPL')

# ✅ CORRECT: Required arguments ensure explicit symbol specification
parser.add_argument('--symbol', type=str, required=True)

# ✅ CORRECT: Unique dataset naming prevents database conflicts
dataset_id = f"dataset_{job_name}_run{run_id}_{timestamp}"
```

**Prevention Framework Implemented:**
- **Comprehensive Test Suite**: 9-test framework covering hardcoded symbols, synthetic data, parameter passing
- **Environment Infrastructure**: Added `table_prefix` property for database table resolution
- **Explicit Requirements**: Removed all default fallbacks that masked configuration issues

**Key Learning**: **Default values and fallbacks often hide real configuration problems.** Explicit requirements and failing fast expose issues early in development rather than runtime.

**Verification Results:**
```
✅ No Hardcoded Symbols: PASS
✅ No Synthetic Data: PASS  
✅ Dataset Naming with Run ID: PASS
✅ Symbol Parameter Passing: PASS
✅ Error Handling No Data: PASS (correctly fails when no data available)
```

### 🛡️ **Code Quality Anti-Patterns Identified**

**❌ DANGEROUS: Silent Fallbacks to Defaults**
```python
# This masks configuration issues and creates runtime surprises
symbols = config.get('symbols', ['AAPL', 'MSFT'])  # Hidden defaults
```

**✅ CORRECT: Explicit Requirements**
```python
# This forces proper configuration and fails fast
if not symbols:
    raise ValueError("symbols parameter is required - no defaults")
```

**❌ DANGEROUS: Mock Data in Non-Test Code**
```python
# This hides real data issues and creates false confidence
try:
    data = load_real_data()
except:
    data = generate_synthetic_data()  # Masks the real problem!
```

**✅ CORRECT: Fail Fast with Real Errors**
```python
# This exposes actual issues for proper resolution
data = load_real_data()  # Let it fail if data unavailable
if data.empty:
    raise ValueError("No real market data available")
```

### 🎯 **Development Best Practices Reinforced**

1. **Test-Driven Cleanup**: Comprehensive test suites expose hidden issues in existing code
2. **Explicit Over Implicit**: Remove defaults and fallbacks that mask real configuration problems  
3. **Infrastructure Integration**: Ensure all components (Environment, database, configuration) work together
4. **Real Data Only**: Never use synthetic/mock data outside of isolated unit tests
5. **Unique Identifiers**: Always include run_id or timestamps in dataset names to prevent collisions

---

**🔥 This is a Docker-first, test-driven development platform. Every change must be validated end-to-end with REAL DATA ONLY.**

#### When Fixing Missing Dependencies
1. **Identify the root cause** - trace the import chain to find where the missing dependency is used
2. **Add to requirements.txt** - include the specific version to ensure reproducibility
3. **Make imports optional** - wrap in try/catch blocks with graceful fallbacks when appropriate
4. **Test locally first** - verify the fix works in the local development environment
5. **Verify with actual CI/CD run** - wait for and confirm green GitHub Actions status

#### Comprehensive Dependency Testing
When fixing CI/CD dependency issues, test the complete scenario:
```bash
# Test all dependencies are importable
python -c "import module_name"

# Test the actual failing test case
pytest path/to/failing/test.py::TestClass::test_method -v

# Test the full import chain that caused the failure
python -c "import the.full.chain.that.failed"
```

#### GitHub Actions Dependency Resolution Pattern
Recent fixes have addressed:
- **PyTorch**: Made optional with graceful fallback to numpy/pandas
- **PyArrow**: Added `pyarrow==18.1.0` and made optional across modules
- **Protocol Buffers**: Added `protobuf==5.29.2` for Google protobuf support
- **ib_insync**: Added `ib_insync==0.9.86` for Interactive Brokers integration

### Verification Requirements
- **Local Testing**: All fixes must pass local test execution before pushing
- **CI/CD Monitoring**: Must wait for and observe green GitHub Actions status
- **Error Reproduction**: Reproduce the exact error scenario before claiming it's fixed
- **Comprehensive Coverage**: Test not just the immediate fix but the entire use case

### Documentation of Fixes
When documenting dependency fixes:
- ✅ "Fixed and verified locally with passing tests"
- ✅ "Confirmed working through GitHub Actions green status"
- ❌ "This should work now" (without verification)
- ❌ "The fix would resolve the issue" (without testing)

## Development Workflow

1. **Identify Issue** - Through actual error observation (logs, test failures, etc.)
2. **Root Cause Analysis** - Trace the complete chain causing the problem
3. **Implement Fix** - Make targeted changes with appropriate fallbacks
4. **Local Verification** - Test the fix in the same environment where issue occurred
5. **Deploy and Monitor** - Push changes and wait for CI/CD confirmation
6. **Document Results** - Record what was actually observed, not assumptions

This approach ensures reliable, verified solutions rather than assumptions that may fail in production environments.

---

## 🚨 Critical Testing Lessons Learned

**Based on real failures and hard-learned lessons from UI interface testing incidents.**

### The Fundamental Testing Failure (2025-01-09)

**Incident**: Developer claimed to fix UI interface issues but made multiple critical testing errors:

#### What Went Wrong:
1. **❌ Tested Wrong Interface** - Fixed `/dataset-detail` when user was using `/eda`
2. **❌ API-Only Testing** - Used only `curl` commands, never saw actual UI behavior
3. **❌ No Browser Verification** - Never opened the interface in browser
4. **❌ False Success Claims** - Made statements like "All fixes verified" without real validation
5. **❌ Ignored User Feedback** - User said "nothing changed" but developer didn't investigate properly

#### Root Cause Analysis:
- **Assumption-Based Development**: Assumed interface structure without investigation
- **Tool-Limited Testing**: Relied only on command-line tools instead of full-stack testing
- **Confirmation Bias**: Looked for evidence that fixes worked rather than testing if they actually worked

### 🎯 Mandatory Testing Protocol

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

### 🔬 Comprehensive Interface Testing Framework

#### HTML/DOM Testing
```python
def test_ui_elements_removed():
    """Test that unwanted elements are actually removed."""
    response = requests.get("http://localhost:4000/interface")
    html = response.text
    
    # ✅ CORRECT: Count occurrences to verify removal
    per_column_controls = html.count("per-column-selector")
    assert per_column_controls == 0, f"Found {per_column_controls} per-column controls"
    
    # ✅ CORRECT: Verify new elements exist  
    global_controls = html.count("global-control")
    assert global_controls > 0, "Global control not found"
```

#### JavaScript Functionality Testing
```python
def test_javascript_functions():
    """Test that JavaScript functions work correctly."""
    # Test API calls return proper data structure
    response = requests.post("http://localhost:4000/api/filter", 
                           json={"symbol": "TSLA"})
    data = response.json()
    
    # ✅ CORRECT: Check for undefined values that cause "X of undefined"
    assert data.get('total_count') != 'undefined', "total_count is undefined"
    assert isinstance(data.get('total_count'), int), "total_count should be integer"
```

#### User Workflow Testing
```python
def test_complete_user_workflow():
    """Test the complete user workflow from start to finish."""
    # 1. Access interface
    # 2. Select dataset
    # 3. Apply filters
    # 4. Verify results display correctly
    # 5. Test all interactive elements
```

### 🚫 Testing Anti-Patterns (NEVER DO THIS)

#### Claims Without Verification
```bash
# ❌ WRONG: "All fixes verified and working"
# ❌ WRONG: "Interface is now functional"  
# ❌ WRONG: "Changes deployed successfully"

# ✅ CORRECT: "Tested in browser at http://localhost:4000/eda"
# ✅ CORRECT: "Verified per-column controls count = 0"
# ✅ CORRECT: "Symbol filter now returns integer values instead of undefined"
```

#### Tool-Limited Testing
```bash
# ❌ WRONG: Only using curl to test UI
# ❌ WRONG: Only testing API endpoints
# ❌ WRONG: Only checking file existence

# ✅ CORRECT: Browser testing for UI changes
# ✅ CORRECT: Full-stack integration testing  
# ✅ CORRECT: User experience validation
```

#### Assumption-Based Fixes
```bash
# ❌ WRONG: Assume you know which interface user is using
# ❌ WRONG: Assume your fix addresses the root cause
# ❌ WRONG: Assume APIs work because they return 200

# ✅ CORRECT: Investigate and identify exact interface
# ✅ CORRECT: Reproduce user's exact issue first
# ✅ CORRECT: Validate API responses contain expected data
```

### 📋 Testing Checklist (MANDATORY)

Before claiming ANY interface fix is complete:

- [ ] **Identified Correct Interface** - Confirmed exact URL user is accessing
- [ ] **Reproduced Original Issue** - Saw the problem with my own eyes
- [ ] **Browser Tested** - Opened interface in actual browser
- [ ] **Element Verification** - Confirmed specific DOM elements added/removed
- [ ] **Functionality Testing** - Tested interactive features work
- [ ] **API Integration** - Verified APIs return proper data structures  
- [ ] **User Workflow** - Completed full user workflow successfully
- [ ] **Automated Test Created** - Created test script to prevent regression
- [ ] **Before/After Documentation** - Clear evidence of what changed

### 🎯 Success Criteria

**You're testing correctly when:**
- You can reproduce user's exact issue before fixing it
- You test in the same environment/interface user is using
- You verify specific measurable changes (element counts, data types, etc.)
- You create automated tests that catch regressions
- You document what was actually observed, not what should happen

**Testing is complete when:**
- User confirms the issue is resolved
- Automated tests prevent regression
- Other team members can verify the fix using your test instructions
- The solution works in the real user environment, not just development

### 💡 Key Insights

1. **Browser is Truth** - If it doesn't work in browser, it doesn't work
2. **User Experience is Truth** - If user says it doesn't work, it doesn't work
3. **APIs ≠ UI** - API returning 200 doesn't mean UI displays correctly
4. **Files ≠ Function** - File existing doesn't mean code executes
5. **Tools Have Limits** - curl can't test JavaScript, DOM manipulation, or user workflows

**Remember**: The user is the ultimate test. If they say it doesn't work, it doesn't work—regardless of what your tests show.