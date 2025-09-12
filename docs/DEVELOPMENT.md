# 💻 ATS Development Guide

**Complete development workflow, testing, and CI/CD processes for the ATS platform.**

---

## 🚨 Mandatory Development Workflow

**EVERY code change must follow this exact process:**

### 1. 🎫 GitHub Issue Management
```bash
# Create issue first
gh issue create --title "feat: add trading signal algorithm" --body "
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

# OR use GitHub CLI
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
# Use run_dev.py for ALL operations
python scripts/run_dev.py setup
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py
python scripts/run_dev.py start --service postgres
python scripts/run_dev.py status

# NEVER run docker commands directly
# NEVER manage container lifecycle manually
```

### 6. 🔄 End-to-End Validation
**Features must complete entire pipeline:**
1. Generate real data using Docker containers
2. Store data in database with correct schema
3. API serves data to external clients
4. Frontend displays data in browser
5. All integration tests pass

### 7. 📋 Pull Request Process
```bash
# Push feature branch
git push origin issue-123/feature-description

# Create PR with comprehensive details
gh pr create --title "feat: description (closes #123)" --body "
## Summary
- Detailed description of changes

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

# Schema validation tests
PYTHONPATH=src pytest tests/unit/test_database_schema_validation.py -v
```

#### Integration Tests (Real Dependencies)
```bash
# Database connectivity, services, APIs
PYTHONPATH=src pytest tests/integration/ -v --tb=short
```

#### System Tests (Full Environment)
```bash
# Complete workflows in deployed environment
PYTHONPATH=src pytest tests/system/ -v --tb=short
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

### GitOps Workflow

#### Development Cycle
```bash
# 1. Safety checks
./scripts/pre_deploy_check.sh

# 2. Make changes
vim k8s/service/deployment.yaml

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
- **✅ Reuse existing patterns** - Check existing services
- **✅ Use official Docker image** - `dragonflyer762/ats-genai:latest`
- **✅ Test external access** - Not just port-forwarding
- **❌ Don't install packages in containers** - Pre-installed in image
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
```

### Anti-Patterns to Avoid
- **❌ Using kubectl directly** for dev operations
- **❌ Setting environment variables manually**
- **❌ Creating new deployment patterns** when existing work
- **❌ Installing packages in containers**
- **❌ Testing only via port-forwarding**
- **❌ Claiming functionality works without tests**
- **❌ Writing tests after code (violates TDD)**
- **❌ Using demo/mock data in dev/staging/prod**

---

## 🆘 Troubleshooting

### Common Issues

#### "Schema validation failing"
```bash
# Check actual database schema
python scripts/run_dev.py query --query "\d+ table_name"

# Common fixes:
# - Wrong table names
# - Wrong column names
# - Missing tables or columns
```

#### "Tests failing in CI but pass locally"
```bash
# Ensure consistent environment
export PYTHONPATH=src
export ENVIRONMENT=test

# Run tests exactly as CI does
uv run pytest tests/integration/ -v --tb=short
```

### Debugging Commands
```bash
# System status
./scripts/deployment_status.sh

# External access info
./scripts/get_external_access.sh all

# Resource conflicts
python scripts/detect_conflicts.py
```

---

## 🚨 Critical Testing Requirements

### Mandatory Testing Protocol

**EVERY UI/Interface change MUST:**

#### 1. Identify the ACTUAL Interface
```bash
# ✅ CORRECT: Find all interfaces
find /workspace -name "*.html" -type f | grep -E "(interface|dashboard|eda)"

# ❌ WRONG: Assume which interface user is using
```

#### 2. Browser-First Testing (MANDATORY)
```bash
# ✅ CORRECT: Test in actual browser
# 1. Open browser to EXACT URL user provided
# 2. Reproduce EXACT issue user described
# 3. Verify problem exists BEFORE claiming to fix

# ❌ WRONG: Test only with curl
```

#### 3. Before/After Verification
```bash
# ✅ CORRECT: Document BEFORE state
# ✅ CORRECT: Implement fix
# ✅ CORRECT: Document AFTER state
# ✅ CORRECT: Compare and verify changes
```

### Testing Anti-Patterns (NEVER DO THIS)

#### Claims Without Verification
- **❌ WRONG**: "All fixes verified and working"
- **❌ WRONG**: "Interface is now functional"
- **✅ CORRECT**: "Tested in browser at http://localhost:4000/eda"
- **✅ CORRECT**: "Verified element counts changed from 5 to 0"

#### Tool-Limited Testing
- **❌ WRONG**: Only using curl to test UI
- **❌ WRONG**: Only testing API endpoints
- **✅ CORRECT**: Browser testing for UI changes
- **✅ CORRECT**: Full-stack integration testing

### Testing Checklist (MANDATORY)

Before claiming ANY interface fix is complete:
- [ ] **Identified Correct Interface** - Confirmed exact URL
- [ ] **Reproduced Original Issue** - Saw problem firsthand
- [ ] **Browser Tested** - Opened interface in browser
- [ ] **Element Verification** - Confirmed DOM changes
- [ ] **Functionality Testing** - Interactive features work
- [ ] **API Integration** - APIs return proper data structures
- [ ] **User Workflow** - Full workflow completed
- [ ] **Automated Test Created** - Regression prevention

---

## 📚 Critical Lessons Learned

### Training Data System Cleanup (2025-08-31)

**Issue**: Hardcoded symbols, synthetic data, infrastructure problems

**Root Causes**:
- Hardcoded symbols in `create_sample_job_config()`
- Default arguments hiding parameterization issues
- Missing environment attributes

**Solution Applied**:
```python
# ❌ WRONG: Hardcoded defaults hide issues
def create_sample_job_config(symbols: List[str] = None):
    if symbols is None:
        symbols = ['AAPL', 'MSFT', 'GOOGL']  # Hardcoded!

# ✅ CORRECT: Explicit requirements
def create_sample_job_config(symbols: List[str] = None):
    if symbols is None:
        raise ValueError("symbols parameter is required")
```

### Dependency Testing Pattern
```bash
# Test all dependencies are importable
python -c "import module_name"

# Test actual failing test case
pytest path/to/failing/test.py::TestClass::test_method -v

# Test full import chain
python -c "import the.full.chain.that.failed"
```

---

## 🎯 Success Criteria

**Development workflow complete when:**
- [ ] GitHub Issue created and linked
- [ ] Feature branch created from main
- [ ] Schema validation passes
- [ ] TDD cycle completed (failing test → code → passing test)
- [ ] Integration tests pass
- [ ] End-to-end validation completed
- [ ] External access tested
- [ ] PR reviewed and approved
- [ ] GitHub Issue automatically closed via PR merge

**You're following best practices when:**
- [ ] Using run_dev for all operations
- [ ] Writing failing tests before code changes
- [ ] Running schema validation before DB changes
- [ ] All integration tests passing
- [ ] Testing external access (not port-forwarding)
- [ ] Completing full end-to-end validation
- [ ] Reusing existing infrastructure patterns

---

**🔥 This is a Docker-first, test-driven development platform. Every change must be validated end-to-end with REAL DATA ONLY.**