# 💻 ATS Development Guide

**Complete development workflow, testing, CI/CD, and GitOps processes for the ATS platform.**

---

## 🚨 Mandatory Development Workflow

**EVERY code change must follow this exact process:**

### 1. 🎫 JIRA Issue Management
- **🚫 NEVER start work without a JIRA ticket**
- Create detailed ticket with acceptance criteria
- Link all commits and PRs to JIRA ticket ID

### 2. 🌿 Git Branching Workflow  
```bash
# Start from latest main
git checkout main && git pull origin main

# Create feature branch with JIRA ID
git checkout -b PGPT-1234/feature-description
```

### 3. 🗄️ Schema Validation FIRST
```bash
# Validate database schema before coding
python scripts/validate_schema.py --check-all
PYTHONPATH=src pytest tests/unit/test_database_schema_validation.py -v

# Get current schema via port forwarding
kubectl port-forward service/postgres 5433:5432 -n ats-dev &
PGPASSWORD=dev_password psql -h localhost -p 5433 -U postgres -d dev_db -c "\d+ table_name"
```

### 4. 🧪 Test-Driven Development (TDD)
```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should FAIL (proves test works)

# 2. Write minimal code to make test pass
# 3. Verify test passes  
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should PASS

# 4. Run full test suite
PYTHONPATH=src pytest tests/ -v
```

### 5. ☸️ Kubernetes-First Development
```bash
# Use scripts/run_dev.py for database operations
python3 scripts/run_dev.py psql --query "SELECT COUNT(*) FROM dev_daily_prices"

# Use kubectl for job management in ats-dev namespace
kubectl apply -f k8s/your-job.yaml -n ats-dev
kubectl logs job/job-name -n ats-dev
kubectl get jobs -n ats-dev

# NEVER run scripts locally for dev environment
# NEVER manually set environment variables
# ALWAYS use ats-dev namespace
```

### 6. 🔄 End-to-End Validation
**Features must complete entire pipeline:**
1. Generate real data in K8s cluster
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
git push origin PGPT-1234/feature-description

# Create PR with comprehensive details
gh pr create --title "feat: description [PGPT-1234]" --body "
## Summary
- Detailed description of changes
- Why this change was needed

## JIRA Ticket  
[PGPT-1234](https://company.atlassian.net/browse/PGPT-1234)

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
| **dev** | `dev_*` | Development | K8s TimescaleDB |
| **intg** | `intg_*` | Integration | K8s TimescaleDB |  
| **prod** | `prod_*` | Production | K8s TimescaleDB |

### Testing Commands
```bash
# Local unit tests
export PYTHONPATH=src
uv run pytest tests/ -v

# Local dev against K8s DB (port-forward)
kubectl port-forward -n ats-dev service/timescaledb 5432:5432
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
- [ ] JIRA ticket created and linked
- [ ] Feature branch created from main
- [ ] Schema validation passes
- [ ] TDD cycle completed (failing test → code → passing test)
- [ ] Integration tests pass
- [ ] End-to-end validation completed
- [ ] External access tested
- [ ] PR reviewed and approved
- [ ] JIRA ticket verified and closed

---

**🔥 This is a Kubernetes-first, test-driven development platform. Every change must be validated end-to-end in the actual K8s cluster with REAL DATA ONLY.**

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