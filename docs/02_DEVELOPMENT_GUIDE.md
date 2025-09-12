# 💻 ATS Development Guide

**Complete development workflow, TDD, testing, and CI/CD processes for the ATS platform.**

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

---

## 🚫 Critical Anti-Patterns

### **NO MOCK/SYNTHETIC DATA IN DEVELOPMENT ENVIRONMENTS**

**DEMO DATA HIDES REAL ISSUES AND CREATES FALSE CONFIDENCE**

**Correct Error Handling:**
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

**Environment Rules:**
- **Unit Tests**: Demo data acceptable for isolated testing
- **Development**: Real database required - fail if unavailable
- **Staging/Production**: Real data only - no fallbacks ever

### **SCHEMA VALIDATION PREVENTS DEV ENVIRONMENT ERRORS**

**SCHEMA ERRORS MUST BE CAUGHT BY UNIT TESTS - NEVER IN DEV ENVIRONMENT**

```bash
# 1. Validate schema compatibility before coding
python scripts/validate_schema.py --check-all

# 2. Run schema validation unit tests
PYTHONPATH=src pytest tests/unit/test_database_schema_validation.py -v

# 3. Check for anti-patterns
pre-commit run schema-anti-patterns
```

---

## 🐳 Docker-First Development

### Primary Development Interface
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

### Container Architecture
**Container Naming Pattern:**
- **DEV**: `ats-dev-{service}` (e.g., `ats-dev-analytics`, `ats-dev-postgres`)
- **INTG**: `ats-intg-{service}` (e.g., `ats-intg-analytics`, `ats-intg-postgres`)

**Port Architecture:**
| Service | DEV Environment | INTG Environment | Internal Port |
|---------|----------------|------------------|---------------|
| **Analytics** | `localhost:3000` | `localhost:4000` | `3000` |
| **PostgreSQL** | `localhost:3432` | `localhost:4432` | `5432` |
| **API** | `localhost:8000` | `localhost:8001` | `8000` |
| **Grafana** | `localhost:3001` | `localhost:4002` | `3000` |

---

## 🧪 Comprehensive Testing Strategy

### Test Hierarchy (MANDATORY)
```bash
# 1. Unit Tests - Fast, isolated
PYTHONPATH=src pytest tests/unit/ -v

# 2. Integration Tests - Database, API, services
PYTHONPATH=src pytest tests/integration/ -v

# 3. End-to-End Tests - Complete workflows  
PYTHONPATH=src pytest tests/e2e/ -v

# 4. Browser Tests - UI functionality (Playwright)
PYTHONPATH=src pytest tests/browser_tests/ -v

# 5. Performance Tests - Load, stress testing
PYTHONPATH=src pytest tests/performance/ -v
```

### Playwright UX Testing (MANDATORY for Frontend)
```bash
# Start services for testing
python scripts/run_dev.py start --service analytics
python scripts/run_dev.py start --service postgres

# Test complete user flows
PYTHONPATH=src python3 -m pytest tests/browser_tests/test_eda_playwright.py -v --tb=short

# Test specific features
PYTHONPATH=src python3 -m pytest tests/browser_tests/ -k "training_dataset" -v
```

### Quality Standards
```bash
# Code quality checks
pylint src/ --score=y
black src/ tests/ --check
mypy src/ --strict

# Security scanning
bandit -r src/ -f json

# Test coverage
pytest --cov=src --cov-report=term-missing --cov-fail-under=80
```

---

## 🔄 End-to-End Validation

**Features must complete entire pipeline:**

### 1. Data Generation
```bash
# Generate real data using Docker containers
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py

# Verify data quality
python scripts/run_dev.py query --query "SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM dev_daily_prices"
```

### 2. Database Storage  
```bash
# Store data with correct schema
python scripts/run_dev.py query --query "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='dev_daily_prices'"

# Validate constraints
python scripts/run_dev.py test --test tests/integration/test_database_constraints.py
```

### 3. API Integration
```bash
# API serves data to external clients
curl -s "http://localhost:3000/api/datasets" | jq
curl -s "http://localhost:8000/api/instruments" | jq

# API contract validation
PYTHONPATH=src pytest tests/integration/test_api_contracts.py -v
```

### 4. Frontend Display
```bash
# Frontend displays data in browser
open http://localhost:3000/eda

# UI testing with Playwright
PYTHONPATH=src pytest tests/browser_tests/test_eda_playwright.py -v
```

### 5. Integration Tests Pass
```bash
# All integration tests must pass
python scripts/run_dev.py test
PYTHONPATH=src pytest tests/integration/ tests/e2e/ -v
```

---

## 📋 Pull Request Process

### Create PR with Comprehensive Details
```bash
# Push feature branch
git push origin issue-123/feature-description

# Create PR with comprehensive details
gh pr create --title "feat: description (closes #123)" --body "
## Summary
- Detailed description of changes

## Related Issue
Closes #123

## Changes Made
- [ ] Database schema updates
- [ ] API endpoint changes
- [ ] Frontend modifications
- [ ] Test coverage added

## Testing Performed
- [ ] Unit tests passing
- [ ] Integration tests passing  
- [ ] Manual testing completed
- [ ] Performance benchmarks acceptable

## Deployment Notes
- [ ] Database migrations required
- [ ] Configuration changes needed
- [ ] Breaking changes (if any)

## Verification
- [ ] Code review completed
- [ ] All CI checks passing
- [ ] Documentation updated
"
```

### Pre-merge Checklist
```bash
# Required before merge approval
- [ ] All tests passing
- [ ] Code coverage >= 80%
- [ ] No security vulnerabilities
- [ ] Performance benchmarks met
- [ ] Documentation updated
- [ ] Schema validation passed
- [ ] End-to-end validation completed
```

---

## 🔧 Development Tools

### Code Quality Tools
```bash
# Auto-formatting
black src/ tests/
isort src/ tests/

# Type checking
mypy src/ --strict

# Linting
pylint src/ --score=y
flake8 src/

# Security
bandit -r src/
```

### Database Tools
```bash
# Schema management
python scripts/run_dev.py query --query "\dt"  # List tables
python scripts/run_dev.py query --query "\d+ table_name"  # Describe table

# Migration management
python scripts/run_migrations.py --environment dev
python scripts/run_migrations.py --validate
```

### Performance Profiling
```bash
# Profile Python code
python -m cProfile -o profile.stats your_script.py
snakeviz profile.stats

# Database query analysis
python scripts/run_dev.py query --query "EXPLAIN ANALYZE SELECT ..."

# Memory profiling
python -m memory_profiler your_script.py
```

---

## 🚨 Emergency Debugging

### Debug-First Methodology
**NO WORKAROUNDS WITHOUT ROOT CAUSE ANALYSIS**

1. **Gather Evidence**
```bash
# Check service status
python scripts/run_dev.py status
docker ps -a | grep -E "(ats|postgres)"

# Get detailed logs
python scripts/run_dev.py logs --service analytics
docker logs container_id --tail 100

# Check system resources
df -h && free -h && docker system df
```

2. **Read Documentation & Code**
```bash
# Check related documentation
grep -r "error_message" docs/
git log --grep="similar_issue" --oneline -10

# Examine source code
find src/ -name "*.py" -exec grep -l "error_pattern" {} \;
```

3. **Systematic Investigation**
```bash
# Test isolated components
python scripts/run_dev.py query --query "SELECT version()"
curl -f http://localhost:3000/health
docker exec container ps aux
```

4. **Document Findings**
```bash
git commit -m "fix: resolve specific issue

Root cause: Detailed explanation of underlying cause
Investigation: Steps taken to identify the problem
Solution: Specific fix implemented with reasoning
Verification: How the fix was validated

Refs: #issue_number"
```

---

## 📊 Success Metrics

### Code Quality Metrics
- Test coverage >= 80%
- Pylint score >= 8.0/10
- Zero security vulnerabilities
- Type coverage >= 90%

### Performance Metrics  
- API response time < 100ms (p95)
- Database query time < 50ms (p95)
- Memory usage < 512MB per service
- CPU usage < 50% under normal load

### Development Velocity
- Time from PR creation to merge < 24h
- Build time < 10 minutes
- Test suite execution < 5 minutes
- Deployment time < 15 minutes

---

**🎯 This development guide ensures high-quality, reliable code that follows enterprise standards while maintaining rapid development velocity.**