# 💻 ATS Development Guide

**Complete development workflow, testing, and CI/CD processes for the ATS Docker-based platform.**

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
PYTHONPATH=src python3 src/validation/validate_schema.py --check-all
PYTHONPATH=src pytest tests/unit/test_database_schema_validation.py -v

# Get current schema
python3 scripts/run_dev.py query --query "\d+ table_name"
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
PYTHONPATH=src pytest tests/core/ tests/config/ tests/signals/test_indicator.py -v --tb=short --maxfail=10
```

### 5. 🐳 Docker-First Development
```bash
# Use python scripts/run_dev.py for ALL operations
python3 scripts/run_dev.py setup                    # Setup dev environment
python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
python3 scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py
python3 scripts/run_dev.py start --service postgres # Start services
python3 scripts/run_dev.py status                   # Check running services

# NEVER run docker commands directly for dev work
# NEVER manage container lifecycle manually
# NEVER manually set environment variables
```

### 6. 🔄 End-to-End Validation
**Features must complete entire pipeline:**
1. Generate real data using Docker containers
2. Store data in database with correct schema
3. API serves data via localhost services
4. All integration tests pass

### 7. 🚀 Integration Environment Testing
**CRITICAL: Test features in ATS-INTG environment:**
```bash
# ATS-INTG uses Docker Compose orchestration
docker-compose -f docker-compose.intg-jobs.yml up -d

# Verify integration deployment
curl -f http://localhost:3002/login      # Grafana monitoring
curl -f http://localhost:9091/-/ready    # Prometheus metrics
PGPASSWORD=intg_password pg_isready -h localhost -p 5434 -U postgres -d intg_db

# Stop integration environment
docker-compose -f docker-compose.intg-jobs.yml down
```

### 8. 🔍 Integration Testing
```bash
# Test actual service startup
PYTHONPATH=src pytest tests/integration/ -v --tb=short

# Test end-to-end workflows
PYTHONPATH=src pytest tests/unit/ -v --tb=short
```

### 9. 📋 Pull Request Process
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
- ✅ ATS-DEV environment deployment verified
- ✅ ATS-INTG environment deployment verified
- ✅ End-to-end functionality verified

## 🚀 CI/CD Integration
This PR will automatically trigger:
- ✅ Unit tests and schema validation
- ✅ Docker image build and push
- ✅ Deployment to ATS-INTG environment (TimescaleDB)
- ✅ Integration tests against live environment

## Verification Checklist
- [ ] Tests pass in both ATS-DEV and ATS-INTG environments
- [ ] Schema validation completed
- [ ] No breaking changes to existing functionality
- [ ] All database operations use correct table/column names
"
```

---

## 🎯 Development Environments

### **Environment Overview**

| Environment | Tables | Purpose | Database | Management |
|-------------|--------|---------|----------|------------|
| **ATS-DEV** | `dev_*` | Development | PostgreSQL 13 | `run_dev.py` script |
| **ATS-INTG** | `intg_*` | Integration Testing | PostgreSQL 13 | Docker Compose |

### **Database Connections**
```bash
# ATS-DEV Database Access
python3 scripts/run_dev.py query --query "SELECT version()"
docker exec ats-dev-postgres psql -U postgres -d dev_db

# ATS-INTG Database Access
PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db
```

---

## 🔧 Development Best Practices

### **Infrastructure Guidelines**
- **✅ Reuse existing patterns** - Check `python3 scripts/run_dev.py status` first
- **✅ Use official Docker image** - Always use `dragonflyer762/ats-genai:latest`
- **✅ Docker containers** - All development through `run_dev.py` interface
- **✅ Schema validation** - ALWAYS run before committing changes

### **Development Anti-Patterns**
- **❌ Using docker commands directly** for dev operations
- **❌ Skipping schema validation** before database changes
- **❌ Not writing tests first** (TDD is mandatory)
- **❌ Installing packages manually** in containers
- **❌ Using mock/demo data** in development environments

---

## 🐛 Common Development Issues

### **Docker Container Problems**
```bash
# Symptom: Container won't start or services unreachable
# Diagnosis:
docker ps -a | grep ats-dev
docker logs ats-dev-postgres --tail=50
docker logs ats-dev-analytics --tail=50

# Solutions:
python3 scripts/run_dev.py status          # Check service status
python3 scripts/run_dev.py stop --all      # Stop all services
python3 scripts/run_dev.py setup           # Restart environment
```

### **Database Connection Issues**
```bash
# Symptom: Applications can't connect to database
# Diagnosis:
docker exec ats-dev-postgres pg_isready -U postgres
python3 scripts/run_dev.py query --query "SELECT version()"

# Solutions:
docker restart ats-dev-postgres            # Restart database
python3 scripts/run_dev.py start --service postgres
```

### **Schema Validation Failures**
```bash
# Symptom: Schema validation errors in CI/CD
# Diagnosis:
PYTHONPATH=src python3 src/validation/validate_schema.py --check-all

# Solutions:
# Fix table/column names to match existing schema
# Add new migrations if schema changes are needed
# Update validation rules if intentional changes
```

---

## ✅ Development Checklist

**Before Committing:**
- [ ] Issue created and linked
- [ ] Tests written first (TDD)
- [ ] Schema validation passes
- [ ] All tests pass locally
- [ ] Services start correctly with `run_dev.py`
- [ ] Database operations use correct names
- [ ] No hardcoded paths or credentials
- [ ] Integration tests pass

**Before Merging:**
- [ ] PR created with comprehensive description
- [ ] ATS-INTG environment tested
- [ ] All CI/CD checks pass
- [ ] Code review completed
- [ ] No breaking changes introduced

---

**🔥 This is a Docker-first, test-driven development platform. Every change must be validated end-to-end in both ATS-DEV and ATS-INTG environments with REAL DATA ONLY.**