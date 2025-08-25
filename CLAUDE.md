# CLAUDE.md - ATS Platform Guide

This file provides focused guidance to Claude Code when working with the ATS fintech platform.

## 🚨 CRITICAL: Be concise about code

**ALWAYS read docs and code about current infra to find best way to reuse existing code:**

**ALWAYS have a document on a new script as to why it is needed and what it does:**

**ALWAYS find opportunities to refactor code to remove duplicate functionality and delete unused code:**

## 🚨 CRITICAL: Kubernetes-First Development

**Never use mock or fake data other than unit test:**

**ALWAYS USE run_test for unit test:**

**ALWAYS USE run_dev for dev environment:**

**Always research existing code or database tables or apps before writing new code:**

**Always refactor code to remove duplicate functionality:**

**Always use same external port for application deployment:**

**ALWAYS create unit test for new code and think hard about test coverage and then run manual test in dev before claiming task is completed:**

**ALWAYS USE KUBERNETES FOR DEV OPERATIONS:**

- ✅ **DEV Environment = Kubernetes (ats-dev namespace)**
- ✅ **Database = postgres service in K8s cluster**  
- ✅ **All operations = Use run_dev (NEVER kubectl directly)**
- ❌ **NEVER run scripts locally for dev environment**
- ❌ **NEVER manually set environment variables**



### Primary Interface: scripts/run_dev.py + kubectl

```bash
# Database operations via run_dev
python3 scripts/run_dev.py psql --query "SELECT COUNT(*) FROM dev_daily_prices"
python3 scripts/run_dev.py status
python3 scripts/run_dev.py logs --job job-name

# Kubernetes operations via kubectl in ats-dev namespace
kubectl apply -f k8s/your-job.yaml -n ats-dev
kubectl get all -n ats-dev
kubectl logs job/job-name -n ats-dev

# ✅ ALWAYS use ats-dev namespace
# ✅ Database: postgres service (host=postgres, port=5432)
```

## 📚 Consolidated Documentation Structure

**IMPORTANT: Documentation has been consolidated to eliminate 90% duplication. Always use the UNIFIED guides.**

### 🚀 **PRIMARY DOCUMENTATION** ⭐ **USE THESE FIRST**

#### **Core 3-File Structure (MANDATORY READING)**
- **[START_HERE.md](docs/START_HERE.md)** ⭐ **START HERE**
  - 15-minute setup, core concepts, role-specific quick actions, troubleshooting
- **[DEVELOPMENT.md](docs/DEVELOPMENT.md)** ⭐
  - Complete development workflow, TDD, schema validation, CI/CD, GitOps, testing
- **[DEPLOYMENT.md](docs/DEPLOYMENT.md)** ⭐
  - All deployment strategies, environments, monitoring, troubleshooting, rollback

#### **Component-Specific Documentation**
- **[Backend Platform](docs/backend-platform/)** - APIs, services, business logic
- **[Data Infrastructure](docs/data-infrastructure/)** - Data pipelines, storage, ETL
- **[ML Platform](docs/ml-platform/)** - Training, models, AI optimization
- **[Online Infrastructure](docs/online-infrastructure/)** - K8s, CI/CD, monitoring

### 📖 **COMPLETE NAVIGATION HUB**
- **[Documentation Hub](docs/README.md)** - Complete navigation with learning paths

### 🚀 **QUICK START PATHS**

#### **New Team Members**
1. **[START_HERE.md](docs/START_HERE.md)** - 15-minute setup and core concepts
2. **[Documentation Hub](docs/README.md)** - Complete navigation for deeper learning

#### **DevOps Engineers**  
1. **[DEPLOYMENT.md](docs/DEPLOYMENT.md)** - All deployment strategies and troubleshooting
2. **[DEVELOPMENT.md](docs/DEVELOPMENT.md)** - CI/CD and GitOps workflows

### 🔧 **OPERATIONAL SCRIPTS** (Ready to Use)

```bash
# Complete workflow scripts available:
./scripts/pre_deploy_check.sh           # Safety checks before deployment  
./scripts/dev_deploy.sh                 # Deploy with team coordination
./scripts/monitor_deployment.sh         # Real-time deployment monitoring
./scripts/rollback_deployment.sh        # Multiple rollback strategies
./scripts/deployment_status.sh          # Comprehensive system status
./scripts/force_argocd_sync.sh          # ArgoCD integration
./scripts/get_external_access.sh        # Service endpoint discovery
```

### 📋 **LEGACY DOCUMENTATION**
- **[Archive Directory](docs/archive/)** - Archived duplicate docs (reference only)
- **Note**: If you find conflicting information, **always follow the UNIFIED guides** marked with ⭐

## 🔥 Critical Development Rules

### Test-Driven Development (MANDATORY)
```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should FAIL (proves test works)

# 2. Implement minimal code to pass test
# (write your code in src/ or scripts/k8s-extracted/)

# 3. Verify test passes  
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should PASS

# 4. Test K8s extracted scripts (if applicable)
python -m pytest scripts/k8s-extracted/ -v

# 5. Integration testing
PYTHONPATH=src pytest tests/integration/ -v
```

### End-to-End Validation Required
**Every feature must be complete end-to-end:**
1. Generate real data in K8s cluster
2. Store data in database with correct schema
3. API serves data to external clients
4. Frontend displays data in browser
5. All integration tests pass

### Infrastructure Best Practices
- **Reuse existing patterns** - Check `kubectl get all -n ats-dev` first
- **Use official Docker image** - Always use `dragonflyer762/ats-genai:latest` from Docker Hub
- **Don't install packages in jobs** - Dependencies are pre-installed in Docker image
- **External script references** - K8s YAML files reference scripts in `scripts/k8s-extracted/`
- **No embedded code** - Keep application logic separate from K8s configuration
- **Test external access** - Not just port-forwarding
- **Environment is pre-configured** - Don't set variables manually

## 📋 Common Commands

### Development
```bash
# Testing
PYTHONPATH=src pytest tests/integration/ -v --tb=short
PYTHONPATH=src pytest tests/ -m database -v
python -m pytest scripts/k8s-extracted/ -v

# Database operations via run_dev
run_dev query "SELECT version()"
run_dev migrate price-unification
```

### Job Management
```bash
# Run jobs
run_dev job price-unification --symbols AAPL,MSFT
run_dev job enhanced-training --symbol TSLA --days-back 120

# Monitor jobs
run_dev list
run_dev logs job-name
run_dev status job-name
```

### External Access Testing
```bash
# Get external IP and port (not localhost)
kubectl get nodes -o wide
kubectl get service service-name -n ats-dev

# Test actual external URL
curl -s "http://NODE_IP:NODE_PORT/health" | jq
```

## 🚨 Critical Anti-Patterns to Avoid

**Infrastructure:**
- ❌ Using kubectl directly for dev operations
- ❌ Setting environment variables manually  
- ❌ Creating new deployment patterns when existing ones work
- ❌ Installing packages in Kubernetes job containers
- ❌ Embedding code in K8s YAML files (use scripts/k8s-extracted/)
- ❌ Testing only via port-forwarding (test external access)

**Development:**
- ❌ Claiming functionality works without tests
- ❌ Writing tests after code (TDD requires tests first)
- ❌ Skipping integration tests (they're mandatory)
- ❌ Not testing actual service startup
- ❌ Half-baked implementations (incomplete end-to-end)

## 🎯 Success Criteria

**You're following best practices when:**
- [ ] Using run_dev for all K8s operations
- [ ] Writing failing tests before code changes
- [ ] Testing K8s extracted scripts independently
- [ ] Running integration tests and seeing them pass
- [ ] Testing external access (not just port-forwarding)
- [ ] Completing full end-to-end validation
- [ ] Reusing existing infrastructure patterns
- [ ] Keeping K8s YAML free of embedded application code

## 🆘 Getting Help

- **Quick Issues**: [Debugging Guide](docs/development/DEBUGGING_GUIDE.md)
- **Role-Specific**: [Role Guides](docs/roles/)
- **Architecture Questions**: [System Architecture](docs/architecture/SYSTEM_ARCHITECTURE.md)
- **New Team Member**: [Quick Start](docs/onboarding/QUICK_START.md)

---

## Database Connection Info (Reference)

**Kubernetes (primary):**
- Host: `postgres` (service in ats-dev), Port: `5432`
- User: `postgres`, Password: `dev_password`, Database: `dev_db`
- Tables: `dev_*` prefixed (e.g., dev_daily_prices, dev_instruments)

**Port-forwarding (local testing only):**
- Host: `localhost`, Port: `5433`  
- User: `postgres`, Password: `dev_password`, Database: `dev_db`
- Command: `kubectl port-forward service/postgres 5433:5432 -n ats-dev`

---

## 🚨 **CRITICAL: SCHEMA VALIDATION PREVENTS DEV ENVIRONMENT ERRORS**

**SCHEMA ERRORS MUST BE CAUGHT BY UNIT TESTS - NEVER IN DEV ENVIRONMENT**

### Required Before Any Database Code

**EVERY database interaction must be validated before deployment:**

```bash
# 1. Validate schema compatibility before committing
python scripts/validate_schema.py --check-all

# 2. Run schema validation unit tests
PYTHONPATH=src pytest tests/unit/test_database_schema_validation.py -v

# 3. Check for anti-patterns
pre-commit run schema-anti-patterns
```

**Schema validation will catch:**
- ❌ Wrong table names (`dev_training_datasets` vs `dev_training_dataset`)
- ❌ Wrong column names (`created_at` vs `creation_timestamp`)  
- ❌ Missing tables or columns
- ❌ SQL syntax errors
- ❌ Type mismatches

**Example validation results:**
```
❌ 11 ERRORS FOUND:
  ❌ enhanced_dataset_visualization_platform_real_data.py:189 - Anti-pattern detected: created_at. Should be "creation_timestamp"
  ❌ enhanced_dataset_visualization_platform_real_data.py:190 - Anti-pattern detected: dev_training_datasets. Should be "dev_training_dataset" (singular)
  ❌ enhanced_dataset_visualization_platform_real_data.py:187 - Table 'dev_training_datasets' does not exist
  ❌ enhanced_dataset_visualization_platform_real_data.py:187 - SQL syntax error: relation "dev_training_datasets" does not exist
```

**CI/CD Integration:**
- Schema validation runs automatically in GitHub Actions
- Deployment blocked if schema validation fails
- Pre-commit hooks prevent bad code from being committed

## 🚨 **CRITICAL: NO DEMO DATA IN DEVELOPMENT ENVIRONMENTS**

**DEMO DATA HIDES REAL ISSUES AND CREATES FALSE CONFIDENCE**

- ❌ **NEVER use demo/mock data** in development, staging, or production environments
- ❌ **NEVER create fallbacks to demo data** when real data is unavailable
- ❌ **NEVER return 200 OK with fake data** when database queries fail
- ✅ **Demo data ONLY in unit tests** - isolated, controlled test scenarios
- ✅ **Fail fast and clearly** when real data/database is unavailable
- ✅ **Show actual errors** - connection failures, missing data, schema problems

**Why Demo Data Is Dangerous:**
- Hides database connection problems and query failures
- Masks data quality issues, missing values, and real-world edge cases
- Creates false performance metrics (demo data is always fast and perfect)
- Prevents detection of authentication, permission, and network issues
- Results in production surprises when real data behaves differently

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

**See [docs/DEVELOPMENT_WORKFLOW.md](docs/DEVELOPMENT_WORKFLOW.md) for complete guidelines.**

---

**📖 For comprehensive information, see the complete documentation structure at [docs/README.md](docs/README.md)**

*This is a Kubernetes-first, test-driven development platform. Every change must be validated end-to-end in the actual K8s cluster with REAL DATA ONLY.*