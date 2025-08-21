# CLAUDE.md - ATS Platform Guide

This file provides focused guidance to Claude Code when working with the ATS fintech platform.

## 🚨 CRITICAL: Kubernetes-First Development

**ALWAYS USE KUBERNETES FOR DEV OPERATIONS:**

- ✅ **DEV Environment = Kubernetes (ats-dev namespace)**
- ✅ **Database = postgres-simple service in K8s cluster**  
- ✅ **All operations = Use dev CLI (NEVER kubectl directly)**
- ❌ **NEVER run scripts locally for dev environment**
- ❌ **NEVER manually set environment variables**

### Primary Interface: Dev CLI

```bash
# Your primary interface - use for ALL operations
python scripts/dev_cli.py query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/dev_cli.py job price-unification --symbols AAPL,MSFT
python scripts/dev_cli.py logs job-name

# ❌ NEVER use kubectl directly for dev work
# ✅ ALWAYS use dev CLI
```

## 📚 Complete Documentation Structure

**For detailed information, navigate to organized documentation:**

### 🚀 New Team Members
- **[Quick Start (15 min)](docs/onboarding/QUICK_START.md)** - Get running immediately
- **[Development Setup](docs/onboarding/DEVELOPMENT_SETUP.md)** - Complete environment setup
- **[Architecture Overview](docs/onboarding/ARCHITECTURE_OVERVIEW.md)** - Understand the system

### 👥 Role-Specific Guides
- **[Product Manager](docs/roles/PRODUCT_MANAGER.md)** - Strategy, metrics, roadmaps
- **[Backend Engineer](docs/roles/BACKEND_ENGINEER.md)** - APIs, services, infrastructure
- **[Data Engineer](docs/roles/DATA_ENGINEER.md)** - Pipelines, processing, storage
- **[Model Developer](docs/roles/MODEL_DEVELOPER.md)** - ML training, evaluation
- **[Frontend Engineer](docs/roles/FRONTEND_ENGINEER.md)** - UI, dashboards, visualization
- **[Release Engineer](docs/roles/RELEASE_ENGINEER.md)** - CI/CD, deployment, monitoring
- **[Oncall Support](docs/roles/ONCALL_SUPPORT.md)** - Incident response, troubleshooting

### 🛠️ Development Process
- **[Development Workflow](docs/development/DEVELOPMENT_WORKFLOW.md)** - TDD, testing, validation
- **[Kubernetes Guide](docs/development/KUBERNETES_GUIDE.md)** - K8s-first development
- **[Testing Guide](docs/development/TESTING_GUIDE.md)** - Testing strategies
- **[Debugging Guide](docs/development/DEBUGGING_GUIDE.md)** - Common issues

### 🏗️ System Architecture  
- **[System Architecture](docs/architecture/SYSTEM_ARCHITECTURE.md)** - High-level design
- **[Database Design](docs/architecture/DATABASE_DESIGN.md)** - Schema, data modeling
- **[API Design](docs/architecture/API_DESIGN.md)** - REST specifications
- **[Infrastructure](docs/architecture/INFRASTRUCTURE.md)** - K8s, containers

### 🚀 Operations
- **[Deployment Guide](docs/operations/DEPLOYMENT_GUIDE.md)** - Production deployments
- **[Monitoring](docs/operations/MONITORING.md)** - Observability, alerting
- **[Troubleshooting](docs/operations/TROUBLESHOOTING.md)** - Operational issues
- **[Runbooks](docs/operations/RUNBOOKS.md)** - Emergency procedures

## 🔥 Critical Development Rules

### Test-Driven Development (MANDATORY)
```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should FAIL (proves test works)

# 2. Implement minimal code to pass test
# (write your code)

# 3. Verify test passes  
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Should PASS

# 4. Integration testing
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
- **Use base Docker images** - Don't install packages in jobs
- **Test external access** - Not just port-forwarding
- **Environment is pre-configured** - Don't set variables manually

## 📋 Common Commands

### Development
```bash
# Testing
PYTHONPATH=src pytest tests/integration/ -v --tb=short
PYTHONPATH=src pytest tests/ -m database -v

# Database operations via dev CLI
python scripts/dev_cli.py query "SELECT version()"
python scripts/dev_cli.py migrate price-unification
```

### Job Management
```bash
# Run jobs
python scripts/dev_cli.py job price-unification --symbols AAPL,MSFT
python scripts/dev_cli.py job enhanced-training --symbol TSLA --days-back 120

# Monitor jobs
python scripts/dev_cli.py list
python scripts/dev_cli.py logs job-name
python scripts/dev_cli.py status job-name
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
- ❌ Testing only via port-forwarding (test external access)

**Development:**
- ❌ Claiming functionality works without tests
- ❌ Writing tests after code (TDD requires tests first)
- ❌ Skipping integration tests (they're mandatory)
- ❌ Not testing actual service startup
- ❌ Half-baked implementations (incomplete end-to-end)

## 🎯 Success Criteria

**You're following best practices when:**
- [ ] Using dev CLI for all K8s operations
- [ ] Writing failing tests before code changes
- [ ] Running integration tests and seeing them pass
- [ ] Testing external access (not just port-forwarding)
- [ ] Completing full end-to-end validation
- [ ] Reusing existing infrastructure patterns

## 🆘 Getting Help

- **Quick Issues**: [Debugging Guide](docs/development/DEBUGGING_GUIDE.md)
- **Role-Specific**: [Role Guides](docs/roles/)
- **Architecture Questions**: [System Architecture](docs/architecture/SYSTEM_ARCHITECTURE.md)
- **New Team Member**: [Quick Start](docs/onboarding/QUICK_START.md)

---

## Database Connection Info (Reference)

**Kubernetes (primary):**
- Host: `postgres-simple`, Port: `5432`
- User: `postgres`, Password: `dev_password`, Database: `dev_db`

**Port-forwarding (local testing only):**
- Host: `localhost`, Port: `5433`  
- User: `postgres`, Password: `postgres`, Database: `dev_db`

---

**📖 For comprehensive information, see the complete documentation structure at [docs/README.md](docs/README.md)**

*This is a Kubernetes-first, test-driven development platform. Every change must be validated end-to-end in the actual K8s cluster.*