# CLAUDE.md - ATS Platform Guide

This file provides focused guidance to Claude Code when working with the ATS fintech platform.

## 🚨 **CRITICAL DEVELOPMENT PRINCIPLES** ⚡

### **🚫 NO MOCK/SYNTHETIC DATA IN NON-TEST CODE**
- **❌ NEVER use mock data, fake data, synthetic data, demo data** outside of unit tests
- **❌ NEVER create fallbacks to demo data** when real data is unavailable  
- **❌ NEVER return 200 OK with fake data** when database queries fail
- **✅ Demo data ONLY in unit tests** - isolated, controlled test scenarios
- **✅ Fail fast and clearly** when real data/database is unavailable
- **✅ Show actual errors** - connection failures, missing data, schema problems

**Why Mock Data Is Dangerous:**
- Hides database connection problems and query failures
- Masks data quality issues, missing values, and real-world edge cases
- Creates false performance metrics (demo data is always fast and perfect)
- Prevents detection of authentication, permission, and network issues
- Results in production surprises when real data behaves differently

### **🔄 ENHANCE EXISTING BEFORE CREATING NEW**
- **❌ NEVER create new files** without checking if existing files can be enhanced
- **❌ NEVER create new services** without checking if existing services can be extended
- **❌ NEVER add new containers/ports** without checking if existing infrastructure can handle it
- **✅ ALWAYS enhance existing services** - add features to current code
- **✅ ALWAYS reuse existing ports/endpoints** - extend current APIs
- **✅ ALWAYS consolidate functionality** - reduce complexity, don't add it

### **📁 MODIFY EXISTING FILES FIRST**
- **❌ NEVER create new scripts** when existing scripts can be modified
- **❌ NEVER duplicate functionality** in new files
- **✅ ALWAYS extend existing files** - add methods, enhance classes
- **✅ ALWAYS look for similar functionality** before writing new code
- **✅ ALWAYS refactor and consolidate** - remove duplication

## 🐳 **Docker-First Development**

**ALWAYS USE DOCKER FOR ALL OPERATIONS:**
- ✅ **DEV Environment = Docker containers on localhost**
- ✅ **GPU Support = Docker with --gpus all flag**  
- ✅ **Database = PostgreSQL container or localhost**
- ✅ **All operations = Use run_dev (handles Docker automatically)**
- ❌ **NEVER run complex setup manually**
- ❌ **NEVER manually manage container lifecycle**

### Primary Interface: run_dev

```bash
# Your primary interface - use for ALL operations
python scripts/run_dev.py setup                    # Setup dev environment
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py
python scripts/run_dev.py run --script scripts/training/train_model.py --gpu  # With GPU
python scripts/run_dev.py start --service postgres # Start database
python scripts/run_dev.py start --service analytics # Start analytics service
python scripts/run_dev.py status                   # Check running services
python scripts/run_dev.py test                     # Run tests

# ❌ NEVER run docker commands manually for dev work
# ✅ ALWAYS use python scripts/run_dev.py
```

## 🧪 **Test-Driven Development (MANDATORY)**

### MANDATORY sequence for ALL code changes:
```bash
# 1. Write failing test FIRST
touch tests/integration/test_new_feature.py
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should FAIL (proves test works)

# 2. Write minimal code to make test pass
# (write your code in src/)

# 3. Verify test passes  
python scripts/run_dev.py test --test tests/integration/test_new_feature.py
# ✅ Should PASS

# 4. Run all tests
python scripts/run_dev.py test

# 5. Integration testing with services
python scripts/run_dev.py setup  # Start required services
python scripts/run_dev.py test --test tests/integration/
```

## 📋 **Common Commands**

### Development Setup
```bash
# Setup complete development environment
python scripts/run_dev.py setup

# Testing
python scripts/run_dev.py test
python scripts/run_dev.py test --test tests/integration/
python scripts/run_dev.py test --test tests/unit/

# Database operations
python scripts/run_dev.py query --query "SELECT version()"
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_daily_prices"
```

### Service Management
```bash
# Start services
python scripts/run_dev.py start --service postgres
python scripts/run_dev.py start --service analytics
python scripts/run_dev.py start --service api

# Check running services
python scripts/run_dev.py status

# Stop services
python scripts/run_dev.py stop --service analytics
```

### Script Execution
```bash
# Run data generation scripts
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py

# Run ML training with GPU
python scripts/run_dev.py run --script scripts/training/train_model.py --gpu

# Check service logs
python scripts/run_dev.py logs --service analytics
```

## 🚨 **Critical Anti-Patterns to Avoid**

**Infrastructure:**
- ❌ Running docker commands directly for dev operations
- ❌ Setting environment variables manually  
- ❌ Creating new container patterns when existing ones work
- ❌ Installing packages manually in containers
- ❌ Running services without using run_dev
- ❌ Managing container lifecycle manually
- ❌ **Creating new services/ports when existing ones can be enhanced**
- ❌ **Adding new containers when existing infrastructure can handle the requirement**

**Development:**
- ❌ Claiming functionality works without tests
- ❌ Writing tests after code (TDD requires tests first)
- ❌ Skipping integration tests (they're mandatory)
- ❌ Not testing actual service startup with run_dev
- ❌ Half-baked implementations (incomplete end-to-end)
- ❌ **Using mock/synthetic data outside of unit tests**
- ❌ **Creating new files when existing files can be enhanced**

## 🎯 **Success Criteria**

**You're following best practices when:**
- [ ] Using run_dev for all development operations
- [ ] Writing failing tests before code changes
- [ ] Running tests with run_dev test command
- [ ] Running integration tests and seeing them pass
- [ ] Testing services with localhost access
- [ ] Completing full end-to-end validation
- [ ] Reusing existing Docker/service patterns
- [ ] Using GPU support when needed for ML workloads
- [ ] **Enhancing existing services instead of creating new ones**
- [ ] **Using real data only - no mock/synthetic data outside tests**
- [ ] **Modifying existing files instead of creating new files**
- [ ] **Consolidating functionality to reduce infrastructure complexity**

## 📚 **Detailed Documentation**

For comprehensive operational procedures, infrastructure details, and development workflows, see:

- **[DEVELOPMENT_WORKFLOW.md](docs/DEVELOPMENT_WORKFLOW.md)** - Complete development processes, TDD, CI/CD
- **[OPERATIONS.md](docs/OPERATIONS.md)** - Daily operations, monitoring, troubleshooting, cron jobs
- **[INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md)** - Database connections, Docker networking, service architecture
- **[START_HERE.md](docs/START_HERE.md)** - 15-minute setup and core concepts
- **[DEVELOPMENT.md](docs/DEVELOPMENT.md)** - Complete development guide and best practices

---

**🔥 This is a Docker-first, test-driven development platform. Every change must be validated end-to-end with REAL DATA ONLY.**