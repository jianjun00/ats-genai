# CLAUDE.md - ATS Platform Guide

This file provides focused guidance to Claude Code when working with the ATS fintech platform.

## 🚨 **CRITICAL DEVELOPMENT PRINCIPLES** ⚡

### **🔍 DEBUG-FIRST: NO WORKAROUNDS WITHOUT ROOT CAUSE ANALYSIS**
- **❌ NEVER use workarounds** without understanding the underlying issue
- **❌ NEVER restart services** without investigating logs first
- **❌ NEVER switch environments** when current environment has problems
- **❌ NEVER use manual SQL** when migration manager fails - fix migration manager
- **❌ NEVER create new infrastructure** when existing infrastructure is broken
- **✅ ALWAYS investigate logs** when commands fail
- **✅ ALWAYS understand WHY** before implementing solutions
- **✅ ALWAYS fix root causes** - not symptoms
- **✅ ALWAYS document findings** in issues/commits for future reference

**Root Cause Analysis Process:**
1. **Gather Evidence**: Check logs, service status, system resources
2. **Read Documentation**: Search docs/code for similar issues
3. **Test Hypotheses**: Form specific hypothesis, test systematically
4. **Implement Fix**: Target root cause, not symptoms
5. **Document**: Record findings in commits/issues for future reference

### **🚫 NO MOCK/SYNTHETIC DATA IN NON-TEST CODE**
- **❌ NEVER use mock data, fake data, synthetic data, demo data** outside of unit tests
- **❌ NEVER create fallbacks to demo data** when real data is unavailable
- **✅ Demo data ONLY in unit tests** - isolated, controlled test scenarios
- **✅ Fail fast and clearly** when real data/database is unavailable

**Why Mock Data Is Dangerous:**
- Hides database connection problems and query failures
- Masks data quality issues and real-world edge cases
- Creates false performance metrics
- Results in production surprises when real data behaves differently

### **🔄 ENHANCE EXISTING BEFORE CREATING NEW**
- **❌ NEVER create new files/services** without checking if existing can be enhanced
- **❌ NEVER duplicate functionality** in new files
- **✅ ALWAYS enhance existing services** - add features to current code
- **✅ ALWAYS consolidate functionality** - reduce complexity, don't add it

## 🐳 **Docker-Compose Architecture (2025-09-12 Migration)**

**CRITICAL: Complete Docker-Compose Migration Completed**
- **✅ MIGRATED**: From individual containers to orchestrated services
- **✅ FIXED**: API authentication (valid keys: POLYGON, TIINGO, EODHD)
- **✅ WORKING**: Analytics service with proper command paths
- **✅ OPERATIONAL**: Environment-specific compose files

### **🚨 USE DOCKER-COMPOSE FOR SERVICE MANAGEMENT**

```bash
# Environment-Specific Service Management
docker-compose -f docker-compose.dev.yml up -d      # Dev environment
docker-compose -f docker-compose.intg.yml up -d     # Integration environment
docker-compose -f docker-compose.monitoring.yml up -d # Monitoring stack

# Service Status & Health
docker-compose -f docker-compose.intg.yml ps
curl -f http://localhost:4000/health                # Analytics health check
```

### **Legacy run_dev Interface (Still Available)**

```bash
python scripts/run_dev.py setup                    # Setup dev environment
python scripts/run_dev.py start --service postgres # Start database
python scripts/run_dev.py start --service analytics # Start analytics service
python scripts/run_dev.py status                   # Check running services
python scripts/run_dev.py test                     # Run tests
```

## 🐳 **CRITICAL DEPLOYMENT ARCHITECTURE**

### **🚨 Docker Network & Services**

**ALL services MUST use `ats-network` for inter-service communication:**
- Create network: `docker network create ats-network`
- **Critical**: All containers must be on same network
- **Debug network issues**: `docker inspect <container> | grep NetworkMode`

### **🔌 Port Architecture**

| Service | DEV | INTG | Internal |
|---------|-----|------|----------|
| Analytics | :3000 | :4000 | :3000 |
| PostgreSQL | :5432 | :4432 | :5432 |
| API | :8000 | :8001 | :8000 |

### **📦 Container & Environment Setup**

**Container Naming:** `ats-{env}-{service}` (e.g., `ats-dev-postgres`, `ats-intg-analytics`)

**Critical Volume Mounts:**
```bash
-v /home/jianjun/ats-genai-admin:/workspace    # Source code
-v /mnt/d/ats-data:/data                       # Training data, minute bars
-v /mnt/d/ats-backup:/backup                   # Database backups
```

**Environment Variables:**
```bash
# DEV
DB_HOST=ats-dev-postgres    # Container name, NOT localhost
DB_PORT=5432               # Internal port
DB_USER=postgres
DB_PASSWORD=dev_password
DB_PASSWORD=dev_password
DB_NAME=dev_db
ENVIRONMENT=dev
PYTHONPATH=/workspace/src

# INTG (same pattern with ats-intg-postgres, intg_password, intg_db)
```

**Data Structure:**
```
/mnt/d/ats-data/
├── minute-bars/firstrate/  # Raw OHLCV INPUT (parquet files)
├── training-data/          # ML-ready OUTPUT (arrayrecord)
├── checkpoints/            # API rate limiting
└── temp/                   # Temporary processing
```

### **🚨 Critical Service Fixes (2025-09-12)**

**✅ RESOLVED: Analytics Service Command Path**
- **Issue**: `src/analytics/unified_analytics_service.py` (non-existent file)
- **Fixed**: `src/services/analytics_service.py` (correct path)
- **Result**: Analytics service fully operational at http://localhost:4000

**✅ RESOLVED: API Key Authentication**
- **Issue**: Placeholder keys (`your_polygon_api_key_here`) causing 401 errors
- **Fixed**: Valid API keys properly set in environment variables
- **Result**: POLYGON_API_KEY, TIINGO_API_KEY, EODHD_API_KEY authenticated

**✅ RESOLVED: Container Networking**
- **Issue**: Services on different networks causing DNS failures
- **Fixed**: All services use `ats-network` with proper container naming
- **Result**: Postgres containers accessible as `ats-intg-postgres`, `ats-dev-postgres`

### **🚨 Service Management Commands**

```bash
# START/STOP Environment Services
docker-compose -f docker-compose.intg.yml up -d     # Start integration
docker-compose -f docker-compose.intg.yml down      # Stop integration

# DEBUG Service Issues
docker logs ats-intg-analytics --tail 20            # Check analytics logs
docker exec ats-intg-postgres pg_isready -U postgres # Test DB connectivity
docker inspect ats-intg-analytics | grep NetworkMode # Verify network
```

### **🚨 Common Issues & Debug**

**Connection Issues:**
- Check network: `docker inspect <container> | grep NetworkMode`
- Test connectivity: `curl -f http://localhost:<port>/health`
- Check logs: `docker logs <container> --tail 20`

**Port Conflicts:**
- Debug: `docker ps | grep -E "(3000|4000|5432|4432)"`
- Fix: Use correct environment port mappings

## 🧪 **Test-Driven Development (MANDATORY)**

**MANDATORY sequence for ALL code changes:**
1. Write failing test FIRST: `python scripts/run_dev.py test --test tests/integration/test_new_feature.py`
2. Write minimal code to make test pass
3. Verify test passes: `python scripts/run_dev.py test`
4. Integration testing: `python scripts/run_dev.py test --test tests/integration/`

## 🎭 **Playwright UX Testing (MANDATORY for Frontend)**

**CRITICAL:** ALWAYS test UX changes with Playwright BEFORE claiming success

```bash
# Start services and run complete user flow tests
python scripts/run_dev.py start --service analytics --service postgres
PYTHONPATH=src python3 -m pytest tests/browser_tests/ -v --tb=short
```

**Requirements:**
- Test complete user workflow from UI interaction to data display
- Verify API endpoints return expected data structure
- Test error cases and edge conditions in UI

## 📋 **Essential Commands**

```bash
# Docker-Compose Service Management (PREFERRED - 2025-09-12)
docker-compose -f docker-compose.intg.yml up -d     # Start integration services
docker-compose -f docker-compose.dev.yml up -d      # Start dev services
docker-compose -f docker-compose.monitoring.yml up -d # Start monitoring
docker-compose -f docker-compose.intg.yml ps        # Check service status
docker-compose -f docker-compose.intg.yml down      # Stop services

# Legacy run_dev Commands (Still Available)
python scripts/run_dev.py setup
python scripts/run_dev.py start --service postgres
python scripts/run_dev.py start --service analytics
python scripts/run_dev.py logs --service analytics

# Database Operations
python scripts/run_dev.py query --query "SELECT version()"
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_instruments"

# Training Data & Run Management
python scripts/run_dev.py get --run-id <run_id>
python scripts/run_dev.py training_dataset get <dataset_id>
python scripts/run_dev.py run --script src/domains/ml/services/training_data/runners/training_data_callback_runner.py
```

## 🚨 **Critical Anti-Patterns to Avoid**

**Infrastructure & Development:**
- ❌ Running docker commands directly (use run_dev instead)
- ❌ Creating new services/files when existing can be enhanced
- ❌ Using mock/synthetic data outside unit tests
- ❌ Claiming functionality works without tests
- ❌ Writing tests after code (TDD requires tests first)
- ❌ Skipping Playwright testing for UX changes
- ❌ Not tracking training data generation in dev_runs table

**Debugging & Problem Solving:**
- ❌ Using workarounds without understanding root cause
- ❌ Restarting services without checking logs first
- ❌ Switching environments when current environment fails
- ❌ Ignoring error messages and trying random fixes
- ❌ Fixing symptoms instead of root causes

## 🎯 **Success Criteria**

**You're following best practices when you:**
- Use run_dev for all development operations
- Write failing tests before code changes
- Run integration tests and see them pass
- Enhance existing services instead of creating new ones
- Use real data only - no mock/synthetic data outside tests
- Track all training data generation in dev_runs table
- Test ALL UX changes with Playwright before claiming success
- Investigate logs before restarting services
- Understand root causes before implementing fixes
- Document investigation findings in commits/issues

## 🚨 **CRITICAL: Training Data Generation Flow**

**❌ DO NOT use `dev_daily_prices` - This table is NOT used for training data**

### **Data Flow: Minute Bars → Training Data Generator → Training Datasets**

```
1. Minute Bar Files (INPUT - Raw Data)
   ↓
2. FileBasedMinuteManager (Reads parquet files)
   ↓
3. FileBasedMinuteMarketDataManager (Aggregates timeframes)
   ↓
4. Training Data Generator (Creates sequences, features, labels)
   ↓
5. Training Datasets (OUTPUT - ML-ready numpy arrays)
```

**🔹 INPUT:** `/mnt/d/ats-data/minute-bars/firstrate/` - Raw minute-level OHLCV parquet files
**🔹 OUTPUT:** `/data/training_data/` - ML-ready ArrayRecord format (.arrayrecord files)

**Training Data Structure:**
- **Format**: `{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord`
- **Timeframes**: 5m, 15m, 1h, 1d (each gets separate ArrayRecord file)
- **Content**: QR4-compliant scalar data (timestamp, symbol, open, high, low, close, volume, vwap)
- **Database**: Registered in `dev_training_dataset` table
- **Tracking**: All runs logged in `dev_runs` table with command_line, git_commit_hash

## 📚 **Additional Documentation**

- **[START_HERE.md](docs/START_HERE.md)** - 15-minute setup and core concepts
- **[DEVELOPMENT.md](docs/DEVELOPMENT.md)** - Complete development guide
- **[OPERATIONS.md](docs/OPERATIONS.md)** - Daily operations and troubleshooting
- **[INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md)** - Database connections and service architecture

---

**🔥 This is a Docker-first, test-driven, DEBUG-FIRST development platform. Every change must be validated end-to-end with REAL DATA ONLY. When systems fail, investigate and understand before implementing workarounds.**

**🚫 ZERO TOLERANCE for file proliferation, superficial testing, and premature claims of completion. Excellence is non-negotiable.**