# CLAUDE.md - ATS Platform Guide

This file provides focused guidance to Claude Code when working with the ATS fintech platform.

## 🚨 **CRITICAL DEVELOPMENT PRINCIPLES**

### **🔍 DEBUG-FIRST: NO WORKAROUNDS**
- **❌ NEVER use workarounds** without understanding root cause
- **❌ NEVER restart services** without investigating logs first
- **❌ NEVER switch environments** when current environment has problems
- **✅ ALWAYS investigate logs** when commands fail
- **✅ ALWAYS understand WHY** before implementing solutions
- **✅ ALWAYS fix root causes** - not symptoms

**Root Cause Analysis Process:**
1. Gather Evidence: Check logs, service status, system resources
2. Test Hypotheses: Form specific hypothesis, test systematically
3. Implement Fix: Target root cause, not symptoms
4. Document: Record findings in commits/issues for future reference

### **🚫 NO MOCK/SYNTHETIC DATA**
- **❌ NEVER use mock data, fake data, synthetic data** outside of unit tests
- **❌ NEVER create fallbacks to demo data** when real data is unavailable
- **✅ Demo data ONLY in unit tests** - isolated, controlled scenarios
- **✅ Fail fast and clearly** when real data/database is unavailable

### **🚫 NO EXCEPTION CATCHING - FAIL FAST**
- **❌ NEVER use try/except blocks** to silence or mask errors
- **❌ NEVER catch Exception, BaseException, or bare except**
- **❌ NEVER provide fallback logic** when core systems fail
- **✅ ALWAYS let exceptions propagate** with full stack traces
- **✅ ALWAYS fix root causes** revealed by crashes

**Allowed Exception Handling (Very Limited):**
- Specific exceptions only: `except FileNotFoundError:` for optional files
- Resource cleanup: `finally:` blocks for closing connections
- Input validation: `except ValueError:` for user input parsing only

### **🔄 ENHANCE EXISTING BEFORE CREATING NEW**
- **❌ NEVER create new files/services** without checking if existing can be enhanced
- **✅ ALWAYS enhance existing services** - add features to current code
- **✅ ALWAYS consolidate functionality** - reduce complexity

### **🌿 MANDATORY BRANCH WORKFLOW**
- **❌ NEVER push commits directly to main branch**
- **❌ NEVER merge changes without code review**
- **✅ ALWAYS create feature/fix branches for ALL changes**
- **✅ ALWAYS submit pull requests for code review**

**Branch Naming:**
- `fix/descriptive-issue-name` - Bug fixes
- `feat/new-feature-name` - New features  
- `test/test-description` - Test additions
- `docs/documentation-update` - Documentation

## 🧠 **MCP Knowledge Graph - Persistent Memory**

**CRITICAL: MCP Knowledge Graph is ACTIVE and MANDATORY for all development sessions**

### **🚨 ALWAYS use MCP Knowledge Graph for:**
- **Platform Knowledge**: Service configurations, API keys, database schemas
- **Issue Tracking**: Store root causes, solutions, debugging patterns
- **Architecture Decisions**: Document design trade-offs and evolution
- **Development Context**: Maintain session continuity across conversations

### **🎯 Usage Patterns**
**Session Startup:**
- Load current project state and recent issues
- Check dependencies and service status
- Review previous solutions and debugging patterns

**During Development:**
- Store discoveries and root causes immediately
- Track configuration updates and performance impacts
- Link relations between services, databases, dependencies

**Session End:**
- Record completion state and next steps
- Document unresolved issues for future sessions
- Save configurations and environment settings

### **🚀 Essential Commands**
```bash
# MCP is automatically active - use natural language:
"Remember that the analytics service uses port 4000 in integration environment"
"What was the solution to the Docker networking issue we solved yesterday?"
"Store this API rate limiting configuration"
```

## 🐳 **Docker Architecture**

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

### **🔌 Port Architecture**

| Service | DEV | INTG | Internal |
|---------|-----|------|----------|
| Analytics | :3000 | :4000 | :3000 |
| PostgreSQL | :5432 | :4432 | :5432 |
| API | :8000 | :8001 | :8000 |

### **📦 Container Setup**
**Container Naming:** `ats-{env}-{service}` (e.g., `ats-dev-postgres`, `ats-intg-analytics`)

**Critical Volume Mounts:**
```bash
-v /home/jianjun/ats-genai-admin:/workspace    # Source code
-v /mnt/d/ats-data:/data                       # Training data, minute bars
-v /mnt/d/ats-backup:/backup                   # Database backups
```

**Environment Variables:**
```bash
DB_HOST=ats-dev-postgres    # Container name, NOT localhost
DB_PORT=5432               # Internal port
DB_USER=postgres
DB_PASSWORD=dev_password    # or intg_password
DB_NAME=dev_db             # or intg_db
ENVIRONMENT=dev            # or intg
PYTHONPATH=/workspace/src
```

### **🚨 Debug Commands**
```bash
# START/STOP Environment Services
docker-compose -f docker-compose.intg.yml up -d     # Start integration
docker-compose -f docker-compose.intg.yml down      # Stop integration

# DEBUG Service Issues
docker logs ats-intg-analytics --tail 20            # Check analytics logs
docker exec ats-intg-postgres pg_isready -U postgres # Test DB connectivity
docker inspect ats-intg-analytics | grep NetworkMode # Verify network
```

## 🧪 **TESTING REQUIREMENTS (MANDATORY)**

### **🚨 CRITICAL: TEST-FIRST DEVELOPMENT**
**MANDATORY sequence for ALL code changes:**
1. **Write failing test FIRST**
2. **Write minimal code** to make test pass
3. **Verify test passes**
4. **Integration testing**

### **🚨 CRITICAL: DEBUG-FIRST ISSUE RESOLUTION**
**MANDATORY sequence for ALL bug fixes:**
1. **Log inputs in the failing method FIRST**
2. **Write failing test to reproduce the issue**
3. **Identify and fix the root cause**
4. **Verify test passes**

### **🎯 FOUR TESTING PILLARS**

#### **1️⃣ TEST-FIRST DEVELOPMENT (TDD)**
- **❌ NEVER write production code without a failing test first**
- **✅ ALWAYS write the test that demonstrates the desired behavior**
- **✅ ALWAYS see the test fail before implementing**

#### **2️⃣ NO EXCEPTION HANDLING IN TESTS**
- **❌ NEVER use try/except blocks in tests to hide failures**
- **✅ ALWAYS let tests fail loudly with clear error messages**
- **✅ ALWAYS use specific assertions that describe expected behavior**

#### **3️⃣ VALIDATE ACTUAL RESULTS (NOT JUST EXISTENCE)**
- **❌ NEVER check only if results exist** (`assert result is not None`)
- **❌ NEVER use superficial validations** (`assert len(result) > 0`)
- **✅ ALWAYS verify exact values, counts, and business logic correctness**
- **✅ ALWAYS validate EXACT error types, messages, and root causes**
- **✅ ALWAYS validate PRECISE time calculations and OHLCV values**

#### **4️⃣ REAL OBJECTS ONLY (NO MOCK OBJECTS)**
- **❌ NEVER use unittest.mock.Mock() or @patch decorators**
- **❌ NEVER mock database connections, APIs, or business objects**
- **✅ ALWAYS use real instances with controlled test data**
- **✅ ALWAYS test actual integration between real components**

### **🎭 Playwright UX Testing**
**CRITICAL:** ALWAYS test UX changes with Playwright BEFORE claiming success

```bash
# Start services and run complete user flow tests
python scripts/run_dev.py start --service analytics --service postgres
PYTHONPATH=src python3 -m pytest tests/browser_tests/ -v --tb=short
```

## 📋 **Essential Commands**

```bash
# Docker-Compose Service Management (PREFERRED)
docker-compose -f docker-compose.intg.yml up -d     # Start integration services
docker-compose -f docker-compose.dev.yml up -d      # Start dev services
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
```

## 🚨 **Critical Anti-Patterns to Avoid**

**Infrastructure & Development:**
- ❌ Starting sessions without retrieving project context from knowledge graph
- ❌ Not storing important discoveries and solutions in persistent memory
- ❌ Running docker commands directly (use docker-compose instead)
- ❌ Creating new services/files when existing can be enhanced
- ❌ Using mock/synthetic data outside unit tests
- ❌ Claiming functionality works without tests
- ❌ Writing tests after code (TDD requires tests first)
- ❌ Skipping Playwright testing for UX changes

**Debugging & Problem Solving:**
- ❌ Using workarounds without understanding root cause
- ❌ Restarting services without checking logs first
- ❌ Switching environments when current environment fails
- ❌ Ignoring error messages and trying random fixes
- ❌ Fixing symptoms instead of root causes
- ❌ **CRITICAL: Fixing issues without reproducing them in tests FIRST**

**Validation & Testing:**
- ❌ **NEVER check prior runs/historical data** to claim current run success
- ❌ **NEVER use old successful results** to validate current failures
- ❌ **NEVER claim "working" based on previous executions**
- ✅ **ALWAYS validate current run outputs** - files, database records, actual results
- ✅ **CURRENT FAILURE = CURRENT PROBLEM** - debug the actual failing case

## 🎯 **Success Criteria**

**You're following best practices when you:**
- Use MCP Knowledge Graph for persistent memory and context continuity
- Store discoveries, solutions, and configurations in knowledge graph
- Retrieve project context at session start and document findings at session end
- Use docker-compose for all service management
- Write failing tests before code changes
- Run integration tests and see them pass
- Enhance existing services instead of creating new ones
- Use real data only - no mock/synthetic data outside tests
- Test ALL UX changes with Playwright before claiming success
- Investigate logs before restarting services
- Understand root causes before implementing fixes
- Document investigation findings in commits/issues AND knowledge graph

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
- **Database**: Registered in `dev_training_dataset` table
- **Tracking**: All runs logged in `dev_runs` table with command_line, git_commit_hash

---

**🔥 This is a Docker-first, test-driven, DEBUG-FIRST development platform. Every change must be validated end-to-end with REAL DATA ONLY. When systems fail, investigate and understand before implementing workarounds.**

**🚫 ZERO TOLERANCE for file proliferation, superficial testing, and premature claims of completion. Excellence is non-negotiable.**