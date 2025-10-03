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

### **🚫 ABSOLUTE ZERO TOLERANCE FOR MOCK OBJECTS - CRITICAL VIOLATION**

**🚨 MANDATORY: ANY USE OF MOCK OBJECTS IS A CRITICAL ERROR**

- **❌ FORBIDDEN: unittest.mock.Mock()** - NEVER use under any circumstances
- **❌ FORBIDDEN: unittest.mock.AsyncMock()** - NEVER use under any circumstances  
- **❌ FORBIDDEN: @patch decorators** - NEVER use under any circumstances
- **❌ FORBIDDEN: Mock() for any service, manager, DAO, or business object**
- **❌ FORBIDDEN: "mock_" prefixed variables** - indicates mock usage
- **❌ FORBIDDEN: Mock objects in fixtures, test setup, or test methods**

**🚨 CRITICAL ENFORCEMENT:**
- **ANY mock usage is an immediate failure** - fix before proceeding
- **Code review must reject any Mock usage** - zero exceptions
- **Tests with Mock objects are invalid** - rewrite with real objects

**✅ MANDATORY REAL OBJECT PATTERNS:**

**1️⃣ Replace Mock Services with Real Instances:**
```python
# ❌ FORBIDDEN - Mock Pattern
mock_universe_state_manager = Mock()
mock_universe_state_manager.addUniverseState = AsyncMock(return_value=True)

# ✅ REQUIRED - Real Object Pattern  
real_universe_state_manager = InMemoryUniverseStateManager()
# OR
real_universe_state_manager = DatabaseUniverseStateManager(test_db_connection)
```

**2️⃣ Replace Mock DAOs with Real Test DAOs:**
```python
# ❌ FORBIDDEN - Mock Pattern
mock_xrefs_dao = Mock()
mock_xrefs_dao.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={363996: 'AAPL'})

# ✅ REQUIRED - Real Object Pattern
real_xrefs_dao = InstrumentXrefsDAO(test_environment)
await real_xrefs_dao.insert_test_mapping(363996, 'AAPL')  # Set up test data
```

**3️⃣ Replace Mock Runners with Real Runners:**
```python
# ❌ FORBIDDEN - Mock Pattern
mock_runner = Mock()
mock_runner.get_environment.return_value = environment
mock_runner.universe_id = 1

# ✅ REQUIRED - Real Object Pattern
real_runner = TestRunner(
    environment=test_environment,
    universe_id=1,
    market_data_manager=real_market_data_manager,
    universe_state_manager=real_universe_state_manager
)
```

**4️⃣ Replace Mock Managers with Real Test Managers:**
```python
# ❌ FORBIDDEN - Mock Pattern
mock_universe_manager = Mock()
mock_universe_manager.instrument_ids = [363996]

# ✅ REQUIRED - Real Object Pattern
real_universe_manager = UniverseManager(test_environment)
await real_universe_manager.add_instrument_to_universe(1, 363996)  # Set up test data
```

**5️⃣ Use Real Test Databases and In-Memory Implementations:**
```python
# ✅ REQUIRED - Real Object Alternatives
- InMemoryUniverseStateManager() - for isolated testing
- TestDatabaseManager() - for integration testing
- FileBasedMarketDataManager() - for data testing
- InMemoryCache() - for cache testing
```

**Why Mock Objects Are Dangerous:**
- **Hide real integration failures** that occur in production
- **Mask interface changes** between components
- **Create false test confidence** with broken real implementations  
- **Hide performance issues** and resource constraints
- **Lead to production surprises** when real objects behave differently

**🔥 ZERO TOLERANCE ENFORCEMENT:**
- **Tests must use real objects only** - no exceptions
- **All mocks must be replaced immediately** when found
- **Code reviews must reject any mock usage** - mandatory fix
- **Mock usage indicates insufficient test infrastructure** - build real alternatives

### **🚫 NO EXCEPTION CATCHING - FAIL FAST POLICY**
- **❌ NEVER use try/except blocks** to silence or mask errors
- **❌ NEVER catch Exception, BaseException, or bare except:**
- **❌ NEVER provide fallback logic** when core systems fail
- **❌ NEVER log errors and continue** - let the application crash
- **✅ ALWAYS let exceptions propagate** with full stack traces
- **✅ ALWAYS fix root causes** revealed by crashes
- **✅ ALWAYS ensure proper cleanup** using context managers and finally blocks

**Why Exception Catching Is Dangerous:**
- Masks configuration problems and dependency failures  
- Hides database connection issues and query failures
- Creates silent failures that corrupt data integrity
- Prevents proper error monitoring and alerting
- Results in production issues that are difficult to diagnose
- Leads to false system health indicators

**Allowed Exception Handling (Very Limited):**
- **Specific exceptions only**: `except FileNotFoundError:` for optional files
- **Resource cleanup**: `finally:` blocks for closing connections  
- **Context managers**: `with` statements for automatic cleanup
- **Input validation**: `except ValueError:` for user input parsing only

**Example Patterns:**

❌ **WRONG - Silent Error Masking:**
```python
try:
    result = critical_business_logic()
    return result
except Exception as e:
    logger.error(f"Error: {e}")
    return None  # Masks the real problem
```

✅ **RIGHT - Fail Fast:**
```python
def critical_business_logic():
    # Let any exception propagate - crash immediately
    result = database.get_critical_data()
    return result
```

❌ **WRONG - Fallback to Mock Data:**
```python
try:
    data = fetch_real_market_data()
except Exception:
    data = generate_mock_data()  # Dangerous fallback
```

✅ **RIGHT - Crash on Data Unavailability:**
```python
def fetch_market_data():
    # If real data is unavailable, the application should crash
    # This forces immediate attention to fix the real issue
    return api_client.get_market_data()
```

### **🔄 ENHANCE EXISTING BEFORE CREATING NEW**
- **❌ NEVER create new files/services** without checking if existing can be enhanced
- **❌ NEVER duplicate functionality** in new files
- **✅ ALWAYS enhance existing services** - add features to current code
- **✅ ALWAYS consolidate functionality** - reduce complexity, don't add it

### **🚫 NO DUPLICATE SCRIPTS OR TEMPORARY FILES - MANDATORY FILE ANALYSIS**
- **❌ NEVER create debug scripts, test files, or utility scripts** without thorough existing code analysis
- **❌ NEVER create temporary files for debugging** - use existing test infrastructure
- **❌ NEVER create new scripts when existing patterns can be reused** 
- **❌ NEVER create standalone utilities** when functionality belongs in existing modules
- **✅ ALWAYS analyze existing files first** to understand patterns and reuse infrastructure
- **✅ ALWAYS use existing debugging/logging mechanisms** instead of new scripts
- **✅ ALWAYS enhance existing test files** rather than creating new ones
- **✅ ALWAYS follow established codebase patterns** for new functionality

**Mandatory Analysis Process Before File Creation:**
1. **Existing Code Search**: Use Grep/Glob to find similar functionality in codebase
2. **Pattern Analysis**: Understand existing debugging/testing patterns
3. **Reuse Assessment**: Identify existing infrastructure that can be enhanced
4. **Integration Check**: Ensure new code follows established patterns
5. **File Necessity**: Only create new files if absolutely necessary and no existing code can be enhanced

**Why Duplicate Files Are Dangerous:**
- Creates maintenance burden with scattered debugging code
- Violates DRY (Don't Repeat Yourself) principles
- Introduces inconsistent patterns across codebase
- Makes debugging harder when logic is scattered
- Creates technical debt that accumulates over time

### **🌿 MANDATORY BRANCH WORKFLOW - NO DIRECT MAIN COMMITS**
- **❌ NEVER push commits directly to main branch**
- **❌ NEVER merge changes without code review**
- **❌ NEVER bypass the branch → PR → review → merge workflow**
- **❌ NEVER commit incomplete work or experimental changes to main**
- **✅ ALWAYS create feature/fix branches for ALL changes**
- **✅ ALWAYS submit pull requests for code review**
- **✅ ALWAYS wait for review approval before merging**

**Why Direct Main Commits Are Dangerous:**
- Bypasses code review and quality gates
- Introduces unreviewed bugs into production code
- Breaks collaborative development workflows
- Prevents proper testing and validation
- Creates merge conflicts and integration issues
- Violates team accountability and knowledge sharing

**Mandatory Git Workflow:**

**1️⃣ Always Create Branches:**
```bash
git checkout -b fix/descriptive-issue-name        # Bug fixes
git checkout -b feat/new-feature-name            # New features  
git checkout -b test/test-description            # Test additions
git checkout -b docs/documentation-update       # Documentation
```

**2️⃣ Branch Naming Conventions:**
- **fix/**: Bug fixes and error corrections
- **feat/**: New features and enhancements
- **test/**: Test additions and test improvements
- **docs/**: Documentation updates
- **refactor/**: Code refactoring without behavior changes
- **chore/**: Maintenance tasks and tooling updates

**3️⃣ Commit Message Standards:**
- Use conventional commit format: `type: description`
- Include comprehensive body explaining the change
- Add test results and verification steps
- Reference related issues or tickets
- Include co-author attribution for Claude Code

**4️⃣ Pull Request Process:**
```bash
git add .
git commit -m "fix: resolve run_id type mismatch in training callback

## Problem
- Production error: 'str' object cannot be interpreted as an integer
- MonthlyTrainingDataDAO expects integer run_id, got string from runner context

## Solution  
- Use RunMetadataTracker for proper integer database run_id generation
- Replace string runner.run_context.run_id with integer database run_id

## Testing
- Added comprehensive test reproducing exact production error
- Verified fix works with real database integration
- Confirms type safety and foreign key constraints

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"

git push origin fix/run-id-type-mismatch-verification
```

**5️⃣ Code Review Requirements:**
- **All changes must be reviewed** before merging to main
- **Include test verification** in PR description
- **Document breaking changes** and migration steps
- **Verify CI/CD passes** before requesting review
- **Wait for approval** before merging

**Example Proper Workflow:**
```bash
# ❌ WRONG: Direct to main
git add .
git commit -m "fix bug"
git push origin main  # FORBIDDEN

# ✅ RIGHT: Branch workflow
git checkout -b fix/training-data-callback-bug
git add .
git commit -m "fix: resolve run_id type mismatch in training callback"
git push origin fix/training-data-callback-bug
# Create PR, wait for review, then merge
```

**Emergency Hotfixes:**
- Even urgent production fixes must go through branch → PR → review
- Use expedited review process but never bypass code review
- Create hotfix branch, fix issue, immediate PR, fast review, merge
- Document emergency rationale in PR description

## 🧠 **MCP Knowledge Graph - Persistent Memory (2025-09-13)**

**CRITICAL: MCP Knowledge Graph is ACTIVE and MANDATORY for all development sessions**

### **🚨 PERSISTENT MEMORY REQUIREMENTS**

**✅ ALWAYS use MCP Knowledge Graph for:**
- **Platform Knowledge**: Remember service configurations, API keys, database schemas
- **Issue Tracking**: Store root causes, solutions, and debugging patterns
- **Architecture Decisions**: Document design trade-offs and evolution
- **Performance Baselines**: Track optimization results and benchmarks
- **Development Context**: Maintain session continuity across conversations

### **📊 Knowledge Graph Structure**

**Core Entities:**
```
🏗️ INFRASTRUCTURE
├── Services (analytics, postgres, redis, etc.)
├── Databases (dev_db, intg_db, schemas)
├── APIs (endpoints, authentication, rate limits)
└── Containers (ats-dev-*, ats-intg-*)

💾 DATA MANAGEMENT  
├── Vendors (POLYGON, TIINGO, EODHD)
├── Data Sources (minute-bars, training-data)
├── Pipelines (ETL, ML training, validation)
└── Quality Issues (gaps, duplicates, performance)

🔧 DEVELOPMENT
├── Issues (bugs, performance, integration)
├── Solutions (fixes, optimizations, patterns)
├── Configurations (Docker, environment variables)
└── Dependencies (services, libraries, versions)
```

### **🎯 MCP Usage Patterns**

**Session Startup:**
1. **Load Context**: Retrieve current project state and recent issues
2. **Check Dependencies**: Verify service status and configurations
3. **Review History**: Access previous solutions and debugging patterns

**During Development:**
1. **Store Discoveries**: Document root causes and solutions immediately
2. **Track Changes**: Record configuration updates and performance impacts
3. **Link Relations**: Connect services, databases, and dependencies

**Session End:**
1. **Update Status**: Record completion state and next steps
2. **Document Blockers**: Store unresolved issues for future sessions
3. **Save Configurations**: Persist environment and service settings

### **🚀 Quick MCP Commands**

**ALWAYS available in Claude Code sessions:**
```bash
# MCP is automatically active - use natural language:
"Remember that the analytics service uses port 4000 in integration environment"
"Store the fact that POLYGON_API_KEY rate limit is 5 requests per minute"
"What was the solution to the Docker networking issue we solved yesterday?"
"Create a relationship between postgres-intg and analytics-intg services"
```

### **🔍 Knowledge Retrieval Examples**

**Before Starting Work:**
- "What are the current service port mappings?"
- "What database connection issues have we encountered?"
- "What was the last optimization we made to the analytics service?"

**During Debugging:**
- "Have we seen this error pattern before?"
- "What's the relationship between this service and the database?"
- "What environment variables does this service require?"

**After Problem Solving:**
- "Remember this Docker networking solution for future sessions"
- "Store this API rate limiting configuration"
- "Document this performance optimization result"

### **⚡ Integration with Development Workflow**

**Docker Commands + Memory:**
```bash
# Start services and remember configuration
docker-compose -f docker-compose.intg.yml up -d
# → MCP stores: "Integration environment started successfully at timestamp X"

# Check service health and remember status  
curl -f http://localhost:4000/health
# → MCP stores: "Analytics service healthy, features: type_system=true, ray_computing=false"
```

**Git Operations + Memory:**
```bash
# Commit changes and remember context
git commit -m "fix: restore analytics service functionality"
# → MCP stores: "Analytics service restored from git history, original 7,956 lines"
```

**🚨 CRITICAL SUCCESS PATTERN:**
- **Start Session**: "What should I know about the current project state?"
- **End Session**: "Remember that [specific achievement/issue/solution]"
- **Next Session**: Automatic context continuity and institutional knowledge

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

## 🧪 **RIGOROUS TESTING PRINCIPLES (MANDATORY)** ⚡

### **🚨 CRITICAL: TEST-FIRST DEVELOPMENT DISCIPLINE**

**MANDATORY sequence for ALL code changes:**
1. **Write failing test FIRST**: `python scripts/run_dev.py test --test tests/integration/test_new_feature.py`
2. **Write minimal code** to make test pass
3. **Verify test passes**: `python scripts/run_dev.py test`
4. **Integration testing**: `python scripts/run_dev.py test --test tests/integration/`

### **🚨 CRITICAL: DEBUG-FIRST ISSUE RESOLUTION DISCIPLINE**

**MANDATORY sequence for ALL bug fixes and issue resolution:**
1. **Log inputs in the failing method FIRST**: Add detailed logging to capture actual inputs
2. **Write failing test to reproduce the issue**: Create test with logged inputs that demonstrates the bug
3. **Identify and fix the root cause**: Analyze test failure to find coding error
4. **Verify test passes**: Ensure the fix resolves the issue completely

**❌ NEVER fix issues without this sequence:**
- **❌ NEVER apply workarounds without understanding root cause**
- **❌ NEVER fix bugs based on assumptions about inputs**
- **❌ NEVER skip logging step - inputs reveal the actual problem**
- **❌ NEVER fix without a test that reproduces the exact failure**

**✅ ALWAYS follow debug-first methodology:**
- **✅ ALWAYS log actual inputs in the failing method first**
- **✅ ALWAYS create a test that reproduces the exact failure scenario**
- **✅ ALWAYS identify the specific coding error causing the issue**
- **✅ ALWAYS verify your fix with the reproducing test**

**Example Pattern:**
```python
# STEP 1: Add logging to failing method
def process_training_data(data, timeframe, symbols):
    logger.debug(f"INPUT DEBUG: data shape={data.shape}, timeframe={timeframe}, symbols={symbols}")
    logger.debug(f"INPUT DEBUG: data types={data.dtypes}, first_row={data.iloc[0] if len(data) > 0 else 'EMPTY'}")
    
    # Method implementation that's failing
    result = transform_data(data, timeframe)
    return result

# STEP 2: Write test with logged inputs that reproduces the issue
def test_training_data_processing_bug_reproduction():
    # Use exact inputs from debug logs that caused the failure
    problematic_data = pd.DataFrame({
        'timestamp': ['2024-01-01 09:30:00'],  # String instead of datetime
        'symbol': ['AAPL'],
        'price': [150.0]
    })
    
    # This test MUST fail initially, demonstrating the bug
    with pytest.raises(TypeError, match="cannot convert string to datetime"):
        result = process_training_data(problematic_data, '5m', ['AAPL'])

# STEP 3: Fix the identified coding error
def process_training_data(data, timeframe, symbols):
    # FIX: Convert string timestamps to datetime objects
    if 'timestamp' in data.columns and data['timestamp'].dtype == 'object':
        data['timestamp'] = pd.to_datetime(data['timestamp'])
    
    result = transform_data(data, timeframe)
    return result

# STEP 4: Verify test now passes
def test_training_data_processing_fixed():
    problematic_data = pd.DataFrame({
        'timestamp': ['2024-01-01 09:30:00'],  # Same problematic input
        'symbol': ['AAPL'],
        'price': [150.0]
    })
    
    # Now this should work without errors
    result = process_training_data(problematic_data, '5m', ['AAPL'])
    assert result is not None
    assert len(result) > 0
```

**Why This Methodology Is Critical:**
- **Logs reveal actual problematic inputs** (not assumed inputs)
- **Tests with real failure cases** prevent regression
- **Root cause identification** eliminates symptoms-only fixes
- **Verification ensures** complete resolution

### **🚨 CRITICAL: REAL OBJECTS AND FAIL-FAST REFACTORING**

**MANDATORY systematic elimination of mock objects and exception masking across entire codebase**

**Scope Identified:**
- **277 files** with mock object usage requiring refactoring
- **420 files** with exception handling in tests requiring cleanup
- **Extensive exception handling** in source code masking real issues

#### **🔧 MOCK OBJECT ELIMINATION (PHASE 1)**

**❌ ANTI-PATTERN: Mock-Heavy Testing**
```python
@pytest.fixture
def mock_instruments_dao(self):
    dao = Mock()
    dao.create_instrument = AsyncMock()
    dao.get_instrument = AsyncMock()
    return dao

def test_service_logic(self, mock_instruments_dao):
    service = InstrumentService(mock_instruments_dao)
    # Test passes but may fail in production
```

**✅ REAL OBJECTS PATTERN: Database-Backed Testing**
```python
@pytest.fixture
async def real_instruments_dao(self, test_environment):
    return InstrumentsDAO(test_environment)

@pytest.fixture
async def clean_database(self, test_environment):
    # Clean database state for each test
    async with test_environment.get_connection() as conn:
        await conn.execute("TRUNCATE TABLE instruments RESTART IDENTITY CASCADE")
    yield test_environment

async def test_service_logic(self, real_instruments_dao):
    service = InstrumentService(real_instruments_dao)
    # Test validates actual integration and database constraints
```

#### **🚨 EXCEPTION MASKING ELIMINATION (PHASE 2)**

**❌ ANTI-PATTERN: Exception Masking in Tests**
```python
def test_service_health(self):
    try:
        result = service.check_health()
        assert result is not None  # Weak assertion
    except Exception as e:
        pytest.fail(f"Error: {e}")  # Masks real issues
```

**✅ FAIL-FAST PATTERN: Clear Error Propagation**
```python
def test_service_health(self):
    result = service.check_health()
    assert result.status == 'healthy'
    assert result.last_check_time is not None
    assert result.error_count == 0
    # Any exception propagates with clear stack trace
```

**❌ ANTI-PATTERN: Generic Exception Masking in Source Code**
```python
def get_market_data(symbol):
    try:
        data = api_client.fetch(symbol)
        return data
    except Exception as e:
        logger.warning(f"API error: {e}")
        return None  # Masks API issues
```

**✅ SPECIFIC EXCEPTION HANDLING: Actionable Error Management**
```python
def get_market_data(symbol):
    try:
        data = api_client.fetch(symbol)
        return data
    except APIRateLimitError as e:
        raise MarketDataUnavailable(f"Rate limit exceeded for {symbol}: {e}")
    except APIAuthenticationError as e:
        raise ConfigurationError(f"Authentication failed for {symbol}: {e}")
    # Let other exceptions propagate - they indicate real bugs
```

#### **📋 SYSTEMATIC REFACTORING APPROACH**

**Priority Order:**
1. **Core Business Logic Tests** (`tests/domains/*/services/`)
2. **Integration Tests** (`tests/integration/`)  
3. **Unit Tests** (`tests/unit/`)
4. **UI/Browser Tests** (lowest priority)

**Refactoring Steps per File:**
1. **Replace Mock Fixtures** with real object fixtures
2. **Remove Exception Masking** from test methods
3. **Add Specific Assertions** that validate exact results
4. **Run Tests** to identify real issues previously hidden
5. **Fix Root Causes** revealed by real object testing

#### **🔧 REAL OBJECT INFRASTRUCTURE REQUIRED**

**Test Database Setup:**
```python
@pytest.fixture(scope="session")
async def test_database():
    db_url = "postgresql://test:test@localhost/test_db"
    # Create test schema, run migrations
    yield db_url

@pytest.fixture
async def clean_database(test_database):
    # Truncate tables, reset sequences for each test
    yield test_database
```

**Real Service Fixtures:**
```python
@pytest.fixture
async def real_market_data_manager(test_environment):
    manager = UnifiedMarketDataManager(
        environment=test_environment,
        data_path="/tmp/test_data"
    )
    await manager.initialize()
    yield manager
    await manager.cleanup()
```

#### **💥 EXPECTED IMPACT: INITIAL TEST FAILURES ARE GOOD**

**When implementing real objects:**
- **Many tests will initially fail** - this reveals hidden issues
- **Database constraint violations** will surface real data problems  
- **Integration issues** between components will be exposed
- **Performance problems** with real data volumes will be discovered
- **Configuration issues** with real services will be revealed

**This is EXACTLY what we want** - real failures show real problems that were hidden by mocks.

#### **📊 SUCCESS METRICS**

**After Refactoring:**
- ✅ **Zero Mock objects** in business logic tests
- ✅ **Zero generic exception handling** in tests
- ✅ **Specific exceptions** with actionable error messages in source code
- ✅ **Real database integration** testing
- ✅ **Clear failure propagation** revealing actual issues

**Documentation References:**
- **Full Methodology**: `REFACTORING_METHODOLOGY.md`
- **Real Objects Example**: `tests/domains/instruments/services/test_instrument_service_impl_real_objects.py`
- **Fail-Fast Tests Example**: `test_agent_comprehensive_playwright_fail_fast.py`
- **Source Code Example**: `src/agents/system_monitor_fail_fast.py`

### **🎯 FOUR PILLARS OF TESTING EXCELLENCE**

#### **1️⃣ TEST-FIRST DEVELOPMENT (MANDATORY TDD)**
- **❌ NEVER write production code without a failing test first**
- **❌ NEVER add features without test coverage**
- **❌ NEVER fix bugs without reproducing them in tests first**
- **✅ ALWAYS write the test that demonstrates the desired behavior**
- **✅ ALWAYS see the test fail before implementing**
- **✅ ALWAYS verify the test passes after implementation**

**Example Pattern:**
```python
# ❌ WRONG: Writing code first
def calculate_portfolio_value(positions):
    return sum(pos.value for pos in positions)

# ✅ RIGHT: Test first
def test_calculate_portfolio_value():
    positions = [Position(value=100), Position(value=200)]
    result = calculate_portfolio_value(positions)
    assert result == 300  # Test fails initially
    
# THEN implement the function
```

#### **2️⃣ NO EXCEPTION HANDLING IN TESTS (LET TESTS FAIL)**
- **❌ NEVER use try/except blocks in tests to hide failures**
- **❌ NEVER catch exceptions unless testing exception behavior**
- **❌ NEVER suppress test failures with broad exception handling**
- **✅ ALWAYS let tests fail loudly with clear error messages**
- **✅ ALWAYS use specific assertions that describe expected behavior**
- **✅ ALWAYS allow unexpected exceptions to surface immediately**

**Example Pattern:**
```python
# ❌ WRONG: Hiding failures
def test_market_data_processing():
    try:
        result = process_market_data(data)
        if result:
            assert True  # Meaningless test
    except Exception:
        pass  # Hides real problems

# ✅ RIGHT: Let it fail clearly
def test_market_data_processing():
    result = process_market_data(test_data)
    assert result.status == 'SUCCESS'
    assert len(result.processed_records) == 100
    assert result.error_count == 0
    # Any unexpected exception will fail the test with clear stack trace
```

#### **3️⃣ VALIDATE ACTUAL RESULTS (NOT JUST EXISTENCE) - ZERO TOLERANCE FOR SUPERFICIAL TESTING**

**🚨 CRITICAL: EXACT VALUE VALIDATION REQUIREMENTS**

- **❌ NEVER check only if results exist (assert result is not None)**
- **❌ NEVER use superficial validations (assert len(result) > 0)**
- **❌ NEVER ignore content validation for performance convenience**
- **❌ NEVER check generic properties without validating specific business values**
- **❌ NEVER validate error scenarios without checking EXACT error types and messages**
- **❌ NEVER check "partial success" without validating which SPECIFIC data is missing and WHY**

**✅ MANDATORY EXACT VALUE VALIDATION:**
- **✅ ALWAYS verify exact values, counts, and business logic correctness**
- **✅ ALWAYS check data integrity and expected transformations**
- **✅ ALWAYS validate edge cases and boundary conditions**
- **✅ ALWAYS validate EXACT error types, messages, and root causes**
- **✅ ALWAYS validate PRECISE time calculations down to the second**
- **✅ ALWAYS validate EXACT OHLCV values match expected mathematical calculations**
- **✅ ALWAYS validate SPECIFIC symbols, parameters, and request details**

**🔥 CONCRETE VALIDATION PATTERNS:**

**❌ TERRIBLE: Superficial Error Validation**
```python
# This is the worst test code - validates nothing meaningful
if scenario['expected_behavior'] == 'partial_success':
    assert result is not None, f"Should return partial data"
    assert len(result) <= len(test_symbols), f"Partial data size incorrect"
    
elif scenario['expected_behavior'] == 'empty_result':
    assert result == {}, f"Should return empty dict"
    
else:
    print(f"Expected exception but got result")  # Validates nothing
```

**✅ EXCELLENT: Exact Value Validation**
```python
# EXACT validation of error scenarios
if scenario['expected_behavior'] == 'partial_success':
    # Validate EXACTLY which symbols are present and missing
    expected_missing = ['DELISTED_SYMBOL', 'INVALID_SYMBOL']
    expected_present = ['AAPL', 'TSLA', 'MSFT']
    
    assert len(result) == len(expected_present), f"Expected exactly {len(expected_present)} symbols"
    
    for symbol in expected_present:
        assert symbol in result, f"Expected symbol {symbol} missing from partial result"
        # Validate EXACT OHLCV structure and values
        ohlcv = result[symbol]
        assert 'open' in ohlcv and ohlcv['open'] > 0, f"{symbol} missing valid open price"
        assert 'high' in ohlcv and ohlcv['high'] >= ohlcv['open'], f"{symbol} high < open: {ohlcv['high']} < {ohlcv['open']}"
        assert 'low' in ohlcv and ohlcv['low'] <= ohlcv['close'], f"{symbol} low > close: {ohlcv['low']} > {ohlcv['close']}"
        assert 'volume' in ohlcv and ohlcv['volume'] >= 0, f"{symbol} invalid volume: {ohlcv['volume']}"
    
    for symbol in expected_missing:
        assert symbol not in result, f"Unexpected symbol {symbol} in partial result"
        
elif scenario['expected_behavior'] == 'api_rate_limit_error':
    # Validate EXACT exception type and message
    with pytest.raises(APIRateLimitError) as exc_info:
        result = await market_data_manager.get_ohlcv(symbols, start, end)
    
    # Validate EXACT error message and details
    assert "rate limit exceeded" in str(exc_info.value).lower(), f"Wrong error message: {exc_info.value}"
    assert exc_info.value.retry_after_seconds > 0, f"Missing retry timing: {exc_info.value.retry_after_seconds}"
    assert exc_info.value.vendor == 'polygon', f"Wrong vendor in error: {exc_info.value.vendor}"
    
elif scenario['expected_behavior'] == 'network_timeout':
    # Validate EXACT timeout behavior
    with pytest.raises(asyncio.TimeoutError) as exc_info:
        result = await asyncio.wait_for(
            market_data_manager.get_ohlcv(symbols, start, end), 
            timeout=5.0
        )
    
    # Validate timeout occurred within expected window
    elapsed_time = time.time() - start_time
    assert 4.5 <= elapsed_time <= 6.0, f"Timeout occurred outside expected window: {elapsed_time}s"
```

**✅ EXCELLENT: Exact OHLCV Value Validation**
```python
def test_ohlcv_calculation_accuracy():
    """Validate EXACT OHLCV calculations with mathematical precision."""
    
    # Test with known minute data
    minute_data = [
        {'timestamp': '09:30', 'open': 100.0, 'high': 102.5, 'low': 99.5, 'close': 101.0, 'volume': 1000},
        {'timestamp': '09:31', 'open': 101.0, 'high': 103.0, 'low': 100.5, 'close': 102.5, 'volume': 1500},
        {'timestamp': '09:32', 'open': 102.5, 'high': 104.0, 'low': 102.0, 'close': 103.5, 'volume': 2000},
        {'timestamp': '09:33', 'open': 103.5, 'high': 105.0, 'low': 103.0, 'close': 104.0, 'volume': 1200},
        {'timestamp': '09:34', 'open': 104.0, 'high': 104.5, 'low': 103.5, 'close': 104.25, 'volume': 800},
    ]
    
    # Aggregate to 5-minute bar
    result = aggregate_to_5min(minute_data)
    
    # Validate EXACT mathematical calculations
    assert result['open'] == 100.0, f"5min open must be first minute open: {result['open']} != 100.0"
    assert result['high'] == 105.0, f"5min high must be max of minute highs: {result['high']} != 105.0"
    assert result['low'] == 99.5, f"5min low must be min of minute lows: {result['low']} != 99.5"
    assert result['close'] == 104.25, f"5min close must be last minute close: {result['close']} != 104.25"
    assert result['volume'] == 6500, f"5min volume must be sum: {result['volume']} != 6500"
    
    # Validate VWAP calculation precision
    expected_vwap = (100.0*1000 + 102.5*1500 + 103.5*2000 + 104.0*1200 + 104.25*800) / 6500
    assert abs(result['vwap'] - expected_vwap) < 0.01, f"VWAP calculation error: {result['vwap']} vs {expected_vwap}"
```

**✅ EXCELLENT: Exact Time Range Validation**
```python
def test_time_range_calculation_precision():
    """Validate EXACT time calculations down to the second."""
    
    test_timestamp = datetime(2024, 1, 15, 14, 30, 0, tzinfo=ZoneInfo("US/Eastern"))
    
    start_time, end_time = builder._get_market_data_time_range(test_timestamp)
    
    # Validate EXACT time calculations
    expected_start = datetime(2024, 1, 15, 13, 30, 0, tzinfo=ZoneInfo("US/Eastern"))
    expected_end = datetime(2024, 1, 15, 14, 30, 0, tzinfo=ZoneInfo("US/Eastern"))
    
    # ZERO tolerance for time discrepancies
    start_diff = abs((start_time.replace(tzinfo=None) - expected_start.replace(tzinfo=None)).total_seconds())
    end_diff = abs((end_time.replace(tzinfo=None) - expected_end.replace(tzinfo=None)).total_seconds())
    
    assert start_diff == 0, f"Start time calculation error: {start_time} vs {expected_start} (diff: {start_diff}s)"
    assert end_diff == 0, f"End time calculation error: {end_time} vs {expected_end} (diff: {end_diff}s)"
    
    # Validate EXACT window duration
    window_duration = end_time - start_time
    expected_duration = timedelta(hours=1)
    
    duration_diff = abs((window_duration - expected_duration).total_seconds())
    assert duration_diff == 0, f"Window duration error: {window_duration} vs {expected_duration} (diff: {duration_diff}s)"
    
    # Validate EXACT timezone handling
    assert start_time.tzinfo.zone == "US/Eastern", f"Start time wrong timezone: {start_time.tzinfo}"
    assert end_time.tzinfo.zone == "US/Eastern", f"End time wrong timezone: {end_time.tzinfo}"
```

**✅ EXCELLENT: Exact Parameter Validation**
```python
def test_market_data_request_parameters():
    """Validate EXACT parameters passed to underlying services."""
    
    symbols = ['AAPL', 'TSLA']
    start_date = '2024-01-15'
    end_date = '2024-01-15'
    timeframe = '1m'
    
    # Capture actual parameters sent to vendor API
    with patch('vendor_api.get_data') as mock_api:
        mock_api.return_value = expected_response
        
        result = await market_data_manager.get_ohlcv(symbols, start_date, end_date, timeframe)
        
        # Validate EXACT API call parameters
        mock_api.assert_called_once()
        actual_call = mock_api.call_args
        
        # Validate EXACT symbols list
        assert actual_call[1]['symbols'] == ['AAPL', 'TSLA'], f"Wrong symbols: {actual_call[1]['symbols']}"
        
        # Validate EXACT date format
        assert actual_call[1]['from'] == '2024-01-15T00:00:00-05:00', f"Wrong start date format: {actual_call[1]['from']}"
        assert actual_call[1]['to'] == '2024-01-15T23:59:59-05:00', f"Wrong end date format: {actual_call[1]['to']}"
        
        # Validate EXACT timeframe mapping
        assert actual_call[1]['multiplier'] == 1, f"Wrong multiplier: {actual_call[1]['multiplier']}"
        assert actual_call[1]['timespan'] == 'minute', f"Wrong timespan: {actual_call[1]['timespan']}"
        
        # Validate EXACT API key usage
        assert 'Authorization' in actual_call[1]['headers'], "Missing Authorization header"
        assert actual_call[1]['headers']['Authorization'].startswith('Bearer '), "Wrong auth format"
```

**🔥 CRITICAL SUCCESS CRITERIA:**

1. **ZERO Superficial Assertions**: Every `assert` must validate exact business values
2. **EXACT Error Validation**: Test specific exception types, messages, and error details
3. **PRECISE Mathematical Validation**: OHLCV calculations must be mathematically correct
4. **EXACT Time Validation**: Zero tolerance for time calculation errors
5. **SPECIFIC Parameter Validation**: Validate exact API parameters, not just that calls happen
6. **CONCRETE Business Logic**: Test actual business rules, not generic existence

#### **4️⃣ REAL OBJECTS ONLY (NO MOCK OBJECTS)**
- **❌ NEVER use unittest.mock.Mock() or @patch decorators**
- **❌ NEVER mock database connections, APIs, or business objects**
- **❌ NEVER use fake implementations that hide integration issues**
- **✅ ALWAYS use real instances with controlled test data**
- **✅ ALWAYS use dependency injection for real test objects**
- **✅ ALWAYS test actual integration between real components**

**Example Pattern:**
```python
# ❌ WRONG: Mock objects hide real integration issues
@patch('database_service.get_connection')
def test_universe_state_builder(mock_db):
    mock_db.return_value = Mock()
    builder = UniverseStateBuilder(mock_db)
    # Test passes but may fail in production

# ✅ RIGHT: Real objects with test data
async def test_universe_state_builder(unit_test_db):
    # Use real database with test schema
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    
    # Use real market data manager with test data
    market_data_manager = UnifiedMarketDataManager(test_data_dir)
    
    # Use real universe state builder
    builder = UniverseStateBuilder(
        environment=environment,
        market_data_manager=market_data_manager
    )
    
    # Test with real data flow
    result = await builder.build_universe_state('AAPL', test_timestamp)
    
    # Validate real business logic
    assert result.symbol == 'AAPL'
    assert result.ohlc.open > 0
    assert len(result.technical_indicators) == 16
```

### **🔍 TESTING ANTI-PATTERNS TO ELIMINATE**

**Superficial Testing:**
- ❌ `assert result` instead of `assert result.value == expected_value`
- ❌ `assert len(data) > 0` instead of `assert len(data) == expected_count`
- ❌ Testing that functions run without testing what they produce

**Mock Overuse:**
- ❌ Mocking core business objects that need real behavior validation
- ❌ Using mocks to avoid setting up test data
- ❌ Creating mock expectations that don't match real interface contracts

**Exception Suppression:**
- ❌ `try: test_function() except: pass` patterns that hide failures
- ❌ Generic exception handling that masks specific failure points
- ❌ Assertions inside try/catch blocks that can be skipped

**Test-After Development:**
- ❌ Writing tests after implementing features (not TDD)
- ❌ Writing tests just to increase coverage metrics
- ❌ Retrofitting tests that confirm existing behavior rather than drive design

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
- ❌ **Starting sessions without retrieving project context from knowledge graph**
- ❌ **Not storing important discoveries and solutions in persistent memory**
- ❌ **Losing institutional knowledge between development sessions**
- ❌ Running docker commands directly (use run_dev instead)
- ❌ Creating new services/files when existing can be enhanced
- ❌ Using mock/synthetic data outside unit tests
- ❌ Claiming functionality works without tests
- ❌ Writing tests after code (TDD requires tests first)
- ❌ Skipping Playwright testing for UX changes
- ❌ Not tracking training data generation in dev_runs table
- ❌ **Using fragmented monitoring systems (coverage_monitor.py, validation scripts)**
- ❌ **Creating separate issue detection instead of using unified service**
- ❌ **Building standalone dashboards when unified dashboard exists**

**Data Quality & Monitoring:**
- ❌ **Using deprecated coverage monitoring scripts (use unified service)**
- ❌ **Creating separate validation systems (use unified validation)**
- ❌ **Building standalone alert systems (use unified alert manager)**
- ❌ **Using deprecated database tables (use unified schema)**
- ❌ **Implementing separate issue resolution (use unified workflows)**

**Validation & Testing - CURRENT RUN ONLY:**
- ❌ **NEVER check prior runs/historical data** to claim current run success
- ❌ **NEVER use old successful results** to validate current failures
- ❌ **NEVER claim "working" based on previous executions**
- ❌ Only the MOST RECENT execution results are relevant for validation
- ✅ **ALWAYS validate current run outputs** - files, database records, actual results
- ✅ **CURRENT FAILURE = CURRENT PROBLEM** - debug the actual failing case
- ✅ **Test-first debugging**: Add test to reproduce → Fix logic → Confirm with passing test

**Debugging & Problem Solving - MANDATORY TEST-FIRST APPROACH:**
- ❌ Using workarounds without understanding root cause
- ❌ Restarting services without checking logs first
- ❌ Switching environments when current environment fails
- ❌ Ignoring error messages and trying random fixes
- ❌ Fixing symptoms instead of root causes
- **❌ CRITICAL: Fixing issues without reproducing them in tests FIRST**
- **✅ MANDATORY: When failure detected → Add test to reproduce → Fix logic → Verify with passing test**

## 🎯 **Success Criteria**

**You're following best practices when you:**
- **Use MCP Knowledge Graph for persistent memory and context continuity**
- **Store discoveries, solutions, and configurations in knowledge graph**
- **Retrieve project context at session start and document findings at session end**
- Use run_dev for all development operations
- Write failing tests before code changes
- Run integration tests and see them pass
- Enhance existing services instead of creating new ones
- Use real data only - no mock/synthetic data outside tests
- Track all training data generation in dev_runs table
- Test ALL UX changes with Playwright before claiming success
- Investigate logs before restarting services
- Understand root causes before implementing fixes
- Document investigation findings in commits/issues AND knowledge graph
- **Use unified data quality service for all monitoring and validation**
- **Leverage consolidated architecture instead of creating fragmented systems**
- **Follow service consolidation patterns established by unified framework**
- **Use shared DTOs and repositories across all quality operations**
- **Implement consistent issue detection, classification, and resolution workflows**

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

## 🔄 **CRITICAL: Unified Data Quality Framework**

**✅ USE unified data quality service for all monitoring, validation, and issue management**

### **Unified Data Quality Architecture:**

```
🔄 CONSOLIDATED MONITORING FLOW:
1. Unified Data Quality Service (Single Interface)
   ↓
2. Coverage Monitoring + Validation + Agent (Integrated)
   ↓
3. Issue Detection + Classification + Resolution (Automated)
   ↓
4. Unified Database + Alerts + Dashboard (Consolidated)
```

**🔹 SERVICE CONTAINER:** `UnifiedDataQualityServiceContainer` - Single entry point
**🔹 UNIFIED DATABASE:** `dev_data_quality_issues` + `dev_data_quality_metrics`
**🔹 AGENT INTEGRATION:** Enhanced agent with coverage + validation capabilities

**Data Quality Commands:**
```python
# Initialize unified service
from domains.data_quality.services.config.unified_data_quality_service_container import UnifiedDataQualityServiceContainer
container = UnifiedDataQualityServiceContainer("dev")
await container.initialize()
unified_service = await container.get_unified_service()

# Start monitoring (coverage + validation + agent)
await container.start_monitoring()

# Detect all issues across categories
all_issues = await unified_service.detect_all_issues(IssueDetectionRequest(
    categories=[IssueCategory.COVERAGE, IssueCategory.VALIDATION]
))

# Get unified dashboard data
dashboard_data = await unified_service.get_dashboard_data()
```

## 📚 **Additional Documentation**

- **[START_HERE.md](docs/START_HERE.md)** - 15-minute setup and core concepts
- **[DEVELOPMENT.md](docs/DEVELOPMENT.md)** - Complete development guide
- **[OPERATIONS.md](docs/OPERATIONS.md)** - Daily operations and troubleshooting
- **[INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md)** - Database connections and service architecture

---

**🔥 This is a Docker-first, test-driven, DEBUG-FIRST development platform. Every change must be validated end-to-end with REAL DATA ONLY. When systems fail, investigate and understand before implementing workarounds.**

**🚫 ZERO TOLERANCE for file proliferation, superficial testing, and premature claims of completion. Excellence is non-negotiable.**