# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Python Dependencies & Environment
```bash
# Install dependencies using uv (recommended)
uv pip install -r requirements.txt

# Or install with dev dependencies
uv sync
```

### Testing
```bash
# Run all tests with proper Python path
PYTHONPATH=src uv run pytest -q

# Run specific test markers
PYTHONPATH=src uv run pytest -m unit
PYTHONPATH=src uv run pytest -m integration
PYTHONPATH=src uv run pytest -m database

# Run with verbose output
PYTHONPATH=src uv run pytest -v --tb=short

# Test specific functionality (integration tests)
PYTHONPATH=src uv run pytest tests/integration/ -v
```

### Testing Best Practices (CRITICAL)
**EVERY change must follow Test-Driven Development (TDD):**

1. **Test Before Fix**: If there's an issue, FIRST create a test that reproduces the failure
2. **Test Before Code**: Every new feature must have a test written first that fails
3. **Test After Change**: Every change must pass all relevant tests before claiming success
4. **Integration Testing**: Always test actual service startup, not just unit tests

```bash
# Example workflow for fixing a bug:
# 1. Write a test that reproduces the bug (should fail)
PYTHONPATH=src pytest tests/integration/test_bug_reproduction.py -v

# 2. Fix the code
# (make your changes)

# 3. Verify the test now passes
PYTHONPATH=src pytest tests/integration/test_bug_reproduction.py -v

# 4. Run full test suite to ensure no regressions
PYTHONPATH=src pytest tests/ -v
```

**Integration Test Examples:**
```bash
# Test actual service startup (catches real issues)
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestAnalyticsPlatformIntegration::test_backend_api_can_start -v

# Test database connectivity (catches auth issues)  
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestRealWorldScenarios::test_database_connectivity -v

# Test frontend dependencies (catches npm issues)
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py::TestAnalyticsPlatformIntegration::test_frontend_dependencies_can_install -v
```

**NEVER claim functionality works without:**
- [ ] Writing a test that verifies the claim
- [ ] Running the test and seeing it pass
- [ ] Testing in a clean environment
- [ ] Verifying actual URLs/services respond

### Database Operations
```bash
# Run database migrations
uv run python src/db/migration_manager.py

# Set up database schema
uv run python src/db/setup_trading_db.py

# Test database connection (dev environment)
uv run python scripts/database/test_ats_dev_db_connection.py

# Verify database setup
uv run python scripts/database/verify_db_setup.py
```

### Kubernetes Job Generation
```bash
# Generate instrument polygon backfill job
uv run python scripts/kubernetes/k8s_job_generator.py --job-type backfill --output k8s/generated/instrument-polygon-backfill.yaml

# Run job directly with apply
uv run python scripts/kubernetes/run_k8s_job.py --apply --file k8s/generated/instrument-polygon-backfill.yaml

# Generate custom instrument job
uv run python scripts/kubernetes/instrument_polygon_job_generator.py --symbols AAPL,MSFT --output k8s/generated/custom-job.yaml
```

### Flyte Workflows
```bash
# Run Flyte instrument polygon workflow
uv run python scripts/flyte/flyte_instrument_polygon_workflow.py --job-type backfill --apply --output-dir k8s/generated
```

### Application Startup
```bash
# Start FastAPI server locally
uvicorn src.main:app --reload

# Start with Docker Compose
docker-compose up --build
```

## Architecture Overview

### Core Structure
```
src/
├── db/                    # Database setup, migrations, and utilities
│   ├── migration_manager.py     # Primary migration handler
│   ├── migrations/              # SQL migration files
│   └── environment_migration.py # Environment migration support
├── dao/                   # Data Access Objects for all entities
├── config/                # Configuration management
│   ├── environment.py     # Environment-aware table naming
│   ├── database.py        # Database connection configuration
│   └── polygon.py         # API configuration
├── secmaster/            # Security master data management
│   └── populate_instrument_polygon.py  # Main instrument population job
├── market_data/          # Market data ingestion and processing
│   ├── agent/            # Data agent components for real-time processing
│   └── eod/              # End-of-day data processing
├── events/               # Event system with multi-source ingestion
├── universe/             # Universe management (SPY, custom)
├── state/                # State management and intervals
├── signals/              # Technical indicators and signal computation
└── main.py               # FastAPI application entrypoint
```

### Key Components

**Database Layer:**
- Uses PostgreSQL with TimescaleDB for time-series data
- Environment-aware table prefixing (dev_, intg_, prod_)
- Migration system with `db_version` tracking
- Connection pooling and retry logic

**Secmaster Jobs:**
- `populate_instrument_polygon.py` - Main instrument data ingestion using Ray for parallel processing
- Environment variables control Ray autoscaling: `RAY_SCHEDULER_EVENTS=0`, `RAY_DISABLE_AUTOMATIC_AUTOSCALING=1`

**Event System:**
- Unified event ingestion from multiple sources (Polygon, Finnhub, FMP, etc.)
- Reconciliation logic for multi-source data merging
- REST API endpoints for event querying and addition

**Kubernetes Integration:**
- Job generators for parameterized Kubernetes jobs
- Flyte workflows for orchestration
- GitOps deployment with Argo CD
- Environment-specific configurations (dev/intg/prod)

### Environment Management
- Configuration via `.env.dev`, `.env.test`, `.env.prod` files
- Environment-aware database table naming using `env.get_table_name()`
- Kubernetes secrets management for API keys and database credentials
- Support for local, Docker, and Kubernetes deployments

### Data Processing Patterns
- Ray for parallel processing in instrument population jobs
- Pandas for data manipulation and technical indicators
- PyTorch for deep learning forecasting models
- Multi-duration interval support (5m, 15m, 1h, 1d)

## Development Guidelines

### Test-Driven Development (TDD) Workflow - MANDATORY

**EVERY change must follow this exact workflow:**

#### 1. **Red Phase** - Write Failing Test First
```bash
# Before fixing any bug or adding any feature, write a test that fails
# Example: If service startup is broken, write a test that tries to start it

# Create test file
touch tests/integration/test_new_feature.py

# Write test that reproduces the issue/tests the new feature
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v
# ✅ Test should FAIL - this proves you can detect the issue
```

#### 2. **Green Phase** - Fix The Code
```bash
# Now fix the actual code to make the test pass
# Make minimal changes needed to pass the test
# Don't add extra features

# Run the test again
PYTHONPATH=src pytest tests/integration/test_new_feature.py -v  
# ✅ Test should now PASS
```

#### 3. **Refactor Phase** - Clean Up
```bash
# Clean up code while keeping tests passing
# Run full test suite to prevent regressions
PYTHONPATH=src pytest tests/ -v
# ✅ All tests should still pass
```

#### 4. **Integration Verification**
```bash
# Test actual service functionality (not just unit tests)
# For web services: test actual HTTP endpoints
# For databases: test actual connections
# For frontends: test actual browser loading

# Example integration tests:
PYTHONPATH=src pytest tests/integration/test_analytics_platform_integration.py -v
```

**Critical Rules:**
- 🚫 **NEVER** claim something works without a passing test
- 🚫 **NEVER** fix code without first writing a failing test  
- 🚫 **NEVER** write tests after the code (except for legacy code)
- ✅ **ALWAYS** write the test first, see it fail, then fix
- ✅ **ALWAYS** test actual integration (services, databases, APIs)
- ✅ **ALWAYS** verify URLs actually work in a browser

### Database Schema Changes
1. Create new migration SQL file in `src/db/migrations/` with sequential numbering
2. Update `src/db/migration_manager.py` if needed
3. Test migration with `uv run python src/db/migration_manager.py`
4. Use environment-aware table names: `env.get_table_name("base_name")`

### Adding New Data Sources
1. Create new ingest module in `src/events/ingest/`
2. Implement source-specific fetcher following existing patterns
3. Add to unified pipeline for reconciliation
4. Update API schemas if needed

### Kubernetes Job Development
1. Use job generators in `scripts/kubernetes/` for consistency
2. Set proper Ray environment variables for non-autoscaling mode
3. Include `PYTHONPATH=src` and `DB_CONNECTION_PARAMS=sslmode=disable`
4. Test with dry-run before applying: `--dry-run`

### Testing Approach
- Unit tests for core business logic
- Integration tests with shared database
- Database-specific tests with proper fixtures
- Use pytest markers: `@pytest.mark.unit`, `@pytest.mark.database`, `@pytest.mark.integration`

## Important Notes

### Development Rules (CRITICAL)
- **ALWAYS use Test-Driven Development** - Write failing test first, then fix
- **NEVER claim functionality works** without passing integration tests
- **ALWAYS test actual service startup** - not just unit tests
- **ALWAYS verify URLs work in browser** before claiming success
- **Test database connectivity** before claiming backend works
- **Test npm install success** before claiming frontend works

### Technical Notes
- Always use `PYTHONPATH=src` when running Python scripts
- Database connections require `sslmode=disable` in Kubernetes environments
- Ray jobs should use local mode in Kubernetes to avoid autoscaling issues
- Environment table prefixing is critical for multi-environment deployments
- All secrets/API keys are managed through environment variables and Kubernetes secrets

### Testing Expectations
- **Integration tests are mandatory** for any new service or feature
- **Tests must actually start services** to catch real startup issues
- **Tests must verify external dependencies** (database, npm packages)
- **Tests must fail when things are broken** - no false positives
- **Every bug fix must start with a reproducing test**

### Kubernetes Verification Process
- **ALWAYS test jobs in actual cluster** before claiming they work
- **Verify secret configurations** match actual Kubernetes secret keys
- **Monitor pod logs** to catch startup failures and configuration errors
- **Test database connectivity** from within Kubernetes environment
- **Only claim success** when `kubectl logs` shows successful execution

## Fintech Team Roles and Responsibilities

### Product Manager

**Responsibilities:**
- Design a portfolio GPT product that generates stock recommendations hourly for paid customers
- Develop product strategy for a multi-modal transformer-based model using news and market data
- Define requirements for interpretable forecasts showing price trajectories over 1-5 days
- Coordinate between data science teams (leveraging DeepSeek or OpenAI OSS models) and engineering
- Design subscription tiers and premium features for the paid customer base
- Establish metrics to measure forecast accuracy, customer retention, and revenue growth
- Ensure regulatory compliance for algorithmic trading recommendations
- Create product roadmap for continuous model improvement and feature expansion

**Prompt Template:**
```
As a Product Manager for our portfolio GPT platform, help me [task]. Consider:
- Model architecture decisions for multi-modal transformers processing [specific data types]
- Forecast presentation formats for [time horizon] price trajectories
- Interpretability requirements for [target user persona]
- Regulatory compliance for automated investment recommendations in [jurisdiction]
- Subscription model optimization for [customer segment]
- Performance metrics for measuring forecast accuracy and customer value
- Competitive differentiation from [similar robo-advisor products]
```

### Backend Engineer

**Responsibilities:**
- Design and implement the recommendation engine API for the portfolio GPT product
- Develop high-performance systems for real-time market data processing and model inference
- Create scalable infrastructure to handle hourly forecast generation for all subscribed users
- Implement secure authentication and authorization for tiered subscription access
- Design caching strategies for optimizing model inference and reducing latency
- Build monitoring systems to track model performance and recommendation accuracy
- Develop APIs for integrating with brokerage platforms for direct trading actions
- Implement rate limiting and quota management for different subscription tiers

**Prompt Template:**
```
As a Backend Engineer for our portfolio GPT platform, help me [task]. Consider:
- Scalability requirements for handling [number] of concurrent forecast requests
- Real-time processing architecture for [data source] market data
- Model serving infrastructure for [model type] transformer inference
- API design for [subscription tier] recommendation delivery
- Authentication and rate limiting for [user type] access patterns
- Caching strategies for optimizing [forecast type] generation
- Integration patterns with [brokerage platform] for trade execution
- Monitoring and alerting for [performance metric] thresholds
```

### Data Engineer

**Responsibilities:**
- Design and implement data pipelines for multi-modal market data and news feeds
- Develop real-time ingestion systems for hourly model training and inference
- Create feature stores optimized for transformer model consumption
- Build data quality validation systems for financial news sentiment analysis
- Implement time-series data storage optimized for price trajectory forecasting
- Design data versioning systems to track model inputs for regulatory compliance
- Develop data enrichment pipelines to combine market data with alternative data sources
- Create efficient data retrieval systems for low-latency model inference

**Prompt Template:**
```
As a Data Engineer for our portfolio GPT platform, help me [task]. Consider:
- Real-time data pipeline architecture for [market data feed/news source]
- Feature engineering for [transformer model] consumption
- Data quality validation for [financial news/market data] sentiment analysis
- Storage optimization for [time horizon] price trajectory forecasting
- Data versioning for [regulatory requirement] compliance
- Integration patterns with [alternative data source]
- Caching strategies for [forecast frequency] model inference
- Data enrichment techniques for [specific signal type] extraction
```

### Release Engineer

**Responsibilities:**
- Design and maintain CI/CD pipelines for hourly model deployment and inference services
- Implement blue-green deployment strategies for zero-downtime recommendation updates
- Create infrastructure as code for scalable transformer model serving environments
- Ensure 99.99% reliability for premium subscription recommendation delivery
- Coordinate versioned releases of model updates with backward compatibility
- Implement comprehensive monitoring for model performance and drift detection
- Design automated rollback mechanisms for degraded recommendation quality
- Establish model validation gates in the deployment pipeline

**Prompt Template:**
```
As a Release Engineer for our portfolio GPT platform, help me [task]. Consider:
- Deployment strategy for [model version/inference service]
- CI/CD pipeline optimization for [hourly/daily] recommendation generation
- Infrastructure as code for [transformer model] serving environment
- Rollback procedures for [recommendation quality degradation]
- Monitoring and alerting for [model drift/inference latency]
- Compliance requirements for [financial recommendation audit trail]
- Scaling strategy for [peak usage period] traffic patterns
- Validation gates for [model accuracy/performance] metrics
```

### Frontend Engineer

**Responsibilities:**
- Develop responsive dashboard for displaying hourly stock recommendations and price trajectories
- Implement interactive data visualizations for 1-5 day price forecasts with confidence intervals
- Create intuitive interfaces for displaying model interpretations of market and news signals
- Design subscription management UI with tiered access controls and feature gates
- Build real-time notification systems for high-confidence trading opportunities
- Implement secure authentication flows with multi-factor authentication for premium users
- Create mobile-responsive interfaces for on-the-go trading recommendations
- Design watchlist functionality for tracking personalized stock recommendations

**Prompt Template:**
```
As a Frontend Engineer for our portfolio GPT platform, help me [task]. Consider:
- User experience for [recommendation display/watchlist feature]
- Interactive data visualization for [price trajectory/confidence interval] charts
- Performance optimization for [real-time recommendation updates]
- Accessibility requirements for [financial data interpretation]
- State management for [subscription tier feature access]
- Security considerations for [trading action integration]
- Mobile responsiveness for [critical trading notification]
- User onboarding flow for [subscription tier] customers
```

### Oncall Support

**Responsibilities:**
- Monitor model inference services and hourly recommendation generation jobs
- Respond to alerts for recommendation quality degradation or model drift
- Diagnose and resolve data pipeline failures affecting forecast generation
- Monitor subscription tier access and resolve authentication issues for premium users
- Perform root cause analysis for incorrect stock recommendations or price trajectories
- Implement circuit breakers for extreme market volatility scenarios
- Maintain audit logs for regulatory compliance of financial recommendations
- Coordinate with data providers during market data feed disruptions

**Prompt Template:**
```
As an Oncall Support Engineer for our portfolio GPT platform, help me [task]. Consider:
- Diagnostic approach for [model drift/recommendation quality] alert
- Impact assessment for [hourly forecast generation] disruption
- Mitigation steps for [data pipeline/model inference] failure
- Communication plan for [premium tier customers/regulatory bodies]
- Root cause analysis for [incorrect price trajectory/recommendation]
- Circuit breaker implementation for [market volatility scenario]
- Audit logging requirements for [financial recommendation compliance]
- Coordination plan for [market data feed] disruption
```

### Model Developer

**Responsibilities:**
- Design and implement multi-modal transformer models for stock price trajectory forecasting
- Develop evaluation frameworks to measure prediction accuracy and confidence intervals
- Create comprehensive backtesting systems for model validation across market regimes
- Generate risk reports analyzing model performance under various market conditions
- Implement model explainability techniques for regulatory compliance and user trust
- Optimize model inference for hourly prediction generation at scale
- Research and integrate novel approaches for financial news sentiment analysis
- Develop ensemble methods to combine multiple signal sources for robust predictions

**Prompt Template:**
```
As a Model Developer for our portfolio GPT platform, help me [task]. Consider:
- Architecture design for [multi-modal transformer/specific model type]
- Feature importance analysis for [market data/news sentiment] signals
- Evaluation metrics for [time horizon] price trajectory forecasts
- Backtesting methodology across [specific market regime/volatility period]
- Risk quantification for [recommendation type/market sector]
- Model explainability techniques for [target audience/regulatory requirement]
- Inference optimization for [latency/throughput] requirements
- Ensemble strategy for [signal combination/model diversity]
```
- PGPASSWORD=postgres should be dev_password