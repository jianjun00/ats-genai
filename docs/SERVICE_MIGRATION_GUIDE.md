# Service Architecture Migration Guide

Complete guide for migrating from DAO-based to service-based architecture using the automated migration tools.

## 🎯 Overview

This migration system provides comprehensive automation for transforming your ATS platform from DAO-based to service-based architecture. It includes:

- **Code Migration**: AST-based transformation of DAO patterns to service interfaces
- **Database Migration**: Schema updates for service-specific requirements  
- **Configuration Migration**: Environment and deployment configuration transformation
- **Test Migration**: Test suite transformation with service-specific patterns
- **Orchestration**: Coordinated execution with rollback capabilities

## 🏗️ Architecture Components

### Core Migration Components

```
src/infrastructure/migration/
├── migration_orchestrator.py     # Main orchestration engine
├── code_migrator.py             # AST-based code transformation
├── database_migrator.py         # Database schema migration
├── config_migrator.py           # Configuration transformation
└── test_migrator.py             # Test suite migration
```

### CLI Interface

```
scripts/migrate_to_services.py   # Command-line interface for all operations
```

## 🚀 Quick Start

### 1. Plan Your Migration

Create a comprehensive migration plan:

```bash
# Plan migration for all services
python scripts/migrate_to_services.py plan --output-file migration_plan.json

# Plan migration for specific services
python scripts/migrate_to_services.py plan --services instruments market_data --output-file plan.json
```

### 2. Execute Migration (Dry Run)

Test the migration without making changes:

```bash
# Execute dry run
python scripts/migrate_to_services.py execute --plan-file migration_plan.json --dry-run
```

### 3. Execute Real Migration

Execute the actual migration:

```bash
# Execute with confirmation
python scripts/migrate_to_services.py execute --plan-file migration_plan.json --confirm

# Execute interactively (will prompt for confirmation)
python scripts/migrate_to_services.py execute --plan-file migration_plan.json
```

### 4. Validate Results

Validate the migration was successful:

```bash
# Validate migration
python scripts/migrate_to_services.py validate --migration-id migration_20241201_120000
```

## 📋 Migration Phases

The migration system executes the following phases in order:

### 1. Preparation Phase
- Verifies prerequisites
- Creates backup directories
- Validates environment

### 2. Code Analysis Phase
- Analyzes existing codebase using AST
- Identifies DAO patterns and business logic
- Calculates migration complexity

### 3. Database Schema Migration Phase
- Generates service-specific database migrations
- Creates service infrastructure tables
- Applies schema changes with rollback support

### 4. Code Migration Phase
- Transforms DAO classes to service interfaces
- Generates service implementations
- Creates cached service wrappers
- Updates imports and dependencies

### 5. Configuration Migration Phase
- Creates service-specific configuration files
- Migrates environment variables
- Generates Docker and Kubernetes configurations
- Sets up service discovery configuration

### 6. Test Migration Phase
- Transforms existing tests for service architecture
- Creates comprehensive test suites (unit, integration, API, performance)
- Generates shared test utilities and fixtures
- Creates CI/CD test scripts

### 7. Integration Validation Phase
- Validates service interactions
- Tests API endpoint functionality
- Verifies database connectivity

### 8. Performance Validation Phase
- Runs performance benchmarks
- Validates response time requirements
- Tests memory usage and resource consumption

### 9. Deployment Preparation Phase
- Creates deployment artifacts
- Generates container configurations
- Prepares monitoring and logging setup

## 🔧 Configuration Options

### Database Configuration

```bash
# Use custom database URL
python scripts/migrate_to_services.py plan --database-url "postgresql://user:pass@host:5432/db"
```

### Directory Configuration

```bash
# Specify custom directories
python scripts/migrate_to_services.py plan \
  --source-dir custom_src \
  --config-dir custom_config \
  --test-dir custom_tests
```

### Migration Options

```bash
# Continue on failure (don't stop at first failed phase)
python scripts/migrate_to_services.py execute --continue-on-failure --plan-file plan.json

# Execute specific services only
python scripts/migrate_to_services.py execute --services instruments market_data
```

## 🔄 Rollback Operations

The migration system provides comprehensive rollback capabilities:

### Complete Rollback

```bash
# Rollback entire migration
python scripts/migrate_to_services.py rollback --migration-id migration_20241201_120000 --confirm
```

### Partial Rollback

```bash
# Rollback to specific phase
python scripts/migrate_to_services.py rollback \
  --migration-id migration_20241201_120000 \
  --target-phase code_migration \
  --confirm
```

## 📊 Status Monitoring

### Check Migration Status

```bash
# Check current migration status
python scripts/migrate_to_services.py status

# Check specific migration status
python scripts/migrate_to_services.py status --migration-id migration_20241201_120000
```

### View Migration Reports

Migration reports are automatically saved to:
- `migration_report_{migration_id}.json` - Comprehensive migration results
- `migration.log` - Detailed execution logs

## 🧪 Generated Service Architecture

The migration transforms your codebase to this structure:

```
src/
├── domains/
│   ├── instruments/
│   │   ├── services/
│   │   │   ├── interfaces/
│   │   │   │   └── instrument_service_interface.py
│   │   │   ├── implementations/
│   │   │   │   └── instrument_service.py
│   │   │   └── cached/
│   │   │       └── cached_instrument_service.py
│   │   ├── repositories/          # Existing DAOs remain
│   │   └── models/               # Existing models remain
│   ├── market_data/              # Same structure
│   ├── analytics/                # Same structure
│   └── user_management/          # Same structure
├── infrastructure/
│   ├── caching/                  # Multi-layer caching system
│   ├── service_discovery/        # Service registry and discovery
│   ├── dependency_injection/     # Service container
│   └── optimization/             # Performance profiling
└── api/
    └── routers/                  # FastAPI routers for each service
```

## 🧪 Test Architecture

The migration creates comprehensive test suites:

```
tests/services/
├── instruments/
│   ├── test_instruments_unit.py        # Unit tests with mocks
│   ├── test_instruments_integration.py # Integration tests
│   ├── test_instruments_api.py         # API endpoint tests
│   └── test_instruments_performance.py # Performance benchmarks
├── market_data/                        # Same structure for each service
├── utilities/
│   └── test_utilities.py              # Shared test utilities
└── conftest.py                         # Pytest configuration
```

## 📦 Configuration Architecture

Service-specific configurations are generated:

```
config/services/
├── instruments/
│   ├── instruments_config.yaml         # Service configuration
│   ├── instruments.env                 # Environment variables
│   ├── instruments.dev.yaml            # Development config
│   ├── instruments.staging.yaml        # Staging config
│   └── instruments.prod.yaml           # Production config
├── docker/
│   └── docker-compose.instruments.yml  # Docker configuration
├── kubernetes/
│   └── instruments-deployment.yaml     # Kubernetes manifests
└── service_discovery.yaml             # Service registry config
```

## ⚡ Performance Optimizations

The migrated architecture includes:

### Multi-Layer Caching
- **L1**: Memory cache for fastest access
- **L2**: Redis cache for shared data
- **L3**: Database cache for query optimization

### Service Discovery
- Automatic service registration
- Health check monitoring
- Load balancing strategies

### Performance Monitoring
- Operation profiling with metrics collection
- Memory and CPU usage tracking
- Performance recommendations

## 🔍 Validation and Quality Assurance

### Code Quality Validation
- AST-based code analysis
- Import dependency verification
- Service interface compliance

### Database Validation
- Schema integrity checks
- Migration rollback verification
- Data consistency validation

### Configuration Validation
- YAML syntax verification
- Environment variable completeness
- Service dependency validation

### Test Coverage Validation
- Test file structure verification
- Coverage analysis and reporting
- Test execution validation

## 🚨 Troubleshooting

### Common Issues

#### Migration Fails During Code Phase
```bash
# Check analysis results
grep "DAO analysis" migration.log

# Validate source directory structure
ls -la src/domains/
```

#### Database Migration Issues
```bash
# Check database connectivity
python -c "import asyncpg; print('DB connection available')"

# Validate migration status
python scripts/migrate_to_services.py status --migration-id YOUR_ID
```

#### Configuration Issues
```bash
# Validate generated configs
find config/services -name "*.yaml" -exec yaml_check {} \;

# Check environment variables
grep -r "MISSING" config/services/
```

### Recovery Procedures

#### Partial Migration Recovery
1. Check migration status to identify failed phase
2. Fix underlying issue (database connection, file permissions, etc.)
3. Re-run migration with `--continue-on-failure`

#### Complete Recovery via Rollback
1. Use rollback command to restore previous state
2. Fix root cause issues
3. Re-plan and re-execute migration

### Validation Failures
1. Run validation command to get detailed error report
2. Address specific validation issues
3. Re-run validation until all checks pass

## 📈 Best Practices

### Pre-Migration
- ✅ Ensure all tests pass
- ✅ Commit all changes to version control
- ✅ Create database backup
- ✅ Run in development environment first
- ✅ Plan migration during low-traffic period

### During Migration
- ✅ Monitor migration logs for issues
- ✅ Keep terminal session active
- ✅ Have rollback plan ready
- ✅ Test services as they become available

### Post-Migration
- ✅ Run comprehensive validation
- ✅ Execute integration test suite
- ✅ Monitor performance metrics
- ✅ Update team documentation
- ✅ Train team on new service architecture

## 📚 Advanced Usage

### Custom Service Templates
Extend the migration system for custom service types:

```python
# Add custom service template
self.service_config_templates['custom_service'] = ServiceConfig(
    service_name='custom_service',
    service_type='custom_type',
    # ... configuration
)
```

### Custom Migration Phases
Add custom phases to the migration pipeline:

```python
# In migration orchestrator
async def _execute_custom_phase(self, migration_plan: MigrationPlan) -> Dict[str, Any]:
    # Custom migration logic
    return {"phase": "custom", "status": "completed"}
```

### Integration with CI/CD
Integrate migration into your deployment pipeline:

```yaml
# .github/workflows/migrate.yml
- name: Run Service Migration
  run: |
    python scripts/migrate_to_services.py execute \
      --plan-file migration_plan.json \
      --confirm \
      --continue-on-failure
```

## 🔗 Related Documentation

- [Service Architecture Design](SERVICE_ARCHITECTURE.md)
- [Caching Infrastructure](CACHING_GUIDE.md)
- [Performance Optimization](PERFORMANCE_GUIDE.md)
- [Testing Strategy](TESTING_GUIDE.md)
- [Deployment Guide](DEPLOYMENT_GUIDE.md)

## 🆘 Support

For issues or questions:
1. Check migration logs: `migration.log`
2. Review validation results
3. Consult troubleshooting section
4. Use rollback if necessary
5. Re-plan migration with adjustments