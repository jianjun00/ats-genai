# Database Management (`src/db/`)

This directory contains the database setup, migration management, and schema maintenance system for the ATS-GenAI trading platform.

## Overview

The database system provides:
- **Automated Migration Management** with versioning and rollback support
- **Environment-Aware Schema** with automatic table prefixing (`dev_`, `intg_`, `prod_`)
- **TimescaleDB Integration** for time-series data optimization
- **Comprehensive Schema** covering all trading entities and market data
- **Data Quality Validation** and integrity checks

## Directory Structure

```
db/
├── migration_manager.py            # Core migration management system
├── environment_migration.py        # Environment-aware migration support
├── setup_trading_db.py            # Complete database setup automation
├── migrations/                     # SQL migration files (30+ migrations)
│   ├── 000_initial_db_version.sql # Version tracking setup
│   ├── 001_initial_schema.sql     # Core trading schema
│   ├── 002_add_stock_splits_table.sql
│   ├── 017_add_polygon_tables.sql # Vendor-specific tables
│   ├── 031_create_economic_events_tables.sql
│   └── 033_create_minute_bars_table.sql
├── create_*.py                     # Individual table creation scripts
├── test_*.py                       # Database testing utilities
└── fixture_*.py                    # Test data management
```

## Core Components

### 🗄️ **Migration Manager** (`migration_manager.py`)

Central migration management system with version control:

```python
from db.migration_manager import MigrationManager

# Initialize migration manager
manager = MigrationManager()

# Run all pending migrations
manager.migrate()

# Check current database version
current_version = manager.get_current_version()
print(f"Database version: {current_version}")

# Rollback to specific version
manager.rollback_to_version(25)

# Validate database integrity
validation_results = manager.validate_database()
```

**Key Features:**
- **Sequential Migration Execution**: Ensures proper order and dependencies
- **Version Tracking**: `db_version` table tracks applied migrations
- **Rollback Support**: Safe rollback to previous versions
- **Validation**: Schema integrity and constraint validation
- **Environment Isolation**: Separate migrations per environment

### 🌍 **Environment-Aware Setup** (`environment_migration.py`)

Multi-environment database management:

```python
from db.environment_migration import EnvironmentMigration
from config.environment import Environment

# Environment-specific migration
env = Environment()  # Detects dev/test/intg/prod
migration = EnvironmentMigration(env)

# Creates tables with proper prefixes:
# dev_daily_prices, intg_daily_prices, prod_daily_prices
migration.setup_environment_schema()

# Environment-specific data seeding
migration.seed_reference_data()
```

**Key Features:**
- **Automatic Table Prefixing**: Environment-based table naming
- **Isolated Environments**: Complete separation between dev/test/prod
- **Reference Data Management**: Environment-specific seed data
- **Configuration Integration**: Uses centralized config system

### 🔧 **Database Setup** (`setup_trading_db.py`)

Complete database initialization and setup:

```python
from db.setup_trading_db import setup_complete_database

# One-command database setup
setup_complete_database(
    environment='dev',
    create_extensions=True,    # TimescaleDB, UUID, etc.
    run_migrations=True,       # Apply all migrations
    seed_data=True,           # Load reference data
    validate_setup=True       # Verify setup integrity
)

# Setup verification
from db.setup_trading_db import verify_database_setup
verification_report = verify_database_setup()
```

**Key Features:**
- **One-Command Setup**: Complete database initialization
- **Extension Management**: TimescaleDB, UUID, PostGIS extensions
- **Data Seeding**: Reference data and test fixtures
- **Setup Validation**: Comprehensive setup verification

## Database Schema Overview

### 📊 **Market Data Tables**
```sql
-- Daily price data with TimescaleDB optimization
{env}_daily_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(12,4),
    high DECIMAL(12,4),
    low DECIMAL(12,4),
    close DECIMAL(12,4),
    volume BIGINT,
    adjusted_close DECIMAL(12,4),
    vendor VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Minute-level market data
{env}_minute_bars (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    open DECIMAL(12,4),
    high DECIMAL(12,4),
    low DECIMAL(12,4),
    close DECIMAL(12,4),
    volume BIGINT,
    vwap DECIMAL(12,4)
);
```

### 🏢 **Instrument Management**
```sql
-- Core instrument data
{env}_instruments (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE,
    name VARCHAR(255),
    sector VARCHAR(100),
    market_cap BIGINT,
    is_active BOOLEAN DEFAULT TRUE
);

-- Cross-reference mappings
{env}_instrument_xrefs (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER REFERENCES {env}_instruments(id),
    vendor VARCHAR(20),
    vendor_symbol VARCHAR(20),
    mapping_type VARCHAR(50)
);
```

### 💰 **Corporate Actions**
```sql
-- Stock splits
{env}_stock_splits (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    split_date DATE,
    split_ratio DECIMAL(10,6),
    vendor VARCHAR(20)
);

-- Dividend payments
{env}_dividends (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    ex_date DATE,
    payment_date DATE,
    amount DECIMAL(10,4),
    vendor VARCHAR(20)
);
```

### 📅 **Event System**
```sql
-- Generic event storage
{env}_events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50),
    symbol VARCHAR(10),
    event_date TIMESTAMP,
    data JSONB,
    source VARCHAR(50)
);

-- Economic events
{env}_economic_events (
    id SERIAL PRIMARY KEY,
    event_name VARCHAR(255),
    event_date TIMESTAMP,
    country VARCHAR(10),
    importance VARCHAR(20),
    actual_value DECIMAL(15,4),
    forecast_value DECIMAL(15,4)
);
```

### 🏛️ **State Management**
```sql
-- Universe state intervals
{env}_universe_state_interval (
    id SERIAL PRIMARY KEY,
    universe_id INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    state_data JSONB
);

-- Instrument intervals
{env}_instrument_interval (
    id SERIAL PRIMARY KEY,
    instrument_id INTEGER,
    interval_start TIMESTAMP,
    interval_end TIMESTAMP,
    interval_data BYTEA
);
```

## Migration System

### 📝 **Migration File Structure**
```sql
-- Migration template
-- migrations/XXX_description.sql

-- Description: Brief description of changes
-- Author: Developer name
-- Date: YYYY-MM-DD

-- Forward migration
BEGIN;

-- Schema changes
CREATE TABLE IF NOT EXISTS {env}_new_table (
    id SERIAL PRIMARY KEY,
    -- table definition
);

-- Data migration (if needed)
INSERT INTO {env}_new_table (columns)
SELECT columns FROM {env}_old_table;

-- Update version
INSERT INTO {env}_db_version (version, description, applied_at)
VALUES (XXX, 'Description of changes', NOW());

COMMIT;

-- Rollback instructions (comments)
-- DROP TABLE {env}_new_table;
-- DELETE FROM {env}_db_version WHERE version = XXX;
```

### 🔄 **Migration Workflow**
```python
# Development workflow
1. Create new migration file: migrations/034_add_new_feature.sql
2. Test migration: python src/db/migration_manager.py --dry-run
3. Apply migration: python src/db/migration_manager.py migrate
4. Validate changes: python src/db/migration_manager.py validate
5. Commit migration file to repository
```

### 📋 **Migration Best Practices**
- **Sequential Numbering**: Use 3-digit sequential numbers (001, 002, 003...)
- **Atomic Operations**: Each migration should be complete and atomic
- **Rollback Planning**: Include rollback instructions in comments
- **Environment Variables**: Use `{env}` placeholder for table prefixes
- **Data Safety**: Always backup before major schema changes

## TimescaleDB Integration

### ⏰ **Time-Series Optimization**
```sql
-- Convert regular tables to hypertables for time-series optimization
SELECT create_hypertable('{env}_daily_prices', 'date', chunk_time_interval => INTERVAL '1 month');
SELECT create_hypertable('{env}_minute_bars', 'time', chunk_time_interval => INTERVAL '1 day');

-- Compression for older data
ALTER TABLE {env}_daily_prices SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol'
);

-- Automatic compression policy
SELECT add_compression_policy('{env}_daily_prices', INTERVAL '3 months');
```

### 📊 **Performance Optimizations**
```sql
-- Continuous aggregates for common queries
CREATE MATERIALIZED VIEW {env}_daily_price_summary
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', date) AS bucket,
       symbol,
       avg(close) as avg_close,
       max(high) as max_high,
       min(low) as min_low,
       sum(volume) as total_volume
FROM {env}_daily_prices
GROUP BY bucket, symbol;

-- Refresh policy
SELECT add_continuous_aggregate_policy('{env}_daily_price_summary',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

## Environment Management

### 🌍 **Multi-Environment Support**
```python
# Environment detection and configuration
from config.environment import Environment

env = Environment()
print(f"Current environment: {env.name}")  # dev, test, intg, prod
print(f"Table prefix: {env.table_prefix}")  # dev_, test_, intg_, prod_

# Environment-specific database URLs
database_urls = {
    'dev': 'postgresql://user:pass@localhost:5432/ats_dev',
    'test': 'postgresql://user:pass@localhost:5433/ats_test',
    'intg': 'postgresql://user:pass@intg-db:5432/ats_intg',
    'prod': 'postgresql://user:pass@prod-db:5432/ats_prod'
}
```

### 🔧 **Environment Setup Commands**
```bash
# Development environment
ENVIRONMENT=dev python src/db/migration_manager.py migrate

# Integration environment  
ENVIRONMENT=intg python src/db/migration_manager.py migrate

# Production environment
ENVIRONMENT=prod python src/db/migration_manager.py migrate

# Environment-specific validation
ENVIRONMENT=prod python src/db/migration_manager.py validate
```

## Database Operations

### 📊 **Common Operations**
```python
from db.migration_manager import MigrationManager

manager = MigrationManager()

# Check migration status
status = manager.get_migration_status()
print(f"Applied migrations: {len(status.applied)}")
print(f"Pending migrations: {len(status.pending)}")

# Apply specific migration
manager.apply_migration(34)

# Validate database integrity
validation = manager.validate_database()
if not validation.is_valid:
    print(f"Validation errors: {validation.errors}")

# Backup before major changes
manager.backup_database('pre_migration_backup')
```

### 🔍 **Database Validation**
```python
from db.setup_trading_db import DatabaseValidator

validator = DatabaseValidator()

# Comprehensive validation
results = validator.validate_complete_schema()

# Specific validations
constraint_validation = validator.validate_constraints()
index_validation = validator.validate_indexes()
data_integrity = validator.validate_data_integrity()

# Performance validation
performance_check = validator.validate_query_performance()
```

## Testing & Development

### 🧪 **Test Database Management**
```python
# Test database setup
from db.test_db_manager import TestDatabaseManager

test_db = TestDatabaseManager()

# Create isolated test database
test_db.create_test_environment('test_feature_x')

# Load test fixtures
test_db.load_fixtures([
    'instruments.sql',
    'daily_prices.sql',
    'events.sql'
])

# Run tests and cleanup
test_db.run_test_suite()
test_db.cleanup_test_environment()
```

### 📋 **Development Workflow**
```bash
# 1. Start development database
docker-compose up postgres

# 2. Run migrations
python src/db/migration_manager.py migrate

# 3. Load development data
python src/db/setup_trading_db.py --seed-dev-data

# 4. Run tests
PYTHONPATH=src python -m pytest tests/db/ -v

# 5. Validate schema
python src/db/migration_manager.py validate
```

## Monitoring & Maintenance

### 📊 **Database Health Monitoring**
```python
from db.monitoring import DatabaseHealthMonitor

monitor = DatabaseHealthMonitor()

# Performance metrics
metrics = monitor.get_performance_metrics()
print(f"Average query time: {metrics.avg_query_time}ms")
print(f"Active connections: {metrics.active_connections}")
print(f"Database size: {metrics.database_size_gb}GB")

# Health checks
health = monitor.perform_health_check()
if not health.is_healthy:
    print(f"Health issues: {health.issues}")
```

### 🧹 **Maintenance Operations**
```python
from db.maintenance import DatabaseMaintenance

maintenance = DatabaseMaintenance()

# Regular maintenance tasks
maintenance.update_table_statistics()
maintenance.reindex_fragmented_indexes()
maintenance.cleanup_old_logs()

# Compression and archival
maintenance.compress_old_time_series_data(months_old=12)
maintenance.archive_historical_data(years_old=5)
```

## Configuration

### ⚙️ **Database Settings**
```python
# Database configuration
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ats_dev',
    'user': 'ats_user',
    'password': 'secure_password',
    'pool_size': 20,
    'max_overflow': 30,
    'pool_timeout': 30,
    'pool_recycle': 3600
}

# TimescaleDB settings
TIMESCALE_CONFIG = {
    'chunk_time_interval': '1 month',
    'compression_policy': '3 months',
    'retention_policy': '5 years',
    'continuous_aggregates': True
}
```

### 🔧 **Environment Variables**
```bash
# Database connection
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ats_dev
DB_USER=ats_user
DB_PASSWORD=secure_password

# Migration settings
MIGRATION_PATH=src/db/migrations
MIGRATION_TABLE=db_version
AUTO_MIGRATE=false

# TimescaleDB settings
ENABLE_TIMESCALEDB=true
CHUNK_TIME_INTERVAL=1month
COMPRESSION_POLICY=3months
```

## Best Practices

### 📋 **Migration Guidelines**
1. **Test First**: Always test migrations on development environment
2. **Atomic Changes**: Keep migrations atomic and reversible
3. **Data Safety**: Backup before applying migrations to production
4. **Documentation**: Include clear descriptions and rollback instructions
5. **Sequential Numbering**: Use consistent migration numbering

### 🔒 **Security Practices**
1. **Least Privilege**: Use role-based access control
2. **Encrypted Connections**: Always use SSL in production
3. **Secret Management**: Store credentials securely
4. **Audit Logging**: Enable database audit logging
5. **Regular Updates**: Keep PostgreSQL and TimescaleDB updated

### ⚡ **Performance Guidelines**
1. **Proper Indexing**: Index frequently queried columns
2. **Query Optimization**: Use EXPLAIN to optimize queries
3. **Connection Pooling**: Use connection pooling for applications
4. **Regular Maintenance**: Run VACUUM and ANALYZE regularly
5. **Monitoring**: Monitor query performance and resource usage

---

**🗄️ This directory provides enterprise-grade database management with automated migrations, multi-environment support, and TimescaleDB optimization for time-series data.**