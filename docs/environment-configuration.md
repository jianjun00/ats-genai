# Environment Configuration System

The Market Forecast App now supports environment-specific configuration management with support for test, integration, and production environments. Each environment has its own database prefixes and configuration settings.

## Overview

The environment configuration system provides:
- **Environment-specific databases**: `test_trading_db`, `intg_trading_db`, `prod_trading_db`
- **Table prefixing**: All tables are prefixed with environment name (e.g., `test_daily_prices`, `intg_daily_prices`, `prod_daily_prices`)
- **Configuration files**: Environment-specific config files in the `config/` directory
- **Environment class**: Centralized configuration management replacing direct `os.environ` usage

## Configuration Files

### Directory Structure
```
config/
├── shared.conf      # Common settings across all environments
├── test.conf        # Test environment specific settings
├── intg.conf        # Integration environment specific settings
└── prod.conf        # Production environment specific settings
```

### Configuration Sections

Each configuration file supports the following sections:

#### `[database]`
- `host`: Database host
- `port`: Database port
- `user`: Database username
- `password`: Database password
- `database`: Database name (environment-specific)
- `prefix`: Table prefix for the environment

#### `[api_keys]`
- API keys for various services (Polygon, Tiingo, etc.)
- Supports environment variable substitution with `${VAR_NAME}` syntax

#### `[logging]`
- `level`: Log level (DEBUG, INFO, WARNING, ERROR)
- `format`: Log format string

#### `[features]`
- Feature flags for environment-specific behavior
- `enable_caching`: Enable/disable caching
- `enable_metrics`: Enable/disable metrics collection
- `strict_validation`: Enable/disable strict validation

## Environment Class Usage

### Basic Usage

```python
from config.environment import get_environment, EnvironmentType, set_environment

# Get current environment (auto-detected from ENVIRONMENT env var)
env = get_environment()

# Set specific environment
set_environment(EnvironmentType.TEST)
env = get_environment()

# Get database URL
db_url = env.get_database_url()
# Returns: postgresql://postgres:password@localhost:5432/test_trading_db

# Get environment-specific table name
table_name = env.get_table_name("daily_prices")
# Returns: test_daily_prices

# Get API key
polygon_key = env.get_api_key("polygon")

# Check feature flags
if env.is_feature_enabled("strict_validation"):
    # Perform strict validation
    pass
```

### Database Configuration

```python
# Get database configuration dictionary
db_config = env.get_database_config()
# Returns: {
#     'host': 'localhost',
#     'port': 5432,
#     'user': 'postgres',
#     'password': 'password',
#     'database': 'test_trading_db',
#     'min_size': 1,
#     'max_size': 10,
#     'command_timeout': 60
# }

# Use with asyncpg
import asyncpg
pool = await asyncpg.create_pool(**db_config)
```

### Updated Class Usage

Classes that previously used `os.environ` or `os.getenv` have been updated to use the Environment class:

```python
# TradingUniverse - now uses environment configuration
from trading.trading_universe import TradingUniverse

# Uses environment database URL and table prefixes automatically
universe = TradingUniverse()

# Or override with custom URL
universe = TradingUniverse(db_url="postgresql://custom:url")

# SecurityMaster - same pattern
from trading.trading_universe import SecurityMaster
master = SecurityMaster()  # Uses environment configuration
```

## Environment Setup

### Setting Environment

Set the `ENVIRONMENT` environment variable to control which configuration is used:

```bash
# Test environment (default)
export ENVIRONMENT=test

# Integration environment
export ENVIRONMENT=intg

# Production environment
export ENVIRONMENT=prod
```

### Database Migration

Use the environment migration utility to set up databases and tables:

```bash
# Setup test environment (creates database, tables, indexes)
PYTHONPATH=src python src/db/environment_migration.py --env test --action setup

# Setup integration environment
PYTHONPATH=src python src/db/environment_migration.py --env intg --action setup

# Setup production environment
PYTHONPATH=src python src/db/environment_migration.py --env prod --action setup
```

### Available Migration Actions

- `setup`: Complete environment setup (database + tables + indexes)
- `create-tables`: Create tables only
- `create-indexes`: Create indexes only
- `drop-tables`: Drop all tables (not allowed for production)

## Environment-Specific Tables

All database tables are automatically prefixed based on the environment:

| Base Table Name | Test Environment | Integration Environment | Production Environment |
|----------------|------------------|------------------------|------------------------|
| daily_prices | test_daily_prices | intg_daily_prices | prod_daily_prices |
| daily_adjusted_prices | test_daily_adjusted_prices | intg_daily_adjusted_prices | prod_daily_adjusted_prices |
| splits | test_splits | intg_splits | prod_splits |
| dividends | test_dividends | intg_dividends | prod_dividends |
| universe | test_universe | intg_universe | prod_universe |
| universe_membership | test_universe_membership | intg_universe_membership | prod_universe_membership |
| events | test_events | intg_events | prod_events |

## Configuration Examples

### Test Environment (`config/test.conf`)
```ini
[database]
host=localhost
port=5432
user=postgres
password=password
database=test_trading_db
prefix=test_

[api_keys]
polygon_api_key=test_polygon_key
tiingo_api_key=test_tiingo_key

[logging]
level=DEBUG

[features]
enable_caching=false
strict_validation=true
```

### Production Environment (`config/prod.conf`)
```ini
[database]
host=${DB_HOST}
port=${DB_PORT}
user=${DB_USER}
password=${DB_PASSWORD}
database=prod_trading_db
prefix=prod_

[api_keys]
polygon_api_key=${POLYGON_API_KEY}
tiingo_api_key=${TIINGO_API_KEY}

[logging]
level=INFO

[features]
enable_caching=true
strict_validation=false
```

## Testing

The environment system includes comprehensive tests:

```bash
# Test environment configuration
PYTHONPATH=src python -m pytest tests/config/ -v

# Test environment-aware classes
PYTHONPATH=src python -m pytest tests/trading/test_trading_universe_environment.py -v
```

## Migration from os.environ

### Before (Old Pattern)
```python
import os
TSDB_URL = os.getenv("TSDB_URL", "postgresql://localhost:5432/trading_db")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

class MyClass:
    def __init__(self, db_url: str):
        self.db_url = db_url
    
    async def query_data(self):
        conn = await asyncpg.connect(self.db_url)
        result = await conn.fetch("SELECT * FROM daily_prices")
```

### After (New Pattern)
```python
from config.environment import get_environment

class MyClass:
    def __init__(self, db_url: Optional[str] = None):
        self.env = get_environment()
        self.db_url = db_url or self.env.get_database_url()
    
    async def query_data(self):
        conn = await asyncpg.connect(self.db_url)
        table_name = self.env.get_table_name("daily_prices")
        result = await conn.fetch(f"SELECT * FROM {table_name}")
```

## Benefits

1. **Environment Isolation**: Complete separation between test, integration, and production data
2. **Configuration Management**: Centralized configuration with environment-specific overrides
3. **Table Prefixing**: Automatic table name prefixing prevents cross-environment data contamination
4. **API Key Management**: Secure API key management with environment variable substitution
5. **Feature Flags**: Environment-specific feature toggles
6. **Database Migration**: Automated database setup and migration utilities
7. **Testing**: Comprehensive test coverage for all configuration scenarios

## Best Practices

1. **Always use the Environment class** instead of direct `os.environ` access
2. **Set ENVIRONMENT variable** in your deployment scripts
3. **Use environment-specific API keys** for external services
4. **Test configuration changes** in test environment first
5. **Keep production secrets secure** using environment variables
6. **Use feature flags** to control environment-specific behavior
7. **Run migration scripts** when setting up new environments
