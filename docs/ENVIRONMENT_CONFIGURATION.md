# Environment-Specific Configuration Guide

This guide explains how to use the ATS platform's environment-specific gin configuration system to manage settings across development, integration, and production environments.

## Overview

The ATS platform uses Google's **gin dependency injection framework** with environment-specific configuration files to eliminate hardcoded values and enable flexible deployment across different environments.

### Key Features

- 🌍 **Automatic Environment Detection** - Detects dev/intg/prod based on environment variables and system indicators
- 🔧 **Environment-Specific Overrides** - Each environment can override base configuration values  
- 📋 **Centralized Configuration** - All hardcoded values moved to gin configuration files
- ✅ **Configuration Validation** - Built-in validation and health checks
- 🔄 **Dynamic Loading** - Configuration can be reloaded without application restart
- 🏗️ **Inheritance System** - Environment configs inherit from base configuration

## Quick Start

### 1. Load Configuration in Your Application

```python
from config.environment_config import load_gin_config

# Automatically detect and load environment configuration
env = load_gin_config()
print(f"Loaded configuration for {env.value} environment")
```

### 2. Use Gin-Configured Parameters

```python
import gin
from dataclasses import dataclass

@gin.configurable
@dataclass
class DatabaseConfig:
    host: str = 'localhost'
    port: int = 5432
    user: str = 'postgres'
    password: str = 'password'
    database: str = 'dev_db'

# Gin automatically injects environment-specific values
db_config = DatabaseConfig()
print(f"Database: {db_config.host}:{db_config.port}")
```

### 3. Set Environment Variable (Optional)

```bash
# Override automatic detection
export ATS_ENVIRONMENT=dev    # or intg, prod, test
```

## Configuration Architecture

### File Structure

```
config/
├── hardcoded_values.gin     # Base configuration (268+ parameters)
├── app_dev.gin             # Development overrides  
├── app_intg.gin            # Integration/staging overrides
├── app_prod.gin            # Production overrides
└── app_test.gin            # Testing overrides
```

### Inheritance Model

Each environment configuration inherits from the base configuration:

```gin
# config/app_dev.gin
include 'config/hardcoded_values.gin'

# Development-specific overrides
database.connection.port = 3432
timeouts.api.default = 60
batch.sizes.default = 10
```

## Environment Detection

The system automatically detects the environment using multiple indicators:

### 1. Environment Variable (Highest Priority)
```bash
export ATS_ENVIRONMENT=dev|intg|prod|test
```

### 2. Database Connection Indicators
- **Port 3432** → Development
- **Port 4432** → Integration  
- **Hostname with 'prod'** → Production

### 3. Container/Hostname Indicators
- **dev**, **development** in hostname → Development
- **intg**, **integration**, **staging** in hostname → Integration
- **prod**, **production** in hostname → Production

### 4. Testing Framework Indicators
- **PYTEST_CURRENT_TEST**, **TEST_ENV**, **TESTING** variables → Test

### 5. Default Fallback
- Defaults to **Development** if no indicators found

## Environment-Specific Configurations

### Development Environment (`app_dev.gin`)

**Purpose**: Local development, debugging, fast iteration

**Key Characteristics**:
```gin
# Smaller batches for faster feedback
batch.sizes.default = 10
batch.processing.max_symbols = 100

# Longer timeouts for debugging
timeouts.api.default = 60
timeouts.database.query = 120

# Limited symbol universe for testing
symbols.default_universe = ['AAPL', 'TSLA', 'MSFT']

# Local database connection
database.connection.host = 'localhost'
database.connection.port = 3432
database.connection.password = 'dev_password'

# More lenient thresholds
thresholds.success_rate = 0.80
thresholds.api_success = 0.70
```

### Integration Environment (`app_intg.gin`)

**Purpose**: CI/CD testing, pre-production validation

**Key Characteristics**:
```gin
# Moderate batch sizes for comprehensive testing
batch.sizes.default = 50
batch.processing.max_symbols = 500

# Production-like timeouts
timeouts.api.default = 45
timeouts.database.query = 90

# Broader symbol coverage
symbols.default_universe = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN', 'META']

# Integration database
database.connection.port = 4432
database.connection.password = 'intg_password'

# Production-level requirements
thresholds.success_rate = 0.85
thresholds.api_success = 0.75
```

### Production Environment (`app_prod.gin`)

**Purpose**: Live trading, maximum performance, strict requirements

**Key Characteristics**:
```gin
# Large batches for throughput
batch.sizes.default = 1000
batch.processing.max_symbols = 10000

# Strict timeouts for performance
timeouts.api.default = 30
timeouts.database.query = 60

# Full market symbol universe
symbols.default_universe = [full S&P 500 list]

# Production database cluster
database.connection.host = 'prod-db-cluster.ats.internal'
database.connection.password = 'REPLACE_WITH_VAULT_PASSWORD'

# Strict requirements
thresholds.success_rate = 0.95
thresholds.api_success = 0.90
```

## Configuration Categories

The gin configuration system organizes 268+ parameters into logical categories:

### API and Service Configuration
- FastAPI application settings (title, version, CORS)
- Service ports and hosts
- Webhook configurations
- Rate limiting and timeouts

### Database Configuration
- Connection parameters (host, port, credentials)
- Connection pool settings
- Query timeouts and limits
- Table names and schemas

### Data Processing Configuration
- Real-time data collection settings
- Batch processing sizes and limits
- External API configurations
- Retry logic and error handling

### Financial Parameters
- Performance thresholds (Sharpe ratio, drawdown)
- Risk tolerance settings
- Success rate requirements
- Coverage thresholds

### Machine Learning Configuration
- Neural network architectures
- Training hyperparameters
- Model dimensions and complexity
- Agent network settings

### Monitoring and Alerting
- Health check intervals
- Metrics collection frequency
- Alert thresholds and retention
- Logging levels and formats

## Usage Patterns

### Application Startup

```python
from config.environment_config import load_gin_config

def main():
    # Load environment configuration at startup
    env = load_gin_config()
    print(f"🚀 Starting ATS platform in {env.value} environment")
    
    # Your gin-configured classes will now use environment-specific values
    start_services()
```

### Service Configuration

```python
import gin
from dataclasses import dataclass

@gin.configurable
@dataclass
class RealtimeCollectorConfig:
    symbols: List[str] = None
    collection_interval: int = 60
    db_host: str = "localhost"
    pool_max_size: int = 10
    timeout_seconds: int = 30

class RealtimeDataCollector:
    def __init__(self):
        # Gin injects environment-specific configuration
        self.config = RealtimeCollectorConfig()
        print(f"Collecting data for {len(self.config.symbols)} symbols")
        print(f"Database: {self.config.db_host}")
```

### Environment-Specific Logic

```python
from config.environment_config import get_current_env, Environment

def configure_logging():
    env = get_current_env()
    
    if env == Environment.DEVELOPMENT:
        logging.basicConfig(level=logging.DEBUG)
    elif env == Environment.PRODUCTION:
        logging.basicConfig(level=logging.WARNING)
    else:
        logging.basicConfig(level=logging.INFO)
```

## Configuration Validation

### Built-in Validation

```python
from config.validation import validate_current_config

# Validate configuration at startup
result = validate_current_config()

if result.is_valid:
    print("✅ Configuration validation passed")
else:
    print("❌ Configuration validation failed:")
    for error in result.errors:
        print(f"   - {error}")
```

### Environment-Specific Validation

```python
from config.validation import validate_environment
from config.environment_config import Environment

# Validate specific environment
result = validate_environment(Environment.PRODUCTION)

if result.is_valid:
    print("✅ Production configuration is valid")
else:
    print("❌ Production configuration has issues")
```

### CI/CD Integration

```bash
#!/bin/bash
# Pre-deployment configuration validation

echo "Validating configuration..."
python -c "
from config.validation import validate_current_config
result = validate_current_config()
if not result.is_valid:
    print('❌ Configuration validation failed')
    for error in result.errors:
        print(f'   - {error}')
    exit(1)
print('✅ Configuration validation passed')
"
```

## Environment Variables Reference

### Core Environment Variables

| Variable | Values | Description |
|----------|--------|-------------|
| `ATS_ENVIRONMENT` | `dev`, `intg`, `prod`, `test` | Explicit environment override |
| `DB_HOST` | hostname | Database host (used for detection) |
| `DB_PORT` | port number | Database port (3432=dev, 4432=intg) |
| `HOSTNAME` | hostname | Container/server hostname |

### Database Environment Variables

| Variable | Development | Integration | Production |
|----------|-------------|-------------|------------|
| `DB_HOST` | `localhost` | `localhost` | `prod-db-cluster.ats.internal` |
| `DB_PORT` | `3432` | `4432` | `5432` |
| `DB_USER` | `postgres` | `postgres` | `ats_prod_user` |
| `DB_PASSWORD` | `dev_password` | `intg_password` | `VAULT_PASSWORD` |
| `DB_NAME` | `dev_db` | `intg_db` | `ats_production` |

## Best Practices

### 1. Environment-Specific Tuning

**Development**:
- Use small batch sizes (10-50) for fast feedback
- Set longer timeouts (60-120s) for debugging
- Limit symbol universe for faster testing
- Enable debug logging

**Integration**:
- Use moderate batch sizes (50-200) for testing
- Set balanced timeouts (45-90s) for automated tests
- Include broader symbol coverage for validation
- Use production-like settings where possible

**Production**:
- Use large batch sizes (1000+) for throughput
- Set strict timeouts (30-60s) for performance
- Include full market symbol universe
- Use warning-level logging only

### 2. Configuration Management

```python
# ✅ Good: Use gin-configurable classes
@gin.configurable
@dataclass
class ServiceConfig:
    timeout: int = 30
    retries: int = 3

# ❌ Bad: Hardcoded values
class Service:
    def __init__(self):
        self.timeout = 30  # Hardcoded!
        self.retries = 3   # Hardcoded!
```

### 3. Validation Integration

```python
# Always validate configuration at startup
def main():
    # Load configuration
    env = load_gin_config()
    
    # Validate configuration
    result = validate_current_config()
    if not result.is_valid:
        logger.error("Configuration validation failed")
        for error in result.errors:
            logger.error(f"  - {error}")
        sys.exit(1)
    
    # Start services
    start_application()
```

### 4. Security Considerations

```gin
# ✅ Good: Use placeholder for secrets
database.connection.password = 'REPLACE_WITH_VAULT_PASSWORD'

# ❌ Bad: Hardcoded secrets
database.connection.password = 'actual_password_here'
```

## Troubleshooting

### Configuration Not Loading

**Problem**: Configuration parameters not taking effect

**Solution**:
```python
# Check if configuration is loaded
from config.environment_config import get_env_info

info = get_env_info()
print("Current environment:", info['current_environment'])
print("Config file:", info['config_file'])
print("Loaded configs:", info['loaded_configs'])
```

### Environment Detection Issues

**Problem**: Wrong environment detected

**Solution**:
```bash
# Set explicit environment variable
export ATS_ENVIRONMENT=dev

# Or check detection indicators
python -c "
from config.environment_config import get_config_loader
loader = get_config_loader()
env = loader.detect_environment()
info = loader.get_environment_info()
print('Detected environment:', env.value)
print('Detection indicators:', info['detection_indicators'])
"
```

### Missing Configuration Files

**Problem**: FileNotFoundError when loading configuration

**Solution**:
```bash
# Check available configuration files
ls -la config/*.gin

# Create missing environment configuration
cp config/app_dev.gin config/app_missing.gin
# Edit config/app_missing.gin with appropriate values
```

### Parameter Access Issues

**Problem**: Gin parameters not injected correctly

**Solution**:
```python
# Ensure class is decorated and configuration is loaded
import gin

@gin.configurable  # Don't forget this!
@dataclass
class MyConfig:
    param: int = 10

# Load configuration before using
load_gin_config()
config = MyConfig()  # Now gets gin-injected values
```

## Migration Guide

### From Hardcoded Values

**Before**:
```python
class DataCollector:
    def __init__(self):
        self.batch_size = 100  # Hardcoded
        self.timeout = 30      # Hardcoded
        self.symbols = ['AAPL', 'MSFT']  # Hardcoded
```

**After**:
```python
@gin.configurable
@dataclass
class CollectorConfig:
    batch_size: int = 100
    timeout: int = 30
    symbols: List[str] = None
    
    def __post_init__(self):
        if self.symbols is None:
            self.symbols = ['AAPL', 'MSFT']

class DataCollector:
    def __init__(self):
        self.config = CollectorConfig()
```

**Configuration file**:
```gin
# config/hardcoded_values.gin
data.CollectorConfig.batch_size = 100
data.CollectorConfig.timeout = 30
data.CollectorConfig.symbols = ['AAPL', 'MSFT', 'GOOGL']

# config/app_dev.gin
include 'config/hardcoded_values.gin'
data.CollectorConfig.batch_size = 10    # Override for dev
data.CollectorConfig.symbols = ['AAPL'] # Smaller set for dev
```

## Examples

See `examples/environment_config_example.py` for comprehensive usage examples including:

- Automatic environment detection
- Explicit environment loading  
- Configuration validation
- Environment-specific behavior
- Parameter access patterns

## Summary

The environment-specific gin configuration system provides:

✅ **Eliminated 268+ hardcoded values** across the platform
✅ **Automatic environment detection** based on system indicators  
✅ **Environment-specific optimization** (dev/intg/prod)
✅ **Centralized configuration management** with inheritance
✅ **Built-in validation and health checks**
✅ **Dynamic configuration loading** without restarts
✅ **Security-conscious credential management**

This system enables flexible, maintainable, and secure deployments across all ATS platform environments.