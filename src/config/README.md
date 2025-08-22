# Configuration Management (`src/config/`)

This directory contains the centralized configuration management system that provides environment-aware settings, database connections, and API configurations for the ATS-GenAI trading platform.

## Overview

The configuration system provides:
- **Environment-Aware Configuration** with automatic dev/test/intg/prod detection
- **Centralized Database Management** with connection pooling and retry logic
- **API Configuration** for all external data vendors (Polygon, Tiingo, etc.)
- **Logging Configuration** with structured logging and environment-specific levels
- **Retry and Resilience** patterns for robust system operation

## Directory Structure

```
config/
├── environment.py          # Environment detection and table prefixing
├── database.py             # Database connection management
├── polygon.py              # Polygon API configuration
├── logging_config.py       # Centralized logging configuration
└── db_retry.py             # Database retry and resilience patterns
```

## Core Components

### 🌍 **Environment Management** (`environment.py`)

Central environment detection and configuration:

```python
from config.environment import Environment

# Automatic environment detection
env = Environment()

print(f"Environment: {env.name}")              # dev, test, intg, prod
print(f"Table prefix: {env.table_prefix}")     # dev_, test_, intg_, prod_
print(f"Is production: {env.is_production}")   # True/False
print(f"Debug mode: {env.debug_mode}")         # True/False

# Environment-aware table naming
table_name = env.get_table_name('daily_prices')
# Returns: dev_daily_prices, test_daily_prices, intg_daily_prices, or prod_daily_prices

# Configuration loading
config = env.load_config()
database_url = config.get('database_url')
api_keys = config.get('api_keys', {})
```

**Key Features:**
- **Automatic Detection**: Detects environment from ENV vars, hostnames, or defaults
- **Table Prefixing**: Provides consistent table naming across environments
- **Configuration Loading**: Environment-specific configuration file loading
- **Validation**: Validates environment settings and required variables
- **Safety Checks**: Prevents accidental production operations in wrong environment

### 🗄️ **Database Configuration** (`database.py`)

Centralized database connection management with pooling and retry logic:

```python
from config.database import DatabaseConfig, get_database_connection

# Get database connection
conn = get_database_connection()

# Connection with specific environment
conn = get_database_connection(environment='test')

# Advanced connection configuration
db_config = DatabaseConfig(
    host='localhost',
    port=5432,
    database='ats_dev',
    user='ats_user',
    password='secure_pass',
    pool_size=20,
    max_overflow=30,
    echo=False  # Set to True for SQL logging
)

# Create connection pool
engine = db_config.create_engine()
session_factory = db_config.create_session_factory()

# Async connection support
async_engine = db_config.create_async_engine()
async_session = db_config.create_async_session()
```

**Key Features:**
- **Connection Pooling**: SQLAlchemy connection pooling with configurable sizes
- **Environment Isolation**: Separate connections per environment
- **Async Support**: Full async/await support for high-performance operations
- **SSL Configuration**: Automatic SSL configuration for production environments
- **Retry Logic**: Built-in retry patterns for transient failures
- **Health Checks**: Connection health monitoring and validation

### 📡 **API Configuration** (`polygon.py`)

External API configuration and client management:

```python
from config.polygon import PolygonConfig, get_polygon_client

# Get configured Polygon client
client = get_polygon_client()

# Advanced configuration
polygon_config = PolygonConfig(
    api_key='your_api_key',
    base_url='https://api.polygon.io',
    timeout=30,
    max_retries=3,
    rate_limit_requests=5,
    rate_limit_period=60,
    enable_caching=True,
    cache_ttl=300  # 5 minutes
)

# Create client with custom config
client = polygon_config.create_client()

# Make API calls with automatic retry
data = client.get_daily_prices('AAPL', '2024-01-01', '2024-01-31')

# Bulk data fetching with rate limiting
symbols = ['AAPL', 'MSFT', 'GOOGL']
data = client.bulk_fetch_daily_prices(symbols, '2024-01-01', '2024-01-31')
```

**Key Features:**
- **Rate Limiting**: Automatic rate limiting to respect API limits
- **Retry Logic**: Exponential backoff for failed API calls
- **Caching**: Intelligent caching to reduce API calls
- **Error Handling**: Comprehensive error handling and logging
- **Bulk Operations**: Optimized bulk data fetching
- **Configuration Validation**: API key and endpoint validation

### 📊 **Logging Configuration** (`logging_config.py`)

Centralized logging with structured output and environment-specific levels:

```python
from config.logging_config import setup_logging, get_logger

# Setup application logging
setup_logging()

# Get logger for specific module
logger = get_logger(__name__)

# Structured logging
logger.info("Processing market data", extra={
    'symbol': 'AAPL',
    'date': '2024-01-15',
    'records_processed': 1000,
    'processing_time_ms': 250
})

# Error logging with context
try:
    process_market_data()
except Exception as e:
    logger.error("Market data processing failed", extra={
        'error': str(e),
        'symbol': 'AAPL',
        'retry_count': 3
    }, exc_info=True)

# Performance logging
with logger.timer('database_query'):
    results = execute_complex_query()
```

**Key Features:**
- **Structured Logging**: JSON-formatted logs with consistent structure
- **Environment-Specific Levels**: DEBUG in dev, INFO in prod
- **Performance Monitoring**: Built-in timing and performance metrics
- **Error Tracking**: Comprehensive error logging with context
- **Log Rotation**: Automatic log rotation and archival
- **Multiple Handlers**: Console, file, and remote logging support

### 🔄 **Database Retry Logic** (`db_retry.py`)

Robust retry patterns for database operations:

```python
from config.db_retry import with_db_retry, DatabaseRetryConfig

# Use decorator for automatic retry
@with_db_retry(max_attempts=3, backoff_factor=2)
def fetch_daily_prices(symbol, start_date, end_date):
    # Database operation that might fail
    return query_database(symbol, start_date, end_date)

# Custom retry configuration
retry_config = DatabaseRetryConfig(
    max_attempts=5,
    initial_delay=1.0,
    backoff_factor=2.0,
    max_delay=60.0,
    jitter=True,
    retryable_exceptions=[psycopg2.OperationalError, sqlalchemy.exc.DisconnectionError]
)

# Use context manager for retry logic
with retry_config.retry_context():
    result = execute_database_operation()

# Async retry support
@with_async_db_retry()
async def async_database_operation():
    async with async_session() as session:
        return await session.execute(query)
```

**Key Features:**
- **Exponential Backoff**: Intelligent delay between retry attempts
- **Jitter Support**: Randomized delays to prevent thundering herd
- **Exception Filtering**: Only retry on specific, retryable exceptions
- **Circuit Breaker**: Prevents excessive retries during outages
- **Async Support**: Full async/await compatibility
- **Metrics**: Retry attempt tracking and success/failure metrics

## Environment Detection

### 🔍 **Automatic Environment Detection**
```python
# Environment detection priority:
1. ENVIRONMENT environment variable
2. STAGE environment variable  
3. Hostname patterns (dev-*, test-*, intg-*, prod-*)
4. Database name patterns (*_dev, *_test, *_intg, *_prod)
5. Default to 'dev'

# Examples:
ENVIRONMENT=prod     → prod environment
STAGE=integration   → intg environment  
hostname=dev-server → dev environment
DB_NAME=ats_prod   → prod environment
```

### 🛡️ **Environment Safety**
```python
from config.environment import Environment

env = Environment()

# Production safety checks
if env.is_production:
    # Require explicit confirmation for destructive operations
    if not confirm_production_operation():
        raise SecurityError("Production operation not confirmed")

# Environment validation
env.validate_environment_setup()

# Required variables check
env.require_variables(['DATABASE_URL', 'POLYGON_API_KEY'])
```

## Configuration Files

### 📁 **Environment-Specific Files**
```
.env.dev        # Development configuration
.env.test       # Test configuration  
.env.intg       # Integration configuration
.env.prod       # Production configuration (encrypted)

config/
├── app_dev.gin     # Gin configuration for development
├── app_test.gin    # Gin configuration for testing
├── app_intg.gin    # Gin configuration for integration
└── app_prod.gin    # Gin configuration for production
```

### ⚙️ **Configuration Structure**
```python
# .env.dev example
ENVIRONMENT=dev
DEBUG=true

# Database settings
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ats_dev
DB_USER=ats_dev
DB_PASSWORD=dev_password
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20

# API keys
POLYGON_API_KEY=dev_api_key
TIINGO_API_KEY=dev_tiingo_key
ALPHA_VANTAGE_API_KEY=dev_av_key

# Logging
LOG_LEVEL=DEBUG
LOG_FORMAT=structured
LOG_FILE=logs/ats_dev.log

# Feature flags
ENABLE_REAL_TIME=true
ENABLE_CACHING=true
ENABLE_METRICS=true
```

## Database Connection Examples

### 🔗 **Basic Connection Usage**
```python
from config.database import get_database_connection
import pandas as pd

# Simple query
with get_database_connection() as conn:
    df = pd.read_sql(
        "SELECT * FROM dev_daily_prices WHERE symbol = %s",
        conn,
        params=['AAPL']
    )

# Bulk insert
with get_database_connection() as conn:
    df.to_sql('dev_daily_prices', conn, if_exists='append', index=False)
```

### 🚀 **Advanced Connection Patterns**
```python
from config.database import DatabaseConfig
from sqlalchemy.orm import sessionmaker

# Custom connection pool
db_config = DatabaseConfig()
engine = db_config.create_engine(pool_size=50, max_overflow=100)

# Session management
Session = sessionmaker(bind=engine)

def get_session():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

# Usage
with get_session() as session:
    instruments = session.query(Instrument).filter_by(active=True).all()
```

### ⚡ **Async Database Operations**
```python
from config.database import get_async_database_connection
import asyncio

async def async_data_processing():
    async with get_async_database_connection() as conn:
        # Async query execution
        result = await conn.execute(
            "SELECT * FROM dev_daily_prices WHERE date >= %s",
            ['2024-01-01']
        )
        
        # Process results asynchronously
        async for row in result:
            await process_price_data(row)

# Run async operation
asyncio.run(async_data_processing())
```

## Configuration Validation

### ✅ **Startup Validation**
```python
from config.environment import Environment
from config.database import DatabaseConfig

def validate_startup_configuration():
    """Validate all configuration at application startup"""
    
    # Environment validation
    env = Environment()
    env.validate_environment_setup()
    
    # Database validation
    db_config = DatabaseConfig()
    db_config.validate_connection()
    
    # API configuration validation
    api_configs = [
        ('POLYGON_API_KEY', validate_polygon_api),
        ('TIINGO_API_KEY', validate_tiingo_api),
        ('ALPHA_VANTAGE_API_KEY', validate_alpha_vantage_api)
    ]
    
    for key, validator in api_configs:
        if env.get_config_value(key):
            validator(env.get_config_value(key))
    
    logger.info("Configuration validation completed successfully")

# Run validation at startup
validate_startup_configuration()
```

### 🔍 **Health Checks**
```python
from config.database import check_database_health
from config.polygon import check_polygon_api_health

async def system_health_check():
    """Comprehensive system health check"""
    
    health_status = {
        'database': await check_database_health(),
        'polygon_api': await check_polygon_api_health(),
        'environment': Environment().is_valid(),
        'logging': logging.getLogger().isEnabledFor(logging.INFO)
    }
    
    overall_health = all(health_status.values())
    
    return {
        'healthy': overall_health,
        'components': health_status,
        'timestamp': datetime.utcnow().isoformat()
    }
```

## Performance & Monitoring

### 📊 **Configuration Metrics**
```python
from config.logging_config import get_logger
import time

logger = get_logger(__name__)

# Connection pool monitoring
def monitor_connection_pool():
    engine = get_database_connection().engine
    pool = engine.pool
    
    metrics = {
        'pool_size': pool.size(),
        'checked_in': pool.checkedin(),
        'checked_out': pool.checkedout(),
        'overflow': pool.overflow(),
        'invalid': pool.invalid()
    }
    
    logger.info("Connection pool metrics", extra=metrics)
    return metrics

# API rate limiting monitoring
def monitor_api_usage():
    from config.polygon import get_api_metrics
    
    metrics = get_api_metrics()
    logger.info("API usage metrics", extra=metrics)
    
    if metrics['requests_per_minute'] > 80:  # 80% of limit
        logger.warning("API rate limit approaching", extra=metrics)
```

### 🎯 **Performance Optimization**
```python
# Connection pool optimization
DATABASE_CONFIG = {
    'pool_size': 20,           # Base connection pool size
    'max_overflow': 30,        # Additional connections under load
    'pool_timeout': 30,        # Timeout waiting for connection
    'pool_recycle': 3600,      # Recycle connections every hour
    'pool_pre_ping': True,     # Validate connections before use
}

# API client optimization
API_CONFIG = {
    'timeout': 30,             # Request timeout
    'max_retries': 3,          # Retry failed requests
    'backoff_factor': 0.5,     # Exponential backoff
    'enable_caching': True,    # Cache responses
    'cache_ttl': 300,          # Cache for 5 minutes
}
```

## Security & Best Practices

### 🔒 **Security Guidelines**
1. **Environment Variables**: Never commit sensitive config to version control
2. **Encryption**: Encrypt production configuration files
3. **Access Control**: Use IAM roles and least privilege principles
4. **Secret Rotation**: Regularly rotate API keys and database passwords
5. **Audit Logging**: Log all configuration access and changes

### 📋 **Configuration Best Practices**
1. **Validation**: Validate all configuration at startup
2. **Defaults**: Provide sensible defaults for optional settings
3. **Documentation**: Document all configuration options
4. **Versioning**: Version configuration changes
5. **Testing**: Test configuration in isolated environments

### 🚨 **Common Pitfalls**
- **Environment Mixing**: Accidentally using prod config in dev
- **Missing Variables**: Not validating required environment variables
- **Connection Leaks**: Not properly closing database connections
- **API Limits**: Exceeding API rate limits without proper handling
- **Log Sensitivity**: Logging sensitive information in structured logs

---

**⚙️ This directory provides enterprise-grade configuration management with environment isolation, robust connection handling, and comprehensive validation for secure, scalable operations.**