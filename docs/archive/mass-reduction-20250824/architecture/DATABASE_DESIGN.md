# Database Configuration & API Keys Summary

## Database Configuration

### Connection Details
- **Host**: `localhost` (local development)
- **Port**: `5432` (standard PostgreSQL port)
- **Database Name**: `dev_db` (development environment)
- **Username**: `postgres`
- **Password**: `dev_password`

### Environment-Specific Databases
```bash
# Development
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=dev_password
DB_NAME=dev_db

# Integration Testing
DB_NAME=intg_db

# Production
DB_NAME=trading_db
```

### Connection Examples
```bash
# PostgreSQL command line
PGPASSWORD=dev_password psql -h localhost -p 5432 -U postgres -d dev_db

# Python connection string
postgresql://postgres:dev_password@localhost:5432/dev_db

# With application
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db python script.py
```

## Vendor API Keys

### EODHD (Primary Historical Data)
- **Environment Variable**: `EODHD_API_KEY`
- **Current Key**: `68aa0c7d2fe831.67386369`
- **Usage**: Primary source for 30-year historical daily prices
- **Rate Limits**: 100,000 API calls per month
- **Coverage**: US stocks, comprehensive historical data back to 1995

### Polygon.io (Validation & Real-time)
- **Environment Variable**: `POLYGON_API_KEY`
- **Current Key**: `${POLYGON_API_KEY}` (set from environment variable)
- **Usage**: Real-time data validation, instrument metadata
- **Rate Limits**: 5 calls per minute (free tier)
- **Coverage**: US stocks, excellent for validation

### Tiingo (Alternative Historical)
- **Environment Variable**: `TIINGO_API_KEY`
- **Current Key**: `5f40b4f36e171405746304ec0e5a6f3aa9ca77e5`
- **Usage**: Alternative historical data source, dividends
- **Rate Limits**: 50 calls per hour (free tier)
- **Coverage**: US stocks, good historical coverage

### Financial Modeling Prep (FMP)
- **Environment Variable**: `FMP_API_KEY`
- **Current Key**: `Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr`
- **Usage**: Financial statements, alternative price data
- **Rate Limits**: 250 calls per day (free tier)
- **Coverage**: US stocks, financial fundamentals

### Other APIs (Available)
- **Finnhub**: `FINNHUB_API_KEY` = `d1tmh5hr01qth6pm1ehgd1tmh5hr01qth6pm1ei0`
- **IEX Cloud**: `IEX_API_KEY` = `db-7XAkHUfJQkMH48GRLMnwrQDNu3RKb`
- **Quandl**: `QUANDL_API_KEY` = `1cPuQtoTJquouUgG-1cZ`
- **OpenAI**: `OPENAI_API_KEY` = `sk-svcacct-C6MvJEL...` (for ML features)
- **Weights & Biases**: `WEIGHTS_AND_BIASES_API_KEY` = `f907f51629a8b9b...` (for ML tracking)

## Database Schema

### Core Tables
- **Instruments**: `dev_instruments` (dev environment)
- **Daily Prices**: `dev_daily_prices_polygon`, `dev_daily_prices_tiingo`
- **Minute Bars**: `dev_minute_bars` (TimescaleDB hypertable)
- **Universe**: `dev_universe`, `dev_universe_membership`

### Environment Table Prefixing
Tables are automatically prefixed based on environment:
- Development: `dev_*`
- Integration: `intg_*`  
- Production: `prod_*`

## Configuration Files

### Environment Files
```
.env.dev          # Development environment variables
.env.test         # Test environment variables  
.env.prod         # Production environment variables
```

### Gin Configuration
```
config/app.gin       # General application config
config/app_dev.gin   # Development-specific config
config/app_prod.gin  # Production-specific config
```

## Database Operations

### Migration Commands
```bash
# Run database migrations
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db uv run python src/db/migration_manager.py migrate

# Check migration version
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db uv run python src/db/migration_manager.py version
```

### Data Population Commands
```bash
# Populate instruments from Polygon
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db uv run python src/secmaster/populate_instrument_polygon.py --environment dev --gin_config config/app_dev.gin

# Sync instruments
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db uv run python src/secmaster/sync_instruments.py --environment dev --gin_config config/app_dev.gin --limit 100

# Populate market cap data
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db uv run python src/secmaster/populate_market_cap_polygon.py --environment dev --gin_config config/app_dev.gin --limit 100
```

## Universe Management

### Create Data Complete Universe
```bash
# Create universe with 5-year complete data
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db python scripts/universe/setup_data_complete_universe.py create --name "high_quality_5y"

# List all universes
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db python scripts/universe/setup_data_complete_universe.py list

# Show universe members
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db python scripts/universe/setup_data_complete_universe.py show 1

# Validate universe quality
PYTHONPATH=src ENVIRONMENT=dev DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=dev_password DB_NAME=dev_db python scripts/universe/setup_data_complete_universe.py validate 1
```

## Testing

### Core Infrastructure Test
```bash
PYTHONPATH=src python test_universe_creation.py
```

### Database Connection Test
```bash
PGPASSWORD=dev_password psql -h localhost -p 5432 -U postgres -d dev_db -c "SELECT version();"
```

## Security Notes

⚠️ **Important Security Considerations**:

1. **Never commit API keys** to version control
2. **Use environment variables** for all sensitive configuration
3. **Rotate API keys** regularly
4. **Use different credentials** for each environment
5. **Limit database user permissions** in production
6. **Enable SSL** for production database connections

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   ```bash
   # Check if PostgreSQL is running
   ps aux | grep postgres
   
   # Check listening port
   ss -tlnp | grep 5432
   
   # Test connection
   PGPASSWORD=dev_password psql -h localhost -p 5432 -U postgres -d dev_db -c "SELECT 1;"
   ```

2. **API Rate Limits**
   - Check vendor documentation for rate limits
   - Implement exponential backoff
   - Consider premium API tiers for higher limits

3. **Environment Configuration**
   ```bash
   # Verify environment variables
   env | grep -E "(DB_|POLYGON_|TIINGO_)"
   
   # Check Gin configuration loading
   PYTHONPATH=src python -c "from config.environment import Environment; env = Environment(); print(env.get_database_url())"
   ```

## Environment Variable Templates

### .env.dev Template
```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=dev_password
DB_NAME=dev_db

# API Keys
POLYGON_API_KEY=your_polygon_api_key_here
TIINGO_API_KEY=your_tiingo_api_key_here

# Application Settings
ENVIRONMENT=dev
LOG_LEVEL=INFO
```

### .env.prod Template
```bash
# Database Configuration (use secure values in production)
DB_HOST=your_prod_db_host
DB_PORT=5432
DB_USER=your_prod_db_user
DB_PASSWORD=your_secure_prod_password
DB_NAME=trading_db

# API Keys (use production keys)
POLYGON_API_KEY=your_prod_polygon_api_key
TIINGO_API_KEY=your_prod_tiingo_api_key

# Application Settings
ENVIRONMENT=prod
LOG_LEVEL=WARNING
```

## Quick Start Commands

### Setup Development Environment
```bash
# 1. Install dependencies
uv sync

# 2. Set environment variables
export ENVIRONMENT=dev
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=postgres
export DB_PASSWORD=dev_password
export DB_NAME=dev_db

# 3. Run migrations
PYTHONPATH=src uv run python src/db/migration_manager.py migrate

# 4. Create data complete universe
PYTHONPATH=src python scripts/universe/setup_data_complete_universe.py create
```

---

*Last Updated: 2025-08-17*
*This configuration is for the ATS GenAI trading system development environment.*