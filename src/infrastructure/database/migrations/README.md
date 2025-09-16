# Database Migrations

This directory contains SQL migration files for the ATS platform database schema.

## Migration Files

The migrations are numbered sequentially and represent the evolution of the database schema:

1. **001_create_base_tables.sql** - Core tables (instruments, vendors, universe, status codes)
2. **002_create_market_data_tables.sql** - Market data tables (daily prices, market cap, fundamentals, corporate actions)  
3. **003_create_trading_tables.sql** - Trading and analytics tables (intervals, indicators, portfolio, backtests)
4. **004_create_news_events_tables.sql** - News and events tables (economic events, earnings, news)
5. **005_create_ml_training_tables.sql** - ML and training data tables (datasets, models, performance tracking)
6. **006_create_monitoring_api_tables.sql** - Monitoring and API tracking tables (health checks, data quality, collection metrics)

## Schema Based on Integration Environment

These migrations are generated from the current `intg` database schema (69 tables) and represent a complete, production-ready database structure for:

- **Instruments & Reference Data** - Securities master, cross-references, aliases
- **Market Data** - Multi-vendor daily prices (Tiingo, Polygon, EODHD), market cap, fundamentals
- **Corporate Actions** - Dividends, stock splits, earnings events
- **Trading Analytics** - Time-series intervals, technical indicators, universe state
- **Portfolio Management** - Holdings, performance tracking, risk metrics
- **News & Events** - Economic events, earnings, real-time news with sentiment
- **ML/Training Data** - Training datasets, model comparisons, support/resistance levels
- **Data Quality & Monitoring** - API health, collection metrics, quality alerts

## Usage

### Using the Migration Manager

```bash
# Apply all migrations
PYTHONPATH=src python src/infrastructure/database/migration_manager.py migrate

# Check current version
PYTHONPATH=src python src/infrastructure/database/migration_manager.py version

# Rollback to specific version  
PYTHONPATH=src python src/infrastructure/database/migration_manager.py rollback --version 3
```

### Using run_dev.py

```bash
# Apply migrations in dev environment
python3 scripts/run_dev.py --environment dev run --script src/infrastructure/database/migration_manager.py migrate

# Apply migrations in intg environment
python3 scripts/run_dev.py --environment intg run --script src/infrastructure/database/migration_manager.py migrate
```

## Environment-Specific Table Prefixes

The migration system supports environment-specific table prefixes:

- **dev** environment: `dev_` prefix (e.g., `dev_instrument`, `dev_daily_price_tiingo`)
- **intg** environment: `intg_` prefix (e.g., `intg_instrument`, `intg_daily_price_tiingo`)
- **prod** environment: `prod_` prefix (e.g., `prod_instrument`, `prod_daily_price_tiingo`)
- **test** environment: `test_` prefix (e.g., `test_instrument`, `test_daily_price_tiingo`)

## Migration Best Practices

1. **Always backup** before running migrations in production
2. **Test migrations** in dev/intg environments first
3. **Use transactions** - All migrations run in transactions and rollback on error
4. **Incremental changes** - Each migration should be a small, atomic change
5. **Backward compatibility** - Avoid breaking changes when possible
6. **Document changes** - Include clear descriptions of what each migration does

## Schema Validation

After running migrations, validate the schema with:

```bash
# Validate all tables exist and have correct structure
PYTHONPATH=src python scripts/validate_schema.py --environment dev

# Check specific tables
python3 scripts/run_dev.py --environment dev query --query "\dt"
python3 scripts/run_dev.py --environment dev query --query "\d instrument"
```

## Rollback Strategy

Each migration can be rolled back by:
1. Dropping tables/columns added in that migration
2. Restoring previous table structures
3. Re-inserting any removed data from backups

The migration manager tracks applied migrations in the `db_version` table and supports rollback to any previous version.