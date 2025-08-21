# DAO Migration Guide

This guide documents the migration from the old scattered DAO structure to the new unified DAO architecture implemented in Phase 2 of the refactoring plan.

## Overview

The refactoring has consolidated vendor-specific DAOs into unified interfaces while eliminating 70%+ code duplication. The new structure provides:

1. **Core Infrastructure** - Centralized configuration, logging, exceptions, and validation
2. **Unified DAO Layer** - Base classes eliminating CRUD duplication
3. **Vendor Consolidation** - Single DAOs per vendor replacing multiple scattered files
4. **Corporate Actions** - Unified dividend and stock split operations
5. **Instrument Management** - Consolidated instrument operations

## New DAO Structure

```
src/dao/
├── base/
│   ├── base_dao.py              # Base DAO with common CRUD operations
│   └── vendor_dao.py            # Base vendor DAO with vendor-specific patterns
├── market_data/
│   └── daily_prices_dao.py      # Unified daily prices across vendors
├── vendors/
│   ├── polygon_dao.py           # Consolidated Polygon operations
│   └── tiingo_dao.py            # Consolidated Tiingo operations
├── corporate_actions/
│   ├── dividends_dao.py         # Unified dividend operations
│   └── stock_splits_dao.py      # Unified stock split operations
└── instruments/
    └── instruments_dao.py       # Unified instrument operations
```

## Migration Mappings

### Old → New Import Mappings

```python
# OLD IMPORTS
from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
from dao.dividend_polygon_dao import DividendPolygonDAO
from dao.dividend_tiingo_dao import DividendTiingoDAO
from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
from dao.stock_splits_tiingo_dao import StockSplitsTiingoDAO
from dao.instrument_polygon_dao import InstrumentPolygonDAO

# NEW IMPORTS
from dao.vendors.polygon_dao import PolygonDAO
from dao.vendors.tiingo_dao import TiingoDAO
from dao.market_data.daily_prices_dao import DailyPricesDAO
from dao.corporate_actions.dividends_dao import DividendsDAO
from dao.corporate_actions.stock_splits_dao import StockSplitsDAO
from dao.instruments.instruments_dao import InstrumentsDAO
```

### Usage Pattern Changes

#### Daily Prices Migration

**Old Pattern:**
```python
# Polygon prices
from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
polygon_dao = DailyPricesPolygonDAO(env)
await polygon_dao.insert_price(date, instrument_id, open_, high, low, close, volume)

# Tiingo prices  
from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
tiingo_dao = DailyPricesTiingoDAO(env)
await tiingo_dao.insert_price(date, instrument_id, open_, high, low, close, adj_close, volume)
```

**New Pattern:**
```python
# Unified approach using vendor-specific DAOs
from dao.vendors.polygon_dao import PolygonDAO
from dao.vendors.tiingo_dao import TiingoDAO

polygon_dao = PolygonDAO()
polygon_dao.insert_daily_price(symbol, date, open_, high, low, close, volume)

tiingo_dao = TiingoDAO()
tiingo_dao.insert_daily_price(symbol, date, open_, high, low, close, volume, adjusted_close=adj_close)

# OR unified approach using unified DAO
from dao.market_data.daily_prices_dao import DailyPricesDAO
daily_prices_dao = DailyPricesDAO()
daily_prices_dao.create(price_data)  # Works with any vendor data
```

#### Dividend Operations Migration

**Old Pattern:**
```python
from dao.dividend_polygon_dao import DividendPolygonDAO
from dao.dividend_tiingo_dao import DividendTiingoDAO

# Separate DAOs for each vendor
polygon_dividend_dao = DividendPolygonDAO(env)
tiingo_dividend_dao = DividendTiingoDAO(env)
```

**New Pattern:**
```python
# Vendor-specific approach
from dao.vendors.polygon_dao import PolygonDAO
from dao.vendors.tiingo_dao import TiingoDAO

polygon_dao = PolygonDAO()
polygon_dao.insert_dividend(symbol, ex_date, amount)

tiingo_dao = TiingoDAO()
tiingo_dao.insert_dividend(symbol, ex_date, amount)

# OR unified approach
from dao.corporate_actions.dividends_dao import DividendsDAO
dividends_dao = DividendsDAO()
dividends_dao.create(dividend_data)  # Works with any vendor data
```

#### Stock Splits Migration

**Old Pattern:**
```python
from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
splits_dao = StockSplitsPolygonDAO(env)
```

**New Pattern:**
```python
# Vendor-specific
from dao.vendors.polygon_dao import PolygonDAO
polygon_dao = PolygonDAO()
polygon_dao.insert_stock_split(symbol, split_date, split_ratio)

# OR unified
from dao.corporate_actions.stock_splits_dao import StockSplitsDAO
splits_dao = StockSplitsDAO()
splits_dao.create(split_data)
```

#### Instrument Operations Migration

**Old Pattern:**
```python
from dao.instrument_polygon_dao import InstrumentPolygonDAO
instrument_dao = InstrumentPolygonDAO(env)
```

**New Pattern:**
```python
# Vendor-specific
from dao.vendors.polygon_dao import PolygonDAO
polygon_dao = PolygonDAO()
polygon_dao.insert_instrument(symbol, name, market)

# OR unified
from dao.instruments.instruments_dao import InstrumentsDAO
instruments_dao = InstrumentsDAO()
instruments_dao.create(instrument_data)
```

## Key Benefits

### 1. Unified Interface
- All DAOs now inherit from `BaseDAO` with consistent CRUD operations
- Standardized error handling and logging
- Consistent validation across all operations

### 2. Vendor Abstraction
- `PolygonDAO` and `TiingoDAO` provide all vendor-specific operations in one place
- Vendor-specific transformations and validations consolidated
- Easy to add new vendors following the same pattern

### 3. Reduced Duplication
- Base DAO eliminates repetitive CRUD code
- Shared validation and transformation logic
- Centralized database connection management

### 4. Better Organization
- Logical grouping of related operations
- Clear separation between vendor-specific and unified operations
- Easier to maintain and extend

## Migration Steps

### Phase 1: Update Core Files
1. Update main application files to use new DAO imports
2. Update configuration and setup scripts
3. Update primary business logic files

### Phase 2: Update Tests
1. Update test files to use new DAO structure
2. Verify all existing functionality works with new DAOs
3. Add tests for new unified operations

### Phase 3: Update Scripts and Tools
1. Update all scripts in the `scripts/` directory
2. Update deployment and migration scripts
3. Update monitoring and maintenance tools

### Phase 4: Cleanup
1. Remove old DAO files after confirming all migrations are working
2. Update documentation and README files
3. Update CI/CD pipelines if needed

## Files Requiring Updates

Based on the grep analysis, the following files need migration:

### Core Application Files
- `src/market_data/eod/daily_price_polygon.py`
- `src/market_data/eod/daily_price_tiingo.py`
- `src/market_data/eod/fast_daily_price_backfill.py`
- `src/market_data/eod/unified_db_daily_price_market_data_manager.py`
- `src/secmaster/dividend_polygon.py`
- `src/secmaster/dividend_tiingo.py`
- All files in `src/secmaster/` that reference old DAOs

### Test Files
- `tests/dao/test_all_daos_with_test_db.py`
- `tests/dao/test_polygon_daos.py`
- `tests/market_data/eod/test_daily_polygon.py`
- `tests/market_data/eod/test_daily_tiingo.py`
- All test files referencing old DAO structure

### Integration Tests
- `intg/db/test_integration_polygon_daos_intg.py`
- `intg/market_data/eod/test_daily_polygon_intg.py`
- `intg/market_data/eod/test_secmaster_daily_prices_tiingo_intg.py`

## Configuration Changes

### Environment Setup
The new DAO structure uses centralized configuration through `core.config.settings`:

```python
# Old pattern
from config.environment import Environment
env = Environment()
dao = SomeDAO(env)

# New pattern  
from core.config.settings import get_settings
# DAOs automatically use centralized settings
dao = SomeDAO()
```

### Database Connection
Database connections are now managed centrally:

```python
# Old pattern
# Each DAO managed its own connection

# New pattern
from core.database.connection_manager import DatabaseConnectionManager
# All DAOs use the centralized connection manager
```

## Validation and Error Handling

### Enhanced Validation
```python
# New validation framework
from core.validation.data_validators import MarketDataValidator
validator = MarketDataValidator()
result = validator.validate(data)
```

### Structured Error Handling
```python
# New exception hierarchy
from core.exceptions.custom_exceptions import DataValidationError, DatabaseError
try:
    dao.create(data)
except DataValidationError as e:
    logger.error(f"Validation failed: {e}")
except DatabaseError as e:
    logger.error(f"Database error: {e}")
```

## Testing the Migration

### 1. Unit Tests
Ensure all existing unit tests pass with new DAO structure:
```bash
PYTHONPATH=src python -m pytest tests/dao/ -v
```

### 2. Integration Tests  
Verify integration tests work with new structure:
```bash
PYTHONPATH=src python -m pytest intg/ -v
```

### 3. End-to-End Testing
Test complete workflows with new DAO structure:
```bash
PYTHONPATH=src python -m pytest tests/market_data/ -v
```

## Rollback Plan

If issues are discovered during migration:

1. **Keep Old Files**: The old DAO files are preserved during migration
2. **Feature Flags**: Implement feature flags to switch between old and new DAO usage
3. **Gradual Migration**: Migrate one component at a time to isolate issues
4. **Comprehensive Testing**: Test each migrated component thoroughly before proceeding

## Performance Considerations

### Connection Pooling
The new structure uses centralized connection pooling which should improve performance:
- Reduced connection overhead
- Better resource utilization
- Configurable pool sizes

### Caching
New validation and transformation logic includes caching for better performance:
- Cached validation rules
- Cached vendor configurations
- Reduced redundant operations

### Bulk Operations
Enhanced bulk operation support:
- Optimized bulk inserts
- Batch validation
- Reduced database round trips

## Monitoring Migration Progress

Track migration progress using these metrics:
1. **Import Usage**: Monitor which files still use old imports
2. **Test Coverage**: Ensure all migrated code has test coverage
3. **Performance**: Monitor database performance during migration
4. **Error Rates**: Track any increase in errors during migration

## Support and Documentation

### Getting Help
- Review this migration guide
- Check the new DAO class documentation
- Review test files for usage examples
- Contact the development team for complex migration scenarios

### Additional Resources
- `src/core/README.md` - Core infrastructure documentation
- `src/dao/README.md` - DAO architecture documentation  
- Test files - Real usage examples
- REPOSITORY_ANALYSIS.md - Original refactoring analysis