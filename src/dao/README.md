# Data Access Objects (`src/dao/`)

This directory contains Data Access Objects (DAOs) that provide a clean interface for database operations across the ATS-GenAI system.

## Overview

The DAO layer abstracts database interactions and provides environment-aware data access with automatic table prefixing for multi-environment deployments (`dev_`, `intg_`, `prod_`).

## Directory Structure

```
dao/
├── daily_market_cap_dao.py         # Market capitalization data
├── daily_prices_dao.py             # Generic daily price operations
├── daily_prices_polygon_dao.py     # Polygon-specific price data
├── daily_prices_tiingo_dao.py      # Tiingo-specific price data
├── db_version_dao.py               # Database version management
├── dividend_polygon_dao.py         # Polygon dividend data
├── dividend_tiingo_dao.py          # Tiingo dividend data
├── dividends_dao.py                # Generic dividend operations
├── economic_events_dao.py          # Economic events data
├── events_dao.py                   # General event system
├── factor_interval_dao.py          # Factor-based intervals
├── fundamentals_dao.py             # Fundamental data
├── instrument_indicator_interval_dao.py  # Indicator intervals
├── instrument_interval_dao.py      # Instrument time intervals
├── instrument_polygon_dao.py       # Polygon instrument data
├── instrument_xrefs_dao.py         # Instrument cross-references
├── instruments_dao.py              # Generic instrument operations
├── secmaster_dao.py                # Security master data
├── status_code_dao.py              # Status codes and metadata
├── stock_splits_dao.py             # Generic stock split operations
├── stock_splits_polygon_dao.py     # Polygon split data
├── stock_splits_tiingo_dao.py      # Tiingo split data
├── universe_dao.py                 # Universe management
├── universe_membership_dao.py      # Universe membership tracking
├── universe_state_interval_dao.py  # Universe state intervals
└── vendors_dao.py                  # Data vendor management
```

## ⚠️ **Current Issues & Refactoring Needed**

### **Problem: High Code Duplication**
The DAO layer has significant duplication with vendor-specific implementations for similar operations:

```python
# Current problematic pattern:
daily_prices_dao.py           # Generic implementation
daily_prices_polygon_dao.py   # Polygon-specific (duplicates logic)
daily_prices_tiingo_dao.py    # Tiingo-specific (duplicates logic)

dividends_dao.py              # Generic implementation  
dividend_polygon_dao.py       # Polygon-specific (duplicates logic)
dividend_tiingo_dao.py        # Tiingo-specific (duplicates logic)

stock_splits_dao.py           # Generic implementation
stock_splits_polygon_dao.py   # Polygon-specific (duplicates logic)
stock_splits_tiingo_dao.py    # Tiingo-specific (duplicates logic)
```

### **Suggested Refactoring**
```python
# Proposed structure:
dao/
├── base/
│   ├── base_dao.py              # Common CRUD operations
│   └── vendor_dao.py            # Base vendor-specific operations
├── market_data/
│   ├── daily_prices_dao.py      # Unified daily prices
│   ├── corporate_actions_dao.py # Unified splits/dividends
│   └── market_cap_dao.py        # Market capitalization
├── instruments/
│   ├── instruments_dao.py       # Instrument management
│   ├── instrument_xrefs_dao.py  # Cross-references
│   └── fundamentals_dao.py      # Fundamental data
├── vendors/
│   ├── polygon_dao.py           # All Polygon operations
│   ├── tiingo_dao.py            # All Tiingo operations
│   └── base_vendor_dao.py       # Vendor base class
├── events/
│   ├── events_dao.py            # Event operations
│   └── economic_events_dao.py   # Economic events
├── universe/
│   ├── universe_dao.py          # Universe management
│   └── universe_membership_dao.py # Membership tracking
└── system/
    ├── db_version_dao.py        # Database versioning
    └── status_code_dao.py       # Status management
```

## Core Features

### 🌍 **Environment-Aware Operations**
All DAOs support automatic environment-specific table prefixing:

```python
from dao.daily_prices_dao import DailyPricesDAO
from config.environment import Environment

# Automatically uses correct table prefix based on environment
env = Environment()  # Uses dev_, intg_, or prod_ prefix
dao = DailyPricesDAO(env)

# Queries dev_daily_prices, intg_daily_prices, or prod_daily_prices
prices = dao.get_daily_prices('AAPL', start_date, end_date)
```

### 🔄 **Multi-Vendor Support**
Support for multiple data vendors with unified interfaces:

```python
# Polygon data
polygon_dao = DailyPricesPolygonDAO(env)
polygon_prices = polygon_dao.get_daily_prices('AAPL', '2024-01-01', '2024-01-31')

# Tiingo data  
tiingo_dao = DailyPricesTiingoDAO(env)
tiingo_prices = tiingo_dao.get_daily_prices('AAPL', '2024-01-01', '2024-01-31')

# Generic interface (uses configured primary vendor)
generic_dao = DailyPricesDAO(env)
prices = generic_dao.get_daily_prices('AAPL', '2024-01-01', '2024-01-31')
```

### 📊 **Comprehensive Data Coverage**

#### **Market Data DAOs**
- **`daily_prices_dao.py`**: OHLCV data with adjustments
- **`daily_market_cap_dao.py`**: Market capitalization data
- **`dividends_dao.py`**: Dividend payments and ex-dates
- **`stock_splits_dao.py`**: Stock split events and ratios

#### **Instrument Management DAOs**
- **`instruments_dao.py`**: Security master data
- **`instrument_xrefs_dao.py`**: Symbol mappings across vendors
- **`fundamentals_dao.py`**: Fundamental data (P/E, revenue, etc.)

#### **Event System DAOs**
- **`events_dao.py`**: General event storage and retrieval
- **`economic_events_dao.py`**: Economic calendar events

#### **State Management DAOs**
- **`instrument_interval_dao.py`**: Time-based instrument state
- **`factor_interval_dao.py`**: Factor exposure intervals
- **`universe_state_interval_dao.py`**: Universe composition over time

## Usage Examples

### **Basic CRUD Operations**
```python
from dao.instruments_dao import InstrumentsDAO
from config.environment import Environment

env = Environment()
dao = InstrumentsDAO(env)

# Create instrument
instrument_id = dao.create_instrument(
    symbol='AAPL',
    name='Apple Inc.',
    sector='Technology',
    market_cap=3000000000000
)

# Read instrument
instrument = dao.get_instrument_by_symbol('AAPL')

# Update instrument
dao.update_instrument(instrument_id, {'sector': 'Technology - Hardware'})

# Delete instrument
dao.delete_instrument(instrument_id)
```

### **Market Data Operations**
```python
from dao.daily_prices_dao import DailyPricesDAO
from datetime import datetime

dao = DailyPricesDAO(env)

# Get price history
prices = dao.get_daily_prices(
    symbol='AAPL',
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31)
)

# Bulk insert prices
price_data = [
    {
        'symbol': 'AAPL',
        'date': datetime(2024, 1, 2),
        'open': 185.0,
        'high': 187.5,
        'low': 184.0,
        'close': 186.5,
        'volume': 50000000,
        'adjusted_close': 186.5
    }
]
dao.bulk_insert_daily_prices(price_data)
```

### **Corporate Actions**
```python
from dao.dividends_dao import DividendsDAO
from dao.stock_splits_dao import StockSplitsDAO

# Dividend operations
div_dao = DividendsDAO(env)
dividends = div_dao.get_dividends_by_symbol('AAPL', year=2024)

# Split operations
split_dao = StockSplitsDAO(env)
splits = split_dao.get_splits_by_symbol('AAPL', start_date, end_date)
```

### **Event Management**
```python
from dao.events_dao import EventsDAO

events_dao = EventsDAO(env)

# Create event
event_id = events_dao.create_event(
    event_type='earnings',
    symbol='AAPL',
    event_date=datetime(2024, 2, 1),
    data={'eps_estimate': 2.10, 'revenue_estimate': 118000000000}
)

# Query events
earnings_events = events_dao.get_events_by_type_and_date_range(
    event_type='earnings',
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31)
)
```

## Database Schema Integration

### **Environment-Specific Tables**
```sql
-- Development environment
dev_daily_prices
dev_instruments  
dev_events

-- Integration environment
intg_daily_prices
intg_instruments
intg_events

-- Production environment
prod_daily_prices
prod_instruments
prod_events
```

### **Common Table Patterns**
- **Created/Updated Timestamps**: All tables include audit timestamps
- **Vendor Source Tracking**: Many tables track data source vendor
- **Data Quality Flags**: Tables include validation status fields
- **Soft Deletes**: Support for logical deletion where appropriate

## Error Handling

### **Database Connection Errors**
```python
from dao.base_dao import DatabaseConnectionError

try:
    prices = dao.get_daily_prices('AAPL', start_date, end_date)
except DatabaseConnectionError as e:
    logger.error(f"Database connection failed: {e}")
    # Implement retry logic or fallback
```

### **Data Validation Errors**
```python
from dao.base_dao import DataValidationError

try:
    dao.create_instrument(invalid_data)
except DataValidationError as e:
    logger.error(f"Data validation failed: {e}")
    # Handle validation errors
```

## Performance Considerations

### **Query Optimization**
- **Indexed Queries**: All DAOs use properly indexed queries
- **Batch Operations**: Bulk insert/update operations for large datasets
- **Connection Pooling**: Efficient database connection management
- **Query Caching**: Frequently accessed data is cached

### **Large Dataset Handling**
```python
# Pagination for large result sets
def get_daily_prices_paginated(self, symbol, start_date, end_date, 
                              page_size=1000, offset=0):
    """Get prices with pagination for large datasets"""
    
# Streaming for very large datasets
def stream_daily_prices(self, symbol, start_date, end_date):
    """Stream prices to avoid memory issues"""
```

## Testing

### **Unit Tests**
```python
# tests/dao/test_daily_prices_dao.py
def test_get_daily_prices():
    dao = DailyPricesDAO(test_env)
    prices = dao.get_daily_prices('AAPL', start_date, end_date)
    assert len(prices) > 0
    assert all(p.symbol == 'AAPL' for p in prices)
```

### **Integration Tests**
```python
# tests/dao/test_dao_integration.py
def test_cross_dao_operations():
    # Test operations across multiple DAOs
    instruments_dao = InstrumentsDAO(test_env)
    prices_dao = DailyPricesDAO(test_env)
    
    # Create instrument then add prices
    instrument_id = instruments_dao.create_instrument(test_data)
    prices_dao.bulk_insert_daily_prices(test_prices)
```

## Migration Guide

### **From Current Structure to Proposed**
1. **Phase 1**: Create base DAO classes with common operations
2. **Phase 2**: Migrate vendor-specific operations to unified structure  
3. **Phase 3**: Update all imports across codebase
4. **Phase 4**: Remove duplicate DAO files

### **Breaking Changes**
- Import paths will change for refactored DAOs
- Some method signatures may be standardized
- Vendor-specific DAOs will be consolidated

## Best Practices

### **DAO Design Principles**
1. **Single Responsibility**: Each DAO handles one entity or closely related entities
2. **Environment Awareness**: All DAOs support multi-environment deployment
3. **Vendor Abstraction**: Hide vendor-specific details behind unified interfaces
4. **Error Handling**: Consistent error handling across all DAOs
5. **Performance**: Optimize for common query patterns

### **Usage Guidelines**
1. **Use Environment**: Always pass Environment instance to DAOs
2. **Handle Errors**: Implement proper error handling for database operations
3. **Batch Operations**: Use bulk operations for large datasets
4. **Connection Management**: Let DAOs handle connection lifecycle
5. **Testing**: Write comprehensive unit and integration tests

---

**🚨 Note**: This directory requires significant refactoring to eliminate code duplication and improve maintainability. See the refactoring section above for recommended improvements.