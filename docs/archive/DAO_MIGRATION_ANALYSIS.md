# DAO Migration Analysis Report
Generated on: Sun Aug 17 13:49:17 PDT 2025

## Summary
- Files requiring migration: 39

## Files Requiring Migration

### intg/db/test_integration_polygon_daos_intg.py

**Old Imports Found:**
```python
Line 5: from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
Line 6: from dao.dividend_polygon_dao import DividendPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 11: dao = StockSplitsPolygonDAO(env)
Line 40: dao = DividendPolygonDAO(env)
```

### intg/market_data/eod/test_daily_polygon_intg.py

**Old Imports Found:**
```python
Line 3: from dao.instrument_polygon_dao import InstrumentPolygonDAO
Line 4: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 14: instrument_dao = InstrumentPolygonDAO(env)
Line 15: prices_dao = DailyPricesPolygonDAO(env)
```

### intg/market_data/eod/test_secmaster_daily_prices_tiingo_intg.py

**Class Usage to Update:**
```python
Line 76: dao = DailyPricesTiingoDAO(env)
```

### scripts/maintenance/migrate_dao_imports.py

**Old Imports Found:**
```python
Line 25: 'from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO':
Line 27: 'from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO':
Line 31: 'from dao.dividend_polygon_dao import DividendPolygonDAO':
Line 33: 'from dao.dividend_tiingo_dao import DividendTiingoDAO':
Line 37: 'from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO':
Line 39: 'from dao.stock_splits_tiingo_dao import StockSplitsTiingoDAO':
Line 43: 'from dao.instrument_polygon_dao import InstrumentPolygonDAO':
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 49: 'DailyPricesPolygonDAO': 'PolygonDAO',
Line 50: 'DailyPricesTiingoDAO': 'TiingoDAO',
Line 51: 'DividendPolygonDAO': 'PolygonDAO',
Line 52: 'DividendTiingoDAO': 'TiingoDAO',
Line 53: 'StockSplitsPolygonDAO': 'PolygonDAO',
... and 2 more
```

### src/dao/market_data/daily_prices_dao.py

### src/dao/vendors/polygon_dao.py

### src/dao/vendors/tiingo_dao.py

### src/market_data/agent/instrument_data_agent.py

**Old Imports Found:**
```python
Line 20: from dao.instrument_polygon_dao import InstrumentPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 166: self.polygon_dao = InstrumentPolygonDAO(self.env)
```

### src/market_data/agent/simple_test_agent.py

**Class Usage to Update:**
```python
Line 24: instrument_polygon_dao = InstrumentPolygonDAO(env)
```

### src/market_data/eod/daily_polygon_ray_utils.py

**Old Imports Found:**
```python
Line 3: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
Line 4: from dao.instrument_polygon_dao import InstrumentPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 18: prices_dao = DailyPricesPolygonDAO(env)
```

### src/market_data/eod/daily_price_polygon.py

**Old Imports Found:**
```python
Line 12: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
Line 21: from dao.instrument_polygon_dao import InstrumentPolygonDAO
Line 305: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 24: instrument_dao = InstrumentPolygonDAO(env)
Line 64: async def insert_prices(prices, instrument_id, shares_outstanding, dao: DailyPricesPolygonDAO, env=None):
Line 97: async def get_existing_dates_polygon(dao: DailyPricesPolygonDAO, instrument_id, start_date, end_date):
Line 137: instrument_dao = InstrumentPolygonDAO(env)
Line 177: instrument_dao = InstrumentPolygonDAO(env)
... and 3 more
```

### src/market_data/eod/daily_price_tiingo.py

**Old Imports Found:**
```python
Line 15: from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
```

**Class Usage to Update:**
```python
Line 42: async def get_existing_dates(dao: DailyPricesTiingoDAO, instrument_id, start_date, end_date):
Line 81: async def fetch_and_insert_symbol(dao: DailyPricesTiingoDAO, session, instrument_id, symbol, start_date, end_date, ok_status_id, no_data_status_id):
Line 322: dao = DailyPricesTiingoDAO(env)
```

### src/market_data/eod/daily_tiingo_ray_utils.py

**Old Imports Found:**
```python
Line 4: from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
```

**Class Usage to Update:**
```python
Line 20: dao = DailyPricesTiingoDAO(env)
```

### src/market_data/eod/fast_daily_price_backfill.py

**Old Imports Found:**
```python
Line 35: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

### src/market_data/eod/unified_db_daily_price_market_data_manager.py

**Old Imports Found:**
```python
Line 5: from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
Line 6: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 19: self.tiingo_dao = DailyPricesTiingoDAO(self.env)
Line 20: self.polygon_dao = DailyPricesPolygonDAO(self.env)
```

### src/secmaster/dividend_polygon.py

**Old Imports Found:**
```python
Line 5: from dao.dividend_polygon_dao import DividendPolygonDAO
Line 22: from dao.dividend_polygon_dao import DividendPolygonDAO
Line 48: from dao.instrument_polygon_dao import InstrumentPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 8: # get_all_spy_tickers is obsolete, use InstrumentPolygonDAO.get_all_symbols instead.
Line 24: dao = DividendPolygonDAO(env)
Line 61: instrument_dao = InstrumentPolygonDAO(env)
Line 62: dividend_dao = DividendPolygonDAO(env)
```

### src/secmaster/dividend_tiingo.py

**Old Imports Found:**
```python
Line 5: from dao.dividend_tiingo_dao import DividendTiingoDAO
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
```

**Class Usage to Update:**
```python
Line 63: dao = DividendTiingoDAO(env)
```

### src/secmaster/native_range_dividend_tiingo.py

**Old Imports Found:**
```python
Line 5: from dao.dividend_tiingo_dao import DividendTiingoDAO
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
```

**Class Usage to Update:**
```python
Line 68: dao = DividendTiingoDAO(env)
```

### src/secmaster/populate_unified_instruments.py

**Old Imports Found:**
```python
Line 4: from dao.instrument_polygon_dao import InstrumentPolygonDAO
Line 124: from dao.instrument_polygon_dao import InstrumentPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 104: print("[INFO] No tickers or universe provided. Fetching all symbols from InstrumentPolygonDAO.")
Line 131: polygon_dao = InstrumentPolygonDAO(env)
Line 295: polygon_dao = InstrumentPolygonDAO(env)
```

### src/secmaster/range_dividend_polygon.py

**Old Imports Found:**
```python
Line 6: from dao.dividend_polygon_dao import DividendPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 90: div_dao = DividendPolygonDAO(env)
```

### src/secmaster/range_dividend_tiingo.py

**Old Imports Found:**
```python
Line 6: from dao.dividend_tiingo_dao import DividendTiingoDAO
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
```

**Class Usage to Update:**
```python
Line 84: div_dao = DividendTiingoDAO(env)
```

### src/secmaster/range_splits_polygon.py

**Old Imports Found:**
```python
Line 6: from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 105: splits_dao = StockSplitsPolygonDAO(env)
```

### src/secmaster/range_splits_tiingo.py

**Old Imports Found:**
```python
Line 6: from dao.stock_splits_tiingo_dao import StockSplitsTiingoDAO
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
```

**Class Usage to Update:**
```python
Line 81: splits_dao = StockSplitsTiingoDAO(env)
```

### src/secmaster/run_daily_price_backfill.py

**Old Imports Found:**
```python
Line 31: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 36: prices_dao = DailyPricesPolygonDAO(env)
```

### src/secmaster/splits_divs_polygon.py

**Old Imports Found:**
```python
Line 63: from dao.instrument_polygon_dao import InstrumentPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 12: # get_all_spy_tickers is obsolete, use InstrumentPolygonDAO.get_all_symbols instead.
Line 71: dao = InstrumentPolygonDAO(env)
```

### src/secmaster/splits_polygon.py

**Old Imports Found:**
```python
Line 5: from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
Line 22: from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
Line 49: from dao.instrument_polygon_dao import InstrumentPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 8: # get_all_spy_tickers is obsolete, use InstrumentPolygonDAO.get_all_symbols instead.
Line 24: dao = StockSplitsPolygonDAO(env)
Line 63: instrument_dao = InstrumentPolygonDAO(env)
Line 64: splits_dao = StockSplitsPolygonDAO(env)
```

### temp/standalone_test_dao.py

**Class Usage to Update:**
```python
Line 87: class TestInstrumentPolygonDAO:
Line 88: """Test version of InstrumentPolygonDAO."""
Line 131: instrument_polygon_dao = TestInstrumentPolygonDAO(env)
```

### tests/dao/test_all_daos_with_test_db.py

**Old Imports Found:**
```python
Line 14: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
Line 15: from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
Line 22: from dao.instrument_polygon_dao import InstrumentPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 511: dao = DailyPricesPolygonDAO(env)
Line 542: dao = DailyPricesTiingoDAO(env)
```

### tests/dao/test_polygon_daos.py

**Old Imports Found:**
```python
Line 5: from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
Line 6: from dao.dividend_polygon_dao import DividendPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 14: dao = StockSplitsPolygonDAO(env)
Line 64: dao = DividendPolygonDAO(env)
```

### tests/market_data/eod/test_daily_polygon.py

**Old Imports Found:**
```python
Line 6: from dao.instrument_polygon_dao import InstrumentPolygonDAO
Line 7: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 16: prices_dao = DailyPricesPolygonDAO(env)
```

### tests/market_data/eod/test_daily_tiingo.py

**Old Imports Found:**
```python
Line 10: from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
```

### tests/market_data/eod/test_turbo_price_backfill_integration.py

**Old Imports Found:**
```python
Line 97: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
Line 188: from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
Line 270: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
Line 352: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
Line 432: from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.tiingo_dao import TiingoDAO
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 98: prices_dao = DailyPricesPolygonDAO(env)
Line 189: prices_dao = DailyPricesTiingoDAO(env)
Line 271: prices_dao = DailyPricesPolygonDAO(env)
Line 353: prices_dao = DailyPricesPolygonDAO(env)
Line 433: prices_dao = DailyPricesPolygonDAO(env)
```

### tests/market_data/eod/test_unify_daily_prices_db.py

**Class Usage to Update:**
```python
Line 21: tiingo_dao = DailyPricesTiingoDAO(env)
Line 22: polygon_dao = DailyPricesPolygonDAO(env)
```

### tests/market_data/eod/test_unify_daily_prices_regression.py

**Class Usage to Update:**
```python
Line 20: polygon_dao = DailyPricesPolygonDAO(env)
```

### tests/market_data/test_unified_db_daily_price_market_data_manager.py

**Class Usage to Update:**
```python
Line 27: tiingo_dao = DailyPricesTiingoDAO(env)
Line 28: polygon_dao = DailyPricesPolygonDAO(env)
```

### tests/market_data/test_unified_mgr.py

**Class Usage to Update:**
```python
Line 16: tiingo_dao = DailyPricesTiingoDAO(env)
Line 17: polygon_dao = DailyPricesPolygonDAO(env)
```

### tests/secmaster/test_dividend_polygon.py

**Old Imports Found:**
```python
Line 24: from dao.dividend_polygon_dao import DividendPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 27: dao = DividendPolygonDAO(env)
```

### tests/secmaster/test_range_splits_polygon.py

**Old Imports Found:**
```python
Line 6: from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

### tests/secmaster/test_splits_polygon.py

**Old Imports Found:**
```python
Line 26: from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
```

**Suggested New Imports:**
```python
from dao.vendors.polygon_dao import PolygonDAO
```

**Class Usage to Update:**
```python
Line 29: dao = StockSplitsPolygonDAO(env)
```


## Migration Instructions

1. Review each file listed above
2. Update imports according to suggestions
3. Update class instantiations and method calls
4. Test thoroughly after each file migration
5. Update corresponding test files

## Next Steps

- Use `migrate_dao_imports.py --migrate <file>` to auto-migrate individual files
- Run tests after each migration: `PYTHONPATH=src python -m pytest`
- Review DAO_MIGRATION_GUIDE.md for detailed patterns
