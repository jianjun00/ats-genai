# UnifiedMarketDataManager Test Data

## Real Minute Bar Test Dataset

This directory contains a curated subset of real FirstRate minute bar data for deterministic testing of UnifiedMarketDataManager.

### Data Selection Criteria:
- **Symbols**: AAPL, TSLA (high-volume, stable instruments)
- **Date Range**: 2024-08-01 to 2024-08-02 (2 trading days)
- **Coverage**: Market open, intraday, market close scenarios
- **Edge Cases**: Market gaps, volume spikes, price movements

### File Structure:
```
test_data/
├── firstrate/
│   ├── A/AAPL/2024/08/AAPL_2024_08.parquet      # Real AAPL minute data
│   └── T/TSLA/2024/08/TSLA_2024_08.parquet      # Real TSLA minute data
└── golden_files/
    └── test_unified_market_data_manager_golden/  # Test file subdirectory
        ├── test_get_ohlcv_1m_single_symbol_golden.json
        ├── test_get_ohlcv_5m_aggregation_golden.json
        ├── test_get_ohlcv_multiple_symbols_golden.json
        └── test_get_minute_ohlc_batch_compatibility_golden.json
```

### Data Characteristics:
- **Volume**: ~390 minutes per symbol per day (market hours)
- **Size**: Small enough for fast test execution
- **Stability**: Immutable historical data ensures consistent test results
- **Realism**: Real market data with actual price movements and volumes

### Regenerating Golden Files:
```bash
# Update golden files when expected behavior changes
PYTHONPATH=src python -m pytest tests/domains/market_data/services/core/test_unified_market_data_manager_golden.py --update-golden
```