# Orphaned Files Analysis

## Files Never Imported by Other Modules

This analysis identifies Python files that are never imported, indicating they may be:
- Standalone scripts (keep)
- Dead code (remove)
- Entry points (keep)
- Legacy code (archive)


## Low Risk Files (5 files)

- `src/intg_conftest.py` - File never imported by other modules
- `src/simple_main.py` - File never imported by other modules
- `src/api/backtest_analytics_api.py` - File never imported by other modules
- `src/market_data/agent/simple_test_agent.py` - File never imported by other modules
- `src/market_data/agent/run_test_with_env.py` - File never imported by other modules

## Medium Risk Files (134 files)

- `src/universe_state_interval_pb2.py` - File never imported by other modules
- `src/utils/db_utils.py` - File never imported by other modules
- `src/modeling/multi_timeframe_signal_pipeline.py` - File never imported by other modules
- `src/modeling/configurable_train_data_generator.py` - File never imported by other modules
- `src/modeling/portfolio_evaluator.py` - File never imported by other modules
- `src/modeling/forecast_with_transformer.py` - File never imported by other modules
- `src/modeling/pytorch_runner_train_data_generator.py` - File never imported by other modules
- `src/modeling/pytorch_multi_instrument_train.py` - File never imported by other modules
- `src/modeling/cross_timeframe_aligner.py` - File never imported by other modules
- `src/modeling/forecast_with_transformer_all_instruments.py` - File never imported by other modules
- ... and 124 more files

## High Risk Files (18 files)

- `src/current_portfolio_api.py` - File never imported by other modules
- `src/analytics_api_dynamic.py` - File never imported by other modules
- `src/services/analytics_service_class.py` - File never imported by other modules
- `src/services/tfdv_integration_service.py` - File never imported by other modules
- `src/services/type_aware_analytics_service.py` - File never imported by other modules
- `src/services/exchange_service.py` - File never imported by other modules
- `src/services/dataset_metadata_service.py` - File never imported by other modules
- `src/storage/dual_write_manager.py` - File never imported by other modules
- `src/storage/multi_scale_minute_manager.py` - File never imported by other modules
- `src/db/migration_manager.py` - File never imported by other modules
- ... and 8 more files

## Recommended Actions

### Low Risk Files
- Review and confirm they are standalone scripts
- Move to `scripts/` directory if they are utilities
- Remove if they are obsolete test files

### Medium Risk Files  
- Manual code review required
- Check if they contain reusable logic
- Consider refactoring into modules if valuable

### High Risk Files
- **DO NOT REMOVE** without thorough analysis
- May be API endpoints or services
- Could be called by external systems
- Verify through runtime analysis

## Archive Script

```bash
#!/bin/bash
# Archive low-risk orphaned files
mkdir -p archived_orphaned_files/$(date +%Y%m%d)

# Move only confirmed low-risk files
echo "This script requires manual customization based on review"
```
