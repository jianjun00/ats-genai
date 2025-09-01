# Quick Cleanup Recommendations

## Immediate Actions

### 1. Consolidate Duplicate File Names


**__init__.py** - 45 copies found:
- src/__init__.py
- src/schema/__init__.py
- src/modeling/__init__.py
- src/llm/__init__.py
- src/pipeline/__init__.py
- src/secmaster/__init__.py
- src/calendars/__init__.py
- src/agents/__init__.py
- src/frontfill/__init__.py
- src/storage/__init__.py
- src/sentiment/__init__.py
- src/state/__init__.py
- src/auth/__init__.py
- src/dao/__init__.py
- src/core/__init__.py
- src/market_data/__init__.py
- src/models/__init__.py
- src/db/__init__.py
- src/signals/__init__.py
- src/universe/__init__.py
- src/events/__init__.py
- src/config/__init__.py
- src/validation/__init__.py
- src/economic_events/__init__.py
- src/training/__init__.py
- src/state/proto/__init__.py
- src/dao/corporate_actions/__init__.py
- src/dao/instruments/__init__.py
- src/dao/market_data/__init__.py
- src/dao/base/__init__.py
- src/dao/vendors/__init__.py
- src/core/utils/__init__.py
- src/core/database/__init__.py
- src/core/exceptions/__init__.py
- src/core/config/__init__.py
- src/core/logging/__init__.py
- src/core/validation/__init__.py
- src/market_data/news/__init__.py
- src/market_data/reconciliation/__init__.py
- src/market_data/minute/__init__.py
- src/market_data/realtime/src/utils/__init__.py
- src/market_data/realtime/src/core/__init__.py
- src/market_data/realtime/src/config/__init__.py
- src/models/attention/__init__.py
- src/events/ingest/__init__.py

**Action:** Review and consolidate or rename for clarity.

**factor_interval_pb2.py** - 2 copies found:
- src/factor_interval_pb2.py
- src/state/proto/factor_interval_pb2.py

**Action:** Review and consolidate or rename for clarity.

**universe_state_interval_pb2.py** - 2 copies found:
- src/universe_state_interval_pb2.py
- src/state/proto/universe_state_interval_pb2.py

**Action:** Review and consolidate or rename for clarity.

**indicator_interval_pb2.py** - 2 copies found:
- src/indicator_interval_pb2.py
- src/state/proto/indicator_interval_pb2.py

**Action:** Review and consolidate or rename for clarity.

**time_duration_pb2.py** - 2 copies found:
- src/time_duration_pb2.py
- src/state/proto/time_duration_pb2.py

**Action:** Review and consolidate or rename for clarity.

**instrument_interval_pb2.py** - 2 copies found:
- src/instrument_interval_pb2.py
- src/state/proto/instrument_interval_pb2.py

**Action:** Review and consolidate or rename for clarity.

**adv_mktcap_polygon.py** - 2 copies found:
- src/secmaster/adv_mktcap_polygon.py
- src/universe/adv_mktcap_polygon.py

**Action:** Review and consolidate or rename for clarity.

**models.py** - 2 copies found:
- src/auth/models.py
- src/market_data/agent/models.py

**Action:** Review and consolidate or rename for clarity.

**daily_prices_dao.py** - 2 copies found:
- src/dao/daily_prices_dao.py
- src/dao/market_data/daily_prices_dao.py

**Action:** Review and consolidate or rename for clarity.

**dividends_dao.py** - 2 copies found:
- src/dao/dividends_dao.py
- src/dao/corporate_actions/dividends_dao.py

**Action:** Review and consolidate or rename for clarity.

**vendor_dao.py** - 2 copies found:
- src/dao/vendor_dao.py
- src/dao/base/vendor_dao.py

**Action:** Review and consolidate or rename for clarity.

**stock_splits_dao.py** - 2 copies found:
- src/dao/stock_splits_dao.py
- src/dao/corporate_actions/stock_splits_dao.py

**Action:** Review and consolidate or rename for clarity.

**instruments_dao.py** - 2 copies found:
- src/dao/instruments_dao.py
- src/dao/instruments/instruments_dao.py

**Action:** Review and consolidate or rename for clarity.

**universe.py** - 2 copies found:
- src/signals/universe.py
- src/universe/universe.py

**Action:** Review and consolidate or rename for clarity.

**data_quality_validator.py** - 2 copies found:
- src/universe/data_quality_validator.py
- src/monitoring/data_quality_validator.py

**Action:** Review and consolidate or rename for clarity.

**logging_config.py** - 2 copies found:
- src/config/logging_config.py
- src/market_data/agent/logging_config.py

**Action:** Review and consolidate or rename for clarity.

**app.py** - 2 copies found:
- src/services/slack_webhook/app.py
- src/services/intraday_populator/app.py

**Action:** Review and consolidate or rename for clarity.

### 2. Create Common Imports Module

Most frequently imported modules that could be centralized:

- `import logging` (used in 230 files)
- `import asyncio` (used in 217 files)
- `import os` (used in 176 files)
- `import asyncpg` (used in 167 files)
- `import pandas as pd` (used in 115 files)
- `from config.environment import Environment` (used in 104 files)
- `import json` (used in 97 files)
- `from dataclasses import dataclass` (used in 94 files)
- `import numpy as np` (used in 87 files)
- `from datetime import datetime, timedelta` (used in 78 files)


**Action:** Create `src/common/imports.py` with these common imports.

### 3. Review Duplicate Functions


**def __init__(self)** - 94 identical signatures:
- registry.py:16
- multi_timeframe_signal_pipeline.py:32
- cross_timeframe_aligner.py:65
- enhanced_feature_types.py:209
- event_analysis.py:400
- pilot_router.py:36
- minute_data_storage_calculator.py:17
- analytics_service.py:71
- exchange_service.py:26
- dev_operations.py:19
- social_media_analyzer.py:91
- social_media_analyzer.py:200
- news_sentiment_analyzer.py:78
- news_sentiment_analyzer.py:193
- news_sentiment_analyzer.py:230
- sentiment_integrator.py:96
- api_key_manager.py:15
- middleware.py:18
- exchange_dao.py:22
- vendor_dao.py:21
- instrument_xref_dao.py:23
- enhanced_indicators.py:210
- enhanced_indicators.py:527
- indicator.py:46
- indicator.py:69
- indicator.py:135
- indicator.py:181
- indicator.py:212
- indicator.py:251
- indicator.py:318
- indicator.py:538
- indicator.py:605
- indicator.py:674
- indicator.py:744
- indicator.py:810
- indicator.py:877
- indicator.py:942
- indicator.py:1010
- indicator.py:1080
- indicator.py:1151
- indicator.py:1222
- indicator.py:1288
- feature_registry.py:42
- feature_registry.py:159
- technical_analysis_framework.py:62
- technical_analysis_framework.py:106
- technical_analysis_framework.py:255
- technical_analysis_framework.py:318
- technical_analysis_framework.py:421
- technical_analysis_framework.py:527
- technical_analysis_framework.py:587
- technical_analysis_framework.py:646
- label_registry.py:190
- economic_events_classifier.py:190
- feature_flags.py:182
- training_data_job_runner.py:37
- backtest_analytics_api.py:94
- factor_framework.py:45
- factor_framework.py:295
- signal_generation.py:82
- signal_generation.py:120
- signal_generation.py:193
- signal_generation.py:286
- signal_generation.py:333
- validate_schema.py:43
- enhanced_eod_service.py:443
- app.py:66
- minute_price_service.py:318
- file_based_minute_service.py:240
- adaptive_sr_model.py:65
- simple_trade_chart.py:51
- dividends_dao.py:24
- stock_splits_dao.py:24
- instruments_dao.py:24
- daily_prices_dao.py:27
- polygon_dao.py:26
- tiingo_dao.py:26
- connection_manager.py:36
- mixins.py:29
- mixins.py:233
- settings.py:38
- alert_handlers.py:39
- run_data_agent_mock.py:268
- test_db_connection.py:24
- mcp_integration.py:22
- streaming_collector.py:40
- gap_detector.py:42
- daily_validation.py:50
- metrics_exporter.py:25
- metrics_exporter.py:41
- simple_streaming_collector.py:40
- base_realtime_collector.py:90
- weekly_backfill.py:68
- realtime_batch_validator.py:51

**Action:** Review for consolidation into utility module.

**def __init__(self, env** - 60 identical signatures:
- analytics_api_dynamic.py:126
- portfolio_analytics.py:159
- secmaster.py:12
- frontfill_orchestrator.py:42
- universe_state_builder.py:242
- dividend_polygon_dao.py:5
- daily_prices_dao.py:6
- daily_prices_fmp_dao.py:5
- instrument_polygon_dao.py:5
- fundamentals_polygon_dao.py:54
- daily_prices_alphavantage_dao.py:5
- stock_splits_polygon_dao.py:5
- fundamentals_fmp_dao.py:54
- instrument_interval_dao.py:6
- instrument_indicator_interval_dao.py:6
- daily_market_cap_dao.py:19
- factor_interval_dao.py:6
- vendors_dao.py:5
- dividends_dao.py:5
- instrument_xrefs_dao.py:128
- universe_state_interval_dao.py:116
- stock_splits_dao.py:5
- fundamentals_tiingo_dao.py:54
- status_code_dao.py:5
- daily_prices_polygon_dao.py:6
- daily_prices_tiingo_dao.py:6
- events_dao.py:6
- fundamentals_dao.py:5
- secmaster_dao.py:6
- dividend_tiingo_dao.py:5
- instruments_dao.py:6
- stock_splits_tiingo_dao.py:5
- db_version_dao.py:5
- universe_dao.py:5
- data_complete_universe_creator.py:44
- modeling_universe_creator.py:44
- historical_universe_creator.py:39
- universe_manager.py:26
- dynamic_modeling_universe.py:67
- data_quality_validator.py:52
- universe_db.py:13
- postgres_prometheus_exporter.py:75
- data_quality_validator.py:75
- population_service.py:28
- enhanced_eod_service.py:81
- enhanced_eod_service.py:204
- enhanced_eod_service.py:279
- enhanced_eod_service.py:362
- minute_price_service.py:151
- minute_price_service.py:209
- minute_price_service.py:263
- sr_backtester.py:101
- daily_prices_quandl_dao.py:6
- unified_db_daily_price_market_data_manager.py:15
- db_daily_price_market_data_manager.py:17
- instrument_mcp_integration.py:22
- unified_fundamental_provider.py:160
- cross_vendor_comparator.py:56
- majority_voting_reconciler.py:65
- file_based_minute_market_data_manager.py:31

**Action:** Review for consolidation into utility module.

**def __post_init__(self)** - 29 identical signatures:
- types.py:64
- types.py:104
- multi_timeframe_signal_pipeline.py:76
- portfolio_evaluator.py:68
- training_data_generator.py:78
- enhanced_feature_types.py:115
- multi_scale_minute_manager.py:52
- multi_scale_sequence.py:60
- multi_scale_sequence.py:76
- oauth_models.py:82
- oauth_models.py:99
- training_dataset_dao.py:56
- data_loader.py:51
- temporal_fusion_transformer.py:65
- enhanced_tft.py:86
- indicator_config.py:14
- indicator.py:16
- factor_framework.py:37
- signal_generation.py:62
- multimodal_dataset_generator.py:81
- tft_training_pipeline.py:90
- app.py:41
- adaptive_sr_model.py:50
- adaptive_backtester.py:58
- support_resistance_model.py:49
- firstrate_minute_adapter.py:77
- enhanced_minute_backfill_orchestrator.py:58
- unified_backfill_orchestrator.py:79
- base_realtime_collector.py:48

**Action:** Review for consolidation into utility module.

**def __init__(self, api_key** - 29 identical signatures:
- populate_market_cap_polygon.py:24
- populate_market_cap_tiingo.py:24
- tiingo_client.py:20
- alpha_vantage_client.py:20
- fred_client.py:20
- polygon_client.py:20
- historical_30year_backfill.py:142
- historical_30year_backfill.py:220
- fast_daily_price_backfill.py:26
- fast_daily_price_backfill.py:90
- turbo_price_backfill.py:25
- turbo_price_backfill.py:110
- eodhd_news_adapter.py:38
- turbo_news_backfill.py:26
- turbo_news_backfill.py:125
- fmp_minute_adapter.py:45
- polygon_fundamentals_adapter.py:48
- tiingo_intraday_adapter.py:47
- fmp_fundamentals_adapter.py:16
- llm_assistant.py:16
- polygon_minute_adapter.py:47
- tiingo_fundamentals_adapter.py:16
- eodhd_minute_adapter.py:45
- eodhd_fundamentals_adapter.py:16
- tiingo_adapter.py:12
- polygon_adapter.py:12
- analyst_data_ingest.py:80
- analyst_data_ingest.py:188
- analyst_data_ingest.py:251

**Action:** Review for consolidation into utility module.

**def __init__(** - 28 identical signatures:
- event_analysis.py:220
- multi_scale_minute_manager.py:65
- multi_scale_sequence.py:125
- file_based_minute_manager.py:113
- data_loader.py:144
- temporal_fusion_transformer.py:123
- temporal_fusion_transformer.py:200
- enhanced_tft.py:97
- enhanced_tft.py:205
- event_integration.py:144
- event_integration.py:215
- event_integration.py:477
- data_validation_reporter.py:81
- custom_exceptions.py:14
- data_validators.py:68
- data_validators.py:155
- data_agent_orchestrator.py:26
- monitoring.py:231
- firstrate_daily_downloader.py:46
- resilience.py:41
- resilience.py:135
- health_api.py:29
- enhanced_minute_backfill_orchestrator.py:160
- unified_backfill_orchestrator.py:101
- cross_scale_attention.py:54
- cross_scale_attention.py:139
- cross_scale_attention.py:189
- cross_scale_attention.py:324

**Action:** Review for consolidation into utility module.


## Quick Win Scripts

```bash
# Find all __init__.py files that might be empty
find src/ -name "__init__.py" -size 0

# Find files with very similar names
find src/ -name "*.py" | sort | uniq -d

# Count import statement frequencies  
grep -h "^import\|^from" src/**/*.py | sort | uniq -c | sort -nr | head -20
```

## Priority Order

1. **High Impact, Low Risk**: Remove unused imports
2. **Medium Impact, Low Risk**: Consolidate identical utility functions
3. **High Impact, Medium Risk**: Merge duplicate files after review
4. **Low Impact, High Value**: Create common imports module
