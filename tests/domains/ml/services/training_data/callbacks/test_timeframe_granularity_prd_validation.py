#!/usr/bin/env python3
"""
CRITICAL: PRD/DRD Timeframe Granularity Validation Test

Validates the CRITICAL TIMEFRAME GRANULARITY ISSUE identified in TRAINING_DATASET_PRD_DRD.md:
- 5m: 12 records/hour (every 5 minutes during market hours)
- 15m: 4 records/hour (every 15 minutes during market hours) 
- 1h: 1 record/hour (baseline correct behavior)
- 1d: 1 record/day (not per hour)
- 1w: 1 record/week (NEW REQUIREMENT - weekly timeframe missing)

NO EXCEPTION CATCHING - Test must fail fast if granularity is wrong.
"""

import pytest
import asyncpg
from datetime import datetime, timedelta
from shared.data_handling.utils.environment import Environment, EnvironmentType
from tests.utils.test_data_setup import setup_single_symbol_test
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig


@pytest.mark.asyncio
async def test_critical_timeframe_granularity_prd_requirement(unit_test_db):
    """
    CRITICAL: Test PRD/DRD timeframe granularity requirement.
    
    This test MUST FAIL if timeframes generate wrong number of records.
    """
    
    # Setup real environment and test data
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    conn = await asyncpg.connect(unit_test_db)
    await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
    await conn.close()
    
    # Configure callback with ALL required timeframes from PRD/DRD
    config = TrainingDataConfig(
        feature_types=['ohlcv', 'technical'],
        signal_names=['rsi', 'macd', 'sma', 'ema']
    )
    
    callback = IntervalBasedTrainingDataCallback(
        symbols=['TSLA'],
        config=config,
        environment=environment
    )
    
    # Test period: 4 hours of market time (9:30 AM - 1:30 PM)
    start_time = datetime(2025, 7, 1, 9, 30, 0)
    end_time = datetime(2025, 7, 1, 13, 30, 0)
    
    # Generate training data for 4-hour period
    current_time = start_time
    while current_time <= end_time:
        # Process each 5-minute interval
        await callback.handleInterval(None, current_time)
        current_time += timedelta(minutes=5)
    
    # Validate EXACT record counts per PRD/DRD requirements
    conn = await asyncpg.connect(unit_test_db)
    
    # Query universe state intervals by timeframe
    universe_state_table = environment.get_table_name('universe_state_interval')
    
    expected_counts = {
        '5m': 48,   # 4 hours × 12 records/hour = 48 records
        '15m': 16,  # 4 hours × 4 records/hour = 16 records  
        '1h': 4,    # 4 hours × 1 record/hour = 4 records
        '1d': 1,    # 1 record for entire day
        '1w': 1     # 1 record for entire week
    }
    
    for timeframe, expected_count in expected_counts.items():
        query = f"""
            SELECT COUNT(*) as actual_count
            FROM {universe_state_table}
            WHERE symbol = 'TSLA'
            AND interval_duration = '{timeframe}'
            AND interval_start >= '{start_time}'
            AND interval_start <= '{end_time}'
        """
        
        result = await conn.fetchrow(query)
        actual_count = result['actual_count']
        
        # FAIL FAST - No exception catching
        assert actual_count == expected_count, (
            f"PRD/DRD VIOLATION: {timeframe} timeframe should generate {expected_count} "
            f"records in 4 hours, but generated {actual_count}. "
            f"This is the CRITICAL TIMEFRAME GRANULARITY ISSUE identified in PRD/DRD."
        )
    
    await conn.close()


@pytest.mark.asyncio 
async def test_arrayrecord_files_match_prd_structure(unit_test_db, tmp_path):
    """
    Test that ArrayRecord files follow exact PRD/DRD structure.
    
    Required structure: {dataset_id}/{SYMBOL}_{YYYY}_{MM}/{timeframe}/{SYMBOL}_{YYYY}_{MM}.arrayrecord
    """
    
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    conn = await asyncpg.connect(unit_test_db)
    await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
    await conn.close()
    
    config = TrainingDataConfig(
        feature_types=['ohlcv', 'technical'],
        signal_names=['rsi', 'macd', 'sma', 'ema']
    )
    
    callback = IntervalBasedTrainingDataCallback(
        symbols=['TSLA'],
        config=config,
        storage_format='arrayrecord',
        output_dir=str(tmp_path),
        environment=environment
    )
    callback.dataset_id = 'dataset_20250701_120000'
    
    # Generate training data
    test_time = datetime(2025, 7, 1, 12, 0, 0)
    example = {
        'symbol': 'TSLA',
        'prediction_timestamp': test_time,
        'timeframe_features': {
            '5m': {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000, 'vwap': 250.5},
            '15m': {'open': 248.0, 'high': 253.0, 'low': 247.0, 'close': 251.0, 'volume': 3000000, 'vwap': 249.8},
            '1h': {'open': 245.0, 'high': 255.0, 'low': 244.0, 'close': 251.0, 'volume': 12000000, 'vwap': 249.2},
            '1d': {'open': 240.0, 'high': 260.0, 'low': 238.0, 'close': 251.0, 'volume': 25000000, 'vwap': 248.5}
        }
    }
    
    await callback._save_simple_arrayrecord([example], test_time)
    
    # Validate EXACT PRD/DRD directory structure
    dataset_id = 'dataset_20250701_120000'
    symbol_date = 'TSLA_2025_07'  # PRD/DRD format: {SYMBOL}_{YYYY}_{MM}
    
    for timeframe in ['5m', '15m', '1h', '1d']:
        # PRD/DRD structure: {dataset_id}/{SYMBOL}_{YYYY}_{MM}/{timeframe}/{SYMBOL}_{YYYY}_{MM}.arrayrecord
        expected_file = tmp_path / dataset_id / symbol_date / timeframe / f"{symbol_date}.arrayrecord"
        
        assert expected_file.exists(), (
            f"PRD/DRD VIOLATION: ArrayRecord file structure incorrect. "
            f"Expected: {expected_file}, "
            f"Required structure: {{dataset_id}}/{{SYMBOL}}_{{YYYY}}_{{MM}}/{{timeframe}}/{{SYMBOL}}_{{YYYY}}_{{MM}}.arrayrecord"
        )
        
        # Validate file contains actual data (not 0-record files)
        assert expected_file.stat().st_size > 0, (
            f"PRD/DRD VIOLATION: ArrayRecord file {expected_file} is empty. "
            f"Context manager cleanup may have failed."
        )


@pytest.mark.asyncio
async def test_technical_indicators_count_prd_requirement(unit_test_db, tmp_path):
    """
    Test that each record contains exactly 16 technical indicators per PRD/DRD.
    """
    
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    conn = await asyncpg.connect(unit_test_db)
    await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
    await conn.close()
    
    config = TrainingDataConfig(
        feature_types=['ohlcv', 'technical'],
        signal_names=['rsi', 'macd', 'sma', 'ema']
    )
    
    callback = IntervalBasedTrainingDataCallback(
        symbols=['TSLA'],
        config=config,
        storage_format='arrayrecord',
        output_dir=str(tmp_path),
        environment=environment
    )
    callback.dataset_id = 'test_indicators'
    
    # Generate training data with technical indicators
    test_time = datetime(2025, 7, 1, 12, 0, 0)
    example = {
        'symbol': 'TSLA',
        'prediction_timestamp': test_time,
        'timeframe_features': {
            '5m': {
                'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000, 'vwap': 250.5,
                # Add all 16 required technical indicators
                'sma_20': 249.5, 'ema_12': 250.2, 'rsi_14': 55.3, 'macd': 0.8, 'bb_upper': 252.1,
                'bb_lower': 247.9, 'atr': 2.3, 'adx': 45.2, 'stoch_k': 67.4, 'stoch_d': 65.1,
                'williams_r': -23.4, 'trix': 0.12, 'cci': 23.7, 'mfi': 58.9, 'obv': 1250000, 'vwap_ratio': 1.002
            }
        }
    }
    
    await callback._save_simple_arrayrecord([example], test_time)
    
    # Read ArrayRecord file and validate technical indicators count
    file_path = tmp_path / 'test_indicators' / 'TSLA_2025_07' / '5m' / 'TSLA_2025_07.arrayrecord'
    assert file_path.exists(), "ArrayRecord file should exist"
    
    import array_record.python.array_record_module as ar
    import json
    
    reader = ar.ArrayRecordReader(str(file_path))
    record = reader.read()
    data = json.loads(record.decode())
    reader.close()
    
    # Count technical indicators (exclude OHLCV base fields)
    base_fields = {'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'vwap'}
    indicator_fields = set(data.keys()) - base_fields
    
    # PRD/DRD requirement: exactly 16 technical indicators per record
    assert len(indicator_fields) == 16, (
        f"PRD/DRD VIOLATION: Each record must contain exactly 16 technical indicators. "
        f"Found {len(indicator_fields)} indicators: {sorted(indicator_fields)}"
    )


@pytest.mark.asyncio
async def test_weekly_timeframe_missing_prd_requirement(unit_test_db):
    """
    Test that validates PRD/DRD identifies missing 1w timeframe.
    
    This test should FAIL until 1w timeframe is implemented.
    """
    
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    
    config = TrainingDataConfig(
        feature_types=['ohlcv'],
        signal_names=['rsi', 'macd']
    )
    
    callback = IntervalBasedTrainingDataCallback(
        symbols=['TSLA'],
        config=config,
        environment=environment
    )
    
    # This should fail if 1w timeframe is not implemented
    test_time = datetime(2025, 7, 1, 12, 0, 0)
    
    # Generate weekly data - this will fail if 1w timeframe not supported
    await callback.handleInterval(None, test_time)
    
    # If we get here, 1w timeframe is implemented
    # Query for weekly records
    conn = await asyncpg.connect(unit_test_db)
    universe_state_table = environment.get_table_name('universe_state_interval')
    
    query = f"""
        SELECT COUNT(*) as weekly_count
        FROM {universe_state_table}
        WHERE interval_duration = '1w'
    """
    
    result = await conn.fetchrow(query)
    weekly_count = result['weekly_count']
    
    await conn.close()
    
    assert weekly_count > 0, (
        "PRD/DRD VIOLATION: Weekly (1w) timeframe is missing. "
        "PRD/DRD requires 1w timeframe with 1 record/week generation."
    )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])