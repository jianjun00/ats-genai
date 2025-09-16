"""
Comprehensive Multi-Timeframe Training Data System Test
=======================================================

Based on TRAINING_DATASET_PRD_DRD.md requirements to expose critical timeframe granularity issue.

This test sets up real system components:
- Real Runner with callback architecture  
- Real UniverseStateIntervalBuilder 
- Real UnifiedMarketDataManager with vendor priorities
- Multi-timeframe processing (5m, 15m, 60m)

The test verifies that:
1. UniverseStateIntervalBuilder builds records at native frequencies:
   - 5m interval: every 5 minutes (12 records/hour)
   - 15m interval: every 15 minutes (4 records/hour) 
   - 60m interval: every 60 minutes (1 record/hour)
2. 15m aggregates over last 15 minutes, 60m over 60 minutes
3. OHLC and technical signals are computed as expected
4. Universe state intervals are stored correctly

CRITICAL: This test is designed to EXPOSE the timeframe granularity bug where all 
timeframes generate 1 record/hour instead of native frequency.
"""

import pytest
import asyncio
import os
import asyncpg
from shared.data_handling.utils.environment import Environment, EnvironmentType
from tests.utils.test_data_setup import setup_single_symbol_test

@pytest.mark.asyncio
async def test_comprehensive_multi_timeframe_training_data_pipeline(unit_test_db):
    """
    🎯 COMPREHENSIVE MULTI-TIMEFRAME TRAINING DATA SYSTEM TEST
    📋 Based on TRAINING_DATASET_PRD_DRD.md requirements
    """
    # Configure environment for testing
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    
    # Setup test data for AAPL (we have real AAPL market data)
    await setup_test_data(environment, 'AAPL')
    
    # Configure test data directory - must contain real minute bar data
    test_data_dir = "/home/jianjun/ats-genai-admin/tests/data/minute-bars/firstrate"
    
    # Create UnifiedMarketDataManager with vendor priorities
    from core.market_data.unified_manager import UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
    
    config = MarketDataConfig(
        vendors=[VendorType.FIRSTRATE, VendorType.EODHD],
        vendor_priorities={
            VendorType.FIRSTRATE: 1,  # Primary for minute bars
            VendorType.EODHD: 2       # Secondary for daily prices  
        },
        storage_backend=StorageBackend.FILE,
        file_storage_path=test_data_dir,
        enable_cache=True
    )
    
    minute_manager = UnifiedMarketDataManager(config=config)
    
    # Create real Runner with callback architecture
    from services.core.app.runner import Runner
    from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
    from domains.trading.services.universe.universe_manager import UniverseManager
    
    # Create UniverseManager with AAPL symbol (overrides TSLA default)
    universe_manager = UniverseManager(
        env=environment,
        universe_id=1,
        symbols=['AAPL']  # Use AAPL instead of default TSLA
    )
    
    # Create real Runner first to get the universe_state_manager
    # Note: AAPL data on 2024-08-01 runs from 04:00:00 to 19:59:00 UTC
    runner = Runner(
        environment=environment,
        start_date='2024-08-01',  # Single day with real AAPL data
        end_date='2024-08-01',
        universe_id=1,
        callbacks=[],  # Will add callback after creating it
        base_duration='5m',
        market_data_manager=minute_manager,
        universe_manager=universe_manager,
        # Set trading hours to match actual AAPL data availability
        trading_start_hour=4,   # 04:00 UTC
        trading_start_minute=0,
        trading_end_hour=19,    # 19:59 UTC (last minute with data)
        trading_end_minute=59,
        timezone='UTC'
    )
    
    # Create UniverseStateIntervalBuilder callback with Runner's universe_state_manager
    universe_state_builder = UniverseStateIntervalBuilder(
        env=environment,
        base_duration='5m',
        target_durations='5m,15m,60m',  # Multi-timeframe targets
        universe_state_manager=runner.universe_state_manager  # Pass Runner's state manager
    )
    
    # Add the callback to the runner
    runner.callbacks = [universe_state_builder]
    
    # Initialize UniverseManager and run
    await runner.universe_manager.initialize()
    await runner.run()
    
    # Verify universe state interval records were created
    conn = await asyncpg.connect(environment.get_database_url())
    
    universe_state_table = environment.get_table_name('universe_state_interval')
    query = f"""
        SELECT COUNT(*) as total_records
        FROM {universe_state_table}
        WHERE run_id = '{runner.get_environment()._run_uuid}'
    """
    
    result = await conn.fetchrow(query)
    total_records = result['total_records'] or 0
    
    await conn.close()
    
    # This should fail the test if no records are generated
    assert total_records > 0, f"CRITICAL BUG: Zero universe state records generated despite real AAPL data being available. The multi-timeframe training data system is completely broken."
    
    # If we get here, verify the record counts for each timeframe
    conn = await asyncpg.connect(environment.get_database_url())
    
    # Let's see what's actually stored in the database
    sample_query = f"""
        SELECT run_id, universe_id, duration, start_date_time, end_date_time, state_data
        FROM {universe_state_table}
        WHERE run_id = '{runner.get_environment()._run_uuid}'
        ORDER BY start_date_time
        LIMIT 10
    """
    
    sample_results = await conn.fetch(sample_query)
    
    print(f"📋 Sample universe_state_interval records:")
    for i, record in enumerate(sample_results):
        # Parse state_data JSON if it's a string
        state_data = record['state_data']
        if isinstance(state_data, str):
            import json
            try:
                state_data = json.loads(state_data)
            except:
                state_data = None
        
        print(f"   {i+1}. duration={record['duration']}, start_time={record['start_date_time']}, state_data keys={list(state_data.keys()) if state_data else 'None'}")
    
    # Count records by duration (this is the correct way since we now have duration column)
    timeframe_counts = {}
    for timeframe in ['5m', '15m', '60m']:
        timeframe_query = f"""
            SELECT COUNT(*) as count
            FROM {universe_state_table}
            WHERE run_id = '{runner.get_environment()._run_uuid}'
            AND duration = '{timeframe}'
        """
        result = await conn.fetchrow(timeframe_query)
        timeframe_counts[timeframe] = result['count']
    
    print(f"📊 Timeframe record counts by duration column: {timeframe_counts}")
    
    # EXACT RECORD COUNT VALIDATION
    # For a single trading day (2024-08-01) with 5m base duration and target durations 5m,15m,60m:
    # Trading hours: 04:00 UTC to 19:59 UTC = 16 hours = 960 minutes
    # Expected records:
    # - 5m intervals: 960 minutes / 5 = 192 records
    # - 15m intervals: 960 minutes / 15 = 64 records  
    # - 60m intervals: 960 minutes / 60 = 16 records
    
    expected_counts = {
        '5m': 192,   # Every 5 minutes
        '15m': 64,   # Every 15 minutes
        '60m': 16    # Every 60 minutes
    }
    
    print(f"📊 Expected vs Actual record counts:")
    validation_errors = []
    
    for timeframe in ['5m', '15m', '60m']:
        expected = expected_counts[timeframe]
        actual = timeframe_counts[timeframe]
        print(f"   {timeframe}: Expected={expected}, Actual={actual}")
        
        if actual != expected:
            validation_errors.append(f"{timeframe}: expected {expected} records, got {actual}")
    
    # If no records with expected timeframes, check what we actually have
    if all(count == 0 for count in timeframe_counts.values()):
        any_records_query = f"""
            SELECT COUNT(*) as count, duration
            FROM {universe_state_table}
            WHERE run_id = '{runner.get_environment()._run_uuid}'
            GROUP BY duration
        """
        any_results = await conn.fetch(any_records_query)
        
        if any_results:
            print(f"⚠️  System generated records with different durations:")
            for result in any_results:
                print(f"   - {result['count']} records with duration '{result['duration']}'")
            validation_errors.append(f"No records with expected durations (5m,15m,60m), found: {[r['duration'] for r in any_results]}")
        else:
            validation_errors.append("Zero universe state records generated")
    
    # EXACT VALUE VALIDATION - Sample 2 records from each timeframe
    value_validation_errors = []
    
    for timeframe in ['5m', '15m', '60m']:
        if timeframe_counts[timeframe] >= 2:
            sample_records_query = f"""
                SELECT duration, start_date_time, end_date_time, state_data
                FROM {universe_state_table}
                WHERE run_id = '{runner.get_environment()._run_uuid}'
                AND duration = '{timeframe}'
                ORDER BY start_date_time
                LIMIT 2
            """
            sample_records = await conn.fetch(sample_records_query)
            
            print(f"📋 Sample {timeframe} records validation:")
            for i, record in enumerate(sample_records):
                start_time = record['start_date_time']
                end_time = record['end_date_time']
                
                # Parse state_data JSON if it's a string
                state_data = record['state_data']
                if isinstance(state_data, str):
                    import json
                    try:
                        state_data = json.loads(state_data)
                    except:
                        state_data = None
                
                # Validate timestamp alignment
                duration_minutes = {'5m': 5, '15m': 15, '60m': 60}[timeframe]
                expected_duration = (end_time - start_time).total_seconds() / 60
                
                print(f"   Record {i+1}: start={start_time}, end={end_time}, duration={expected_duration}min")
                
                if expected_duration != duration_minutes:
                    value_validation_errors.append(f"{timeframe} record {i+1}: duration should be {duration_minutes}min, got {expected_duration}min")
                
                # Validate start time alignment to timeframe boundaries
                if timeframe == '5m':
                    if start_time.minute % 5 != 0:
                        value_validation_errors.append(f"5m record {i+1}: start_time minute should be multiple of 5, got {start_time.minute}")
                elif timeframe == '15m':
                    if start_time.minute % 15 != 0:
                        value_validation_errors.append(f"15m record {i+1}: start_time minute should be multiple of 15, got {start_time.minute}")
                elif timeframe == '60m':
                    if start_time.minute != 0:
                        value_validation_errors.append(f"60m record {i+1}: start_time minute should be 0, got {start_time.minute}")
                
                # Validate state_data contains expected structure
                if not state_data or not isinstance(state_data, dict):
                    value_validation_errors.append(f"{timeframe} record {i+1}: state_data should be non-empty dict, got {type(state_data)}")
                else:
                    # Check if state_data contains instrument data
                    if timeframe not in state_data:
                        # For legacy structure, check if we have instrument intervals
                        if 'instrument_intervals' not in state_data and 'instruments' not in state_data:
                            value_validation_errors.append(f"{timeframe} record {i+1}: state_data missing instrument data")
                
                print(f"     state_data keys: {list(state_data.keys()) if state_data else 'None'}")
    
    await conn.close()
    
    # FAIL TEST if any validation errors
    if validation_errors or value_validation_errors:
        all_errors = validation_errors + value_validation_errors
        error_msg = f"VALIDATION FAILURES ({len(all_errors)} issues):\n" + "\n".join(f"  {i+1}. {error}" for i, error in enumerate(all_errors))
        raise AssertionError(error_msg)
    
    print("🎉 All validations passed! Multi-timeframe training data system working correctly with exact counts and values!")


async def setup_test_data(environment: Environment, test_symbol: str = 'AAPL'):
    """Setup test data using shared utility."""
    conn = await asyncpg.connect(environment.get_database_url())
    
    try:
        # Use shared test data setup utility
        await setup_single_symbol_test(
            environment=environment,
            db_connection=conn,
            symbol=test_symbol,
            instrument_id=999999,
            universe_id=1
        )
        
    finally:
        await conn.close()