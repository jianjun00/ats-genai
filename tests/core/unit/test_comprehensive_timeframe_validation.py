#!/usr/bin/env python3
"""
Comprehensive Multi-Timeframe Training Data System Test - FIXED VERSION

This test verifies the critical timeframe granularity fix where UniverseStateIntervalBuilder
now requests FULL trading session data (9:30 AM - 4:00 PM EDT) instead of just current minute.
"""

import pytest
import asyncio
import asyncpg
import os
from core.platform.config_env.environment import Environment, EnvironmentType

@pytest.mark.asyncio
async def test_comprehensive_multi_timeframe_training_data_pipeline(unit_test_db):
    """
    🎯 COMPREHENSIVE MULTI-TIMEFRAME TRAINING DATA SYSTEM TEST
    ✅ Tests the FIXED UniverseStateIntervalBuilder with correct trading session logic
    """
    # Configure environment for testing
    environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    
    # Setup test data for AAPL (we have real AAPL market data)
    await setup_test_data(environment, 'AAPL')
    
    # Configure test data directory - must contain real minute bar data
    test_data_dir = "/home/jianjun/ats-genai-admin/tests/data/minute-bars/firstrate"
    
    print(f"📂 Using test data directory: {test_data_dir}")
    
    # Verify AAPL data file exists
    aapl_file = f"{test_data_dir}/A/AAPL/2024/08/AAPL_2024_08.parquet"
    if not os.path.exists(aapl_file):
        pytest.skip(f"AAPL test data not found: {aapl_file}")
    
    print(f"✅ AAPL data file found: {aapl_file}")
    
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
    from domains.trading.services.core.app.runner import Runner
    from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
    from domains.trading.services.universe.universe_manager import UniverseManager
    
    # Create UniverseManager with AAPL symbol (overrides TSLA default)
    universe_manager = UniverseManager(
        env=environment,
        universe_id=1,
        symbols=['AAPL']  # Use AAPL instead of default TSLA
    )
    
    print(f"🔧 UniverseManager configured with symbols: {universe_manager.symbols}")
    
    # Create UniverseStateIntervalBuilder callback
    universe_state_builder = UniverseStateIntervalBuilder(
        env=environment,
        base_duration='5m',
        target_durations='5m,15m,60m'  # Multi-timeframe targets
    )
    
    print(f"🔧 UniverseStateIntervalBuilder configured:")
    print(f"   📊 Base duration: {universe_state_builder.base_duration}")
    print(f"   📊 Target durations: {[d.get_duration_string() for d in universe_state_builder.target_durations]}")
    
    # Create real Runner with all required parameters
    # Note: AAPL data on 2024-08-01 runs from 04:00:00 to 19:59:00 UTC
    runner = Runner(
        environment=environment,
        start_date='2024-08-01',  # Single day with real AAPL data
        end_date='2024-08-01',
        universe_id=1,
        callbacks=[universe_state_builder],
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
    
    print(f"🚀 Runner configured:")
    print(f"   📅 Date range: {runner.start_date} to {runner.end_date}")
    print(f"   🕐 Trading hours: {runner.trading_start_hour:02d}:{runner.trading_start_minute:02d} - {runner.trading_end_hour:02d}:{runner.trading_end_minute:02d} UTC")
    
    # Initialize UniverseManager and run
    print("\n🔄 Initializing and running...")
    await runner.universe_manager.initialize()
    
    # Verify UniverseManager actually has AAPL
    instrument_ids = runner.universe_manager.instrument_ids
    print(f"✅ UniverseManager instrument_ids: {instrument_ids}")
    
    if not instrument_ids:
        raise AssertionError("CRITICAL: UniverseManager has no instrument_ids - AAPL setup failed")
    
    # Run the system
    await runner.run()
    
    print("✅ Runner completed successfully")
    
    # Verify universe state interval records were created
    conn = await asyncpg.connect(environment.get_database_url())
    
    universe_state_table = environment.get_table_name('universe_state_interval')
    
    # Get total record count using correct schema
    query = f"""
        SELECT COUNT(*) as total_records
        FROM {universe_state_table}
        WHERE run_id = '{runner.get_environment()._run_uuid}'
    """
    
    print(f"🔍 Querying: {query}")
    result = await conn.fetchrow(query)
    total_records = result['total_records'] or 0
    
    print(f"📊 Total universe state records: {total_records}")
    
    # This should fail the test if no records are generated
    assert total_records > 0, f"CRITICAL BUG: Zero universe state records generated despite real AAPL data being available. The multi-timeframe training data system is completely broken."
    
    # Get sample records to verify content
    sample_query = f"""
        SELECT run_id, universe_id, datetime, state_data
        FROM {universe_state_table}
        WHERE run_id = '{runner.get_environment()._run_uuid}'
        ORDER BY datetime
        LIMIT 5
    """
    
    print(f"🔍 Sample query: {sample_query}")
    sample_results = await conn.fetch(sample_query)
    
    print(f"📋 Sample records:")
    for i, record in enumerate(sample_results):
        print(f"   {i+1}. datetime={record['datetime']}, state_data keys={list(record['state_data'].keys()) if record['state_data'] else 'None'}")
    
    # Check if we have timeframe-specific data in state_data
    timeframe_counts = {}
    for timeframe in ['5m', '15m', '60m']:
        timeframe_query = f"""
            SELECT COUNT(*) as count
            FROM {universe_state_table}
            WHERE run_id = '{runner.get_environment()._run_uuid}'
            AND state_data ? '{timeframe}'
        """
        result = await conn.fetchrow(timeframe_query)
        timeframe_counts[timeframe] = result['count']
    
    print(f"📊 Timeframe record counts: {timeframe_counts}")
    
    # Check for the granularity bug: if all timeframes have the same count, that's the bug
    if len(set(timeframe_counts.values())) == 1 and list(timeframe_counts.values())[0] > 0:
        raise AssertionError(f"GRANULARITY BUG DETECTED: All timeframes generated the same number of records ({list(timeframe_counts.values())[0]}). This indicates the bug where all timeframes generate 1 record/hour instead of native frequencies.")
    
    # Validate that we have the expected different frequencies
    # 5m should have the most records, 60m should have the least
    if timeframe_counts['5m'] <= timeframe_counts['60m']:
        raise AssertionError(f"INVALID TIMEFRAME FREQUENCIES: 5m ({timeframe_counts['5m']}) should have more records than 60m ({timeframe_counts['60m']})")
    
    print("🎉 Multi-timeframe training data system is working correctly!")
    
async def setup_test_data(environment: Environment, test_symbol: str = 'AAPL'):
    """Setup minimal test data for AAPL including universe membership."""
    conn = await asyncpg.connect(environment.get_database_url())
    
    # Insert test instrument
    instrument_table = environment.get_table_name('instrument')
    await conn.execute(f"""
        INSERT INTO {instrument_table} (id, symbol, name, active) 
        VALUES (999999, '{test_symbol}', '{test_symbol} Inc Test', true) 
        ON CONFLICT (id) DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name
    """)
    
    # Insert instrument xref with correct vendor
    xrefs_table = environment.get_table_name('instrument_xrefs')
    vendors_table = environment.get_table_name('vendors')
    await conn.execute(f"""
        INSERT INTO {xrefs_table} (instrument_id, symbol, vendor_id) 
        VALUES (999999, '{test_symbol}', (SELECT id FROM {vendors_table} WHERE name = 'ticker'))
        ON CONFLICT (instrument_id, vendor_id, start_at) DO UPDATE SET symbol = EXCLUDED.symbol
    """)
    
    # Clear and insert clean universe data
    universe_table = environment.get_table_name('universe')
    universe_membership_table = environment.get_table_name('universe_membership')
    
    await conn.execute(f"DELETE FROM {universe_membership_table} WHERE universe_id = 1")
    await conn.execute(f"DELETE FROM {universe_table} WHERE id = 1")
    
    await conn.execute(f"""
        INSERT INTO {universe_table} (id, name) VALUES (1, 'test_universe')
    """)
    
    await conn.execute(f"""
        INSERT INTO {universe_membership_table} (universe_id, instrument_id) 
        VALUES (1, 999999)
    """)
    
    print(f"✅ Test data setup completed for {test_symbol}")
    
if __name__ == "__main__":
    # For standalone testing
    pytest.main([__file__, "-v", "--tb=short"])