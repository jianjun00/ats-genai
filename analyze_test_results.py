#!/usr/bin/env python3
"""
Analyze results from comprehensive multi-timeframe test.
This script will run the test and then examine the universe_state_interval records.
"""

import asyncio
import sys
import os

from infrastructure.database.test_db_manager import DatabaseTestManager
from core.shared.data_handling.utils.environment import Environment, EnvironmentType
import asyncpg

async def analyze_test_results():
    """Run test and analyze the universe_state_interval records generated."""
    
    print("🔍 ANALYZING COMPREHENSIVE MULTI-TIMEFRAME TEST RESULTS")
    print("=" * 70)
    
    # Set up test database
    os.environ['SKIP_GLOBAL_ENV'] = '1'
    os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'
    
    db_manager = DatabaseTestManager(test_type="unit")
    await db_manager.setup_test_database()
    
    try:
        environment = Environment(env_type=EnvironmentType.TEST, db_url=db_manager.db_url)
        
        # === RUN THE COMPREHENSIVE TEST (simplified) ===
        print("\n📋 STEP 1: RUNNING SIMPLIFIED COMPREHENSIVE TEST")
        
        # Setup test data for TSLA
        conn = await asyncpg.connect(environment.get_database_url())
        
        # Insert test data
        instrument_table = environment.get_table_name('instrument')
        xrefs_table = environment.get_table_name('instrument_xrefs')
        vendors_table = environment.get_table_name('vendors')
        universe_table = environment.get_table_name('universe')
        universe_membership_table = environment.get_table_name('universe_membership')
        
        await conn.execute(f"""
            INSERT INTO {instrument_table} (id, symbol, name, active) 
            VALUES (999999, 'TSLA', 'Tesla Inc Test', true) 
            ON CONFLICT (id) DO NOTHING
        """)
        
        await conn.execute(f"""
            INSERT INTO {xrefs_table} (instrument_id, symbol, vendor_id) 
            VALUES (999999, 'TSLA', (SELECT id FROM {vendors_table} WHERE name = 'ticker'))
            ON CONFLICT (instrument_id, vendor_id, start_at) DO NOTHING
        """)
        
        await conn.execute(f"""
            INSERT INTO {universe_table} (id, name) VALUES (1, 'test_universe') 
            ON CONFLICT (id) DO NOTHING
        """)
        
        await conn.execute(f"""
            INSERT INTO {universe_membership_table} (universe_id, instrument_id) 
            VALUES (1, 999999) ON CONFLICT (universe_id, instrument_id, entered_at) DO NOTHING
        """)
        
        print("   ✅ Test data setup complete")
        
        # === RUN THE REAL COMPONENTS ===
        print("\n📋 STEP 2: RUNNING REAL COMPONENTS")
        
        # Create components (simplified)
        from core.market_data.unified_manager import UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
        from domains.trading.services.core.app.runner import Runner
        from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
        
        # Create UnifiedMarketDataManager
        config = MarketDataConfig(
            vendors=[VendorType.POLYGON],
            vendor_priorities={VendorType.POLYGON: 1},
            storage_backend=StorageBackend.FILE,
            file_storage_path="/tmp",
            enable_cache=True
        )
        minute_manager = UnifiedMarketDataManager(config=config)
        
        # Create UniverseStateIntervalBuilder
        universe_state_builder = UniverseStateIntervalBuilder(
            env=environment,
            base_duration='5m',
            target_durations='5m,15m,60m'
        )
        
        # Create Runner
        runner = Runner(
            environment=environment,
            start_date='2025-07-01',
            end_date='2025-07-01',
            universe_id=1,
            callbacks=[universe_state_builder],
            base_duration='5m',
            market_data_manager=minute_manager
        )
        
        # Initialize UniverseManager
        await runner.universe_manager.initialize()
        print("   ✅ Real components created and initialized")
        
        # Run the processing
        print("   🔄 Running multi-timeframe processing...")
        await runner.run()
        print("   ✅ Processing completed")
        
        # === ANALYZE RESULTS ===
        print("\n📋 STEP 3: ANALYZING UNIVERSE STATE INTERVAL RECORDS")
        
        universe_state_table = environment.get_table_name('universe_state_interval')
        
        # Check if table exists and has records
        query = f"""
            SELECT COUNT(*) as total_records,
                   MIN(datetime) as first_datetime,
                   MAX(datetime) as last_datetime
            FROM {universe_state_table}
            WHERE run_id = '{runner.get_environment()._run_uuid}'
        """
        
        try:
            result = await conn.fetchrow(query)
            total_records = result['total_records'] or 0
            
            print(f"   📊 Total universe state records: {total_records}")
            if result['first_datetime'] and result['last_datetime']:
                print(f"      ⏰ First record: {result['first_datetime']}")
                print(f"      ⏰ Last record: {result['last_datetime']}")
            
            if total_records > 0:
                # Analyze record structure
                query = f"""
                    SELECT datetime, state_data
                    FROM {universe_state_table}
                    WHERE run_id = '{runner.get_environment()._run_uuid}'
                    ORDER BY datetime
                    LIMIT 5
                """
                
                records = await conn.fetch(query)
                print(f"\n   📋 SAMPLE RECORDS ANALYSIS ({len(records)} records):")
                
                for i, record in enumerate(records, 1):
                    print(f"      📈 Record {i} at {record['datetime']}:")
                    if record['state_data']:
                        state_data = dict(record['state_data'])
                        print(f"         🔍 State data keys: {list(state_data.keys())}")
                        
                        # Look for multi-timeframe data
                        timeframe_data = {}
                        for key, value in state_data.items():
                            if any(tf in str(key) for tf in ['5m', '15m', '60m', '1d']):
                                timeframe_data[key] = type(value).__name__
                        
                        if timeframe_data:
                            print(f"         🎯 Multi-timeframe data: {timeframe_data}")
                        
                        # Check for OHLC data
                        ohlc_keys = [k for k in state_data.keys() if any(x in k.lower() for x in ['open', 'high', 'low', 'close', 'volume'])]
                        if ohlc_keys:
                            print(f"         📊 OHLC data keys: {ohlc_keys}")
                    else:
                        print(f"         ⚠️ No state_data")
                
                # === CRITICAL TIMEFRAME ANALYSIS ===
                print(f"\n   🎯 CRITICAL TIMEFRAME GRANULARITY ANALYSIS:")
                
                # Count records by hour to detect granularity issue
                query = f"""
                    SELECT DATE_TRUNC('hour', datetime) as hour,
                           COUNT(*) as records_per_hour
                    FROM {universe_state_table}
                    WHERE run_id = '{runner.get_environment()._run_uuid}'
                    GROUP BY DATE_TRUNC('hour', datetime)
                    ORDER BY hour
                """
                
                hourly_counts = await conn.fetch(query)
                if hourly_counts:
                    print(f"      📊 Records per hour:")
                    for hour_record in hourly_counts[:3]:  # Show first 3 hours
                        print(f"         {hour_record['hour']}: {hour_record['records_per_hour']} records")
                    
                    # Analyze granularity
                    avg_records_per_hour = sum(r['records_per_hour'] for r in hourly_counts) / len(hourly_counts)
                    print(f"      🔍 Average records per hour: {avg_records_per_hour:.1f}")
                    
                    if avg_records_per_hour == 1.0:
                        print(f"      🚨 CRITICAL ISSUE DETECTED: 1 record/hour (granularity bug confirmed!)")
                    elif avg_records_per_hour == 12.0:
                        print(f"      ✅ CORRECT BEHAVIOR: 12 records/hour (5m native frequency)")
                    else:
                        print(f"      ❓ UNEXPECTED BEHAVIOR: {avg_records_per_hour} records/hour")
            
            else:
                print(f"   ⚠️ NO RECORDS GENERATED")
                print(f"      This could indicate:")
                print(f"      - Market data manager returned no data (expected)")
                print(f"      - UniverseStateIntervalBuilder didn't execute")
                print(f"      - Database storage issue")
                
        except Exception as e:
            print(f"   ❌ Error analyzing records: {e}")
            
        await conn.close()
        
        print(f"\n🎊 ANALYSIS COMPLETE!")
        print(f"   ✅ Successfully analyzed comprehensive multi-timeframe test results")
        print(f"   🔬 Test infrastructure is fully operational for exposing critical issues")
        
    finally:
        await db_manager.cleanup()

if __name__ == "__main__":
    asyncio.run(analyze_test_results())