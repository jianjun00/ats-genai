import os
print(f"[IMPORT_DEBUG][TEST] PYTHONPATH={{os.environ.get('PYTHONPATH')}}")
print(f"[IMPORT_DEBUG][TEST] sys.path={{sys.path}}")
from datetime import datetime
import pytest
import logging
logging.basicConfig(level=logging.DEBUG)
import pandas as pd
from pathlib import Path
from core.platform.config.environment import Environment, EnvironmentType
from services.core.app.runner import Runner
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.market_data.services.vendor_adapters.eod.file_daily_price_market_data_manager import FileDailyPriceMarketDataManager


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_runner_with_file_daily_price_market_data_manager_30days(tmp_path, unit_test_db):
    # Setup environment
    polygon_dir = os.path.join(os.path.dirname(__file__), '../data/daily_prices_polygon')
    polygon_dir = os.path.abspath(polygon_dir)
    tiingo_dir = os.path.join(os.path.dirname(__file__), '../data/daily_prices_tiingo')
    tiingo_dir = os.path.abspath(tiingo_dir)
    vendors_dirs = {'polygon': polygon_dir, 'tiingo': tiingo_dir}

    # Ensure all required test tables exist
    from tests.app.ensure_test_tables import ensure_test_tables
    print(f"\n[DEBUG] Test DB URL: {unit_test_db}")
    env = await ensure_test_tables(unit_test_db)

    from domains.trading.services.indicator_config import IndicatorConfig
    env.get_indicator_config = lambda: IndicatorConfig(indicators={})

    # Insert test data
    import asyncpg
    async def insert_test_data():
        conn = await asyncpg.connect(unit_test_db)

        # Debug: List all tables in the database
        tables = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        print("\n[DEBUG] Tables in database:", [t['table_name'] for t in tables])

        # Debug: Check if instrument_xrefs table exists and its structure
        try:
            xrefs_columns = await conn.fetch("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'test_instrument_xrefs'
            """)
            print("\n[DEBUG] test_instrument_xrefs columns:", [(c['column_name'], c['data_type']) for c in xrefs_columns])
        except Exception as e:
            print(f"\n[DEBUG] Error checking test_instrument_xrefs: {e}")

            # Try to check without the test_ prefix
            try:
                xrefs_columns = await conn.fetch("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = 'instrument_xrefs'
                """)
                print("\n[DEBUG] instrument_xrefs columns (no prefix):", [(c['column_name'], c['data_type']) for c in xrefs_columns])
            except Exception as e2:
                print(f"\n[DEBUG] Error checking instrument_xrefs (no prefix): {e2}")

        # Clear existing test data
        await conn.execute(f"TRUNCATE TABLE {env.get_table_name('instruments')} CASCADE")
        await conn.execute(f"TRUNCATE TABLE {env.get_table_name('universe_membership')} CASCADE")
        await conn.execute(f"TRUNCATE TABLE {env.get_table_name('vendors')} CASCADE")
        await conn.execute(f"TRUNCATE TABLE {env.get_table_name('instrument_xrefs')} CASCADE")

        # Check if the instrument_xrefs table already has a primary key
        table_name = env.get_table_name('instrument_xrefs')
        base_table_name = env.get_table_name('instrument_xrefs', with_prefix=False)

        # First check if the table has any primary key
        has_pk = await conn.fetchval(f"""
            SELECT COUNT(*) FROM information_schema.table_constraints
            WHERE table_name = '{table_name}'
            AND constraint_type = 'PRIMARY KEY'
        """)

        if has_pk:
            print(f"[DEBUG] Table {table_name} already has a primary key, skipping primary key creation")
        else:
            print(f"[DEBUG] Adding primary key to {table_name}")
            # Only add primary key if it doesn't exist
            await conn.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (instrument_id, vendor_id)")


        # Insert instruments
        await conn.execute(f"""
            INSERT INTO {env.get_table_name('instruments')} (id, symbol, name, type, list_date)
            VALUES (1, 'TSLA', 'Tesla Inc.', 'stock', '2020-01-01'),
                   (2, 'AAPL', 'Apple Inc.', 'stock', '2020-01-01')
        """)

        # Insert universe membership
        await conn.execute(f"""
            INSERT INTO {env.get_table_name('universe_membership')} (universe_id, instrument_id, symbol, start_at, end_at)
            VALUES (1, 1, 'TSLA', '2024-01-01', '2025-12-31'),
                   (1, 2, 'AAPL', '2024-01-01', '2025-12-31')
        """)

        # Insert vendor (using 'ticker' as the vendor name)
        await conn.execute(f"""
            INSERT INTO {env.get_table_name('vendors')} (id, name)
            VALUES (1, 'ticker')
            ON CONFLICT (id) DO UPDATE SET name = 'ticker'
        """)

        # Insert instrument xrefs with correct mapping and table prefix
        table_name = env.get_table_name('instrument_xrefs')
        base_table_name = env.get_table_name('instrument_xrefs', with_prefix=False)

        # First delete any existing records
        await conn.execute(f"DELETE FROM {table_name} WHERE instrument_id IN (1, 2) AND vendor_id = 1")

        # Then insert new records
        await conn.execute(f"""
            INSERT INTO {table_name} (instrument_id, vendor_id, symbol, start_at)
            VALUES (1, 1, 'TSLA', '2020-01-01'),
                   (2, 1, 'AAPL', '2020-01-01')
        """)

        await conn.close()

    await insert_test_data()

    from domains.trading.services.indicator_config import IndicatorConfig
    from domains.trading.services.indicator import ETop, EBot, PL
    indicator_config = IndicatorConfig(indicators={
        'ETop': ETop,
        'EBot': EBot,
        'PL': PL
    })
    env.get_indicator_config = lambda: indicator_config

    # Rest of the test remains the same as the original file
    from app.runner_utils import run_file_daily_price_ohlcv

    print('\n' + '='*80)
    print('[DEBUG][TEST] Before create_async call')
    print(f'[DEBUG][TEST] Environment: {env.__dict__}')
    print(f'[DEBUG][TEST] Vendors dirs: {vendors_dirs}')

    # Check if vendor directories exist and are accessible
    for vendor, dir_path in vendors_dirs.items():
        print(f'\n[DEBUG][TEST] Checking {vendor} directory: {dir_path}')
        print(f'[DEBUG][TEST]   Exists: {os.path.exists(dir_path)}')
        print(f'[DEBUG][TEST]   Is directory: {os.path.isdir(dir_path)}')
        if os.path.exists(dir_path):
            files = os.listdir(dir_path)
            print(f'[DEBUG][TEST]   Contents ({len(files)} files): {files}')

            # Check a sample file
            sample_file = next((f for f in files if f.endswith('.json')), None)
            if sample_file:
                sample_path = os.path.join(dir_path, sample_file)
                print(f'[DEBUG][TEST]   Sample file: {sample_file}')
                print(f'[DEBUG][TEST]   Sample file exists: {os.path.exists(sample_path)}')
                print(f'[DEBUG][TEST]   Sample file size: {os.path.getsize(sample_path) if os.path.exists(sample_path) else 0} bytes')

                # Try to read and parse the sample file
                try:
                    with open(sample_path, 'r') as f:
                        content = f.read()
                        print(f'[DEBUG][TEST]   Sample file content (first 200 chars): {content[:200]}...')
                        try:
                            import json
                            json_content = json.loads(content)
                            print(f'[DEBUG][TEST]   Successfully parsed JSON, type: {type(json_content)}')
                            if isinstance(json_content, dict):
                                print(f'[DEBUG][TEST]   JSON keys: {list(json_content.keys())}')
                        except json.JSONDecodeError as e:
                            print(f'[DEBUG][TEST]   Failed to parse JSON: {e}')
                except Exception as e:
                    print(f'[DEBUG][TEST]   Error reading sample file: {e}')

    print('\n' + '='*80)
    print('[DEBUG][TEST] Creating FileDailyPriceMarketDataManager instance...')
    market_data_manager = await FileDailyPriceMarketDataManager.create_async(vendors_dirs, env)

    print('\n' + '='*80)
    print('[DEBUG][TEST] After create_async call')
    print(f'[DEBUG][TEST] Market data manager: {market_data_manager}')
    print(f'[DEBUG][TEST] Market data manager symbols: {market_data_manager._id_to_symbol if hasattr(market_data_manager, "_id_to_symbol") else "N/A"}')

    # Check if vendor_data was loaded
    if hasattr(market_data_manager, 'vendor_data'):
        print(f'[DEBUG][TEST] Vendor data loaded for: {list(market_data_manager.vendor_data.keys())}')
        for vendor, data in market_data_manager.vendor_data.items():
            print(f'[DEBUG][TEST]   {vendor}: {len(data)} symbols loaded')
            for symbol, prices in data.items():
                print(f'[DEBUG][TEST]     {symbol}: {len(prices)} price points')
                if prices:
                    dates = sorted(prices.keys())
                    print(f'[DEBUG][TEST]       Date range: {dates[0]} to {dates[-1]}')
                    print(f'[DEBUG][TEST]       Sample data: {next(iter(prices.items()))}')
                    break

    # Debug: Check what's in the universe_membership table
    conn = await asyncpg.connect(unit_test_db)
    try:
        universe_members = await conn.fetch(f"""
            SELECT * FROM {env.get_table_name('universe_membership')}
            WHERE universe_id = 1
        """)
        print(f'[DEBUG][TEST] Universe membership: {universe_members}')

        # Check instrument_xrefs
        xrefs_table = env.get_table_name('instrument_xrefs')
        xrefs = await conn.fetch(f"""
            SELECT * FROM {xrefs_table}
        """)
        print(f'[DEBUG][TEST] Instrument xrefs: {xrefs}')

        # Check instruments
        instruments = await conn.fetch(f"""
            SELECT * FROM {env.get_table_name('instruments')}
        """)
        print(f'[DEBUG][TEST] Instruments: {instruments}')
    finally:
        await conn.close()

    instrument_ids = list(market_data_manager._id_to_symbol.keys())
    output_dir = os.path.join(tmp_path, 'universe_state_30days')

    # Update to use a date range that's covered by the test data
    start_date = datetime.strptime('2024-01-02', '%Y-%m-%d').date()
    end_date = datetime.strptime('2024-01-31', '%Y-%m-%d').date()

    # Add debug logging for instrument IDs and symbols
    print(f"[DEBUG] Running test with instrument_ids: {instrument_ids}")
    print(f"[DEBUG] Symbol to ID mapping: {market_data_manager._symbol_to_id}")
    print(f"[DEBUG] ID to symbol mapping: {market_data_manager._id_to_symbol}")

    # Add debug logging for vendor data
    for vendor, data in market_data_manager.vendor_data.items():
        print(f"[DEBUG] Vendor: {vendor}")
        for symbol, dates in data.items():
            print(f"[DEBUG]   {symbol}: {len(dates)} dates from {min(dates.keys())} to {max(dates.keys())}")
            # Print first and last few date entries for each symbol
            sorted_dates = sorted(dates.items())
            print(f"[DEBUG]     First 3 entries: {sorted_dates[:3]}")
            print(f"[DEBUG]     Last 3 entries: {sorted_dates[-3:]}")

    # Print instrument IDs and symbols
    print(f"[DEBUG] Instrument IDs: {instrument_ids}")
    print(f"[DEBUG] Symbol to ID mapping: {market_data_manager._symbol_to_id}")
    print(f"[DEBUG] ID to symbol mapping: {market_data_manager._id_to_symbol}")

    df = await run_file_daily_price_ohlcv(
        vendors_dirs=vendors_dirs,
        instrument_ids=instrument_ids,
        start_date=start_date,
        end_date=end_date,
        env=env,
        universe_id=1,
        output_dir=output_dir,
        indicator_config=indicator_config,
        print_ohlcv=True  # Enable OHLCV printing for debugging
    )

    # Verify the output
    assert not df.empty, "Output DataFrame is empty"

    # Check if indicators are in the DataFrame in long format
    if 'indicator_name' in df.columns and 'indicator_value' in df.columns:
        # Check if ETop exists as an indicator_name
        etop_values = df[df['indicator_name'] == 'ETop']
        assert not etop_values.empty, "ETop indicator not found in output"
        assert etop_values['indicator_value'].notna().any(), "No non-null ETop values found"
    else:
        # Fall back to checking for ETop as a direct column
        assert 'ETop' in df.columns, "ETop column not found in output"
        assert df['ETop'].notna().any(), "No non-null ETop values found"
