import os
import tempfile
from datetime import datetime, timedelta, date
import pytest
import logging
logging.basicConfig(level=logging.DEBUG)
import pandas as pd
from pathlib import Path
from config.environment import Environment, EnvironmentType
from app.runner import Runner
from state.universe_state_builder import UniverseStateBuilder
from state.universe_state_manager import UniverseStateManager
from market_data.eod.file_daily_price_market_data_manager import FileDailyPriceMarketDataManager


@pytest.mark.asyncio
async def test_runner_with_file_daily_price_market_data_manager_30days(tmp_path, unit_test_db):
    # Setup environment
    polygon_dir = os.path.join(os.path.dirname(__file__), '../data/daily_prices_polygon')
    polygon_dir = os.path.abspath(polygon_dir)
    tiingo_dir = os.path.join(os.path.dirname(__file__), '../data/daily_prices_tiingo')
    tiingo_dir = os.path.abspath(tiingo_dir)
    vendors_dirs = {'polygon': polygon_dir, 'tiingo': tiingo_dir}
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    env.get_table_name = lambda table: f"test_{table}"
    from signals.indicator_config import IndicatorConfig
    env.get_indicator_config = lambda: IndicatorConfig(indicators={})

    # Use FileDailyPriceMarketDataManager
    market_data_manager = FileDailyPriceMarketDataManager(vendors_dirs)
    instrument_ids = list(market_data_manager._id_to_symbol.keys())
    for iid in instrument_ids:
        assert isinstance(iid, int), f"instrument_id must be int, got {type(iid)}: {iid}"
    # Use a 30-day window
    start_date = '2024-12-02'
    end_date = '2024-12-31'

    output_dir = os.path.join(tmp_path, 'universe_state_30days')
    universe_state_manager = UniverseStateManager(env=env, base_path=output_dir)

    import asyncpg
    import asyncio
    async def insert_test_data():
        conn = await asyncpg.connect(unit_test_db)
        await conn.execute(f"""
            INSERT INTO {env.get_table_name('instruments')} (id, symbol, name, type, list_date)
            VALUES (1, 'AAPL', 'Apple Inc.', 'stock', '2020-01-01')
            ON CONFLICT (id) DO NOTHING
        """)
        await conn.execute(f"""
            INSERT INTO {env.get_table_name('universe_membership')} (universe_id, instrument_id, symbol, start_at, end_at)
            VALUES (1, 1, 'AAPL', '2024-01-01', '2025-12-31')
            ON CONFLICT DO NOTHING
        """)
        await conn.close()
    await insert_test_data()

    from signals.indicator_config import IndicatorConfig
    from signals.indicator import ETop, EBot, PL
    indicator_config = IndicatorConfig(indicators={
        'ETop': ETop,
        'EBot': EBot,
        'PL': PL
    })
    env.get_indicator_config = lambda: indicator_config
    builder = UniverseStateBuilder(
        env=env,
        base_duration='1d',
        target_durations='1d'
    )
    builder.universe_state_manager = universe_state_manager
    from src.app.runner import Runner
    runner = Runner(
        start_date=start_date,
        end_date=end_date,
        environment=env,
        universe_id=1,
        callbacks=[builder],
        base_duration='1d'
    )
    runner.market_data_manager = market_data_manager
    runner.universe_manager.instrument_ids = instrument_ids
    runner.universe_state_manager = universe_state_manager
    await runner.run()

    states_dir = os.path.join(output_dir, 'states')
    state_files = []
    for root, dirs, files in os.walk(states_dir):
        for file in files:
            if file.startswith('universe_state_') and file.endswith('.parquet'):
                state_files.append(os.path.join(root, file))
    assert state_files, 'No universe state files created.'
    # Read all state files and concatenate
    dfs = [pd.read_parquet(f) for f in sorted(state_files)]
    df = pd.concat(dfs, ignore_index=True)
    assert not df.empty
    # Check that the date range matches the 30-day window
    df_dates = set(pd.to_datetime(df['start_date_time']).dt.date)
    expected_dates = set(pd.date_range(start=start_date, end=end_date).date)
    missing_dates = expected_dates - df_dates
    if missing_dates:
        print(f"[ERROR] Missing dates in output: {missing_dates}")
    else:
        print(f"[OK] All 30 days present in output.")
    assert not missing_dates, f"Missing dates in 30-day output: {missing_dates}"
    # Indicator checks
    required_indicators = ['ETop', 'EBot', 'PL']
    for ind in required_indicators:
        ind_rows = df[df['indicator_name'] == ind] if 'indicator_name' in df.columns else pd.DataFrame()
        if not ind_rows.empty:
            non_null_count = ind_rows['indicator_value'].notnull().sum()
            print(f"[DEBUG] Indicator '{ind}' exists with {non_null_count} non-null values.")
            assert non_null_count > 0, f"Indicator '{ind}' exists but has no non-null values!"
        else:
            print(f"[ERROR] Indicator '{ind}' is missing from universe state output!")
            assert False, f"Indicator '{ind}' missing in output parquet!"
    # Print OHLC + indicator values for each day
    ohlc_cols = ['start_date_time', 'instrument_id', 'open', 'high', 'low', 'close']
    base_df = df[ohlc_cols].drop_duplicates()
    for idx, row in base_df.iterrows():
        date = row['start_date_time']
        instrument_id = row['instrument_id']
        open_ = row['open']
        high = row['high']
        low = row['low']
        close = row['close']
        indicator_vals = {}
        for ind in required_indicators:
            val = df[(df['start_date_time'] == date) & (df['instrument_id'] == instrument_id) & (df['indicator_name'] == ind)]['indicator_value']
            indicator_vals[ind] = val.iloc[0] if not val.empty else None
        print(f"date: {date}, instrument_id: {instrument_id}, open: {open_}, high: {high}, low: {low}, close: {close}, etop: {indicator_vals['ETop']}, ebot: {indicator_vals['EBot']}, pldot: {indicator_vals['PL']}")
    for col in ['instrument_id', 'open', 'close', 'traded_volume', 'traded_dollar']:
        assert col in df.columns
    min_date = df['start_date_time'].min()
    max_date = df['end_date_time'].max()
    assert pd.to_datetime(min_date) >= pd.Timestamp(start_date)
    # Allow max_date to be up to end_date + 1 day (inclusive window)
    allowed_max = pd.Timestamp(end_date) + pd.Timedelta(days=1)
    assert pd.to_datetime(max_date) <= allowed_max, f"max_date {max_date} exceeds allowed {allowed_max} for end_date {end_date}"

@pytest.mark.asyncio
async def test_runner_with_file_daily_price_market_data_manager(tmp_path, unit_test_db):
    # Setup environment
    polygon_dir = os.path.join(os.path.dirname(__file__), '../data/daily_prices_polygon')
    polygon_dir = os.path.abspath(polygon_dir)
    tiingo_dir = os.path.join(os.path.dirname(__file__), '../data/daily_prices_tiingo')
    tiingo_dir = os.path.abspath(tiingo_dir)
    vendors_dirs = {'polygon': polygon_dir, 'tiingo': tiingo_dir}
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    env.get_table_name = lambda table: f"test_{table}"
    from signals.indicator_config import IndicatorConfig
    env.get_indicator_config = lambda: IndicatorConfig(indicators={})

    # Patch UniverseManager and UniverseDB to avoid real DB access


    # Use FileDailyPriceMarketDataManager
    market_data_manager = FileDailyPriceMarketDataManager(vendors_dirs)
    # Get instrument IDs from the file manager
    instrument_ids = list(market_data_manager._id_to_symbol.keys())
    # Assert all instrument_ids are ints
    for iid in instrument_ids:
        assert isinstance(iid, int), f"instrument_id must be int, got {type(iid)}: {iid}"
    start_date = '2024-01-01'
    end_date = '2025-07-31'

    # Set output directory for universe state files
    output_dir = os.path.join(tmp_path, 'universe_state')
    # Create UniverseStateManager with test output dir
    universe_state_manager = UniverseStateManager(env=env, base_path=output_dir)

    import asyncpg
    import asyncio
    # Insert a test instrument and membership into the test DB
    async def insert_test_data():
        conn = await asyncpg.connect(unit_test_db)
        # Insert instrument (id SERIAL PRIMARY KEY, symbol TEXT NOT NULL, name TEXT, type TEXT, list_date DATE, ...)
        await conn.execute(f"""
            INSERT INTO {env.get_table_name('instruments')} (id, symbol, name, type, list_date)
            VALUES (1, 'AAPL', 'Apple Inc.', 'stock', '2020-01-01')
            ON CONFLICT (id) DO NOTHING
        """)
        # Insert universe membership (universe_id INTEGER, instrument_id INTEGER, symbol TEXT, start_at DATE, end_at DATE, ...)
        await conn.execute(f"""
            INSERT INTO {env.get_table_name('universe_membership')} (universe_id, instrument_id, symbol, start_at, end_at)
            VALUES (1, 1, 'AAPL', '2024-01-01', '2025-12-31')
            ON CONFLICT DO NOTHING
        """)
        # Print table contents for verification
        rows = await conn.fetch(f"SELECT * FROM {env.get_table_name('universe_membership')}")
        print(f"[DEBUG] universe_membership rows: {rows}")
        rows = await conn.fetch(f"SELECT * FROM {env.get_table_name('instruments')}")
        print(f"[DEBUG] instruments rows: {rows}")
        await conn.close()
    await insert_test_data()

    # Create and patch a UniverseStateBuilder instance directly
    # Provide a minimal valid indicator_config for UniverseStateBuilder
    from signals.indicator_config import IndicatorConfig
    from signals.indicator import ETop, EBot, PL
    indicator_config = IndicatorConfig(indicators={
        'ETop': ETop,
        'EBot': EBot,
        'PL': PL
    })
    env.get_indicator_config = lambda: indicator_config
    builder = UniverseStateBuilder(
        env=env,
        base_duration='1d',
        target_durations='1d'
    )
    builder.universe_state_manager = universe_state_manager
    # Create runner, passing builder as a callback instance
    from src.app.runner import Runner
    runner = Runner(
        start_date=start_date,
        end_date=end_date,
        environment=env,
        universe_id=1,
        callbacks=[builder],
        base_duration='1d'
    )
    # Patch the runner's market_data_manager to use the file-based manager before any test logic
    runner.market_data_manager = market_data_manager
    print(f"[DEBUG][test] runner.market_data_manager patched to: {type(runner.market_data_manager)}, id: {id(runner.market_data_manager)}")
    runner.universe_manager.instrument_ids = instrument_ids
    runner.universe_state_manager = universe_state_manager  # Ensure correct output_dir is used
    await runner.run()


    # Check universe state output in the correct subdirectory
    states_dir = os.path.join(output_dir, 'states')
    state_files = []
    for root, dirs, files in os.walk(states_dir):
        for file in files:
            if file.startswith('universe_state_') and file.endswith('.parquet'):
                state_files.append(os.path.join(root, file))
    assert state_files, 'No universe state files created.'
    # Optionally, load one and check expected content
    df = pd.read_parquet(state_files[-1])
    assert not df.empty
    # Check that all dates in the DataFrame exactly match the runner's current date (end_date)
    df_dates = set(pd.to_datetime(df['start_date_time']).dt.date)
    run_date = pd.to_datetime(end_date).date()
    if df_dates != {run_date}:
        print(f"[ERROR] Dates in output: {df_dates}, expected only: {run_date}")
    else:
        print(f"[OK] All universe state rows are for current run date: {run_date}")
    # --- Indicator existence and debug (normalized/long-form) ---
    required_indicators = ['ETop', 'EBot', 'PL']
    for ind in required_indicators:
        ind_rows = df[df['indicator_name'] == ind] if 'indicator_name' in df.columns else pd.DataFrame()
        if not ind_rows.empty:
            non_null_count = ind_rows['indicator_value'].notnull().sum()
            print(f"[DEBUG] Indicator '{ind}' exists with {non_null_count} non-null values.")
            assert non_null_count > 0, f"Indicator '{ind}' exists but has no non-null values!"
        else:
            print(f"[ERROR] Indicator '{ind}' is missing from universe state output!")
            assert False, f"Indicator '{ind}' missing in output parquet!"

    # Print daily OHLC and indicator values for each instrument/date
    # Group by instrument/date, then print OHLC + indicators
    ohlc_cols = ['start_date_time', 'instrument_id', 'open', 'high', 'low', 'close']
    base_df = df[ohlc_cols].drop_duplicates()
    for idx, row in base_df.iterrows():
        date = row['start_date_time']
        instrument_id = row['instrument_id']
        open_ = row['open']
        high = row['high']
        low = row['low']
        close = row['close']
        # Extract indicator values for this instrument/date
        indicator_vals = {}
        for ind in required_indicators:
            val = df[(df['start_date_time'] == date) & (df['instrument_id'] == instrument_id) & (df['indicator_name'] == ind)]['indicator_value']
            indicator_vals[ind] = val.iloc[0] if not val.empty else None
        print(f"date: {date}, instrument_id: {instrument_id}, open: {open_}, high: {high}, low: {low}, close: {close}, etop: {indicator_vals['ETop']}, ebot: {indicator_vals['EBot']}, pldot: {indicator_vals['PL']}")
    # Example: check expected columns
    for col in ['instrument_id', 'open', 'close', 'traded_volume', 'traded_dollar']:
        assert col in df.columns
    # Example: check date range
    min_date = df['start_date_time'].min()
    max_date = df['end_date_time'].max()
    assert pd.to_datetime(min_date) >= pd.Timestamp(start_date)
    assert pd.to_datetime(max_date) <= pd.Timestamp(end_date)
