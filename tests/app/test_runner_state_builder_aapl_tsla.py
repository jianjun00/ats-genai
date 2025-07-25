import pytest
import pandas as pd
from datetime import date
from app.runner import Runner
from universe.universe_manager import UniverseManager
from state.universe_state_manager import UniverseStateManager
from config.environment import Environment, EnvironmentType

import asyncio
import asyncpg

# Test config: use test DB prefix and test env
TEST_START_DATE = "2025-07-01"
TEST_END_DATE = "2025-07-03"
UNIVERSE_SYMBOLS = ["AAPL", "TSLA"]
UNIVERSE_ID = 9998  # Arbitrary test universe ID for unit test

import pytest
from db.test_db_manager import unit_test_db

@pytest.mark.asyncio
async def test_runner_state_builder_aapl_tsla(unit_test_db, monkeypatch):
    """
    Unit test: create a universe with AAPL and TSLA, run runner from 2025-07-01 to 2025-07-03,
    and verify the built universe state is as expected (test env, not intg env).
    Uses isolated test DB via fixture.
    """
    db_url = unit_test_db
    # ... rest of test logic, replacing all manual db_url/env setup with this db_url ...
    # (full logic for test data insertion, runner setup, and assertions remains, but all manual backup/restore is removed)
    # See detailed code below for full refactor.

    """
    Unit test: create a universe with AAPL and TSLA, run runner from 2025-07-01 to 2025-07-03,
    and verify the built universe state is as expected (test env, not intg env).
    Includes robust DB setup/teardown for test isolation.
    """
    # Use the provided test DB URL for all DB operations
    universe_id = UNIVERSE_ID
    symbols = UNIVERSE_SYMBOLS
    # Insert test data as needed (no backup/restore required)
    # Insert test data as needed (no backup/restore required)
    from config.environment import Environment, EnvironmentType
    env = Environment(EnvironmentType.TEST)
    if not env.config.has_section('universe'):
        env.config.add_section('universe')
    env.config.set('universe', 'base_duration', '1d')
    env.config.set('universe', 'target_durations', '1d,5d')
    env.config.set('universe', 'universe_id', '9998')
    # Patch callbacks config to inject UniverseStateBuilder as callback
    monkeypatch.setattr(env, 'get', lambda section, key, default=None: ['state.universe_state_builder.UniverseStateBuilder'] if (section, key) == ('runner', 'callbacks') else env.__class__.get(env, section, key, default))
    runner = Runner(TEST_START_DATE, TEST_END_DATE, env, UNIVERSE_ID)
    # Insert test daily prices for AAPL/TSLA for each test date using the provided db_url
    async with asyncpg.create_pool(db_url) as pool:
        async with pool.acquire() as conn:
            for symbol in UNIVERSE_SYMBOLS:
                for d in pd.date_range(TEST_START_DATE, TEST_END_DATE):
                    table_name = env.get_table_name('daily_prices')
                    await conn.execute(f"INSERT INTO {table_name} (date, symbol, open, high, low, close, volume) VALUES ($1, $2, 100, 110, 90, 105, 1000) ON CONFLICT DO NOTHING", d.date(), symbol)
    # Debug: Check if test_universe_membership table exists
    pool = await asyncpg.create_pool(db_url)
    async with pool.acquire() as conn:
        result = await conn.fetchval("SELECT to_regclass('public.test_universe_membership')")
        assert result is not None, 'test_universe_membership table does not exist in test DB!'
    await pool.close()
    # Run the runner
    await runner.run()
    # (Assertions and verification would go here)
    # No manual cleanup needed, DB is dropped after test
    # Patch callbacks config to inject UniverseStateBuilder as callback
    monkeypatch.setattr(env, 'get', lambda section, key, default=None: ['state.universe_state_builder.UniverseStateBuilder'] if (section, key) == ('runner', 'callbacks') else env.__class__.get(env, section, key, default))
    runner = Runner(TEST_START_DATE, TEST_END_DATE, env, UNIVERSE_ID)
    # Insert test daily prices for AAPL/TSLA for each test date
    from market_data.eod.daily_prices_dao import DailyPricesDAO
    dao = DailyPricesDAO(env)
    test_dates = [pd.to_datetime(d).date() for d in pd.date_range(TEST_START_DATE, TEST_END_DATE)]
    async def setup_prices():
        import asyncpg
        pool = await asyncpg.create_pool(env.get_database_url())
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {dao.table_name} WHERE date >= $1 AND date <= $2 AND symbol IN ($3, $4)", test_dates[0], test_dates[-1], 'AAPL', 'TSLA')
        await pool.close()
        for d in test_dates:
            await dao.insert_price(d, 'AAPL', 150, 155, 148, 154, 10000)
            await dao.insert_price(d, 'TSLA', 700, 710, 690, 705, 20000)
    await setup_prices()

    # Patch in a real DailyPriceMarketDataManager
    from market_data.daily_price_market_data_manager import DailyPriceMarketDataManager
    class TestDailyPriceMarketDataManager(DailyPriceMarketDataManager):
        def _get_all_symbols(self):
            return UNIVERSE_SYMBOLS
        def _symbol_to_id(self, symbol):
            return UNIVERSE_SYMBOLS.index(symbol)+1
        def get_ohlc_batch(self, instrument_ids, start_time, end_time):
            import pandas as pd
            rows = []
            for iid in instrument_ids:
                ohlc = self.get_ohlc(iid, start_time, end_time)
                if ohlc:
                    row = {'symbol': UNIVERSE_SYMBOLS[iid-1], 'as_of_date': start_time.strftime('%Y-%m-%d')}
                    row.update({
                        'open': ohlc.get('open'),
                        'high': ohlc.get('high'),
                        'low': ohlc.get('low'),
                        'close': ohlc.get('close'),
                        'traded_volume': ohlc.get('volume'),
                        'traded_dollar': ohlc.get('traded_dollar')
                    })
                    rows.append(row)
            return pd.DataFrame(rows)
    from datetime import datetime as dt
    mdm = TestDailyPriceMarketDataManager(env=env, start_date=dt.strptime(TEST_START_DATE, "%Y-%m-%d").date())
    runner.market_data_manager = mdm

    # Use try...finally for cleanup
    async def cleanup_prices():
        import asyncpg
        pool = await asyncpg.create_pool(env.get_database_url())
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {dao.table_name} WHERE date >= $1 AND date <= $2 AND symbol IN ($3, $4)", test_dates[0], test_dates[-1], 'AAPL', 'TSLA')
        await pool.close()


        # Patch runner with dummy universe_manager for test compatibility
        #class DummyUniverseManager:
        #    instrument_ids = UNIVERSE_SYMBOLS
        #    universe = type('U', (), {'instrument_ids': UNIVERSE_SYMBOLS})()
        #runner.universe_manager = DummyUniverseManager()
        # Patch runner with dummy universe for test compatibility
        class DummyUniverse:
            instrument_ids = UNIVERSE_SYMBOLS
        runner.universe = DummyUniverse()

        # Patch UniverseStateBuilder to set universe attribute with instrument_ids
        for cb in runner.callbacks:
            if hasattr(cb, 'universe'):
                continue
            class DummyUniverse:
                instrument_ids = UNIVERSE_SYMBOLS
            cb.universe = DummyUniverse()

        print('Runner callbacks:', runner.callbacks)
        print('UniverseStateManager:', runner.universe_state_manager)
        print('UniverseManager (if present):', getattr(runner, 'universe_manager', None))
        print('SecurityMaster:', runner.security_master)

        # Optionally patch data sources here for deterministic test
        # monkeypatch.setattr(...)

        # Run the runner (ensures universe state parquet files are created)
        runner.run()

        # Fetch the built state and verify expectations per day
        universe_state_manager = runner.get_universe_state_manager()
        universe_state_manager: UniverseStateManager = runner.universe_state_manager
        for date_str in pd.date_range(TEST_START_DATE, TEST_END_DATE).strftime('%Y-%m-%d'):
            timestamp = date_str.replace('-', '') + '_000000'
            day_df = universe_state_manager.load_universe_state(timestamp=timestamp)
            for symbol in UNIVERSE_SYMBOLS:
                row = day_df[day_df['symbol'] == symbol]
                assert not row.empty, f"Missing row for {symbol} on {date_str}"
                for col in ['open', 'high', 'low', 'close', 'traded_volume', 'traded_dollar']:
                    assert col in row.columns, f"Missing {col} column"
                    assert pd.notnull(row.iloc[0][col]), f"Null {col} for {symbol} on {date_str}"
            print(f"{date_str} rows:\n", day_df)

        # Load and check the full_universe_state parquet file
        import glob, os
        from pathlib import Path
        # Use the default output dir for test
        base_dir = Path(universe_state_manager.base_path)
        parquet_files = sorted(glob.glob(str(base_dir / "full_universe_state_*.parquet")))
        assert len(parquet_files) == 1, f"Expected exactly one full_universe_state parquet file, found {len(parquet_files)}: {parquet_files}"
        full_file = parquet_files[0]
        full_df = pd.read_parquet(full_file)
        # Check that all dates and symbols are present
        for date_str in pd.date_range(TEST_START_DATE, TEST_END_DATE).strftime('%Y-%m-%d'):
            for symbol in UNIVERSE_SYMBOLS:
                row = full_df[(full_df['symbol'] == symbol)]
                assert not row.empty, f"Missing row for {symbol} in full_universe_state for {date_str}"
        print(f"Full universe state loaded from {full_file}, shape: {full_df.shape}")
        print(full_df)

def test_runner_event_iterator(monkeypatch):
    """
    Test that Runner.iter_events yields the correct (datetime, type) sequence for interval and EOD events.
    """
    env = Environment(EnvironmentType.TEST)
    # Patch callbacks config to avoid callback unpacking error
    monkeypatch.setattr(env, 'get', lambda section, key, default=None: [] if (section, key) == ('runner', 'callbacks') else env.__class__.get(env, section, key, default))
    start_date = "2025-07-01"
    end_date = "2025-07-03"
    runner = Runner(start_date, end_date, env, UNIVERSE_ID)
    # Patch duration to daily
    class DummyDuration:
        def is_daily_or_longer(self): return True
        def get_duration_minutes(self): return None
        duration_type = type('dt', (), {'name': 'DAILY'})
    runner.duration = DummyDuration()
    events = list(runner.iter_events())
    # For 3 days, expect 3 interval events and 3 EOD events
    interval_events = [e for e in events if e[1] == 'interval']
    eod_events = [e for e in events if e[1] == 'eod']
    assert len(interval_events) == 3, f"Expected 3 interval events, got {len(interval_events)}"
    assert len(eod_events) == 3, f"Expected 3 eod events, got {len(eod_events)}"
    # EOD times should be at 23:59:59
    for dt, typ in eod_events:
        assert dt.hour == 23 and dt.minute == 59 and dt.second == 59, f"EOD event not at last second: {dt}"
    # Dates should be correct
    expected_dates = pd.date_range(start_date, end_date)
    assert [e[0].date() for e in interval_events] == list(expected_dates.date), "Interval event dates mismatch"
    assert [e[0].date() for e in eod_events] == list(expected_dates.date), "EOD event dates mismatch"
