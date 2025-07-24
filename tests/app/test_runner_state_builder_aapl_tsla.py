import pytest
import pandas as pd
from datetime import date
from app.runner import Runner
from universe.universe_manager import UniverseManager
from state.universe_state_manager import UniverseStateManager
from config.environment import Environment, EnvironmentType

# Test config: use test DB prefix and test env
TEST_START_DATE = "2025-07-01"
TEST_END_DATE = "2025-07-03"
UNIVERSE_SYMBOLS = ["AAPL", "TSLA"]
UNIVERSE_ID = 9998  # Arbitrary test universe ID for unit test

@pytest.mark.asyncio
def test_runner_state_builder_aapl_tsla(monkeypatch):
    """
    Unit test: create a universe with AAPL and TSLA, run runner from 2025-07-01 to 2025-07-03,
    and verify the built universe state is as expected (test env, not intg env).
    """
    env = Environment(EnvironmentType.TEST)
    # Patch callbacks config to inject UniverseStateBuilder as callback
    monkeypatch.setattr(env, 'get', lambda section, key, default=None: ['state.universe_state_builder.UniverseStateBuilder'] if (section, key) == ('runner', 'callbacks') else env.__class__.get(env, section, key, default))
    runner = Runner(TEST_START_DATE, TEST_END_DATE, env, UNIVERSE_ID)
    # Patch in a dummy market_data_manager for UniverseStateBuilder compatibility
    class DummyMarketDataManager:
        def get_ohlc_batch(self, instrument_ids, start_time, end_time):
            # Return dummy OHLCV data as expected by UniverseStateBuilder
            import pandas as pd
            # Build a DataFrame with required columns for each instrument
            data = []
            for symbol in instrument_ids:
                data.append({
                    'symbol': symbol,
                    'open': 100.0,
                    'high': 110.0,
                    'low': 90.0,
                    'close': 105.0,
                    'traded_volume': 1000,
                    'traded_dollar': 100000,
                    'as_of_date': start_time.strftime('%Y-%m-%d')
                })
            return pd.DataFrame(data)
    runner.market_data_manager = DummyMarketDataManager()

    # Patch runner with dummy universe_manager for test compatibility
    class DummyUniverseManager:
        instrument_ids = UNIVERSE_SYMBOLS
        universe = type('U', (), {'instrument_ids': UNIVERSE_SYMBOLS})()
    runner.universe_manager = DummyUniverseManager()
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

    # Run the runner (assuming it populates universe_state_manager)
    runner.run()
    print('Runner finished running.')

    # Fetch the built state and verify expectations
    universe_state_manager = runner.get_universe_state_manager()
    try:
        df = universe_state_manager.load_universe_state()
    except FileNotFoundError as e:
        print('FileNotFoundError:', e)
        print('UniverseStateManager state:', vars(universe_state_manager))
        import os
        state_dir = getattr(universe_state_manager, 'state_dir', None)
        if state_dir and os.path.exists(state_dir):
            print('State directory contents:', os.listdir(state_dir))
        else:
            print('State directory not found or not set.')
        raise

    print('Universe state DataFrame columns:', df.columns.tolist())
    print('Sample universe state DataFrame:', df.head())
    # Assert that for each test date, both AAPL and TSLA are present
    for test_date in pd.date_range(TEST_START_DATE, TEST_END_DATE):
        date_str = test_date.strftime('%Y-%m-%d')
        day_df = df[df['as_of_date'] == date_str]
        assert set(day_df['symbol']) == set(UNIVERSE_SYMBOLS)
        # Check OHLCV fields
        for symbol in UNIVERSE_SYMBOLS:
            row = day_df[day_df['symbol'] == symbol]
            assert not row.empty, f"Missing row for {symbol} on {date_str}"
            for col in ['open', 'high', 'low', 'close', 'traded_volume', 'traded_dollar']:
                assert col in row.columns, f"Missing {col} column"
                assert pd.notnull(row.iloc[0][col]), f"Null {col} for {symbol} on {date_str}"
        # Print all columns for manual inspection (including indicators if present)
        print(f"{date_str} rows:\n", day_df)

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
