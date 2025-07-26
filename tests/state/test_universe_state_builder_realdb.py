import pytest
import pandas as pd
import asyncio
import asyncpg
from datetime import datetime, date

from src.state.universe_state_builder import UniverseStateBuilder
from src.state.universe_state_manager import UniverseStateManager
from config.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db

@pytest.mark.asyncio
async def test_universe_state_builder_real_db(unit_test_db, tmp_path):
    """
    Real database test: UniverseStateBuilder end-to-end with test DB.
    - Sets up required tables and test data in a real (prefixed) test database
    - Runs UniverseStateBuilder logic for a single day
    - Verifies output universe state is persisted and correct
    """
    # Setup environment for test DB
    env = Environment(EnvironmentType.TEST)
    env.get_database_url = lambda: unit_test_db
    base_path = tmp_path / "universe_state"
    state_manager = UniverseStateManager(env=env, base_path=base_path)
    builder = UniverseStateBuilder(env=env)

    # Insert required daily_prices data for test symbols
    test_date = date(2025, 7, 25)
    symbols = ["AAPL", "TSLA"]
    table_name = env.get_table_name("daily_prices")
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        # Clean up
        await conn.execute(f"DELETE FROM {table_name}")
        # Insert test data
        for symbol in symbols:
            await conn.execute(
                f"INSERT INTO {table_name} (date, symbol, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                test_date, symbol, 100, 110, 90, 105, 1000
            )
    await pool.close()

    # Patch environment to use only our test symbols
    env.get_all_symbols = lambda: symbols
    env.get_target_durations = lambda: [env.get_base_duration()]

    # Mock runner object with required attributes
    class DummyRunner:
        def __init__(self, env, state_manager):
            self.env = env
            self.universe_manager = type('UM', (), {"instrument_ids": symbols})()
            self.market_data_manager = DummyMarketDataManager(symbols)
            self.universe_state_manager = state_manager
    class DummyMarketDataManager:
        def __init__(self, symbols):
            self.symbols = symbols
        def get_ohlc_batch(self, instrument_ids, start_time, end_time):
            # Return same OHLC for all
            return {s: {"open": 100, "high": 110, "low": 90, "close": 105, "volume": 1000} for s in instrument_ids}
    runner = DummyRunner(env, state_manager)

    # Run builder logic for a single day
    builder.handleInterval(runner, datetime.combine(test_date, datetime.min.time()))

    # Check that universe state file is created
    state_files = list((base_path / "states").glob("universe_state_*.parquet"))
    assert len(state_files) > 0, "No universe state file created"
    df = pd.read_parquet(state_files[0])
    # Should have one row per symbol
    assert set(df["instrument_id"]) == set(symbols)
    assert all(df["open"] == 100)
    assert all(df["close"] == 105)
    assert all(df["traded_volume"] == 1000)
    assert all(df["traded_dollar"] == 105000)
