import pytest
import pandas as pd
import asyncpg
from datetime import datetime, date

from state.universe_state_builder import UniverseStateIntervalBuilder
from state.universe_state_manager import UniverseStateManager
from core.shared.utils.environment import Environment, EnvironmentType

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_state_builder_real_db(unit_test_db, tmp_path):
    """
    Real database test: UniverseStateIntervalBuilder end-to-end with test DB.
    - Sets up required tables and test data in a real (prefixed) test database
    - Runs UniverseStateIntervalBuilder logic for a single day
    - Verifies output universe state is persisted and correct
    """
    # Setup environment for test DB
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    env.get_database_url = lambda: unit_test_db
    # Ensure indicator config includes all required indicators
    from domains.trading.services.indicator_config import IndicatorConfig
    env.get_indicator_config = lambda: IndicatorConfig.default_config()
    base_path = tmp_path / "universe_state"
    state_manager = UniverseStateManager(env=env, base_path=base_path)
    builder = UniverseStateIntervalBuilder(env=env, target_durations='5m,15m,60m', base_duration='5m', universe_state_manager=state_manager)

    symbols = ["AAPL", "TSLA"]
    # Insert test instruments and get their ids
    symbol_to_id = {}
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        for symbol in symbols:
            await conn.execute(f"INSERT INTO {env.get_table_name('instruments')} (symbol, name) VALUES ($1, $2) ON CONFLICT (symbol) DO NOTHING", symbol, symbol)
            row = await conn.fetchrow(f"SELECT id FROM {env.get_table_name('instruments')} WHERE symbol = $1", symbol)
            assert row, f"Instrument ID not found for symbol {symbol}"
            symbol_to_id[symbol] = row['id']

    # Insert required daily_price_polygon data for test symbols
    test_date = date(2025, 7, 25)
    table_name = env.get_table_name("daily_price_polygon")
    async with pool.acquire() as conn:
        # Clean up
        await conn.execute(f"DELETE FROM {table_name}")
        # Insert test data
        for symbol in symbols:
            instrument_id = symbol_to_id[symbol]
            await conn.execute(
                f"INSERT INTO {table_name} (date, symbol, instrument_id, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                test_date, symbol, instrument_id, 100, 110, 90, 105, 1000
            )
    await pool.close()

    # Patch environment to use only our test symbols
    env.get_all_symbols = lambda: symbols
    env.get_target_durations = lambda: [env.get_base_duration()]

    # Mock runner object with required attributes
    class DummyRunner:
        def __init__(self, env, state_manager):
            self.env = env
            # Use integer instrument IDs for DB-backed persistence
            self.universe_manager = type('UM', (), {"instrument_ids": list(symbol_to_id.values())})()
            self.market_data_manager = DummyMarketDataManager(symbols)
            self.universe_state_manager = state_manager
            # Provide universe_id used by builder when constructing UniverseStateInterval
            self.universe_id = 1
    class DummyMarketDataManager:
        def __init__(self, symbols):
            self.symbols = symbols
        async def get_ohlc_batch(self, instrument_ids, start_time, end_time):
            # Return same OHLC for all
            return {s: {"open": 100, "high": 110, "low": 90, "close": 105, "volume": 1000} for s in instrument_ids}
    runner = DummyRunner(env, state_manager)

    # Run builder logic for a single day
    await builder.handleInterval(runner, datetime.combine(test_date, datetime.min.time()))

    # Check that universe state file is created
    state_files = list((base_path / "states").glob("universe_state_*.parquet"))
    assert len(state_files) > 0, "No universe state file created"
    df = pd.read_parquet(state_files[0])
    # Instrument rows: no indicator_name
    instrument_rows = df[df["indicator_name"].isna() if "indicator_name" in df.columns else [True]*len(df)]
    assert set(instrument_rows["instrument_id"]) == set(symbol_to_id.values())
    assert all(instrument_rows["open"] == 100)
    assert all(instrument_rows["close"] == 105)
    vol_col = "traded_volume" if "traded_volume" in instrument_rows.columns else ("volume" if "volume" in instrument_rows.columns else None)
    assert vol_col is not None, f"Expected volume column missing; available columns: {list(instrument_rows.columns)}"
    assert all(instrument_rows[vol_col] == 1000)
    if "traded_dollar" in instrument_rows.columns:
        assert all(instrument_rows["traded_dollar"] == 105000)
    else:
        # Fallback: verify close * volume equals expected
        assert all((instrument_rows["close"] * instrument_rows[vol_col]) == 105000)

    # Check indicator columns exist and are computed for each symbol
    indicator_names = ["OneOneDot", "ETop", "EBot", "OneOneHigh", "OneOneLow"]
    for symbol in symbols:
        for ind in indicator_names:
            rows = df[(df["indicator_name"] == ind) & (df["symbol"] == symbol)] if "symbol" in df.columns else df[(df["indicator_name"] == ind)]
            assert not rows.empty, f"No row found for indicator {ind} and symbol {symbol}"
            for _, row in rows.iterrows():
                if row.get("indicator_status") == "ok":
                    assert pd.notnull(row["indicator_value"]), f"Indicator {ind} for symbol {symbol} has null value when status is ok"
