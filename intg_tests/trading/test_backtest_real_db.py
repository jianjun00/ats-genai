import pytest
import asyncio
from datetime import date, timedelta
import numpy as np
from trading.backtest import run_backtest

from config.environment import get_environment, set_environment, EnvironmentType
set_environment(EnvironmentType.INTEGRATION)
env = get_environment()
TSDB_URL = env.get_database_url()

@pytest.mark.asyncio
async def test_backtest_real_db_aapl_tsla(backup_and_restore_tables):
    """
    Integration test: run backtest for AAPL and TSLA over real trading_db for 2025-01-01 to 2025-02-01.
    Requires daily_prices and related tables to be populated for AAPL and TSLA for this date range.
    """
    class Args:
        start_date = "2025-01-01"
        end_date = "2025-02-01"
        data_start_days = 30
        env = env
        # Optionally, you could add a universe filter if supported
    # Ensure TEST_UNIVERSE exists in DB
    from universe.universe_db import UniverseDB
    universe_db = UniverseDB(env)
    universe_id = await universe_db.get_universe_id('TEST_UNIVERSE')
    if universe_id is None:
        await universe_db.add_universe('TEST_UNIVERSE', 'Test universe for backtest integration test')
    # Patch TradingUniverse to only include AAPL and TSLA
    from trading.trading_universe import TradingUniverse
    orig_update_for_end_of_day = TradingUniverse.update_for_end_of_day
    async def patched_update_for_end_of_day(self, as_of_date):
        await orig_update_for_end_of_day(self, as_of_date)
        self.current_universe = [s for s in self.current_universe if s in ("AAPL", "TSLA")]
    TradingUniverse.update_for_end_of_day = patched_update_for_end_of_day
    # Run backtest
    await run_backtest(Args())
    # Restore
    TradingUniverse.update_for_end_of_day = orig_update_for_end_of_day
