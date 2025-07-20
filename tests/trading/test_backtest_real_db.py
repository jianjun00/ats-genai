import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
import pytest
import asyncio
from datetime import date, timedelta
import numpy as np
from trading.backtest import run_backtest

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

@pytest.mark.asyncio
async def test_backtest_real_db_aapl_tsla():
    """
    Integration test: run backtest for AAPL and TSLA over real trading_db for 2025-01-01 to 2025-02-01.
    Requires daily_prices and related tables to be populated for AAPL and TSLA for this date range.
    """
    class Args:
        start_date = "2025-01-01"
        end_date = "2025-02-01"
        data_start_days = 30
        db_url = TSDB_URL
        # Optionally, you could add a universe filter if supported
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
