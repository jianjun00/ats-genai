import os
import pytest
import asyncio
import asyncpg
from datetime import date, timedelta
import importlib.util
import sys
from pathlib import Path

DB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")
SCRIPT_PATH = Path(__file__).parent.parent.parent / "src/universe/daily_tiingo.py"

@pytest.mark.asyncio
async def test_download_and_populate_daily_prices_tiingo(tmp_path):
    test_symbol = "AAPL"
    test_start = "2025-01-01"
    test_end = "2025-01-10"
    # Clean up any existing data for the test symbol and range
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM daily_prices_tiingo WHERE symbol = $1 AND date >= $2 AND date <= $3",
            test_symbol, test_start, test_end
        )
    await pool.close()
    # Dynamically import the script as a module
    spec = importlib.util.spec_from_file_location("tiingo_script", SCRIPT_PATH)
    tiingo_script = importlib.util.module_from_spec(spec)
    sys.modules["tiingo_script"] = tiingo_script
    spec.loader.exec_module(tiingo_script)
    # Run the main function with test args (simulate CLI)
    sys_argv_backup = sys.argv
    sys.argv = [str(SCRIPT_PATH), "--start", test_start, "--end", test_end, "--tickers", test_symbol]
    try:
        await tiingo_script.main()
    finally:
        sys.argv = sys_argv_backup
    # Check DB for expected data
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM daily_prices_tiingo WHERE symbol = $1 AND date >= $2 AND date <= $3 ORDER BY date",
            test_symbol, test_start, test_end
        )
        assert len(rows) > 0, "No prices inserted for test symbol and range"
        # Now re-run for the same range, should NOT re-fetch from Tiingo (no duplicates, no new rows)
        before_count = len(rows)
    await pool.close()
    sys.argv = [str(SCRIPT_PATH), "--start", test_start, "--end", test_end, "--tickers", test_symbol]
    await tiingo_script.main()
    # Check DB again
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        rows2 = await conn.fetch(
            "SELECT * FROM daily_prices_tiingo WHERE symbol = $1 AND date >= $2 AND date <= $3 ORDER BY date",
            test_symbol, test_start, test_end
        )
        assert len(rows2) == before_count, "Duplicate or new rows were inserted when none should have been fetched"
    await pool.close()
