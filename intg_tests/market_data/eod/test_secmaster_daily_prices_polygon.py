import os
import pytest
import asyncio
import asyncpg
from datetime import date
import importlib.util
import sys
from pathlib import Path
import uuid
from dotenv import load_dotenv

# Ensure .env is loaded for API keys, DB URL, etc.
load_dotenv()

# Ensure tests/ is in sys.path for direct execution (python3 ...)
THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = THIS_DIR.parent.parent
TESTS_DIR = PROJECT_ROOT / "tests"


from intg_tests.db.test_intg_db_base import AsyncPGTestDBBase, get_test_db_url
from config.environment import get_environment, set_environment, EnvironmentType
SCRIPT_PATH = Path(__file__).parent.parent.parent / "src/market_data/eod/daily_polygon.py"

# Ensure environment is set for integration tests and env is globally available
set_environment(EnvironmentType.INTEGRATION)
global env
env = get_environment()

class TestIntegrationPolygon(AsyncPGTestDBBase):
    @pytest.mark.asyncio
    async def test_download_and_populate_daily_prices_polygon(self, tmp_path):
        test_symbol = "AAPL"
        test_start = date(2025, 1, 1)
        test_end = date(2025, 1, 10)
        # Clean up any existing data for the test symbol and range
        pool = await asyncpg.create_pool(env.get_database_url(), min_size=1, max_size=2)
        async with pool.acquire() as conn:
            await conn.execute(
                f"DELETE FROM {env.get_table_name('daily_prices_polygon')} WHERE symbol = $1 AND date >= $2 AND date <= $3",
                test_symbol, test_start, test_end
            )
        await pool.close()
        # Dynamically import the script as a module
        spec = importlib.util.spec_from_file_location("polygon_script", SCRIPT_PATH)
        polygon_script = importlib.util.module_from_spec(spec)
        sys.modules["polygon_script"] = polygon_script
        spec.loader.exec_module(polygon_script)
        # Run the main function with test args (simulate CLI)
        sys_argv_backup = sys.argv
        sys.argv = [str(SCRIPT_PATH), "--start_date", test_start.isoformat(), "--end_date", test_end.isoformat(), "--ticker", test_symbol]
        try:
            await polygon_script.main()
        finally:
            sys.argv = sys_argv_backup
        # Check DB for expected data
        pool = await asyncpg.create_pool(env.get_database_url(), min_size=1, max_size=2)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {env.get_table_name('daily_prices_polygon')} WHERE symbol = $1 AND date >= $2 AND date <= $3 ORDER BY date",
                test_symbol, test_start, test_end
            )
            assert len(rows) > 0, "No prices inserted for test symbol and range"
            before_count = len(rows)
        await pool.close()
        # Now re-run for the same range, should NOT re-fetch from Polygon (no duplicates, no new rows)
        sys.argv = [str(SCRIPT_PATH), "--start_date", test_start.isoformat(), "--end_date", test_end.isoformat(), "--ticker", test_symbol]
        await polygon_script.main()
        # Check DB again
        pool = await asyncpg.create_pool(env.get_database_url(), min_size=1, max_size=2)
        async with pool.acquire() as conn:
            rows2 = await conn.fetch(
                f"SELECT * FROM {env.get_table_name('daily_prices_polygon')} WHERE symbol = $1 AND date >= $2 AND date <= $3 ORDER BY date",
                test_symbol, test_start, test_end
            )
            assert len(rows2) == before_count, "Duplicate or new rows were inserted when none should have been fetched"
        await pool.close()
