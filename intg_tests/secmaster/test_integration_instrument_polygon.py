import os
import sys
from pathlib import Path
# Ensure tests/ is in sys.path for direct execution and pytest
THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = THIS_DIR.parent.parent
TESTS_DIR = PROJECT_ROOT / "tests"
SRC_DIR = PROJECT_ROOT / "src"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))
if str(SRC_DIR.resolve()) not in sys.path:
    sys.path.insert(0, str(SRC_DIR.resolve()))
import pytest
import asyncpg
from dotenv import load_dotenv
from tests.db.test_db_base import AsyncPGTestDBBase
import importlib.util
from config.environment import get_environment, set_environment, EnvironmentType

load_dotenv()

# Set integration environment for these tests
set_environment(EnvironmentType.INTEGRATION)

SCRIPT_PATH = Path(__file__).parent.parent.parent / "src/secmaster/populate_instrument_polygon.py"

class TestIntegrationInstrumentPolygon(AsyncPGTestDBBase):
    @pytest.mark.asyncio
    async def test_download_and_verify_instrument_polygon(self, tmp_path):
        env = get_environment()
        test_symbols = ["AAPL", "TSLA", "CRWV"]
        # Clean up any old rows
        pool = await asyncpg.create_pool(env.get_database_url())
        async with pool.acquire() as conn:
            instrument_table = env.get_table_name("instrument_polygon")
            for sym in test_symbols:
                await conn.execute(f"DELETE FROM {instrument_table} WHERE symbol = $1", sym)
        await pool.close()
        # Patch the script to only fetch these tickers
        spec = importlib.util.spec_from_file_location("populate_script", SCRIPT_PATH)
        populate_script = importlib.util.module_from_spec(spec)
        sys.modules["populate_script"] = populate_script
        spec.loader.exec_module(populate_script)
        # Monkeypatch fetch_and_store_instruments to only fetch these tickers
        import asyncio
        async def fetch_selected():
            pool = await asyncpg.create_pool(env.get_database_url())
            for symbol in test_symbols:
                url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={env.get_api_key('polygon')}"
                import requests
                resp = requests.get(url)
                assert resp.status_code == 200, f"Polygon API failed for {symbol}: {resp.status_code} {resp.text}"
                data = resp.json()
                item = data.get('results')
                assert item is not None, f"No results for {symbol}"
                await populate_script.upsert_instrument(pool, item)
            await pool.close()
        # Run the monkeypatched function
        await fetch_selected()
        # Verify data in DB
        pool = await asyncpg.create_pool(env.get_database_url())
        async with pool.acquire() as conn:
            instrument_table = env.get_table_name("instrument_polygon")
            for sym in test_symbols:
                row = await conn.fetchrow(f"SELECT * FROM {instrument_table} WHERE symbol = $1", sym)
                assert row is not None, f"No row found for {sym} in {instrument_table}"
                assert row['symbol'] == sym
                assert row['name'] is not None and len(row['name']) > 0
                assert row['exchange'] is not None and len(row['exchange']) > 0
                assert row['raw'] is not None
        await pool.close()
