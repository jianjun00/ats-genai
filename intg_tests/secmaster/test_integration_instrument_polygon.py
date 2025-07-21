import os
import sys
from pathlib import Path
# Ensure tests/ is in sys.path for direct execution and pytest
THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = THIS_DIR.parent.parent
TESTS_DIR = PROJECT_ROOT / "tests"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))
import pytest
import asyncpg
from dotenv import load_dotenv
from db.test_db_base import AsyncPGTestDBBase
import importlib.util

load_dotenv()

SCRIPT_PATH = Path(__file__).parent.parent.parent / "src/secmaster/populate_instrument_polygon.py"

class TestIntegrationInstrumentPolygon(AsyncPGTestDBBase):
    @pytest.mark.asyncio
    async def test_download_and_verify_instrument_polygon(self, tmp_path):
        test_symbols = ["AAPL", "TSLA", "CRWV"]
        # Clean up any old rows
        pool = await asyncpg.create_pool(os.environ["TSDB_URL"])
        async with pool.acquire() as conn:
            for sym in test_symbols:
                await conn.execute("DELETE FROM instrument_polygon WHERE symbol = $1", sym)
        await pool.close()
        # Patch the script to only fetch these tickers
        spec = importlib.util.spec_from_file_location("populate_script", SCRIPT_PATH)
        populate_script = importlib.util.module_from_spec(spec)
        sys.modules["populate_script"] = populate_script
        spec.loader.exec_module(populate_script)
        # Monkeypatch fetch_and_store_instruments to only fetch these tickers
        import asyncio
        async def fetch_selected():
            pool = await asyncpg.create_pool(os.environ["TSDB_URL"])
            for symbol in test_symbols:
                url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={os.environ['POLYGON_API_KEY']}"
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
        pool = await asyncpg.create_pool(os.environ["TSDB_URL"])
        async with pool.acquire() as conn:
            for sym in test_symbols:
                row = await conn.fetchrow("SELECT * FROM instrument_polygon WHERE symbol = $1", sym)
                assert row is not None, f"No row found for {sym}"
                assert row['symbol'] == sym
                assert row['name'] is not None and len(row['name']) > 0
                assert row['exchange'] is not None and len(row['exchange']) > 0
                assert row['raw'] is not None
        await pool.close()
