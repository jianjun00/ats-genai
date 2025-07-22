import os
import pytest
import asyncpg
from datetime import date
from dotenv import load_dotenv
from intg_tests.db.test_intg_db_base import AsyncPGTestDBBase
import importlib.util
import sys
from pathlib import Path

load_dotenv()

SCRIPT_PATH = Path(__file__).parent.parent.parent / "src/secmaster/unify_daily_prices.py"

class TestUnifyDailyPrices(AsyncPGTestDBBase):
    @pytest.mark.asyncio
    async def test_unify_various_cases(self, tmp_path):
        test_symbol = "TESTU"
        d1 = date(2025, 1, 1)
        d2 = date(2025, 1, 2)
        d3 = date(2025, 1, 3)
        d4 = date(2025, 1, 4)
        # Clean up all tables for test symbol
        pool = await asyncpg.create_pool(os.environ["TSDB_URL"])
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM daily_prices WHERE symbol = $1", test_symbol)
            await conn.execute("DELETE FROM daily_prices_tiingo WHERE symbol = $1", test_symbol)
            await conn.execute("DELETE FROM daily_prices_polygon WHERE symbol = $1", test_symbol)
        await pool.close()
        # Insert test data
        pool = await asyncpg.create_pool(os.environ["TSDB_URL"])
        async with pool.acquire() as conn:
            # Only Tiingo on d1
            await conn.execute("INSERT INTO daily_prices_tiingo (date, symbol, open, high, low, close, adjClose, volume) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", d1, test_symbol, 10, 11, 9, 10.5, 10.4, 1000)
            # Only Polygon on d2
            await conn.execute("INSERT INTO daily_prices_polygon (date, symbol, open, high, low, close, volume, market_cap) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", d2, test_symbol, 20, 21, 19, 20.5, 2000, 0)
            # Both, close enough on d3
            await conn.execute("INSERT INTO daily_prices_tiingo (date, symbol, open, high, low, close, adjClose, volume) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", d3, test_symbol, 30, 31, 29, 30.5, 30.4, 3000)
            await conn.execute("INSERT INTO daily_prices_polygon (date, symbol, open, high, low, close, volume, market_cap) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", d3, test_symbol, 30.01, 31, 29, 30.51, 3000, 0)
            # Both, conflict on d4
            await conn.execute("INSERT INTO daily_prices_tiingo (date, symbol, open, high, low, close, adjClose, volume) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", d4, test_symbol, 40, 41, 39, 40.5, 40.4, 4000)
            await conn.execute("INSERT INTO daily_prices_polygon (date, symbol, open, high, low, close, volume, market_cap) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", d4, test_symbol, 50, 51, 49, 50.5, 5000, 0)
        await pool.close()
        # Run the unify script
        spec = importlib.util.spec_from_file_location("unify_script", SCRIPT_PATH)
        unify_script = importlib.util.module_from_spec(spec)
        sys.modules["unify_script"] = unify_script
        spec.loader.exec_module(unify_script)
        await unify_script.unify_daily_prices(test_symbol, d1, d4)
        # Check results
        pool = await asyncpg.create_pool(os.environ["TSDB_URL"])
        async with pool.acquire() as conn:
            # d1: only Tiingo
            row = await conn.fetchrow("SELECT * FROM daily_prices WHERE symbol = $1 AND date = $2", test_symbol, d1)
            assert row['source'] == 'tiingo'
            assert row['status'] == 'valid'
            assert row['note'] is None
            # d2: only Polygon
            row = await conn.fetchrow("SELECT * FROM daily_prices WHERE symbol = $1 AND date = $2", test_symbol, d2)
            assert row['source'] == 'polygon'
            assert row['status'] == 'valid'
            assert row['note'] is None
            # d3: both, close enough
            row = await conn.fetchrow("SELECT * FROM daily_prices WHERE symbol = $1 AND date = $2", test_symbol, d3)
            assert row['source'] == 'both'
            assert row['status'] == 'valid'
            assert row['note'] is None
            # d4: both, conflict
            row = await conn.fetchrow("SELECT * FROM daily_prices WHERE symbol = $1 AND date = $2", test_symbol, d4)
            assert row['source'] == 'both'
            assert row['status'] == 'conflict'
            assert row['note'] is not None and 'tiingo=40' in row['note'] and 'polygon=50' in row['note']
        await pool.close()
