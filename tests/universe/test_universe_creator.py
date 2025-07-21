import pytest
import asyncpg
import os
from datetime import date, timedelta
from src.universe import universe_creator
from tests.db.test_db_base import AsyncPGTestDBBase

class TestUniverseCreator(AsyncPGTestDBBase):
    @pytest.mark.asyncio
    async def test_universe_add_remove(self, monkeypatch):
        # Use the test DB created by AsyncPGTestDBBase, schema is initialized
        print(f"[DEBUG] Using test DB URL: {self._db_url}")
        assert "test_db_" in self._db_url, f"Unexpected DB URL: {self._db_url}"
        pool = await asyncpg.create_pool(self._db_url)
        universe_table = 'universe_membership'
        # Insert test instruments only
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO instrument_polygon (symbol, list_date, delist_date)
                VALUES ('TESTA', '2025-01-01', NULL),
                       ('TESTB', '2025-01-01', '2025-01-03'),
                       ('TESTC', '2025-01-01', NULL)
            """)
        # Insert daily prices
        # Insert ADV_WINDOW days of history before the test start date for each symbol
        ADV_WINDOW = 20
        start_hist = date(2024,12,13)  # 2025-01-01 minus 19 days
        end_test = date(2025,1,4)
        prices = []
        # Generate all dates from start_hist to end_test (inclusive)
        num_days = (end_test - start_hist).days + 1
        for i in range(num_days):
            d = start_hist + timedelta(days=i)
            # Use special values for test window days, else default history values
            if d == date(2025,1,1):
                prices.append(('TESTA', d, 10, 200000))
                prices.append(('TESTB', d, 20, 200000))
                prices.append(('TESTC', d, 4, 150000))
            elif d == date(2025,1,2):
                prices.append(('TESTA', d, 12, 210000))
                prices.append(('TESTB', d, 21, 210000))
                prices.append(('TESTC', d, 4, 160000))
            elif d == date(2025,1,3):
                prices.append(('TESTA', d, 15, 220000))
                prices.append(('TESTB', d, 22, 220000))
                prices.append(('TESTC', d, 4, 170000))
            elif d == date(2025,1,4):
                prices.append(('TESTA', d, 14, 230000))
                prices.append(('TESTB', d, 23, 230000))
                prices.append(('TESTC', d, 4, 180000))
            else:
                prices.append(('TESTA', d, 10, 200000))
                prices.append(('TESTB', d, 20, 200000))
                prices.append(('TESTC', d, 4, 150000))

        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO daily_prices_tiingo (symbol, date, close, volume) VALUES ($1, $2, $3, $4)",
                prices
            )
        # Patch get_all_symbols to only return test symbols
        async def test_get_all_symbols(pool):
            return {'TESTA': None, 'TESTB': date(2025,1,3), 'TESTC': None}
        monkeypatch.setattr(universe_creator, 'get_all_symbols', test_get_all_symbols)
        # Run the universe_creator logic
        args = [
            '--start_date', '2025-01-01',
            '--end_date', '2025-01-04',
            '--min_price', '5',
            '--min_adv', '100000',
            '--universe_name', 'default'
        ]
        monkeypatch.setattr('sys.argv', ['universe_creator.py'] + args)
        await universe_creator.main()
        # Check results in universe table
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT universe_id, start_at, symbol, end_at FROM universe_membership ORDER BY start_at, symbol")
                all_rows = await conn.fetch("SELECT * FROM universe_membership ORDER BY start_at, symbol")
                print("[DEBUG] universe_membership full contents:", all_rows)
            # Verify (start_at, symbol) as before
            # Define expected intervals as (start_at, symbol, end_at)
            expected = [
                (date(2025,1,1), 'TESTA', None),
                (date(2025,1,1), 'TESTB', date(2025,1,4)),
            ]
            actual = [(r['start_at'], r['symbol'], r['end_at']) for r in rows]
            print(f"[DEBUG] actual intervals: {actual}")
            print(f"[DEBUG] expected intervals: {expected}")
            assert set(expected) == set(actual)

            # Also verify end_at if present
            if rows and 'end_at' in rows[0]:
                actual_end = {(r['start_at'], r['symbol']): r['end_at'] for r in rows}
                print(f"[DEBUG] actual end_at: {actual_end}")
                # You can define expected_end here if you know the expected end dates for each (start_at, symbol)
                # For now, just print for manual inspection
            else:
                print("[DEBUG] end_at column not present in universe_membership.")
        finally:
            await pool.close()
            import asyncio
            await asyncio.sleep(0.1)  # Ensure all connections are closed before DB drop

