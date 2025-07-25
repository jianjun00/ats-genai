import pytest
import asyncpg
import os
from datetime import date, timedelta
from src.universe import universe_creator
from db.test_db_manager import unit_test_db
from src.config import Environment, EnvironmentType

@pytest.mark.asyncio
async def test_universe_add_remove(unit_test_db, monkeypatch):
    # Use the test DB created by unit_test_db fixture, schema is initialized
    print(f"[DEBUG] Using test DB URL: {unit_test_db}")
    assert "test_trading_db_" in unit_test_db, f"Unexpected DB URL: {unit_test_db}"
    pool = await asyncpg.create_pool(unit_test_db)
    env = Environment(EnvironmentType.TEST)
    universe_table = env.get_table_name('universe')
    instrument_table = env.get_table_name('instrument_polygon')
    daily_prices_table = env.get_table_name('daily_prices_tiingo')
    membership_table = env.get_table_name('universe_membership')

    # Insert test universe row for 'default'
    async with pool.acquire() as conn:
        await conn.execute(f"""
            INSERT INTO {universe_table} (id, name, description)
            VALUES (1, 'default', 'Default universe for daily screening')
            ON CONFLICT (id) DO NOTHING
        """)
    print("[DEBUG] Inserted test universe row for 'default'")
    # Insert test instruments with all required columns
    async with pool.acquire() as conn:
        await conn.execute(f"""
            INSERT INTO {instrument_table}
                (symbol, name, exchange, type, currency, figi, isin, list_date, delist_date, created_at, updated_at)
            VALUES
                ('TESTA', 'Test A', 'XNYS', 'CS', 'USD', 'FIGI1', 'ISIN1', '2025-01-01', NULL, now(), now()),
                ('TESTB', 'Test B', 'XNYS', 'CS', 'USD', 'FIGI2', 'ISIN2', '2025-01-01', '2025-01-03', now(), now()),
                ('TESTC', 'Test C', 'XNYS', 'CS', 'USD', 'FIGI3', 'ISIN3', '2025-01-01', NULL, now(), now())
        """)
    print("[DEBUG] Inserted test instruments into test_instrument_polygon with all required columns")
    # Insert daily prices
    # Insert ADV_WINDOW days of history before the test start date for each symbol
    ADV_WINDOW = 20
    start_hist = date(2024,12,13)  # 2025-01-01 minus 19 days
    end_test = date(2025,1,4)
    prices = []
    # Generate all dates from start_hist to end_test (inclusive)
    num_days = (end_test - start_hist).days + 1
    print(f"[DEBUG] ADV_WINDOW: {ADV_WINDOW}, start_hist: {start_hist}, end_test: {end_test}, num_days: {num_days}")
    for i in range(num_days):
        d = start_hist + timedelta(days=i)
        # Use special values for test window days, else default history values
        # For all days, TESTA and TESTB meet thresholds, TESTC does not
        prices.append(('TESTA', d, 10, 200000))  # Always meets min_price=5, min_adv=100000
        prices.append(('TESTB', d, 20, 200000))  # Always meets min_price=5, min_adv=100000
        prices.append(('TESTC', d, 4, 150000))   # Never meets min_price=5

    async with pool.acquire() as conn:
        await conn.executemany(
            f"INSERT INTO {daily_prices_table} (symbol, date, close, volume) VALUES ($1, $2, $3, $4)",
            prices
        )
    print(f"[DEBUG] Inserted {len(prices)} rows into test_daily_prices_tiingo")
    print(f"[DEBUG] Sample inserted prices: {prices[:5]}")
    # Print all prices for TESTA, TESTB, TESTC
    for symbol in ['TESTA', 'TESTB', 'TESTC']:
        symbol_prices = [p for p in prices if p[0] == symbol]
        print(f"[DEBUG] All prices for {symbol}: {symbol_prices}")
    # Print table contents before running universe_creator
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT * FROM {daily_prices_table} ORDER BY date, symbol")
        print(f"[DEBUG] test_daily_prices_tiingo contents before universe_creator: {rows}")
        insts = await conn.fetch(f"SELECT * FROM {instrument_table} ORDER BY symbol")
        print(f"[DEBUG] test_instrument_polygon contents before universe_creator: {insts}")
    # Run the universe_creator logic with --environment test for table prefixing
    args = [
        '--start_date', '2025-01-01',
        '--end_date', '2025-01-04',
        '--min_price', '5',
        '--min_adv', '100000',
        '--universe_name', 'default',
        '--environment', 'test'
    ]
    monkeypatch.setattr('sys.argv', ['universe_creator.py'] + args)
    await universe_creator.main()
    print("[DEBUG] Ran universe_creator.main()")
    # Print universe_membership table after running universe_creator
    async with pool.acquire() as conn:
        rows_after = await conn.fetch(f"SELECT * FROM {membership_table} ORDER BY start_at, symbol")
        print(f"[DEBUG] test_universe_membership contents after universe_creator: {rows_after}")
    # Check results in universe table
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT universe_id, start_at, symbol, end_at FROM {membership_table} ORDER BY start_at, symbol")
            all_rows = await conn.fetch(f"SELECT * FROM {membership_table} ORDER BY start_at, symbol")
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
