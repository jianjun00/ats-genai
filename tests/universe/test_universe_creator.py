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
    assert "trading_db" not in unit_test_db, f"Unexpected DB URL contains legacy trading_db: {unit_test_db}"
    assert "test" in unit_test_db, f"Test DB URL should contain 'test': {unit_test_db}"
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
    # Insert test instruments into both instruments and instrument_polygon
    instruments_table = env.get_table_name('instruments')
    async with pool.acquire() as conn:
        await conn.execute(f"""
            INSERT INTO {instruments_table}
                (symbol, name, exchange, type, currency, figi, isin, list_date, delist_date, created_at, updated_at)
            VALUES
                ('TESTA', 'Test A', 'XNYS', 'CS', 'USD', 'FIGI1', 'ISIN1', '2025-01-01', NULL, now(), now()),
                ('TESTB', 'Test B', 'XNYS', 'CS', 'USD', 'FIGI2', 'ISIN2', '2025-01-01', '2025-01-03', now(), now()),
                ('TESTC', 'Test C', 'XNYS', 'CS', 'USD', 'FIGI3', 'ISIN3', '2025-01-01', NULL, now(), now())
        """)
    async with pool.acquire() as conn:
        await conn.execute(f"""
            INSERT INTO {instrument_table}
                (symbol, name, exchange, type, currency, figi, isin, list_date, delist_date, created_at, updated_at)
            VALUES
                ('TESTA', 'Test A', 'XNYS', 'CS', 'USD', 'FIGI1', 'ISIN1', '2025-01-01', NULL, now(), now()),
                ('TESTB', 'Test B', 'XNYS', 'CS', 'USD', 'FIGI2', 'ISIN2', '2025-01-01', '2025-01-03', now(), now()),
                ('TESTC', 'Test C', 'XNYS', 'CS', 'USD', 'FIGI3', 'ISIN3', '2025-01-01', NULL, now(), now())
        """)
    print("[DEBUG] Inserted test instruments into both test_instruments and test_instrument_polygon with all required columns")
    # Print schema for daily_prices_tiingo
    async with pool.acquire() as conn:
        columns = await conn.fetch(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{daily_prices_table}'
            ORDER BY ordinal_position
        """)
        print(f"[DEBUG] Schema for {daily_prices_table}:")
        for col in columns:
            print(f"  {col['column_name']} {col['data_type']} nullable={col['is_nullable']}")
        indexes = await conn.fetch(f"""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = '{daily_prices_table}'
        """)
        print(f"[DEBUG] Indexes for {daily_prices_table}:")
        for idx in indexes:
            print(f"  {idx['indexname']}: {idx['indexdef']}")
        # Print all rows before insert
        rows = await conn.fetch(f"SELECT date, symbol, instrument_id FROM {daily_prices_table}")
        print(f"[DEBUG] Existing rows in {daily_prices_table} before test insert: {len(rows)}")
        for row in rows:
            print(f"  {row}")
        # Clean up table before test insert
        await conn.execute(f"DELETE FROM {daily_prices_table}")
        print(f"[DEBUG] Deleted all rows from {daily_prices_table} before test insert.")
    # Insert daily prices
    # Fetch instrument_id for each symbol from instruments
    async with pool.acquire() as conn:
        inst_rows = await conn.fetch(f"SELECT symbol, id FROM {instruments_table}")
        symbol_to_id = {row['symbol']: row['id'] for row in inst_rows}
    print(f"[DEBUG] symbol_to_id mapping: {symbol_to_id}")

    # Insert ADV_WINDOW days of history before the test start date for each symbol
    ADV_WINDOW = 20
    start_hist = date(2024,12,13)  # 2025-01-01 minus 19 days
    end_test = date(2025,1,4)
    # instrument_polygon PK is symbol, no id column
    # Use symbol as key for prices, and do not fetch id
    prices = []
    num_days = (end_test - start_hist).days + 1
    print(f"[DEBUG] ADV_WINDOW: {ADV_WINDOW}, start_hist: {start_hist}, end_test: {end_test}, num_days: {num_days}")
    for i in range(num_days):
        d = start_hist + timedelta(days=i)
        for symbol, close, volume in [('TESTA', 10, 200000), ('TESTB', 20, 200000), ('TESTC', 4, 150000)]:
            instrument_id = symbol_to_id[symbol]
            prices.append((d, symbol, instrument_id, close, volume))
    # Debug output for all price rows before filtering
    print("[DEBUG] All price rows before dedup:")
    for row in prices:
        print(f"  {row}")
    # Remove duplicates by (date, instrument_id)
    seen = set()
    deduped_prices = []
    for row in prices:
        key = (row[0], row[2])
        if key not in seen:
            deduped_prices.append(row)
            seen.add(key)
    print(f"[DEBUG] Deduped {len(deduped_prices)} price rows (removed {len(prices) - len(deduped_prices)} duplicates)")
    # Insert only the first row for isolation test
    first_row = deduped_prices[0]
    print(f"[DEBUG] Attempting single-row insert: {first_row} in test_universe_add_remove")

    async with pool.acquire() as conn:
        await conn.executemany(
            f"INSERT INTO {daily_prices_table} (date, symbol, instrument_id, close, volume) VALUES ($1, $2, $3, $4, $5)",
            prices
        )
    print(f"[DEBUG] Inserted {len(prices)} rows into test_daily_prices_tiingo with instrument_id")
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
    # Call the business logic directly instead of main()
    await universe_creator.create_universe_membership(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 4),
        min_adv=100000,
        min_price=5,
        universe_name='default',
        env=env,
        pool=pool
    )
    print("[DEBUG] Ran create_universe_membership()")
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
            def to_date(dt):
                return dt.date() if hasattr(dt, 'date') else dt
            actual = [(to_date(r['start_at']), r['symbol'], to_date(r['end_at']) if r['end_at'] else None) for r in rows]
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
