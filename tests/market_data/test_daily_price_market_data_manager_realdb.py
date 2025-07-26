import pytest
import asyncio
from datetime import date, datetime
from market_data.daily_price_market_data_manager import DailyPriceMarketDataManager
from db.test_db_manager import unit_test_db, TestDatabaseManager
from market_data.eod.daily_prices_dao import DailyPricesDAO
from config.environment import get_environment

async def insert_test_fixtures(env, instrument_rows):
    import asyncpg
    vendor_table = env.get_table_name('vendors')
    instrument_table = env.get_table_name('instruments')
    xref_table = env.get_table_name('instrument_xrefs')
    pool = await asyncpg.create_pool(env.get_database_url())
    async with pool.acquire() as conn:
        for inst in instrument_rows:
            # Insert vendor row (ticker as vendor name)
            await conn.execute(f"""
                INSERT INTO {vendor_table} (vendor_id, name, description) VALUES ($1, $2, $3)
                ON CONFLICT (vendor_id) DO NOTHING;
            """, inst['vendor_id'], inst['ticker'], f"Vendor for {inst['ticker']}")
            # Insert instrument row
            await conn.execute(f"""
                INSERT INTO {instrument_table} (id, symbol, name, exchange, type, currency, active, list_date) VALUES ($1, $2, $3, 'NASDAQ', 'Equity', 'USD', TRUE, '2000-01-01')
                ON CONFLICT (id) DO NOTHING;
            """, inst['instrument_id'], inst['symbol'], f"{inst['symbol']} Inc.")
            # Insert instrument_xref row
            await conn.execute(f"""
                INSERT INTO {xref_table} (instrument_id, vendor_id, symbol, type, start_at, end_at) VALUES ($1, $2, $3, 'Equity', $4, NULL)
                ON CONFLICT DO NOTHING;
            """, inst['instrument_id'], inst['vendor_id'], inst['symbol'], inst['xref_start'])
    await pool.close()

async def cleanup_test_fixtures(env, instrument_rows):
    import asyncpg
    vendor_table = env.get_table_name('vendors')
    instrument_table = env.get_table_name('instruments')
    xref_table = env.get_table_name('instrument_xrefs')
    daily_prices_table = env.get_table_name('daily_prices')
    pool = await asyncpg.create_pool(env.get_database_url())
    vendor_ids = [inst['vendor_id'] for inst in instrument_rows]
    instrument_ids = [inst['instrument_id'] for inst in instrument_rows]
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {daily_prices_table} WHERE instrument_id = ANY($1)", instrument_ids)
        await conn.execute(f"DELETE FROM {xref_table} WHERE instrument_id = ANY($1)", instrument_ids)
        await conn.execute(f"DELETE FROM {instrument_table} WHERE id = ANY($1)", instrument_ids)
        await conn.execute(f"DELETE FROM {vendor_table} WHERE vendor_id = ANY($1)", vendor_ids)
    await pool.close()
@pytest.mark.asyncio
async def test_daily_price_manager_sod_eod(unit_test_db):
    env = get_environment()
    dao = DailyPricesDAO(env)
    today = date(2024, 1, 2)
    # Cleanup before test
    import asyncpg
    pool = await asyncpg.create_pool(env.get_database_url())
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {dao.table_name} WHERE date=$1 AND symbol IN ($2, $3)", today, 'AAPL', 'TSLA')
    await pool.close()
    # Prepare instrument, vendor, xref rows for canonical instrument_ids
    instrument_rows = [
        {'instrument_id': 1, 'symbol': 'AAPL', 'ticker': 'AAPL', 'vendor_id': 10, 'xref_start': today},
        {'instrument_id': 2, 'symbol': 'TSLA', 'ticker': 'TSLA', 'vendor_id': 20, 'xref_start': today}
    ]
    await insert_test_fixtures(env, instrument_rows)
    try:
        """
        Test SOD/EOD and get_ohlc with real DB and real daily_prices table.
        """
        env = get_environment()
        dao = DailyPricesDAO(env)
        today = date(2024, 1, 2)
        await dao.insert_price(today, 'AAPL', 1, 155, 148, 154, 10000)
        await dao.insert_price(today, 'TSLA', 2, 710, 690, 705, 20000)

        class TestManager(DailyPriceMarketDataManager):
            def _get_all_symbols(self):
                return ['AAPL', 'TSLA']
            def _symbol_to_id(self, symbol):
                return 1 if symbol == 'AAPL' else 2
            def _get_exchange_open_close(self, cur_date):
                return (
                    datetime(2024, 1, 2, 9, 30),
                    datetime(2024, 1, 2, 16, 0)
                )

        manager = TestManager(env=env)
        await manager.update_for_sod(None, datetime(2024, 1, 2, 9, 30))
        ohlc_aapl = manager.get_ohlc(1, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
        assert ohlc_aapl['open'] == 155
        assert ohlc_aapl['close'] == 154
        assert ohlc_aapl['high'] == 155
        assert ohlc_aapl['low'] == 148
        assert ohlc_aapl['volume'] == 10000
        assert ohlc_aapl['traded_dollar'] == 1540000
        ohlc_tsla = manager.get_ohlc(2, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
        assert ohlc_tsla['open'] == 710
        assert ohlc_tsla['close'] == 705
        assert ohlc_tsla['high'] == 710
        assert ohlc_tsla['low'] == 690
        assert ohlc_tsla['volume'] == 20000
        assert ohlc_tsla['traded_dollar'] == 14100000
        await manager.update_for_eod(today)
        await asyncio.sleep(0)
        assert manager._intervals == {}
    finally:
        await cleanup_test_fixtures(env, instrument_rows)

@pytest.mark.asyncio
async def test_daily_price_manager_last_price_before_start(unit_test_db):
    env = get_environment()
    dao = DailyPricesDAO(env)
    prev_date = date(2023, 12, 29)
    # Cleanup before test
    import asyncpg
    pool = await asyncpg.create_pool(env.get_database_url())
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {dao.table_name} WHERE date=$1 AND symbol IN ($2, $3)", prev_date, 'AAPL', 'TSLA')
    await pool.close()
    # Prepare instrument, vendor, xref rows for all instrument_ids used in this test
    instrument_rows = [
        {'instrument_id': 1, 'symbol': 'AAPL', 'ticker': 'AAPL', 'vendor_id': 10, 'xref_start': prev_date},
        {'instrument_id': 2, 'symbol': 'TSLA', 'ticker': 'TSLA', 'vendor_id': 20, 'xref_start': prev_date},
        {'instrument_id': 100, 'symbol': 'AAPL', 'ticker': 'AAPL', 'vendor_id': 10, 'xref_start': prev_date},
        {'instrument_id': 105, 'symbol': 'AAPL', 'ticker': 'AAPL', 'vendor_id': 10, 'xref_start': prev_date},
        {'instrument_id': 400, 'symbol': 'TSLA', 'ticker': 'TSLA', 'vendor_id': 20, 'xref_start': prev_date},
        {'instrument_id': 410, 'symbol': 'TSLA', 'ticker': 'TSLA', 'vendor_id': 20, 'xref_start': prev_date}
    ]
    await insert_test_fixtures(env, instrument_rows)
    try:
        """
        Test loading last price before start_date during construction.
        """
        env = get_environment()
        dao = DailyPricesDAO(env)
        prev_date = date(2023, 12, 29)
        await dao.insert_price(prev_date, 'AAPL', 100, 110, 90, 105, 5000)
        await dao.insert_price(prev_date, 'TSLA', 400, 420, 390, 410, 8000)

        class TestManager(DailyPriceMarketDataManager):
            def _get_all_symbols(self):
                return ['AAPL', 'TSLA']
            def _symbol_to_id(self, symbol):
                return 1 if symbol == 'AAPL' else 2
            def _get_exchange_open_close(self, cur_date):
                return (
                    datetime(2023, 12, 29, 9, 30),
                    datetime(2023, 12, 29, 16, 0)
                )
        manager = TestManager(env=env, start_date=date(2024, 1, 2))
        await manager._load_last_prices_before_start()
        assert manager._last_prices[1]['close'] == 105
        assert manager._last_prices[2]['close'] == 410
    finally:
        await cleanup_test_fixtures(env, instrument_rows)

@pytest.mark.asyncio
async def test_daily_price_manager_multi_dates(unit_test_db):
    env = get_environment()
    dao = DailyPricesDAO(env)
    d1 = date(2024, 1, 2)
    d2 = date(2024, 1, 3)
    # Cleanup before test
    import asyncpg
    pool = await asyncpg.create_pool(env.get_database_url())
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {dao.table_name} WHERE (date=$1 OR date=$2) AND symbol IN ($3, $4)", d1, d2, 'AAPL', 'TSLA')
    await pool.close()
    # Prepare instrument, vendor, xref rows for canonical instrument_ids
    instrument_rows = [
        {'instrument_id': 1, 'symbol': 'AAPL', 'ticker': 'AAPL', 'vendor_id': 10, 'xref_start': d1},
        {'instrument_id': 2, 'symbol': 'TSLA', 'ticker': 'TSLA', 'vendor_id': 20, 'xref_start': d1}
    ]
    await insert_test_fixtures(env, instrument_rows)
    try:
        """
        Test SOD/EOD and get_ohlc for multiple dates and intervals.
        """
        env = get_environment()
        dao = DailyPricesDAO(env)
        d1 = date(2024, 1, 2)
        d2 = date(2024, 1, 3)
        await dao.insert_price(d1, 'AAPL', 1, 155, 148, 154, 10000)
        await dao.insert_price(d1, 'TSLA', 2, 710, 690, 705, 20000)
        await dao.insert_price(d2, 'AAPL', 1, 158, 153, 157, 12000)
        await dao.insert_price(d2, 'TSLA', 2, 715, 705, 712, 21000)

        class TestManager(DailyPriceMarketDataManager):
            def _get_all_symbols(self):
                return ['AAPL', 'TSLA']
            def _symbol_to_id(self, symbol):
                return 1 if symbol == 'AAPL' else 2
            def _get_exchange_open_close(self, cur_date):
                return (
                    datetime(cur_date.year, cur_date.month, cur_date.day, 9, 30),
                    datetime(cur_date.year, cur_date.month, cur_date.day, 16, 0)
                )

        manager = TestManager(env=env)
        await manager.update_for_sod(None, datetime(2024, 1, 2, 9, 30))
        ohlc_aapl_d1 = manager.get_ohlc(1, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
        assert ohlc_aapl_d1['open'] == 155
        assert ohlc_aapl_d1['close'] == 154
        ohlc_tsla_d1 = manager.get_ohlc(2, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
        assert ohlc_tsla_d1['open'] == 710
        assert ohlc_tsla_d1['close'] == 705
        await manager.update_for_sod(None, datetime(2024, 1, 3, 9, 30))
        ohlc_aapl_d2 = manager.get_ohlc(1, datetime(2024, 1, 3, 9, 30), datetime(2024, 1, 3, 16, 0))
        assert ohlc_aapl_d2['open'] == 158
        assert ohlc_aapl_d2['close'] == 157
        ohlc_tsla_d2 = manager.get_ohlc(2, datetime(2024, 1, 3, 9, 30), datetime(2024, 1, 3, 16, 0))
        assert ohlc_tsla_d2['open'] == 715
        assert ohlc_tsla_d2['close'] == 712
        missing = manager.get_ohlc(1, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
        assert missing is None or missing['close'] == 154
        await manager.update_for_sod(None, datetime(2024, 1, 2, 9, 30))
        ohlc_aapl_d1b = manager.get_ohlc(1, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
        assert ohlc_aapl_d1b['open'] == 155
        assert ohlc_aapl_d1b['close'] == 154
        ohlc_tsla_d1b = manager.get_ohlc(2, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
        assert ohlc_tsla_d1b['open'] == 710
        assert ohlc_tsla_d1b['close'] == 705
        await manager.update_for_eod(None, datetime(2024, 1, 2, 16, 0))
        await asyncio.sleep(0)
        assert manager._intervals == {}
    finally:
        await cleanup_test_fixtures(env, instrument_rows)
