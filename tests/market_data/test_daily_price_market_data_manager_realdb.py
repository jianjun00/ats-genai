import pytest
import asyncio
from datetime import date, datetime
from market_data.daily_price_market_data_manager import DailyPriceMarketDataManager
from db.test_db_manager import unit_test_db, TestDatabaseManager
from market_data.eod.daily_prices_dao import DailyPricesDAO
from config.environment import get_environment

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
    # Prepare fixture: insert daily price for two symbols
    """
    Test SOD/EOD and get_ohlc with real DB and real daily_prices table.
    """
    env = get_environment()
    # Prepare fixture: insert daily price for two symbols
    dao = DailyPricesDAO(env)
    today = date(2024, 1, 2)
    await dao.insert_price(today, 'AAPL', 150, 155, 148, 154, 10000)
    await dao.insert_price(today, 'TSLA', 700, 710, 690, 705, 20000)

    # Patch _get_all_symbols and _symbol_to_id
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
    # SOD
    await manager.update_for_sod(None, datetime(2024, 1, 2, 9, 30))
    # Check intervals
    ohlc_aapl = manager.get_ohlc(1, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
    assert ohlc_aapl['open'] == 150
    assert ohlc_aapl['close'] == 154
    assert ohlc_aapl['high'] == 155
    assert ohlc_aapl['low'] == 148
    assert ohlc_aapl['volume'] == 10000
    assert ohlc_aapl['traded_dollar'] == 1540000
    ohlc_tsla = manager.get_ohlc(2, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
    assert ohlc_tsla['open'] == 700
    assert ohlc_tsla['close'] == 705
    assert ohlc_tsla['high'] == 710
    assert ohlc_tsla['low'] == 690
    assert ohlc_tsla['volume'] == 20000
    assert ohlc_tsla['traded_dollar'] == 14100000
    # EOD clears intervals
    await manager.update_for_eod(today)
    await asyncio.sleep(0)
    assert manager._intervals == {}

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
    # Insert test data
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
    # Construct with start_date, triggers loading last price
    manager = TestManager(env=env, start_date=date(2024, 1, 2))
    await manager._load_last_prices_before_start()
    # Check _last_prices
    assert manager._last_prices[1]['close'] == 105
    assert manager._last_prices[2]['close'] == 410

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
    # Insert test data
    """
    Test SOD/EOD and get_ohlc for multiple dates and intervals.
    """
    env = get_environment()
    dao = DailyPricesDAO(env)
    # Insert daily prices for 2 symbols over 2 days
    d1 = date(2024, 1, 2)
    d2 = date(2024, 1, 3)
    await dao.insert_price(d1, 'AAPL', 150, 155, 148, 154, 10000)
    await dao.insert_price(d1, 'TSLA', 700, 710, 690, 705, 20000)
    await dao.insert_price(d2, 'AAPL', 156, 158, 153, 157, 12000)
    await dao.insert_price(d2, 'TSLA', 710, 715, 705, 712, 21000)

    class TestManager(DailyPriceMarketDataManager):
        def _get_all_symbols(self):
            return ['AAPL', 'TSLA']
        def _symbol_to_id(self, symbol):
            return 1 if symbol == 'AAPL' else 2
        def _get_exchange_open_close(self, cur_date):
            # Return open/close for the given date
            return (
                datetime(cur_date.year, cur_date.month, cur_date.day, 9, 30),
                datetime(cur_date.year, cur_date.month, cur_date.day, 16, 0)
            )

    manager = TestManager(env=env)
    # SOD for d1
    await manager.update_for_sod(None, datetime(2024, 1, 2, 9, 30))
    ohlc_aapl_d1 = manager.get_ohlc(1, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
    assert ohlc_aapl_d1['open'] == 150
    assert ohlc_aapl_d1['close'] == 154
    # SOD for d2
    await manager.update_for_sod(None, datetime(2024, 1, 3, 9, 30))
    ohlc_aapl_d2 = manager.get_ohlc(1, datetime(2024, 1, 3, 9, 30), datetime(2024, 1, 3, 16, 0))
    assert ohlc_aapl_d2['open'] == 156
    assert ohlc_aapl_d2['close'] == 157
    # get_ohlc for d1 after d2 SOD (should not find, returns None or fallback)
    missing = manager.get_ohlc(1, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
    assert missing is None or missing['close'] == 154  # fallback if last_prices is set
    # SOD for d1 again, should reload d1
    await manager.update_for_sod(None, datetime(2024, 1, 2, 9, 30))
    ohlc_aapl_d1b = manager.get_ohlc(1, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
    assert ohlc_aapl_d1b['open'] == 150
    assert ohlc_aapl_d1b['close'] == 154
    # EOD clears intervals
    await manager.update_for_eod(None, datetime(2024, 1, 2, 16, 0))
    await asyncio.sleep(0)
    assert manager._intervals == {}
