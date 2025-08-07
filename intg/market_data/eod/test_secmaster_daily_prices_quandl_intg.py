import pytest
import asyncio
from config.environment import Environment, EnvironmentType
from market_data.eod.daily_prices_quandl_dao import DailyPricesQuandlDAO
from market_data.eod import daily_quandl

@pytest.mark.asyncio
async def test_quandl_ingestion_and_dao(tmp_path, monkeypatch):
    from intg_tests.db.test_intg_db_base_intg import get_test_db_url
    env = Environment(env_type=EnvironmentType.INTEGRATION, db_url=get_test_db_url())
    dao = DailyPricesQuandlDAO(env)
    import asyncpg
    dao.pool = await asyncpg.create_pool(env.get_database_url())

    # Prepare fake prices
    import datetime
    prices = [
        {'date': datetime.date(2023, 7, 1), 'open': 100.0, 'high': 110.0, 'low': 95.0, 'close': 105.0, 'volume': 10000},
        {'date': datetime.date(2023, 7, 2), 'open': 106.0, 'high': 112.0, 'low': 101.0, 'close': 108.0, 'volume': 12000},
    ]
    symbol = 'FAKE'

    # Clean up any existing test data
    import asyncpg
    pool = await asyncpg.create_pool(env.get_database_url())
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {dao.table} WHERE symbol = $1", symbol)
    await pool.close()

    # Insert prices using DAO
    await dao.batch_insert_prices(prices, symbol)
    stored = await dao.get_prices(symbol, datetime.date(2023, 7, 1), datetime.date(2023, 7, 2))
    assert len(stored) == 2
    assert stored[0]['date'] == datetime.date(2023, 7, 1)
    assert stored[1]['date'] == datetime.date(2023, 7, 2)
    await dao.pool.close()
    assert stored[1]['close'] == 108.0

    # Test ingestion script logic with monkeypatched download
    async def fake_get_all_spy_tickers():
        return [symbol]
    monkeypatch.setattr(daily_quandl, 'get_all_spy_tickers', fake_get_all_spy_tickers)

    def fake_download_prices_quandl(ticker, start, end, api_key):
        return prices
    monkeypatch.setattr(daily_quandl, 'download_prices_quandl', fake_download_prices_quandl)

    # Run main ingestion logic for test env
    class Args:
        ticker = symbol
        start = '2023-07-01'
        end = '2023-07-02'
        environment = 'intg'

    # Patch argparse to return test args
    monkeypatch.setattr('argparse.ArgumentParser.parse_args', lambda self: Args)
    await daily_quandl.main()
    # Re-initialize DAO pool after main(), since pool may be closed
    dao.pool = await asyncpg.create_pool(env.get_database_url())
    stored2 = await dao.get_prices(symbol, datetime.date(2023, 7, 1), datetime.date(2023, 7, 2))
    assert len(stored2) == 2  # No duplicates
    await dao.pool.close()
