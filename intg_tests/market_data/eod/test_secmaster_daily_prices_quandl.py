import pytest
import asyncio
from config.environment import set_environment, EnvironmentType, get_environment
from market_data.eod.daily_prices_quandl_dao import DailyPricesQuandlDAO
from market_data.eod import daily_quandl

@pytest.mark.asyncio
async def test_quandl_ingestion_and_dao(tmp_path, monkeypatch):
    # Set environment to test
    set_environment(EnvironmentType.TEST)
    env = get_environment()
    dao = DailyPricesQuandlDAO(env)

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
    stored = await dao.list_prices(symbol)
    assert len(stored) == 2
    assert stored[0]['symbol'] == symbol
    assert stored[0]['open'] == 100.0
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
        environment = 'test'
    # Patch argparse to return test args
    monkeypatch.setattr('argparse.ArgumentParser.parse_args', lambda self: Args)
    await daily_quandl.main()
    # Should not error, and should insert again (ON CONFLICT DO NOTHING)
    stored2 = await dao.list_prices(symbol)
    assert len(stored2) == 2  # No duplicates
