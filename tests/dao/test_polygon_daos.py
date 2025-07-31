import pytest
import asyncio
from config.environment import get_environment, set_environment, EnvironmentType
from db.test_db_manager import unit_test_db
from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
from dao.dividend_polygon_dao import DividendPolygonDAO

@pytest.mark.asyncio
async def test_stock_splits_polygon_dao(unit_test_db):
    set_environment(EnvironmentType.TEST)
    env = get_environment()
    # Patch env to use the actual test DB URL
    env.config.set('database', 'database', unit_test_db.split('/')[-1])
    dao = StockSplitsPolygonDAO(env)
    import datetime
    split = {
        'symbol': 'AAPL',
        'execution_date': datetime.date(2024, 7, 30),
        'split_from': 2,
        'split_to': 1,
        'cash_amount': 0.0,
        'declaration_date': datetime.date(2024, 7, 1),
        'payment_date': datetime.date(2024, 7, 31),
        'record_date': datetime.date(2024, 7, 25),
        'description': '2-for-1 split',
        'refid': 'SPLIT123',
    }
    await dao.insert_split(split)
    splits = await dao.get_splits_by_symbol('AAPL')
    assert len(splits) == 1
    s = splits[0]
    assert s['symbol'] == 'AAPL'
    assert s['execution_date'].strftime('%Y-%m-%d') == '2024-07-30'
    assert s['split_from'] == 2
    assert s['split_to'] == 1
    assert s['description'] == '2-for-1 split'
    all_splits = await dao.get_all_splits()
    assert any(row['symbol'] == 'AAPL' for row in all_splits)

@pytest.mark.asyncio
async def test_dividend_polygon_dao(unit_test_db):
    set_environment(EnvironmentType.TEST)
    env = get_environment()
    # Patch env to use the actual test DB URL
    env.config.set('database', 'database', unit_test_db.split('/')[-1])
    dao = DividendPolygonDAO(env)
    import datetime
    dividend = {
        'symbol': 'AAPL',
        'ex_dividend_date': datetime.date(2024, 8, 1),
        'cash_amount': 0.24,
        'declaration_date': datetime.date(2024, 7, 10),
        'payment_date': datetime.date(2024, 8, 15),
        'record_date': datetime.date(2024, 8, 5),
        'description': 'Quarterly dividend',
        'refid': 'DIV123',
    }
    await dao.insert_dividend(dividend)
    divs = await dao.get_dividends_by_symbol('AAPL')
    assert len(divs) == 1
    d = divs[0]
    assert d['symbol'] == 'AAPL'
    assert d['ex_dividend_date'].strftime('%Y-%m-%d') == '2024-08-01'
    assert d['cash_amount'] == 0.24
    assert d['description'] == 'Quarterly dividend'
    all_divs = await dao.get_all_dividends()
    assert any(row['symbol'] == 'AAPL' for row in all_divs)
