import pytest
import asyncio
import datetime
from config.environment import set_environment, EnvironmentType, get_environment
from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
from dao.dividend_polygon_dao import DividendPolygonDAO

@pytest.mark.asyncio
async def test_integration_stock_splits_polygon_dao(clean_integration_db):
    set_environment(EnvironmentType.INTEGRATION)
    env = get_environment()
    # Patch env to use the actual integration test DB URL
    env.config.set('database', 'database', clean_integration_db.split('/')[-1])
    dao = StockSplitsPolygonDAO(env)
    dao.db_url = clean_integration_db
    split = {
        'symbol': 'MSFT',
        'execution_date': datetime.date(2025, 7, 30),
        'split_from': 3,
        'split_to': 2,
        'cash_amount': 0.0,
        'declaration_date': datetime.date(2025, 7, 1),
        'payment_date': datetime.date(2025, 7, 31),
        'record_date': datetime.date(2025, 7, 25),
        'description': '3-for-2 split',
        'refid': 'SPLIT999',
    }
    await dao.insert_split(split)
    splits = await dao.get_splits_by_symbol('MSFT')
    assert len(splits) == 1
    s = splits[0]
    assert s['symbol'] == 'MSFT'
    assert s['execution_date'] == datetime.date(2025, 7, 30)
    assert s['split_from'] == 3
    assert s['split_to'] == 2
    assert s['description'] == '3-for-2 split'
    all_splits = await dao.get_all_splits()
    assert any(row['symbol'] == 'MSFT' for row in all_splits)

@pytest.mark.asyncio
async def test_integration_dividend_polygon_dao(clean_integration_db):
    set_environment(EnvironmentType.INTEGRATION)
    env = get_environment()
    # Patch env to use the actual integration test DB URL
    env.config.set('database', 'database', clean_integration_db.split('/')[-1])
    dao = DividendPolygonDAO(env)
    dao.db_url = clean_integration_db
    dividend = {
        'symbol': 'MSFT',
        'ex_dividend_date': datetime.date(2025, 8, 1),
        'cash_amount': 0.42,
        'declaration_date': datetime.date(2025, 7, 10),
        'payment_date': datetime.date(2025, 8, 15),
        'record_date': datetime.date(2025, 8, 5),
        'description': 'Quarterly dividend',
        'refid': 'DIV999',
    }
    await dao.insert_dividend(dividend)
    divs = await dao.get_dividends_by_symbol('MSFT')
    assert len(divs) == 1
    d = divs[0]
    assert d['symbol'] == 'MSFT'
    assert d['ex_dividend_date'] == datetime.date(2025, 8, 1)
    assert d['cash_amount'] == 0.42
    assert d['description'] == 'Quarterly dividend'
    all_divs = await dao.get_all_dividends()
    assert any(row['symbol'] == 'MSFT' for row in all_divs)
