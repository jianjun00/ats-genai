import pytest
import asyncio
from config.environment import EnvironmentType
from db.test_db_manager import unit_test_db
from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
from dao.dividend_polygon_dao import DividendPolygonDAO
from config.environment import Environment

@pytest.mark.asyncio
async def test_stock_splits_polygon_dao(unit_test_db):
    import logging
    logging.debug(f"[TEST DEBUG] unit_test_db: {unit_test_db}")
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
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

async def debug_table_existence(db_url, table_name):
    import asyncpg
    import logging
    logging.debug(f"[DEBUG] Checking existence of table '{table_name}' in DB: {db_url}")
    pool = await asyncpg.create_pool(db_url)
    try:
        async with pool.acquire() as conn:
            # Print all tables in DB
            tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")
            logging.debug(f"[DEBUG] All tables in DB: {[t['tablename'] for t in tables]}")
            result = await conn.fetchval("SELECT to_regclass($1)", table_name)
            if result is None:
                logging.debug(f"[DEBUG] Table '{table_name}' does NOT exist in database!")
            else:
                logging.debug(f"[DEBUG] Table '{table_name}' exists as: {result}")
    finally:
        await pool.close()

@pytest.mark.asyncio
async def test_dividend_polygon_dao(unit_test_db):
    import logging
    logging.debug(f"[TEST DEBUG] unit_test_db: {unit_test_db}")
    await debug_table_existence(unit_test_db, "test_dividend_polygon")
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
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
