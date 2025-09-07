import pytest
import datetime
from shared.utils.environment import EnvironmentType
from db.test_db_manager import unit_test_db
from core.dao.vendors.polygon_dao import PolygonDAO
from shared.utils.environment import Environment


def test_stock_splits_polygon_dao(unit_test_db):
    """Test PolygonDAO stock split operations."""
    import logging
    logging.debug(f"[TEST DEBUG] unit_test_db: {unit_test_db}")

    # Create PolygonDAO instance
    dao = PolygonDAO()

    # Test data
    symbol = 'AAPL'
    execution_date = datetime.date(2024, 7, 30)
    split_from = 2.0
    split_to = 1.0
    declaration_date = datetime.date(2024, 7, 1)
    payment_date = datetime.date(2024, 7, 31)
    record_date = datetime.date(2024, 7, 25)
    description = '2-for-1 split'
    refid = 'SPLIT123'

    # Insert stock split
    split_id = dao.insert_stock_split(
        symbol=symbol,
        split_date=execution_date,
        split_ratio=split_to/split_from,  # 0.5 for 2:1 split
        declaration_date=declaration_date,
        payment_date=payment_date,
        record_date=record_date,
        description=description,
        refid=refid
    )

    assert split_id is not None

    # Get splits by symbol
    splits = dao.get_stock_splits(symbol)
    assert len(splits) >= 1

    # Find our split
    our_split = None
    for split in splits:
        if split['data']['refid'] == refid:
            our_split = split
            break

    assert our_split is not None
    assert our_split['symbol'] == symbol
    assert our_split['date'] == execution_date
    assert our_split['data']['description'] == description


def test_dividend_polygon_dao(unit_test_db):
    """Test PolygonDAO dividend operations."""
    import logging
    logging.debug(f"[TEST DEBUG] unit_test_db: {unit_test_db}")

    # Create PolygonDAO instance
    dao = PolygonDAO()

    # Test data
    symbol = 'AAPL'
    ex_dividend_date = datetime.date(2024, 8, 1)
    cash_amount = 0.24
    declaration_date = datetime.date(2024, 7, 15)
    payment_date = datetime.date(2024, 8, 15)
    record_date = datetime.date(2024, 8, 5)
    description = 'Quarterly dividend'
    refid = 'DIV123'

    # Insert dividend
    dividend_id = dao.insert_dividend(
        symbol=symbol,
        ex_date=ex_dividend_date,
        amount=cash_amount,
        declaration_date=declaration_date,
        payment_date=payment_date,
        record_date=record_date,
        description=description,
        refid=refid
    )

    assert dividend_id is not None

    # Get dividends by symbol
    dividends = dao.get_dividends(symbol)
    assert len(dividends) >= 1

    # Find our dividend
    our_dividend = None
    for dividend in dividends:
        if dividend['data']['refid'] == refid:
            our_dividend = dividend
            break

    assert our_dividend is not None
    assert our_dividend['symbol'] == symbol
    assert our_dividend['date'] == ex_dividend_date
    assert our_dividend['data']['amount'] == cash_amount
    assert our_dividend['data']['description'] == description


async def debug_table_existence(db_url, table_name):
    """Debug helper function to check table existence."""
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
            exists = result is not None
            logging.debug(f"[DEBUG] Table '{table_name}' exists: {exists}")
            return exists
    finally:
        await pool.close()