import pytest
import asyncpg
from secmaster import splits_polygon


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_insert_splits_polygon_inserts_correctly(unit_test_db):
    # Example Polygon splits API response
    splits = [
        {
            "execution_date": "2023-09-01",
            "ticker": "AAPL",
            "split_from": 2,
            "split_to": 3,
            "cash_amount": None,
            "declaration_date": "2023-08-01",
            "payment_date": "2023-09-15",
            "record_date": "2023-09-10",
            "description": "3-for-2 split",
            "refid": "123456"
        }
    ]
    ticker = "AAPL"
        # Patch: Inject DAO with test DB URL
    from dao.stock_splits_polygon_dao import StockSplitsPolygonDAO
    from shared.utils.environment import Environment, EnvironmentType
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    dao = StockSplitsPolygonDAO(env)
    await splits_polygon.insert_splits_polygon(splits, ticker, dao=dao)

    # Query the test DB to verify insertion
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        table_name = env.get_table_name('stock_splits_polygon')
        rows = await conn.fetch(f"SELECT * FROM {table_name} WHERE symbol = $1", ticker)
        assert len(rows) == 1
        row = rows[0]
        assert row['symbol'] == 'AAPL'
        assert row['execution_date'].strftime('%Y-%m-%d') == '2023-09-01'
        assert row['split_from'] == 2
        assert row['split_to'] == 3
        assert row['cash_amount'] is None
        assert row['declaration_date'].strftime('%Y-%m-%d') == '2023-08-01'
        assert row['payment_date'].strftime('%Y-%m-%d') == '2023-09-15'
        assert row['record_date'].strftime('%Y-%m-%d') == '2023-09-10'
        assert row['description'] == '3-for-2 split'
        assert row['refid'] == '123456'
    await pool.close()
