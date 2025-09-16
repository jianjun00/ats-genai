import pytest
import asyncpg
from secmaster import dividend_polygon


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_insert_dividends_polygon_inserts_correctly(unit_test_db):
    # Example Polygon dividends API response
    dividends = [
        {
            "ex_dividend_date": "2023-09-01",
            "ticker": "AAPL",
            "cash_amount": 0.22,
            "declaration_date": "2023-08-01",
            "payment_date": "2023-09-15",
            "record_date": "2023-09-10",
            "description": "Quarterly dividend",
            "refid": "654321"
        }
    ]
    ticker = "AAPL"
        # Patch: Inject DAO with test DB URL
    from dao.dividend_polygon_dao import DividendPolygonDAO
    from shared.utils.environment import Environment, EnvironmentType
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    dao = DividendPolygonDAO(env)
    await dividend_polygon.insert_dividends_polygon(dividends, ticker, dao=dao)

    # Query the test DB to verify insertion
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        table_name = env.get_table_name('dividend_polygon')
        rows = await conn.fetch(f"SELECT * FROM {table_name} WHERE symbol = $1", ticker)
        assert len(rows) == 1
        row = rows[0]
        assert row['symbol'] == 'AAPL'
        assert row['ex_dividend_date'].strftime('%Y-%m-%d') == '2023-09-01'
        assert row['cash_amount'] == 0.22
        assert row['declaration_date'].strftime('%Y-%m-%d') == '2023-08-01'
        assert row['payment_date'].strftime('%Y-%m-%d') == '2023-09-15'
        assert row['record_date'].strftime('%Y-%m-%d') == '2023-09-10'
        assert row['description'] == 'Quarterly dividend'
        assert row['refid'] == '654321'
    await pool.close()
