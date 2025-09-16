import pytest
from datetime import date
from vendor.tiingo.services.range_dividend_tiingo import parse_date, map_tiingo_dividend, insert_dividends_tiingo

from vendor.tiingo.services.range_dividend_tiingo import get_symbols_from_dividend_polygon

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_get_symbols_from_dividend_polygon_parses_dates(tmp_path):
    # Setup: create a test DB and table
    import uuid
    db_name = f"test_db_{uuid.uuid4().hex[:8]}"
    db_url = f"postgresql://localhost/{db_name}"
    # For real test, use a test DB fixture; here, just check type conversion and SQL
    class DummyEnv:
        def get_database_url(self):
            return "postgresql://user:pass@localhost:5432/fake"  # Not actually used
        def get_table_name(self, name):
            return "dividend_polygon"
    class DummyConn:
        def __init__(self):
            self.last_args = None
        async def fetch(self, query, start, end):
            self.last_args = (query, start, end)
            return [{"symbol": "AAPL"}, {"symbol": "MSFT"}]
    class DummyAcquireCtx:
        async def __aenter__(self):
            return DummyConn()
        async def __aexit__(self, exc_type, exc, tb):
            pass
    class DummyPool:
        def acquire(self):
            return DummyAcquireCtx()
        async def close(self):
            pass
    async def dummy_create_pool(db_url):
        return DummyPool()
    import secmaster.range_dividend_tiingo as mod
    orig_create_pool = mod.asyncpg.create_pool
    mod.asyncpg.create_pool = dummy_create_pool
    try:
        env = DummyEnv()
        # Pass string dates, should be parsed to date
        symbols = await get_symbols_from_dividend_polygon(env, "2022-01-01", "2022-12-31")
        assert symbols == ["AAPL", "MSFT"]
    finally:
        mod.asyncpg.create_pool = orig_create_pool

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_parse_date_handles_none_and_date():
    assert parse_date(None) is None
    d = date(2022, 1, 1)
    assert parse_date(d) == d
    assert parse_date('2022-01-01') == d

def test_map_tiingo_dividend_basic():
    div = {
        'ticker': 'AAPL',
        'exDate': '2023-01-15',
        'cashAmount': 0.22,
        'declarationDate': '2022-12-01',
        'payDate': '2023-01-20',
        'recordDate': '2023-01-18',
        'description': 'Quarterly dividend',
        'id': 'tiingo123',
    }
    mapped = map_tiingo_dividend(div)
    assert mapped['symbol'] == 'AAPL'
    assert mapped['ex_dividend_date'] == date(2023, 1, 15)
    assert mapped['cash_amount'] == 0.22
    assert mapped['declaration_date'] == date(2022, 12, 1)
    assert mapped['payment_date'] == date(2023, 1, 20)
    assert mapped['record_date'] == date(2023, 1, 18)
    assert mapped['description'] == 'Quarterly dividend'
    assert mapped['refid'] == 'tiingo123'

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_insert_dividends_tiingo_inserts_valid_dividends():
    class DummyDAO:
        def __init__(self):
            self.inserted = []
        async def insert_dividend(self, div):
            self.inserted.append(div)
    dao = DummyDAO()
    dividends = [
        {
            'ticker': 'AAPL',
            'exDate': '2023-01-15',
            'cashAmount': 0.22,
            'declarationDate': '2022-12-01',
            'payDate': '2023-01-20',
            'recordDate': '2023-01-18',
            'description': 'Quarterly dividend',
            'id': 'tiingo123',
        },
        # Should be skipped (missing required fields)
        {
            'ticker': 'AAPL',
            'exDate': None,
            'cashAmount': 0.22,
        }
    ]
    await insert_dividends_tiingo(dividends, dao)
    assert len(core.dao.inserted) == 1
    d = dao.inserted[0]
    assert d['symbol'] == 'AAPL'
    assert d['ex_dividend_date'] == date(2023, 1, 15)
    assert d['cash_amount'] == 0.22
    assert d['declaration_date'] == date(2022, 12, 1)
    assert d['payment_date'] == date(2023, 1, 20)
    assert d['record_date'] == date(2023, 1, 18)
    assert d['description'] == 'Quarterly dividend'
    assert d['refid'] == 'tiingo123'
