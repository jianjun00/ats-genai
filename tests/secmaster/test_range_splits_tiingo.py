import pytest
import asyncio
from datetime import date
from secmaster.range_splits_tiingo import parse_date, map_tiingo_split, insert_splits_tiingo

import types
from secmaster.range_splits_tiingo import get_symbols_from_stock_splits_polygon

@pytest.mark.asyncio
async def test_get_symbols_from_stock_splits_polygon_parses_dates():
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
    import secmaster.range_splits_tiingo as mod
    orig_create_pool = mod.asyncpg.create_pool
    mod.asyncpg.create_pool = dummy_create_pool
    try:
        class DummyEnv:
            def get_database_url(self):
                return "postgresql://user:pass@localhost:5432/fake"
            def get_table_name(self, name):
                return "stock_splits_polygon"
        env = DummyEnv()
        symbols = await get_symbols_from_stock_splits_polygon(env, "2022-01-01", "2022-12-31")
        assert symbols == ["AAPL", "MSFT"]
    finally:
        mod.asyncpg.create_pool = orig_create_pool

@pytest.mark.asyncio
async def test_parse_date_handles_none_and_date():
    assert parse_date(None) is None
    d = date(2022, 1, 1)
    assert parse_date(d) == d
    assert parse_date('2022-01-01') == d

def test_map_tiingo_split_basic():
    split = {
        'ticker': 'AAPL',
        'executionDate': '2023-01-15',
        'fromFactor': 4,
        'toFactor': 1,
        'cashAmount': 0.0,
        'declarationDate': '2022-12-01',
        'payDate': '2023-01-20',
        'recordDate': '2023-01-18',
        'description': '4-for-1 split',
        'id': 'split123',
    }
    mapped = map_tiingo_split(split)
    assert mapped['symbol'] == 'AAPL'
    assert mapped['execution_date'] == date(2023, 1, 15)
    assert mapped['split_from'] == 4
    assert mapped['split_to'] == 1
    assert mapped['cash_amount'] == 0.0
    assert mapped['declaration_date'] == date(2022, 12, 1)
    assert mapped['payment_date'] == date(2023, 1, 20)
    assert mapped['record_date'] == date(2023, 1, 18)
    assert mapped['description'] == '4-for-1 split'
    assert mapped['refid'] == 'split123'

@pytest.mark.asyncio
async def test_insert_splits_tiingo_inserts_valid_splits():
    class DummyDAO:
        def __init__(self):
            self.inserted = []
        async def insert_split(self, split):
            self.inserted.append(split)
    dao = DummyDAO()
    splits = [
        {
            'ticker': 'AAPL',
            'executionDate': '2023-01-15',
            'fromFactor': 4,
            'toFactor': 1,
            'cashAmount': 0.0,
            'declarationDate': '2022-12-01',
            'payDate': '2023-01-20',
            'recordDate': '2023-01-18',
            'description': '4-for-1 split',
            'id': 'split123',
        },
        # Should be skipped (missing required fields)
        {
            'ticker': 'AAPL',
            'executionDate': None,
            'fromFactor': 2,
            'toFactor': 1,
        }
    ]
    await insert_splits_tiingo(splits, dao)
    assert len(dao.inserted) == 1
    s = dao.inserted[0]
    assert s['symbol'] == 'AAPL'
    assert s['execution_date'] == date(2023, 1, 15)
    assert s['split_from'] == 4
    assert s['split_to'] == 1
    assert s['cash_amount'] == 0.0
    assert s['declaration_date'] == date(2022, 12, 1)
    assert s['payment_date'] == date(2023, 1, 20)
    assert s['record_date'] == date(2023, 1, 18)
    assert s['description'] == '4-for-1 split'
    assert s['refid'] == 'split123'
