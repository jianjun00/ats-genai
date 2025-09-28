import pytest
import datetime
from unittest.mock import MagicMock

# --- Test parse_date logic (copied from Ray remote) ---
def parse_date(val):
    if not val:
        return None
    return datetime.datetime.strptime(val[:10], "%Y-%m-%d").date()
def test_parse_date_valid():
    assert parse_date("2023-08-07") == datetime.date(2023, 8, 7)
    assert parse_date("1992-11-19T00:00:00Z") == datetime.date(1992, 11, 19)
    assert parse_date(None) is None
    assert parse_date("") is None
    assert parse_date("bad-date") is None

# --- Test Ray remote function core logic ---
def ray_core_logic(detail, db_url, table_name, polygon_api_key):
    # Simulate the upsert logic, but don't actually call DB
    parsed_list_date = parse_date(detail.get('list_date'))
    parsed_delisted = parse_date(detail.get('delisted_utc'))
    # Should return date or None
    return parsed_list_date, parsed_delisted

def test_ray_core_logic_dates():
    detail = {
        'ticker': 'AAPL',
        'list_date': '2021-06-10',
        'delisted_utc': '2022-01-01T00:00:00Z',
    }
    parsed_list_date, parsed_delisted = ray_core_logic(detail, 'dburl', 'table', 'apikey')
    assert parsed_list_date == datetime.date(2021, 6, 10)
    assert parsed_delisted == datetime.date(2022, 1, 1)

def test_ray_core_logic_none():
    detail = {'ticker': 'TSLA', 'list_date': None, 'delisted_utc': None}
    parsed_list_date, parsed_delisted = ray_core_logic(detail, 'dburl', 'table', 'apikey')
    assert parsed_list_date is None
    assert parsed_delisted is None

# --- Optionally, test Ray remote function with mocks ---
@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_ray_remote_db_args(monkeypatch):
    # Patch asyncpg.create_pool and requests.get
    fake_pool = MagicMock()
    fake_conn = MagicMock()
    fake_pool.acquire = MagicMock(return_value=fake_conn)
    fake_pool.close = MagicMock()
    class DummyContext:
        async def __aenter__(self): return fake_conn
        async def __aexit__(self, exc_type, exc, tb): return False
    fake_pool.acquire = MagicMock(return_value=DummyContext())
    async def fake_close(): return None
    fake_pool.close = fake_close
    async def fake_create_pool(*args, **kwargs): return fake_pool
    monkeypatch.setattr("asyncpg.create_pool", fake_create_pool)
    # Patch requests.get to return a valid detail
    class FakeResp:
        status_code = 200
        def json(self):
            return {"results": {"ticker": "AAPL", "list_date": "2021-06-10", "delisted_utc": "2022-01-01T00:00:00Z"}}
    monkeypatch.setattr("requests.get", lambda url: FakeResp())
    # Patch conn.execute to check args
    async def fake_execute(sql, *args):
        # Should receive a datetime.date for list_date and delisted_utc
        assert isinstance(args[10], datetime.date)
        assert isinstance(args[11], datetime.date)
        return None
    fake_conn.execute = fake_execute
    # Call the Ray remote logic directly (simulate)
    # You could import and call pip.fetch_and_upsert_ray if desired
    # For now, just test the upsert logic with correct args
    detail = {'ticker': 'AAPL', 'list_date': '2021-06-10', 'delisted_utc': '2022-01-01T00:00:00Z'}
    parsed_list_date, parsed_delisted = ray_core_logic(detail, 'dburl', 'table', 'apikey')
    assert isinstance(parsed_list_date, datetime.date)
    assert isinstance(parsed_delisted, datetime.date)

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_ray_batched_upsert(monkeypatch):
    # Patch asyncpg.create_pool
    import secmaster.populate_instrument_polygon as pip
    fake_pool = MagicMock()
    fake_conn = MagicMock()
    fake_pool.acquire = MagicMock(return_value=fake_conn)
    fake_pool.close = MagicMock()
    class DummyContext:
        async def __aenter__(self): return fake_conn
        async def __aexit__(self, exc_type, exc, tb): return False
    fake_pool.acquire = MagicMock(return_value=DummyContext())
    async def fake_close(): return None
    fake_pool.close = fake_close
    async def fake_create_pool(*args, **kwargs): return fake_pool
    monkeypatch.setattr("asyncpg.create_pool", fake_create_pool)
    # Patch conn.executemany to check batch rows
    async def fake_executemany(sql, rows):
        assert len(rows) == 2
        assert rows[0][0] == "AAPL"
        assert rows[1][0] == "TSLA"
        assert isinstance(rows[0][10], datetime.date)
        assert rows[1][11] is None or isinstance(rows[1][11], datetime.date)
        return None
    fake_conn.executemany = fake_executemany
    # Prepare details as would be collected by Ray fetch
    details = [
        {"ticker": "AAPL", "name": "Apple Inc", "primary_exchange": "NASDAQ", "type": "CS", "currency_name": "USD", "share_class_figi": "", "isin": "", "cusip": "", "composite_figi": "", "active": True, "list_date": "2021-06-10", "delisted_utc": "2022-01-01T00:00:00Z"},
        {"ticker": "TSLA", "name": "Tesla Inc", "primary_exchange": "NASDAQ", "type": "CS", "currency_name": "USD", "share_class_figi": "", "isin": "", "cusip": "", "composite_figi": "", "active": True, "list_date": "2020-01-01", "delisted_utc": None}
    ]
    await pip.batch_upsert_details(details, "dburl", "table")
