import pytest
import asyncio
from unittest.mock import patch, MagicMock
from datetime import date
from vendor.polygon.services.range_splits_polygon import insert_splits_polygon, parse_date, date_chunks, main as range_main
from core.dao.stock_splits_polygon_dao import StockSplitsPolygonDAO

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_insert_splits_polygon_inserts_valid_splits():
    # Mock DAO
    class DummyDAO:
        def __init__(self):
            self.inserted = []
        async def insert_split(self, split):
            self.inserted.append(split)
    dao = DummyDAO()
    splits = [
        {
            'ticker': 'AAPL',
            'execution_date': '2023-01-15',
            'split_from': 4,
            'split_to': 1,
            'cash_amount': 0.0,
            'declaration_date': '2022-12-01',
            'payment_date': '2023-01-20',
            'record_date': '2023-01-18',
            'description': '4-for-1 split',
            'refid': 'split123',
        },
        # This split should be skipped (missing required fields)
        {
            'ticker': 'AAPL',
            'execution_date': None,
            'split_from': 2,
            'split_to': 1,
        }
    ]
    await insert_splits_polygon(splits, dao)
    assert len(core.dao.inserted) == 1
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

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_parse_date_handles_none_and_date():
    assert parse_date(None) is None
    d = date(2022, 1, 1)
    assert parse_date(d) == d
    assert parse_date('2022-01-01') == d

def test_date_chunks_basic():
    chunks = list(date_chunks('2022-01-01', '2022-01-15', chunk_days=5))
    assert chunks == [
        ('2022-01-01', '2022-01-05'),
        ('2022-01-06', '2022-01-10'),
        ('2022-01-11', '2022-01-15'),
    ]

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_chunked_fetch_and_aggregation(monkeypatch):
    calls = []
    def fake_fetch_splits_polygon(start, end, api_key):
        calls.append((start, end))
        # Return dummy splits with unique id per chunk
        return [{"ticker": f"TICK_{start}_{end}", "execution_date": start, "split_from": 2, "split_to": 1}]
    monkeypatch.setattr('secmaster.range_splits_polygon.fetch_splits_polygon', fake_fetch_splits_polygon)
    class DummyDAO:
        def __init__(self):
            self.inserted = []
        async def insert_split(self, split):
            self.inserted.append(split)
    # Simulate main logic for 2022-01-01 to 2022-01-10
    all_splits = []
    for chunk_start, chunk_end in date_chunks('2022-01-01', '2022-01-10', chunk_days=5):
        chunk_splits = fake_fetch_splits_polygon(chunk_start, chunk_end, 'dummy_key')
        all_splits.extend(chunk_splits)
    assert len(all_splits) == 2
    assert all_splits[0]['ticker'] == 'TICK_2022-01-01_2022-01-05'
    assert all_splits[1]['ticker'] == 'TICK_2022-01-06_2022-01-10'
    # Test that chunking called fetch with correct intervals
    assert calls == [
        ('2022-01-01', '2022-01-05'),
        ('2022-01-06', '2022-01-10'),
    ]
    # Test DAO insert
    dao = DummyDAO()
    await insert_splits_polygon(all_splits, dao)
    assert len(core.dao.inserted) == 2
