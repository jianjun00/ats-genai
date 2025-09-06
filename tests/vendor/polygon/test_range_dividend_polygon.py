import pytest
import asyncio
from datetime import date
from vendor.polygon.services.range_dividend_polygon import parse_date, date_chunks, insert_dividends_polygon

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
async def test_insert_dividends_polygon_inserts_valid_dividends():
    class DummyDAO:
        def __init__(self):
            self.inserted = []
        async def insert_dividend(self, div):
            self.inserted.append(div)
    dao = DummyDAO()
    dividends = [
        {
            'ticker': 'AAPL',
            'ex_dividend_date': '2023-01-15',
            'cash_amount': 0.22,
            'declaration_date': '2022-12-01',
            'payment_date': '2023-01-20',
            'record_date': '2023-01-18',
            'description': 'Quarterly dividend',
            'refid': 'div123',
        },
        # Should be skipped (missing required fields)
        {
            'ticker': 'AAPL',
            'ex_dividend_date': None,
            'cash_amount': 0.22,
        }
    ]
    await insert_dividends_polygon(dividends, dao)
    assert len(dao.inserted) == 1
    d = dao.inserted[0]
    assert d['symbol'] == 'AAPL'
    assert d['ex_dividend_date'] == date(2023, 1, 15)
    assert d['cash_amount'] == 0.22
    assert d['declaration_date'] == date(2022, 12, 1)
    assert d['payment_date'] == date(2023, 1, 20)
    assert d['record_date'] == date(2023, 1, 18)
    assert d['description'] == 'Quarterly dividend'
    assert d['refid'] == 'div123'
