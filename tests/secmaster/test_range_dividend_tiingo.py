import pytest
import asyncio
from datetime import date
from secmaster.range_dividend_tiingo import parse_date, map_tiingo_dividend, insert_dividends_tiingo

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
    assert len(dao.inserted) == 1
    d = dao.inserted[0]
    assert d['symbol'] == 'AAPL'
    assert d['ex_dividend_date'] == date(2023, 1, 15)
    assert d['cash_amount'] == 0.22
    assert d['declaration_date'] == date(2022, 12, 1)
    assert d['payment_date'] == date(2023, 1, 20)
    assert d['record_date'] == date(2023, 1, 18)
    assert d['description'] == 'Quarterly dividend'
    assert d['refid'] == 'tiingo123'
