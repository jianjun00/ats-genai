import os
import pytest
import requests
from unittest.mock import patch, MagicMock
from datetime import datetime, date
from domains.market_data.services.agent.polygon_adapter import PolygonAdapter
from domains.market_data.services.agent.models import InstrumentMetadata, EODPrice
import responses

@responses.activate
def test_fetch_instruments_parsing():
    api_key = "fakekey"
    adapter = PolygonAdapter(api_key=api_key)
    fake_response = {
        "results": [
            {
                "ticker": "AAPL",
                "name": "Apple Inc.",
                "primary_exchange": "NASDAQ",
                "sic_description": "Technology",
                "list_date": "1980-12-12",
                "delisted_utc": None,
                "cik": "0000320193"
            }
        ]
    }
    responses.add(
        responses.GET,
        f"https://api.polygon.io/v3/reference/tickers?active=true&apiKey={api_key}",
        json=fake_response,
        status=200
    )
    instruments = adapter.fetch_instruments()
    assert len(instruments) == 1
    m = instruments[0]
    assert isinstance(m, InstrumentMetadata)
    assert m.symbol == "AAPL"
    assert m.name == "Apple Inc."
    assert m.exchange == "NASDAQ"
    assert m.sector == "Technology"
    from datetime import date
    assert m.list_date == date(1980, 12, 12)

@responses.activate
def test_fetch_eod_parsing():
    api_key = "fakekey"
    adapter = PolygonAdapter(api_key=api_key)
    fake_eod = {
        "results": [
            {
                "t": 1690848000000,  # 2023-08-01 in ms
                "o": 190.0,
                "h": 195.0,
                "l": 189.0,
                "c": 192.0,
                "v": 1000000
            }
        ]
    }
    responses.add(
        responses.GET,
        "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2023-08-01/2023-08-01?adjusted=true&sort=asc&limit=50000&apiKey=fakekey",
        json=fake_eod,
        status=200
    )
    eod_prices = adapter.fetch_eod(["AAPL"], "2023-08-01", "2023-08-01")
    assert len(eod_prices) == 1
    p = eod_prices[0]
    assert isinstance(p, EODPrice)
    assert p.instrument_id == "AAPL"
    assert p.date == date(2023, 8, 1)
    assert p.open == 190.0
    assert p.close == 192.0
    assert p.volume == 1000000
