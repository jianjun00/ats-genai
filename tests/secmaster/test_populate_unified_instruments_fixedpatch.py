import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from secmaster.populate_unified_instruments import main as populate_main

@pytest.mark.asyncio
async def test_populate_unified_instruments_tickers_only(monkeypatch):
    # Mock DAOs
    mock_polygon = AsyncMock()
    mock_instruments = AsyncMock()
    mock_membership = AsyncMock()
    # Only tickers provided, no universe_id
    tickers = ['AAPL', 'TSLA']
    # Mock instrument data
    mock_polygon.get_instrument.side_effect = lambda symbol: {
        'AAPL': {'symbol': 'AAPL', 'name': 'Apple', 'exchange': 'NASDAQ', 'type': 'CS', 'currency': 'USD'},
        'TSLA': {'symbol': 'TSLA', 'name': 'Tesla', 'exchange': 'NASDAQ', 'type': 'CS', 'currency': 'USD'}
    }.get(symbol)
    with patch('secmaster.populate_unified_instruments.InstrumentPolygonDAO', return_value=mock_polygon), \
         patch('secmaster.populate_unified_instruments.InstrumentsDAO', return_value=mock_instruments), \
         patch('secmaster.populate_unified_instruments.UniverseMembershipDAO', return_value=mock_membership):
        await populate_main('test', tickers=tickers, universe_id=None)
    # Check inserts called
    assert mock_instruments.create_instrument.await_count == 2
    called_symbols = [call.kwargs['symbol'] for call in mock_instruments.create_instrument.call_args_list]
    assert set(called_symbols) == set(tickers)

@pytest.mark.asyncio
async def test_populate_unified_instruments_universe(monkeypatch):
    # Mock DAOs
    mock_polygon = AsyncMock()
    mock_instruments = AsyncMock()
    mock_membership = AsyncMock()
    tickers = None
    # Membership DAO returns tickers
    mock_membership.get_memberships_by_universe.return_value = [
        {'symbol': 'AAPL'}, {'symbol': 'TSLA'}
    ]
    mock_polygon.get_instrument.side_effect = lambda symbol: {
        'AAPL': {'symbol': 'AAPL', 'name': 'Apple', 'exchange': 'NASDAQ', 'type': 'CS', 'currency': 'USD'},
        'TSLA': {'symbol': 'TSLA', 'name': 'Tesla', 'exchange': 'NASDAQ', 'type': 'CS', 'currency': 'USD'}
    }.get(symbol)
    with patch('secmaster.populate_unified_instruments.InstrumentPolygonDAO', return_value=mock_polygon), \
         patch('secmaster.populate_unified_instruments.InstrumentsDAO', return_value=mock_instruments), \
         patch('secmaster.populate_unified_instruments.UniverseMembershipDAO', return_value=mock_membership):
        await populate_main('test', tickers=tickers, universe_id=123)
    # Check inserts called
    assert mock_instruments.create_instrument.await_count == 2
    called_symbols = [call.kwargs['symbol'] for call in mock_instruments.create_instrument.call_args_list]
    assert set(called_symbols) == {'AAPL', 'TSLA'}
