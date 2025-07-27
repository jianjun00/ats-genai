import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from secmaster.populate_unified_instruments import main as populate_main
from dao.instrument_xrefs_dao import InstrumentXrefsDAO
from dao.vendors_dao import VendorsDAO

@pytest.mark.asyncio
async def test_populate_unified_instruments_tickers_only(monkeypatch):
    # Mock DAOs
    mock_polygon = AsyncMock()
    mock_instruments = AsyncMock()
    mock_membership = AsyncMock()
    mock_xrefs = AsyncMock()
    mock_vendors = AsyncMock()
    tickers = ['AAPL', 'TSLA']
    # Mock instrument data
    mock_polygon.get_instrument.side_effect = lambda symbol: {
        'AAPL': {'symbol': 'AAPL', 'name': 'Apple', 'exchange': 'NASDAQ', 'type': 'CS', 'currency': 'USD'},
        'TSLA': {'symbol': 'TSLA', 'name': 'Tesla', 'exchange': 'NASDAQ', 'type': 'CS', 'currency': 'USD'}
    }.get(symbol)
    # Simulate xref exists for AAPL, not for TSLA
    mock_xrefs.find_xref.side_effect = lambda vendor_id, symbol: {'id': 123} if symbol == 'AAPL' else None
    # VendorsDAO returns dummy vendor
    mock_vendors.get_vendor_by_name.return_value = {'id': 1, 'name': 'Ticker'}
    with patch('secmaster.populate_unified_instruments.InstrumentPolygonDAO', return_value=mock_polygon), \
         patch('secmaster.populate_unified_instruments.InstrumentsDAO', return_value=mock_instruments), \
         patch('secmaster.populate_unified_instruments.UniverseMembershipDAO', return_value=mock_membership), \
         patch('secmaster.populate_unified_instruments.InstrumentXrefsDAO', return_value=mock_xrefs), \
         patch('secmaster.populate_unified_instruments.VendorsDAO', return_value=mock_vendors):
        await populate_main('test', tickers=tickers, universe_id=None)
    # Should only create instrument for TSLA
    assert mock_instruments.create_instrument.await_count == 1
    called_symbols = [call.kwargs['symbol'] for call in mock_instruments.create_instrument.call_args_list]
    assert called_symbols == ['TSLA']
    # Should call create_xref for TSLA only
    assert mock_xrefs.create_xref.await_count == 1
    xref_call = mock_xrefs.create_xref.call_args_list[0].kwargs
    assert xref_call['symbol'] == 'TSLA'
    assert xref_call['vendor_id'] == 1

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
