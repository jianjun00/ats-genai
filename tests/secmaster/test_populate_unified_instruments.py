import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from secmaster.populate_unified_instruments import populate_unified_instruments, parse_date

@pytest.mark.asyncio
async def test_populate_unified_instruments_inserts_new_instruments(monkeypatch):
    # Setup mocks
    polygon_dao = MagicMock()
    polygon_dao.get_all_symbols = AsyncMock(return_value=['FOO'])
    polygon_dao.get_instrument_by_symbol = AsyncMock(return_value={
        'symbol': 'FOO',
        'name': 'Foo Inc.',
        'exchange': 'XNYS',
        'type': 'CS',
        'currency': 'USD',
        'list_date': '2021-01-01',
        'delist_date': None,
    })
    instruments_dao = AsyncMock()
    xrefs_dao = AsyncMock()
    vendors_dao = AsyncMock()

    # Vendors
    vendors_dao.get_vendor_by_name.return_value = {'id': 42}

    # Xref does not exist
    xrefs_dao.find_xref.return_value = None
    # Instrument creation returns instrument id
    instruments_dao.create_instrument.return_value = 123

    # Run
    await populate_unified_instruments(
        polygon_dao, instruments_dao, xrefs_dao, vendors_dao, tickers=['FOO'], debug=True
    )

    # Assert instrument created
    instruments_dao.create_instrument.assert_awaited_once_with(
        symbol='FOO', name='Foo Inc.', exchange='XNYS', type_='CS', currency='USD', list_date='2021-01-01', delist_date=None
    )
    # Assert xref created with correct parsed dates
    xrefs_dao.create_xref.assert_awaited_once()
    args, kwargs = xrefs_dao.create_xref.await_args
    assert kwargs['instrument_id'] == 123
    assert kwargs['vendor_id'] == 42
    assert kwargs['symbol'] == 'FOO'
    assert kwargs['start_at'] == parse_date('2021-01-01')
    assert kwargs['end_at'] is None

@pytest.mark.asyncio
async def test_populate_unified_instruments_skips_existing_xref():
    polygon_dao = MagicMock()
    polygon_dao.get_all_symbols = AsyncMock(return_value=['BAR'])
    polygon_dao.get_instrument_by_symbol = AsyncMock()
    instruments_dao = AsyncMock()
    xrefs_dao = AsyncMock()
    vendors_dao = AsyncMock()
    vendors_dao.get_vendor_by_name.return_value = {'id': 1}
    xrefs_dao.find_xref.return_value = {'symbol': 'BAR'}  # Pretend xref exists

    await populate_unified_instruments(
        polygon_dao, instruments_dao, xrefs_dao, vendors_dao, tickers=['BAR'], debug=True
    )
    instruments_dao.create_instrument.assert_not_called()
    xrefs_dao.create_xref.assert_not_called()
