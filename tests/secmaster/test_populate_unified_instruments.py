import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from domains.instruments.services.populate_unified_instruments import populate_unified_instruments, parse_date

from domains.instruments.services.populate_unified_instruments import parse_date
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_batch_logic_mixed_existing_and_new():
    # Import batch_logic from the module (copy-paste or expose for test)
    from domains.instruments.services.populate_unified_instruments import batch_logic_for_test
    # Setup mocks
    polygon_dao = MagicMock()
    polygon_core.dao.get_instrument_by_symbol = AsyncMock(side_effect=lambda symbol: {
        'FOO': {'symbol': 'FOO', 'name': 'Foo Inc.', 'exchange': 'XNYS', 'type': 'CS', 'currency': 'USD', 'list_date': '2021-01-01', 'delist_date': None},
        'BAR': {'symbol': 'BAR', 'name': 'Bar Inc.', 'exchange': 'XNYS', 'type': 'CS', 'currency': 'USD', 'list_date': '2022-02-02', 'delist_date': None}
    }[symbol])
    instruments_dao = AsyncMock()
    xrefs_dao = AsyncMock()
    vendors_dao = AsyncMock()
    vendors_core.dao.get_vendor_by_name.return_value = {'id': 42}
    batch = ['FOO', 'BAR']
    debug = True

    # Xref: FOO exists, BAR does not
    async def resolve_instrument_id(symbol, vendor_id=None, at_date=None):
        if symbol == 'FOO':
            return 100
        return None
    xrefs_core.dao.resolve_instrument_id.side_effect = resolve_instrument_id

    async def create_instruments_batch(instruments, pool_min_size=1, pool_max_size=1):
        assert len(instruments) == 1 and instruments[0]['symbol'] == 'BAR'
        return [200]
    instruments_core.dao.create_instruments_batch.side_effect = create_instruments_batch

    async def get_instrument_by_symbol(symbol):
        if symbol == 'BAR':
            return {'id': 200}
        if symbol == 'FOO':
            return {'id': 100}
    instruments_core.dao.get_instrument_by_symbol.side_effect = get_instrument_by_symbol

    xrefs_core.dao.create_xrefs_batch = AsyncMock()

    # Call direct batch logic
    await batch_logic_for_test(batch, polygon_dao, instruments_dao, xrefs_dao, vendors_dao, debug)

    instruments_core.dao.create_instruments_batch.assert_awaited_once()
    xrefs_core.dao.create_xrefs_batch.assert_awaited_once()
    xref_args, xref_kwargs = xrefs_core.dao.create_xrefs_batch.await_args
    xref_batch = xref_args[0]
    assert len(xref_batch) == 1
    assert xref_batch[0]['symbol'] == 'BAR'
    assert xref_batch[0]['instrument_id'] == 200
    assert xref_batch[0]['vendor_id'] == 42
    assert xref_batch[0]['start_at'] == parse_date('2022-02-02')
    assert xref_batch[0]['end_at'] is None

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_batch_logic_skips_existing_xref():
    from domains.instruments.services.populate_unified_instruments import batch_logic_for_test
    polygon_dao = MagicMock()
    polygon_core.dao.get_instrument_by_symbol = AsyncMock(return_value={'symbol': 'BAR', 'name': 'Bar Inc.', 'exchange': 'XNYS', 'type': 'CS', 'currency': 'USD', 'list_date': '2022-02-02', 'delist_date': None})
    instruments_dao = AsyncMock()
    xrefs_dao = AsyncMock()
    vendors_dao = AsyncMock()
    vendors_core.dao.get_vendor_by_name.return_value = {'id': 1}
    # Simulate that xref already exists
    async def resolve_instrument_id(symbol, vendor_id=None, at_date=None):
        return 123  # already exists
    xrefs_core.dao.resolve_instrument_id.side_effect = resolve_instrument_id
    batch = ['BAR']
    debug = True

    # Call direct batch logic
    result = await batch_logic_for_test(batch, polygon_dao, instruments_dao, xrefs_dao, vendors_dao, debug)
    # Should not call create_instruments_batch or create_xrefs_batch
    instruments_core.dao.create_instruments_batch.assert_not_awaited()
    xrefs_core.dao.create_xrefs_batch.assert_not_awaited()
    # Should return 1 valid instrument processed
    assert result == 1
    instruments_core.dao.create_instrument.assert_not_called()
    xrefs_core.dao.create_xref.assert_not_called()
