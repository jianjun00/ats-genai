import pytest
import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch, MagicMock
from src.universe.universe_manager import UniverseManager
from src.universe.universe_manager import UniverseMembershipChange

@pytest.mark.asyncio
async def test_update_universe_membership_applies_changes(monkeypatch, test_env):
    # Mock environment and DB
    manager = UniverseManager(env=test_env)
    mock_conn = AsyncMock()
    class DummyConn:
        async def __aenter__(self): return mock_conn
        async def __aexit__(self, exc_type, exc, tb): pass
    class DummyPool:
        def acquire(self):
            return DummyConn()
        async def close(self): pass
    dummy_pool = DummyPool()
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=dummy_pool))
    manager._apply_single_membership_change = AsyncMock()
    changes = [MagicMock(spec=UniverseMembershipChange), MagicMock(spec=UniverseMembershipChange)]
    await manager.update_universe_membership(changes)
    assert manager._apply_single_membership_change.await_count == len(changes)

@pytest.mark.asyncio
async def test_update_universe_membership_no_changes(monkeypatch, test_env):
    manager = UniverseManager(env=test_env)
    # Should not call DB or _apply_single_membership_change
    manager._apply_single_membership_change = AsyncMock()
    await manager.update_universe_membership([])
    assert manager._apply_single_membership_change.await_count == 0

@pytest.mark.asyncio
async def test_get_members(monkeypatch, test_env):
    manager = UniverseManager(env=test_env)
    expected = ['AAPL', 'TSLA']
    manager.universe_db.get_universe_members = AsyncMock(return_value=expected)
    members = await manager.get_members(1, date(2025, 7, 15))
    assert members == expected

@pytest.mark.asyncio
async def test_update_for_eod(monkeypatch, test_env):
    manager = UniverseManager(env=test_env)
    # Simulate DB returning dicts as from DAO
    changes = [{
        'universe_id': 1,
        'symbol': 'AAPL',
        'action': 'add',
        'effective_date': '2025-07-15',
        'reason': 'test'
    }]
    manager.universe_db.get_membership_changes = AsyncMock(return_value=changes)
    manager.update_universe_membership = AsyncMock()
    class DummyRunner:
        pass
    import datetime
    await manager.update_for_eod(DummyRunner(), datetime.datetime(2025, 7, 15))
    manager.universe_db.get_membership_changes.assert_awaited_once()
    # update_universe_membership should be called with a list of UniverseMembershipChange objects
    args, kwargs = manager.update_universe_membership.call_args
    assert len(args) == 1
    membership_changes = args[0]
    assert len(membership_changes) == 1
    change = membership_changes[0]
    assert change.universe_id == 1
    assert change.symbol == 'AAPL'
    assert change.action == 'add'
    assert change.effective_date == '2025-07-15'
    assert change.reason == 'test'

@pytest.mark.asyncio
async def test_update_for_sod(monkeypatch, caplog, test_env):
    manager = UniverseManager(env=test_env)
    expected_ids = ['AAPL', 'TSLA', 'GOOG']
    # Patch get_members to return expected_ids
    manager.get_members = AsyncMock(return_value=expected_ids)
    class DummyRunner:
        pass
    import datetime
    with caplog.at_level('INFO'):
        await manager.update_for_sod(DummyRunner(), datetime.datetime(2025, 7, 24))
    # Assert instrument_ids is set
    assert hasattr(manager, 'instrument_ids')
    assert manager.instrument_ids == expected_ids
    # Assert logging
    assert any(f'UniverseManager.update_for_sod called for universe_id={manager.universe_id} at 2025-07-24' in r.message for r in caplog.records)
    assert any(f"UniverseManager.instrument_ids set to {expected_ids}" in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_update_for_sod_error(monkeypatch, test_env):
    manager = UniverseManager(env=test_env)
    manager.universe_id = 99  # Patch universe_id to valid value
    # Patch get_members to raise
    manager.get_members = AsyncMock(side_effect=RuntimeError('DB error'))
    class DummyRunner:
        pass
    import datetime
    with pytest.raises(RuntimeError, match='DB error'):
        await manager.update_for_sod(DummyRunner(), datetime.datetime(2025, 7, 24))
