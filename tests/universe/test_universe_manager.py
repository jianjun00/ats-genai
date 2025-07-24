import pytest
import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch, MagicMock
from src.universe.universe_manager import UniverseManager
from src.universe.universe_manager import UniverseMembershipChange

@pytest.mark.asyncio
async def test_update_universe_membership_applies_changes(monkeypatch):
    # Mock environment and DB
    manager = UniverseManager()
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
async def test_update_universe_membership_no_changes(monkeypatch):
    manager = UniverseManager()
    # Should not call DB or _apply_single_membership_change
    manager._apply_single_membership_change = AsyncMock()
    await manager.update_universe_membership([])
    assert manager._apply_single_membership_change.await_count == 0

@pytest.mark.asyncio
async def test_get_members(monkeypatch):
    manager = UniverseManager()
    expected = ['AAPL', 'TSLA']
    manager.universe_db.get_universe_members = AsyncMock(return_value=expected)
    members = await manager.get_members(1, date(2025, 7, 15))
    assert members == expected

@pytest.mark.asyncio
async def test_update_for_eod(monkeypatch):
    manager = UniverseManager()
    changes = [MagicMock(spec=UniverseMembershipChange)]
    manager.universe_db.get_membership_changes = AsyncMock(return_value=changes)
    manager.update_universe_membership = AsyncMock()
    await manager.update_for_eod(1, date(2025, 7, 15))
    manager.universe_db.get_membership_changes.assert_awaited_once()
    manager.update_universe_membership.assert_awaited_once_with(changes)

@pytest.mark.asyncio
async def test_update_for_sod(monkeypatch, caplog):
    manager = UniverseManager()
    expected_ids = ['AAPL', 'TSLA', 'GOOG']
    # Patch get_members to return expected_ids
    manager.get_members = AsyncMock(return_value=expected_ids)
    # Track logs
    with caplog.at_level('INFO'):
        await manager.update_for_sod(42, date(2025, 7, 24))
    # Assert instrument_ids is set
    assert hasattr(manager, 'instrument_ids')
    assert manager.instrument_ids == expected_ids
    # Assert logging
    assert any('UniverseManager.update_for_sod called for universe_id=42 at 2025-07-24' in r.message for r in caplog.records)
    assert any(f"UniverseManager.instrument_ids set to {expected_ids}" in r.message for r in caplog.records)

@pytest.mark.asyncio
async def test_update_for_sod_error(monkeypatch):
    manager = UniverseManager()
    # Patch get_members to raise
    manager.get_members = AsyncMock(side_effect=RuntimeError('DB error'))
    with pytest.raises(RuntimeError, match='DB error'):
        await manager.update_for_sod(99, date(2025, 7, 24))
