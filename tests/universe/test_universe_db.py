import pytest
import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock
from src.universe.universe_db import UniverseDB

@pytest.mark.asyncio
async def test_get_universe_id_found(monkeypatch):
    db = UniverseDB()
    mock_universe = {'id': 123, 'name': 'TEST'}
    db.universe_dao = MagicMock()
    db.universe_dao.get_universe_by_name = AsyncMock(return_value=mock_universe)
    uid = await db.get_universe_id('TEST')
    assert uid == 123
    db.universe_dao.get_universe_by_name.assert_awaited_once_with('TEST')

@pytest.mark.asyncio
async def test_get_universe_id_not_found(monkeypatch):
    db = UniverseDB()
    db.universe_dao = MagicMock()
    db.universe_dao.get_universe_by_name = AsyncMock(return_value=None)
    uid = await db.get_universe_id('MISSING')
    assert uid is None

@pytest.mark.asyncio
async def test_get_universe_members(monkeypatch):
    db = UniverseDB()
    db.universe_membership_dao = MagicMock()
    db.universe_membership_dao.get_active_memberships = AsyncMock(return_value=[{'symbol': 'AAPL'}, {'symbol': 'TSLA'}])
    members = await db.get_universe_members(1, date(2025, 7, 24))
    assert members == ['AAPL', 'TSLA']
    db.universe_membership_dao.get_active_memberships.assert_awaited_once_with(1, date(2025, 7, 24))

@pytest.mark.asyncio
async def test_add_universe(monkeypatch):
    db = UniverseDB()
    db.universe_dao = MagicMock()
    db.universe_dao.create_universe = AsyncMock(return_value=42)
    uid = await db.add_universe('NEW', 'desc')
    assert uid == 42
    db.universe_dao.create_universe.assert_awaited_once_with('NEW', 'desc')

@pytest.mark.asyncio
async def test_add_universe_membership(monkeypatch):
    db = UniverseDB()
    db.universe_membership_dao = MagicMock()
    db.universe_membership_dao.add_membership_full = AsyncMock()
    await db.add_universe_membership(1, 'AAPL', date(2025, 7, 24), None)
    db.universe_membership_dao.add_membership_full.assert_awaited_once_with(1, 'AAPL', date(2025, 7, 24), None)

@pytest.mark.asyncio
async def test_update_universe_membership_end(monkeypatch):
    db = UniverseDB()
    db.universe_membership_dao = MagicMock()
    db.universe_membership_dao.update_membership_end = AsyncMock()
    await db.update_universe_membership_end(1, 'AAPL', date(2025, 7, 24))
    db.universe_membership_dao.update_membership_end.assert_awaited_once_with(1, 'AAPL', date(2025, 7, 24))
