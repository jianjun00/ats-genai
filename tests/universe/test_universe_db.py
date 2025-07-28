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
    db.universe_membership_dao.add_membership_full.assert_awaited_once_with(universe_id=1, symbol='AAPL', start_at=date(2025, 7, 24), end_at=None)

from config.environment import Environment

@pytest.mark.asyncio
async def test_update_universe_membership_end(unit_test_db, monkeypatch):
    from config.environment import EnvironmentType
    env = Environment(EnvironmentType.TEST)
    # Patch get_database_url to return the test DB URL
    env.get_database_url = lambda: unit_test_db
    db = UniverseDB(env)
    db.universe_membership_dao = MagicMock()
    db.universe_membership_dao.update_membership_end = AsyncMock()
    # Patch InstrumentXrefsDAO at the import path actually used in the function
    mock_xrefs_dao = MagicMock()
    mock_xrefs_dao.resolve_instrument_id = AsyncMock(return_value=123)
    monkeypatch.setattr("dao.instrument_xrefs_dao.InstrumentXrefsDAO", lambda env: mock_xrefs_dao)
    await db.update_universe_membership_end(1, 'AAPL', date(2025, 7, 24))
    mock_xrefs_dao.resolve_instrument_id.assert_awaited_once_with('AAPL')
    db.universe_membership_dao.update_membership_end.assert_awaited_once_with(universe_id=1, instrument_id=123, end_at=date(2025, 7, 24))
