import os
import pytest
import asyncio
import asyncpg
from pathlib import Path
import sys
import importlib.util

import aiohttp
from bs4 import BeautifulSoup

# Add src to path for environment configuration
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from config.environment import get_environment, set_environment, EnvironmentType

# Set integration environment for these tests
set_environment(EnvironmentType.INTEGRATION)

# Use the actual Wikipedia page for integration
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SCRIPT_PATH = Path(__file__).parent.parent.parent / "src/universe/spy_events_wiki.py"

@pytest.mark.asyncio
async def test_populate_spy_universe_events(tmp_path):
    env = get_environment()
    print(f"[DEBUG TEST] DB URL: {env.get_database_url()}")
    print(f"[DEBUG TEST] universe table: {env.get_table_name('universe')}")
    print(f"[DEBUG TEST] universe_membership table: {env.get_table_name('universe_membership')}")
    # Use a test universe name to avoid clobbering production data
    test_universe = "SPY_TEST_INTEGRATION"
    # Connect to DB
    pool = await asyncpg.create_pool(env.get_database_url(), min_size=1, max_size=2)
    async with pool.acquire() as conn:
        universe_membership_table = env.get_table_name("universe_membership")
        universe_table = env.get_table_name("universe")
        await conn.execute(f"DELETE FROM {universe_membership_table} WHERE universe_id IN (SELECT id FROM {universe_table} WHERE name = $1)", test_universe)
        await conn.execute(f"DELETE FROM {universe_table} WHERE name = $1", test_universe)
        # Reset the sequence so that the next insert uses the lowest available id
        # Set the sequence to max(id) or 1 to avoid duplicate key errors
        await conn.execute(f"SELECT setval('{universe_table}_id_seq', (SELECT COALESCE(MAX(id), 1) FROM {universe_table}))")
    await pool.close()
    # Dynamically import the script as a module
    spec = importlib.util.spec_from_file_location("spy_events_script", SCRIPT_PATH)
    spy_events_script = importlib.util.module_from_spec(spec)
    sys.modules["spy_events_script"] = spy_events_script
    spec.loader.exec_module(spy_events_script)
    # Run the main function with test universe and DB URL to avoid argparse
    # Patch sys.argv to avoid pytest argument clash
    import unittest.mock
    with unittest.mock.patch('sys.argv', ['spy_events_wiki.py']):
        await spy_events_script.main(db_url=env.get_database_url(), universe_name=test_universe, tickers=None, args=None)

    # Check DB for expected effects
    pool = await asyncpg.create_pool(env.get_database_url(), min_size=1, max_size=2)
    async with pool.acquire() as conn:
        universe_table = env.get_table_name("universe")
        universe_membership_table = env.get_table_name("universe_membership")
        universe_row = await conn.fetchrow(f"SELECT id FROM {universe_table} WHERE name = $1", test_universe)
        assert universe_row is not None, "Universe was not created"
        universe_id = universe_row['id']
        # Should have at least 100 membership intervals (usually >400)
        memberships = await conn.fetch(f"SELECT * FROM {universe_membership_table} WHERE universe_id = $1", universe_id)
        assert len(memberships) > 100, f"Expected >100 memberships, got {len(memberships)}"
        # Check that at least one membership has end_at IS NULL (currently active)
        open_members = [m for m in memberships if m['end_at'] is None]
        assert len(open_members) > 0, "No currently active members found"
        # Check that at least one membership has end_at NOT NULL (historically removed)
        closed_members = [m for m in memberships if m['end_at'] is not None]
        assert len(closed_members) > 0, "No removed members found"

        # --- Enhanced assertions for DDOG and TTD ---
        # DDOG: Added July 9, 2025, replacing JNPR
        # TTD: Added July 18, 2025, replacing ANSS
        ddog_add_date = '2025-07-09'
        ttd_add_date = '2025-07-18'
        ddog = next((m for m in memberships if m['symbol'] == 'DDOG'), None)
        ttd = next((m for m in memberships if m['symbol'] == 'TTD'), None)
        assert ddog is not None, 'DDOG should be in test universe membership'
        assert ttd is not None, 'TTD should be in test universe membership'
        assert str(ddog['start_at']) == ddog_add_date, f"DDOG start_at should be {ddog_add_date}, got {ddog['start_at']}"
        assert str(ttd['start_at']) == ttd_add_date, f"TTD start_at should be {ttd_add_date}, got {ttd['start_at']}"

        # The removed symbols on that date should have end_at set to the same date
        jnpr = next((m for m in memberships if m['symbol'] == 'JNPR'), None)
        anss = next((m for m in memberships if m['symbol'] == 'ANSS'), None)
        assert jnpr is not None, 'JNPR should have a removal record'
        assert anss is not None, 'ANSS should have a removal record'
        assert str(jnpr['end_at']) == ddog_add_date, f"JNPR end_at should be {ddog_add_date}, got {jnpr['end_at']}"
        assert str(anss['end_at']) == ttd_add_date, f"ANSS end_at should be {ttd_add_date}, got {anss['end_at']}"
    await pool.close()
