import os
import sys
import subprocess
import asyncpg
import pytest

SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/universe/populate_spy_universe_events_from_wikipedia.py'))

TICKERS_TO_CHECK = ["JNPR", "TSLA", "AAPL", "TTD", "HOOD", "ANSS"]

@pytest.mark.asyncio
async def test_populate_spy_universe_events_and_membership(tmp_path):
    # Use integration environment
    cmd = [sys.executable, SCRIPT_PATH, '--universe_name', 'SPY', '--environment', 'intg', '--tickers', ','.join(TICKERS_TO_CHECK)]
    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src'))
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[SCRIPT STDOUT]\n{result.stdout}")
        print(f"[SCRIPT STDERR]\n{result.stderr}")
    assert result.returncode == 0, f"Script failed: {result.stderr}"
    assert "Done populating SPY universe membership events." in result.stdout
    # Connect to integration DB and check universe_membership and universe_membership_changes
    from config.environment import get_environment, set_environment, EnvironmentType
    set_environment(EnvironmentType.INTEGRATION)
    env_obj = get_environment()
    db_url = env_obj.get_database_url()
    universe_table = env_obj.get_table_name('universe')
    membership_table = env_obj.get_table_name('universe_membership')
    changes_table = env_obj.get_table_name('universe_membership_changes')
    pool = await asyncpg.create_pool(db_url)
    async with pool.acquire() as conn:
        # Get universe_id for SPY
        row = await conn.fetchrow(f"SELECT id FROM {universe_table} WHERE name = $1", "SPY")
        assert row, "SPY universe not found"
        universe_id = row['id']
        # Check membership for each ticker
        # Debug: print all memberships for this universe
        all_memberships = await conn.fetch(f"SELECT * FROM {membership_table} WHERE universe_id = $1", universe_id)
        print(f"[DEBUG] All memberships for universe_id={universe_id}: {all_memberships}")
        for ticker in TICKERS_TO_CHECK:
            m_rows = await conn.fetch(f"SELECT * FROM {membership_table} WHERE universe_id = $1 AND symbol = $2", universe_id, ticker)
            print(f"[DEBUG] Membership rows for {ticker}: {m_rows}")
            if ticker == "AAPL":
                aapl_events = await conn.fetch(f"SELECT * FROM {changes_table} WHERE symbol = $1", "AAPL")
                print(f"[DEBUG] All membership event rows for AAPL: {aapl_events}")
            assert m_rows, f"No membership found for {ticker}"
            c_rows = await conn.fetch(f"SELECT * FROM {changes_table} WHERE symbol = $1", ticker)
            print(f"[DEBUG] Membership event rows for {ticker}: {c_rows}")
            assert c_rows, f"No membership event found for {ticker}"
    await pool.close()
