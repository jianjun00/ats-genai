import pytest
import asyncio
import asyncpg
import sys
import os
from pathlib import Path

SCRIPT_PATH = Path(__file__).parent.parent.parent / "src/secmaster/populate_unified_instruments.py"

@pytest.mark.asyncio
async def test_populate_unified_instruments_integration():
    # Setup: use integration environment
    from config.environment import set_environment, EnvironmentType, get_environment
    set_environment(EnvironmentType.INTEGRATION)
    env = get_environment()
    # Pick a test ticker that exists in intg_instrument_polygon
    test_ticker = 'AAPL'
    # Remove from intg_instruments if present
    pool = await asyncpg.create_pool(env.get_database_url())
    instruments_table = env.get_table_name('instruments')
    await pool.execute(f"DELETE FROM {instruments_table} WHERE symbol = $1", test_ticker)
    await pool.close()
    # Run script as subprocess
    import subprocess
    cmd = [sys.executable, str(SCRIPT_PATH), '--environment', 'intg', '--tickers', test_ticker]
    env_vars = os.environ.copy()
    env_vars['PYTHONPATH'] = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src'))
    result = subprocess.run(cmd, env=env_vars, capture_output=True, text=True)
    print("[SCRIPT STDOUT]", result.stdout)
    print("[SCRIPT STDERR]", result.stderr)
    assert result.returncode == 0
    # Check that AAPL now exists in intg_instruments
    pool = await asyncpg.create_pool(env.get_database_url())
    row = await pool.fetchrow(f"SELECT * FROM {instruments_table} WHERE symbol = $1", test_ticker)
    await pool.close()
    assert row is not None
    assert row['symbol'] == test_ticker
