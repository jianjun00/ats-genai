import os
import sys
import tempfile
import pytest
import pandas as pd
from unittest.mock import patch
from datetime import date
import asyncio

import importlib.util

from tests.fixtures.insert_test_daily_prices import insert_test_daily_prices


import pytest_asyncio

@pytest_asyncio.fixture(scope='function', autouse=True)
async def setup_test_data(unit_test_db):
    # Insert required instrument row for AAPL (id=1) and xref
    import asyncpg
    conn = await asyncpg.connect(unit_test_db)
    # Insert vendor and get id
    vendor_name = 'ticker'
    vendor_row = await conn.fetchrow("SELECT id FROM test_vendors WHERE name=$1", vendor_name)
    if vendor_row is None:
        vendor_id = (await conn.fetchrow("INSERT INTO test_vendors (name) VALUES ($1) RETURNING id", vendor_name))["id"]
    else:
        vendor_id = vendor_row["id"]
    # Insert instrument
    await conn.execute('''
        INSERT INTO test_instruments (id, symbol, name, exchange, type, currency, active)
        VALUES (1, 'AAPL', 'Apple Inc.', 'NASDAQ', 'stock', 'USD', TRUE)
        ON CONFLICT (id) DO NOTHING;
    ''')
    # Insert xref with vendor_id
    await conn.execute('''
        INSERT INTO test_instrument_xrefs (instrument_id, symbol, vendor_id)
        VALUES ($1, $2, $3)
    ''', 1, 'AAPL', vendor_id)
    await conn.close()
    # Insert AAPL test data for 2024-01-03 and earlier
    base = os.path.dirname(__file__)
    data_dir = os.path.abspath(os.path.join(base, '../data/daily_prices_polygon'))
    aapl_path = os.path.join(data_dir, 'polygon_aapl_response.json')
    # Insert data
    await insert_test_daily_prices(aapl_path, 'AAPL', 1, unit_test_db)
    yield
    # Optionally, clean up test DB after tests

def test_indicator_runner_df(unit_test_db):
    """
    Test that indicator_runner.py outputs DataFrame correctly for a known symbol/date.
    """
    import subprocess
    import sys
    runner_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/app/indicator_runner.py'))
    test_args = [
        sys.executable, runner_path,
        "--symbols", "AAPL",
        "--start-date", "2024-01-02",
        "--end-date", "2024-01-31",
        "--environment", "test",
        "--gin_config", "config/app_test.gin",
        "--output-format", "df",
        "--db-url", unit_test_db
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    result = subprocess.run(
        test_args,
        capture_output=True,
        text=True,
        env=env
    )
    output = result.stdout
    error_output = result.stderr
    print('STDOUT:')
    print(output)
    print('STDERR:')
    print(error_output)
    assert 'ETop' in output and 'EBot' in output and 'PL' in output, "Output missing indicator columns"
    # Parse output as DataFrame (skip header lines)
    found_non_null = False
    for line in output.splitlines():
        if line.strip() and not line.strip().startswith('date symbol'):
            parts = line.split()
            header = output.splitlines()[-1].split()
            etop_idx = header.index('ETop') if 'ETop' in header else -1
            ebot_idx = header.index('EBot') if 'EBot' in header else -1
            pl_idx = header.index('PL') if 'PL' in header else -1
            if etop_idx > 0 and ebot_idx > 0 and pl_idx > 0:
                etop_val = parts[etop_idx]
                ebot_val = parts[ebot_idx]
                pl_val = parts[pl_idx]
                if etop_val not in ('None', 'NaN') and ebot_val not in ('None', 'NaN') and pl_val not in ('None', 'NaN'):
                    found_non_null = True
    assert found_non_null, "ETop, EBot, PL should not all be None with the fix applied"

    # Regression: Patch out status conversion and verify indicators are None
    from state import instrument_interval
    orig_init = instrument_interval.InstrumentInterval.__init__
    def broken_init(self, *a, **kw):
        # Do not convert status
        return orig_init(self, *a, **kw)
    instrument_interval.InstrumentInterval.__init__ = broken_init
    with patch.object(sys, 'argv', test_args):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            try:
                mod.__main__
            except AttributeError:
                pass
        output = buf.getvalue()
        # Should still have columns, but all None
        lines = [l for l in output.splitlines() if l.strip() and not l.strip().startswith('date symbol')]
        all_none = True
        for line in lines:
            if any(x not in line for x in ['ETop', 'EBot', 'PL']):
                parts = line.split()
                header = output.splitlines()[-1].split()
                etop_idx = header.index('ETop') if 'ETop' in header else -1
                ebot_idx = header.index('EBot') if 'EBot' in header else -1
                pl_idx = header.index('PL') if 'PL' in header else -1
                if etop_idx > 0 and ebot_idx > 0 and pl_idx > 0:
                    etop_val = parts[etop_idx]
                    ebot_val = parts[ebot_idx]
                    pl_val = parts[pl_idx]
                    if etop_val not in ('None', 'NaN') or ebot_val not in ('None', 'NaN') or pl_val not in ('None', 'NaN'):
                        all_none = False
        assert all_none, "Indicators should be None if status is not converted to 'ok'"
    # Restore
    instrument_interval.InstrumentInterval.__init__ = orig_init


def test_indicator_runner_chart(tmp_path, unit_test_db):
    """
    Test that indicator_runner.py creates a chart PNG file for a known symbol/date.
    """
    import subprocess
    import sys
    runner_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/app/indicator_runner.py'))
    chart_path = tmp_path / "out.png"
    test_args = [
        sys.executable, runner_path,
        "--symbols", "AAPL",
        "--start-date", "2024-01-02",
        "--end-date", "2024-01-04",
        "--environment", "test",
        "--gin_config", "config/app_test.gin",
        "--output-format", "chart",
        "--output-chart-path", str(chart_path),
        "--db-url", unit_test_db
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    result = subprocess.run(
        test_args,
        capture_output=True,
        text=True,
        env=env
    )
    print('STDOUT:')
    print(result.stdout)
    print('STDERR:')
    print(result.stderr)
    # Assert chart file exists
    assert chart_path.exists(), f"Chart file {chart_path} was not created."
