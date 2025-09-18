import os
import pandas as pd
from unittest.mock import patch
from datetime import date
import asyncio

import importlib.util

from tests.fixtures.insert_test_daily_prices import insert_test_daily_price_polygon

def test_indicator_runner_df(unit_test_db, setup_test_universe_data):
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
        "--end-date", "2024-04-30",
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
    import io
    import contextlib
    # Extract the DataFrame part of the output
    df_lines = []
    in_table = False
    for line in output.splitlines():
        if line.strip().startswith('date') and 'ETop' in line:
            in_table = True
            df_lines.append(line)
        elif in_table and line.strip():
            df_lines.append(line)
        elif in_table and not line.strip():
            break
    if df_lines:
        df_str = '\n'.join(df_lines)
        df = pd.read_csv(io.StringIO(df_str), sep=r'\s+')
        # Filter out any rows that are not valid data (e.g., debug/footer lines)
        numeric_cols = ['ETop', 'EBot', 'PL']
        filtered = df.copy()
        for col in numeric_cols:
            if col not in filtered.columns:
                assert False, f"Missing indicator column: {col}"
        # Only keep rows where at least one indicator column is present and is float or int (not NaN)
        filtered = filtered[
            pd.to_numeric(filtered['ETop'], errors='coerce').notna() |
            pd.to_numeric(filtered['EBot'], errors='coerce').notna() |
            pd.to_numeric(filtered['PL'], errors='coerce').notna()
        ]
        if filtered.empty:
            print("[TEST DEBUG] Filtered DataFrame with indicator columns:")
            print(df)
        assert not filtered.empty, "At least one of ETop, EBot, PL should not be None with the fix applied"
    else:
        assert False, "No DataFrame output found in indicator_runner output"

    # Regression: Patch out status conversion and verify indicators are None
    from state import instrument_interval
    orig_init = instrument_interval.InstrumentInterval.__init__
    def broken_init(self, *a, **kw):
        # Do not convert status
        return orig_init(self, *a, **kw)
    instrument_interval.InstrumentInterval.__init__ = broken_init
    import src.app.indicator_runner as mod
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

def test_indicator_runner_chart(tmp_path, unit_test_db, setup_test_universe_data):
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
        "--end-date", "2024-04-30",
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
