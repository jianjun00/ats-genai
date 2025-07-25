import os
import sys
import tempfile
import shutil
import subprocess
import pytest
from pathlib import Path

from src.state.universe_state_manager import UniverseStateManager
from db.test_db_manager import unit_test_db


def run_cli(args, tmp_path):
    return subprocess.run([
        sys.executable, str(Path(__file__).parent.parent.parent / "src/state/universe_state_manager.py"),
        *args,
        "--saved_dir", str(tmp_path)
    ], capture_output=True, text=True)


def test_debug_calendar_import(tmp_path):
    # Run a subprocess to print sys.path and calendar module file
    debug_script = tmp_path / "debug_calendar.py"
    debug_script.write_text(
        """
import sys
try:
    import calendar
    print('calendar.__file__:', calendar.__file__)
    print('calendar.day_abbr:', list(getattr(calendar, 'day_abbr', [])))
except Exception as e:
    print('calendar import error:', e)
print('sys.path:', sys.path)
"""
    )
    result = subprocess.run([
        sys.executable, str(debug_script)
    ], capture_output=True, text=True)
    print('DEBUG STDOUT:', result.stdout)
    print('DEBUG STDERR:', result.stderr)
    assert 'calendar.__file__' in result.stdout
    assert '/calendar.py' in result.stdout or '/calendar.pyc' in result.stdout
    # Should show day_abbr as 7 items or error message
    assert 'calendar.day_abbr:' in result.stdout
    assert 'sys.path:' in result.stdout


def test_cli_build_and_inspect(tmp_path):
    # Setup: create dummy universe state files for inspect
    manager = UniverseStateManager(base_path=tmp_path)
    import pandas as pd
    df = pd.DataFrame({
        'instrument_id': [1, 2],
        'low': [10, 20],
        'high': [15, 25],
        'close': [12, 22],
        'volume': [1000, 2000],
        'adv': [1100, 2100],
        'pldot': [0.5, 0.6],
        'etop': [0.2, 0.3],
        'ebot': [0.1, 0.2],
    })
    timestamp = "20240101_000000"
    manager.save_universe_state(df, timestamp=timestamp)

    # CLI inspect (print)
    result = run_cli([
        "--start_date", "2024-01-01",
        "--end_date", "2024-01-01",
        "--universe_id", "dummy",
        "--action", "inspect",
        "--instrument_id", "1",
        "--mode", "print"
    ], tmp_path)
    assert "low=10" in result.stdout
    assert "high=15" in result.stdout
    assert result.returncode == 0

    # CLI inspect (graph) - should not error, but we can't check the plot
    result = run_cli([
        "--start_date", "2024-01-01",
        "--end_date", "2024-01-01",
        "--universe_id", "dummy",
        "--action", "inspect",
        "--instrument_id", "1",
        "--mode", "graph"
    ], tmp_path)
    assert result.returncode == 0


def test_cli_build_and_inspect_all_signals(tmp_path):
    """
    Build a universe state with all technical signals, then inspect the persisted state and verify
    all signals (high, low, close, pldot, oneonedot, etop, ebot) are as expected.
    """
    import os
    import pandas as pd
    # Extend DummyBuilder to include oneonedot for this test
    dummy_signals = {
        'instrument_id': [1],
        'low': [11],
        'high': [22],
        'close': [15],
        'volume': [100],
        'adv': [110],
        'pldot': [0.5],
        'oneonedot': [0.9],
        'etop': [0.7],
        'ebot': [0.2],
    }
    # Write a custom DummyBuilder to src/utils/tests/dummy_builder.py for this test
    dummy_builder_path = Path(__file__).parent.parent.parent / "src/utils/tests/dummy_builder.py"
    with open(dummy_builder_path, "w") as f:
        f.write(
            """
import pandas as pd
class DummyBuilder:
    def __init__(self, universe, state_manager): pass
    def build_universe_state(self, date_str):
        return pd.DataFrame({
            'instrument_id': [1],
            'low': [11],
            'high': [22],
            'close': [15],
            'volume': [100],
            'adv': [110],
            'pldot': [0.5],
            'oneonedot': [0.9],
            'etop': [0.7],
            'ebot': [0.2]
        })
"""
        )
        f.flush()
        import os
        os.fsync(f.fileno())
    env = os.environ.copy()
    env["UNIVERSE_BUILDER_CLASS"] = "utils.tests.dummy_builder.DummyBuilder"
    project_root = str(Path(__file__).parent.parent.parent)
    env["PYTHONPATH"] = project_root + (":" + env["PYTHONPATH"] if "PYTHONPATH" in env else "")
    # Build
    result = subprocess.run([
        sys.executable, str(Path(__file__).parent.parent.parent / "src/state/universe_state_manager.py"),
        "--start_date", "2024-01-03",
        "--end_date", "2024-01-03",
        "--universe_id", "dummy",
        "--action", "build",
        "--saved_dir", str(tmp_path)
    ], capture_output=True, text=True, env=env)
    assert "Built and saved universe state for 2024-01-03" in result.stdout
    assert result.returncode == 0
    # Wait for file to exist before inspect
    import time
    parquet_path = Path(tmp_path) / "states" / "universe_state_20240103_000000.parquet"
    for _ in range(10):
        if parquet_path.exists():
            break
        time.sleep(0.1)
    assert parquet_path.exists(), f"Expected state file {parquet_path} not found after build."
    # Debug: print contents of states/ dir before inspect
    print("DEBUG: states dir contents before inspect:", list((Path(tmp_path) / "states").iterdir()))
    # Inspect
    result2 = subprocess.run([
        sys.executable, str(Path(__file__).parent.parent.parent / "src/state/universe_state_manager.py"),
        "--start_date", "2024-01-03",
        "--end_date", "2024-01-03",
        "--universe_id", "dummy",
        "--action", "inspect",
        "--instrument_id", "1",
        "--mode", "print",
        "--fields", "low", "high", "close", "pldot", "oneonedot", "etop", "ebot",
        "--saved_dir", str(tmp_path)
    ], capture_output=True, text=True, env=env)
    out = result2.stdout
    err = result2.stderr
    print("DEBUG: inspect CLI stdout:", out)
    print("DEBUG: inspect CLI stderr:", err)
    assert "low=11" in out
    assert "high=22" in out
    assert "close=15" in out
    assert "pldot=0.5" in out
    assert "oneonedot=0.9" in out
    assert "etop=0.7" in out
    assert "ebot=0.2" in out
    assert result2.returncode == 0


def test_cli_inspect_missing_instrument(tmp_path):
    # Should print None for all fields if instrument_id not present
    manager = UniverseStateManager(base_path=tmp_path)
    import pandas as pd
    df = pd.DataFrame({
        'instrument_id': [2],
        'low': [20],
        'high': [25],
        'close': [22],
        'volume': [2000],
        'adv': [2100],
        'pldot': [0.6],
        'etop': [0.3],
        'ebot': [0.2],
    })
    timestamp = "20240103_000000"
    manager.save_universe_state(df, timestamp=timestamp)
    result = run_cli([
        "--start_date", "2024-01-03",
        "--end_date", "2024-01-03",
        "--universe_id", "dummy",
        "--action", "inspect",
        "--instrument_id", "1",
        "--mode", "print"
    ], tmp_path)
    assert "low=None" in result.stdout
    assert result.returncode == 0


def test_cli_invalid_date_range(tmp_path):
    # End date before start date should yield no states found
    result = run_cli([
        "--start_date", "2024-01-05",
        "--end_date", "2024-01-01",
        "--universe_id", "dummy",
        "--action", "inspect",
        "--instrument_id", "1",
        "--mode", "print"
    ], tmp_path)
    assert "No universe states found" in result.stdout or result.returncode != 0


def test_cli_missing_required_args(tmp_path):
    # Missing required argument should fail
    result = run_cli([
        "--start_date", "2024-01-01",
        "--end_date", "2024-01-01",
        "--universe_id", "dummy",
        "--action", "inspect",
        # missing --instrument_id
        "--mode", "print"
    ], tmp_path)
    assert "--instrument_id is required" in result.stdout or result.returncode != 0
