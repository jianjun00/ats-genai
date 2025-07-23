import os
import sys
import tempfile
import shutil
import subprocess
import pytest
from pathlib import Path

from src.state.universe_state_manager import UniverseStateManager


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


def test_cli_build_action(tmp_path):
    # Build action should create universe state files
    # Use a stub builder by patching UniverseStateBuilder
    import pandas as pd
    from unittest.mock import patch
    dummy_df = pd.DataFrame({
        'instrument_id': [1], 'low': [5], 'high': [10], 'close': [8], 'volume': [100],
        'adv': [110], 'pldot': [0.1], 'etop': [0.2], 'ebot': [0.3]
    })
    class DummyBuilder:
        def __init__(self, universe, state_manager): pass
        def build_universe_state(self, date_str): return dummy_df
    # Set env so CLI subprocess uses DummyBuilder
    import os
    env = os.environ.copy()
    env["UNIVERSE_BUILDER_CLASS"] = "utils.tests.dummy_builder.DummyBuilder"
    # Ensure both src and tests are on PYTHONPATH
    project_root = str(Path(__file__).parent.parent.parent)
    env["PYTHONPATH"] = project_root + (":" + env["PYTHONPATH"] if "PYTHONPATH" in env else "")
    result = subprocess.run([
        sys.executable, str(Path(__file__).parent.parent.parent / "src/state/universe_state_manager.py"),
        "--start_date", "2024-01-02",
        "--end_date", "2024-01-02",
        "--universe_id", "dummy",
        "--action", "build",
        "--saved_dir", str(tmp_path)
    ], capture_output=True, text=True, env=env)
    assert "Built and saved universe state for 2024-01-02" in result.stdout
    assert result.returncode == 0
    # Now inspect to verify file
    result2 = run_cli([
        "--start_date", "2024-01-02",
        "--end_date", "2024-01-02",
        "--universe_id", "dummy",
        "--action", "inspect",
        "--instrument_id", "1",
        "--mode", "print"
    ], tmp_path)
    assert "low=5" in result2.stdout
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
