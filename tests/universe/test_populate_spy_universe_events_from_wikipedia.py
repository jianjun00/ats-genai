import subprocess
import sys
import os
import pytest

SCRIPT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/universe/populate_spy_universe_events_from_wikipedia.py'))

@pytest.mark.parametrize("env_arg", ["test"])
def test_environment_argument(monkeypatch, env_arg):
    """
    Test that the --environment argument is accepted and sets the environment correctly.
    This test only checks that the script runs and emits expected log output for environment set.
    """
    cmd = [sys.executable, SCRIPT_PATH, '--universe_name', 'SPY', '--environment', env_arg]
    # Use a dummy tickers argument to avoid full Wikipedia scrape
    cmd += ['--tickers', 'AAPL']
    env = os.environ.copy()
    env['PYTHONPATH'] = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src'))
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert result.returncode == 0, f"Script failed: {result.stderr}"
    assert f"Set environment to {env_arg}" in result.stdout or f"Set environment to {env_arg}" in result.stderr
    assert "Done populating SPY universe membership events." in result.stdout
