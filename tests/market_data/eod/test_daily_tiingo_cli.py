import pytest
import subprocess
import sys
import os
import tempfile
from pathlib import Path

import pytest_asyncio
from db.test_db_manager import intg_db

@pytest.mark.asyncio
async def test_daily_tiingo_multi_ticker(intg_db, monkeypatch):
    """
    Test that the daily_tiingo.py CLI accepts --tickers (comma-separated) and attempts to process each ticker.
    This test runs the CLI in a subprocess and checks output for expected tickers.
    """
    # Find the script path
    script = Path(__file__).parent.parent.parent.parent / "src/market_data/eod/daily_tiingo.py"
    assert script.exists(), f"Script not found: {script}"
    
    # Set up environment variables
    monkeypatch.setenv("TIINGO_API_KEY", "testkey")
    # Use the test DB (unit_test_db is a file path, extract DB name)
    db_name = Path(unit_test_db).name if "://" not in unit_test_db else unit_test_db.split("/")[-1]
    db_url = f"postgresql://user:pass@localhost/{db_name}"
    monkeypatch.setenv("DATABASE_URL", db_url)

    # Choose two fake tickers
    tickers = "AAPL,TSLA"
    start_date = "2024-01-01"
    end_date = "2024-01-10"
    
    # Run the CLI with --tickers argument
    result = subprocess.run([
        sys.executable, str(script),
        "--tickers", tickers,
        "--start_date", start_date,
        "--end_date", end_date,
        "--environment", "test"
    ], capture_output=True, text=True, env=os.environ.copy())
    
    # Output should mention both tickers
    assert "AAPL" in result.stdout, f"Output does not mention AAPL: {result.stdout}"
    assert "TSLA" in result.stdout, f"Output does not mention TSLA: {result.stdout}"
    assert result.returncode == 0 or result.returncode == 1, f"Non-zero exit: {result.returncode}\n{result.stdout}\n{result.stderr}"
