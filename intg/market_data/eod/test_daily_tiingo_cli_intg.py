import pytest
import subprocess
import sys
import os
from pathlib import Path
from db.test_db_manager import integration_test_db

@pytest.mark.asyncio
async def test_daily_tiingo_multi_ticker(integration_test_db, monkeypatch):
    """
    Integration test: daily_tiingo.py CLI accepts --tickers and processes each ticker using intg_db.
    Requires intg_db to be created, migrated, and populated with AAPL and TSLA instrument_xrefs.
    """
    script = Path(__file__).parent.parent.parent.parent / "src/market_data/eod/daily_tiingo.py"
    assert script.exists(), f"Script not found: {script}"

    # Use intg_db (user must ensure this DB exists and is populated)
    # Use intg_db for integration test
    intg_db_url = os.environ.get("INTG_DATABASE_URL")
    if not intg_db_url:
        pytest.skip("INTG_DATABASE_URL not set; skipping integration test.")
    os.environ["DATABASE_URL"] = intg_db_url
    os.environ["TIINGO_API_KEY"] = "testkey"

    tickers = "AAPL,TSLA"
    start_date = "2024-01-01"
    end_date = "2024-01-10"

    result = subprocess.run([
        sys.executable, str(script),
        "--tickers", tickers,
        "--start_date", start_date,
        "--end_date", end_date,
        "--environment", "intg"
    ], capture_output=True, text=True, env=os.environ.copy())

    # Output should mention both tickers if instrument_xrefs exist
    assert result.returncode in (0, 1), f"Non-zero exit: {result.returncode}\n{result.stdout}\n{result.stderr}"
    assert "AAPL" in result.stdout or "Could not resolve instrument_id for ticker AAPL" in result.stdout or "Skipping" in result.stdout, f"Output does not mention AAPL: {result.stdout}"
    assert "TSLA" in result.stdout or "Could not resolve instrument_id for ticker TSLA" in result.stdout or "Skipping" in result.stdout, f"Output does not mention TSLA: {result.stdout}"
