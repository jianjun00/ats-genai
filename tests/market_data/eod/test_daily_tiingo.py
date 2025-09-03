import pytest
import pytest_asyncio
import asyncpg
import os
from datetime import date
from db.test_db_manager import unit_test_db_clean
from shared.utils.environment import Environment, EnvironmentType
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
from domains.market_data.repositories.daily_prices_tiingo_dao import DailyPricesTiingoDAO
import asyncio

# Helper to run the main logic from daily_tiingo.py (should be refactored for direct import)
from domains.market_data.services.eod import daily_price_tiingo


def test_tiingo_log_fixture(log_fixture):
    """Test loading Tiingo API logs for AAPL from tests/data/daily_prices_tiingo."""
    req, resp = log_fixture('daily_prices_tiingo', 'aapl', '')
    assert 'url' in req
    assert isinstance(resp, (list, dict))
    # Check that the response contains at least one price row if not empty
    if isinstance(resp, list) and resp:
        assert 'date' in resp[0]

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_no_prior_instrument_and_xref(unit_test_db):
    """Test when there is no prior instrument and instrument_xref entry."""
    from shared.utils.environment import Environment
    env = Environment(env_type=EnvironmentType.TEST, db_url="postgresql://postgres:password@localhost:5432/test_db")
    # Environment no longer has a config attribute; ensure db_url is correct in constructor.
    # DB is empty: no instruments, no xrefs
    # Should skip or error gracefully
    # Try running the main logic for a random instrument_id
        # Refactored: call the ingestion logic directly, not CLI main
    if hasattr(daily_tiingo, 'run_ingestion'):
        # run_ingestion(start_date: str, end_date: str, instrument_id: int)
        result = await daily_tiingo.run_ingestion(
            "2024-01-01",
            "2024-01-10",
            999999  # Nonexistent
        )
        # Should not throw, and should print a skip message or similar
        # (You may want to capture stdout for assertion)
    else:
        pytest.skip('run_ingestion() not implemented in daily_tiingo.py; cannot test ingestion logic directly.')

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_existing_instrument_and_xref_with_vendor(unit_test_db):
    """Test when instrument and instrument_xref exist with ticker vendor."""
    # TODO: Insert instrument and xref, then run main for that instrument_id
    pass

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_empty_list_and_delist_date(unit_test_db):
    """Test when list date and delist date are empty (should process full range)."""
    # TODO: Insert instrument/xref with no list/delist date, run main
    pass

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_valid_list_date(unit_test_db):
    """Test when list date is valid (should start from list date)."""
    # TODO: Insert instrument/xref with valid list_date, run main, check range
    pass

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_invalid_delist_date(unit_test_db):
    """Test when delist date is invalid (should process up to end_date)."""
    # TODO: Insert instrument/xref with invalid delist_date, run main
    pass
