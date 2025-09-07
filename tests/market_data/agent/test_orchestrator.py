import pytest
import asyncio
from unittest import mock
from datetime import datetime, date, timedelta
import json

from domains.market_data.services.agent.data_agent_orchestrator import DataAgentOrchestrator
from domains.market_data.services.agent.base_adapter import VendorAdapter
from domains.market_data.services.agent.models import EODPrice, ReconciledRecord
from domains.market_data.services.agent.reconciliation import ReconciliationEngine

# Mock adapter for testing
class MockAdapter(VendorAdapter):
    vendor_name = "mock"

    def __init__(self, eod_data=None):
        self.eod_data = eod_data or []

    def fetch_instruments(self):
        return []

    def fetch_eod(self, symbols, start_date, end_date):
        return [p for p in self.eod_data if p.instrument_id in symbols and start_date <= p.date <= end_date]

    def fetch_ticks(self, symbol, start_dt, end_dt):
        return []

    def fetch_interval(self, symbol, interval, start_dt, end_dt):
        return []

# Mock DAO for testing
class MockReconciledRecordDAO:
    def __init__(self):
        self.records = {}

    async def insert(self, record):
        key = (record.instrument_id, record.as_of, record.data_type)
        self.records[key] = record

    async def get(self, instrument_id, as_of, data_type):
        key = (instrument_id, as_of, data_type)
        return self.records.get(key)

    async def list_for_instrument(self, instrument_id, data_type="eod"):
        return [r for (i, _, t), r in self.records.items() if i == instrument_id and t == data_type]

# Mock database pool
class MockPool:
    def acquire(self):
        return MockConnection()

    def release(self, conn):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

class MockConnection:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def fetch(self, query, *args):
        if "instrument_id" in query:
            return [{"instrument_id": "AAPL"}, {"instrument_id": "MSFT"}]
        return []

    async def fetchrow(self, query, *args):
        return None

    async def execute(self, query, *args):
        pass

@pytest.fixture
def mock_pool():
    return MockPool()

@pytest.fixture
def mock_dao():
    return MockReconciledRecordDAO()

@pytest.fixture
def mock_adapters():
    today = date.today()
    yesterday = today - timedelta(days=1)

    # Create sample EOD data
    polygon_data = [
        EODPrice(
            instrument_id="AAPL",
            date=yesterday,
            open=150.0,
            high=155.0,
            low=149.0,
            close=153.0,
            adj_close=None,
            volume=1000000,
            vendor="polygon",
            quality_score=0.9
        ),
        EODPrice(
            instrument_id="MSFT",
            date=yesterday,
            open=250.0,
            high=255.0,
            low=249.0,
            close=253.0,
            adj_close=None,
            volume=500000,
            vendor="polygon",
            quality_score=0.9
        )
    ]

    tiingo_data = [
        EODPrice(
            instrument_id="AAPL",
            date=yesterday,
            open=150.5,
            high=155.5,
            low=149.5,
            close=153.5,
            adj_close=153.5,
            volume=1010000,
            vendor="tiingo",
            quality_score=0.95
        ),
        EODPrice(
            instrument_id="MSFT",
            date=yesterday,
            open=250.5,
            high=255.5,
            low=249.5,
            close=253.5,
            adj_close=253.5,
            volume=510000,
            vendor="tiingo",
            quality_score=0.95
        )
    ]

    return {
        "polygon": MockAdapter(polygon_data),
        "tiingo": MockAdapter(tiingo_data)
    }

@pytest.fixture
def reconciliation_engine():
    return ReconciliationEngine(vendor_priority=["tiingo", "polygon"])

@pytest.fixture
def orchestrator(mock_pool, mock_adapters, reconciliation_engine, monkeypatch):
    # Patch the DAO to use our mock
    dao = MockReconciledRecordDAO()
    monkeypatch.setattr("src.market_data.agent.data_agent_orchestrator.ReconciledRecordDAO", lambda pool: dao)

    return DataAgentOrchestrator(
        pool=mock_pool,
        adapters=mock_adapters,
        reconciliation_engine=reconciliation_engine,
        lookback_years=1
    )

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_get_all_symbols(orchestrator):
    """Test retrieving all symbols"""
    symbols = await orchestrator.get_all_symbols()
    assert symbols == {"AAPL", "MSFT"}

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_get_missing_data_points(orchestrator):
    """Test identifying missing data points"""
    # Mock the get_all_symbols method to return a fixed set
    orchestrator.get_all_symbols = mock.AsyncMock(return_value={"AAPL"})

    # Call the method
    missing_points = await orchestrator.get_missing_data_points()

    # Should have entries for each day in the lookback period
    assert len(missing_points) > 0
    assert all(p["symbol"] == "AAPL" for p in missing_points)
    assert all(isinstance(p["date"], date) for p in missing_points)

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_process_data_point(orchestrator):
    """Test processing a single data point"""
    yesterday = date.today() - timedelta(days=1)
    data_point = {"symbol": "AAPL", "date": yesterday}

    # Process the data point
    await orchestrator._process_data_point(data_point)

    # Check if record was stored
    records = await orchestrator.core.dao.list_for_instrument("AAPL")
    assert len(records) == 1
    assert records[0].instrument_id == "AAPL"
    # Convert datetime to date for comparison if needed
    if isinstance(records[0].as_of, datetime):
        assert records[0].as_of.date() == yesterday
    else:
        assert records[0].as_of == yesterday
    assert records[0].data_type == "eod"

    # Verify values were reconciled
    value = records[0].value
    assert value["close"] is not None
    assert value["open"] is not None

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_run_backfill_loop(orchestrator):
    """Test running the backfill loop"""
    # Mock methods to control execution
    orchestrator.get_missing_data_points = mock.AsyncMock(
        side_effect=[
            [{"symbol": "AAPL", "date": date.today() - timedelta(days=1)}],
            []  # Empty on second call to terminate loop
        ]
    )
    orchestrator._process_batch = mock.AsyncMock()

    # Run backfill loop with max 2 iterations
    await orchestrator.run_backfill_loop(batch_size=10, max_iterations=2)

    # Verify methods were called
    assert orchestrator.get_missing_data_points.call_count == 2
    orchestrator._process_batch.assert_called_once()

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_run_frontfill_loop(orchestrator):
    """Test running the frontfill loop"""
    # Mock methods
    orchestrator._is_market_closed = mock.MagicMock(return_value=True)
    orchestrator.get_all_symbols = mock.AsyncMock(return_value={"AAPL", "MSFT"})
    orchestrator._process_data_point = mock.AsyncMock()

    # Run frontfill loop
    await orchestrator.run_frontfill_loop()

    # Verify methods were called
    orchestrator._is_market_closed.assert_called_once()
    orchestrator.get_all_symbols.assert_called_once()
    assert orchestrator._process_data_point.call_count == 2  # Once for each symbol

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_market_closed_check(orchestrator):
    """Test market closed check logic"""
    # Test with different hours
    with mock.patch("src.market_data.agent.data_agent_orchestrator.datetime") as mock_dt:
        # Market open (10 AM ET)
        mock_dt.now.return_value = datetime(2025, 1, 1, 15, 0)  # 10 AM ET (assuming UTC-5)
        assert not orchestrator._is_market_closed()

        # Market closed (5 PM ET)
        mock_dt.now.return_value = datetime(2025, 1, 1, 22, 0)  # 5 PM ET (assuming UTC-5)
        assert orchestrator._is_market_closed()

        # Market closed (before open, 8 AM ET)
        mock_dt.now.return_value = datetime(2025, 1, 1, 13, 0)  # 8 AM ET (assuming UTC-5)
        assert orchestrator._is_market_closed()
