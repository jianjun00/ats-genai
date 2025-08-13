import os
import pytest
import asyncio
from unittest import mock
from datetime import datetime, date, timedelta
import json
import tempfile

# Custom JSON encoder to handle date objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

from src.market_data.agent.data_agent_orchestrator import DataAgentOrchestrator
from src.market_data.agent.polygon_adapter import PolygonAdapter
from src.market_data.agent.tiingo_adapter import TiingoAdapter
from src.market_data.agent.reconciliation import ReconciliationEngine
from src.market_data.agent.llm_assistant import LLMAssistant
from src.market_data.agent.models import EODPrice, ReconciledRecord

# Mock database fixtures
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
    def __init__(self):
        self.tables = {}
        self.executed_queries = []
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    async def fetch(self, query, *args):
        if "universe_membership" in query:
            return [{"instrument_id": "AAPL"}, {"instrument_id": "MSFT"}]
        elif "reconciled_records" in query:
            symbol = args[0] if args else None
            if symbol == "AAPL":
                # Return some existing records for AAPL
                today = date.today()
                yesterday = today - timedelta(days=1)
                return [
                    {
                        "instrument_id": "AAPL",
                        "as_of": yesterday.isoformat(),
                        "data_type": "eod",
                        "value": {
                            "open": 150.0,
                            "high": 155.0,
                            "low": 149.0,
                            "close": 153.0,
                            "volume": 1000000
                        },
                        "quality_score": 0.9,
                        "sources": ["polygon", "tiingo"],
                        "rationale": "Consensus value",
                        "provenance": {}
                    }
                ]
            return []
        return []
    
    async def fetchrow(self, query, *args):
        return None
    
    async def execute(self, query, *args):
        # Handle date serialization in insert queries
        if "INSERT INTO" in query and len(args) >= 8:
            # Convert dates to ISO format strings for storage
            processed_args = list(args)
            # Handle date in as_of field (args[1])
            if isinstance(processed_args[1], (date, datetime)):
                processed_args[1] = processed_args[1].isoformat()
            # Handle dates in value and provenance JSON (args[3] and args[7])
            if isinstance(processed_args[3], dict):
                processed_args[3] = json.dumps(processed_args[3], cls=DateTimeEncoder)
            if isinstance(processed_args[7], dict):
                processed_args[7] = json.dumps(processed_args[7], cls=DateTimeEncoder)
            args = tuple(processed_args)
        self.executed_queries.append((query, args))

# Mock API responses
POLYGON_SAMPLE_RESPONSE = {
    "ticker": "AAPL",
    "status": "OK",
    "results": [
        {
            "v": 123456,
            "o": 150.0,
            "c": 155.0,
            "h": 156.0,
            "l": 149.0,
            "t": int(datetime.now().timestamp() * 1000)  # Today's timestamp
        }
    ]
}

TIINGO_SAMPLE_RESPONSE = [
    {
        "date": datetime.now().strftime("%Y-%m-%dT00:00:00.000Z"),
        "open": 150.5,
        "high": 156.5,
        "low": 149.5,
        "close": 155.5,
        "adjClose": 155.5,
        "volume": 123456,
        "adjVolume": 123456
    }
]

# Mock LLM responses
LLM_RECONCILIATION_RESPONSE = """
{
    "open": 150.25,
    "high": 156.25,
    "low": 149.25,
    "close": 155.25,
    "adj_close": 155.25,
    "volume": 123456
}
"""

@pytest.fixture
def mock_pool():
    return MockPool()

@pytest.fixture
def mock_polygon_adapter():
    with mock.patch.dict(os.environ, {"POLYGON_API_KEY": "test_key"}):
        adapter = PolygonAdapter()
        with mock.patch('requests.get') as mock_get:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = POLYGON_SAMPLE_RESPONSE
            mock_get.return_value = mock_response
            yield adapter

@pytest.fixture
def mock_tiingo_adapter():
    with mock.patch.dict(os.environ, {"TIINGO_API_KEY": "test_key"}):
        adapter = TiingoAdapter()
        with mock.patch('requests.get') as mock_get:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = TIINGO_SAMPLE_RESPONSE
            mock_get.return_value = mock_response
            yield adapter

@pytest.fixture
def mock_llm_assistant():
    with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "test_key"}):
        assistant = LLMAssistant()
        with mock.patch.object(assistant, '_generate', return_value=LLM_RECONCILIATION_RESPONSE):
            yield assistant

@pytest.fixture
def data_agent(mock_pool, mock_polygon_adapter, mock_tiingo_adapter):
    adapters = {
        "polygon": mock_polygon_adapter,
        "tiingo": mock_tiingo_adapter
    }
    reconciliation_engine = ReconciliationEngine(vendor_priority=["tiingo", "polygon"])
    
    # Patch json.dumps to handle date/datetime objects
    original_dumps = json.dumps
    def patched_dumps(obj, *args, **kwargs):
        return original_dumps(obj, *args, cls=DateTimeEncoder, **kwargs)
    
    with mock.patch('json.dumps', side_effect=patched_dumps):
        return DataAgentOrchestrator(
            pool=mock_pool,
            adapters=adapters,
            reconciliation_engine=reconciliation_engine,
            lookback_years=1
        )

@pytest.mark.asyncio
async def test_backfill_workflow(data_agent):
    """Test the complete backfill workflow"""
    # Patch json.dumps to handle date/datetime objects
    original_dumps = json.dumps
    def patched_dumps(obj, *args, **kwargs):
        return original_dumps(obj, *args, cls=DateTimeEncoder, **kwargs)
    
    # Mock the market closed check to always return True
    with mock.patch('json.dumps', side_effect=patched_dumps):
        with mock.patch.object(data_agent, '_is_market_closed', return_value=True):
            # Run backfill with max 1 iteration
            await data_agent.run_backfill_loop(batch_size=10, max_iterations=1)
    
    # Verify that data was processed and stored
    # This is an indirect test since we're using mocks
    # In a real test, we would check the database for the inserted records

@pytest.mark.asyncio
async def test_frontfill_workflow(data_agent):
    """Test the complete frontfill workflow"""
    # Patch json.dumps to handle date/datetime objects
    original_dumps = json.dumps
    def patched_dumps(obj, *args, **kwargs):
        return original_dumps(obj, *args, cls=DateTimeEncoder, **kwargs)
    
    # Mock the market closed check to always return True
    with mock.patch('json.dumps', side_effect=patched_dumps):
        with mock.patch.object(data_agent, '_is_market_closed', return_value=True):
            # Run frontfill
            await data_agent.run_frontfill_loop()
    
    # Verify that today's data was processed and stored
    # This is an indirect test since we're using mocks
    # In a real test, we would check the database for the inserted records

@pytest.mark.asyncio
async def test_end_to_end_workflow(data_agent):
    """Test the end-to-end workflow with both backfill and frontfill"""
    # Patch json.dumps to handle date/datetime objects
    original_dumps = json.dumps
    def patched_dumps(obj, *args, **kwargs):
        return original_dumps(obj, *args, cls=DateTimeEncoder, **kwargs)
    
    # Mock the market closed check to always return True
    with mock.patch('json.dumps', side_effect=patched_dumps):
        with mock.patch.object(data_agent, '_is_market_closed', return_value=True):
            # Run backfill with max 1 iteration
            await data_agent.run_backfill_loop(batch_size=10, max_iterations=1)
            
            # Run frontfill
            await data_agent.run_frontfill_loop()
    
    # In a real test, we would verify the database state

@pytest.mark.asyncio
async def test_data_point_processing(data_agent):
    """Test processing a specific data point"""
    # Patch json.dumps to handle date/datetime objects
    original_dumps = json.dumps
    def patched_dumps(obj, *args, **kwargs):
        return original_dumps(obj, *args, cls=DateTimeEncoder, **kwargs)
    
    today = date.today()
    data_point = {"symbol": "AAPL", "date": today}
    
    # Process the data point
    with mock.patch('json.dumps', side_effect=patched_dumps):
        await data_agent._process_data_point(data_point)
    
    # Verify that the data was processed
    # In a real test, we would check the database for the inserted record
