#!/usr/bin/env python
"""
Mock test runner for the data agent.
This script sets up and runs the data agent locally with mock adapters for testing purposes.
"""

import asyncio
import argparse
import json
import logging
import time
from datetime import date, datetime, timedelta

from src.market_data.agent.mock_metrics_helper import MockMetricsHelper
from src.core.shared.utils.environment import Environment

from src.market_data.agent.data_agent_orchestrator import DataAgentOrchestrator
from src.market_data.agent.base_adapter import VendorAdapter
from src.market_data.agent.models import EODPrice
from src.market_data.agent.reconciliation import ReconciliationEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Custom JSON encoder to handle date objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

# Mock adapter for testing
class MockPolygonAdapter(VendorAdapter):
    vendor_name = "polygon"

    def __init__(self, api_key=None):
        self.api_key = api_key or "mock_key"

    async def fetch_instruments(self):
        return [
            {"instrument_id": "AAPL", "name": "Apple Inc.", "type": "stock"},
            {"instrument_id": "MSFT", "name": "Microsoft Corporation", "type": "stock"},
            {"instrument_id": "GOOGL", "name": "Alphabet Inc.", "type": "stock"}
        ]

    async def fetch_eod(self, symbols, start_date, end_date):
        results = []
        current_date = start_date
        while current_date <= end_date:
            for symbol in symbols:
                # Generate mock data with some randomness for each symbol and date
                base_price = 100.0
                if symbol == "AAPL":
                    base_price = 150.0
                elif symbol == "MSFT":
                    base_price = 250.0
                elif symbol == "GOOGL":
                    base_price = 2000.0

                # Add some variation based on the date
                day_factor = (current_date.day % 10) / 10.0

                results.append(EODPrice(
                    instrument_id=symbol,
                    date=current_date,
                    open=base_price * (1.0 - 0.01 * day_factor),
                    high=base_price * (1.0 + 0.02 * day_factor),
                    low=base_price * (1.0 - 0.02 * day_factor),
                    close=base_price * (1.0 + 0.01 * day_factor),
                    adj_close=base_price * (1.0 + 0.01 * day_factor),
                    volume=int(1000000 * (1.0 + day_factor)),
                    vendor="polygon",
                    quality_score=0.9
                ))
            current_date += timedelta(days=1)
        return results

    async def fetch_ticks(self, symbol, start_dt, end_dt):
        return []

    async def fetch_interval(self, symbol, interval, start_dt, end_dt):
        return []

    async def fetch_ticks(self, symbol, start_dt, end_dt):
        return []

    async def fetch_interval(self, symbol, interval, start_dt, end_dt):
        return []

# Mock Tiingo adapter
class MockTiingoAdapter(VendorAdapter):
    vendor_name = "tiingo"

    def __init__(self, api_key=None):
        self.api_key = api_key or "mock_key"

    async def fetch_instruments(self):
        return [
            {"instrument_id": "AAPL", "name": "Apple Inc.", "type": "stock"},
            {"instrument_id": "MSFT", "name": "Microsoft Corporation", "type": "stock"},
            {"instrument_id": "GOOGL", "name": "Alphabet Inc.", "type": "stock"}
        ]

    async def fetch_eod(self, symbols, start_date, end_date):
        results = []
        current_date = start_date
        while current_date <= end_date:
            for symbol in symbols:
                # Generate mock data with some randomness for each symbol and date
                base_price = 100.0
                if symbol == "AAPL":
                    base_price = 150.5
                elif symbol == "MSFT":
                    base_price = 250.5
                elif symbol == "GOOGL":
                    base_price = 2000.5

                # Add some variation based on the date
                day_factor = (current_date.day % 10) / 10.0

                results.append(EODPrice(
                    instrument_id=symbol,
                    date=current_date,
                    open=base_price * (1.0 - 0.01 * day_factor),
                    high=base_price * (1.0 + 0.02 * day_factor),
                    low=base_price * (1.0 - 0.02 * day_factor),
                    close=base_price * (1.0 + 0.01 * day_factor),
                    adj_close=base_price * (1.0 + 0.01 * day_factor),
                    volume=int(1010000 * (1.0 + day_factor)),
                    vendor="tiingo",
                    quality_score=0.95
                ))
            current_date += timedelta(days=1)
        return results

    async def fetch_ticks(self, symbol, start_dt, end_dt):
        return []

    async def fetch_interval(self, symbol, interval, start_dt, end_dt):
        return []

    async def fetch_ticks(self, symbol, start_dt, end_dt):
        return []

    async def fetch_interval(self, symbol, interval, start_dt, end_dt):
        return []

# Mock LLM assistant
class MockLLMAssistant:
    def __init__(self, api_key=None):
        self.api_key = api_key or "mock_key"

    async def select_best_source(self, sources, data_type):
        # Always prefer tiingo for testing
        return "tiingo" if "tiingo" in sources else sources[0]

    async def get_recommended_sources(self, available_sources, data_type):
        # Return all sources in priority order
        return ["tiingo", "polygon"] if "tiingo" in available_sources else available_sources

    async def reconcile_data_conflicts(self, records):
        if not records:
            return None

        # Simple reconciliation: average the values
        result = {}
        for field in ["open", "high", "low", "close", "volume"]:
            values = [getattr(r, field) for r in records if hasattr(r, field) and getattr(r, field) is not None]
            if values:
                result[field] = sum(values) / len(values)

        # Use adj_close from the first record that has it
        for r in records:
            if hasattr(r, "adj_close") and r.adj_close is not None:
                result["adj_close"] = r.adj_close
                break

        return result

    async def detect_anomalies(self, record, historical_data):
        # Simple anomaly detection: always return False for testing
        return False, "No anomalies detected"

async def setup_database():
    """Set up database connection pool"""
    env = Environment()
    db_host = env.get_db_host()
    db_port = env.get_db_port()
    db_name = env.get_db_name()
    db_user = env.get_db_user()
    db_password = env.get_db_password()

    # Create connection pool
    try:
        pool = await asyncpg.create_pool(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )

        logger.info(f"Connected to database {db_name} at {db_host}:{db_port}")

        # Ensure required tables exist
        async with pool.acquire() as conn:
            # Check if reconciled_records table exists
            table_name = env.get_table_name("reconciled_records")
            exists = await conn.fetchval(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = '{table_name}'
                )
            """)

            if not exists:
                logger.info(f"Creating table {table_name}")
                await conn.execute(f"""
                    CREATE TABLE {table_name} (
                        instrument_id VARCHAR(20) NOT NULL,
                        as_of DATE NOT NULL,
                        data_type VARCHAR(20) NOT NULL,
                        value JSONB NOT NULL,
                        quality_score FLOAT NOT NULL,
                        sources TEXT[] NOT NULL,
                        rationale TEXT,
                        provenance JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (instrument_id, as_of, data_type)
                    )
                """)

        return pool
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        logger.info("Using in-memory mock database instead")
        return MockPool()

# Mock database for testing without a real database
class MockPool:
    def acquire(self):
        return MockConnection()

    def release(self, conn):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def close(self):
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
        if "universe_membership" in query or "information_schema.tables" in query:
            return [{"instrument_id": "AAPL"}, {"instrument_id": "MSFT"}, {"instrument_id": "GOOGL"}]
        elif "reconciled_records" in query:
            symbol = args[0] if args else None
            if symbol in ["AAPL", "MSFT", "GOOGL"]:
                # Return empty list to simulate no existing records
                return []
            return []
        return []

    async def fetchval(self, query, *args):
        if "information_schema.tables" in query:
            return True
        return None

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
        logger.info(f"Executed query: {query[:100]}...")

async def run_backfill(orchestrator, args):
    """Run backfill process"""
    logger.info("Starting backfill process")

    # Store the original method
    original_process = orchestrator._process_data_point

    # Create a patched version that properly awaits adapter fetch calls
    async def patched_process(data_point):
        symbol = data_point["symbol"]
        target_date = data_point["date"]

        start_time = time.time()
        success = True
        sources_status = {}

        # Fetch from all adapters
        all_prices = []
        for vendor_name, adapter in orchestrator.adapters.items():
            try:
                source_start = time.time()

                # Use a small date range (just the target date) and properly await
                prices = await adapter.fetch_eod([symbol], target_date, target_date)
                # Filter to exact date match
                prices = [p for p in prices if p.date == target_date]
                all_prices.extend(prices)

                # Record source metrics directly
                if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
                    source_elapsed = time.time() - source_start
                    orchestrator.metrics.record_source_result(vendor_name, True, source_elapsed)
                    sources_status[vendor_name] = True

            except Exception as e:
                logger.error(f"Error fetching {symbol} data from {vendor_name}: {e}")
                success = False
                sources_status[vendor_name] = False

                # Record source failure directly
                if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
                    orchestrator.metrics.record_source_result(vendor_name, False, 0.0)

        if not all_prices:
            logger.warning(f"No data found for {symbol} on {target_date}")
            success = False

            # Record reconciliation metrics directly - no data case
            if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
                orchestrator.metrics.record_reconciliation(0, False)

            # Record overall data point processing directly - failure
            if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
                elapsed = time.time() - start_time
                orchestrator.metrics.record_data_point_processed(False, elapsed)

            return

        # Reconcile data from multiple sources
        had_conflict = len(all_prices) > 1 and len(set(p.vendor for p in all_prices)) > 1
        reconciled = orchestrator.reconciliation_engine.reconcile_eod_prices(all_prices)

        # Record reconciliation metrics directly
        if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
            orchestrator.metrics.record_reconciliation(
                len(set(p.vendor for p in all_prices)),
                had_conflict
            )

        if reconciled:
            # Store the reconciled record
            await orchestrator.dao.insert(reconciled)
            logger.info(f"Stored reconciled data for {symbol} on {target_date}")
        else:
            success = False

        # Record overall data point processing directly
        if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
            elapsed = time.time() - start_time
            orchestrator.metrics.record_data_point_processed(success, elapsed)

    # Apply the patch
    orchestrator._process_data_point = patched_process

    # Simple implementation to simulate backfill without relying on orchestrator's decorated method
    batch_size = args.batch_size or 10
    max_iterations = args.max_iterations or 1

    # Process a few mock data points to simulate backfill
    for i in range(max_iterations):
        for j in range(batch_size):
            symbol = f"MOCK{j+1}"
            # Use dates in the past
            target_date = date.today() - timedelta(days=j+1)

            logger.info(f"[Backfill] Processing {symbol} for {target_date}")
            await patched_process({
                "symbol": symbol,
                "date": target_date
            })

    # Restore original method
    orchestrator._process_data_point = original_process

    # Force log metrics at the end if monitoring is enabled
    if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
        orchestrator.metrics.log_metrics()

    logger.info("Backfill process completed")

async def run_frontfill(orchestrator, args):
    """Run frontfill process"""
    logger.info("Starting frontfill process")

    # Store the original method
    original_process = orchestrator._process_data_point

    # Create a patched version that properly awaits adapter fetch calls
    async def patched_process(data_point):
        symbol = data_point["symbol"]
        target_date = data_point["date"]

        start_time = time.time()
        success = True
        sources_status = {}

        # Fetch from all adapters
        all_prices = []
        for vendor_name, adapter in orchestrator.adapters.items():
            try:
                source_start = time.time()

                # Use a small date range (just the target date) and properly await
                prices = await adapter.fetch_eod([symbol], target_date, target_date)
                # Filter to exact date match
                prices = [p for p in prices if p.date == target_date]
                all_prices.extend(prices)

                # Record source metrics directly
                if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
                    source_elapsed = time.time() - source_start
                    orchestrator.metrics.record_source_result(vendor_name, True, source_elapsed)
                    sources_status[vendor_name] = True

            except Exception as e:
                logger.error(f"Error fetching {symbol} data from {vendor_name}: {e}")
                success = False
                sources_status[vendor_name] = False

                # Record source failure directly
                if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
                    orchestrator.metrics.record_source_result(vendor_name, False, 0.0)

        if not all_prices:
            logger.warning(f"No data found for {symbol} on {target_date}")
            success = False

            # Record reconciliation metrics directly - no data case
            if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
                orchestrator.metrics.record_reconciliation(0, False)

            # Record overall data point processing directly - failure
            if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
                elapsed = time.time() - start_time
                orchestrator.metrics.record_data_point_processed(False, elapsed)

            return

        # Reconcile data from multiple sources
        had_conflict = len(all_prices) > 1 and len(set(p.vendor for p in all_prices)) > 1
        reconciled = orchestrator.reconciliation_engine.reconcile_eod_prices(all_prices)

        # Record reconciliation metrics directly
        if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
            orchestrator.metrics.record_reconciliation(
                len(set(p.vendor for p in all_prices)),
                had_conflict
            )

        if reconciled:
            # Store the reconciled record
            await orchestrator.dao.insert(reconciled)
            logger.info(f"Stored reconciled data for {symbol} on {target_date}")
        else:
            success = False

        # Record overall data point processing directly
        if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
            elapsed = time.time() - start_time
            orchestrator.metrics.record_data_point_processed(success, elapsed)

    # Apply the patch
    orchestrator._process_data_point = patched_process

    # Simple implementation to simulate frontfill without relying on orchestrator's decorated method
    # Process a few mock symbols for today's date
    symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "META"]
    today = date.today()

    for symbol in symbols:
        logger.info(f"[Frontfill] Processing {symbol} for {today}")
        await patched_process({
            "symbol": symbol,
            "date": today
        })

    # Restore original method
    orchestrator._process_data_point = original_process

    # Force log metrics at the end if monitoring is enabled
    if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
        orchestrator.metrics.log_metrics()

    logger.info("Frontfill process completed")

async def process_symbol(orchestrator, args):
    """Process a specific symbol"""
    symbol = args.symbol
    if not symbol:
        logger.error("Symbol is required for process-symbol mode")
        return

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else date.today() - timedelta(days=5)
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else date.today()

    # Store the original method
    original_process = orchestrator._process_data_point

    # Create a patched version that properly awaits adapter fetch calls
    async def patched_process(data_point):
        symbol = data_point["symbol"]
        target_date = data_point["date"]

        # Fetch from all adapters
        all_prices = []
        for vendor_name, adapter in orchestrator.adapters.items():
            try:
                # Use a small date range (just the target date) and properly await
                prices = await adapter.fetch_eod([symbol], target_date, target_date)
                # Filter to exact date match
                prices = [p for p in prices if p.date == target_date]
                all_prices.extend(prices)
            except Exception as e:
                logger.error(f"Error fetching {symbol} data from {vendor_name}: {e}")

        if not all_prices:
            logger.warning(f"No data found for {symbol} on {target_date}")
            return

        # Reconcile data from multiple sources
        reconciled = orchestrator.reconciliation_engine.reconcile_eod_prices(all_prices)

        if reconciled:
            # Store the reconciled record
            await orchestrator.dao.insert(reconciled)
            logger.info(f"Stored reconciled data for {symbol} on {target_date}")

    # Apply the patch
    orchestrator._process_data_point = patched_process

    current_date = start_date
    while current_date <= end_date:
        logger.info(f"Processing {symbol} for {current_date}")

        # Record start time for metrics
        start_time = time.time()
        success = True
        sources_status = {}

        # Process the data point
        try:
            await orchestrator._process_data_point({
                "symbol": symbol,
                "date": current_date
            })

            # Assume both sources succeeded for mock metrics
            sources_status = {
                "polygon": True,
                "tiingo": True
            }
        except Exception as e:
            logger.error(f"Error processing {symbol} for {current_date}: {e}")
            success = False
            sources_status = {
                "polygon": False,
                "tiingo": False
            }

        # Record metrics
        elapsed = time.time() - start_time
        MockMetricsHelper.record_data_point_metrics(
            orchestrator,
            symbol,
            current_date,
            sources_status,
            success,
            elapsed
        )

        current_date += timedelta(days=1)

    # Restore original method
    orchestrator._process_data_point = original_process

    # Force log metrics at the end if monitoring is enabled
    if hasattr(orchestrator, 'metrics') and orchestrator.metrics:
        orchestrator.metrics.log_metrics()

    logger.info(f"Completed processing {symbol} from {start_date} to {end_date}")

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Data Agent Mock Runner")
    parser.add_argument("mode", choices=["backfill", "frontfill", "process-symbol"],
                      help="Operation mode")
    parser.add_argument("--batch-size", type=int, default=10,
                      help="Batch size for backfill (default: 10)")
    parser.add_argument("--max-iterations", type=int, default=1,
                      help="Maximum iterations for backfill (default: 1)")
    parser.add_argument("--symbol", type=str,
                      help="Symbol to process (required for process-symbol mode)")
    parser.add_argument("--start-date", type=str,
                      help="Start date for process-symbol mode (format: YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str,
                      help="End date for process-symbol mode (format: YYYY-MM-DD)")
    parser.add_argument("--vendor-priority", type=str, default="tiingo,polygon",
                      help="Comma-separated list of vendor priority (default: tiingo,polygon)")
    parser.add_argument("--use-mock-db", action="store_true",
                      help="Use mock database instead of real database connection")

    args = parser.parse_args()

    # Set up database
    pool = None
    if args.use_mock_db:
        logger.info("Using mock database")
        pool = MockPool()
    else:
        pool = await setup_database()

    try:
        # Initialize mock adapters
        adapters = {
            "polygon": MockPolygonAdapter(),
            "tiingo": MockTiingoAdapter()
        }
        logger.info("Initialized mock adapters")

        # Initialize mock LLM assistant
        MockLLMAssistant()
        logger.info("Initialized mock LLM assistant")

        # Patch json.dumps to handle date/datetime objects
        original_dumps = json.dumps
        def patched_dumps(obj, *args, **kwargs):
            return original_dumps(obj, *args, cls=DateTimeEncoder, **kwargs)

        # Parse vendor priority
        vendor_priority = args.vendor_priority.split(",")

        # Initialize reconciliation engine
        reconciliation_engine = ReconciliationEngine(vendor_priority=vendor_priority)

        # Mock JSON dumps for date/datetime serialization
        from unittest.mock import patch
        with patch('json.dumps', side_effect=patched_dumps):
            # Initialize orchestrator
            orchestrator = DataAgentOrchestrator(
                pool=pool,
                adapters=adapters,
                reconciliation_engine=reconciliation_engine,
                lookback_years=1  # Use 1 year for testing
            )

            # Run the selected mode
            if args.mode == "backfill":
                await run_backfill(orchestrator, args)
            elif args.mode == "frontfill":
                await run_frontfill(orchestrator, args)
            elif args.mode == "process-symbol":
                await process_symbol(orchestrator, args)

    finally:
        # Close the connection pool
        if pool:
            await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
