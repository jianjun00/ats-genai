#!/usr/bin/env python
"""
Local test runner for the data agent.
This script sets up and runs the data agent locally for testing purposes.
"""

import os
import sys
import asyncio
import logging
import argparse
import json
from datetime import datetime, date, timedelta

import asyncpg
from config.environment import Environment

from src.market_data.agent.data_agent_orchestrator import DataAgentOrchestrator
from src.market_data.agent.polygon_adapter import PolygonAdapter
from src.market_data.agent.tiingo_adapter import TiingoAdapter
from src.market_data.agent.reconciliation import ReconciliationEngine
from src.market_data.agent.llm_assistant import LLMAssistant

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def setup_database():
    """Set up database connection pool"""
    env = Environment()
    db_host = env.get_db_host()
    db_port = env.get_db_port()
    db_name = env.get_db_name()
    db_user = env.get_db_user()
    db_password = env.get_db_password()
    
    # Create connection pool
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

async def run_backfill(orchestrator, args):
    """Run backfill process"""
    logger.info("Starting backfill process")
    await orchestrator.run_backfill_loop(
        batch_size=args.batch_size,
        max_iterations=args.max_iterations
    )
    logger.info("Backfill process completed")

async def run_frontfill(orchestrator, args):
    """Run frontfill process"""
    logger.info("Starting frontfill process")
    await orchestrator.run_frontfill_loop()
    logger.info("Frontfill process completed")

async def process_symbol(orchestrator, args):
    """Process a specific symbol"""
    symbol = args.symbol
    if not symbol:
        logger.error("Symbol is required for process-symbol mode")
        return
    
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else date.today() - timedelta(days=5)
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else date.today()
    
    current_date = start_date
    while current_date <= end_date:
        logger.info(f"Processing {symbol} for {current_date}")
        await orchestrator._process_data_point({
            "symbol": symbol,
            "date": current_date
        })
        current_date += timedelta(days=1)
    
    logger.info(f"Completed processing {symbol} from {start_date} to {end_date}")

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Data Agent Local Runner")
    parser.add_argument("mode", choices=["backfill", "frontfill", "process-symbol"],
                      help="Operation mode")
    parser.add_argument("--batch-size", type=int, default=100,
                      help="Batch size for backfill (default: 100)")
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
    
    args = parser.parse_args()
    
    # Set up database
    pool = await setup_database()
    
    try:
        # Initialize adapters
        adapters = {}
        
        # Polygon adapter
        polygon_api_key = os.getenv("POLYGON_API_KEY")
        if polygon_api_key:
            adapters["polygon"] = PolygonAdapter(api_key=polygon_api_key)
            logger.info("Initialized Polygon adapter")
        else:
            logger.warning("POLYGON_API_KEY not set, Polygon adapter not initialized")
        
        # Tiingo adapter
        tiingo_api_key = os.getenv("TIINGO_API_KEY")
        if tiingo_api_key:
            adapters["tiingo"] = TiingoAdapter(api_key=tiingo_api_key)
            logger.info("Initialized Tiingo adapter")
        else:
            logger.warning("TIINGO_API_KEY not set, Tiingo adapter not initialized")
        
        if not adapters:
            logger.error("No adapters initialized. Please set API keys.")
            return
        
        # Initialize LLM assistant
        llm_assistant = None
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if openai_api_key:
            llm_assistant = LLMAssistant(api_key=openai_api_key)
            logger.info("Initialized LLM assistant")
        else:
            logger.warning("OPENAI_API_KEY not set, LLM assistant not initialized")
        
        # Parse vendor priority
        vendor_priority = args.vendor_priority.split(",")
        
        # Initialize reconciliation engine
        reconciliation_engine = ReconciliationEngine(vendor_priority=vendor_priority)
        
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
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
