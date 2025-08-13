#!/usr/bin/env python
"""
Enhanced Data Agent Runner

This script demonstrates the enhanced data agent with all monitoring and operational
improvements implemented. It sets up the data agent with:

1. Enhanced alerting (logging, Slack, email)
2. Prometheus metrics integration
3. Health API endpoints
4. Circuit breaker and retry logic
5. Configurable logging
6. Graceful shutdown handling

Usage:
    python -m src.market_data.agent.run_enhanced_data_agent [--mock] [--backfill] [--frontfill]
"""

import os
import sys
import asyncio
import logging
import argparse
import signal
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

# Import data agent components
from market_data.agent.data_agent_orchestrator import DataAgentOrchestrator
from market_data.agent.reconciliation import ReconciliationEngine
from market_data.agent.mock_adapter import MockAdapter
from market_data.agent.tiingo_adapter import TiingoAdapter
from market_data.agent.polygon_adapter import PolygonAdapter
from market_data.agent.alert_handlers import (
    LoggingAlertHandler, SlackAlertHandler, EmailAlertHandler, CompositeAlertHandler
)
from market_data.agent.logging_config import setup_logging

# Set up logger
logger = logging.getLogger(__name__)

# Global variable to store the orchestrator for graceful shutdown
orchestrator = None

async def setup_data_agent(args: argparse.Namespace) -> DataAgentOrchestrator:
    """
    Set up the data agent orchestrator with all enhancements.
    
    Args:
        args: Command line arguments
        
    Returns:
        DataAgentOrchestrator instance
    """
    # Set up logging
    setup_logging(
        log_level=args.log_level,
        log_file=args.log_file,
        json_format=args.json_logs
    )
    
    # Create adapters
    if args.mock:
        logger.info("Using mock adapters")
        adapters = {
            "mock_tiingo": MockAdapter("tiingo", success_rate=0.95),
            "mock_polygon": MockAdapter("polygon", success_rate=0.9)
        }
    else:
        logger.info("Using real adapters")
        # Check for API keys
        tiingo_api_key = os.environ.get("TIINGO_API_KEY")
        polygon_api_key = os.environ.get("POLYGON_API_KEY")
        
        if not tiingo_api_key or not polygon_api_key:
            logger.error("Missing API keys. Set TIINGO_API_KEY and POLYGON_API_KEY environment variables.")
            logger.error("Falling back to mock adapters.")
            adapters = {
                "mock_tiingo": MockAdapter("tiingo", success_rate=0.95),
                "mock_polygon": MockAdapter("polygon", success_rate=0.9)
            }
        else:
            adapters = {
                "tiingo": TiingoAdapter(tiingo_api_key),
                "polygon": PolygonAdapter(polygon_api_key)
            }
    
    # Create reconciliation engine
    reconciliation_engine = ReconciliationEngine()
    
    # Set up alert handlers
    alert_handler = CompositeAlertHandler()
    alert_handler.add_handler(LoggingAlertHandler())
    
    # Add Slack handler if configured
    if os.environ.get("SLACK_WEBHOOK_URL"):
        alert_handler.add_handler(SlackAlertHandler())
        logger.info("Slack alerting enabled")
    
    # Add Email handler if configured
    if os.environ.get("ALERT_EMAIL_RECIPIENTS"):
        alert_handler.add_handler(EmailAlertHandler())
        logger.info("Email alerting enabled")
    
    # Database connection string
    db_connection_string = os.environ.get(
        "DATABASE_URL", 
        "postgresql://postgres:postgres@localhost:5432/market_data"
    )
    
    # Create orchestrator with all enhancements
    orchestrator = await DataAgentOrchestrator.create(
        db_connection_string=db_connection_string,
        adapters=adapters,
        reconciliation_engine=reconciliation_engine,
        lookback_years=args.lookback_years,
        enable_monitoring=True,
        alert_handler=alert_handler,
        enable_prometheus=args.prometheus,
        prometheus_port=args.prometheus_port,
        max_retries=args.max_retries,
        enable_circuit_breaker=args.circuit_breaker,
        enable_health_api=args.health_api,
        health_api_port=args.health_api_port,
        log_level=args.log_level,
        log_file=args.log_file,
        json_logs=args.json_logs
    )
    
    logger.info("Data agent orchestrator created with all enhancements")
    return orchestrator

async def run_data_agent(args: argparse.Namespace) -> None:
    """
    Run the data agent with the specified operations.
    
    Args:
        args: Command line arguments
    """
    global orchestrator
    orchestrator = await setup_data_agent(args)
    
    try:
        if args.backfill:
            logger.info("Starting backfill operation")
            await orchestrator.run_backfill_loop(
                batch_size=args.batch_size,
                max_iterations=args.max_iterations
            )
            logger.info("Backfill operation completed")
        
        if args.frontfill:
            logger.info("Starting frontfill operation")
            await orchestrator.run_frontfill_loop()
            logger.info("Frontfill operation completed")
            
        if not args.backfill and not args.frontfill:
            logger.info("No operation specified, running in monitoring-only mode")
            # Keep the process running to serve metrics and health API
            while True:
                await asyncio.sleep(10)
                
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully")
    except Exception as e:
        logger.error(f"Error running data agent: {e}", exc_info=True)
    finally:
        if orchestrator:
            await orchestrator.shutdown()

def signal_handler(sig, frame):
    """Handle termination signals for graceful shutdown."""
    logger.info(f"Received signal {sig}, initiating graceful shutdown")
    if orchestrator:
        # Create a new event loop for the shutdown
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(orchestrator.shutdown())
        finally:
            loop.close()
    sys.exit(0)

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Enhanced Data Agent Runner")
    
    # Operation modes
    parser.add_argument("--mock", action="store_true", help="Use mock adapters")
    parser.add_argument("--backfill", action="store_true", help="Run backfill operation")
    parser.add_argument("--frontfill", action="store_true", help="Run frontfill operation")
    
    # Operational parameters
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    parser.add_argument("--max-iterations", type=int, default=None, help="Maximum iterations for backfill")
    parser.add_argument("--lookback-years", type=int, default=5, help="Years to look back for historical data")
    parser.add_argument("--max-retries", type=int, default=3, help="Maximum retries for data fetching")
    
    # Monitoring and operational features
    parser.add_argument("--prometheus", action="store_true", help="Enable Prometheus metrics")
    parser.add_argument("--prometheus-port", type=int, default=8000, help="Prometheus metrics port")
    parser.add_argument("--health-api", action="store_true", help="Enable health API")
    parser.add_argument("--health-api-port", type=int, default=8081, help="Health API port")
    parser.add_argument("--circuit-breaker", action="store_true", help="Enable circuit breaker")
    
    # Logging configuration
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        default="INFO", help="Logging level")
    parser.add_argument("--log-file", help="Log file path")
    parser.add_argument("--json-logs", action="store_true", help="Use JSON log format")
    
    return parser.parse_args()

def main():
    """Main entry point."""
    # Parse command line arguments
    args = parse_args()
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run the data agent
    asyncio.run(run_data_agent(args))

if __name__ == "__main__":
    main()
