#!/usr/bin/env python3
"""
Run Instrument Data Agent

This script schedules and runs the instrument data agent to maintain instruments and instrument_xrefs tables.
It can be used for both one-time backfill and daily updates.
"""

import argparse
import asyncio
import logging
import os
import sys
import time
import schedule

# Add src to PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from core.platform.config_env.environment import Environment, EnvironmentType
from domains.market_data.services.agent.instrument_data_agent import InstrumentDataAgent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("instrument_agent_runner.log")
    ]
)
logger = logging.getLogger("instrument_agent_runner")


async def run_daily_update(environment_type: str, debug: bool = False, gin_config: str = None):
    """Run the daily instrument update."""
    logger.info("Starting scheduled daily instrument update")

    # Initialize environment
    env = Environment(gin_config, EnvironmentType(environment_type))

    # Create agent
    agent = InstrumentDataAgent(env, debug=debug)

    # Check if market is closed
    if not await agent.is_market_closed():
        logger.info("Market is still open. Waiting until market close.")
        return

    # Run daily update
    try:
        result = await agent.run_daily_update()
        logger.info(f"Daily update completed: {result['status']}")
    except Exception as e:
        logger.error(f"Error in daily update: {str(e)}", exc_info=True)


async def run_backfill(environment_type: str, debug: bool = False, gin_config: str = None):
    """Run the instrument backfill."""
    logger.info("Starting instrument backfill")

    # Initialize environment
    env = Environment(gin_config, EnvironmentType(environment_type))

    # Create agent
    agent = InstrumentDataAgent(env, debug=debug)

    # Run backfill
    try:
        result = await agent.run_backfill()
        logger.info(f"Backfill completed: {result['status']}")
    except Exception as e:
        logger.error(f"Error in backfill: {str(e)}", exc_info=True)


def schedule_daily_update(environment_type: str, debug: bool = False, gin_config: str = None):
    """Schedule the daily instrument update to run after market close."""
    # Schedule daily update at 4:30 PM ET (after market close)
    schedule.every().monday.at("16:30").do(
        lambda: asyncio.run(run_daily_update(environment_type, debug, gin_config))
    )
    schedule.every().tuesday.at("16:30").do(
        lambda: asyncio.run(run_daily_update(environment_type, debug, gin_config))
    )
    schedule.every().wednesday.at("16:30").do(
        lambda: asyncio.run(run_daily_update(environment_type, debug, gin_config))
    )
    schedule.every().thursday.at("16:30").do(
        lambda: asyncio.run(run_daily_update(environment_type, debug, gin_config))
    )
    schedule.every().friday.at("16:30").do(
        lambda: asyncio.run(run_daily_update(environment_type, debug, gin_config))
    )

    logger.info("Daily instrument update scheduled to run at 4:30 PM ET on weekdays")


def main():
    """Main entry point for the instrument agent runner."""
    parser = argparse.ArgumentParser(description="Run Instrument Data Agent")
    parser.add_argument("operation", choices=["backfill", "daily_update", "schedule"],
                        help="Operation to perform")
    parser.add_argument("--environment", type=str, default="intg",
                        choices=["test", "intg", "prod"], help="Environment to use")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--gin_config", type=str, default=None, help="Path to gin config file")

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if args.operation == "backfill":
        # Run backfill once
        asyncio.run(run_backfill(args.environment, args.debug, args.gin_config))

    elif args.operation == "daily_update":
        # Run daily update once
        asyncio.run(run_daily_update(args.environment, args.debug, args.gin_config))

    elif args.operation == "schedule":
        # Schedule daily updates
        schedule_daily_update(args.environment, args.debug, args.gin_config)

        # Keep the script running to execute scheduled tasks
        logger.info("Scheduler started. Press Ctrl+C to exit.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
