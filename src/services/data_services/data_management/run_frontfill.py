#!/usr/bin/env python3
"""
Frontfill Runner Script.
Command-line interface for running frontfill jobs.
"""

import asyncio
import logging
import argparse
import os
from typing import Dict

from core.platform.config.environment import Environment, EnvironmentType
from frontfill.frontfill_orchestrator import run_frontfill_orchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_orchestrator_mode(env: Environment, api_keys: Dict[str, str]):
    """Run the full orchestrator (daemon mode)."""
    logger.info("Starting Frontfill Orchestrator in daemon mode")
    await run_frontfill_orchestrator(env, api_keys)


async def run_single_job(env: Environment, api_keys: Dict[str, str],
                        job_type: str, vendor: str, dry_run: bool = False):
    """Run a single frontfill job."""
    from frontfill.frontfill_orchestrator import FrontfillOrchestrator

    logger.info(f"Running single job: {job_type}:{vendor}")

    orchestrator = FrontfillOrchestrator(env, api_keys)
    await orchestrator.initialize()

    try:
        stats = await orchestrator.run_manual_job(job_type, vendor)
        logger.info(f"Job completed successfully: {stats}")
        return True
    except Exception as e:
        logger.error(f"Job failed: {e}")
        return False
    finally:
        await orchestrator.stop()


async def run_daily_jobs(env: Environment, api_keys: Dict[str, str]):
    """Run all daily jobs (instruments + daily prices)."""
    from frontfill.frontfill_orchestrator import FrontfillOrchestrator

    logger.info("Running all daily jobs")

    orchestrator = FrontfillOrchestrator(env, api_keys)
    await orchestrator.initialize()

    success_count = 0
    total_jobs = 0

    try:
        # Run instruments job
        if api_keys.get("polygon"):
            try:
                await orchestrator.run_manual_job("instruments", "polygon")
                success_count += 1
            except Exception as e:
                logger.error(f"Instruments job failed: {e}")
            total_jobs += 1

        # Run daily prices jobs
        for vendor in ["polygon", "tiingo"]:
            if api_keys.get(vendor):
                try:
                    await orchestrator.run_manual_job("daily_prices", vendor)
                    success_count += 1
                except Exception as e:
                    logger.error(f"Daily prices {vendor} job failed: {e}")
                total_jobs += 1

        logger.info(f"Daily jobs completed: {success_count}/{total_jobs} successful")
        return success_count == total_jobs

    finally:
        await orchestrator.stop()


async def run_frequent_jobs(env: Environment, api_keys: Dict[str, str]):
    """Run all frequent jobs (news + economic events)."""
    from frontfill.frontfill_orchestrator import FrontfillOrchestrator

    logger.info("Running all frequent jobs")

    orchestrator = FrontfillOrchestrator(env, api_keys)
    await orchestrator.initialize()

    success_count = 0
    total_jobs = 0

    try:
        # Run news jobs
        for vendor in ["polygon", "tiingo"]:
            if api_keys.get(vendor):
                try:
                    await orchestrator.run_manual_job("news", vendor)
                    success_count += 1
                except Exception as e:
                    logger.error(f"News {vendor} job failed: {e}")
                total_jobs += 1

        # Run economic events jobs
        for vendor in ["polygon", "tiingo", "alpha_vantage", "fred"]:
            if api_keys.get(vendor):
                try:
                    await orchestrator.run_manual_job("economic_events", vendor)
                    success_count += 1
                except Exception as e:
                    logger.error(f"Economic events {vendor} job failed: {e}")
                total_jobs += 1

        logger.info(f"Frequent jobs completed: {success_count}/{total_jobs} successful")
        return success_count == total_jobs

    finally:
        await orchestrator.stop()


def get_api_keys_from_env() -> Dict[str, str]:
    """Get API keys from environment variables."""
    return {
        "polygon": os.getenv("POLYGON_API_KEY"),
        "tiingo": os.getenv("TIINGO_API_KEY"),
        "alpha_vantage": os.getenv("ALPHA_VANTAGE_API_KEY"),
        "fred": os.getenv("FRED_API_KEY"),
        "finnhub": os.getenv("FINNHUB_API_KEY")
    }


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Frontfill data ingestion system"
    )

    parser.add_argument(
        "--mode",
        choices=["orchestrator", "single", "daily", "frequent"],
        default="orchestrator",
        help="Execution mode"
    )

    parser.add_argument(
        "--environment",
        choices=["dev", "intg", "prod"],
        default="dev",
        help="Environment"
    )

    parser.add_argument(
        "--job-type",
        choices=["instruments", "daily_prices", "news", "economic_events"],
        help="Job type (for single mode)"
    )

    parser.add_argument(
        "--vendor",
        choices=["polygon", "tiingo", "alpha_vantage", "fred", "finnhub"],
        help="Vendor (for single mode)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode (don't actually insert data)"
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Log level"
    )

    args = parser.parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Get environment and API keys
    env_type = EnvironmentType(args.environment.upper())
    env = Environment(env_type)
    api_keys = get_api_keys_from_env()

    # Validate required API keys
    required_keys = []
    if args.mode == "single":
        if not args.job_type or not args.vendor:
            parser.error("--job-type and --vendor required for single mode")
        required_keys = [args.vendor]
    else:
        required_keys = ["polygon", "tiingo"]  # Minimum required

    missing_keys = [key for key in required_keys if not api_keys.get(key)]
    if missing_keys:
        logger.error(f"Missing API keys: {missing_keys}")
        logger.error("Set environment variables: POLYGON_API_KEY, TIINGO_API_KEY, etc.")
        exit(1)

    # Run the appropriate mode
    try:
        if args.mode == "orchestrator":
            success = asyncio.run(run_orchestrator_mode(env, api_keys))
        elif args.mode == "single":
            success = asyncio.run(run_single_job(
                env, api_keys, args.job_type, args.vendor, args.dry_run
            ))
        elif args.mode == "daily":
            success = asyncio.run(run_daily_jobs(env, api_keys))
        elif args.mode == "frequent":
            success = asyncio.run(run_frequent_jobs(env, api_keys))
        else:
            parser.error(f"Unknown mode: {args.mode}")

        exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()