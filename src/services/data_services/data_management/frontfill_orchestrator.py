#!/usr/bin/env python3
"""
Frontfill Orchestrator.
Manages and schedules all frontfill jobs with proper timing and coordination.
"""

import asyncio
import logging
from datetime import datetime, time, timedelta
from typing import Dict, Any
from dataclasses import dataclass
import signal

from core.platform.config.environment import Environment
from core.config.database import get_connection_pool
from frontfill.checkpoint_manager import CheckpointManager
from frontfill.daily_prices_frontfill import create_daily_prices_frontfill_jobs
from frontfill.news_frontfill import create_news_frontfill_jobs
from frontfill.economic_events_frontfill import (
    create_economic_events_frontfill_jobs,
    create_instruments_frontfill_job
)
from frontfill.validation_integration import ValidationIntegration, ValidationConfig

logger = logging.getLogger(__name__)


@dataclass
class ScheduleConfig:
    """Configuration for scheduled job execution."""
    job_name: str
    cron_expression: str  # Simple cron-like expression
    enabled: bool = True
    max_runtime_minutes: int = 60
    retry_on_failure: bool = True
    max_retries: int = 3


class FrontfillOrchestrator:
    """Orchestrates all frontfill jobs with proper scheduling."""

    def __init__(self, env: Environment, api_keys: Dict[str, str]):
        self.env = env
        self.api_keys = api_keys
        self.connection_pool = None
        self.checkpoint_manager = None
        self.validation_integration = None
        self.running = False
        self.tasks = []

        # Job registry
        self.daily_jobs = []  # Run once after market close
        self.frequent_jobs = []  # Run every 5 minutes

        # Validation configuration
        self.validation_config = ValidationConfig(
            enable_post_frontfill_validation=True,
            enable_missing_data_detection=True,
            enable_automatic_backfill=True,
            quality_threshold=80.0,
            backfill_priority_threshold=3,
            max_concurrent_backfills=5,
            validation_delay_hours=1  # Run validation 1 hour after daily jobs
        )

        # Schedule configurations
        self.schedules = {
            # Daily jobs (after market close at 6:30 PM EST)
            "instruments_update": ScheduleConfig(
                job_name="instruments_update",
                cron_expression="30 18 * * 1-5",  # 6:30 PM Monday-Friday
                max_runtime_minutes=30
            ),
            "daily_price_polygon": ScheduleConfig(
                job_name="daily_price_polygon",
                cron_expression="00 19 * * 1-5",  # 7:00 PM Monday-Friday
                max_runtime_minutes=120
            ),
            "daily_price_tiingo": ScheduleConfig(
                job_name="daily_price_tiingo",
                cron_expression="30 19 * * 1-5",  # 7:30 PM Monday-Friday
                max_runtime_minutes=120
            ),
            "post_frontfill_validation": ScheduleConfig(
                job_name="post_frontfill_validation",
                cron_expression="00 21 * * 1-5",  # 9:00 PM Monday-Friday (after daily jobs)
                max_runtime_minutes=30
            ),

            # Frequent jobs (every 5 minutes during market hours + extended)
            "news_polygon": ScheduleConfig(
                job_name="news_polygon",
                cron_expression="*/5 * * * *",  # Every 5 minutes
                max_runtime_minutes=10
            ),
            "news_tiingo": ScheduleConfig(
                job_name="news_tiingo",
                cron_expression="*/5 * * * *",  # Every 5 minutes
                max_runtime_minutes=10
            ),
            "economic_events_update": ScheduleConfig(
                job_name="economic_events_update",
                cron_expression="*/5 * * * *",  # Every 5 minutes
                max_runtime_minutes=15
            )
        }

    async def initialize(self):
        """Initialize the orchestrator and all jobs."""
        logger.info("Initializing Frontfill Orchestrator")

        # Get database connection
        self.connection_pool = await get_connection_pool(self.env)
        self.checkpoint_manager = CheckpointManager(self.connection_pool, self.env)

        # Initialize validation integration
        self.validation_integration = ValidationIntegration(
            self.connection_pool, self.env, self.api_keys, self.validation_config
        )
        await self.validation_integration.initialize()

        # Initialize checkpoint tables
        await self.checkpoint_manager.initialize_tables()

        # Create job instances
        await self._create_job_instances()

        logger.info(f"Initialized {len(self.daily_jobs)} daily jobs and {len(self.frequent_jobs)} frequent jobs")

    async def _create_job_instances(self):
        """Create all job instances."""
        # Create daily prices jobs
        if self.api_keys.get("polygon") and self.api_keys.get("tiingo"):
            daily_price_jobs = await create_daily_prices_frontfill_jobs(
                self.connection_pool, self.env,
                self.api_keys["polygon"], self.api_keys["tiingo"]
            )
            self.daily_jobs.extend(daily_price_jobs)

        # Create instruments job
        if self.api_keys.get("polygon"):
            instruments_job = await create_instruments_frontfill_job(
                self.connection_pool, self.env, self.api_keys["polygon"]
            )
            self.daily_jobs.append(instruments_job)

        # Create news jobs
        news_jobs = await create_news_frontfill_jobs(
            self.connection_pool, self.env,
            self.api_keys.get("polygon", ""),
            self.api_keys.get("tiingo", ""),
            self.api_keys.get("finnhub")
        )
        self.frequent_jobs.extend(news_jobs)

        # Create economic events jobs
        economic_events_jobs = await create_economic_events_frontfill_jobs(
            self.connection_pool, self.env,
            self.api_keys.get("polygon"),
            self.api_keys.get("tiingo"),
            self.api_keys.get("alpha_vantage"),
            self.api_keys.get("fred")
        )
        self.frequent_jobs.extend(economic_events_jobs)

    async def start(self):
        """Start the orchestrator."""
        logger.info("Starting Frontfill Orchestrator")
        self.running = True

        # Start schedulers
        self.tasks = [
            asyncio.create_task(self._daily_job_scheduler()),
            asyncio.create_task(self._frequent_job_scheduler()),
            asyncio.create_task(self._validation_scheduler()),
            asyncio.create_task(self._monitoring_task())
        ]

        # Wait for all tasks
        try:
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            logger.info("Orchestrator tasks cancelled")

    async def stop(self):
        """Stop the orchestrator gracefully."""
        logger.info("Stopping Frontfill Orchestrator")
        self.running = False

        # Cancel all tasks
        for task in self.tasks:
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*self.tasks, return_exceptions=True)

        # Close database connection
        if self.connection_pool:
            await self.connection_pool.close()

        logger.info("Orchestrator stopped")

    async def _daily_job_scheduler(self):
        """Scheduler for daily jobs (after market close)."""
        logger.info("Starting daily job scheduler")

        while self.running:
            try:
                current_time = datetime.now().time()
                current_weekday = datetime.now().weekday()  # 0=Monday, 6=Sunday

                # Only run on weekdays (Monday=0 to Friday=4)
                if current_weekday < 5:
                    # Check if it's time for daily jobs (after 6:30 PM)
                    market_close_time = time(18, 30)  # 6:30 PM

                    if current_time >= market_close_time:
                        await self._run_daily_jobs()

                        # Wait until next day to avoid running multiple times
                        await self._wait_until_next_day()

                # Check every hour during non-market days
                await asyncio.sleep(3600)  # 1 hour

            except Exception as e:
                logger.error(f"Error in daily job scheduler: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying

    async def _frequent_job_scheduler(self):
        """Scheduler for frequent jobs (every 5 minutes)."""
        logger.info("Starting frequent job scheduler")

        while self.running:
            try:
                # Run frequent jobs every 5 minutes
                await self._run_frequent_jobs()

                # Wait 5 minutes
                await asyncio.sleep(300)

            except Exception as e:
                logger.error(f"Error in frequent job scheduler: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying

    async def _run_daily_jobs(self):
        """Run all daily jobs."""
        logger.info("Running daily jobs")

        for job in self.daily_jobs:
            try:
                job_name = job.config.job_name
                logger.info(f"Starting daily job: {job_name}")

                # Initialize job
                await job.initialize()

                # Run job
                stats = await job.run_frontfill()

                logger.info(f"Completed daily job {job_name}: {stats}")

            except Exception as e:
                logger.error(f"Error running daily job {job.config.job_name}: {e}")

        # Schedule validation to run after daily jobs complete
        await self._schedule_post_frontfill_validation()

    async def _run_frequent_jobs(self):
        """Run all frequent jobs."""
        logger.debug("Running frequent jobs")

        # Run jobs in parallel for efficiency
        tasks = []
        for job in self.frequent_jobs:
            task = asyncio.create_task(self._run_single_frequent_job(job))
            tasks.append(task)

        # Wait for all jobs to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log any errors
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                job_name = self.frequent_jobs[i].config.job_name
                logger.error(f"Error in frequent job {job_name}: {result}")

    async def _run_single_frequent_job(self, job):
        """Run a single frequent job with timeout."""
        job_name = job.config.job_name

        try:
            # Initialize job
            await job.initialize()

            # Run with timeout
            stats = await asyncio.wait_for(
                job.run_frontfill(),
                timeout=600  # 10 minute timeout for frequent jobs
            )

            logger.debug(f"Completed frequent job {job_name}: processed {stats.get('records_processed', 0)} records")

        except asyncio.TimeoutError:
            logger.warning(f"Frequent job {job_name} timed out")
        except Exception as e:
            logger.error(f"Error in frequent job {job_name}: {e}")

    async def _monitoring_task(self):
        """Background monitoring task."""
        logger.info("Starting monitoring task")

        while self.running:
            try:
                # Clean up old job runs every hour
                await self.checkpoint_manager.cleanup_old_job_runs(days_to_keep=7)

                # Log system status every 30 minutes
                await self._log_system_status()

                # Wait 30 minutes
                await asyncio.sleep(1800)

            except Exception as e:
                logger.error(f"Error in monitoring task: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying

    async def _log_system_status(self):
        """Log system status and statistics."""
        try:
            # Get recent job runs
            recent_runs = await self.checkpoint_manager.get_recent_job_runs(limit=20)

            # Count by status
            status_counts = {}
            for run in recent_runs:
                status = run.status.value
                status_counts[status] = status_counts.get(status, 0) + 1

            logger.info(f"System Status - Recent job runs: {status_counts}")

            # Log job statistics
            total_processed = sum(run.records_processed for run in recent_runs)
            total_inserted = sum(run.records_inserted for run in recent_runs)

            logger.info(f"System Status - Records processed: {total_processed}, inserted: {total_inserted}")

        except Exception as e:
            logger.error(f"Error logging system status: {e}")

    async def _validation_scheduler(self):
        """Scheduler for validation tasks."""
        logger.info("Starting validation scheduler")

        while self.running:
            try:
                current_time = datetime.now().time()
                current_weekday = datetime.now().weekday()

                # Only run on weekdays after validation time (9:00 PM)
                if current_weekday < 5:
                    validation_time = time(21, 0)  # 9:00 PM

                    if current_time >= validation_time:
                        await self._run_post_frontfill_validation()

                        # Wait until next day
                        await self._wait_until_next_day()

                # Check every hour
                await asyncio.sleep(3600)

            except Exception as e:
                logger.error(f"Error in validation scheduler: {e}")
                await asyncio.sleep(300)

    async def _schedule_post_frontfill_validation(self):
        """Schedule validation to run after daily jobs with delay."""
        delay_seconds = self.validation_config.validation_delay_hours * 3600
        logger.info(f"Scheduling post-frontfill validation in {delay_seconds/3600:.1f} hours")

        # Schedule validation task
        asyncio.create_task(self._delayed_validation(delay_seconds))

    async def _delayed_validation(self, delay_seconds: int):
        """Run validation after a delay."""
        await asyncio.sleep(delay_seconds)

        if self.running:
            await self._run_post_frontfill_validation()

    async def _run_post_frontfill_validation(self):
        """Run post-frontfill validation."""
        try:
            # Get yesterday's date for validation
            validation_date = datetime.now().date() - timedelta(days=1)

            logger.info(f"Running post-frontfill validation for {validation_date}")

            # Run validation
            results = await self.validation_integration.run_post_frontfill_validation(
                validation_date
            )

            # Log results
            quality_score = results["quality_score"]
            passed = results["validation_passed"]
            actions = results["actions_taken"]

            logger.info(f"Validation completed - Quality: {quality_score:.2f}, "
                       f"Passed: {passed}, Actions: {actions}")

            # Log critical issues
            if not passed:
                logger.warning(f"Validation failed for {validation_date} - "
                              f"quality score {quality_score:.2f} below threshold")

            # Store validation results in checkpoint
            await self.checkpoint_manager.update_checkpoint(
                "daily_validation_results",
                validation_date.isoformat(),
                {
                    "quality_score": quality_score,
                    "validation_passed": passed,
                    "actions_taken": actions,
                    "backfill_results": results.get("backfill_results")
                }
            )

        except Exception as e:
            logger.error(f"Error in post-frontfill validation: {e}")

    async def _wait_until_next_day(self):
        """Wait until the next day."""
        now = datetime.now()
        next_day = now.replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(days=1)
        wait_seconds = (next_day - now).total_seconds()

        logger.info(f"Waiting {wait_seconds/3600:.1f} hours until next day")
        await asyncio.sleep(wait_seconds)

    async def run_manual_job(self, job_type: str, vendor: str) -> Dict[str, Any]:
        """Run a specific job manually."""
        logger.info(f"Running manual job: {job_type}:{vendor}")

        # Find the job
        all_jobs = self.daily_jobs + self.frequent_jobs
        target_job = None

        for job in all_jobs:
            if job.config.job_type == job_type and job.config.vendor == vendor:
                target_job = job
                break

        if not target_job:
            raise ValueError(f"Job not found: {job_type}:{vendor}")

        # Initialize and run
        await target_job.initialize()
        stats = await target_job.run_frontfill()

        logger.info(f"Manual job completed: {stats}")
        return stats

    async def run_manual_validation(self, validation_date: date = None) -> Dict[str, Any]:
        """Run validation manually for a specific date."""
        if validation_date is None:
            validation_date = datetime.now().date() - timedelta(days=1)

        logger.info(f"Running manual validation for {validation_date}")

        if not self.validation_integration:
            raise RuntimeError("Validation integration not initialized")

        results = await self.validation_integration.run_post_frontfill_validation(validation_date)
        logger.info(f"Manual validation completed: quality score {results['quality_score']:.2f}")

        return results

    async def get_job_status(self) -> Dict[str, Any]:
        """Get status of all jobs."""
        status = {
            "orchestrator_running": self.running,
            "daily_jobs": len(self.daily_jobs),
            "frequent_jobs": len(self.frequent_jobs),
            "validation_enabled": self.validation_integration is not None,
            "recent_runs": [],
            "recent_validations": []
        }

        # Get recent job runs
        recent_runs = await self.checkpoint_manager.get_recent_job_runs(limit=10)
        for run in recent_runs:
            status["recent_runs"].append({
                "job_name": run.job_name,
                "vendor": run.vendor,
                "status": run.status.value,
                "start_time": run.start_time.isoformat(),
                "records_processed": run.records_processed,
                "records_inserted": run.records_inserted
            })

        # Get recent validation results
        try:
            validation_checkpoints = await self.checkpoint_manager.get_checkpoints_by_job(
                "daily_validation_results", limit=5
            )
            for checkpoint in validation_checkpoints:
                checkpoint_data = checkpoint.checkpoint_data
                status["recent_validations"].append({
                    "date": checkpoint_data.get("validation_date"),
                    "quality_score": checkpoint_data.get("quality_score"),
                    "validation_passed": checkpoint_data.get("validation_passed"),
                    "actions_taken": checkpoint_data.get("actions_taken", [])
                })
        except Exception as e:
            logger.warning(f"Could not get recent validation results: {e}")

        return status


# Signal handling for graceful shutdown
async def signal_handler(orchestrator: FrontfillOrchestrator, signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    await orchestrator.stop()


# Main execution function
async def run_frontfill_orchestrator(env: Environment, api_keys: Dict[str, str]):
    """Main function to run the frontfill orchestrator."""
    orchestrator = FrontfillOrchestrator(env, api_keys)

    # Set up signal handlers for graceful shutdown
    def signal_callback(signum, frame):
        asyncio.create_task(signal_handler(orchestrator, signum, frame))

    signal.signal(signal.SIGINT, signal_callback)
    signal.signal(signal.SIGTERM, signal_callback)

    try:
        # Initialize and start
        await orchestrator.initialize()
        await orchestrator.start()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Error running orchestrator: {e}")
        raise
    finally:
        await orchestrator.stop()