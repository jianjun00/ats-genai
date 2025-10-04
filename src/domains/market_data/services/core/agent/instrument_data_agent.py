"""
Instrument Data Agent

This module implements an agentic workflow for maintaining instruments and instrument_xrefs tables.
It handles both one-time backfill and daily updates after market close.
"""

import argparse
import asyncio
import logging
from datetime import datetime, date, time, timedelta
import os
import sys
import traceback
from datetime import datetime, date, time, timedelta
from pathlib import Path

from core.platform.config.environment import Environment, EnvironmentType
from infrastructure.vendor.polygon.dao.instrument_polygon_dao import InstrumentPolygonDAO
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
from infrastructure.database.repositories.vendors_dao import VendorsDAO

# Import the existing populate functions
from infrastructure.vendor.polygon.services.populate_instrument_polygon import fetch_and_store_instruments
from domains.instruments.services.populate_unified_instruments import populate_unified_instruments

# Add src to PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# Configure logging
def setup_logging():
    """Set up logging based on environment."""
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format
    )

    # Add handler for dev environment
    if os.environ.get('ENVIRONMENT') == 'dev':
        # Create logs directory if it doesn't exist
        log_dir = Path('/var/log')
        if not log_dir.exists():
            try:
                log_dir = Path('./logs')
                log_dir.mkdir(exist_ok=True)
            except Exception as e:
                logging.warning(f"Could not create logs directory: {str(e)}")
                return

        try:
            # Add file handler for persistent logs
            log_file = log_dir / 'instrument_agent.log'
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(log_format))
            logging.getLogger().addHandler(file_handler)
            logging.info(f"Logging to file: {log_file}")
        except Exception as e:
            logging.warning(f"Could not set up file logging: {str(e)}")
# Initialize logger
setup_logging()
logger = logging.getLogger("instrument_data_agent")


class UpdateStatus(Enum):
    """Status of an instrument update operation."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class UpdateType(Enum):
    """Type of update operation."""
    BACKFILL = "backfill"
    DAILY_UPDATE = "daily_update"


class InstrumentUpdatePlan:
    """
    Represents a plan for updating instrument data.
    """
    def __init__(self, update_type: UpdateType, start_date: date, end_date: date):
        self.update_type = update_type
        self.start_date = start_date
        self.end_date = end_date
        self.status = UpdateStatus.PENDING
        self.steps = []
        self.results = {}
        self.created_at = datetime.now()
        self.updated_at = datetime.now()

        # Initialize steps based on update type
        if update_type == UpdateType.BACKFILL:
            self.steps = [
                {"name": "populate_instrument_polygon", "status": UpdateStatus.PENDING},
                {"name": "populate_unified_instruments", "status": UpdateStatus.PENDING},
                {"name": "generate_report", "status": UpdateStatus.PENDING}
            ]
        elif update_type == UpdateType.DAILY_UPDATE:
            self.steps = [
                {"name": "populate_instrument_polygon", "status": UpdateStatus.PENDING},
                {"name": "populate_unified_instruments", "status": UpdateStatus.PENDING},
                {"name": "generate_report", "status": UpdateStatus.PENDING}
            ]

    def update_step_status(self, step_name: str, status: UpdateStatus, result: Dict = None):
        """Update the status of a step in the plan."""
        for step in self.steps:
            if step["name"] == step_name:
                step["status"] = status
                if result:
                    self.results[step_name] = result
                self.updated_at = datetime.now()
                return True
        return False

    def get_next_pending_step(self) -> Optional[Dict]:
        """Get the next pending step in the plan."""
        for step in self.steps:
            if step["status"] == UpdateStatus.PENDING:
                return step
        return None

    def is_completed(self) -> bool:
        """Check if all steps in the plan are completed."""
        return all(step["status"] == UpdateStatus.COMPLETED for step in self.steps)

    def to_dict(self) -> Dict:
        """Convert the plan to a dictionary."""
        return {
            "update_type": self.update_type.value,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "status": self.status.value,
            "steps": [
                {
                    "name": step["name"],
                    "status": step["status"].value
                }
                for step in self.steps
            ],
            "results": self.results,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


class InstrumentDataAgent:
    """
    Agent responsible for maintaining instruments and instrument_xrefs tables.
    """
    def __init__(self, environment: Environment, debug: bool = False):
        self.env = environment
        self.debug = debug
        self.logger = logger
        if debug:
            self.logger.setLevel(logging.DEBUG)

        # Initialize DAOs
        self.polygon_dao = InstrumentPolygonDAO(self.env)
        self.instruments_dao = InstrumentsDAO(self.env)
        self.xrefs_dao = InstrumentXrefsDAO(self.env)
        self.vendors_dao = VendorsDAO(self.env)

        # Current update plan
        self.current_plan = None

    async def create_backfill_plan(self) -> InstrumentUpdatePlan:
        """Create a plan for backfilling instrument data."""
        today = date.today()
        plan = InstrumentUpdatePlan(
            update_type=UpdateType.BACKFILL,
            start_date=None,  # Backfill doesn't need specific dates
            end_date=today
        )
        self.current_plan = plan
        return plan

    async def create_daily_update_plan(self) -> InstrumentUpdatePlan:
        """Create a plan for daily instrument updates."""
        today = date.today()
        plan = InstrumentUpdatePlan(
            update_type=UpdateType.DAILY_UPDATE,
            start_date=today - timedelta(days=7),  # Look back a week to catch any missed updates
            end_date=today
        )
        self.current_plan = plan
        return plan

    async def execute_plan(self, plan: InstrumentUpdatePlan):
        """Execute the given instrument update plan."""
        self.logger.info(f"Executing {plan.update_type} plan from {plan.start_date} to {plan.end_date}")

        # Track overall success/failure
        plan_success = True
        is_dev_env = os.environ.get('ENVIRONMENT') == 'dev'
        dry_run = os.environ.get('DRY_RUN', 'false').lower() == 'true'

        if dry_run:
            self.logger.info("DRY RUN MODE: No actual changes will be made to the database")

        while not plan.is_completed():
            step = plan.get_next_pending_step()
            if not step:
                break

            step_name = step["name"]
            self.logger.info(f"Executing step: {step_name}")

            # Update step status to in progress
            plan.update_step_status(step_name, UpdateStatus.IN_PROGRESS)

            try:
                # Skip actual execution in dry run mode
                if dry_run and step_name != "generate_report":
                    self.logger.info(f"DRY RUN: Skipping actual execution of {step_name}")
                    result = {"status": "skipped", "reason": "dry_run"}
                else:
                    # Execute step
                    if step_name == "populate_instrument_polygon":
                        result = await self._execute_populate_instrument_polygon()
                    elif step_name == "populate_unified_instruments":
                        result = await self._execute_populate_unified_instruments()
                    elif step_name == "generate_report":
                        result = await self._generate_report(plan)
                    else:
                        raise ValueError(f"Unknown step: {step_name}")

                # Update step status to completed
                plan.update_step_status(step_name, UpdateStatus.COMPLETED, result)
                self.logger.info(f"Step {step_name} completed")

            except Exception as e:
                plan_success = False
                error_msg = f"Step {step_name} failed: {str(e)}"
                self.logger.error(error_msg)

                # Enhanced error logging for dev environment
                if is_dev_env:
                    self.logger.error("Detailed error information:")
                    self.logger.error(traceback.format_exc())

                    # Save error details to file in dev environment
                    try:
                        error_dir = Path('./logs/errors')
                        error_dir.mkdir(exist_ok=True, parents=True)
                        error_file = error_dir / f"instrument_agent_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
                        with open(error_file, 'w') as f:
                            f.write(f"Error in step {step_name} at {datetime.now()}\n")
                            f.write(f"Error message: {str(e)}\n\n")
                            f.write(traceback.format_exc())
                        self.logger.info(f"Error details saved to {error_file}")
                    except Exception as log_error:
                        self.logger.warning(f"Failed to save error details: {str(log_error)}")

                plan.update_step_status(step_name, UpdateStatus.FAILED, {"error": str(e)})

                # In dev environment, continue with next step instead of failing completely
                if is_dev_env:
                    self.logger.warning("Continuing with next step due to dev environment setting")
                    continue
                else:
                    # In production, fail the entire plan
                    raise

        status = "success" if plan_success else "partial_success"
        self.logger.info(f"Plan execution completed with status: {status}")

        # Update plan status
        if plan_success:
            plan.status = UpdateStatus.COMPLETED
            self.logger.info(f"Plan completed successfully")
        else:
            plan.status = UpdateStatus.FAILED
            self.logger.error(f"Plan failed")

        return plan.to_dict()

    async def _execute_populate_instrument_polygon(self) -> Dict:
        """Execute the populate_instrument_polygon step."""
        start_time = datetime.now()
        self.logger.info("Starting populate_instrument_polygon")

        try:
            # Call the existing function to populate instrument_polygon
            await fetch_and_store_instruments()

            # Get stats on the number of instruments in the table
            count = await self.polygon_dao.count_instruments()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "instrument_count": count,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }

            self.logger.info(f"populate_instrument_polygon completed: {count} instruments, {duration:.2f} seconds")
            return result

        except Exception as e:
            self.logger.error(f"Error in populate_instrument_polygon: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat()
            }

    async def _execute_populate_unified_instruments(self) -> Dict:
        """Execute the populate_unified_instruments step."""
        start_time = datetime.now()
        self.logger.info("Starting populate_unified_instruments")

        try:
            # Call the existing function to populate unified instruments
            await populate_unified_instruments(
                self.polygon_dao,
                self.instruments_dao,
                self.xrefs_dao,
                self.vendors_dao,
                tickers=None,
                debug=self.debug
            )

            # Get stats on the number of instruments in the tables
            instruments_count = await self.instruments_dao.count_instruments()
            xrefs_count = await self.xrefs_dao.count_xrefs()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = {
                "status": "success",
                "instruments_count": instruments_count,
                "xrefs_count": xrefs_count,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }

            self.logger.info(f"populate_unified_instruments completed: {instruments_count} instruments, {xrefs_count} xrefs, {duration:.2f} seconds")
            return result

        except Exception as e:
            self.logger.error(f"Error in populate_unified_instruments: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat()
            }

    async def _generate_report(self, plan: InstrumentUpdatePlan) -> Dict:
        """Generate a report of the instrument update."""
        self.logger.info("Generating report")

        # Get the results from previous steps
        polygon_result = plan.results.get("populate_instrument_polygon", {})
        unified_result = plan.results.get("populate_unified_instruments", {})

        # Calculate statistics
        polygon_count = polygon_result.get("instrument_count", 0)
        instruments_count = unified_result.get("instruments_count", 0)
        xrefs_count = unified_result.get("xrefs_count", 0)

        # Generate report
        report = {
            "update_type": plan.update_type.value,
            "start_date": plan.start_date.isoformat() if plan.start_date else None,
            "end_date": plan.end_date.isoformat() if plan.end_date else None,
            "polygon_instruments": polygon_count,
            "unified_instruments": instruments_count,
            "instrument_xrefs": xrefs_count,
            "new_instruments": instruments_count - (plan.results.get("previous_instruments_count", 0) or 0),
            "new_xrefs": xrefs_count - (plan.results.get("previous_xrefs_count", 0) or 0),
            "total_duration_seconds": sum(
                step_result.get("duration_seconds", 0)
                for step_result in plan.results.values()
                if isinstance(step_result, dict)
            ),
            "timestamp": datetime.now().isoformat()
        }

        # Save report to file
        report_dir = "reports"
        os.makedirs(report_dir, exist_ok=True)
        report_file = f"{report_dir}/instrument_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        import json
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)

        self.logger.info(f"Report generated and saved to {report_file}")

        return {
            "status": "success",
            "report": report,
            "report_file": report_file
        }

    async def run_backfill(self) -> Dict:
        """Run a one-time backfill of instrument data."""
        self.logger.info("Starting instrument backfill")

        # Get initial counts for reporting
        try:
            previous_instruments_count = await self.instruments_dao.count_instruments()
            previous_xrefs_count = await self.xrefs_dao.count_xrefs()
        except Exception:
            previous_instruments_count = 0
            previous_xrefs_count = 0

        # Create and execute backfill plan
        plan = await self.create_backfill_plan()
        plan.results["previous_instruments_count"] = previous_instruments_count
        plan.results["previous_xrefs_count"] = previous_xrefs_count

        result = await self.execute_plan(plan)
        self.logger.info("Backfill completed")

        return result

    async def run_daily_update(self) -> Dict:
        """Run daily update of instrument data."""
        self.logger.info("Starting daily instrument update")

        # Get initial counts for reporting
        try:
            previous_instruments_count = await self.instruments_dao.count_instruments()
            previous_xrefs_count = await self.xrefs_dao.count_xrefs()
        except Exception:
            previous_instruments_count = 0
            previous_xrefs_count = 0

        # Create and execute daily update plan
        plan = await self.create_daily_update_plan()
        plan.results["previous_instruments_count"] = previous_instruments_count
        plan.results["previous_xrefs_count"] = previous_xrefs_count

        result = await self.execute_plan(plan)
        self.logger.info("Daily update completed")

        return result

    async def is_market_closed(self) -> bool:
        """Check if the market is closed."""
        # Simple implementation - check if current time is after 4:00 PM ET
        # In a real implementation, you would use a market calendar or API
        now = datetime.now()
        market_close_time = time(16, 0)  # 4:00 PM

        return now.time() >= market_close_time


async def main(environment_type: str, operation: str, debug: bool = False, gin_config: str = None):
    """Main entry point for the instrument data agent."""
    # Initialize environment
    env = Environment(gin_config, EnvironmentType(environment_type))

    # Create agent
    agent = InstrumentDataAgent(env, debug=debug)

    # Execute requested operation
    if operation == "backfill":
        result = await agent.run_backfill()
        print(f"Backfill completed: {result['status']}")

    elif operation == "daily_update":
        # Check if market is closed
        if not await agent.is_market_closed():
            print("Market is still open. Daily update should run after market close.")
            if not debug:
                return
            print("Running anyway because debug mode is enabled.")

        result = await agent.run_daily_update()
        print(f"Daily update completed: {result['status']}")

    else:
        print(f"Unknown operation: {operation}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Instrument Data Agent for maintaining instruments and instrument_xrefs tables")
    parser.add_argument("operation", choices=["backfill", "daily_update"], help="Operation to perform")
    parser.add_argument("--environment", type=str, default="intg", choices=["test", "intg", "prod"], help="Environment to use")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--gin_config", type=str, default=None, help="Path to gin config file")

    args = parser.parse_args()

    asyncio.run(main(args.environment, args.operation, args.debug, args.gin_config))
