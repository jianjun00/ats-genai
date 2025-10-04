#!/usr/bin/env python3
"""
Data Validation Runner.
Command-line interface for running daily prices validation and backfill operations.
"""

import asyncio
import logging
import argparse
import os
import json
from datetime import date, datetime, timedelta
from typing import Dict, Optional

from core.platform.config_env.environment import Environment, EnvironmentType
from core.config.database import get_connection_pool
from validation.daily_price_polygon_validator import DailyPricesValidator
from validation.missing_data_handler import MissingDataHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_validation(env: Environment, validation_date: date,
                        vendors: Optional[list] = None):
    """Run daily prices validation for a specific date."""
    logger.info(f"Running daily prices validation for {validation_date}")

    pool = await get_connection_pool(env)
    validator = DailyPricesValidator(pool, env)

    try:
        await validator.initialize()
        results = await validator.validate_daily_price_polygon(validation_date, vendors)

        # Print results
        print("\n📊 VALIDATION RESULTS")
        print(f"Date: {results['date']}")
        print(f"Instruments validated: {results['instruments_validated']}")
        print(f"Total issues: {results['total_issues']}")
        print(f"Data quality score: {results['data_quality_score']:.2f}/100")

        if results['issues_by_severity']:
            print("\nIssues by severity:")
            for severity, count in results['issues_by_severity'].items():
                print(f"  {severity}: {count}")

        if results['issues_by_rule']:
            print("\nIssues by type:")
            for rule, count in results['issues_by_rule'].items():
                print(f"  {rule}: {count}")

        return results['data_quality_score'] >= 80  # Pass if quality >= 80%

    finally:
        await pool.close()


async def run_missing_data_detection(env: Environment, start_date: date,
                                   end_date: date, api_keys: Dict[str, str],
                                   priority_symbols: Optional[list] = None):
    """Detect missing data gaps."""
    logger.info(f"Detecting missing data from {start_date} to {end_date}")

    pool = await get_connection_pool(env)
    handler = MissingDataHandler(pool, env, api_keys)

    try:
        gaps = await handler.detect_missing_data(start_date, end_date, priority_symbols)

        # Print results
        print(f"\n📊 MISSING DATA ANALYSIS")
        print(f"Period: {start_date} to {end_date}")
        print(f"Total gaps found: {len(gaps)}")

        if gaps:
            # Group by vendor
            by_vendor = {}
            by_priority = {}

            for gap in gaps:
                # By vendor
                if gap.vendor not in by_vendor:
                    by_vendor[gap.vendor] = []
                by_vendor[gap.vendor].append(gap)

                # By priority
                if gap.backfill_priority not in by_priority:
                    by_priority[gap.backfill_priority] = []
                by_priority[gap.backfill_priority].append(gap)

            print("\nGaps by vendor:")
            for vendor, vendor_gaps in by_vendor.items():
                total_days = sum(g.trading_days_missing for g in vendor_gaps)
                print(f"  {vendor}: {len(vendor_gaps)} gaps, {total_days} missing days")

            print("\nGaps by priority:")
            priority_names = {1: "Critical", 2: "High", 3: "Medium", 4: "Low", 5: "Very Low"}
            for priority in sorted(by_priority.keys()):
                priority_gaps = by_priority[priority]
                total_days = sum(g.trading_days_missing for g in priority_gaps)
                print(f"  {priority_names.get(priority, f'Priority {priority}')}: "
                      f"{len(priority_gaps)} gaps, {total_days} missing days")

            # Show top 10 critical gaps
            critical_gaps = [g for g in gaps if g.backfill_priority <= 2]
            if critical_gaps:
                print(f"\nTop {min(10, len(critical_gaps))} critical gaps:")
                for i, gap in enumerate(critical_gaps[:10]):
                    print(f"  {i+1}. {gap.symbol} ({gap.vendor}): "
                          f"{gap.trading_days_missing} days, "
                          f"{gap.gap_start} to {gap.gap_end}")

        return gaps

    finally:
        await pool.close()


async def run_backfill(env: Environment, start_date: date, end_date: date,
                      api_keys: Dict[str, str], priority_threshold: int = 3,
                      max_concurrent: int = 5, dry_run: bool = False):
    """Run backfill operation for missing data."""
    logger.info(f"Running backfill from {start_date} to {end_date}")

    pool = await get_connection_pool(env)
    handler = MissingDataHandler(pool, env, api_keys)

    try:
        # First detect gaps
        gaps = await handler.detect_missing_data(start_date, end_date)
        priority_gaps = [g for g in gaps if g.backfill_priority <= priority_threshold]

        print(f"\n🔄 BACKFILL OPERATION")
        print(f"Found {len(gaps)} total gaps, {len(priority_gaps)} priority gaps")
        print(f"Priority threshold: {priority_threshold} (1=critical, 5=low)")
        print(f"Max concurrent requests: {max_concurrent}")
        print(f"Dry run: {dry_run}")

        if not priority_gaps:
            print("No priority gaps to backfill")
            return True

        if dry_run:
            print("\nWould backfill:")
            for gap in priority_gaps[:20]:  # Show first 20
                print(f"  {gap.symbol} ({gap.vendor}): {gap.trading_days_missing} days")
            if len(priority_gaps) > 20:
                print(f"  ... and {len(priority_gaps) - 20} more")
            return True

        # Run actual backfill
        results = await handler.backfill_missing_data(
            gaps, max_concurrent, priority_threshold
        )

        # Print results
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        total_records = sum(r.records_added for r in successful)

        print(f"\n📈 BACKFILL RESULTS")
        print(f"Successful: {len(successful)}")
        print(f"Failed: {len(failed)}")
        print(f"Total records added: {total_records}")

        if failed:
            print(f"\nFirst 10 failures:")
            for i, result in enumerate(failed[:10]):
                print(f"  {i+1}. {result.symbol} ({result.vendor}) {result.target_date}: "
                      f"{result.error_message}")

        return len(failed) == 0

    finally:
        await pool.close()


async def generate_validation_report(env: Environment, start_date: date,
                                   end_date: date, output_file: Optional[str] = None):
    """Generate comprehensive validation report."""
    logger.info(f"Generating validation report from {start_date} to {end_date}")

    pool = await get_connection_pool(env)
    validator = DailyPricesValidator(pool, env)

    try:
        await validator.initialize()
        report = await validator.get_validation_report(start_date, end_date)

        # Print summary
        print(f"\n📋 VALIDATION REPORT")
        print(f"Period: {start_date} to {end_date}")
        print(f"Overall quality score: {report['overall_quality']:.2f}/100")

        if report['daily_summaries']:
            print(f"\nDaily summaries: {len(report['daily_summaries'])} days")
            avg_quality = sum(d['data_quality_score'] for d in report['daily_summaries']) / len(report['daily_summaries'])
            print(f"Average daily quality: {avg_quality:.2f}/100")

        if report['top_issues']:
            print(f"\nTop issues:")
            for issue in report['top_issues'][:10]:
                print(f"  {issue['rule']} ({issue['severity']}): {issue['count']} occurrences")

        if report['problem_instruments']:
            print(f"\nInstruments with most issues:")
            for inst in report['problem_instruments'][:10]:
                print(f"  {inst['instrument_symbol']}: {inst['issue_count']} issues")

        # Save to file if requested
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            print(f"\nFull report saved to: {output_file}")

        return report

    finally:
        await pool.close()


def get_api_keys_from_env() -> Dict[str, str]:
    """Get API keys using centralized management system with environment fallback."""
    try:
        # Use centralized API key management system
        from core.platform.config_env.environment import env

        if env:
            api_keys = {
                "polygon": env.get_api_key('polygon'),
                "tiingo": env.get_api_key('tiingo')
            }

            # Log successful centralized key retrieval
            logger.info("✅ Using centralized API key management")
            for vendor, key in api_keys.items():
                if key:
                    logger.debug(f"   {vendor.upper()}: {key[:8]}...{key[-4:]}")

            return api_keys
        else:
            raise ImportError("Environment not initialized")

    except Exception as e:
        logger.warning(f"⚠️  Centralized API keys not available: {e}")
        logger.info("🔄 Falling back to environment variable lookup")

        # Fallback to environment variables
        return {
            "polygon": os.getenv("POLYGON_API_KEY"),
            "tiingo": os.getenv("TIINGO_API_KEY")
        }


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Daily prices data validation and backfill system"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Validation command
    validate_parser = subparsers.add_parser("validate", help="Run validation for a specific date")
    validate_parser.add_argument("--date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                                default=date.today() - timedelta(days=1),
                                help="Date to validate (YYYY-MM-DD, default: yesterday)")
    validate_parser.add_argument("--vendors", nargs="+", choices=["polygon", "tiingo"],
                                help="Vendors to validate (default: all)")

    # Missing data detection command
    detect_parser = subparsers.add_parser("detect", help="Detect missing data gaps")
    detect_parser.add_argument("--start-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                              default=date.today() - timedelta(days=30),
                              help="Start date (YYYY-MM-DD, default: 30 days ago)")
    detect_parser.add_argument("--end-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                              default=date.today() - timedelta(days=1),
                              help="End date (YYYY-MM-DD, default: yesterday)")
    detect_parser.add_argument("--priority-symbols", nargs="+",
                              help="Focus on specific high-priority symbols")

    # Backfill command
    backfill_parser = subparsers.add_parser("backfill", help="Backfill missing data")
    backfill_parser.add_argument("--start-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                                default=date.today() - timedelta(days=7),
                                help="Start date (YYYY-MM-DD, default: 7 days ago)")
    backfill_parser.add_argument("--end-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                                default=date.today() - timedelta(days=1),
                                help="End date (YYYY-MM-DD, default: yesterday)")
    backfill_parser.add_argument("--priority-threshold", type=int, default=3,
                                help="Backfill priority threshold (1=critical only, 5=all)")
    backfill_parser.add_argument("--max-concurrent", type=int, default=5,
                                help="Maximum concurrent API requests")
    backfill_parser.add_argument("--dry-run", action="store_true",
                                help="Show what would be backfilled without doing it")

    # Report command
    report_parser = subparsers.add_parser("report", help="Generate validation report")
    report_parser.add_argument("--start-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                              default=date.today() - timedelta(days=7),
                              help="Start date (YYYY-MM-DD, default: 7 days ago)")
    report_parser.add_argument("--end-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                              default=date.today() - timedelta(days=1),
                              help="End date (YYYY-MM-DD, default: yesterday)")
    report_parser.add_argument("--output", help="Output file for full report (JSON)")

    # Global arguments
    parser.add_argument("--environment", choices=["dev", "intg", "prod"], default="dev",
                       help="Environment")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       default="INFO", help="Log level")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Get environment and API keys
    env_type = EnvironmentType(args.environment.upper())
    env = Environment(env_type)
    api_keys = get_api_keys_from_env()

    # Validate API keys for backfill operations
    if args.command == "backfill" and not args.dry_run:
        missing_keys = [k for k in ["polygon", "tiingo"] if not api_keys.get(k)]
        if missing_keys:
            logger.error(f"Missing API keys for backfill: {missing_keys}")
            logger.error("Set environment variables: POLYGON_API_KEY, TIINGO_API_KEY")
            exit(1)

    # Run the appropriate command
    try:
        if args.command == "validate":
            success = asyncio.run(run_validation(env, args.date, args.vendors))
        elif args.command == "detect":
            gaps = asyncio.run(run_missing_data_detection(
                env, args.start_date, args.end_date, api_keys, args.priority_symbols
            ))
            success = len(gaps) == 0  # Success if no gaps found
        elif args.command == "backfill":
            success = asyncio.run(run_backfill(
                env, args.start_date, args.end_date, api_keys,
                args.priority_threshold, args.max_concurrent, args.dry_run
            ))
        elif args.command == "report":
            report = asyncio.run(generate_validation_report(
                env, args.start_date, args.end_date, args.output
            ))
            success = report['overall_quality'] >= 80  # Success if quality >= 80%
        else:
            parser.error(f"Unknown command: {args.command}")

        exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        exit(0)
    except Exception as e:
        logger.error(f"Command failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()