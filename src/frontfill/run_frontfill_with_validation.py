#!/usr/bin/env python3
"""
Complete Frontfill and Validation System Runner.
Orchestrates data collection, validation, and quality monitoring.
"""

import asyncio
import argparse
import logging
import os
import json
from datetime import datetime, date, timedelta
from typing import Dict, Any

from config.environment import Environment, EnvironmentType
from config.database import get_connection_pool
from frontfill.frontfill_orchestrator import FrontfillOrchestrator, run_frontfill_orchestrator
from frontfill.validation_integration import ValidationIntegration, ValidationConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_api_keys_from_env() -> Dict[str, str]:
    """Get API keys from environment variables."""
    return {
        "polygon": os.getenv("POLYGON_API_KEY"),
        "tiingo": os.getenv("TIINGO_API_KEY"),
        "alpha_vantage": os.getenv("ALPHA_VANTAGE_API_KEY"),
        "fred": os.getenv("FRED_API_KEY"),
        "finnhub": os.getenv("FINNHUB_API_KEY")
    }


async def run_orchestrator(env: Environment, api_keys: Dict[str, str]):
    """Run the complete frontfill orchestrator."""
    logger.info("Starting complete frontfill and validation orchestrator")
    await run_frontfill_orchestrator(env, api_keys)


async def run_manual_validation(env: Environment, api_keys: Dict[str, str], 
                               validation_date: date = None, output_file: str = None):
    """Run manual validation for a specific date."""
    if validation_date is None:
        validation_date = date.today() - timedelta(days=1)
    
    logger.info(f"Running manual validation for {validation_date}")
    
    # Initialize validation system
    connection_pool = await get_connection_pool(env)
    validation_config = ValidationConfig()
    
    try:
        validation_integration = ValidationIntegration(
            connection_pool, env, api_keys, validation_config
        )
        await validation_integration.initialize()
        
        # Run validation
        results = await validation_integration.run_post_frontfill_validation(validation_date)
        
        # Display results
        print(f"\n🔍 VALIDATION RESULTS FOR {validation_date}")
        print(f"Quality Score: {results['quality_score']:.2f}/100")
        print(f"Validation Passed: {'✅ Yes' if results['validation_passed'] else '❌ No'}")
        print(f"Instruments Validated: {results.get('instruments_validated', 0)}")
        print(f"Total Issues: {results.get('total_issues', 0)}")
        
        if results.get("actions_taken"):
            print(f"\nActions Taken:")
            for action in results["actions_taken"]:
                print(f"  • {action}")
        
        if results.get("backfill_results"):
            backfill = results["backfill_results"]
            print(f"\nBackfill Results:")
            print(f"  • Total gaps: {backfill['total_gaps']}")
            print(f"  • Critical gaps: {backfill['critical_gaps']}")
            print(f"  • Successful backfills: {backfill['successful_backfills']}")
            print(f"  • Records added: {backfill['records_added']}")
        
        # Save to file if requested
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\nFull results saved to: {output_file}")
        
        return results["validation_passed"]
        
    finally:
        await connection_pool.close()


async def run_missing_data_analysis(env: Environment, api_keys: Dict[str, str],
                                   start_date: date, end_date: date, 
                                   output_file: str = None):
    """Run comprehensive missing data analysis."""
    logger.info(f"Running missing data analysis from {start_date} to {end_date}")
    
    connection_pool = await get_connection_pool(env)
    validation_config = ValidationConfig()
    
    try:
        validation_integration = ValidationIntegration(
            connection_pool, env, api_keys, validation_config
        )
        await validation_integration.initialize()
        
        # Run analysis
        report = await validation_integration.run_missing_data_analysis(start_date, end_date)
        
        # Display results
        print(f"\n📊 MISSING DATA ANALYSIS: {start_date} to {end_date}")
        
        summary = report.get("summary", {})
        print(f"Total gaps: {summary.get('total_gaps', 0)}")
        print(f"Total missing days: {summary.get('total_missing_days', 0)}")
        print(f"Average gap size: {summary.get('avg_gap_size', 0)} days")
        print(f"Symbols affected: {summary.get('symbols_affected', 0)}")
        
        # By vendor
        by_vendor = report.get("by_vendor", {})
        if by_vendor:
            print(f"\nGaps by vendor:")
            for vendor, stats in by_vendor.items():
                print(f"  • {vendor}: {stats['gap_count']} gaps, {stats['missing_days']} missing days")
        
        # By priority
        by_priority = report.get("by_priority", {})
        if by_priority:
            print(f"\nGaps by priority:")
            for priority, stats in by_priority.items():
                print(f"  • {priority}: {stats['gap_count']} gaps, {stats['missing_days']} missing days")
        
        # Critical gaps
        critical_gaps = report.get("critical_gaps", [])
        if critical_gaps:
            print(f"\nTop {min(10, len(critical_gaps))} critical gaps:")
            for i, gap in enumerate(critical_gaps[:10]):
                print(f"  {i+1}. {gap['symbol']} ({gap['vendor']}): "
                      f"{gap['missing_days']} days, {gap['gap_start']} to {gap['gap_end']}")
        
        # Recommendations
        recommendations = report.get("recommendations", [])
        if recommendations:
            print(f"\nRecommendations:")
            for rec in recommendations:
                print(f"  {rec}")
        
        # Save to file if requested
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            print(f"\nFull report saved to: {output_file}")
        
        return len(critical_gaps) == 0  # Success if no critical gaps
        
    finally:
        await connection_pool.close()


async def get_system_status(env: Environment, api_keys: Dict[str, str]):
    """Get system status and recent activity."""
    connection_pool = await get_connection_pool(env)
    
    try:
        orchestrator = FrontfillOrchestrator(env, api_keys)
        orchestrator.connection_pool = connection_pool
        
        # Initialize components
        from frontfill.checkpoint_manager import CheckpointManager
        from frontfill.validation_integration import ValidationIntegration
        
        orchestrator.checkpoint_manager = CheckpointManager(connection_pool, env)
        orchestrator.validation_integration = ValidationIntegration(
            connection_pool, env, api_keys, ValidationConfig()
        )
        
        # Get status
        status = await orchestrator.get_job_status()
        
        # Display status
        print(f"\n📊 SYSTEM STATUS")
        print(f"Orchestrator running: {'✅ Yes' if status['orchestrator_running'] else '❌ No'}")
        print(f"Daily jobs: {status['daily_jobs']}")
        print(f"Frequent jobs: {status['frequent_jobs']}")
        print(f"Validation enabled: {'✅ Yes' if status['validation_enabled'] else '❌ No'}")
        
        # Recent job runs
        recent_runs = status.get("recent_runs", [])
        if recent_runs:
            print(f"\nRecent job runs:")
            for run in recent_runs[:5]:
                print(f"  • {run['job_name']} ({run['vendor']}): "
                      f"{run['status']} - {run['records_processed']} processed")
        
        # Recent validations
        recent_validations = status.get("recent_validations", [])
        if recent_validations:
            print(f"\nRecent validations:")
            for val in recent_validations[:5]:
                passed_icon = "✅" if val.get("validation_passed") else "❌"
                print(f"  • {val.get('date')}: {passed_icon} "
                      f"Score: {val.get('quality_score', 0):.1f}/100")
        
        return status
        
    finally:
        await connection_pool.close()


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Complete frontfill and validation system"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Run orchestrator command
    orchestrator_parser = subparsers.add_parser("orchestrator", help="Run complete orchestrator")
    
    # Manual validation command
    validate_parser = subparsers.add_parser("validate", help="Run manual validation")
    validate_parser.add_argument("--date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                                default=date.today() - timedelta(days=1),
                                help="Date to validate (YYYY-MM-DD, default: yesterday)")
    validate_parser.add_argument("--output", help="Output file for results (JSON)")
    
    # Missing data analysis command
    analysis_parser = subparsers.add_parser("analyze", help="Run missing data analysis")
    analysis_parser.add_argument("--start-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                                default=date.today() - timedelta(days=30),
                                help="Start date (YYYY-MM-DD, default: 30 days ago)")
    analysis_parser.add_argument("--end-date", type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
                                default=date.today() - timedelta(days=1),
                                help="End date (YYYY-MM-DD, default: yesterday)")
    analysis_parser.add_argument("--output", help="Output file for report (JSON)")
    
    # System status command
    status_parser = subparsers.add_parser("status", help="Get system status")
    
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
    
    # Validate required API keys
    required_keys = ["polygon", "tiingo"]
    missing_keys = [k for k in required_keys if not api_keys.get(k)]
    
    if missing_keys and args.command in ["validate", "analyze"]:
        logger.error(f"Missing required API keys: {missing_keys}")
        logger.error("Set environment variables: POLYGON_API_KEY, TIINGO_API_KEY")
        exit(1)
    
    # Run the appropriate command
    try:
        if args.command == "orchestrator":
            asyncio.run(run_orchestrator(env, api_keys))
        elif args.command == "validate":
            success = asyncio.run(run_manual_validation(env, api_keys, args.date, args.output))
        elif args.command == "analyze":
            success = asyncio.run(run_missing_data_analysis(
                env, api_keys, args.start_date, args.end_date, args.output
            ))
        elif args.command == "status":
            status = asyncio.run(get_system_status(env, api_keys))
            success = True
        else:
            parser.error(f"Unknown command: {args.command}")
        
        if args.command != "orchestrator":
            exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        exit(0)
    except Exception as e:
        logger.error(f"Command failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()