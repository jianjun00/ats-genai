#!/usr/bin/env python3
"""
Daily Data Validation Report Runner

Runs daily validation reports and posts to Slack #ats-dev channel.
Can be scheduled via cron or run manually.
"""

import asyncio
import os
import sys
import logging
from datetime import date, timedelta
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from monitoring.data_validation_reporter import run_daily_validation_report

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/tmp/daily_validation.log')
    ]
)

logger = logging.getLogger(__name__)


async def main():
    """Run daily validation report."""
    logger.info("Starting daily data validation report...")
    
    # Configuration from environment variables
    db_url = os.getenv(
        "DATABASE_URL", 
        "postgresql://postgres:postgres@localhost:5433/dev_db"
    )
    
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not slack_webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set - Slack notifications disabled")
    
    # Symbols to validate (can be overridden by command line)
    default_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 
        'NVDA', 'META', 'NFLX', 'ADBE', 'CRM', 'ORCL'
    ]
    
    symbols = None  # Will use all active symbols from database
    if len(sys.argv) > 1:
        if sys.argv[1] == '--default':
            symbols = default_symbols
        elif sys.argv[1] != '--all':
            symbols = sys.argv[1].split(',')
    
    try:
        # Run validation report
        report = await run_daily_validation_report(
            db_url=db_url,
            symbols=symbols,
            slack_webhook_url=slack_webhook_url,
            post_to_slack=True,
            save_to_file=True
        )
        
        # Log summary
        logger.info(f"Validation report completed:")
        logger.info(f"  Report date: {report.report_date}")
        logger.info(f"  Total issues: {report.total_issues}")
        logger.info(f"  Critical issues: {report.critical_issues}")
        logger.info(f"  Warning issues: {report.warning_issues}")
        logger.info(f"  Symbols analyzed: {report.summary_stats['total_symbols']}")
        logger.info(f"  Average coverage: {report.summary_stats['avg_coverage']:.1f}%")
        logger.info(f"  Average quality: {report.summary_stats['avg_quality']:.3f}")
        
        # Alert if critical issues found
        if report.critical_issues > 0:
            logger.error(f"CRITICAL: {report.critical_issues} critical data issues detected!")
            # Could trigger additional alerts here
        
        return 0 if report.critical_issues == 0 else 1
        
    except Exception as e:
        logger.error(f"Error running validation report: {e}")
        return 2


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)