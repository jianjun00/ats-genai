#!/usr/bin/env python3
"""
Daily Universe Evaluator Job
CRITICAL P0 FIX #2: Implements automated daily universe membership evaluation

Runs daily to:
- Calculate 50-day rolling volume averages for all stocks
- Process membership exits (volume fell below $100M threshold)
- Process membership entries (volume exceeded $100M threshold)
- Log all membership changes with proper audit trail
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

sys.path.append('/home/jianjun/ats-genai-admin/src')

from src.domains.trading.services.universe_membership_manager import UniverseMembershipManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        # In production, add file handler:
        # logging.FileHandler('/var/log/universe-evaluator.log')
    ]
)

logger = logging.getLogger(__name__)

class DailyUniverseEvaluator:
    """Daily job for universe membership evaluation"""

    def __init__(self, environment: str = None):
        """Initialize evaluator with environment detection"""
        self.environment = environment or os.getenv('ENVIRONMENT', 'dev')
        self.manager = UniverseMembershipManager(environment=self.environment)

        # Universe configurations - adjust based on environment
        if self.environment == 'intg':
            self.universes = [
                {'id': 2, 'name': 'high_volume_large_cap', 'enabled': True},
                {'id': 3, 'name': 'test_high_volume_large_cap', 'enabled': True},
                {'id': 4, 'name': 'validation_universe_intg_comparison', 'enabled': True}
            ]
        else:  # dev environment
            self.universes = [
                {'id': 2, 'name': 'high_volume_large_cap', 'enabled': True},
                {'id': 3, 'name': 'test_high_volume_large_cap', 'enabled': True},
                {'id': 17, 'name': 'validation_universe_fix_comparison', 'enabled': True}
            ]

        logger.info(f"Initialized Daily Universe Evaluator for environment: {self.environment}")

    def run_daily_evaluation(self, evaluation_date: datetime = None) -> Dict[str, Any]:
        """
        Run daily evaluation for all configured universes
        This is the main entry point called by the scheduler
        """
        if evaluation_date is None:
            evaluation_date = datetime.now()

        logger.info(f"🚀 Starting daily universe evaluation for {evaluation_date.date()}")

        total_results = {
            'evaluation_date': evaluation_date,
            'environment': self.environment,
            'universes_processed': 0,
            'total_entries': 0,
            'total_exits': 0,
            'universe_results': {},
            'errors': [],
            'execution_time_seconds': 0
        }

        start_time = datetime.now()

        try:
            # Process each configured universe
            for universe_config in self.universes:
                if not universe_config['enabled']:
                    logger.info(f"⏭️ Skipping disabled universe {universe_config['id']}")
                    continue

                try:
                    logger.info(f"📊 Evaluating universe {universe_config['id']} ({universe_config['name']})")

                    # Run evaluation for this universe
                    result = self.manager.evaluate_daily_membership(
                        evaluation_date=evaluation_date,
                        universe_id=universe_config['id']
                    )

                    # Add to total results
                    total_results['universe_results'][universe_config['id']] = result
                    total_results['universes_processed'] += 1
                    total_results['total_entries'] += len(result['entries'])
                    total_results['total_exits'] += len(result['exits'])

                    # Log significant events
                    self._log_significant_events(universe_config, result)

                except Exception as e:
                    error_msg = f"Failed to evaluate universe {universe_config['id']}: {str(e)}"
                    logger.error(error_msg)
                    total_results['errors'].append(error_msg)

            # Calculate execution time
            total_results['execution_time_seconds'] = (datetime.now() - start_time).total_seconds()

            # Generate summary report
            self._generate_daily_summary(total_results)

            # Check for alerts
            self._check_alert_conditions(total_results)

            logger.info(f"✅ Daily evaluation completed successfully")
            logger.info(f"   Universes: {total_results['universes_processed']}")
            logger.info(f"   Entries: {total_results['total_entries']}")
            logger.info(f"   Exits: {total_results['total_exits']}")
            logger.info(f"   Duration: {total_results['execution_time_seconds']:.2f}s")

            return total_results

        except Exception as e:
            logger.error(f"❌ Daily evaluation failed: {str(e)}")
            total_results['errors'].append(f"Critical failure: {str(e)}")
            raise

    def _log_significant_events(self, universe_config: Dict, result: Dict):
        """Log significant entry/exit events"""
        universe_name = universe_config['name']

        # Log entries
        for entry in result['entries']:
            logger.info(f"📈 ENTRY - {universe_name}: {entry['symbol']} ({entry['reason']})")

        # Log exits
        for exit in result['exits']:
            logger.info(f"📉 EXIT - {universe_name}: {exit['symbol']} ({exit['reason']})")

        # Log if no changes (for audit trail)
        if not result['entries'] and not result['exits']:
            logger.info(f"🔄 NO CHANGES - {universe_name}: {result['total_active_after']} members stable")

    def _generate_daily_summary(self, results: Dict):
        """Generate human-readable daily summary"""
        date_str = results['evaluation_date'].strftime('%Y-%m-%d')

        summary_lines = [
            f"📊 DAILY UNIVERSE EVALUATION SUMMARY - {date_str}",
            "="*60,
            f"Environment: {results['environment']}",
            f"Execution Time: {results['execution_time_seconds']:.2f}s",
            ""
        ]

        # Universe-by-universe summary
        for universe_id, universe_result in results['universe_results'].items():
            universe_name = next(u['name'] for u in self.universes if u['id'] == universe_id)

            summary_lines.extend([
                f"🌐 Universe {universe_id} ({universe_name}):",
                f"   Active Members: {universe_result['total_active_after']}",
                f"   Entries: {len(universe_result['entries'])}",
                f"   Exits: {len(universe_result['exits'])}",
            ])

            # Show specific entries/exits
            if universe_result['entries']:
                summary_lines.append("   📈 New Entries:")
                for entry in universe_result['entries'][:5]:  # Show first 5
                    summary_lines.append(f"     • {entry['symbol']}: {entry['reason']}")
                if len(universe_result['entries']) > 5:
                    summary_lines.append(f"     ... and {len(universe_result['entries']) - 5} more")

            if universe_result['exits']:
                summary_lines.append("   📉 Exits:")
                for exit in universe_result['exits'][:5]:  # Show first 5
                    summary_lines.append(f"     • {exit['symbol']}: {exit['reason']}")
                if len(universe_result['exits']) > 5:
                    summary_lines.append(f"     ... and {len(universe_result['exits']) - 5} more")

            summary_lines.append("")

        # Overall totals
        summary_lines.extend([
            "📋 OVERALL TOTALS:",
            f"   Universes Processed: {results['universes_processed']}",
            f"   Total Entries: {results['total_entries']}",
            f"   Total Exits: {results['total_exits']}",
        ])

        if results['errors']:
            summary_lines.extend([
                "",
                "❌ ERRORS:",
                *[f"   • {error}" for error in results['errors']]
            ])

        summary_lines.append("="*60)

        # Log the complete summary
        for line in summary_lines:
            logger.info(line)

    def _check_alert_conditions(self, results: Dict):
        """Check for conditions that should trigger alerts"""
        alert_triggered = False

        # High volume of changes alert
        total_changes = results['total_entries'] + results['total_exits']
        if total_changes >= 10:
            logger.warning(f"🚨 HIGH VOLUME ALERT: {total_changes} membership changes detected")
            alert_triggered = True

        # Major stock changes alert
        major_stocks = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'META', 'AMZN']

        for universe_result in results['universe_results'].values():
            for entry in universe_result['entries']:
                if entry['symbol'] in major_stocks:
                    logger.warning(f"🚨 MAJOR STOCK ENTRY: {entry['symbol']} entered universe")
                    alert_triggered = True

            for exit in universe_result['exits']:
                if exit['symbol'] in major_stocks:
                    logger.warning(f"🚨 MAJOR STOCK EXIT: {exit['symbol']} exited universe")
                    alert_triggered = True

        # Execution time alert
        if results['execution_time_seconds'] > 300:  # 5 minutes
            logger.warning(f"🚨 PERFORMANCE ALERT: Evaluation took {results['execution_time_seconds']:.2f}s")
            alert_triggered = True

        # Error alert
        if results['errors']:
            logger.warning(f"🚨 ERROR ALERT: {len(results['errors'])} errors occurred")
            alert_triggered = True

        # Success notification (no alerts)
        if not alert_triggered and total_changes > 0:
            logger.info(f"✅ Normal operation: {total_changes} membership changes processed successfully")

    def run_historical_backfill(self, start_date: datetime, end_date: datetime):
        """
        Run historical backfill to correct past membership data
        This is used for Fix #3 - Historical data correction
        """
        logger.info(f"🔄 Starting historical backfill: {start_date.date()} to {end_date.date()}")

        current_date = start_date
        processed_days = 0

        while current_date <= end_date:
            # Skip weekends (markets closed)
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                try:
                    logger.info(f"Processing {current_date.date()}")
                    self.run_daily_evaluation(evaluation_date=current_date)
                    processed_days += 1

                except Exception as e:
                    logger.error(f"Failed to process {current_date.date()}: {str(e)}")

            current_date += timedelta(days=1)

        logger.info(f"✅ Historical backfill completed: {processed_days} trading days processed")

def main():
    """Main entry point for daily job"""
    import argparse

    parser = argparse.ArgumentParser(description='Daily Universe Membership Evaluator')
    parser.add_argument('--environment', help='Environment (dev/intg/prod)', default=None)
    parser.add_argument('--date', help='Evaluation date (YYYY-MM-DD)', default=None)
    parser.add_argument('--backfill-start', help='Backfill start date (YYYY-MM-DD)', default=None)
    parser.add_argument('--backfill-end', help='Backfill end date (YYYY-MM-DD)', default=None)

    args = parser.parse_args()

    # Initialize evaluator
    evaluator = DailyUniverseEvaluator(environment=args.environment)

    try:
        if args.backfill_start and args.backfill_end:
            # Historical backfill mode
            start_date = datetime.strptime(args.backfill_start, '%Y-%m-%d')
            end_date = datetime.strptime(args.backfill_end, '%Y-%m-%d')
            evaluator.run_historical_backfill(start_date, end_date)

        else:
            # Normal daily evaluation
            evaluation_date = None
            if args.date:
                evaluation_date = datetime.strptime(args.date, '%Y-%m-%d')

            results = evaluator.run_daily_evaluation(evaluation_date)

            # Exit with error code if there were errors
            if results['errors']:
                sys.exit(1)

        logger.info("🎉 Daily universe evaluator completed successfully")

    except Exception as e:
        logger.error(f"❌ Daily universe evaluator failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()