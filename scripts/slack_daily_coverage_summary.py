#!/usr/bin/env python3
"""
ATS-INTG Slack Daily Coverage Summary

Sends a daily Slack notification with a formatted table showing instrument coverage
for the past 90 days across all vendors.

Features:
- 90-day lookback coverage analysis
- Formatted table with coverage percentages and missing data counts
- Trading day calculations (excludes weekends and holidays)
- Summary metrics and trends
- Configurable via environment variables

Usage:
    python3 scripts/slack_daily_coverage_summary.py
    python3 scripts/slack_daily_coverage_summary.py --test
    python3 scripts/slack_daily_coverage_summary.py --days 30
"""

import asyncio
import asyncpg
import logging
import os
import sys
import json
import requests
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config.database import Database
from config.environment import Environment

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class VendorSummary:
    """Summary stats for a single vendor."""
    vendor: str
    total_instruments: int
    instruments_with_data: int
    coverage_percentage: float
    missing_instruments: int
    data_freshness_hours: float
    trend_direction: str  # "↑", "↓", or "→"

class SlackDailyCoverageSummary:
    """Slack daily coverage summary notifier for ATS-INTG."""

    def __init__(self):
        self.environment = "intg"
        self.db_config = Database()
        self.db_pool: Optional[asyncpg.Pool] = None
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')

        # US holidays for trading day calculation
        current_year = datetime.now().year
        self.us_holidays = self._get_major_holidays(current_year)

    def _get_major_holidays(self, year: int) -> set:
        """Get major US holidays that affect trading."""
        holidays = set()

        # Major holidays that close markets
        holidays.add(date(year, 1, 1))   # New Year's Day
        holidays.add(date(year, 7, 4))   # Independence Day
        holidays.add(date(year, 12, 25)) # Christmas Day

        # Thanksgiving (4th Thursday of November)
        nov_1 = date(year, 11, 1)
        days_to_thursday = (3 - nov_1.weekday()) % 7
        first_thursday = nov_1 + timedelta(days=days_to_thursday)
        thanksgiving = first_thursday + timedelta(days=21)  # 4th Thursday
        holidays.add(thanksgiving)

        # Day after Thanksgiving
        holidays.add(thanksgiving + timedelta(days=1))

        return holidays

    def is_trading_day(self, check_date: date) -> bool:
        """Check if a date is a trading day (excludes weekends and US holidays)."""
        # Skip weekends
        if check_date.weekday() >= 5:
            return False
        # Skip US holidays
        return check_date not in self.us_holidays

    def get_trading_days(self, days: int) -> List[date]:
        """Get list of trading days for the past N days."""
        trading_days = []
        current_date = date.today()
        check_date = current_date - timedelta(days=1)  # Start from yesterday

        while len(trading_days) < min(days, 200):  # Safety limit
            if self.is_trading_day(check_date):
                trading_days.append(check_date)
            check_date -= timedelta(days=1)

            # Stop if we go too far back
            if check_date < date.today() - timedelta(days=365):
                break

        return sorted(trading_days)

    async def initialize(self):
        """Initialize database connection."""
        try:
            self.db_pool = await asyncpg.create_pool(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                min_size=1,
                max_size=5,
                command_timeout=30
            )
            logger.info("✅ Database connection pool initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            return False

    async def get_active_instruments(self) -> List[Tuple[int, str]]:
        """Get list of active instruments."""
        query = """
        SELECT id, symbol
        FROM intg_instruments
        WHERE active = true
        AND symbol IS NOT NULL
        AND symbol != ''
        ORDER BY symbol
        """

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [(row['id'], row['symbol']) for row in rows]

    async def get_vendor_coverage_summary(self, vendor: str, instruments: List[Tuple[int, str]],
                                        trading_days: List[date]) -> VendorSummary:
        """Get coverage summary for a specific vendor over the trading days."""
        if not trading_days:
            return VendorSummary(vendor, len(instruments), 0, 0.0, len(instruments), 0.0, "→")

        # Get coverage for recent period (last 30 trading days for trend)
        recent_days = trading_days[:30] if len(trading_days) >= 30 else trading_days

        # Query for data coverage
        instrument_ids = [inst_id for inst_id, _ in instruments]

        # Get total coverage over all trading days
        total_query = f"""
        SELECT COUNT(*) as total_records
        FROM intg_daily_prices_{vendor}
        WHERE instrument_id = ANY($1)
        AND date = ANY($2)
        """

        # Get recent coverage for trend calculation
        recent_query = f"""
        SELECT COUNT(*) as recent_records
        FROM intg_daily_prices_{vendor}
        WHERE instrument_id = ANY($1)
        AND date = ANY($2)
        """

        # Get data freshness
        freshness_query = f"""
        SELECT MAX(date) as latest_date
        FROM intg_daily_prices_{vendor}
        WHERE instrument_id = ANY($1)
        """

        try:
            async with self.db_pool.acquire() as conn:
                # Total coverage
                total_result = await conn.fetchrow(total_query, instrument_ids, trading_days)
                total_records = total_result['total_records'] if total_result else 0

                # Recent coverage for trend
                recent_result = await conn.fetchrow(recent_query, instrument_ids, recent_days)
                recent_records = recent_result['recent_records'] if recent_result else 0

                # Data freshness
                freshness_result = await conn.fetchrow(freshness_query, instrument_ids)
                latest_date = freshness_result['latest_date'] if freshness_result else None

                # Calculate metrics
                expected_total_records = len(instruments) * len(trading_days)
                expected_recent_records = len(instruments) * len(recent_days)

                coverage_percentage = (total_records / expected_total_records * 100) if expected_total_records > 0 else 0.0
                recent_coverage = (recent_records / expected_recent_records * 100) if expected_recent_records > 0 else 0.0

                # Calculate trend (simplified)
                if recent_coverage > coverage_percentage * 1.05:
                    trend = "↑"
                elif recent_coverage < coverage_percentage * 0.95:
                    trend = "↓"
                else:
                    trend = "→"

                # Data freshness in hours
                if latest_date:
                    freshness_hours = (datetime.now().date() - latest_date).days * 24.0
                else:
                    freshness_hours = float('inf')

                instruments_with_data = int(total_records / len(trading_days)) if trading_days else 0
                missing_instruments = len(instruments) - instruments_with_data

                return VendorSummary(
                    vendor=vendor,
                    total_instruments=len(instruments),
                    instruments_with_data=instruments_with_data,
                    coverage_percentage=coverage_percentage,
                    missing_instruments=missing_instruments,
                    data_freshness_hours=freshness_hours,
                    trend_direction=trend
                )

        except Exception as e:
            logger.error(f"❌ Error getting {vendor} coverage: {e}")
            return VendorSummary(vendor, len(instruments), 0, 0.0, len(instruments), 0.0, "→")

    async def generate_coverage_report(self, days: int = 90) -> Dict:
        """Generate comprehensive coverage report."""
        logger.info(f"🚀 Generating {days}-day coverage report...")

        # Get trading days
        trading_days = self.get_trading_days(days)
        logger.info(f"📅 Analyzing {len(trading_days)} trading days")

        # Get active instruments
        instruments = await self.get_active_instruments()
        logger.info(f"📊 Found {len(instruments)} active instruments")

        # Analyze coverage for each vendor
        vendors = ['tiingo', 'polygon', 'eodhd']
        vendor_summaries = []

        for vendor in vendors:
            summary = await self.get_vendor_coverage_summary(vendor, instruments, trading_days)
            vendor_summaries.append(summary)
            logger.info(f"✅ {vendor}: {summary.coverage_percentage:.1f}% coverage")

        # Calculate overall stats
        total_coverage = sum(s.coverage_percentage for s in vendor_summaries) / len(vendor_summaries)
        best_vendor = max(vendor_summaries, key=lambda s: s.coverage_percentage)

        return {
            'timestamp': datetime.now().isoformat(),
            'analysis_period': f"{len(trading_days)} trading days ({days} calendar days)",
            'total_instruments': len(instruments),
            'vendor_summaries': vendor_summaries,
            'overall_coverage': total_coverage,
            'best_vendor': best_vendor.vendor,
            'trading_days_analyzed': len(trading_days)
        }

    def format_slack_message(self, report: Dict) -> Dict:
        """Format coverage report as Slack message with table."""
        vendor_summaries = report['vendor_summaries']

        # Create table header
        table_lines = [
            "```",
            "📊 ATS-INTG DAILY PRICE COVERAGE SUMMARY",
            "=" * 60,
            f"Analysis Period: {report['analysis_period']}",
            f"Total Instruments: {report['total_instruments']:,}",
            "",
            "VENDOR COVERAGE TABLE:",
            "-" * 60,
            "Vendor    | Coverage | With Data | Missing | Trend | Freshness",
            "-" * 60
        ]

        # Add vendor rows
        for summary in vendor_summaries:
            freshness_str = f"{summary.data_freshness_hours:.0f}h" if summary.data_freshness_hours != float('inf') else "N/A"
            row = f"{summary.vendor:<8} | {summary.coverage_percentage:6.1f}% | {summary.instruments_with_data:8,} | {summary.missing_instruments:6,} | {summary.trend_direction:4} | {freshness_str:>7}"
            table_lines.append(row)

        # Add summary
        table_lines.extend([
            "-" * 60,
            f"Overall Average: {report['overall_coverage']:.1f}%",
            f"Best Vendor: {report['best_vendor']} ({max(s.coverage_percentage for s in vendor_summaries):.1f}%)",
            f"Report Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}",
            "```"
        ])

        # Create Slack message
        message = {
            "text": "📊 ATS-INTG Daily Price Coverage Summary",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*📊 ATS-INTG Daily Price Coverage Summary*\n{report['analysis_period']}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "\n".join(table_lines)
                    }
                }
            ]
        }

        # Add alert if coverage is low
        avg_coverage = report['overall_coverage']
        if avg_coverage < 50.0:
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"🚨 *ALERT*: Overall coverage is {avg_coverage:.1f}% - below recommended 50%"
                }
            })

        return message

    async def send_slack_notification(self, message: Dict, test_mode: bool = False) -> bool:
        """Send notification to Slack channel."""
        if not self.slack_webhook_url:
            if test_mode:
                logger.info("🧪 TEST MODE: Would send Slack message:")
                logger.info(json.dumps(message, indent=2))
                return True
            else:
                logger.warning("⚠️ SLACK_WEBHOOK_URL not configured - skipping notification")
                return False

        try:
            response = requests.post(
                self.slack_webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )

            if response.status_code == 200:
                logger.info("✅ Slack notification sent successfully")
                return True
            else:
                logger.error(f"❌ Slack notification failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"❌ Error sending Slack notification: {e}")
            return False

    async def run(self, days: int = 90, test_mode: bool = False):
        """Run daily coverage summary and send to Slack."""
        logger.info("================================================================================")
        logger.info("ATS-INTG SLACK DAILY COVERAGE SUMMARY")
        logger.info("================================================================================")
        logger.info(f"Analysis period: {days} days")
        logger.info(f"Test mode: {test_mode}")

        # Initialize database
        if not await self.initialize():
            logger.error("❌ Cannot initialize database connection")
            sys.exit(1)

        try:
            # Generate coverage report
            report = await self.generate_coverage_report(days)

            # Format Slack message
            message = self.format_slack_message(report)

            # Send notification
            success = await self.send_slack_notification(message, test_mode)

            if success:
                logger.info("✅ Daily coverage summary completed successfully")
            else:
                logger.error("❌ Failed to send daily coverage summary")
                sys.exit(1)

        except Exception as e:
            logger.error(f"❌ Error in daily coverage summary: {e}")
            sys.exit(1)
        finally:
            if self.db_pool:
                await self.db_pool.close()

async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="ATS-INTG Slack Daily Coverage Summary")
    parser.add_argument('--days', type=int, default=90, help='Days to analyze (default: 90)')
    parser.add_argument('--test', action='store_true', help='Test mode - print message instead of sending')
    args = parser.parse_args()

    summary = SlackDailyCoverageSummary()
    await summary.run(days=args.days, test_mode=args.test)

if __name__ == "__main__":
    asyncio.run(main())