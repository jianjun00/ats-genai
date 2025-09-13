#!/usr/bin/env python3
"""
ATS-INTG Slack Minute Bars Summary Notification

Sends comprehensive daily and weekly summaries of 1-minute bar backfill
processing to Slack channels with detailed statistics and file organization info.

Features:
- Daily processing summary with key metrics
- Weekly comprehensive report with trends
- File organization statistics by date/symbol
- Processing error reporting
- Storage usage monitoring
- Interactive buttons for manual operations

Usage:
    # Daily summary (default)
    python3 scripts/slack_minute_bars_summary.py --daily

    # Weekly comprehensive summary
    python3 scripts/slack_minute_bars_summary.py --weekly

    # Test notification
    python3 scripts/slack_minute_bars_summary.py --test
"""

import os
import sys
import asyncio
import logging
import json
import argparse
import aiohttp
import asyncpg
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from collections import defaultdict, Counter
import pandas as pd

# Add src to Python path
sys.path.insert(0, '/workspace/src')

from core.run_aware_logging import setup_run_aware_logging

logger = logging.getLogger(__name__)

class SlackMinuteBarsNotifier:
    """
    Slack notification service for daily 1-minute bar backfill summaries.
    """

    def __init__(
        self,
        slack_webhook: Optional[str] = None,
        daily_path: str = "/data/firstrate-data/daily",
        prometheus_gateway: Optional[str] = None
    ):
        self.slack_webhook = slack_webhook or os.getenv('SLACK_WEBHOOK_URL')
        self.daily_path = Path(daily_path)
        self.prometheus_gateway = prometheus_gateway or os.getenv('PROMETHEUS_PUSHGATEWAY_URL')
        self.db_pool = None

        if not self.slack_webhook:
            logger.warning("⚠️  Slack webhook URL not configured")

    async def initialize(self):
        """Initialize database connections."""
        try:
            # Database connection for INTG environment
            db_url = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'intg_password')}@{os.getenv('DB_HOST', 'ats-intg-postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'intg_db')}"

            self.db_pool = await asyncpg.create_pool(
                db_url,
                min_size=1,
                max_size=5,
                command_timeout=30
            )

            logger.info("✅ Slack notifier initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize Slack notifier: {e}")
            raise

    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()

    async def get_prometheus_metrics(self) -> Dict:
        """Fetch current metrics from Prometheus gateway."""
        if not self.prometheus_gateway:
            return {}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.prometheus_gateway}/metrics") as response:
                    if response.status == 200:
                        metrics_text = await response.text()

                        # Parse key metrics
                        metrics = {}
                        for line in metrics_text.split('\n'):
                            if line.startswith('ats_daily_minute_backfill_'):
                                parts = line.split(' ')
                                if len(parts) >= 2:
                                    metric_name = parts[0]
                                    metric_value = parts[1]

                                    # Extract metric type and labels
                                    if '{' in metric_name:
                                        base_name = metric_name.split('{')[0]
                                        labels_part = metric_name.split('{')[1].split('}')[0]

                                        if base_name not in metrics:
                                            metrics[base_name] = {}
                                        metrics[base_name][labels_part] = float(metric_value)
                                    else:
                                        metrics[metric_name] = float(metric_value)

                        return metrics

        except Exception as e:
            logger.error(f"❌ Failed to fetch Prometheus metrics: {e}")

        return {}

    async def get_file_system_stats(self, lookback_days: int = 7) -> Dict:
        """Get file system statistics for processed minute bars."""
        stats = {
            'total_files': 0,
            'total_size_mb': 0,
            'files_by_date': defaultdict(int),
            'files_by_symbol_letter': defaultdict(int),
            'latest_files': [],
            'size_by_date': defaultdict(float),
            'processing_dates': [],
            'symbols_processed': set(),
            'file_errors': []
        }

        try:
            if not self.daily_path.exists():
                logger.warning(f"📁 Daily path does not exist: {self.daily_path}")
                return stats

            # Analyze files from the last N days
            cutoff_date = date.today() - timedelta(days=lookback_days)

            for year_dir in self.daily_path.iterdir():
                if not year_dir.is_dir() or not year_dir.name.isdigit():
                    continue

                for month_dir in year_dir.iterdir():
                    if not month_dir.is_dir() or not month_dir.name.isdigit():
                        continue

                    for day_dir in month_dir.iterdir():
                        if not day_dir.is_dir() or not day_dir.name.isdigit():
                            continue

                        # Parse date
                        try:
                            file_date = date(int(year_dir.name), int(month_dir.name), int(day_dir.name))

                            # Skip if too old
                            if file_date < cutoff_date:
                                continue

                            stats['processing_dates'].append(file_date.isoformat())

                        except ValueError:
                            continue

                        # Process letter directories
                        for letter_dir in day_dir.iterdir():
                            if not letter_dir.is_dir():
                                continue

                            # Count files in this letter directory
                            for file_path in letter_dir.glob('*.parquet'):
                                try:
                                    file_size = file_path.stat().st_size / (1024 * 1024)  # MB

                                    stats['total_files'] += 1
                                    stats['total_size_mb'] += file_size
                                    stats['files_by_date'][file_date.isoformat()] += 1
                                    stats['files_by_symbol_letter'][letter_dir.name] += 1
                                    stats['size_by_date'][file_date.isoformat()] += file_size

                                    # Extract symbol from filename
                                    symbol = file_path.stem.split('_')[0]
                                    stats['symbols_processed'].add(symbol)

                                    # Track latest files
                                    if len(stats['latest_files']) < 20:
                                        stats['latest_files'].append({
                                            'path': str(file_path),
                                            'symbol': symbol,
                                            'date': file_date.isoformat(),
                                            'size_mb': file_size,
                                            'modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
                                        })

                                except Exception as e:
                                    stats['file_errors'].append(f"{file_path}: {str(e)}")

            # Convert sets to counts
            stats['unique_symbols_processed'] = len(stats['symbols_processed'])
            stats['symbols_processed'] = list(stats['symbols_processed'])[:50]  # Limit for display
            stats['processing_dates'] = sorted(list(set(stats['processing_dates'])))

            logger.info(f"📊 File system stats: {stats['total_files']} files, {stats['total_size_mb']:.1f}MB")

        except Exception as e:
            logger.error(f"❌ Error getting file system stats: {e}")
            stats['file_errors'].append(f"File system analysis error: {str(e)}")

        return stats

    async def get_database_stats(self) -> Dict:
        """Get database statistics for instruments and processing."""
        stats = {
            'total_instruments': 0,
            'critical_etfs': 0,
            'active_stocks': 0,
            'instrument_types': {},
            'recent_activity': {}
        }

        try:
            async with self.db_pool.acquire() as conn:
                # Total instruments
                total_query = "SELECT COUNT(*) FROM intg_instrument WHERE active = true"
                stats['total_instruments'] = await conn.fetchval(total_query)

                # Instrument type breakdown
                type_query = """
                    SELECT
                        CASE
                            WHEN symbol IN ('SPY', 'QQQ', 'VTI', 'IWM', 'EFA', 'VWO', 'GLD', 'SLV', 'TLT', 'HYG',
                                            'LQD', 'EEM', 'XLF', 'XLK', 'XLE', 'XLI', 'XLV', 'XLY', 'XLP', 'XLU',
                                            'VNQ', 'EWJ', 'FXI', 'EWZ', 'RSX', 'ARKK', 'ARKG', 'ARKW', 'JETS', 'ICLN')
                            THEN 'critical_etf'
                            WHEN symbol LIKE '%--%' OR symbol LIKE '%-%' OR symbol LIKE '%.%'
                            THEN 'other_etf'
                            ELSE 'stock'
                        END as instrument_type,
                        COUNT(*) as count
                    FROM intg_instrument
                    WHERE active = true
                    GROUP BY 1
                """

                type_rows = await conn.fetch(type_query)
                for row in type_rows:
                    inst_type = row['instrument_type']
                    count = row['count']
                    stats['instrument_types'][inst_type] = count

                    if inst_type == 'critical_etf':
                        stats['critical_etfs'] = count
                    elif inst_type == 'stock':
                        stats['active_stocks'] = count

        except Exception as e:
            logger.error(f"❌ Error getting database stats: {e}")

        return stats

    def create_daily_summary_message(self, file_stats: Dict, db_stats: Dict, prometheus_metrics: Dict) -> Dict:
        """Create Slack message for daily summary."""

        # Calculate key metrics
        files_today = file_stats.get('files_by_date', {}).get(date.today().isoformat(), 0)
        files_yesterday = file_stats.get('files_by_date', {}).get((date.today() - timedelta(days=1)).isoformat(), 0)

        total_files = file_stats.get('total_files', 0)
        total_size = file_stats.get('total_size_mb', 0)
        unique_symbols = file_stats.get('unique_symbols_processed', 0)

        # Process trend indicators
        trend_indicator = "→"
        if files_today > files_yesterday:
            trend_indicator = "↗️"
        elif files_today < files_yesterday:
            trend_indicator = "↘️"

        message = {
            "text": "📊 ATS-INTG Daily 1-Minute Bar Summary",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "📊 Daily 1-Minute Bar Processing Summary"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*📅 Date:* {date.today().isoformat()}\n*📁 Files Processed:* {files_today:,} {trend_indicator}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*🎯 Unique Symbols:* {unique_symbols:,}\n*💾 Total Size:* {total_size:.1f} MB"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*📊 7-Day Summary:*\n• Total files: {total_files:,}\n• Storage used: {total_size:.1f} MB\n• Processing dates: {len(file_stats.get('processing_dates', []))}"
                    }
                }
            ]
        }

        # Add instrument breakdown if available
        if db_stats.get('instrument_types'):
            breakdown_text = "*🏢 Instrument Types:*\n"
            for inst_type, count in db_stats['instrument_types'].items():
                breakdown_text += f"• {inst_type}: {count:,}\n"

            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": breakdown_text.strip()
                }
            })

        # Add file distribution by letter if available
        letter_dist = file_stats.get('files_by_symbol_letter', {})
        if letter_dist:
            top_letters = sorted(letter_dist.items(), key=lambda x: x[1], reverse=True)[:5]
            letter_text = "*📝 Top Symbol Letters:*\n"
            for letter, count in top_letters:
                letter_text += f"• {letter}: {count:,} files\n"

            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": letter_text.strip()
                }
            })

        # Add errors if any
        file_errors = file_stats.get('file_errors', [])
        if file_errors:
            error_count = len(file_errors)
            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*⚠️  Processing Errors:* {error_count}\nFirst error: {file_errors[0] if file_errors else 'None'}"
                }
            })

        # Add action buttons
        message["blocks"].append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "📊 View Metrics"
                    },
                    "url": f"{self.prometheus_gateway}/metrics" if self.prometheus_gateway else "http://localhost:4080/metrics",
                    "action_id": "view_metrics"
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "🔄 Manual Backfill"
                    },
                    "style": "primary",
                    "action_id": "manual_backfill"
                }
            ]
        })

        return message

    def create_weekly_summary_message(self, file_stats: Dict, db_stats: Dict, prometheus_metrics: Dict) -> Dict:
        """Create comprehensive weekly summary message."""

        total_files = file_stats.get('total_files', 0)
        total_size = file_stats.get('total_size_mb', 0)
        unique_symbols = file_stats.get('unique_symbols_processed', 0)
        processing_dates = file_stats.get('processing_dates', [])

        message = {
            "text": "📈 ATS-INTG Weekly 1-Minute Bar Summary",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "📈 Weekly 1-Minute Bar Processing Report"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*📅 Week Ending:* {date.today().isoformat()}\n*📁 Total Files:* {total_files:,}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*🎯 Unique Symbols:* {unique_symbols:,}\n*💾 Total Storage:* {total_size:.1f} MB"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*📊 Processing Coverage:*\n• Trading days processed: {len(processing_dates)}\n• Date range: {processing_dates[0] if processing_dates else 'N/A'} to {processing_dates[-1] if processing_dates else 'N/A'}"
                    }
                }
            ]
        }

        # Add detailed daily breakdown
        files_by_date = file_stats.get('files_by_date', {})
        if files_by_date:
            daily_breakdown = "*📅 Daily File Counts:*\n"
            for date_str in sorted(files_by_date.keys())[-7:]:  # Last 7 days
                count = files_by_date[date_str]
                daily_breakdown += f"• {date_str}: {count:,} files\n"

            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": daily_breakdown.strip()
                }
            })

        # Add storage breakdown by date
        size_by_date = file_stats.get('size_by_date', {})
        if size_by_date:
            storage_breakdown = "*💾 Daily Storage (MB):*\n"
            for date_str in sorted(size_by_date.keys())[-7:]:  # Last 7 days
                size = size_by_date[date_str]
                storage_breakdown += f"• {date_str}: {size:.1f} MB\n"

            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": storage_breakdown.strip()
                }
            })

        # Add instrument type summary
        if db_stats.get('instrument_types'):
            inst_summary = "*🏢 Instrument Universe:*\n"
            total_instruments = sum(db_stats['instrument_types'].values())

            for inst_type, count in db_stats['instrument_types'].items():
                percentage = (count / total_instruments * 100) if total_instruments > 0 else 0
                inst_summary += f"• {inst_type}: {count:,} ({percentage:.1f}%)\n"

            message["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": inst_summary.strip()
                }
            })

        return message

    async def send_slack_message(self, message: Dict) -> bool:
        """Send message to Slack webhook."""
        if not self.slack_webhook:
            logger.warning("⚠️  Slack webhook not configured, message not sent")
            return False

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.slack_webhook,
                    json=message,
                    headers={'Content-Type': 'application/json'}
                ) as response:

                    if response.status == 200:
                        logger.info("✅ Slack message sent successfully")
                        return True
                    else:
                        logger.error(f"❌ Slack API returned status {response.status}")
                        return False

        except Exception as e:
            logger.error(f"❌ Failed to send Slack message: {e}")
            return False

    async def send_daily_summary(self) -> bool:
        """Send daily minute bars processing summary."""
        try:
            logger.info("📊 Generating daily minute bars summary...")

            # Gather data
            file_stats = await self.get_file_system_stats(lookback_days=7)
            db_stats = await self.get_database_stats()
            prometheus_metrics = await self.get_prometheus_metrics()

            # Create message
            message = self.create_daily_summary_message(file_stats, db_stats, prometheus_metrics)

            # Send to Slack
            return await self.send_slack_message(message)

        except Exception as e:
            logger.error(f"❌ Failed to send daily summary: {e}")
            return False

    async def send_weekly_summary(self) -> bool:
        """Send comprehensive weekly summary."""
        try:
            logger.info("📈 Generating weekly minute bars summary...")

            # Gather data with longer lookback
            file_stats = await self.get_file_system_stats(lookback_days=14)
            db_stats = await self.get_database_stats()
            prometheus_metrics = await self.get_prometheus_metrics()

            # Create message
            message = self.create_weekly_summary_message(file_stats, db_stats, prometheus_metrics)

            # Send to Slack
            return await self.send_slack_message(message)

        except Exception as e:
            logger.error(f"❌ Failed to send weekly summary: {e}")
            return False

    async def send_test_message(self) -> bool:
        """Send test message to verify Slack integration."""
        try:
            test_message = {
                "text": "🧪 ATS-INTG Minute Bars Test",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "🧪 ATS-INTG Test Notification"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Test Time:* {datetime.now().isoformat()}\n*Service:* Daily 1-Minute Bar Slack Notifications\n*Status:* ✅ Working"
                        }
                    }
                ]
            }

            return await self.send_slack_message(test_message)

        except Exception as e:
            logger.error(f"❌ Test message failed: {e}")
            return False


async def main():
    """Main function for Slack minute bars notifications."""
    parser = argparse.ArgumentParser(description='ATS-INTG Slack Minute Bars Summary')

    parser.add_argument('--daily', action='store_true', help='Send daily summary (default)')
    parser.add_argument('--weekly', action='store_true', help='Send weekly comprehensive summary')
    parser.add_argument('--test', action='store_true', help='Send test message')
    parser.add_argument('--slack-webhook', help='Slack webhook URL')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Configure logging
    log_level = "DEBUG" if args.debug else "INFO"
    setup_run_aware_logging(log_level=log_level)

    logger.info("="*80)
    logger.info("ATS-INTG SLACK MINUTE BARS NOTIFICATIONS")
    logger.info("="*80)

    # Initialize notifier
    notifier = SlackMinuteBarsNotifier(slack_webhook=args.slack_webhook)

    try:
        await notifier.initialize()

        success = False

        if args.test:
            logger.info("🧪 Sending test notification...")
            success = await notifier.send_test_message()

        elif args.weekly:
            logger.info("📈 Sending weekly summary...")
            success = await notifier.send_weekly_summary()

        else:  # Default to daily
            logger.info("📊 Sending daily summary...")
            success = await notifier.send_daily_summary()

        if success:
            logger.info("✅ Notification sent successfully")
            return 0
        else:
            logger.error("❌ Notification failed")
            return 1

    except KeyboardInterrupt:
        logger.info("📤 Received keyboard interrupt")
        return 1
    except Exception as e:
        logger.error(f"❌ Notification service failed: {e}")
        return 1
    finally:
        await notifier.close()
        logger.info("✅ Slack notification service shutdown complete")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))