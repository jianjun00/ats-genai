#!/usr/bin/env python3
"""
Daily Prices Validation Job for ATS Platform

Computes missing and abnormal daily price metrics for the past 90 days
across all vendors and pushes metrics to Prometheus for Grafana monitoring.

Features:
- Missing price detection per vendor
- Abnormal price detection (price spikes, negative prices, zero volume)
- 90-day rolling window analysis
- Prometheus metrics export for Grafana dashboard
- Vendor-specific statistics (EODHD, Tiingo, Polygon)
- Integration with ATS-INTG monitoring system

Usage:
    # Daily automated run (via cron)
    PYTHONPATH=src python3 scripts/daily_prices_validation.py

    # Manual run with debug output
    PYTHONPATH=src python3 scripts/daily_prices_validation.py --debug

    # Specific date range analysis
    PYTHONPATH=src python3 scripts/daily_prices_validation.py --days 30 --debug
"""

import os
import sys
import asyncio
import asyncpg
import logging
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, NamedTuple
from dataclasses import dataclass
import statistics
from collections import defaultdict, Counter

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.shared.run_aware_logging import setup_run_aware_logging

@dataclass
class ValidationMetrics:
    """Validation metrics for a specific vendor and date range"""
    vendor: str
    date_range_days: int
    total_expected_prices: int
    missing_prices: int
    abnormal_prices: int
    zero_volume_prices: int
    negative_prices: int
    price_spike_prices: int
    validation_timestamp: datetime

    @property
    def missing_percentage(self) -> float:
        return (self.missing_prices / self.total_expected_prices * 100) if self.total_expected_prices > 0 else 0.0

    @property
    def abnormal_percentage(self) -> float:
        return (self.abnormal_prices / self.total_expected_prices * 100) if self.total_expected_prices > 0 else 0.0

class DailyPricesValidator:
    """Daily prices validation engine with Prometheus metrics export"""

    def __init__(self, db_host: str = "localhost", db_port: int = 4432,
                 db_user: str = "postgres", db_password: str = "intg_password",
                 db_name: str = "intg_db"):
        self.db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        self.logger = logging.getLogger(__name__)
        self.vendors = ['eodhd', 'tiingo', 'polygon']
        self.prometheus_gateway_url = "http://localhost:9091"

        self.logger.info(f"🔧 Initialized DailyPricesValidator for {db_host}:{db_port}/{db_name}")

    async def get_active_instruments_count(self) -> int:
        """Get count of active instruments across all vendors"""
        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                # Count distinct instruments that have any recent price data
                query = """
                SELECT COUNT(DISTINCT symbol) as active_count
                FROM intg_daily_price_polygon
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                """
                result = await conn.fetchrow(query)
                return result['active_count'] if result else 0

    async def validate_vendor_prices(self, vendor: str, days: int = 90) -> ValidationMetrics:
        """Validate prices for a specific vendor over the past N days"""
        self.logger.info(f"📊 Validating {vendor} prices for past {days} days...")

        start_date = date.today() - timedelta(days=days)
        end_date = date.today()

        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                # Get expected number of price records (trading days * active symbols)
                expected_query = """
                WITH date_series AS (
                    SELECT generate_series($1::date, $2::date, '1 day'::interval)::date as date_val
                ),
                trading_days AS (
                    SELECT date_val as trading_date
                    FROM date_series
                    WHERE EXTRACT(DOW FROM date_val) NOT IN (0, 6)
                ),
                active_symbols AS (
                    SELECT DISTINCT symbol
                    FROM intg_daily_price_polygon_{vendor}
                    WHERE date >= $1
                )
                SELECT
                    (SELECT COUNT(*) FROM trading_days) * (SELECT COUNT(*) FROM active_symbols) as expected_total
                """.replace("{vendor}", vendor)

                expected_result = await conn.fetchrow(expected_query, start_date, end_date)
                total_expected = expected_result['expected_total'] if expected_result else 0

                # Count missing prices (expected but not found)
                missing_query = """
                WITH date_series AS (
                    SELECT generate_series($1::date, $2::date, '1 day'::interval)::date as date_val
                ),
                trading_days AS (
                    SELECT date_val as trading_date
                    FROM date_series
                    WHERE EXTRACT(DOW FROM date_val) NOT IN (0, 6)
                ),
                active_symbols AS (
                    SELECT DISTINCT symbol
                    FROM intg_daily_price_polygon_{vendor}
                    WHERE date >= $1
                ),
                expected_prices AS (
                    SELECT s.symbol, d.trading_date as date
                    FROM active_symbols s
                    CROSS JOIN trading_days d
                ),
                actual_prices AS (
                    SELECT symbol, date
                    FROM intg_daily_price_polygon_{vendor}
                    WHERE date BETWEEN $1 AND $2
                )
                SELECT COUNT(*) as missing_count
                FROM expected_prices e
                LEFT JOIN actual_prices a ON e.symbol = a.symbol AND e.date = a.date
                WHERE a.symbol IS NULL
                """.replace("{vendor}", vendor)

                missing_result = await conn.fetchrow(missing_query, start_date, end_date)
                missing_count = missing_result['missing_count'] if missing_result else 0

                # Count abnormal prices
                abnormal_query = """
                SELECT
                    COUNT(*) FILTER (WHERE close <= 0 OR high <= 0 OR low <= 0) as negative_prices,
                    COUNT(*) FILTER (WHERE volume = 0) as zero_volume_prices,
                    COUNT(*) FILTER (WHERE low > 0 AND high / low > 3.0) as price_spike_prices,
                    COUNT(*) as total_records
                FROM intg_daily_price_polygon_{vendor}
                WHERE date BETWEEN $1 AND $2
                """.replace("{vendor}", vendor)

                abnormal_result = await conn.fetchrow(abnormal_query, start_date, end_date)

                negative_prices = abnormal_result['negative_prices'] if abnormal_result else 0
                zero_volume_prices = abnormal_result['zero_volume_prices'] if abnormal_result else 0
                price_spike_prices = abnormal_result['price_spike_prices'] if abnormal_result else 0
                total_abnormal = negative_prices + zero_volume_prices + price_spike_prices

                metrics = ValidationMetrics(
                    vendor=vendor,
                    date_range_days=days,
                    total_expected_prices=total_expected,
                    missing_prices=missing_count,
                    abnormal_prices=total_abnormal,
                    zero_volume_prices=zero_volume_prices,
                    negative_prices=negative_prices,
                    price_spike_prices=price_spike_prices,
                    validation_timestamp=datetime.now()
                )

                self.logger.info(f"✅ {vendor}: {missing_count} missing, {total_abnormal} abnormal out of {total_expected} expected prices")
                return metrics

    async def export_prometheus_metrics(self, metrics_list: List[ValidationMetrics]) -> bool:
        """Export validation metrics to Prometheus pushgateway"""
        try:
            import requests

            # Build Prometheus metrics format
            prometheus_metrics = []
            timestamp = int(datetime.now().timestamp() * 1000)

            for metrics in metrics_list:
                vendor = metrics.vendor

                # Missing prices metrics
                prometheus_metrics.extend([
                    f'ats_daily_prices_missing_count{{vendor="{vendor}",environment="intg"}} {metrics.missing_prices} {timestamp}',
                    f'ats_daily_prices_missing_percentage{{vendor="{vendor}",environment="intg"}} {metrics.missing_percentage:.2f} {timestamp}',
                    f'ats_daily_prices_expected_total{{vendor="{vendor}",environment="intg"}} {metrics.total_expected_prices} {timestamp}',
                ])

                # Abnormal prices metrics
                prometheus_metrics.extend([
                    f'ats_daily_prices_abnormal_count{{vendor="{vendor}",environment="intg"}} {metrics.abnormal_prices} {timestamp}',
                    f'ats_daily_prices_abnormal_percentage{{vendor="{vendor}",environment="intg"}} {metrics.abnormal_percentage:.2f} {timestamp}',
                    f'ats_daily_prices_negative_count{{vendor="{vendor}",environment="intg"}} {metrics.negative_prices} {timestamp}',
                    f'ats_daily_prices_zero_volume_count{{vendor="{vendor}",environment="intg"}} {metrics.zero_volume_prices} {timestamp}',
                    f'ats_daily_prices_price_spike_count{{vendor="{vendor}",environment="intg"}} {metrics.price_spike_prices} {timestamp}',
                ])

            # Overall validation timestamp
            prometheus_metrics.append(f'ats_daily_prices_validation_timestamp{{environment="intg"}} {timestamp}')

            # Push to Prometheus gateway
            metrics_payload = '\n'.join(prometheus_metrics) + '\n'

            response = requests.post(
                f"{self.prometheus_gateway_url}/metrics/job/ats-daily-prices-validation/instance/intg",
                headers={'Content-Type': 'text/plain; version=0.0.4'},
                data=metrics_payload,
                timeout=10
            )

            if response.status_code == 200:
                self.logger.info(f"✅ Successfully exported {len(prometheus_metrics)} metrics to Prometheus")
                return True
            else:
                self.logger.error(f"❌ Failed to export metrics to Prometheus: {response.status_code} {response.text}")
                return False

        except Exception as e:
            self.logger.error(f"❌ Error exporting Prometheus metrics: {e}")
            return False

    async def run_validation(self, days: int = 90) -> List[ValidationMetrics]:
        """Run complete validation across all vendors"""
        self.logger.info(f"🚀 Starting daily prices validation for past {days} days")

        metrics_list = []
        for vendor in self.vendors:
            try:
                metrics = await self.validate_vendor_prices(vendor, days)
                metrics_list.append(metrics)
            except Exception as e:
                self.logger.error(f"❌ Failed to validate {vendor} prices: {e}")
                continue

        # Export metrics to Prometheus
        export_success = await self.export_prometheus_metrics(metrics_list)

        # Summary
        total_missing = sum(m.missing_prices for m in metrics_list)
        total_abnormal = sum(m.abnormal_prices for m in metrics_list)
        total_expected = sum(m.total_expected_prices for m in metrics_list)

        self.logger.info(f"📊 Validation Summary:")
        self.logger.info(f"   • Total Expected: {total_expected:,} price records")
        self.logger.info(f"   • Total Missing: {total_missing:,} ({total_missing/total_expected*100:.2f}%)")
        self.logger.info(f"   • Total Abnormal: {total_abnormal:,} ({total_abnormal/total_expected*100:.2f}%)")
        self.logger.info(f"   • Metrics Export: {'✅ Success' if export_success else '❌ Failed'}")

        return metrics_list

async def main():
    """Main validation job entry point"""
    parser = argparse.ArgumentParser(description="Daily Prices Validation Job")
    parser.add_argument("--days", type=int, default=90, help="Number of days to analyze (default: 90)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--dry-run", action="store_true", help="Run without exporting metrics")

    args = parser.parse_args()

    # Setup logging
    log_level = "DEBUG" if args.debug else "INFO"
    setup_run_aware_logging(log_level=log_level)

    logger = logging.getLogger(__name__)
    logger.info(f"🚀 Starting ATS Daily Prices Validation Job")
    logger.info(f"📅 Analysis period: {args.days} days")
    logger.info(f"🔧 Debug mode: {args.debug}")
    logger.info(f"🧪 Dry run mode: {args.dry_run}")

    try:
        # Initialize validator
        validator = DailyPricesValidator()

        if args.dry_run:
            logger.info("🧪 DRY RUN: Running validation without metrics export")
            # Override export method for dry run
            async def dry_run_export(metrics):
                logger.info("🧪 DRY RUN: Would export metrics to Prometheus")
                return True
            validator.export_prometheus_metrics = dry_run_export

        # Run validation
        metrics_list = await validator.run_validation(days=args.days)

        # Print detailed results if debug mode
        if args.debug:
            logger.info("📋 Detailed Validation Results:")
            for metrics in metrics_list:
                logger.info(f"   📊 {metrics.vendor.upper()}:")
                logger.info(f"      • Expected: {metrics.total_expected_prices:,} prices")
                logger.info(f"      • Missing: {metrics.missing_prices:,} ({metrics.missing_percentage:.2f}%)")
                logger.info(f"      • Abnormal: {metrics.abnormal_prices:,} ({metrics.abnormal_percentage:.2f}%)")
                logger.info(f"      • Negative: {metrics.negative_prices:,}")
                logger.info(f"      • Zero Volume: {metrics.zero_volume_prices:,}")
                logger.info(f"      • Price Spikes: {metrics.price_spike_prices:,}")

        logger.info("✅ Daily prices validation completed successfully")

    except Exception as e:
        logger.error(f"❌ Daily prices validation failed: {e}")
        if args.debug:
            import traceback
            logger.error(f"📋 Full traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())