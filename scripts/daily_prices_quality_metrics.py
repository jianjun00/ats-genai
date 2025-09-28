#!/usr/bin/env python3
"""
Daily Prices Quality Metrics Generator

Generates and pushes metrics to SignOz for:
- Missing daily prices for weekdays (excluding holidays)
- Bad/invalid daily prices detection
- Coverage statistics per vendor (Polygon, Tiingo, EODHD)

Usage:
    python scripts/daily_price_polygon_quality_metrics.py [--days 90] [--push-metrics]
"""

import argparse
import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from prometheus_client import Counter, Gauge, push_to_gateway, CollectorRegistry

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.platform.database.connection_manager import DatabaseConnectionManager


class MarketCalendar:
    """Simple market calendar for US trading days"""

    @staticmethod
    def get_us_holidays(year):
        """Get major US market holidays for given year"""
        holidays = [
            f"{year}-01-01",  # New Year's Day
            f"{year}-07-04",  # Independence Day
            f"{year}-12-25",  # Christmas Day
        ]

        # Add approximate dates for variable holidays
        # (In production, use proper holiday library like pandas.tseries.holiday)
        if year == 2025:
            holidays.extend([
                "2025-01-20",  # MLK Day
                "2025-02-17",  # Presidents Day
                "2025-05-26",  # Memorial Day
                "2025-09-01",  # Labor Day
                "2025-11-27",  # Thanksgiving
                "2025-11-28",  # Black Friday
            ])
        elif year == 2024:
            holidays.extend([
                "2024-01-15",  # MLK Day
                "2024-02-19",  # Presidents Day
                "2024-05-27",  # Memorial Day
                "2024-09-02",  # Labor Day
                "2024-11-28",  # Thanksgiving
                "2024-11-29",  # Black Friday
            ])

        return set(holidays)

    @staticmethod
    def is_trading_day(date_str):
        """Check if given date is a trading day (weekday, not holiday)"""
        date_obj = pd.to_datetime(date_str).date()

        # Skip weekends
        if date_obj.weekday() >= 5:  # Saturday=5, Sunday=6
            return False

        # Skip holidays
        holidays = MarketCalendar.get_us_holidays(date_obj.year)
        if date_str in holidays:
            return False

        return True
    @staticmethod
    def get_expected_trading_days(start_date, end_date):
        """Get list of expected trading days in date range"""
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        trading_days = []

        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')
            if MarketCalendar.is_trading_day(date_str):
                trading_days.append(date_str)

        return trading_days


class DailyPricesQualityAnalyzer:
    """Analyzes daily prices quality and generates metrics"""

    def __init__(self, environment='intg'):
        self.environment = environment
        # Set environment for proper database connection
        os.environ['ENVIRONMENT'] = environment
        if environment == 'intg':
            os.environ['DB_HOST'] = 'localhost'
            os.environ['DB_PORT'] = '4432'
            os.environ['DB_PASSWORD'] = 'intg_password'
            os.environ['DB_NAME'] = 'intg_db'
        else:
            os.environ['DB_HOST'] = 'localhost'
            os.environ['DB_PORT'] = '3432'
            os.environ['DB_PASSWORD'] = 'dev_password'
            os.environ['DB_NAME'] = 'dev_db'

        self.db = DatabaseConnectionManager()

        # Setup Prometheus metrics
        self.registry = CollectorRegistry()

        # Missing prices metrics
        self.missing_prices_by_vendor = Gauge(
            'ats_daily_price_polygon_missing_symbols_total',
            'Number of symbols missing daily prices per vendor',
            ['vendor', 'environment'],
            registry=self.registry
        )

        self.missing_price_records = Gauge(
            'ats_daily_price_polygon_missing_records_total',
            'Number of missing daily price records per vendor',
            ['vendor', 'environment'],
            registry=self.registry
        )

        # Bad prices metrics
        self.bad_prices_by_vendor = Gauge(
            'ats_daily_price_polygon_bad_symbols_total',
            'Number of symbols with bad daily prices per vendor',
            ['vendor', 'environment'],
            registry=self.registry
        )

        self.bad_price_records = Gauge(
            'ats_daily_price_polygon_bad_records_total',
            'Number of bad daily price records per vendor',
            ['vendor', 'environment'],
            registry=self.registry
        )

        # Coverage metrics
        self.coverage_percentage = Gauge(
            'ats_daily_price_polygon_coverage_percent',
            'Daily prices coverage percentage per vendor',
            ['vendor', 'environment'],
            registry=self.registry
        )

        self.vendors = ['polygon', 'tiingo', 'eodhd']

    def analyze_missing_prices(self, days=90):
        """Analyze missing daily prices for trading days"""
        print(f"🔍 Analyzing missing daily prices for past {days} days...")

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)

        # Get expected trading days
        expected_trading_days = MarketCalendar.get_expected_trading_days(start_date, end_date)
        total_expected_days = len(expected_trading_days)

        print(f"📅 Expected trading days: {total_expected_days} (excluding weekends & holidays)")

        missing_results = {}

        for vendor in self.vendors:
            table_name = f"{self.environment}_daily_price_polygon_{vendor}"

            # Get symbols that should have data
            symbols_query = f"""
            SELECT DISTINCT symbol
            FROM {table_name}
            WHERE date >= %s - INTERVAL '1 year'
            ORDER BY symbol
            """

            with self.db.get_raw_connection() as conn:
                symbols_df = pd.read_sql(symbols_query, conn, params=[start_date])
                active_symbols = symbols_df['symbol'].tolist()

            if not active_symbols:
                print(f"⚠️ No active symbols found for {vendor}")
                continue

            print(f"📊 {vendor.upper()}: Checking {len(active_symbols)} active symbols")

            # Check coverage for each symbol on trading days
            coverage_query = f"""
            SELECT
                symbol,
                COUNT(*) as days_with_data,
                %s::int - COUNT(*) as missing_days
            FROM {table_name}
            WHERE symbol = ANY(%s)
              AND date >= %s
              AND date <= %s
              AND EXTRACT(DOW FROM date) NOT IN (0, 6)  -- Exclude weekends
            GROUP BY symbol
            """

            with self.db.get_raw_connection() as conn:
                coverage_df = pd.read_sql(
                    coverage_query,
                    conn,
                    params=[total_expected_days, active_symbols, start_date, end_date]
                )

            # Calculate missing statistics
            symbols_with_missing = len(coverage_df[coverage_df['missing_days'] > 0])
            total_missing_records = coverage_df['missing_days'].sum()

            coverage_pct = ((coverage_df['days_with_data'].sum() /
                           (len(active_symbols) * total_expected_days)) * 100)

            missing_results[vendor] = {
                'symbols_with_missing': symbols_with_missing,
                'total_missing_records': total_missing_records,
                'coverage_percent': round(coverage_pct, 2),
                'active_symbols': len(active_symbols)
            }

            print(f"  ❌ Symbols with missing data: {symbols_with_missing}")
            print(f"  📉 Total missing records: {total_missing_records}")
            print(f"  📊 Coverage: {coverage_pct:.2f}%")

        return missing_results

    def analyze_bad_prices(self, days=90):
        """Analyze bad/invalid daily prices"""
        print(f"🔍 Analyzing bad daily prices for past {days} days...")

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)

        bad_results = {}

        for vendor in self.vendors:
            table_name = f"{self.environment}_daily_price_polygon_{vendor}"

            # Query for bad prices (invalid values, abnormal patterns)
            bad_prices_query = f"""
            SELECT
                COUNT(DISTINCT symbol) as symbols_with_bad_prices,
                COUNT(*) as bad_price_records
            FROM {table_name}
            WHERE date >= %s
              AND date <= %s
              AND (
                open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR  -- Invalid prices
                volume < 0 OR                                          -- Negative volume
                high < low OR                                          -- High < Low
                high < open OR high < close OR                         -- High < Open/Close
                low > open OR low > close OR                           -- Low > Open/Close
                ABS(close - open) / open > 0.5 OR                      -- >50% price change
                open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL  -- NULL values
              )
            """

            with self.db.get_raw_connection() as conn:
                result = pd.read_sql(bad_prices_query, conn, params=[start_date, end_date])

            if len(result) > 0:
                symbols_with_bad = result.iloc[0]['symbols_with_bad_prices'] or 0
                bad_records = result.iloc[0]['bad_price_records'] or 0
            else:
                symbols_with_bad = 0
                bad_records = 0

            bad_results[vendor] = {
                'symbols_with_bad_prices': symbols_with_bad,
                'bad_price_records': bad_records
            }

            print(f"📊 {vendor.upper()}:")
            print(f"  ⚠️ Symbols with bad prices: {symbols_with_bad}")
            print(f"  🚨 Bad price records: {bad_records}")

        return bad_results

    def push_metrics_to_signoz(self, missing_results, bad_results, gateway_url=None):
        """Push metrics to SignOz via both Prometheus gateway and OTLP"""
        if not gateway_url:
            gateway_url = os.getenv('PROMETHEUS_GATEWAY', 'localhost:9091')

        print(f"📤 Pushing metrics to SignOz via Pushgateway at {gateway_url} and OTLP...")

        # Set missing prices metrics
        for vendor, data in missing_results.items():
            if 'error' not in data:
                self.missing_prices_by_vendor.labels(
                    vendor=vendor, environment=self.environment
                ).set(data.get('symbols_with_missing', 0))

                self.missing_price_records.labels(
                    vendor=vendor, environment=self.environment
                ).set(data.get('total_missing_records', 0))

                self.coverage_percentage.labels(
                    vendor=vendor, environment=self.environment
                ).set(data.get('coverage_percent', 0))

        # Set bad prices metrics
        for vendor, data in bad_results.items():
            if 'error' not in data:
                self.bad_prices_by_vendor.labels(
                    vendor=vendor, environment=self.environment
                ).set(data.get('symbols_with_bad_prices', 0))

                self.bad_price_records.labels(
                    vendor=vendor, environment=self.environment
                ).set(data.get('bad_price_records', 0))

        # Push to Pushgateway (existing)
        push_to_gateway(
            gateway_url,
            job='daily-prices-quality-metrics',
            registry=self.registry
        )

        # Also push directly to SignOz via OTLP
        self.push_metrics_via_otlp(missing_results, bad_results)

        print("✅ Metrics pushed successfully to SignOz via both Pushgateway and OTLP!")

        return True

    def push_metrics_via_otlp(self, missing_results, bad_results):
        """Push metrics directly to SignOz via OTLP"""
        import requests
        import json
        from datetime import datetime
        import time

        # SignOz OTLP HTTP endpoint
        otlp_url = "http://localhost:4318/v1/metrics"

        # Convert our metrics to OTLP format
        current_time = int(time.time() * 1_000_000_000)  # nanoseconds

        metrics_data = []

        # Add missing prices metrics
        for vendor, data in missing_results.items():
            if 'error' not in data:
                metrics_data.extend([
                    {
                        "name": "ats_daily_price_polygon_missing_symbols_total",
                        "description": "Number of symbols missing daily prices per vendor",
                        "unit": "1",
                        "gauge": {
                            "dataPoints": [{
                                "attributes": [
                                    {"key": "vendor", "value": {"stringValue": vendor}},
                                    {"key": "environment", "value": {"stringValue": self.environment}}
                                ],
                                "timeUnixNano": str(current_time),
                                "asDouble": float(data.get('symbols_with_missing', 0))
                            }]
                        }
                    },
                    {
                        "name": "ats_daily_price_polygon_missing_records_total",
                        "description": "Number of missing daily price records per vendor",
                        "unit": "1",
                        "gauge": {
                            "dataPoints": [{
                                "attributes": [
                                    {"key": "vendor", "value": {"stringValue": vendor}},
                                    {"key": "environment", "value": {"stringValue": self.environment}}
                                ],
                                "timeUnixNano": str(current_time),
                                "asDouble": float(data.get('total_missing_records', 0))
                            }]
                        }
                    },
                    {
                        "name": "ats_daily_price_polygon_coverage_percent",
                        "description": "Daily prices coverage percentage per vendor",
                        "unit": "1",
                        "gauge": {
                            "dataPoints": [{
                                "attributes": [
                                    {"key": "vendor", "value": {"stringValue": vendor}},
                                    {"key": "environment", "value": {"stringValue": self.environment}}
                                ],
                                "timeUnixNano": str(current_time),
                                "asDouble": float(data.get('coverage_percent', 0))
                            }]
                        }
                    }
                ])

        # Add bad prices metrics
        for vendor, data in bad_results.items():
            if 'error' not in data:
                metrics_data.extend([
                    {
                        "name": "ats_daily_price_polygon_bad_symbols_total",
                        "description": "Number of symbols with bad daily prices per vendor",
                        "unit": "1",
                        "gauge": {
                            "dataPoints": [{
                                "attributes": [
                                    {"key": "vendor", "value": {"stringValue": vendor}},
                                    {"key": "environment", "value": {"stringValue": self.environment}}
                                ],
                                "timeUnixNano": str(current_time),
                                "asDouble": float(data.get('symbols_with_bad_prices', 0))
                            }]
                        }
                    },
                    {
                        "name": "ats_daily_price_polygon_bad_records_total",
                        "description": "Number of bad daily price records per vendor",
                        "unit": "1",
                        "gauge": {
                            "dataPoints": [{
                                "attributes": [
                                    {"key": "vendor", "value": {"stringValue": vendor}},
                                    {"key": "environment", "value": {"stringValue": self.environment}}
                                ],
                                "timeUnixNano": str(current_time),
                                "asDouble": float(data.get('bad_price_records', 0))
                            }]
                        }
                    }
                ])

        # Build OTLP payload
        otlp_payload = {
            "resourceMetrics": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "ats-daily-prices-metrics"}},
                        {"key": "service.version", "value": {"stringValue": "1.0.0"}}
                    ]
                },
                "scopeMetrics": [{
                    "scope": {
                        "name": "ats.daily_price_polygon.metrics",
                        "version": "1.0.0"
                    },
                    "metrics": metrics_data
                }]
            }]
        }

        # Send to SignOz
        headers = {
            "Content-Type": "application/json"
        }

        response = requests.post(otlp_url, json=otlp_payload, headers=headers)
        if response.status_code == 200:
            print("✅ OTLP metrics pushed successfully to SignOz")
        else:
            print(f"⚠️ OTLP push returned status {response.status_code}: {response.text}")

    def generate_report(self, missing_results, bad_results):
        """Generate summary report"""
        print("\n" + "="*60)
        print("📊 DAILY PRICES QUALITY REPORT")
        print("="*60)

        print(f"\n🗓️ Analysis Period: Past {args.days} trading days")
        print(f"🏢 Environment: {self.environment.upper()}")
        print(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        print(f"\n📈 MISSING PRICES SUMMARY:")
        print("-" * 40)
        for vendor in self.vendors:
            if vendor in missing_results and 'error' not in missing_results[vendor]:
                data = missing_results[vendor]
                print(f"{vendor.upper():>10}: {data.get('symbols_with_missing', 0):>6} symbols, "
                      f"{data.get('total_missing_records', 0):>8} records, "
                      f"{data.get('coverage_percent', 0):>6.2f}% coverage")

        print(f"\n🚨 BAD PRICES SUMMARY:")
        print("-" * 40)
        for vendor in self.vendors:
            if vendor in bad_results and 'error' not in bad_results[vendor]:
                data = bad_results[vendor]
                print(f"{vendor.upper():>10}: {data.get('symbols_with_bad_prices', 0):>6} symbols, "
                      f"{data.get('bad_price_records', 0):>8} bad records")

        print("\n" + "="*60)


def main():
    parser = argparse.ArgumentParser(description='Generate daily prices quality metrics')
    parser.add_argument('--days', type=int, default=90,
                       help='Number of days to analyze (default: 90)')
    parser.add_argument('--environment', choices=['dev', 'intg'], default='intg',
                       help='Database environment (default: intg)')
    parser.add_argument('--push-metrics', action='store_true',
                       help='Push metrics to SignOz/Prometheus gateway')
    parser.add_argument('--gateway', type=str,
                       help='Prometheus gateway URL (default: localhost:9091)')

    global args
    args = parser.parse_args()

    analyzer = DailyPricesQualityAnalyzer(environment=args.environment)

    # Analyze missing prices
    missing_results = analyzer.analyze_missing_prices(days=args.days)

    # Analyze bad prices
    bad_results = analyzer.analyze_bad_prices(days=args.days)

    # Generate report
    analyzer.generate_report(missing_results, bad_results)

    # Push metrics if requested
    if args.push_metrics:
        success = analyzer.push_metrics_to_signoz(
            missing_results, bad_results, gateway_url=args.gateway
        )
        if not success:
            sys.exit(1)


if __name__ == '__main__':
    main()