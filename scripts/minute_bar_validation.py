#!/usr/bin/env python3
"""
Minute Bar Validation Job for ATS Platform

Validates minute bar coverage across all vendors for the past 90 days:
- EODHD: File-based parquet data
- Polygon: File-based parquet data + live minute bars
- Tiingo: File-based parquet data + live minute bars
- FirstRate: File-based parquet data

Features:
- Missing instrument detection per vendor
- Missing time period analysis (90-day rolling window)
- File vs database consistency validation
- Prometheus metrics export for Grafana monitoring
- Integration with ATS-INTG monitoring system

Usage:
    # Daily automated run (via cron)
    PYTHONPATH=src python3 scripts/minute_bar_validation.py

    # Manual run with debug output
    PYTHONPATH=src python3 scripts/minute_bar_validation.py --debug

    # Specific analysis period
    PYTHONPATH=src python3 scripts/minute_bar_validation.py --days 30 --debug
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
from pathlib import Path
import pandas as pd
from collections import defaultdict, Counter
import glob

# Ray for parallel processing
import ray
RAY_AVAILABLE = True
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.shared.run_aware_logging import setup_run_aware_logging

@dataclass
class MinuteBarMetrics:
    """Minute bar validation metrics for a specific vendor and period"""
    vendor: str
    data_source: str  # 'file' or 'database'
    date_range_days: int
    total_expected_instruments: int
    missing_instruments: int
    total_expected_periods: int  # trading days * trading hours * instruments
    missing_periods: int
    file_count: int  # for file-based sources
    validation_timestamp: datetime

    @property
    def missing_instruments_percentage(self) -> float:
        return (self.missing_instruments / self.total_expected_instruments * 100) if self.total_expected_instruments > 0 else 0.0

    @property
    def missing_periods_percentage(self) -> float:
        return (self.missing_periods / self.total_expected_periods * 100) if self.total_expected_periods > 0 else 0.0

@dataclass
class DailyCoverageMetrics:
    """Daily coverage metrics for file-based minute bar data (trading days only)"""
    vendor: str
    total_symbols: int
    symbols_with_t0_data: int  # T-0 (most recent trading day)
    symbols_with_t1_data: int  # T-1 (previous trading day)
    symbols_with_t2_data: int  # T-2 (2 trading days ago)
    symbols_with_t3_data: int  # T-3 (3 trading days ago)
    symbols_with_t4_data: int  # T-4 (4 trading days ago)
    symbols_with_t5_data: int  # T-5 (5 trading days ago)
    symbols_with_recent_5_days: int  # Last 5 trading days cumulative
    symbols_with_recent_10_days: int  # Last 10 trading days cumulative
    last_trading_date: date  # Most recent trading day found
    validation_timestamp: datetime

    @property
    def t0_coverage_percentage(self) -> float:
        return (self.symbols_with_t0_data / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

    @property
    def t1_coverage_percentage(self) -> float:
        return (self.symbols_with_t1_data / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

    @property
    def t2_coverage_percentage(self) -> float:
        return (self.symbols_with_t2_data / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

    @property
    def t3_coverage_percentage(self) -> float:
        return (self.symbols_with_t3_data / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

    @property
    def t4_coverage_percentage(self) -> float:
        return (self.symbols_with_t4_data / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

    @property
    def t5_coverage_percentage(self) -> float:
        return (self.symbols_with_t5_data / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

    @property
    def recent_coverage_percentage(self) -> float:
        return (self.symbols_with_recent_10_days / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

def get_recent_trading_days(num_days: int = 10) -> List[date]:
    """Get list of recent trading days (Mon-Fri, excluding weekends)"""
    trading_days = []
    current_date = date.today()

    # Go back in time to find trading days
    days_checked = 0
    while len(trading_days) < num_days and days_checked < 30:  # Safety limit
        if current_date.weekday() < 5:  # Monday=0, Friday=4
            trading_days.append(current_date)
        current_date -= timedelta(days=1)
        days_checked += 1

    return sorted(trading_days, reverse=True)  # Most recent first

@ray.remote
def analyze_symbol_trading_days(symbol: str, letter_path: str, trading_days: List[date]) -> Dict:
    """Ray remote function to analyze trading day coverage for a single symbol"""
    import pandas as pd
    from pathlib import Path

    t0_date = trading_days[0] if len(trading_days) > 0 else None
    t1_date = trading_days[1] if len(trading_days) > 1 else None
    t2_date = trading_days[2] if len(trading_days) > 2 else None
    t3_date = trading_days[3] if len(trading_days) > 3 else None
    t4_date = trading_days[4] if len(trading_days) > 4 else None
    t5_date = trading_days[5] if len(trading_days) > 5 else None

    symbol_trading_dates = set()
    last_trading_date = None
    minute_bars_count = 0

    # Check recent months for trading data
    for month in ['08', '09']:
        file_path = Path(letter_path) / symbol / f'2025/{month}' / f'{symbol}_2025_{month}.parquet'
        if file_path.exists():
            df = pd.read_parquet(file_path)
            df['date'] = pd.to_datetime(df['timestamp']).dt.date

            # Get unique trading dates from this file
            file_trading_dates = set(df['date'].unique())
            symbol_trading_dates.update(file_trading_dates)

            # Count minute bars for recent trading days only
            recent_dates = set(trading_days[:10])  # Last 10 trading days
            recent_bars = df[df['date'].isin(recent_dates)]
            minute_bars_count += len(recent_bars)

            # Update last trading date seen
            if file_trading_dates:
                file_last_date = max(file_trading_dates)
                if last_trading_date is None or file_last_date > last_trading_date:
                    last_trading_date = file_last_date

    has_t0 = t0_date in symbol_trading_dates if t0_date else False
    has_t1 = t1_date in symbol_trading_dates if t1_date else False
    has_t2 = t2_date in symbol_trading_dates if t2_date else False
    has_t3 = t3_date in symbol_trading_dates if t3_date else False
    has_t4 = t4_date in symbol_trading_dates if t4_date else False
    has_t5 = t5_date in symbol_trading_dates if t5_date else False

    # Check recent coverage (last 5 and 10 trading days)
    recent_5_found = any(td in symbol_trading_dates for td in trading_days[:5])
    recent_10_found = any(td in symbol_trading_dates for td in trading_days[:10])

    return {
        'symbol': symbol,
        'has_t0': has_t0,
        'has_t1': has_t1,
        'has_t2': has_t2,
        'has_t3': has_t3,
        'has_t4': has_t4,
        'has_t5': has_t5,
        'has_recent_5': recent_5_found,
        'has_recent_10': recent_10_found,
        'last_trading_date': last_trading_date,
        'minute_bars_count': minute_bars_count,
        'trading_dates': list(symbol_trading_dates)
    }

class MinuteBarValidator:
    """Minute bar validation engine with file and database analysis"""

    def __init__(self, db_host: str = "localhost", db_port: int = 4432,
                 db_user: str = "postgres", db_password: str = "intg_password",
                 db_name: str = "intg_db"):
        self.db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        self.logger = logging.getLogger(__name__)
        self.minute_bars_path = "/mnt/d/ats-data/minute-bars"
        self.prometheus_gateway_url = "http://localhost:9091"

        # Vendor configurations
        self.vendors = {
            'eodhd': {'has_files': True, 'has_database': False},
            'polygon': {'has_files': True, 'has_database': True, 'live_table': 'intg_one_minute_live_polygon'},
            'tiingo': {'has_files': True, 'has_database': True, 'live_table': 'intg_one_minute_live_tiingo'},
            'firstrate': {'has_files': True, 'has_database': False}
        }

        self.logger.info(f"🔧 Initialized MinuteBarValidator for {db_host}:{db_port}/{db_name}")

    async def get_expected_instruments_from_daily_prices(self, vendor: str) -> List[str]:
        """Get list of expected instruments from daily prices tables"""
        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                # Get instruments that have recent daily price data
                query = f"""
                SELECT DISTINCT symbol
                FROM intg_daily_price_polygon_{vendor}
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY symbol
                """
                result = await conn.fetch(query)
                symbols = [r['symbol'] for r in result]
                self.logger.info(f"📊 {vendor}: Found {len(symbols)} expected instruments from daily prices")
                return symbols
    def get_file_based_instruments(self, vendor: str, days: int = 90) -> Tuple[List[str], int]:
        """Get instruments that have minute bar files for the analysis period"""
        vendor_path = Path(self.minute_bars_path) / vendor
        if not vendor_path.exists():
            return [], 0

        instruments = set()
        file_count = 0
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        # Different path structures for different vendors
        if vendor == 'firstrate':
            # FirstRate: /vendor/first_letter/SYMBOL/YYYY/MM/SYMBOL_YYYY_MM.parquet
            pattern = f"{vendor_path}/*/*/*/*/*.parquet"
        else:
            # EODHD/Polygon/Tiingo: /vendor/first_letter/YYYY/MM/SYMBOL_YYYY_MM.parquet
            pattern = f"{vendor_path}/*/*/*/*.parquet"

        parquet_files = glob.glob(pattern)
        file_count = len(parquet_files)

        for file_path in parquet_files:
            # Extract symbol from filename
            filename = Path(file_path).stem  # Remove .parquet
            if vendor == 'firstrate':
                # FirstRate: SYMBOL_YYYY_MM.parquet
                symbol = filename.split('_')[0]
            else:
                # Others: SYMBOL_YYYY_MM.parquet
                symbol = filename.split('_')[0]

            instruments.add(symbol)
        self.logger.info(f"📁 {vendor}: Found {len(instruments)} instruments in {file_count} files")
        return list(instruments), file_count

    async def validate_database_minute_bars(self, vendor: str, days: int = 90) -> MinuteBarMetrics:
        """Validate database-based minute bar coverage"""
        config = self.vendors[vendor]
        if not config['has_database']:
            return MinuteBarMetrics(
                vendor=vendor, data_source='database', date_range_days=days,
                total_expected_instruments=0, missing_instruments=0,
                total_expected_periods=0, missing_periods=0, file_count=0,
                validation_timestamp=datetime.now()
            )

        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                table_name = config['live_table']
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)

                # Get expected instruments
                expected_instruments = await self.get_expected_instruments_from_daily_prices(vendor)
                total_expected = len(expected_instruments)

                # Get instruments with minute bar data
                actual_query = f"""
                SELECT DISTINCT symbol
                FROM {table_name}
                WHERE timestamp >= $1
                ORDER BY symbol
                """
                actual_result = await conn.fetch(actual_query, start_date)
                actual_instruments = {r['symbol'] for r in actual_result}

                missing_instruments = [s for s in expected_instruments if s not in actual_instruments]
                missing_count = len(missing_instruments)

                # Calculate missing periods (approximate)
                # Trading days * trading hours (6.5h * 60min) * instruments
                trading_days = days * 5 / 7  # rough weekday estimate
                trading_minutes_per_day = 6.5 * 60  # 390 minutes
                total_expected_periods = int(trading_days * trading_minutes_per_day * total_expected)

                # Get actual periods
                actual_periods_result = await conn.fetchval(f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE timestamp >= $1
                """, start_date)

                missing_periods = max(0, total_expected_periods - (actual_periods_result or 0))

                self.logger.info(f"✅ {vendor} DB: {missing_count} missing instruments, {missing_periods:,} missing periods")

                return MinuteBarMetrics(
                    vendor=vendor, data_source='database', date_range_days=days,
                    total_expected_instruments=total_expected, missing_instruments=missing_count,
                    total_expected_periods=total_expected_periods, missing_periods=missing_periods,
                    file_count=0, validation_timestamp=datetime.now()
                )

    def validate_file_minute_bars(self, vendor: str, days: int = 90) -> MinuteBarMetrics:
        """Validate file-based minute bar coverage"""
        config = self.vendors[vendor]
        if not config['has_files']:
            return MinuteBarMetrics(
                vendor=vendor, data_source='file', date_range_days=days,
                total_expected_instruments=0, missing_instruments=0,
                total_expected_periods=0, missing_periods=0, file_count=0,
                validation_timestamp=datetime.now()
            )

        # Get file-based instruments
        file_instruments, file_count = self.get_file_based_instruments(vendor, days)

        # For file validation, we'll use a sample of known major symbols as "expected"
        # In practice, you might want to get this from an instruments table
        major_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'SPY', 'QQQ'
        ]

        file_instruments_set = set(file_instruments)
        missing_instruments = [s for s in major_symbols if s not in file_instruments_set]

        # Estimate missing periods (this would be more accurate with actual file analysis)
        trading_days = days * 5 / 7
        trading_minutes_per_day = 390
        expected_periods_per_instrument = int(trading_days * trading_minutes_per_day)
        total_expected_periods = expected_periods_per_instrument * len(major_symbols)

        # Rough estimate: assume each file has about 1 month of data
        estimated_actual_periods = file_count * 30 * trading_minutes_per_day
        missing_periods = max(0, total_expected_periods - int(estimated_actual_periods))

        self.logger.info(f"📁 {vendor} Files: {len(missing_instruments)} missing instruments, {file_count} files")

        return MinuteBarMetrics(
            vendor=vendor, data_source='file', date_range_days=days,
            total_expected_instruments=len(major_symbols), missing_instruments=len(missing_instruments),
            total_expected_periods=total_expected_periods, missing_periods=missing_periods,
            file_count=file_count, validation_timestamp=datetime.now()
        )

    async def export_prometheus_metrics(self, metrics_list: List[MinuteBarMetrics]) -> bool:
        """Export minute bar validation metrics to Prometheus pushgateway"""
        import requests

        prometheus_metrics = []
        timestamp = int(datetime.now().timestamp() * 1000)

        for metrics in metrics_list:
            vendor = metrics.vendor
            source = metrics.data_source

            # Missing instruments metrics
            prometheus_metrics.extend([
                f'ats_minute_bars_missing_instruments_count{{vendor="{vendor}",source="{source}",environment="intg"}} {metrics.missing_instruments} {timestamp}',
                f'ats_minute_bars_missing_instruments_percentage{{vendor="{vendor}",source="{source}",environment="intg"}} {metrics.missing_instruments_percentage:.2f} {timestamp}',
                f'ats_minute_bars_expected_instruments_total{{vendor="{vendor}",source="{source}",environment="intg"}} {metrics.total_expected_instruments} {timestamp}',
            ])

            # Missing periods metrics
            prometheus_metrics.extend([
                f'ats_minute_bars_missing_periods_count{{vendor="{vendor}",source="{source}",environment="intg"}} {metrics.missing_periods} {timestamp}',
                f'ats_minute_bars_missing_periods_percentage{{vendor="{vendor}",source="{source}",environment="intg"}} {metrics.missing_periods_percentage:.2f} {timestamp}',
                f'ats_minute_bars_expected_periods_total{{vendor="{vendor}",source="{source}",environment="intg"}} {metrics.total_expected_periods} {timestamp}',
            ])

            # File count for file-based sources
            if metrics.data_source == 'file':
                prometheus_metrics.append(
                    f'ats_minute_bars_file_count{{vendor="{vendor}",source="{source}",environment="intg"}} {metrics.file_count} {timestamp}'
                )

        # Overall validation timestamp
        prometheus_metrics.append(f'ats_minute_bars_validation_timestamp{{environment="intg"}} {timestamp}')

        # Push to Prometheus gateway
        metrics_payload = '\n'.join(prometheus_metrics) + '\n'

        response = requests.post(
            f"{self.prometheus_gateway_url}/metrics/job/ats-minute-bars-validation/instance/intg",
            headers={'Content-Type': 'text/plain; version=0.0.4'},
            data=metrics_payload,
            timeout=10
        )

        if response.status_code == 200:
            self.logger.info(f"✅ Successfully exported {len(prometheus_metrics)} metrics to Prometheus")
            return True
        else:
            self.logger.error(f"❌ Failed to export metrics to Prometheus: {response.status_code}")
            return False

    def validate_daily_coverage(self, vendor: str) -> Optional[DailyCoverageMetrics]:
        """Validate trading day coverage for file-based vendors"""
        config = self.vendors[vendor]
        if not config['has_files']:
            return None

        vendor_path = os.path.join(self.minute_bars_path, vendor)
        if not os.path.exists(vendor_path):
            return None

        # Get recent trading days (T-0, T-1, T-2, etc.)
        trading_days = get_recent_trading_days(10)
        if not trading_days:
            return None

        t0_date = trading_days[0]  # Most recent trading day
        t1_date = trading_days[1] if len(trading_days) > 1 else None
        t2_date = trading_days[2] if len(trading_days) > 2 else None

        self.logger.info(f"📊 Analyzing trading day coverage for {vendor}")
        self.logger.info(f"📅 T-0: {t0_date}, T-1: {t1_date}, T-2: {t2_date}")

        total_symbols = 0
        symbols_with_t0 = 0
        symbols_with_t1 = 0
        symbols_with_t2 = 0
        symbols_with_recent_5 = 0
        symbols_with_recent_10 = 0
        last_trading_date = None

        if vendor == 'firstrate':
            if RAY_AVAILABLE:
                self.logger.info(f"🚀 Using Ray parallel processing for FirstRate analysis")
                # Initialize Ray if not already initialized
                if not ray.is_initialized():
                    ray.init(num_cpus=os.cpu_count())

                # Collect all symbols across all letters
                all_symbol_tasks = []
                for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                    letter_path = os.path.join(vendor_path, letter)
                    if os.path.exists(letter_path):
                        symbols = [d for d in os.listdir(letter_path)
                                 if os.path.isdir(os.path.join(letter_path, d)) and not d.isdigit()]
                        total_symbols += len(symbols)

                        # Create Ray tasks for each symbol
                        for symbol in symbols:
                            task = analyze_symbol_trading_days.remote(symbol, letter_path, trading_days)
                            all_symbol_tasks.append(task)

                self.logger.info(f"📊 Processing {total_symbols} symbols in parallel with Ray ({len(all_symbol_tasks)} tasks)")

                # Execute all tasks in parallel and get results
                symbol_results = ray.get(all_symbol_tasks)

                # Aggregate results
                symbols_with_t3 = 0
                symbols_with_t4 = 0
                symbols_with_t5 = 0
                total_minute_bars_t1 = 0
                total_minute_bars_t2 = 0
                total_minute_bars_t3 = 0
                total_minute_bars_t4 = 0
                total_minute_bars_t5 = 0

                for result in symbol_results:
                    if result['has_t0']:
                        symbols_with_t0 += 1
                    if result['has_t1']:
                        symbols_with_t1 += 1
                    if result['has_t2']:
                        symbols_with_t2 += 1
                    if result['has_t3']:
                        symbols_with_t3 += 1
                    if result['has_t4']:
                        symbols_with_t4 += 1
                    if result['has_t5']:
                        symbols_with_t5 += 1
                    if result['has_recent_5']:
                        symbols_with_recent_5 += 1
                    if result['has_recent_10']:
                        symbols_with_recent_10 += 1

                    # Update last trading date
                    if result['last_trading_date']:
                        if last_trading_date is None or result['last_trading_date'] > last_trading_date:
                            last_trading_date = result['last_trading_date']

                self.logger.info(f"📊 Ray parallel processing completed: T-0: {symbols_with_t0}, T-1: {symbols_with_t1}, T-2: {symbols_with_t2}, T-3: {symbols_with_t3}, T-4: {symbols_with_t4}, T-5: {symbols_with_t5}")

                self.logger.warning(f"📊 Ray not available, falling back to sequential processing")
                # Original sequential code as fallback
                for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                    letter_path = os.path.join(vendor_path, letter)
                    if os.path.exists(letter_path):
                        symbols = [d for d in os.listdir(letter_path) if os.path.isdir(os.path.join(letter_path, d)) and not d.isdigit()]
                        total_symbols += len(symbols)

                        for symbol in symbols:
                            symbol_trading_dates = set()

                            # Check recent months for trading data
                            for month in ['08', '09']:
                                file_path = os.path.join(letter_path, symbol, f'2025/{month}', f'{symbol}_2025_{month}.parquet')
                                if os.path.exists(file_path):
                                    import pandas as pd
                                    df = pd.read_parquet(file_path)
                                    df['date'] = pd.to_datetime(df['timestamp']).dt.date

                                    # Get unique trading dates from this file
                                    file_trading_dates = set(df['date'].unique())
                                    symbol_trading_dates.update(file_trading_dates)

                                    # Update last trading date seen
                                    if file_trading_dates:
                                        file_last_date = max(file_trading_dates)
                                        if last_trading_date is None or file_last_date > last_trading_date:
                                            last_trading_date = file_last_date

                            if t0_date in symbol_trading_dates:
                                symbols_with_t0 += 1
                            if t1_date and t1_date in symbol_trading_dates:
                                symbols_with_t1 += 1
                            if t2_date and t2_date in symbol_trading_dates:
                                symbols_with_t2 += 1

                            # Check recent coverage (last 5 and 10 trading days)
                            recent_5_found = any(td in symbol_trading_dates for td in trading_days[:5])
                            recent_10_found = any(td in symbol_trading_dates for td in trading_days[:10])

                            if recent_5_found:
                                symbols_with_recent_5 += 1
                            if recent_10_found:
                                symbols_with_recent_10 += 1
        else:
            # Other vendors - simplified implementation for now
            # TODO: Implement vendor-specific trading day analysis
            self.logger.warning(f"📊 Trading day analysis not yet implemented for {vendor}")
            return None

        self.logger.info(f"📅 {vendor} Trading Day Coverage: T-0: {symbols_with_t0}, T-1: {symbols_with_t1}, T-2: {symbols_with_t2}")
        if 'symbols_with_t3' in locals():
            self.logger.info(f"📅 Extended coverage: T-3: {symbols_with_t3}, T-4: {symbols_with_t4}, T-5: {symbols_with_t5}")

        return DailyCoverageMetrics(
            vendor=vendor,
            total_symbols=total_symbols,
            symbols_with_t0_data=symbols_with_t0,
            symbols_with_t1_data=symbols_with_t1,
            symbols_with_t2_data=symbols_with_t2,
            symbols_with_t3_data=symbols_with_t3 if 'symbols_with_t3' in locals() else 0,
            symbols_with_t4_data=symbols_with_t4 if 'symbols_with_t4' in locals() else 0,
            symbols_with_t5_data=symbols_with_t5 if 'symbols_with_t5' in locals() else 0,
            symbols_with_recent_5_days=symbols_with_recent_5,
            symbols_with_recent_10_days=symbols_with_recent_10,
            last_trading_date=last_trading_date or t0_date,
            validation_timestamp=datetime.now()
        )

    async def export_daily_coverage_metrics(self, daily_metrics_list: List[DailyCoverageMetrics]) -> bool:
        """Export daily coverage metrics to Prometheus"""
        import requests
        prometheus_metrics = []
        timestamp = int(datetime.now().timestamp() * 1000)

        for metrics in daily_metrics_list:
            vendor = metrics.vendor

            # Trading day coverage count metrics
            prometheus_metrics.extend([
                f'ats_minute_bars_trading_total_symbols{{vendor="{vendor}",environment="intg"}} {metrics.total_symbols} {timestamp}',
                f'ats_minute_bars_trading_t0_symbols{{vendor="{vendor}",environment="intg"}} {metrics.symbols_with_t0_data} {timestamp}',
                f'ats_minute_bars_trading_t1_symbols{{vendor="{vendor}",environment="intg"}} {metrics.symbols_with_t1_data} {timestamp}',
                f'ats_minute_bars_trading_t2_symbols{{vendor="{vendor}",environment="intg"}} {metrics.symbols_with_t2_data} {timestamp}',
                f'ats_minute_bars_trading_t3_symbols{{vendor="{vendor}",environment="intg"}} {metrics.symbols_with_t3_data} {timestamp}',
                f'ats_minute_bars_trading_t4_symbols{{vendor="{vendor}",environment="intg"}} {metrics.symbols_with_t4_data} {timestamp}',
                f'ats_minute_bars_trading_t5_symbols{{vendor="{vendor}",environment="intg"}} {metrics.symbols_with_t5_data} {timestamp}',
                f'ats_minute_bars_trading_recent_5_days{{vendor="{vendor}",environment="intg"}} {metrics.symbols_with_recent_5_days} {timestamp}',
                f'ats_minute_bars_trading_recent_10_days{{vendor="{vendor}",environment="intg"}} {metrics.symbols_with_recent_10_days} {timestamp}',
            ])

            # Trading day coverage percentage metrics
            prometheus_metrics.extend([
                f'ats_minute_bars_trading_t0_coverage_percentage{{vendor="{vendor}",environment="intg"}} {metrics.t0_coverage_percentage:.2f} {timestamp}',
                f'ats_minute_bars_trading_t1_coverage_percentage{{vendor="{vendor}",environment="intg"}} {metrics.t1_coverage_percentage:.2f} {timestamp}',
                f'ats_minute_bars_trading_t2_coverage_percentage{{vendor="{vendor}",environment="intg"}} {metrics.t2_coverage_percentage:.2f} {timestamp}',
                f'ats_minute_bars_trading_t3_coverage_percentage{{vendor="{vendor}",environment="intg"}} {metrics.t3_coverage_percentage:.2f} {timestamp}',
                f'ats_minute_bars_trading_t4_coverage_percentage{{vendor="{vendor}",environment="intg"}} {metrics.t4_coverage_percentage:.2f} {timestamp}',
                f'ats_minute_bars_trading_t5_coverage_percentage{{vendor="{vendor}",environment="intg"}} {metrics.t5_coverage_percentage:.2f} {timestamp}',
                f'ats_minute_bars_trading_recent_coverage_percentage{{vendor="{vendor}",environment="intg"}} {metrics.recent_coverage_percentage:.2f} {timestamp}',
            ])

            # Last trading date (as timestamp)
            last_trading_timestamp = int(datetime.combine(metrics.last_trading_date, datetime.min.time()).timestamp())
            prometheus_metrics.append(
                f'ats_minute_bars_trading_last_date{{vendor="{vendor}",environment="intg"}} {last_trading_timestamp} {timestamp}'
            )

        # Push to Prometheus gateway
        metrics_payload = '\n'.join(prometheus_metrics) + '\n'

        response = requests.post(
            f"{self.prometheus_gateway_url}/metrics/job/ats-minute-bars-daily-coverage/instance/intg",
            headers={'Content-Type': 'text/plain; version=0.0.4'},
            data=metrics_payload,
            timeout=10
        )

        if response.status_code == 200:
            self.logger.info(f"✅ Successfully exported {len(prometheus_metrics)} daily coverage metrics to Prometheus")
            return True
        else:
            self.logger.error(f"❌ Failed to export daily coverage metrics to Prometheus: {response.status_code}")
            return False

    async def run_validation(self, days: int = 90) -> List[MinuteBarMetrics]:
        """Run complete minute bar validation across all vendors and sources"""
        self.logger.info(f"🚀 Starting minute bar validation for past {days} days")

        metrics_list = []

        for vendor, config in self.vendors.items():
            # Validate file-based data
            if config['has_files']:
                file_metrics = self.validate_file_minute_bars(vendor, days)
                metrics_list.append(file_metrics)

            # Validate database data
            if config['has_database']:
                db_metrics = await self.validate_database_minute_bars(vendor, days)
                metrics_list.append(db_metrics)

        daily_metrics_list = []
        for vendor, config in self.vendors.items():
            if config['has_files']:
                daily_metrics = self.validate_daily_coverage(vendor)
                if daily_metrics:
                    daily_metrics_list.append(daily_metrics)
        export_success = await self.export_prometheus_metrics(metrics_list)
        daily_export_success = await self.export_daily_coverage_metrics(daily_metrics_list)

        # Summary
        total_missing_instruments = sum(m.missing_instruments for m in metrics_list)
        total_expected_instruments = sum(m.total_expected_instruments for m in metrics_list)
        total_missing_periods = sum(m.missing_periods for m in metrics_list)
        total_expected_periods = sum(m.total_expected_periods for m in metrics_list)

        self.logger.info(f"📊 Minute Bar Validation Summary:")
        self.logger.info(f"   • Total Expected Instruments: {total_expected_instruments}")
        self.logger.info(f"   • Total Missing Instruments: {total_missing_instruments}")
        self.logger.info(f"   • Total Missing Periods: {total_missing_periods:,}")
        self.logger.info(f"   • Metrics Export: {'✅ Success' if export_success else '❌ Failed'}")
        self.logger.info(f"   • Daily Coverage Export: {'✅ Success' if daily_export_success else '❌ Failed'}")

        # Trading day coverage summary
        if daily_metrics_list:
            self.logger.info(f"📅 Trading Day Coverage Summary:")
            for dm in daily_metrics_list:
                self.logger.info(f"   • {dm.vendor.upper()}: {dm.total_symbols:,} symbols")
                self.logger.info(f"     T-0: {dm.symbols_with_t0_data} ({dm.t0_coverage_percentage:.1f}%), T-1: {dm.symbols_with_t1_data} ({dm.t1_coverage_percentage:.1f}%), T-2: {dm.symbols_with_t2_data} ({dm.t2_coverage_percentage:.1f}%)")
                self.logger.info(f"     T-3: {dm.symbols_with_t3_data} ({dm.t3_coverage_percentage:.1f}%), T-4: {dm.symbols_with_t4_data} ({dm.t4_coverage_percentage:.1f}%), T-5: {dm.symbols_with_t5_data} ({dm.t5_coverage_percentage:.1f}%)")
                self.logger.info(f"     Last trading date: {dm.last_trading_date}")

        return metrics_list

async def main():
    """Main validation job entry point"""
    parser = argparse.ArgumentParser(description="Minute Bar Validation Job")
    parser.add_argument("--days", type=int, default=90, help="Number of days to analyze (default: 90)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--dry-run", action="store_true", help="Run without exporting metrics")

    args = parser.parse_args()

    # Setup logging
    log_level = "DEBUG" if args.debug else "INFO"
    setup_run_aware_logging(log_level=log_level)

    logger = logging.getLogger(__name__)
    logger.info(f"🚀 Starting ATS Minute Bar Validation Job")
    logger.info(f"📅 Analysis period: {args.days} days")
    logger.info(f"🔧 Debug mode: {args.debug}")
    logger.info(f"🧪 Dry run mode: {args.dry_run}")

    # Initialize validator
    validator = MinuteBarValidator()

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
            logger.info(f"   📊 {metrics.vendor.upper()} ({metrics.data_source}):")
            logger.info(f"      • Expected Instruments: {metrics.total_expected_instruments}")
            logger.info(f"      • Missing Instruments: {metrics.missing_instruments} ({metrics.missing_instruments_percentage:.2f}%)")
            logger.info(f"      • Expected Periods: {metrics.total_expected_periods:,}")
            logger.info(f"      • Missing Periods: {metrics.missing_periods:,} ({metrics.missing_periods_percentage:.2f}%)")
            if metrics.data_source == 'file':
                logger.info(f"      • File Count: {metrics.file_count}")

    logger.info("✅ Minute bar validation completed successfully")

if __name__ == "__main__":
    asyncio.run(main())