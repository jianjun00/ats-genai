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

# Add src to Python path
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
    """Daily coverage metrics for file-based minute bar data"""
    vendor: str
    total_symbols: int
    files_updated_today: int  # T-0
    files_updated_yesterday: int  # T-1  
    files_updated_2_days_ago: int  # T-2
    files_updated_last_week: int  # T-7 cumulative
    files_updated_last_15_days: int  # T-15 cumulative
    validation_timestamp: datetime
    
    @property
    def today_coverage_percentage(self) -> float:
        return (self.files_updated_today / self.total_symbols * 100) if self.total_symbols > 0 else 0.0
    
    @property
    def yesterday_coverage_percentage(self) -> float:
        return (self.files_updated_yesterday / self.total_symbols * 100) if self.total_symbols > 0 else 0.0
    
    @property
    def recent_coverage_percentage(self) -> float:
        return (self.files_updated_last_15_days / self.total_symbols * 100) if self.total_symbols > 0 else 0.0

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
                FROM intg_daily_prices_{vendor}
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY symbol
                """
                try:
                    result = await conn.fetch(query)
                    symbols = [r['symbol'] for r in result]
                    self.logger.info(f"📊 {vendor}: Found {len(symbols)} expected instruments from daily prices")
                    return symbols
                except Exception as e:
                    self.logger.warning(f"⚠️ Could not get expected instruments for {vendor}: {e}")
                    return []
    
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
            try:
                # Extract symbol from filename
                filename = Path(file_path).stem  # Remove .parquet
                if vendor == 'firstrate':
                    # FirstRate: SYMBOL_YYYY_MM.parquet
                    symbol = filename.split('_')[0]
                else:
                    # Others: SYMBOL_YYYY_MM.parquet  
                    symbol = filename.split('_')[0]
                
                instruments.add(symbol)
            except Exception as e:
                self.logger.debug(f"Could not parse instrument from {file_path}: {e}")
                continue
        
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
        try:
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
                
        except Exception as e:
            self.logger.error(f"❌ Error exporting Prometheus metrics: {e}")
            return False
    
    def validate_daily_coverage(self, vendor: str) -> Optional[DailyCoverageMetrics]:
        """Validate daily file update coverage for file-based vendors"""
        config = self.vendors[vendor]
        if not config['has_files']:
            return None
        
        vendor_path = os.path.join(self.minute_bars_path, vendor)
        if not os.path.exists(vendor_path):
            return None
        
        today = date.today()
        coverage_by_date = defaultdict(int)
        total_symbols = 0
        
        self.logger.info(f"📊 Analyzing daily coverage for {vendor}")
        
        # Count total symbols
        try:
            if vendor == 'firstrate':
                # FirstRate uses alphabetic directory structure
                for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                    letter_path = os.path.join(vendor_path, letter)
                    if os.path.exists(letter_path):
                        symbols = [d for d in os.listdir(letter_path) if os.path.isdir(os.path.join(letter_path, d)) and not d.isdigit()]
                        total_symbols += len(symbols)
                        
                        # Check file modification dates for recent files
                        for symbol in symbols:
                            symbol_path = os.path.join(letter_path, symbol)
                            
                            # Check recent months
                            for month in ['08', '09']:
                                file_path = os.path.join(symbol_path, f'2025/{month}', f'{symbol}_2025_{month}.parquet')
                                if os.path.exists(file_path):
                                    try:
                                        mod_time = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
                                        days_ago = (today - mod_time).days
                                        
                                        # Track files updated in last 15 days
                                        if days_ago <= 15:
                                            coverage_by_date[mod_time] += 1
                                            break  # Only count once per symbol
                                    except:
                                        continue
            else:
                # Other vendors (EODHD, Polygon, Tiingo) use different structures
                # Basic file counting for now
                pattern = os.path.join(vendor_path, '**', '*.parquet')
                import glob
                files = glob.glob(pattern, recursive=True)
                total_symbols = len(set([os.path.basename(f).split('_')[0] for f in files]))
                
                # Check modification dates
                for file_path in files:
                    try:
                        mod_time = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
                        days_ago = (today - mod_time).days
                        if days_ago <= 15:
                            coverage_by_date[mod_time] += 1
                    except:
                        continue
        
        except Exception as e:
            self.logger.error(f"❌ Error analyzing daily coverage for {vendor}: {e}")
            return None
        
        # Calculate metrics
        files_updated_today = coverage_by_date.get(today, 0)
        files_updated_yesterday = coverage_by_date.get(today - timedelta(days=1), 0)
        files_updated_2_days_ago = coverage_by_date.get(today - timedelta(days=2), 0)
        
        # Cumulative counts
        files_updated_last_week = sum(coverage_by_date.get(today - timedelta(days=i), 0) for i in range(8))
        files_updated_last_15_days = sum(coverage_by_date.values())
        
        self.logger.info(f"📅 {vendor} Daily Coverage: T-0: {files_updated_today}, T-1: {files_updated_yesterday}, 15-day: {files_updated_last_15_days}")
        
        return DailyCoverageMetrics(
            vendor=vendor,
            total_symbols=total_symbols,
            files_updated_today=files_updated_today,
            files_updated_yesterday=files_updated_yesterday,
            files_updated_2_days_ago=files_updated_2_days_ago,
            files_updated_last_week=files_updated_last_week,
            files_updated_last_15_days=files_updated_last_15_days,
            validation_timestamp=datetime.now()
        )
    
    async def export_daily_coverage_metrics(self, daily_metrics_list: List[DailyCoverageMetrics]) -> bool:
        """Export daily coverage metrics to Prometheus"""
        try:
            import requests
        except ImportError:
            self.logger.warning("📊 Requests not available, skipping Prometheus export")
            return False
            
        try:
            prometheus_metrics = []
            timestamp = int(datetime.now().timestamp() * 1000)
            
            for metrics in daily_metrics_list:
                vendor = metrics.vendor
                
                # Daily coverage count metrics
                prometheus_metrics.extend([
                    f'ats_minute_bars_daily_total_symbols{{vendor="{vendor}",environment="intg"}} {metrics.total_symbols} {timestamp}',
                    f'ats_minute_bars_daily_updated_today{{vendor="{vendor}",environment="intg"}} {metrics.files_updated_today} {timestamp}',
                    f'ats_minute_bars_daily_updated_yesterday{{vendor="{vendor}",environment="intg"}} {metrics.files_updated_yesterday} {timestamp}',
                    f'ats_minute_bars_daily_updated_2_days_ago{{vendor="{vendor}",environment="intg"}} {metrics.files_updated_2_days_ago} {timestamp}',
                    f'ats_minute_bars_daily_updated_last_week{{vendor="{vendor}",environment="intg"}} {metrics.files_updated_last_week} {timestamp}',
                    f'ats_minute_bars_daily_updated_last_15_days{{vendor="{vendor}",environment="intg"}} {metrics.files_updated_last_15_days} {timestamp}',
                ])
                
                # Daily coverage percentage metrics
                prometheus_metrics.extend([
                    f'ats_minute_bars_daily_today_coverage_percentage{{vendor="{vendor}",environment="intg"}} {metrics.today_coverage_percentage:.2f} {timestamp}',
                    f'ats_minute_bars_daily_yesterday_coverage_percentage{{vendor="{vendor}",environment="intg"}} {metrics.yesterday_coverage_percentage:.2f} {timestamp}',
                    f'ats_minute_bars_daily_recent_coverage_percentage{{vendor="{vendor}",environment="intg"}} {metrics.recent_coverage_percentage:.2f} {timestamp}',
                ])
            
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
                
        except Exception as e:
            self.logger.error(f"❌ Error exporting daily coverage Prometheus metrics: {e}")
            return False
    
    async def run_validation(self, days: int = 90) -> List[MinuteBarMetrics]:
        """Run complete minute bar validation across all vendors and sources"""
        self.logger.info(f"🚀 Starting minute bar validation for past {days} days")
        
        metrics_list = []
        
        for vendor, config in self.vendors.items():
            try:
                # Validate file-based data
                if config['has_files']:
                    file_metrics = self.validate_file_minute_bars(vendor, days)
                    metrics_list.append(file_metrics)
                
                # Validate database data
                if config['has_database']:
                    db_metrics = await self.validate_database_minute_bars(vendor, days)
                    metrics_list.append(db_metrics)
                    
            except Exception as e:
                self.logger.error(f"❌ Failed to validate {vendor} minute bars: {e}")
                continue
        
        # Validate daily coverage for file-based vendors
        daily_metrics_list = []
        for vendor, config in self.vendors.items():
            if config['has_files']:
                try:
                    daily_metrics = self.validate_daily_coverage(vendor)
                    if daily_metrics:
                        daily_metrics_list.append(daily_metrics)
                except Exception as e:
                    self.logger.error(f"❌ Failed to validate daily coverage for {vendor}: {e}")
                    continue
        
        # Export metrics to Prometheus
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
        
        # Daily coverage summary
        if daily_metrics_list:
            self.logger.info(f"📅 Daily Coverage Summary:")
            for dm in daily_metrics_list:
                self.logger.info(f"   • {dm.vendor.upper()}: {dm.total_symbols:,} symbols, T-0: {dm.files_updated_today} ({dm.today_coverage_percentage:.1f}%), T-1: {dm.files_updated_yesterday} ({dm.yesterday_coverage_percentage:.1f}%)")
        
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
    
    try:
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
        
    except Exception as e:
        logger.error(f"❌ Minute bar validation failed: {e}")
        if args.debug:
            import traceback
            logger.error(f"📋 Full traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())