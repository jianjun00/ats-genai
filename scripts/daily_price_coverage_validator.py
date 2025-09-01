#!/usr/bin/env python3
"""
ATS-INTG Daily Price Coverage Validator

Validates that all active instruments have daily prices for trading days over the past 90 days.
Generates Prometheus metrics and sends Slack alerts for missing data.

Features:
- 90-day trading day coverage validation
- Holiday calendar integration (excludes weekends and US holidays)
- Per-vendor coverage analysis (Tiingo, Polygon, EODHD)
- Prometheus metrics export
- Slack alerting for missing price data
- Daily coverage reporting with trends

Usage:
    python3 scripts/daily_price_coverage_validator.py
    python3 scripts/daily_price_coverage_validator.py --vendors tiingo,polygon --days 30
    python3 scripts/daily_price_coverage_validator.py --export-prometheus --alert-threshold 0.95
"""

import asyncio
import asyncpg
import logging
import os
import sys
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Set
from pathlib import Path
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict
import aiohttp
import time
from collections import defaultdict
# import holidays  # Not available in container

# Add src to path for imports
sys.path.insert(0, '/workspace/src')

from config.environment import Environment

logger = logging.getLogger(__name__)

@dataclass
class CoverageMetric:
    """Daily price coverage metric for a specific date."""
    date: date
    vendor: str
    total_instruments: int
    instruments_with_data: int
    coverage_percentage: float
    missing_instruments: List[str]
    is_trading_day: bool

@dataclass
class VendorCoverageSummary:
    """Summary of coverage for a vendor over the validation period."""
    vendor: str
    validation_days: int
    trading_days: int
    total_expected_records: int
    total_actual_records: int
    overall_coverage_percentage: float
    daily_metrics: List[CoverageMetric]
    worst_coverage_dates: List[Tuple[date, float]]
    instruments_frequently_missing: List[Tuple[str, int]]

@dataclass
class PrometheusMetrics:
    """Prometheus metrics for daily price coverage."""
    timestamp: datetime
    total_instruments: int
    instruments_with_recent_data: Dict[str, int]  # vendor -> count
    coverage_percentage: Dict[str, float]  # vendor -> percentage
    missing_data_alerts: Dict[str, int]  # vendor -> count of missing
    data_freshness_hours: Dict[str, float]  # vendor -> hours since latest

class DailyPriceCoverageValidator:
    """Main class for validating daily price coverage."""
    
    def __init__(self, validation_days: int = 90):
        self.validation_days = validation_days
        self.db_pool = None
        
        # US holidays list (major holidays only - simplified for container compatibility)
        self.us_holidays = self._get_us_holidays()
        
        # Coverage results
        self.vendor_summaries = {}
        self.prometheus_metrics = None
        
    def _get_us_holidays(self) -> Set[date]:
        """Get set of major US holidays for current year and last year."""
        current_year = datetime.now().year
        holidays_set = set()
        
        # Major market holidays for current and previous year
        for year in [current_year - 1, current_year]:
            # New Year's Day
            holidays_set.add(date(year, 1, 1))
            # Independence Day
            holidays_set.add(date(year, 7, 4))
            # Christmas
            holidays_set.add(date(year, 12, 25))
            
        return holidays_set
        
    async def initialize(self):
        """Initialize database connections."""
        try:
            # Database connection for INTG environment
            db_url = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'intg_password')}@{os.getenv('DB_HOST', 'ats-intg-postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'intg_db')}"
            
            self.db_pool = await asyncpg.create_pool(
                db_url,
                min_size=2,
                max_size=10,
                command_timeout=300
            )
            
            logger.info("✅ Daily price coverage validator initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize coverage validator: {e}")
            raise
            
    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()
            
    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """Get list of trading days (excluding weekends and US holidays) in date range."""
        trading_days = []
        current_date = start_date
        
        while current_date <= end_date:
            # Skip weekends (Saturday=5, Sunday=6)
            if current_date.weekday() < 5:
                # Skip US holidays
                if current_date not in self.us_holidays:
                    trading_days.append(current_date)
            current_date += timedelta(days=1)
            
        return trading_days
        
    async def get_active_instruments(self) -> List[Tuple[int, str]]:
        """Get list of active instruments (id, symbol) for validation."""
        async with self.db_pool.acquire() as conn:
            query = """
            SELECT id, symbol 
            FROM intg_instruments 
            WHERE active = true 
              AND symbol IS NOT NULL 
              AND symbol != ''
              AND symbol ~ '^[A-Z]{1,5}$'
            ORDER BY symbol
            """
            
            rows = await conn.fetch(query)
            instruments = [(row['id'], row['symbol']) for row in rows]
            
        logger.info(f"Retrieved {len(instruments)} active instruments for coverage validation")
        return instruments
        
    async def validate_vendor_coverage(self, vendor: str, instruments: List[Tuple[int, str]], 
                                     trading_days: List[date]) -> VendorCoverageSummary:
        """Validate daily price coverage for a specific vendor."""
        start_time = time.time()
        table_name = f"intg_daily_prices_{vendor}"
        
        logger.info(f"🔍 Validating {vendor} coverage for {len(instruments)} instruments across {len(trading_days)} trading days")
        
        daily_metrics = []
        total_expected_records = len(instruments) * len(trading_days)
        total_actual_records = 0
        
        async with self.db_pool.acquire() as conn:
            # Check if table exists
            table_exists_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = $1
            )
            """
            table_exists = await conn.fetchval(table_exists_query, table_name)
            
            if not table_exists:
                logger.warning(f"⚠️ Table {table_name} does not exist - skipping {vendor}")
                return VendorCoverageSummary(
                    vendor=vendor,
                    validation_days=self.validation_days,
                    trading_days=len(trading_days),
                    total_expected_records=total_expected_records,
                    total_actual_records=0,
                    overall_coverage_percentage=0.0,
                    daily_metrics=[],
                    worst_coverage_dates=[],
                    instruments_frequently_missing=[]
                )
            
            # Validate coverage for each trading day
            for trading_day in trading_days:
                try:
                    # Get instruments with data for this date
                    coverage_query = f"""
                    SELECT DISTINCT instrument_id
                    FROM {table_name}
                    WHERE date = $1
                    """
                    
                    instruments_with_data = await conn.fetch(coverage_query, trading_day)
                    instruments_with_data_ids = {row['instrument_id'] for row in instruments_with_data}
                    
                    # Calculate coverage
                    instruments_with_data_count = len(instruments_with_data_ids)
                    coverage_percentage = (instruments_with_data_count / len(instruments)) * 100 if instruments else 0
                    
                    # Find missing instruments
                    all_instrument_ids = {inst_id for inst_id, symbol in instruments}
                    missing_instrument_ids = all_instrument_ids - instruments_with_data_ids
                    missing_symbols = [symbol for inst_id, symbol in instruments if inst_id in missing_instrument_ids]
                    
                    # Create daily metric
                    daily_metric = CoverageMetric(
                        date=trading_day,
                        vendor=vendor,
                        total_instruments=len(instruments),
                        instruments_with_data=instruments_with_data_count,
                        coverage_percentage=coverage_percentage,
                        missing_instruments=missing_symbols[:50],  # Limit for readability
                        is_trading_day=True
                    )
                    
                    daily_metrics.append(daily_metric)
                    total_actual_records += instruments_with_data_count
                    
                    # Log significant coverage issues
                    if coverage_percentage < 90:
                        logger.warning(f"⚠️ {vendor} {trading_day}: Low coverage {coverage_percentage:.1f}% ({len(missing_symbols)} missing)")
                    
                except Exception as e:
                    logger.error(f"❌ Error validating {vendor} coverage for {trading_day}: {e}")
                    
        # Calculate overall coverage
        overall_coverage_percentage = (total_actual_records / total_expected_records) * 100 if total_expected_records > 0 else 0
        
        # Find worst coverage dates
        worst_coverage_dates = sorted(
            [(metric.date, metric.coverage_percentage) for metric in daily_metrics],
            key=lambda x: x[1]
        )[:10]  # Top 10 worst dates
        
        # Find instruments frequently missing data
        instrument_missing_counts = defaultdict(int)
        for metric in daily_metrics:
            for symbol in metric.missing_instruments:
                instrument_missing_counts[symbol] += 1
                
        instruments_frequently_missing = sorted(
            instrument_missing_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:20]  # Top 20 frequently missing
        
        summary = VendorCoverageSummary(
            vendor=vendor,
            validation_days=self.validation_days,
            trading_days=len(trading_days),
            total_expected_records=total_expected_records,
            total_actual_records=total_actual_records,
            overall_coverage_percentage=overall_coverage_percentage,
            daily_metrics=daily_metrics,
            worst_coverage_dates=worst_coverage_dates,
            instruments_frequently_missing=instruments_frequently_missing
        )
        
        execution_time = time.time() - start_time
        logger.info(f"✅ {vendor} coverage validation completed in {execution_time:.1f}s: {overall_coverage_percentage:.1f}% coverage")
        
        return summary
        
    async def generate_prometheus_metrics(self) -> PrometheusMetrics:
        """Generate Prometheus metrics from coverage validation results."""
        logger.info("📊 Generating Prometheus metrics...")
        
        # Calculate metrics from vendor summaries
        total_instruments = 0
        instruments_with_recent_data = {}
        coverage_percentage = {}
        missing_data_alerts = {}
        data_freshness_hours = {}
        
        async with self.db_pool.acquire() as conn:
            # Get total instruments
            total_instruments_query = "SELECT COUNT(*) FROM intg_instruments WHERE active = true"
            total_instruments = await conn.fetchval(total_instruments_query)
            
            # Calculate per-vendor metrics
            for vendor, summary in self.vendor_summaries.items():
                table_name = f"intg_daily_prices_{vendor}"
                
                try:
                    # Instruments with recent data (last 7 days)
                    recent_cutoff = date.today() - timedelta(days=7)
                    recent_data_query = f"""
                    SELECT COUNT(DISTINCT instrument_id) 
                    FROM {table_name} 
                    WHERE date >= $1
                    """
                    instruments_with_recent_data[vendor] = await conn.fetchval(recent_data_query, recent_cutoff)
                    
                    # Overall coverage percentage
                    coverage_percentage[vendor] = summary.overall_coverage_percentage
                    
                    # Missing data alerts (instruments missing data in last 5 days)
                    alert_cutoff = date.today() - timedelta(days=5)
                    trading_days_recent = self.get_trading_days(alert_cutoff, date.today() - timedelta(days=1))
                    
                    if trading_days_recent:
                        # Count instruments missing data on most recent trading day
                        latest_trading_day = trading_days_recent[-1]
                        missing_query = f"""
                        SELECT COUNT(*) 
                        FROM intg_instruments i
                        WHERE i.active = true
                          AND i.id NOT IN (
                              SELECT DISTINCT instrument_id 
                              FROM {table_name} 
                              WHERE date = $1
                          )
                        """
                        missing_data_alerts[vendor] = await conn.fetchval(missing_query, latest_trading_day)
                    else:
                        missing_data_alerts[vendor] = 0
                        
                    # Data freshness (hours since most recent data)
                    freshness_query = f"SELECT MAX(date) FROM {table_name}"
                    latest_date = await conn.fetchval(freshness_query)
                    
                    if latest_date:
                        hours_since = (datetime.now().date() - latest_date).days * 24
                        data_freshness_hours[vendor] = float(hours_since)
                    else:
                        data_freshness_hours[vendor] = float('inf')
                        
                except Exception as e:
                    logger.error(f"❌ Error calculating metrics for {vendor}: {e}")
                    instruments_with_recent_data[vendor] = 0
                    coverage_percentage[vendor] = 0.0
                    missing_data_alerts[vendor] = total_instruments
                    data_freshness_hours[vendor] = float('inf')
        
        self.prometheus_metrics = PrometheusMetrics(
            timestamp=datetime.now(),
            total_instruments=total_instruments,
            instruments_with_recent_data=instruments_with_recent_data,
            coverage_percentage=coverage_percentage,
            missing_data_alerts=missing_data_alerts,
            data_freshness_hours=data_freshness_hours
        )
        
        logger.info(f"✅ Prometheus metrics generated: {total_instruments} instruments, {len(coverage_percentage)} vendors")
        return self.prometheus_metrics
        
    async def export_prometheus_metrics(self, metrics_file: str = "/logs/prometheus_metrics.txt"):
        """Export Prometheus metrics to file for scraping."""
        if not self.prometheus_metrics:
            logger.warning("⚠️ No Prometheus metrics available - run generate_prometheus_metrics first")
            return
            
        logger.info(f"📤 Exporting Prometheus metrics to {metrics_file}")
        
        metrics_content = []
        timestamp = int(self.prometheus_metrics.timestamp.timestamp())
        
        # Total instruments gauge
        metrics_content.append(f"# HELP ats_total_instruments Total number of active instruments")
        metrics_content.append(f"# TYPE ats_total_instruments gauge")
        metrics_content.append(f"ats_total_instruments {self.prometheus_metrics.total_instruments} {timestamp}")
        
        # Instruments with recent data by vendor
        metrics_content.append(f"# HELP ats_instruments_with_recent_data Number of instruments with data in last 7 days")
        metrics_content.append(f"# TYPE ats_instruments_with_recent_data gauge")
        for vendor, count in self.prometheus_metrics.instruments_with_recent_data.items():
            metrics_content.append(f'ats_instruments_with_recent_data{{vendor="{vendor}"}} {count} {timestamp}')
            
        # Coverage percentage by vendor
        metrics_content.append(f"# HELP ats_price_coverage_percentage Daily price coverage percentage over validation period")
        metrics_content.append(f"# TYPE ats_price_coverage_percentage gauge")
        for vendor, percentage in self.prometheus_metrics.coverage_percentage.items():
            metrics_content.append(f'ats_price_coverage_percentage{{vendor="{vendor}"}} {percentage:.2f} {timestamp}')
            
        # Missing data alerts by vendor
        metrics_content.append(f"# HELP ats_missing_price_data_alerts Number of instruments missing recent price data")
        metrics_content.append(f"# TYPE ats_missing_price_data_alerts gauge")
        for vendor, count in self.prometheus_metrics.missing_data_alerts.items():
            metrics_content.append(f'ats_missing_price_data_alerts{{vendor="{vendor}"}} {count} {timestamp}')
            
        # Data freshness by vendor
        metrics_content.append(f"# HELP ats_data_freshness_hours Hours since most recent price data")
        metrics_content.append(f"# TYPE ats_data_freshness_hours gauge")
        for vendor, hours in self.prometheus_metrics.data_freshness_hours.items():
            if hours != float('inf'):
                metrics_content.append(f'ats_data_freshness_hours{{vendor="{vendor}"}} {hours:.1f} {timestamp}')
            
        # Write metrics to file
        output_dir = Path(metrics_file).parent
        output_dir.mkdir(exist_ok=True)
        
        with open(metrics_file, 'w') as f:
            f.write('\n'.join(metrics_content) + '\n')
            
        logger.info(f"✅ Prometheus metrics exported: {len(metrics_content)} lines written")
        
    async def send_slack_alerts(self, alert_threshold: float = 0.95):
        """Send Slack alerts for vendors with coverage below threshold."""
        webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        if not webhook_url:
            logger.warning("⚠️ SLACK_WEBHOOK_URL not configured - skipping alerts")
            return
            
        alerts_to_send = []
        
        # Check coverage thresholds
        for vendor, summary in self.vendor_summaries.items():
            if summary.overall_coverage_percentage < (alert_threshold * 100):
                alert_severity = "🚨" if summary.overall_coverage_percentage < 80 else "⚠️"
                
                # Find recent missing data
                recent_metrics = [m for m in summary.daily_metrics if m.date >= date.today() - timedelta(days=7)]
                avg_recent_coverage = np.mean([m.coverage_percentage for m in recent_metrics]) if recent_metrics else 0
                
                alert_info = {
                    'vendor': vendor,
                    'severity': alert_severity,
                    'overall_coverage': summary.overall_coverage_percentage,
                    'recent_coverage': avg_recent_coverage,
                    'missing_count': len(summary.instruments_frequently_missing),
                    'worst_dates': summary.worst_coverage_dates[:3]
                }
                alerts_to_send.append(alert_info)
        
        if not alerts_to_send:
            # Send success summary if no alerts
            await self._send_success_summary()
            return
            
        # Send alerts
        for alert in alerts_to_send:
            alert_text = f"{alert['severity']} **Daily Price Coverage Alert - {alert['vendor'].upper()}**\n\n"
            alert_text += f"📊 **Overall Coverage**: {alert['overall_coverage']:.1f}% (Threshold: {alert_threshold*100:.0f}%)\n"
            alert_text += f"📈 **Recent Coverage**: {alert['recent_coverage']:.1f}% (Last 7 days)\n"
            alert_text += f"📋 **Instruments Frequently Missing**: {alert['missing_count']}\n"
            
            if alert['worst_dates']:
                alert_text += f"\n**📉 Worst Coverage Dates:**\n"
                for date_val, coverage in alert['worst_dates']:
                    alert_text += f"• {date_val}: {coverage:.1f}%\n"
                    
            # Add frequently missing instruments
            if alert['vendor'] in self.vendor_summaries:
                frequently_missing = self.vendor_summaries[alert['vendor']].instruments_frequently_missing[:5]
                if frequently_missing:
                    alert_text += f"\n**🔍 Top Missing Instruments:**\n"
                    for symbol, missing_days in frequently_missing:
                        alert_text += f"• {symbol}: Missing {missing_days} days\n"
            
            try:
                async with aiohttp.ClientSession() as session:
                    payload = {"text": alert_text}
                    async with session.post(webhook_url, json=payload) as resp:
                        if resp.status == 200:
                            logger.info(f"✅ Coverage alert sent for {alert['vendor']}")
                        else:
                            logger.error(f"❌ Failed to send alert for {alert['vendor']}: {resp.status}")
                            
                # Rate limiting between alerts
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ Error sending alert for {alert['vendor']}: {e}")
                
    async def _send_success_summary(self):
        """Send success summary when no alerts are needed."""
        webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        if not webhook_url:
            return
            
        summary_text = f"✅ **Daily Price Coverage Validation - All Systems OK**\n\n"
        
        if self.prometheus_metrics:
            summary_text += f"📊 **Coverage Summary ({self.validation_days} days)**:\n"
            for vendor, coverage in self.prometheus_metrics.coverage_percentage.items():
                recent_count = self.prometheus_metrics.instruments_with_recent_data.get(vendor, 0)
                summary_text += f"• {vendor.upper()}: {coverage:.1f}% ({recent_count} instruments with recent data)\n"
                
            total_missing = sum(self.prometheus_metrics.missing_data_alerts.values())
            summary_text += f"\n📈 **Total Instruments**: {self.prometheus_metrics.total_instruments}\n"
            summary_text += f"⚠️ **Missing Recent Data**: {total_missing}\n"
            
        try:
            async with aiohttp.ClientSession() as session:
                payload = {"text": summary_text}
                async with session.post(webhook_url, json=payload) as resp:
                    if resp.status == 200:
                        logger.info("✅ Coverage success summary sent")
                        
        except Exception as e:
            logger.error(f"❌ Error sending success summary: {e}")
            
    async def generate_coverage_report(self) -> Dict:
        """Generate comprehensive coverage validation report."""
        report = {
            'validation_date': date.today().isoformat(),
            'validation_timestamp': datetime.now().isoformat(),
            'validation_period_days': self.validation_days,
            'vendor_summaries': {},
            'prometheus_metrics': None,
            'summary_statistics': {}
        }
        
        # Add vendor summaries
        for vendor, summary in self.vendor_summaries.items():
            report['vendor_summaries'][vendor] = asdict(summary)
            
        # Add Prometheus metrics
        if self.prometheus_metrics:
            report['prometheus_metrics'] = asdict(self.prometheus_metrics)
            
        # Calculate summary statistics
        if self.vendor_summaries:
            all_coverages = [s.overall_coverage_percentage for s in self.vendor_summaries.values()]
            report['summary_statistics'] = {
                'vendors_validated': len(self.vendor_summaries),
                'average_coverage': np.mean(all_coverages),
                'min_coverage': min(all_coverages),
                'max_coverage': max(all_coverages),
                'vendors_below_95_percent': len([c for c in all_coverages if c < 95]),
                'vendors_below_90_percent': len([c for c in all_coverages if c < 90])
            }
            
        return report
        
    async def run_validation(self, vendors: List[str] = None, export_prometheus: bool = True, 
                           alert_threshold: float = 0.95) -> Dict:
        """Run complete daily price coverage validation."""
        start_time = time.time()
        logger.info("🚀 Starting daily price coverage validation...")
        
        try:
            # Default to all vendors if none specified
            if not vendors:
                vendors = ['tiingo', 'polygon', 'eodhd']
                
            # Calculate validation period
            end_date = date.today() - timedelta(days=1)  # Yesterday (most recent complete day)
            start_date = end_date - timedelta(days=self.validation_days)
            trading_days = self.get_trading_days(start_date, end_date)
            
            logger.info(f"📅 Validation period: {start_date} to {end_date} ({len(trading_days)} trading days)")
            
            # Get active instruments
            instruments = await self.get_active_instruments()
            if not instruments:
                logger.error("❌ No active instruments found for validation")
                return {'error': 'No instruments available'}
                
            # Validate coverage for each vendor
            for vendor in vendors:
                logger.info(f"\n🔍 Validating {vendor} coverage...")
                summary = await self.validate_vendor_coverage(vendor, instruments, trading_days)
                self.vendor_summaries[vendor] = summary
                
                # Log summary
                logger.info(f"✅ {vendor}: {summary.overall_coverage_percentage:.1f}% coverage ({summary.total_actual_records:,} records)")
                
            # Generate Prometheus metrics
            if export_prometheus:
                logger.info("\n📊 Generating Prometheus metrics...")
                await self.generate_prometheus_metrics()
                await self.export_prometheus_metrics()
                
            # Send Slack alerts
            logger.info("\n📤 Checking alert thresholds...")
            await self.send_slack_alerts(alert_threshold=alert_threshold)
            
            # Generate final report
            report = await self.generate_coverage_report()
            report['execution_time_seconds'] = time.time() - start_time
            
            # Save report
            output_dir = Path("/logs")
            output_dir.mkdir(exist_ok=True)
            
            report_file = output_dir / f"price_coverage_validation_{date.today().strftime('%Y%m%d')}.json"
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
                
            logger.info(f"📋 Coverage validation report saved: {report_file}")
            
            # Log final summary
            logger.info(f"\n✅ Coverage validation completed in {report['execution_time_seconds']:.1f} seconds")
            
            if report.get('summary_statistics'):
                stats = report['summary_statistics']
                logger.info(f"📊 Summary: {stats['vendors_validated']} vendors, avg coverage {stats['average_coverage']:.1f}%")
                
                if stats['vendors_below_95_percent'] > 0:
                    logger.warning(f"⚠️ {stats['vendors_below_95_percent']} vendors below 95% coverage")
                else:
                    logger.info("✅ All vendors above 95% coverage threshold")
                    
            return report
            
        except Exception as e:
            logger.error(f"❌ Coverage validation failed: {e}")
            import traceback
            traceback.print_exc()
            raise

async def main():
    """Main function for daily price coverage validation."""
    import argparse
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='ATS-INTG Daily Price Coverage Validator')
    parser.add_argument('--vendors', type=str, help='Comma-separated list of vendors (tiingo,polygon,eodhd)')
    parser.add_argument('--days', type=int, default=90, help='Number of days to validate (default: 90)')
    parser.add_argument('--export-prometheus', action='store_true', default=True, help='Export Prometheus metrics')
    parser.add_argument('--alert-threshold', type=float, default=0.95, help='Alert threshold for coverage percentage')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        
    # Parse vendors
    vendors = None
    if args.vendors:
        vendors = [v.strip() for v in args.vendors.split(',')]
        
    logger.info("="*80)
    logger.info("ATS-INTG DAILY PRICE COVERAGE VALIDATION")
    logger.info("="*80)
    logger.info(f"Validation period: {args.days} days")
    logger.info(f"Vendors: {vendors or 'all'}")
    logger.info(f"Alert threshold: {args.alert_threshold*100:.0f}%")
    logger.info(f"Export Prometheus: {args.export_prometheus}")
    
    # Initialize and run validation
    validator = DailyPriceCoverageValidator(validation_days=args.days)
    
    try:
        await validator.initialize()
        
        report = await validator.run_validation(
            vendors=vendors,
            export_prometheus=args.export_prometheus,
            alert_threshold=args.alert_threshold
        )
        
        logger.info("\n🎯 COVERAGE VALIDATION COMPLETED SUCCESSFULLY")
        
    finally:
        await validator.close()

if __name__ == "__main__":
    asyncio.run(main())