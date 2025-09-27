#!/usr/bin/env python3
"""
ATS-INTG Weekly Maintenance Script

Performs comprehensive data quality checks, cleanup operations, and system maintenance
for the ATS Integration environment on a weekly basis.

Features:
- Data quality analysis across all vendors
- Orphaned record cleanup
- Performance statistics
- Database maintenance
- Storage cleanup
- Health monitoring

Usage:
    python3 scripts/weekly_maintenance.py
    python3 scripts/weekly_maintenance.py --deep-clean
    python3 scripts/weekly_maintenance.py --vendors tiingo,polygon
"""

import asyncio
import asyncpg
import logging
import os
import sys
import json
import shutil
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import pandas as pd
import subprocess
from dataclasses import dataclass, asdict

# Add src to path for imports
sys.path.insert(0, '/workspace/src')

from config.database import Database
from dao.instruments_dao import InstrumentsDAO
from dao.daily_prices_dao import DailyPricesDAO

logger = logging.getLogger(__name__)

@dataclass
class DataQualityMetric:
    """Data quality metric for analysis."""
    vendor: str
    metric_name: str
    value: float
    threshold: float
    status: str  # 'pass', 'warning', 'fail'
    description: str

@dataclass
class MaintenanceResult:
    """Results from weekly maintenance operations."""
    operation: str
    success: bool
    records_affected: int
    execution_time_seconds: float
    details: Dict
    errors: List[str]

class WeeklyMaintenance:
    """Main class for weekly maintenance operations."""

    def __init__(self):
        self.db_pool = None
        self.maintenance_results = []
        self.data_quality_metrics = []

    async def initialize(self):
        """Initialize database connections."""
        # Database connection for INTG environment
        db_url = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'intg_password')}@{os.getenv('DB_HOST', 'ats-intg-postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'intg_db')}"

        self.db_pool = await asyncpg.create_pool(
            db_url,
            min_size=2,
            max_size=10,
            command_timeout=600  # 10 minutes for maintenance operations
        )

        logger.info("✅ Weekly maintenance initialized successfully")

    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()

    async def analyze_data_quality(self) -> List[DataQualityMetric]:
        """Perform comprehensive data quality analysis."""
        logger.info("🔍 Starting data quality analysis...")
        metrics = []

        async with self.db_pool.acquire() as conn:
            # Get basic statistics
            vendors = ['tiingo', 'polygon', 'eodhd']

            for vendor in vendors:
                table_name = f"intg_daily_price_{vendor}"

                # Data completeness metrics
                completeness_metrics = await self._analyze_data_completeness(conn, vendor, table_name)
                metrics.extend(completeness_metrics)

                # Data freshness metrics
                freshness_metrics = await self._analyze_data_freshness(conn, vendor, table_name)
                metrics.extend(freshness_metrics)

                # Data consistency metrics
                consistency_metrics = await self._analyze_data_consistency(conn, vendor, table_name)
                metrics.extend(consistency_metrics)

        self.data_quality_metrics = metrics
        logger.info(f"✅ Data quality analysis completed: {len(metrics)} metrics generated")

        return metrics

    async def _analyze_data_completeness(self, conn, vendor: str, table_name: str) -> List[DataQualityMetric]:
        """Analyze data completeness for a vendor."""
        metrics = []

        # Total records
        total_records_query = f"SELECT COUNT(*) as count FROM {table_name}"
        total_records = await conn.fetchval(total_records_query)

        # Records with missing critical fields
        missing_close_query = f"SELECT COUNT(*) as count FROM {table_name} WHERE close IS NULL OR close = 0"
        missing_close = await conn.fetchval(missing_close_query)

        # Completeness percentage
        completeness_pct = ((total_records - missing_close) / max(total_records, 1)) * 100

        metrics.append(DataQualityMetric(
            vendor=vendor,
            metric_name='data_completeness',
            value=completeness_pct,
            threshold=95.0,
            status='pass' if completeness_pct >= 95.0 else ('warning' if completeness_pct >= 90.0 else 'fail'),
            description=f"Percentage of records with valid close prices ({total_records - missing_close} of {total_records})"
        ))

        return metrics

    async def _analyze_data_freshness(self, conn, vendor: str, table_name: str) -> List[DataQualityMetric]:
        """Analyze data freshness for a vendor."""
        metrics = []

        # Most recent data date
        latest_date_query = f"SELECT MAX(date) as latest_date FROM {table_name}"
        latest_date = await conn.fetchval(latest_date_query)

        if latest_date:
            days_since_update = (date.today() - latest_date).days

            metrics.append(DataQualityMetric(
                vendor=vendor,
                metric_name='data_freshness',
                value=days_since_update,
                threshold=5.0,
                status='pass' if days_since_update <= 5 else ('warning' if days_since_update <= 10 else 'fail'),
                description=f"Days since most recent data ({latest_date})"
            ))
        else:
            metrics.append(DataQualityMetric(
                vendor=vendor,
                metric_name='data_freshness',
                value=999,
                threshold=5.0,
                status='fail',
                description="No data found"
            ))

        return metrics

    async def _analyze_data_consistency(self, conn, vendor: str, table_name: str) -> List[DataQualityMetric]:
        """Analyze data consistency for a vendor."""
        metrics = []

        # Price consistency (high >= low, etc.)
        inconsistent_prices_query = f"""
        SELECT COUNT(*) as count
        FROM {table_name}
        WHERE high < low OR high < close OR high < open OR low > close OR low > open
        """
        inconsistent_prices = await conn.fetchval(inconsistent_prices_query)

        # Total records for percentage calculation
        total_records_query = f"SELECT COUNT(*) as count FROM {table_name}"
        total_records = await conn.fetchval(total_records_query)

        if total_records > 0:
            consistency_pct = ((total_records - inconsistent_prices) / total_records) * 100

            metrics.append(DataQualityMetric(
                vendor=vendor,
                metric_name='price_consistency',
                value=consistency_pct,
                threshold=99.0,
                status='pass' if consistency_pct >= 99.0 else ('warning' if consistency_pct >= 95.0 else 'fail'),
                description=f"Percentage of records with consistent OHLC prices ({inconsistent_prices} inconsistent of {total_records})"
            ))

        return metrics

    async def cleanup_orphaned_records(self) -> MaintenanceResult:
        """Clean up orphaned records and invalid data."""
        logger.info("🧹 Starting orphaned record cleanup...")
        start_time = asyncio.get_event_loop().time()

        result = MaintenanceResult(
            operation='cleanup_orphaned_records',
            success=True,
            records_affected=0,
            execution_time_seconds=0,
            details={},
            errors=[]
        )

        async with self.db_pool.acquire() as conn:
            vendors = ['tiingo', 'polygon', 'eodhd']
            cleanup_details = {}

            for vendor in vendors:
                table_name = f"intg_daily_price_{vendor}"

                # Remove records with invalid dates (far future or far past)
                invalid_dates_query = f"""
                DELETE FROM {table_name}
                WHERE date < '1900-01-01' OR date > CURRENT_DATE + INTERVAL '1 year'
                """
                invalid_dates_deleted = await conn.execute(invalid_dates_query)
                invalid_count = int(invalid_dates_deleted.split()[-1])

                # Remove records with all NULL price fields
                null_prices_query = f"""
                DELETE FROM {table_name}
                WHERE (open IS NULL OR open = 0)
                  AND (high IS NULL OR high = 0)
                  AND (low IS NULL OR low = 0)
                  AND (close IS NULL OR close = 0)
                """
                null_prices_deleted = await conn.execute(null_prices_query)
                null_count = int(null_prices_deleted.split()[-1])

                # Remove records referencing non-existent instruments
                orphaned_query = f"""
                DELETE FROM {table_name}
                WHERE instrument_id NOT IN (SELECT id FROM intg_instrument)
                """
                orphaned_deleted = await conn.execute(orphaned_query)
                orphaned_count = int(orphaned_deleted.split()[-1])

                vendor_total = invalid_count + null_count + orphaned_count
                result.records_affected += vendor_total

                cleanup_details[vendor] = {
                    'invalid_dates_removed': invalid_count,
                    'null_prices_removed': null_count,
                    'orphaned_records_removed': orphaned_count,
                    'total_removed': vendor_total
                }

                logger.info(f"✅ {vendor}: removed {vendor_total} records")

            result.details = cleanup_details

        result.execution_time_seconds = asyncio.get_event_loop().time() - start_time
        return result

    async def optimize_database_performance(self) -> MaintenanceResult:
        """Optimize database performance with maintenance operations."""
        logger.info("⚡ Starting database performance optimization...")
        start_time = asyncio.get_event_loop().time()

        result = MaintenanceResult(
            operation='database_optimization',
            success=True,
            records_affected=0,
            execution_time_seconds=0,
            details={},
            errors=[]
        )

        async with self.db_pool.acquire() as conn:
            optimization_details = {}

            # Update table statistics
            tables_to_analyze = [
                'intg_instruments',
                'intg_daily_price_tiingo',
                'intg_daily_price_polygon',
                'intg_daily_price_eodhd'
            ]

            for table in tables_to_analyze:
                await conn.execute(f"ANALYZE {table}")
                logger.info(f"✅ Analyzed table statistics: {table}")
            for table in tables_to_analyze:
                # Note: VACUUM cannot be run inside a transaction, so we'll skip it
                # await conn.execute(f"VACUUM ANALYZE {table}")
                pass
            price_tables = [t for t in tables_to_analyze if 'daily_prices' in t]
            for table in price_tables:
                # Check index usage and rebuild if needed
                index_query = f"""
                SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
                FROM pg_stat_user_indexes
                WHERE tablename = '{table}'
                """
                indexes = await conn.fetch(index_query)
                optimization_details[f"{table}_indexes"] = len(indexes)

            result.details = optimization_details

        result.execution_time_seconds = asyncio.get_event_loop().time() - start_time
        return result

    async def cleanup_storage_space(self) -> MaintenanceResult:
        """Clean up temporary files and logs to free storage space."""
        logger.info("💾 Starting storage space cleanup...")
        start_time = asyncio.get_event_loop().time()

        result = MaintenanceResult(
            operation='storage_cleanup',
            success=True,
            records_affected=0,
            execution_time_seconds=0,
            details={},
            errors=[]
        )

        cleanup_details = {}

        # Clean up old log files (older than 30 days)
        log_dirs = ['/logs', '/data/temp', '/data/analysis']

        for log_dir in log_dirs:
            if Path(log_dir).exists():
                files_removed = 0
                bytes_freed = 0
                cutoff_date = datetime.now() - timedelta(days=30)

                for file_path in Path(log_dir).rglob('*'):
                    if file_path.is_file():
                        # Check file age
                        file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_time < cutoff_date:
                            # Check if it's a log file or temp file
                            if any(ext in file_path.suffix.lower() for ext in ['.log', '.tmp', '.temp']):
                                file_size = file_path.stat().st_size
                                file_path.unlink()
                                files_removed += 1
                                bytes_freed += file_size
                cleanup_details[log_dir] = {
                    'files_removed': files_removed,
                    'bytes_freed': bytes_freed,
                    'mb_freed': round(bytes_freed / (1024 * 1024), 2)
                }

                if files_removed > 0:
                    logger.info(f"✅ {log_dir}: removed {files_removed} files ({cleanup_details[log_dir]['mb_freed']} MB)")

        disk_usage = shutil.disk_usage('/data')
        cleanup_details['disk_usage'] = {
            'total_gb': round(disk_usage.total / (1024**3), 2),
            'free_gb': round(disk_usage.free / (1024**3), 2),
            'used_gb': round((disk_usage.total - disk_usage.free) / (1024**3), 2),
            'free_percent': round((disk_usage.free / disk_usage.total) * 100, 1)
        }
        result.details = cleanup_details
        result.records_affected = sum(d.get('files_removed', 0) for d in cleanup_details.values() if isinstance(d, dict))

        result.execution_time_seconds = asyncio.get_event_loop().time() - start_time
        return result

    async def generate_performance_report(self) -> Dict:
        """Generate comprehensive performance and health report."""
        logger.info("📊 Generating performance report...")

        report = {
            'maintenance_date': date.today().isoformat(),
            'maintenance_timestamp': datetime.now().isoformat(),
            'data_quality_metrics': [],
            'maintenance_results': [],
            'performance_summary': {},
            'recommendations': []
        }

        async with self.db_pool.acquire() as conn:
            # Database size information
            db_stats_query = """
            SELECT
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'intg_%'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            """

            db_stats = await conn.fetch(db_stats_query)

            # Performance statistics
            performance_stats = {
                'database_size': {
                    'total_tables': len(db_stats),
                    'largest_table': db_stats[0]['tablename'] if db_stats else None,
                    'largest_table_size': db_stats[0]['size'] if db_stats else None,
                    'total_size_bytes': sum(row['size_bytes'] for row in db_stats)
                }
            }

            # Record counts by vendor
            vendors = ['tiingo', 'polygon', 'eodhd']
            vendor_stats = {}

            for vendor in vendors:
                table_name = f"intg_daily_price_{vendor}"
                count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                count = await conn.fetchval(count_query)

                latest_query = f"SELECT MAX(date) as latest_date FROM {table_name}"
                latest_date = await conn.fetchval(latest_query)

                vendor_stats[vendor] = {
                    'total_records': count,
                    'latest_date': latest_date.isoformat() if latest_date else None
                }
            performance_stats['vendor_statistics'] = vendor_stats
            report['performance_summary'] = performance_stats

        # Add data quality metrics
        report['data_quality_metrics'] = [asdict(metric) for metric in self.data_quality_metrics]

        # Add maintenance results
        report['maintenance_results'] = [asdict(result) for result in self.maintenance_results]

        # Generate recommendations based on results
        recommendations = []

        # Check data quality metrics for recommendations
        failed_metrics = [m for m in self.data_quality_metrics if m.status == 'fail']
        if failed_metrics:
            recommendations.append(f"Address {len(failed_metrics)} failing data quality metrics")

        # Check maintenance results for issues
        failed_operations = [r for r in self.maintenance_results if not r.success]
        if failed_operations:
            recommendations.append(f"Investigate {len(failed_operations)} failed maintenance operations")

        # Check disk space
        if 'disk_usage' in report.get('maintenance_results', [{}])[-1].get('details', {}):
            disk_info = None
            for result in self.maintenance_results:
                if 'disk_usage' in result.details:
                    disk_info = result.details['disk_usage']
                    break

            if disk_info and disk_info.get('free_percent', 100) < 20:
                recommendations.append("Disk space is running low - consider expanding storage")

        report['recommendations'] = recommendations

        return report

    async def send_maintenance_summary(self, report: Dict):
        """Send weekly maintenance summary via Slack."""
        webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        if not webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not configured - skipping summary notification")
            return

        # Create summary message
        summary_text = f"📅 **Weekly Maintenance Summary - {report['maintenance_date']}**\n\n"

        # Data quality summary
        metrics = report.get('data_quality_metrics', [])
        if metrics:
            passed_metrics = len([m for m in metrics if m['status'] == 'pass'])
            failed_metrics = len([m for m in metrics if m['status'] == 'fail'])
            summary_text += f"📊 **Data Quality:** {passed_metrics} passed, {failed_metrics} failed\n"

        # Maintenance operations summary
        operations = report.get('maintenance_results', [])
        if operations:
            successful_ops = len([op for op in operations if op['success']])
            summary_text += f"🔧 **Maintenance:** {successful_ops}/{len(operations)} operations successful\n"

        # Performance summary
        perf = report.get('performance_summary', {})
        if 'vendor_statistics' in perf:
            vendor_stats = perf['vendor_statistics']
            total_records = sum(v.get('total_records', 0) for v in vendor_stats.values() if isinstance(v, dict))
            summary_text += f"📈 **Data Volume:** {total_records:,} total price records\n"

        # Recommendations
        recommendations = report.get('recommendations', [])
        if recommendations:
            summary_text += f"\n⚠️ **Recommendations:**\n"
            for rec in recommendations[:3]:  # Show first 3
                summary_text += f"• {rec}\n"

        # Send notification
        import aiohttp
        async with aiohttp.ClientSession() as session:
            payload = {"text": summary_text}
            async with session.post(webhook_url, json=payload) as resp:
                if resp.status == 200:
                    logger.info("✅ Maintenance summary sent successfully")
                else:
                    logger.error(f"❌ Failed to send maintenance summary: {resp.status}")

    async def run_weekly_maintenance(self, deep_clean: bool = False) -> Dict:
        """Run complete weekly maintenance process."""
        start_time = asyncio.get_event_loop().time()
        logger.info("🚀 Starting weekly maintenance process...")

        # Step 1: Data quality analysis
        logger.info("\n🔍 Step 1: Data Quality Analysis")
        await self.analyze_data_quality()

        # Step 2: Cleanup orphaned records
        logger.info("\n🧹 Step 2: Orphaned Record Cleanup")
        cleanup_result = await self.cleanup_orphaned_records()
        self.maintenance_results.append(cleanup_result)

        # Step 3: Database optimization
        logger.info("\n⚡ Step 3: Database Optimization")
        optimization_result = await self.optimize_database_performance()
        self.maintenance_results.append(optimization_result)

        # Step 4: Storage cleanup
        logger.info("\n💾 Step 4: Storage Cleanup")
        storage_result = await self.cleanup_storage_space()
        self.maintenance_results.append(storage_result)

        # Step 5: Generate comprehensive report
        logger.info("\n📊 Step 5: Performance Report Generation")
        report = await self.generate_performance_report()
        report['execution_time_seconds'] = asyncio.get_event_loop().time() - start_time
        report['deep_clean_enabled'] = deep_clean

        # Step 6: Send summary notification
        logger.info("\n📤 Step 6: Sending Maintenance Summary")
        await self.send_maintenance_summary(report)

        # Save detailed report
        output_dir = Path("/logs")
        output_dir.mkdir(exist_ok=True)

        report_file = output_dir / f"weekly_maintenance_report_{date.today().strftime('%Y%m%d')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"📋 Weekly maintenance report saved: {report_file}")

        # Log final summary
        total_operations = len(self.maintenance_results)
        successful_operations = len([r for r in self.maintenance_results if r.success])
        total_records_affected = sum(r.records_affected for r in self.maintenance_results)

        logger.info(f"\n✅ Weekly maintenance completed in {report['execution_time_seconds']:.1f} seconds")
        logger.info(f"📊 Operations: {successful_operations}/{total_operations} successful")
        logger.info(f"📈 Records affected: {total_records_affected:,}")

        failed_metrics = len([m for m in self.data_quality_metrics if m.status == 'fail'])
        if failed_metrics > 0:
            logger.warning(f"⚠️ {failed_metrics} data quality metrics require attention")
        else:
            logger.info("✅ All data quality metrics passed")

        return report

async def main():
    """Main function for weekly maintenance."""
    import argparse

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(description='ATS-INTG Weekly Maintenance')
    parser.add_argument('--deep-clean', action='store_true', help='Enable deep cleaning operations')
    parser.add_argument('--vendors', type=str, help='Comma-separated list of vendors to focus on')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("="*80)
    logger.info("ATS-INTG WEEKLY MAINTENANCE")
    logger.info("="*80)
    logger.info(f"Deep clean: {args.deep_clean}")
    logger.info(f"Vendors focus: {args.vendors or 'all'}")

    # Initialize and run maintenance
    maintenance = WeeklyMaintenance()

    await maintenance.initialize()

    report = await maintenance.run_weekly_maintenance(deep_clean=args.deep_clean)

    logger.info("\n🎯 WEEKLY MAINTENANCE COMPLETED SUCCESSFULLY")

if __name__ == "__main__":
    asyncio.run(main())