#!/usr/bin/env python3
"""
Earnings Data Quality Monitoring System

Real-time monitoring and validation of earnings data quality across all vendors.
Provides alerts, quality scores, and automated remediation.
"""

import asyncio
import os
import sys
from datetime import datetime
from typing import Dict, List
from dataclasses import dataclass
import json

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from core.platform.config.environment import Environment
from core.logging.logger_config import get_logger

logger = get_logger(__name__)

@dataclass
class QualityMetric:
    """Quality metric for earnings data"""
    metric_name: str
    current_value: float
    target_value: float
    threshold_warning: float
    threshold_critical: float
    status: str  # 'good', 'warning', 'critical'
    last_updated: datetime

@dataclass
class VendorHealth:
    """Vendor health status"""
    vendor: str
    total_records: int
    complete_records: int
    error_rate: float
    api_calls_today: int
    rate_limit_delays: int
    avg_quality_score: float
    last_success: datetime

class EarningsQualityMonitor:
    """Real-time earnings data quality monitoring system"""

    def __init__(self):
        self.env = Environment()
        self.quality_thresholds = {
            'eps_coverage': {'target': 0.90, 'warning': 0.85, 'critical': 0.70},
            'revenue_coverage': {'target': 0.95, 'warning': 0.90, 'critical': 0.80},
            'call_timing_coverage': {'target': 0.80, 'warning': 0.60, 'critical': 0.40},
            'vendor_error_rate': {'target': 0.02, 'warning': 0.05, 'critical': 0.10},
            'data_freshness': {'target': 1.0, 'warning': 0.95, 'critical': 0.85}
        }

    async def check_eps_coverage(self, days: int = 30) -> float:
        """Check EPS data coverage over last N days"""
        query = f"""
        SELECT
            COUNT(*) as total_earnings,
            COUNT(CASE WHEN eps_actual_cents IS NOT NULL THEN 1 END) as eps_count
        FROM {self.env.get_table_name('earnings_events')} ee
        JOIN {self.env.get_table_name('financial_events')} fe ON ee.financial_event_id = fe.id
        WHERE ee.created_at >= NOW() - INTERVAL '{days} days'
          AND fe.vendor = 'polygon'
        """

        async with self.env.database.create_pool_with_retry() as pool:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query)
                if row['total_earnings'] == 0:
                    return 1.0
                return row['eps_count'] / row['total_earnings']

    async def check_revenue_coverage(self, days: int = 30) -> float:
        """Check revenue data coverage over last N days"""
        query = f"""
        SELECT
            COUNT(*) as total_earnings,
            COUNT(CASE WHEN revenue_actual_cents IS NOT NULL THEN 1 END) as revenue_count
        FROM {self.env.get_table_name('earnings_events')} ee
        WHERE ee.created_at >= NOW() - INTERVAL '{days} days'
        """

        async with self.env.database.create_pool_with_retry() as pool:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query)
                if row['total_earnings'] == 0:
                    return 1.0
                return row['revenue_count'] / row['total_earnings']

    async def check_call_timing_coverage(self, days: int = 30) -> float:
        """Check earnings call timing coverage"""
        query = f"""
        SELECT
            COUNT(*) as total_earnings,
            COUNT(CASE WHEN earnings_call_datetime IS NOT NULL THEN 1 END) as call_count
        FROM {self.env.get_table_name('earnings_events')} ee
        WHERE ee.created_at >= NOW() - INTERVAL '{days} days'
        """

        async with self.env.database.create_pool_with_retry() as pool:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query)
                if row['total_earnings'] == 0:
                    return 1.0
                return row['call_count'] / row['total_earnings']

    async def check_vendor_health(self, vendor: str, days: int = 7) -> VendorHealth:
        """Check health metrics for specific vendor"""
        query = f"""
        SELECT
            COUNT(*) as total_records,
            COUNT(CASE WHEN ee.eps_actual_cents IS NOT NULL AND ee.revenue_actual_cents IS NOT NULL THEN 1 END) as complete_records,
            AVG(CASE
                WHEN ee.eps_actual_cents IS NOT NULL THEN 0.4 ELSE 0 END +
                CASE WHEN ee.revenue_actual_cents IS NOT NULL THEN 0.4 ELSE 0 END +
                CASE WHEN ee.earnings_call_datetime IS NOT NULL THEN 0.2 ELSE 0 END
            ) as avg_quality_score,
            MAX(ee.created_at) as last_success
        FROM {self.env.get_table_name('earnings_events')} ee
        JOIN {self.env.get_table_name('financial_events')} fe ON ee.financial_event_id = fe.id
        WHERE fe.vendor = $1
          AND ee.created_at >= NOW() - INTERVAL '{days} days'
        """

        async with self.env.database.create_pool_with_retry() as pool:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query, vendor)

                if not row or row['total_records'] == 0:
                    return VendorHealth(
                        vendor=vendor,
                        total_records=0,
                        complete_records=0,
                        error_rate=1.0,
                        api_calls_today=0,
                        rate_limit_delays=0,
                        avg_quality_score=0.0,
                        last_success=datetime.min
                    )

                error_rate = 1.0 - (row['complete_records'] / row['total_records'])

                return VendorHealth(
                    vendor=vendor,
                    total_records=row['total_records'],
                    complete_records=row['complete_records'],
                    error_rate=error_rate,
                    api_calls_today=0,  # Would need separate tracking
                    rate_limit_delays=0,  # Would need separate tracking
                    avg_quality_score=row['avg_quality_score'] or 0.0,
                    last_success=row['last_success'] or datetime.min
                )

    async def calculate_quality_metrics(self) -> List[QualityMetric]:
        """Calculate all quality metrics"""
        metrics = []

        # EPS Coverage
        eps_coverage = await self.check_eps_coverage()
        thresholds = self.quality_thresholds['eps_coverage']
        eps_status = self._get_status(eps_coverage, thresholds)

        metrics.append(QualityMetric(
            metric_name="EPS Coverage (30 days)",
            current_value=eps_coverage,
            target_value=thresholds['target'],
            threshold_warning=thresholds['warning'],
            threshold_critical=thresholds['critical'],
            status=eps_status,
            last_updated=datetime.now()
        ))

        # Revenue Coverage
        revenue_coverage = await self.check_revenue_coverage()
        thresholds = self.quality_thresholds['revenue_coverage']
        revenue_status = self._get_status(revenue_coverage, thresholds)

        metrics.append(QualityMetric(
            metric_name="Revenue Coverage (30 days)",
            current_value=revenue_coverage,
            target_value=thresholds['target'],
            threshold_warning=thresholds['warning'],
            threshold_critical=thresholds['critical'],
            status=revenue_status,
            last_updated=datetime.now()
        ))

        # Call Timing Coverage
        call_coverage = await self.check_call_timing_coverage()
        thresholds = self.quality_thresholds['call_timing_coverage']
        call_status = self._get_status(call_coverage, thresholds)

        metrics.append(QualityMetric(
            metric_name="Call Timing Coverage (30 days)",
            current_value=call_coverage,
            target_value=thresholds['target'],
            threshold_warning=thresholds['warning'],
            threshold_critical=thresholds['critical'],
            status=call_status,
            last_updated=datetime.now()
        ))

        return metrics

    def _get_status(self, value: float, thresholds: Dict) -> str:
        """Determine status based on thresholds"""
        if value >= thresholds['target']:
            return 'good'
        elif value >= thresholds['warning']:
            return 'warning'
        elif value >= thresholds['critical']:
            return 'critical'
        else:
            return 'critical'

    async def generate_quality_report(self) -> Dict:
        """Generate comprehensive quality report"""
        metrics = await self.calculate_quality_metrics()

        # Vendor health for all vendors
        vendors = ['polygon', 'eodhd', 'tiingo', 'alpha_vantage']
        vendor_health = {}

        for vendor in vendors:
            health = await self.check_vendor_health(vendor)
            vendor_health[vendor] = health

        # Overall quality score
        metric_scores = [m.current_value for m in metrics if m.metric_name != "Call Timing Coverage (30 days)"]  # Less critical
        overall_score = sum(metric_scores) / len(metric_scores) if metric_scores else 0.0

        return {
            'timestamp': datetime.now().isoformat(),
            'overall_quality_score': overall_score,
            'status': 'good' if overall_score >= 0.85 else 'warning' if overall_score >= 0.70 else 'critical',
            'metrics': [
                {
                    'name': m.metric_name,
                    'value': m.current_value,
                    'target': m.target_value,
                    'status': m.status
                }
                for m in metrics
            ],
            'vendor_health': {
                vendor: {
                    'total_records': health.total_records,
                    'completion_rate': health.complete_records / health.total_records if health.total_records > 0 else 0,
                    'error_rate': health.error_rate,
                    'quality_score': health.avg_quality_score,
                    'last_success': health.last_success.isoformat() if health.last_success != datetime.min else None
                }
                for vendor, health in vendor_health.items()
            }
        }

    async def send_alert(self, message: str, severity: str = 'warning'):
        """Send quality alert (placeholder - implement with actual alerting)"""
        logger.warning(f"EARNINGS QUALITY ALERT [{severity.upper()}]: {message}")

        # TODO: Implement actual alerting (Slack, email, etc.)
        alert_data = {
            'timestamp': datetime.now().isoformat(),
            'severity': severity,
            'message': message,
            'service': 'earnings_quality_monitor'
        }

        # For now, just log the alert
        logger.info(f"Alert data: {json.dumps(alert_data, indent=2)}")

    async def run_quality_check(self) -> Dict:
        """Run comprehensive quality check and send alerts if needed"""
        logger.info("Running earnings data quality check...")

        try:
            report = await self.generate_quality_report()

            # Check for alerts
            for metric in report['metrics']:
                if metric['status'] == 'critical':
                    await self.send_alert(
                        f"{metric['name']}: {metric['value']:.1%} (below critical threshold)",
                        severity='critical'
                    )
                elif metric['status'] == 'warning':
                    await self.send_alert(
                        f"{metric['name']}: {metric['value']:.1%} (below target)",
                        severity='warning'
                    )

            # Check vendor health
            for vendor, health in report['vendor_health'].items():
                if health['error_rate'] > 0.10:
                    await self.send_alert(
                        f"{vendor}: High error rate {health['error_rate']:.1%}",
                        severity='critical'
                    )

            # Overall status alert
            if report['status'] == 'critical':
                await self.send_alert(
                    f"Overall earnings data quality is CRITICAL: {report['overall_quality_score']:.1%}",
                    severity='critical'
                )

            logger.info(f"Quality check completed. Overall score: {report['overall_quality_score']:.1%}")
            return report

        except Exception as e:
            logger.error(f"Quality check failed: {e}")
            await self.send_alert(f"Quality monitoring system failure: {e}", severity='critical')
            raise

async def main():
    """Main function for command line usage"""
    import argparse

    parser = argparse.ArgumentParser(description="Earnings Data Quality Monitor")
    parser.add_argument('--report', action='store_true', help='Generate quality report')
    parser.add_argument('--monitor', action='store_true', help='Run continuous monitoring')
    parser.add_argument('--interval', type=int, default=300, help='Monitoring interval in seconds')

    args = parser.parse_args()

    monitor = EarningsQualityMonitor()

    if args.report:
        report = await monitor.run_quality_check()
        print(json.dumps(report, indent=2, default=str))

    elif args.monitor:
        logger.info(f"Starting continuous monitoring (interval: {args.interval}s)")
        while True:
            try:
                await monitor.run_quality_check()
                await asyncio.sleep(args.interval)
            except KeyboardInterrupt:
                logger.info("Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(60)  # Wait before retry

    else:
        # Single quality check
        await monitor.run_quality_check()

if __name__ == "__main__":
    asyncio.run(main())