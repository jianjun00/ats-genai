#!/usr/bin/env python3
"""
News Health Monitoring System for ATS-INTG

Monitors the health and performance of news ingestion systems:
- Data freshness and coverage gaps
- API health and rate limit compliance
- Database performance and storage metrics
- Real-time ingestion pipeline health
- Alert generation for critical issues

Features:
- Comprehensive health checks across all vendors
- Prometheus metrics export
- Slack notifications for critical alerts
- Automated recovery suggestions
- Performance trend analysis

Usage:
    python3 scripts/news_health_monitor.py
    python3 scripts/news_health_monitor.py --alert-threshold 60
    python3 scripts/news_health_monitor.py --prometheus-push
"""

import asyncio
import asyncpg
import aiohttp
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class VendorHealthMetrics:
    """Health metrics for a news vendor."""
    vendor: str
    total_articles: int
    articles_last_24h: int
    articles_last_hour: int
    latest_article_age_minutes: float
    oldest_article_age_days: float
    average_articles_per_hour: float
    health_score: float  # 0.0 to 1.0
    issues: List[str]
    recommendations: List[str]

@dataclass
class SystemHealthReport:
    """Overall system health report."""
    timestamp: datetime
    overall_health_score: float
    vendor_metrics: Dict[str, VendorHealthMetrics]
    database_metrics: Dict[str, float]
    realtime_pipeline_status: str
    critical_alerts: List[str]
    warnings: List[str]
    recommendations: List[str]

class NewsHealthMonitor:
    """Main news health monitoring system."""

    def __init__(self, alert_threshold_minutes: int = 60):
        self.alert_threshold_minutes = alert_threshold_minutes
        self.db_pool = None
        self.expected_vendors = ['tiingo', 'polygon', 'eodhd']

        # Health thresholds
        self.thresholds = {
            'critical_freshness_minutes': 240,  # 4 hours
            'warning_freshness_minutes': 120,   # 2 hours
            'min_articles_per_hour': 5,         # Minimum expected per vendor
            'max_articles_per_hour': 500,       # Maximum reasonable per vendor
            'min_health_score': 0.7,            # Below this triggers alerts
            'database_query_timeout_seconds': 30
        }

    async def initialize(self):
        """Initialize the monitoring system."""
        logger.info("🔍 Initializing news health monitoring system...")

        # Database connection
        db_url = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'intg_password')}@{os.getenv('DB_HOST', 'ats-intg-postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'intg_db')}"

        self.db_pool = await asyncpg.create_pool(
            db_url,
            min_size=2,
            max_size=5,
            command_timeout=self.thresholds['database_query_timeout_seconds']
        )

        logger.info("✅ Database connection pool initialized")

    async def cleanup(self):
        """Clean up resources."""
        if self.db_pool:
            await self.db_pool.close()

    async def check_vendor_health(self, vendor: str) -> VendorHealthMetrics:
        """Check health metrics for a specific vendor."""
        async with self.db_pool.acquire() as conn:
            now = datetime.now(timezone.utc)

            # Basic article counts
            total_articles = await conn.fetchval(
                "SELECT COUNT(*) FROM intg_realtime_news WHERE vendor = $1",
                vendor
            ) or 0

            articles_last_24h = await conn.fetchval(
                "SELECT COUNT(*) FROM intg_realtime_news WHERE vendor = $1 AND published_utc >= $2",
                vendor, now - timedelta(hours=24)
            ) or 0

            articles_last_hour = await conn.fetchval(
                "SELECT COUNT(*) FROM intg_realtime_news WHERE vendor = $1 AND published_utc >= $2",
                vendor, now - timedelta(hours=1)
            ) or 0

            # Freshness metrics
            latest_article = await conn.fetchrow(
                "SELECT published_utc FROM intg_realtime_news WHERE vendor = $1 ORDER BY published_utc DESC LIMIT 1",
                vendor
            )

            oldest_article = await conn.fetchrow(
                "SELECT published_utc FROM intg_realtime_news WHERE vendor = $1 ORDER BY published_utc ASC LIMIT 1",
                vendor
            )

            latest_article_age_minutes = 0
            oldest_article_age_days = 0

            if latest_article:
                latest_article_age_minutes = (now - latest_article['published_utc']).total_seconds() / 60

            if oldest_article:
                oldest_article_age_days = (now - oldest_article['published_utc']).days

            # Average articles per hour (last 24h)
            average_articles_per_hour = articles_last_24h / 24.0

            # Calculate health score and identify issues
            health_score, issues, recommendations = self._calculate_vendor_health(
                vendor, total_articles, articles_last_24h, articles_last_hour,
                latest_article_age_minutes, average_articles_per_hour
            )

            return VendorHealthMetrics(
                vendor=vendor,
                total_articles=total_articles,
                articles_last_24h=articles_last_24h,
                articles_last_hour=articles_last_hour,
                latest_article_age_minutes=latest_article_age_minutes,
                oldest_article_age_days=oldest_article_age_days,
                average_articles_per_hour=average_articles_per_hour,
                health_score=health_score,
                issues=issues,
                recommendations=recommendations
            )

    def _calculate_vendor_health(self, vendor: str, total: int, last_24h: int, last_hour: int,
                                freshness_minutes: float, avg_per_hour: float) -> Tuple[float, List[str], List[str]]:
        """Calculate health score and identify issues for a vendor."""
        score = 1.0
        issues = []
        recommendations = []

        # Freshness check (40% of score)
        if freshness_minutes > self.thresholds['critical_freshness_minutes']:
            score -= 0.4
            issues.append(f"Critical: No articles in {freshness_minutes:.0f} minutes")
            recommendations.append(f"Check {vendor} API connectivity and real-time ingestion")
        elif freshness_minutes > self.thresholds['warning_freshness_minutes']:
            score -= 0.2
            issues.append(f"Warning: Last article {freshness_minutes:.0f} minutes old")
            recommendations.append(f"Monitor {vendor} ingestion pipeline")

        # Volume check (30% of score)
        if avg_per_hour < self.thresholds['min_articles_per_hour']:
            score -= 0.3
            issues.append(f"Low volume: {avg_per_hour:.1f} articles/hour (expected >{self.thresholds['min_articles_per_hour']})")
            recommendations.append(f"Review {vendor} API limits and query parameters")
        elif avg_per_hour > self.thresholds['max_articles_per_hour']:
            score -= 0.1
            issues.append(f"High volume: {avg_per_hour:.1f} articles/hour (unusual spike)")
            recommendations.append(f"Check {vendor} for duplicate articles or API issues")

        # Data availability (30% of score)
        if total == 0:
            score -= 0.3
            issues.append("No articles found")
            recommendations.append(f"Run initial {vendor} backfill")
        elif last_24h == 0:
            score -= 0.2
            issues.append("No articles in last 24 hours")
            recommendations.append(f"Check {vendor} real-time ingestion and API key")

        # Ensure score is between 0 and 1
        score = max(0.0, min(1.0, score))

        return score, issues, recommendations

    async def check_database_health(self) -> Dict[str, float]:
        """Check database health metrics."""
        async with self.db_pool.acquire() as conn:
            metrics = {}

            try:
                # Table size
                table_size = await conn.fetchval(
                    "SELECT pg_size_pretty(pg_total_relation_size('intg_realtime_news'))"
                )
                metrics['table_size_readable'] = table_size

                # Index performance
                index_usage = await conn.fetchval(
                    """
                    SELECT idx_scan::float / (seq_scan + idx_scan + 1) as index_usage_ratio
                    FROM pg_stat_user_tables
                    WHERE relname = 'intg_realtime_news'
                    """
                )
                metrics['index_usage_ratio'] = index_usage or 0.0

                # Query performance (average query time)
                query_start = datetime.now()
                await conn.fetchval("SELECT COUNT(*) FROM intg_realtime_news LIMIT 1")
                query_time = (datetime.now() - query_start).total_seconds() * 1000
                metrics['avg_query_time_ms'] = query_time

                # Connection pool health
                metrics['db_pool_size'] = self.db_pool.get_size()
                metrics['db_pool_idle'] = self.db_pool.get_idle_size()

            except Exception as e:
                logger.error(f"Database health check error: {e}")
                metrics['error'] = str(e)

        return metrics

    async def check_realtime_pipeline_status(self) -> str:
        """Check the status of real-time ingestion pipeline."""
        try:
            # Check if real-time ingestion deployment is running
            # This would typically involve Kubernetes API calls
            # For now, we'll check based on recent data patterns

            async with self.db_pool.acquire() as conn:
                now = datetime.now(timezone.utc)

                # Check for consistent data flow (articles every hour for last 6 hours)
                recent_hours = []
                for i in range(6):
                    hour_start = now - timedelta(hours=i+1)
                    hour_end = now - timedelta(hours=i)

                    count = await conn.fetchval(
                        "SELECT COUNT(*) FROM intg_realtime_news WHERE published_utc >= $1 AND published_utc < $2",
                        hour_start, hour_end
                    )
                    recent_hours.append(count or 0)

                # Pipeline is healthy if we have data in most recent hours
                active_hours = len([h for h in recent_hours if h > 0])

                if active_hours >= 4:  # At least 4 of last 6 hours have data
                    return "healthy"
                elif active_hours >= 2:
                    return "degraded"
                else:
                    return "unhealthy"

        except Exception as e:
            logger.error(f"Pipeline status check error: {e}")
            return "error"

    async def generate_health_report(self) -> SystemHealthReport:
        """Generate comprehensive health report."""
        logger.info("📊 Generating news system health report...")

        timestamp = datetime.now(timezone.utc)
        vendor_metrics = {}
        critical_alerts = []
        warnings = []
        recommendations = []

        # Check each vendor
        for vendor in self.expected_vendors:
            try:
                metrics = await self.check_vendor_health(vendor)
                vendor_metrics[vendor] = metrics

                # Collect alerts and recommendations
                for issue in metrics.issues:
                    if issue.startswith("Critical"):
                        critical_alerts.append(f"{vendor}: {issue}")
                    else:
                        warnings.append(f"{vendor}: {issue}")

                recommendations.extend([f"{vendor}: {rec}" for rec in metrics.recommendations])

            except Exception as e:
                logger.error(f"Error checking {vendor} health: {e}")
                critical_alerts.append(f"{vendor}: Health check failed - {e}")

        # Check database health
        database_metrics = await self.check_database_health()

        # Check real-time pipeline
        pipeline_status = await self.check_realtime_pipeline_status()

        if pipeline_status in ['degraded', 'unhealthy', 'error']:
            critical_alerts.append(f"Real-time pipeline: {pipeline_status}")
            recommendations.append("Check real-time news ingestion deployment status")

        # Calculate overall health score
        vendor_scores = [m.health_score for m in vendor_metrics.values()]
        overall_health_score = sum(vendor_scores) / len(vendor_scores) if vendor_scores else 0.0

        # Adjust for critical system issues
        if pipeline_status == 'unhealthy':
            overall_health_score *= 0.5
        elif pipeline_status == 'degraded':
            overall_health_score *= 0.8

        return SystemHealthReport(
            timestamp=timestamp,
            overall_health_score=overall_health_score,
            vendor_metrics=vendor_metrics,
            database_metrics=database_metrics,
            realtime_pipeline_status=pipeline_status,
            critical_alerts=critical_alerts,
            warnings=warnings,
            recommendations=recommendations
        )

    async def send_alert_notification(self, report: SystemHealthReport):
        """Send alert notifications via Slack."""
        webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        if not webhook_url:
            logger.info("SLACK_WEBHOOK_URL not configured - skipping notification")
            return

        # Only send if there are critical alerts or health score is low
        should_alert = (
            len(report.critical_alerts) > 0 or
            report.overall_health_score < self.thresholds['min_health_score']
        )

        if not should_alert:
            logger.info("✅ System healthy - no alerts to send")
            return

        # Create alert message
        health_emoji = "🟢" if report.overall_health_score >= 0.8 else "🟡" if report.overall_health_score >= 0.6 else "🔴"

        message = f"{health_emoji} **ATS-INTG News System Health Alert**\n"
        message += f"Time: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        message += f"Overall Health: {report.overall_health_score:.1%}\n\n"

        if report.critical_alerts:
            message += f"🚨 **Critical Issues ({len(report.critical_alerts)}):**\n"
            for alert in report.critical_alerts:
                message += f"• {alert}\n"
            message += "\n"

        if report.warnings:
            message += f"⚠️ **Warnings ({len(report.warnings)}):**\n"
            for warning in report.warnings[:3]:  # Limit to first 3
                message += f"• {warning}\n"
            if len(report.warnings) > 3:
                message += f"• ...and {len(report.warnings) - 3} more\n"
            message += "\n"

        if report.recommendations:
            message += f"💡 **Recommendations:**\n"
            for rec in report.recommendations[:3]:  # Limit to first 3
                message += f"• {rec}\n"
            if len(report.recommendations) > 3:
                message += f"• ...and {len(report.recommendations) - 3} more\n"

        try:
            async with aiohttp.ClientSession() as session:
                payload = {"text": message}
                async with session.post(webhook_url, json=payload) as resp:
                    if resp.status == 200:
                        logger.info("✅ Alert notification sent to Slack")
                    else:
                        logger.error(f"❌ Failed to send alert notification: {resp.status}")
        except Exception as e:
            logger.error(f"❌ Error sending alert notification: {e}")

    async def export_prometheus_metrics(self, report: SystemHealthReport):
        """Export metrics to Prometheus Pushgateway."""
        gateway_url = os.getenv('PROMETHEUS_GATEWAY', 'localhost:9091')
        job_name = 'ats_news_health_monitor'

        if not gateway_url:
            logger.info("PROMETHEUS_GATEWAY not configured - skipping metrics export")
            return

        try:
            metrics = []

            # Overall health score
            metrics.append(f'ats_news_system_health_score {report.overall_health_score}')

            # Vendor-specific metrics
            for vendor, vendor_metrics in report.vendor_metrics.items():
                metrics.append(f'ats_news_vendor_health_score{{vendor="{vendor}"}} {vendor_metrics.health_score}')
                metrics.append(f'ats_news_vendor_total_articles{{vendor="{vendor}"}} {vendor_metrics.total_articles}')
                metrics.append(f'ats_news_vendor_articles_24h{{vendor="{vendor}"}} {vendor_metrics.articles_last_24h}')
                metrics.append(f'ats_news_vendor_articles_1h{{vendor="{vendor}"}} {vendor_metrics.articles_last_hour}')
                metrics.append(f'ats_news_vendor_freshness_minutes{{vendor="{vendor}"}} {vendor_metrics.latest_article_age_minutes}')
                metrics.append(f'ats_news_vendor_avg_per_hour{{vendor="{vendor}"}} {vendor_metrics.average_articles_per_hour}')

            # Database metrics
            if 'avg_query_time_ms' in report.database_metrics:
                metrics.append(f'ats_news_db_query_time_ms {report.database_metrics["avg_query_time_ms"]}')

            # Pipeline status (convert to numeric)
            pipeline_score = {'healthy': 1.0, 'degraded': 0.5, 'unhealthy': 0.0, 'error': 0.0}.get(
                report.realtime_pipeline_status, 0.0
            )
            metrics.append(f'ats_news_pipeline_health_score {pipeline_score}')

            # Alert counts
            metrics.append(f'ats_news_critical_alerts_count {len(report.critical_alerts)}')
            metrics.append(f'ats_news_warnings_count {len(report.warnings)}')

            # Push to Pushgateway
            metrics_text = '\n'.join(metrics) + '\n'

            async with aiohttp.ClientSession() as session:
                url = f"http://{gateway_url}/metrics/job/{job_name}"
                async with session.post(url, data=metrics_text) as resp:
                    if resp.status in [200, 202]:
                        logger.info("✅ Metrics exported to Prometheus")
                    else:
                        logger.error(f"❌ Failed to export metrics: {resp.status}")

        except Exception as e:
            logger.error(f"❌ Error exporting metrics: {e}")

    def print_health_report(self, report: SystemHealthReport):
        """Print formatted health report."""
        health_emoji = "🟢" if report.overall_health_score >= 0.8 else "🟡" if report.overall_health_score >= 0.6 else "🔴"

        print("="*80)
        print("📰 ATS-INTG NEWS SYSTEM HEALTH REPORT")
        print("="*80)
        print(f"Timestamp: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"Overall Health: {health_emoji} {report.overall_health_score:.1%}")
        print(f"Pipeline Status: {report.realtime_pipeline_status.upper()}")

        print(f"\n📊 VENDOR METRICS:")
        print("-"*60)
        print(f"{'Vendor':<8} {'Health':<8} {'Total':<8} {'24h':<6} {'1h':<4} {'Fresh':<8} {'Rate/h':<8}")
        print("-"*60)

        for vendor, metrics in report.vendor_metrics.items():
            health_icon = "🟢" if metrics.health_score >= 0.8 else "🟡" if metrics.health_score >= 0.6 else "🔴"
            print(f"{vendor:<8} {health_icon}{metrics.health_score:.1%} {metrics.total_articles:<8} "
                  f"{metrics.articles_last_24h:<6} {metrics.articles_last_hour:<4} "
                  f"{metrics.latest_article_age_minutes:.0f}m {metrics.average_articles_per_hour:.1f}")

        if report.database_metrics:
            print(f"\n💾 DATABASE METRICS:")
            for key, value in report.database_metrics.items():
                if key != 'error':
                    print(f"  {key}: {value}")

        if report.critical_alerts:
            print(f"\n🚨 CRITICAL ALERTS ({len(report.critical_alerts)}):")
            for alert in report.critical_alerts:
                print(f"  • {alert}")

        if report.warnings:
            print(f"\n⚠️ WARNINGS ({len(report.warnings)}):")
            for warning in report.warnings:
                print(f"  • {warning}")

        if report.recommendations:
            print(f"\n💡 RECOMMENDATIONS ({len(report.recommendations)}):")
            for rec in report.recommendations:
                print(f"  • {rec}")

        print("="*80)

    async def run_health_check(self, send_notifications: bool = True, export_metrics: bool = True):
        """Run complete health check."""
        try:
            # Generate health report
            report = await self.generate_health_report()

            # Print report
            self.print_health_report(report)

            # Send notifications if enabled
            if send_notifications:
                await self.send_alert_notification(report)

            # Export metrics if enabled
            if export_metrics:
                await self.export_prometheus_metrics(report)

            return report.overall_health_score >= self.thresholds['min_health_score']

        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return False

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='News Health Monitoring System')
    parser.add_argument('--alert-threshold', type=int, default=60,
                       help='Alert threshold in minutes for stale data (default: 60)')
    parser.add_argument('--no-notifications', action='store_true',
                       help='Disable Slack notifications')
    parser.add_argument('--no-metrics', action='store_true',
                       help='Disable Prometheus metrics export')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("="*80)
    logger.info("ATS-INTG NEWS HEALTH MONITORING")
    logger.info("="*80)

    # Initialize and run monitoring
    monitor = NewsHealthMonitor(args.alert_threshold)

    try:
        await monitor.initialize()

        success = await monitor.run_health_check(
            send_notifications=not args.no_notifications,
            export_metrics=not args.no_metrics
        )

        return 0 if success else 1

    except Exception as e:
        logger.error(f"❌ Monitoring failed: {e}")
        return 1
    finally:
        await monitor.cleanup()

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)