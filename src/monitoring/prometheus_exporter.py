#!/usr/bin/env python3
"""
ATS Data Coverage Monitoring - Prometheus Metrics Exporter
Exports coverage metrics in Prometheus format for Grafana integration.
"""

import asyncio
import asyncpg
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, date, timedelta
from dataclasses import dataclass
import logging
import os

logger = logging.getLogger(__name__)

@dataclass
class PrometheusMetric:
    """Represents a single Prometheus metric."""
    name: str
    metric_type: str  # 'gauge', 'counter', 'histogram'
    help_text: str
    value: float
    labels: Dict[str, str] = None
    timestamp: Optional[int] = None

class PrometheusExporter:
    """Exports ATS coverage monitoring metrics in Prometheus format."""

    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 4432)),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'intg_password'),
            'database': os.getenv('DB_NAME', 'intg_db'),
        }

    async def get_db_connection(self) -> asyncpg.Connection:
        """Get database connection."""
        return await asyncpg.connect(**self.db_config)

    async def collect_coverage_metrics(self) -> List[PrometheusMetric]:
        """Collect all coverage metrics for Prometheus export."""
        metrics = []

        try:
            conn = await self.get_db_connection()

            # 1. Overall coverage percentage
            overall_coverage = await self._get_overall_coverage(conn)
            metrics.append(PrometheusMetric(
                name="ats_data_coverage_percentage",
                metric_type="gauge",
                help_text="Overall data coverage percentage across all symbols",
                value=overall_coverage
            ))

            # 2. Coverage by data type
            coverage_by_type = await self._get_coverage_by_type(conn)
            for data_type, coverage in coverage_by_type.items():
                metrics.append(PrometheusMetric(
                    name="ats_data_coverage_percentage_by_type",
                    metric_type="gauge",
                    help_text="Data coverage percentage by data type",
                    value=coverage,
                    labels={"data_type": data_type}
                ))

            # 3. Total gap count
            gap_count = await self._get_total_gap_count(conn)
            metrics.append(PrometheusMetric(
                name="ats_data_gaps_total",
                metric_type="gauge",
                help_text="Total number of data gaps requiring backfill",
                value=gap_count
            ))

            # 4. Priority gaps (high priority only)
            priority_gaps = await self._get_priority_gap_count(conn)
            metrics.append(PrometheusMetric(
                name="ats_data_gaps_high_priority",
                metric_type="gauge",
                help_text="Number of high priority data gaps (priority >= 7)",
                value=priority_gaps
            ))

            # 5. Coverage by symbol (top 20 priority symbols)
            symbol_coverage = await self._get_symbol_coverage(conn)
            for symbol, coverage in symbol_coverage.items():
                metrics.append(PrometheusMetric(
                    name="ats_data_coverage_by_symbol",
                    metric_type="gauge",
                    help_text="Data coverage percentage by symbol",
                    value=coverage,
                    labels={"symbol": symbol}
                ))

            # 6. Recent backfill operations
            recent_backfills = await self._get_recent_backfill_count(conn)
            metrics.append(PrometheusMetric(
                name="ats_backfill_operations_24h",
                metric_type="gauge",
                help_text="Number of backfill operations completed in last 24 hours",
                value=recent_backfills
            ))

            # 7. Data freshness (days since last update)
            data_freshness = await self._get_data_freshness(conn)
            for data_type, days_old in data_freshness.items():
                metrics.append(PrometheusMetric(
                    name="ats_data_freshness_days",
                    metric_type="gauge",
                    help_text="Days since last data update by type",
                    value=days_old,
                    labels={"data_type": data_type}
                ))

            # 8. Coverage trending (7-day change)
            coverage_trend = await self._get_coverage_trend(conn)
            metrics.append(PrometheusMetric(
                name="ats_data_coverage_trend_7d",
                metric_type="gauge",
                help_text="7-day change in overall coverage percentage",
                value=coverage_trend
            ))

            await conn.close()

        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            # Add error metric
            metrics.append(PrometheusMetric(
                name="ats_monitoring_errors_total",
                metric_type="counter",
                help_text="Total number of monitoring errors",
                value=1
            ))

        return metrics

    async def _get_overall_coverage(self, conn: asyncpg.Connection) -> float:
        """Get overall coverage percentage."""
        query = """
        SELECT COALESCE(AVG(coverage_percentage), 0) as overall_coverage
        FROM dev_daily_coverage_metrics
        WHERE date = CURRENT_DATE - INTERVAL '1 day'
        """
        result = await conn.fetchval(query)
        return float(result or 0)

    async def _get_coverage_by_type(self, conn: asyncpg.Connection) -> Dict[str, float]:
        """Get coverage percentage by data type."""
        query = """
        SELECT data_type, COALESCE(AVG(coverage_percentage), 0) as coverage
        FROM dev_daily_coverage_metrics
        WHERE date = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY data_type
        """
        rows = await conn.fetch(query)
        return {row['data_type']: float(row['coverage']) for row in rows}

    async def _get_total_gap_count(self, conn: asyncpg.Connection) -> int:
        """Get total number of gaps."""
        query = "SELECT COUNT(*) FROM dev_coverage_gaps WHERE status = 'pending'"
        result = await conn.fetchval(query)
        return int(result or 0)

    async def _get_priority_gap_count(self, conn: asyncpg.Connection) -> int:
        """Get number of high priority gaps."""
        query = """
        SELECT COUNT(*) FROM dev_coverage_gaps
        WHERE status = 'pending' AND priority_score >= 7
        """
        result = await conn.fetchval(query)
        return int(result or 0)

    async def _get_symbol_coverage(self, conn: asyncpg.Connection) -> Dict[str, float]:
        """Get coverage by symbol for top priority symbols."""
        query = """
        SELECT
            dct.symbol,
            COALESCE(AVG(dcm.coverage_percentage), 0) as coverage
        FROM dev_data_coverage_tracking dct
        LEFT JOIN dev_daily_coverage_metrics dcm ON dct.symbol = dcm.symbol
            AND dcm.date = CURRENT_DATE - INTERVAL '1 day'
        WHERE dct.symbol IN (
            SELECT symbol FROM dev_priority_symbols
            ORDER BY priority_level DESC LIMIT 20
        )
        GROUP BY dct.symbol
        ORDER BY coverage ASC
        LIMIT 20
        """
        rows = await conn.fetch(query)
        return {row['symbol']: float(row['coverage']) for row in rows}

    async def _get_recent_backfill_count(self, conn: asyncpg.Connection) -> int:
        """Get number of recent backfill operations."""
        query = """
        SELECT COUNT(*) FROM dev_backfill_operations
        WHERE completed_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
        AND status = 'completed'
        """
        result = await conn.fetchval(query)
        return int(result or 0)

    async def _get_data_freshness(self, conn: asyncpg.Connection) -> Dict[str, float]:
        """Get data freshness by type."""
        query = """
        SELECT
            data_type,
            EXTRACT(days FROM CURRENT_DATE - MAX(date)) as days_old
        FROM dev_data_coverage_tracking
        GROUP BY data_type
        """
        rows = await conn.fetch(query)
        return {row['data_type']: float(row['days_old'] or 0) for row in rows}

    async def _get_coverage_trend(self, conn: asyncpg.Connection) -> float:
        """Get 7-day coverage trend."""
        query = """
        WITH recent_coverage AS (
            SELECT date, AVG(coverage_percentage) as daily_coverage
            FROM dev_daily_coverage_metrics
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY date
            ORDER BY date
        ),
        trend AS (
            SELECT
                FIRST_VALUE(daily_coverage) OVER (ORDER BY date ASC) as start_coverage,
                LAST_VALUE(daily_coverage) OVER (ORDER BY date ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as end_coverage
            FROM recent_coverage
            LIMIT 1
        )
        SELECT COALESCE(end_coverage - start_coverage, 0) as trend
        FROM trend
        """
        result = await conn.fetchval(query)
        return float(result or 0)

    def format_prometheus_metrics(self, metrics: List[PrometheusMetric]) -> str:
        """Format metrics in Prometheus exposition format."""
        output = []

        for metric in metrics:
            # Add help text
            output.append(f"# HELP {metric.name} {metric.help_text}")

            # Add type
            output.append(f"# TYPE {metric.name} {metric.metric_type}")

            # Format metric line
            if metric.labels:
                label_str = ",".join([f'{k}="{v}"' for k, v in metric.labels.items()])
                metric_line = f"{metric.name}{{{label_str}}} {metric.value}"
            else:
                metric_line = f"{metric.name} {metric.value}"

            # Add timestamp if provided
            if metric.timestamp:
                metric_line += f" {metric.timestamp}"

            output.append(metric_line)
            output.append("")  # Empty line between metrics

        return "\n".join(output)

    async def export_metrics_to_file(self, output_path: str = "/tmp/ats_coverage_metrics.prom"):
        """Export metrics to file for Prometheus scraping."""
        try:
            metrics = await self.collect_coverage_metrics()
            prometheus_output = self.format_prometheus_metrics(metrics)

            with open(output_path, 'w') as f:
                f.write(prometheus_output)

            logger.info(f"✅ Exported {len(metrics)} metrics to {output_path}")

        except Exception as e:
            logger.error(f"❌ Failed to export metrics: {e}")
            raise

async def main():
    """Main entry point for metrics export."""
    logging.basicConfig(level=logging.INFO)

    exporter = PrometheusExporter()
    await exporter.export_metrics_to_file()

if __name__ == "__main__":
    asyncio.run(main())