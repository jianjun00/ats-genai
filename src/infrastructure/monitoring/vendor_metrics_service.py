#!/usr/bin/env python3
"""
Vendor Metrics Service for ATS Real-time Collection Monitoring

Tracks API calls, minute bar collection metrics, and vendor performance.
Provides comprehensive dashboards and monitoring for vendor API status.
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


@dataclass
class ApiCallMetrics:
    """Metrics for an API call."""
    vendor: str
    endpoint: str
    method: str
    status_code: int
    response_time_ms: int
    response_size_bytes: Optional[int] = None
    error_message: Optional[str] = None
    symbols_requested: Optional[List[str]] = None
    rate_limit_remaining: Optional[int] = None
    rate_limit_reset: Optional[datetime] = None


@dataclass
class MinuteBarCollectionMetrics:
    """Metrics for minute bar data collection."""
    vendor: str
    symbol: str
    records_collected: int
    collection_success: bool
    api_calls_made: int = 1
    total_response_time_ms: Optional[int] = None
    error_details: Optional[str] = None
    data_quality_score: float = 0.0


@dataclass
class VendorHealthSummary:
    """Summary of vendor API health."""
    vendor: str
    total_calls: int
    successful_calls: int
    failed_calls: int
    avg_response_time_ms: float
    success_rate: float
    rate_limit_hits: int
    most_common_error: Optional[str]


class VendorMetricsService:
    """Service for tracking and reporting vendor API metrics."""

    def __init__(self, db_host: str = "ats-intg-postgres", db_port: int = 5432,
                 db_user: str = "postgres", db_password: str = "intg_password",
                 db_name: str = "intg_db"):
        self.db_config = {
            "host": db_host,
            "port": db_port,
            "user": db_user,
            "password": db_password,
            "database": db_name
        }

    @asynccontextmanager
    async def get_connection(self):
        """Get database connection."""
        conn = None
        try:
            conn = await asyncpg.connect(**self.db_config)
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                await conn.close()

    async def track_api_call(self, metrics: ApiCallMetrics):
        """Track an individual API call."""
        try:
            async with self.get_connection() as conn:
                await conn.execute("""
                    INSERT INTO intg_api_calls (
                        vendor, endpoint, method, status_code, response_time_ms,
                        response_size_bytes, error_message, symbols_requested,
                        symbols_count, rate_limit_remaining, rate_limit_reset
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                    metrics.vendor, metrics.endpoint, metrics.method,
                    metrics.status_code, metrics.response_time_ms,
                    metrics.response_size_bytes, metrics.error_message,
                    metrics.symbols_requested,
                    len(metrics.symbols_requested) if metrics.symbols_requested else 1,
                    metrics.rate_limit_remaining, metrics.rate_limit_reset
                )

                logger.debug(f"Tracked API call: {metrics.vendor} {metrics.endpoint} -> {metrics.status_code}")

        except Exception as e:
            logger.error(f"Failed to track API call: {e}")

    async def track_minute_bar_collection(self, metrics: MinuteBarCollectionMetrics):
        """Track minute bar collection event."""
        try:
            async with self.get_connection() as conn:
                await conn.execute("""
                    INSERT INTO intg_minute_bar_collection_metrics (
                        vendor, symbol, records_collected, collection_success,
                        api_calls_made, total_response_time_ms, error_details,
                        data_quality_score
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                    metrics.vendor, metrics.symbol, metrics.records_collected,
                    metrics.collection_success, metrics.api_calls_made,
                    metrics.total_response_time_ms, metrics.error_details,
                    metrics.data_quality_score
                )

                logger.debug(f"Tracked collection: {metrics.vendor} {metrics.symbol} -> {metrics.records_collected} records")

        except Exception as e:
            logger.error(f"Failed to track minute bar collection: {e}")

    async def get_vendor_health_summary(self, vendor: str = None,
                                       hours: int = 24) -> List[VendorHealthSummary]:
        """Get vendor health summary for the specified period."""
        try:
            async with self.get_connection() as conn:
                query = """
                    SELECT
                        vendor,
                        COUNT(*) as total_calls,
                        COUNT(*) FILTER (WHERE status_code BETWEEN 200 AND 299) as successful_calls,
                        COUNT(*) FILTER (WHERE status_code >= 400) as failed_calls,
                        ROUND(AVG(response_time_ms), 2) as avg_response_time_ms,
                        ROUND(
                            COUNT(*) FILTER (WHERE status_code BETWEEN 200 AND 299) * 100.0 / COUNT(*),
                            4
                        ) as success_rate,
                        COUNT(*) FILTER (WHERE status_code = 429) as rate_limit_hits,
                        MODE() WITHIN GROUP (ORDER BY error_message) as most_common_error
                    FROM intg_api_calls
                    WHERE request_timestamp >= NOW() - INTERVAL '%d hours'
                """ % hours

                if vendor:
                    query += " AND vendor = $1 GROUP BY vendor"
                    rows = await conn.fetch(query, vendor)
                else:
                    query += " GROUP BY vendor ORDER BY vendor"
                    rows = await conn.fetch(query)

                return [
                    VendorHealthSummary(
                        vendor=row['vendor'],
                        total_calls=row['total_calls'],
                        successful_calls=row['successful_calls'],
                        failed_calls=row['failed_calls'],
                        avg_response_time_ms=float(row['avg_response_time_ms'] or 0),
                        success_rate=float(row['success_rate'] or 0),
                        rate_limit_hits=row['rate_limit_hits'],
                        most_common_error=row['most_common_error']
                    )
                    for row in rows
                ]

        except Exception as e:
            logger.error(f"Failed to get vendor health summary: {e}")
            return []

    async def get_minute_bar_collection_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Get minute bar collection statistics."""
        try:
            async with self.get_connection() as conn:
                # Collection stats by vendor
                collection_stats = await conn.fetch("""
                    SELECT
                        vendor,
                        symbol,
                        COUNT(*) as collection_events,
                        SUM(records_collected) as total_records,
                        COUNT(*) FILTER (WHERE collection_success = true) as successful_collections,
                        ROUND(AVG(data_quality_score), 3) as avg_quality_score,
                        MAX(collection_timestamp) as latest_collection
                    FROM intg_minute_bar_collection_metrics
                    WHERE collection_timestamp >= NOW() - INTERVAL '%d hours'
                    GROUP BY vendor, symbol
                    ORDER BY vendor, symbol
                """ % hours)

                # Current live data freshness
                live_data_stats = await conn.fetch("""
                    SELECT
                        'Tiingo' as vendor,
                        symbol,
                        COUNT(*) as current_records,
                        MAX(timestamp) as latest_timestamp,
                        EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60 as minutes_since_last
                    FROM intg_one_minute_live_tiingo
                    WHERE timestamp >= NOW() - INTERVAL '24 hours'
                    GROUP BY symbol
                    UNION ALL
                    SELECT
                        'Polygon' as vendor,
                        symbol,
                        COUNT(*) as current_records,
                        MAX(timestamp) as latest_timestamp,
                        EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60 as minutes_since_last
                    FROM intg_one_minute_live_polygon
                    WHERE timestamp >= NOW() - INTERVAL '24 hours'
                    GROUP BY symbol
                    ORDER BY vendor, symbol
                """)

                return {
                    "collection_stats": [dict(row) for row in collection_stats],
                    "live_data_stats": [dict(row) for row in live_data_stats],
                    "period_hours": hours,
                    "generated_at": datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"Failed to get minute bar collection stats: {e}")
            return {"error": str(e)}

    async def get_api_status_breakdown(self, hours: int = 24) -> Dict[str, Any]:
        """Get detailed API status code breakdown."""
        try:
            async with self.get_connection() as conn:
                status_breakdown = await conn.fetch("""
                    SELECT
                        vendor,
                        status_code,
                        COUNT(*) as call_count,
                        ROUND(AVG(response_time_ms), 2) as avg_response_time,
                        MAX(request_timestamp) as last_occurrence
                    FROM intg_api_calls
                    WHERE request_timestamp >= NOW() - INTERVAL '%d hours'
                    GROUP BY vendor, status_code
                    ORDER BY vendor, status_code
                """ % hours)

                # Recent errors
                recent_errors = await conn.fetch("""
                    SELECT
                        vendor,
                        endpoint,
                        status_code,
                        error_message,
                        request_timestamp,
                        response_time_ms
                    FROM intg_api_calls
                    WHERE request_timestamp >= NOW() - INTERVAL '%d hours'
                      AND status_code >= 400
                    ORDER BY request_timestamp DESC
                    LIMIT 20
                """ % hours)

                return {
                    "status_breakdown": [dict(row) for row in status_breakdown],
                    "recent_errors": [dict(row) for row in recent_errors],
                    "period_hours": hours
                }

        except Exception as e:
            logger.error(f"Failed to get API status breakdown: {e}")
            return {"error": str(e)}

    async def generate_prometheus_metrics(self) -> str:
        """Generate Prometheus metrics for vendor monitoring."""
        try:
            metrics_lines = [
                "# HELP ats_vendor_api_calls_total Total API calls by vendor and status code",
                "# TYPE ats_vendor_api_calls_total counter",
                "",
                "# HELP ats_vendor_api_response_time_seconds API response time by vendor",
                "# TYPE ats_vendor_api_response_time_seconds histogram",
                "",
                "# HELP ats_minute_bar_records_collected_total Total minute bar records collected",
                "# TYPE ats_minute_bar_records_collected_total counter",
                "",
                "# HELP ats_minute_bar_collection_success_rate Collection success rate by vendor",
                "# TYPE ats_minute_bar_collection_success_rate gauge",
                ""
            ]

            # Get recent metrics
            async with self.get_connection() as conn:
                # API call metrics
                api_metrics = await conn.fetch("""
                    SELECT vendor, status_code, COUNT(*) as count,
                           AVG(response_time_ms) as avg_response_time
                    FROM intg_api_calls
                    WHERE request_timestamp >= NOW() - INTERVAL '1 hour'
                    GROUP BY vendor, status_code
                """)

                timestamp = int(datetime.now().timestamp())

                for row in api_metrics:
                    vendor = row['vendor']
                    status_code = row['status_code']
                    count = row['count']
                    avg_time = float(row['avg_response_time'] or 0) / 1000.0  # Convert to seconds

                    metrics_lines.append(
                        f'ats_vendor_api_calls_total{{vendor="{vendor}",status_code="{status_code}"}} {count} {timestamp}'
                    )
                    metrics_lines.append(
                        f'ats_vendor_api_response_time_seconds{{vendor="{vendor}"}} {avg_time:.3f} {timestamp}'
                    )

                # Collection metrics
                collection_metrics = await conn.fetch("""
                    SELECT vendor, symbol,
                           SUM(records_collected) as total_records,
                           COUNT(*) FILTER (WHERE collection_success = true) * 100.0 / COUNT(*) as success_rate
                    FROM intg_minute_bar_collection_metrics
                    WHERE collection_timestamp >= NOW() - INTERVAL '1 hour'
                    GROUP BY vendor, symbol
                """)

                for row in collection_metrics:
                    vendor = row['vendor']
                    symbol = row['symbol']
                    total_records = row['total_records'] or 0
                    success_rate = float(row['success_rate'] or 0) / 100.0

                    metrics_lines.append(
                        f'ats_minute_bar_records_collected_total{{vendor="{vendor}",symbol="{symbol}"}} {total_records} {timestamp}'
                    )
                    metrics_lines.append(
                        f'ats_minute_bar_collection_success_rate{{vendor="{vendor}",symbol="{symbol}"}} {success_rate:.4f} {timestamp}'
                    )

            return '\n'.join(metrics_lines) + '\n'

        except Exception as e:
            logger.error(f"Failed to generate Prometheus metrics: {e}")
            return f"# ERROR: {e}\n"

    async def update_vendor_health_summary(self):
        """Update the vendor health summary table with current period data."""
        try:
            async with self.get_connection() as conn:
                # Update hourly summaries
                await conn.execute("""
                    INSERT INTO intg_vendor_api_health (
                        vendor, period_start, period_end, total_calls, successful_calls,
                        failed_calls, avg_response_time_ms, success_rate, rate_limit_hits,
                        most_common_error
                    )
                    SELECT
                        vendor,
                        date_trunc('hour', NOW() - INTERVAL '1 hour') as period_start,
                        date_trunc('hour', NOW()) as period_end,
                        COUNT(*) as total_calls,
                        COUNT(*) FILTER (WHERE status_code BETWEEN 200 AND 299) as successful_calls,
                        COUNT(*) FILTER (WHERE status_code >= 400) as failed_calls,
                        ROUND(AVG(response_time_ms), 2) as avg_response_time_ms,
                        ROUND(COUNT(*) FILTER (WHERE status_code BETWEEN 200 AND 299) * 100.0 / COUNT(*), 4) as success_rate,
                        COUNT(*) FILTER (WHERE status_code = 429) as rate_limit_hits,
                        MODE() WITHIN GROUP (ORDER BY error_message) as most_common_error
                    FROM intg_api_calls
                    WHERE request_timestamp >= NOW() - INTERVAL '1 hour'
                      AND request_timestamp < date_trunc('hour', NOW())
                    GROUP BY vendor
                    ON CONFLICT (vendor, period_start, period_end) DO UPDATE SET
                        total_calls = EXCLUDED.total_calls,
                        successful_calls = EXCLUDED.successful_calls,
                        failed_calls = EXCLUDED.failed_calls,
                        avg_response_time_ms = EXCLUDED.avg_response_time_ms,
                        success_rate = EXCLUDED.success_rate,
                        rate_limit_hits = EXCLUDED.rate_limit_hits,
                        most_common_error = EXCLUDED.most_common_error,
                        updated_at = NOW()
                """)

                logger.info("Updated vendor health summary")

        except Exception as e:
            logger.error(f"Failed to update vendor health summary: {e}")


# Example usage and testing
if __name__ == "__main__":
    async def test_metrics_service():
        """Test the metrics service."""
        service = VendorMetricsService()

        # Test API call tracking
        api_metrics = ApiCallMetrics(
            vendor="tiingo",
            endpoint="/tiingo/daily/AAPL/prices",
            method="GET",
            status_code=200,
            response_time_ms=150,
            response_size_bytes=2048,
            symbols_requested=["AAPL"],
            rate_limit_remaining=999
        )
        await service.track_api_call(api_metrics)

        # Test minute bar collection tracking
        collection_metrics = MinuteBarCollectionMetrics(
            vendor="tiingo",
            symbol="AAPL",
            records_collected=60,
            collection_success=True,
            api_calls_made=1,
            total_response_time_ms=150,
            data_quality_score=0.95
        )
        await service.track_minute_bar_collection(collection_metrics)

        # Get summaries
        health_summary = await service.get_vendor_health_summary()
        print("Vendor Health Summary:", health_summary)

        collection_stats = await service.get_minute_bar_collection_stats()
        print("Collection Stats:", collection_stats)

        prometheus_metrics = await service.generate_prometheus_metrics()
        print("Prometheus Metrics:", prometheus_metrics[:500])

    asyncio.run(test_metrics_service())