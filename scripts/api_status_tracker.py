#!/usr/bin/env python3
"""
ATS-INTG API Status Code Tracking System

Tracks API status codes and response metrics for all vendor data collection.
Provides metrics for Prometheus and creates detailed status dashboards.

Features:
- Status code tracking by vendor (Tiingo, EODHD, Polygon, Alpha Vantage, FMP)
- Request latency and response size metrics
- Error rate and success rate calculations
- Rate limiting detection and tracking
- Prometheus-compatible metrics endpoint
- Database persistence for historical analysis

Usage:
    from api_status_tracker import APIStatusTracker

    tracker = APIStatusTracker()

    # Track API calls
    tracker.track_request("tiingo", "daily_prices", 200, latency_ms=150, response_size=1024)
    tracker.track_request("polygon", "fundamentals", 429, latency_ms=50)

    # Get metrics
    metrics = tracker.get_prometheus_metrics()
"""

import asyncio
import asyncpg
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict, Counter
import json
import time
from dataclasses import dataclass, asdict
from pathlib import Path

# Add src to Python path
sys.path.insert(0, '/workspace/src')

logger = logging.getLogger(__name__)

@dataclass
class APIRequestRecord:
    """Record for a single API request."""
    vendor: str
    api_endpoint: str  # e.g., "daily_prices", "fundamentals", "news"
    status_code: int
    latency_ms: float
    response_size_bytes: Optional[int] = None
    timestamp: datetime = None
    error_message: Optional[str] = None
    symbol: Optional[str] = None
    request_url: Optional[str] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

class APIStatusTracker:
    """
    Tracks API status codes and metrics for all vendor data collection operations.

    Provides real-time metrics for monitoring API health, rate limiting,
    and performance across all data vendors.
    """

    def __init__(self):
        self.db_pool = None

        # In-memory metrics for real-time tracking
        self.request_counts = defaultdict(lambda: defaultdict(int))  # vendor -> status_code -> count
        self.api_endpoint_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))  # vendor -> endpoint -> status_code -> count
        self.latency_stats = defaultdict(list)  # vendor -> [latency_ms]
        self.error_messages = defaultdict(list)  # vendor -> [error_messages]
        self.rate_limit_events = defaultdict(int)  # vendor -> count

        # Recent requests for dashboard (last 1000 per vendor)
        self.recent_requests = defaultdict(lambda: [])  # vendor -> [APIRequestRecord]
        self.max_recent_requests = 1000

        # Metrics cache
        self.metrics_cache = {}
        self.cache_expiry = 0
        self.cache_duration = 30  # seconds

        # Vendor configurations
        self.vendor_configs = {
            'tiingo': {
                'endpoints': ['daily_prices', 'fundamentals', 'news', 'instruments'],
                'rate_limit_codes': [429],
                'expected_success_codes': [200],
                'base_url': 'api.tiingo.com'
            },
            'polygon': {
                'endpoints': ['daily_prices', 'fundamentals', 'news', 'minute_bars'],
                'rate_limit_codes': [429],
                'expected_success_codes': [200],
                'base_url': 'api.polygon.io'
            },
            'eodhd': {
                'endpoints': ['daily_prices', 'fundamentals', 'news', 'instruments'],
                'rate_limit_codes': [429, 403],
                'expected_success_codes': [200],
                'base_url': 'eodhd.com'
            },
            'alpha_vantage': {
                'endpoints': ['fundamentals', 'economic_indicators'],
                'rate_limit_codes': [429, 403],
                'expected_success_codes': [200],
                'base_url': 'alphavantage.co'
            },
            'fmp': {
                'endpoints': ['fundamentals', 'earnings'],
                'rate_limit_codes': [429, 403],
                'expected_success_codes': [200],
                'base_url': 'financialmodelingprep.com'
            },
            'firstrate': {
                'endpoints': ['minute_bars', 'daily_download'],
                'rate_limit_codes': [429, 403],
                'expected_success_codes': [200],
                'base_url': 'firstrate.com'
            }
        }

    async def initialize(self):
        """Initialize database connections and create tables."""
        try:
            # Database connection for INTG environment
            db_url = f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'intg_password')}@{os.getenv('DB_HOST', 'ats-intg-postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'intg_db')}"

            self.db_pool = await asyncpg.create_pool(
                db_url,
                min_size=2,
                max_size=10,
                command_timeout=60
            )

            # Create API status tracking table
            await self.create_tables()

            logger.info("✅ API Status Tracker initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize API Status Tracker: {e}")
            raise

    async def create_tables(self):
        """Create database tables for API status tracking."""
        try:
            async with self.db_pool.acquire() as conn:
                # Create API requests table
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS intg_api_requests (
                        id SERIAL PRIMARY KEY,
                        vendor VARCHAR(50) NOT NULL,
                        api_endpoint VARCHAR(100) NOT NULL,
                        status_code INTEGER NOT NULL,
                        latency_ms FLOAT NOT NULL,
                        response_size_bytes INTEGER,
                        timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        error_message TEXT,
                        symbol VARCHAR(20),
                        request_url TEXT,

                        -- Indexes for efficient querying
                        INDEX idx_api_requests_vendor_timestamp (vendor, timestamp),
                        INDEX idx_api_requests_status_timestamp (status_code, timestamp),
                        INDEX idx_api_requests_endpoint_timestamp (api_endpoint, timestamp)
                    )
                """)

                # Create API status summary table (for fast dashboard queries)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS intg_api_status_summary (
                        id SERIAL PRIMARY KEY,
                        vendor VARCHAR(50) NOT NULL,
                        api_endpoint VARCHAR(100) NOT NULL,
                        date DATE NOT NULL,
                        hour INTEGER NOT NULL, -- 0-23
                        success_count INTEGER DEFAULT 0,
                        error_count INTEGER DEFAULT 0,
                        rate_limit_count INTEGER DEFAULT 0,
                        total_requests INTEGER DEFAULT 0,
                        avg_latency_ms FLOAT DEFAULT 0,
                        max_latency_ms FLOAT DEFAULT 0,
                        total_response_size_bytes BIGINT DEFAULT 0,
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

                        UNIQUE(vendor, api_endpoint, date, hour)
                    )
                """)

                logger.info("✅ API status tracking tables created")

        except Exception as e:
            logger.error(f"❌ Failed to create API status tables: {e}")
            raise

    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()

    def track_request(
        self,
        vendor: str,
        api_endpoint: str,
        status_code: int,
        latency_ms: float,
        response_size_bytes: Optional[int] = None,
        error_message: Optional[str] = None,
        symbol: Optional[str] = None,
        request_url: Optional[str] = None
    ):
        """
        Track a single API request with status code and metrics.

        Args:
            vendor: Vendor name (tiingo, polygon, eodhd, etc.)
            api_endpoint: API endpoint type (daily_prices, fundamentals, etc.)
            status_code: HTTP status code
            latency_ms: Request latency in milliseconds
            response_size_bytes: Response size in bytes
            error_message: Error message if request failed
            symbol: Symbol being requested (for context)
            request_url: Full request URL (for debugging)
        """
        try:
            # Create record
            record = APIRequestRecord(
                vendor=vendor,
                api_endpoint=api_endpoint,
                status_code=status_code,
                latency_ms=latency_ms,
                response_size_bytes=response_size_bytes,
                error_message=error_message,
                symbol=symbol,
                request_url=request_url
            )

            # Update in-memory metrics
            self.request_counts[vendor][status_code] += 1
            self.api_endpoint_counts[vendor][api_endpoint][status_code] += 1
            self.latency_stats[vendor].append(latency_ms)

            # Track rate limiting
            vendor_config = self.vendor_configs.get(vendor, {})
            rate_limit_codes = vendor_config.get('rate_limit_codes', [429])
            if status_code in rate_limit_codes:
                self.rate_limit_events[vendor] += 1

            # Track error messages
            if error_message and status_code >= 400:
                self.error_messages[vendor].append({
                    'timestamp': record.timestamp,
                    'status_code': status_code,
                    'message': error_message,
                    'endpoint': api_endpoint
                })

            # Add to recent requests (keep only last N)
            vendor_recent = self.recent_requests[vendor]
            vendor_recent.append(record)
            if len(vendor_recent) > self.max_recent_requests:
                vendor_recent.pop(0)  # Remove oldest

            # Persist to database (fire-and-forget)
            asyncio.create_task(self._persist_record(record))

            # Clear cache to force refresh
            self.metrics_cache = {}

            logger.debug(f"📊 Tracked {vendor} {api_endpoint} request: {status_code} ({latency_ms:.1f}ms)")

        except Exception as e:
            logger.error(f"❌ Failed to track API request: {e}")

    async def _persist_record(self, record: APIRequestRecord):
        """Persist API request record to database."""
        try:
            if not self.db_pool:
                return

            async with self.db_pool.acquire() as conn:
                # Insert request record
                await conn.execute("""
                    INSERT INTO intg_api_requests
                    (vendor, api_endpoint, status_code, latency_ms, response_size_bytes,
                     timestamp, error_message, symbol, request_url)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                record.vendor, record.api_endpoint, record.status_code,
                record.latency_ms, record.response_size_bytes, record.timestamp,
                record.error_message, record.symbol, record.request_url)

                # Update hourly summary
                await self._update_hourly_summary(conn, record)

        except Exception as e:
            logger.error(f"❌ Failed to persist API request record: {e}")

    async def _update_hourly_summary(self, conn, record: APIRequestRecord):
        """Update hourly summary statistics."""
        try:
            date_part = record.timestamp.date()
            hour_part = record.timestamp.hour

            # Determine if this is a success, error, or rate limit
            is_success = 1 if record.status_code < 400 else 0
            is_error = 1 if record.status_code >= 400 else 0
            is_rate_limit = 1 if record.status_code in self.vendor_configs.get(record.vendor, {}).get('rate_limit_codes', [429]) else 0

            # Upsert summary record
            await conn.execute("""
                INSERT INTO intg_api_status_summary
                (vendor, api_endpoint, date, hour, success_count, error_count, rate_limit_count,
                 total_requests, avg_latency_ms, max_latency_ms, total_response_size_bytes)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $8, COALESCE($9, 0))
                ON CONFLICT (vendor, api_endpoint, date, hour)
                DO UPDATE SET
                    success_count = intg_api_status_summary.success_count + $5,
                    error_count = intg_api_status_summary.error_count + $6,
                    rate_limit_count = intg_api_status_summary.rate_limit_count + $7,
                    total_requests = intg_api_status_summary.total_requests + 1,
                    avg_latency_ms = (intg_api_status_summary.avg_latency_ms * intg_api_status_summary.total_requests + $8) / (intg_api_status_summary.total_requests + 1),
                    max_latency_ms = GREATEST(intg_api_status_summary.max_latency_ms, $8),
                    total_response_size_bytes = intg_api_status_summary.total_response_size_bytes + COALESCE($9, 0),
                    last_updated = NOW()
            """,
            record.vendor, record.api_endpoint, date_part, hour_part,
            is_success, is_error, is_rate_limit, record.latency_ms, record.response_size_bytes)

        except Exception as e:
            logger.error(f"❌ Failed to update hourly summary: {e}")

    def get_metrics_summary(self) -> Dict:
        """Get current metrics summary for all vendors."""
        try:
            summary = {
                'vendors': {},
                'totals': {
                    'total_requests': 0,
                    'total_errors': 0,
                    'total_rate_limits': 0,
                    'avg_latency_ms': 0
                },
                'timestamp': datetime.now().isoformat()
            }

            all_latencies = []

            for vendor in self.request_counts:
                vendor_data = {
                    'total_requests': sum(self.request_counts[vendor].values()),
                    'status_codes': dict(self.request_counts[vendor]),
                    'endpoints': {},
                    'rate_limits': self.rate_limit_events.get(vendor, 0),
                    'avg_latency_ms': 0,
                    'max_latency_ms': 0,
                    'recent_errors': []
                }

                # Calculate latency stats
                if vendor in self.latency_stats and self.latency_stats[vendor]:
                    latencies = self.latency_stats[vendor]
                    vendor_data['avg_latency_ms'] = sum(latencies) / len(latencies)
                    vendor_data['max_latency_ms'] = max(latencies)
                    all_latencies.extend(latencies)

                # Add endpoint breakdown
                if vendor in self.api_endpoint_counts:
                    for endpoint, status_counts in self.api_endpoint_counts[vendor].items():
                        vendor_data['endpoints'][endpoint] = {
                            'total_requests': sum(status_counts.values()),
                            'status_codes': dict(status_counts),
                            'success_rate': (status_counts.get(200, 0) / sum(status_counts.values())) * 100 if status_counts else 0
                        }

                # Add recent errors (last 5)
                if vendor in self.error_messages:
                    vendor_data['recent_errors'] = self.error_messages[vendor][-5:]

                # Calculate success rate
                success_requests = sum(count for status, count in self.request_counts[vendor].items() if status < 400)
                vendor_data['success_rate'] = (success_requests / vendor_data['total_requests']) * 100 if vendor_data['total_requests'] else 0

                summary['vendors'][vendor] = vendor_data

                # Update totals
                summary['totals']['total_requests'] += vendor_data['total_requests']
                summary['totals']['total_errors'] += sum(count for status, count in self.request_counts[vendor].items() if status >= 400)
                summary['totals']['total_rate_limits'] += vendor_data['rate_limits']

            # Calculate overall average latency
            if all_latencies:
                summary['totals']['avg_latency_ms'] = sum(all_latencies) / len(all_latencies)

            return summary

        except Exception as e:
            logger.error(f"❌ Failed to get metrics summary: {e}")
            return {}

    def get_prometheus_metrics(self) -> str:
        """Generate Prometheus-compatible metrics string."""
        try:
            # Check cache
            now = time.time()
            if now < self.cache_expiry and self.metrics_cache:
                return self.metrics_cache.get('prometheus', '')

            metrics_lines = []
            timestamp = int(now)

            # Total request metrics by vendor and status code
            for vendor, status_counts in self.request_counts.items():
                for status_code, count in status_counts.items():
                    metrics_lines.append(f'ats_api_requests_total{{vendor="{vendor}",status_code="{status_code}"}} {count} {timestamp}')

            # API endpoint metrics
            for vendor, endpoints in self.api_endpoint_counts.items():
                for endpoint, status_counts in endpoints.items():
                    for status_code, count in status_counts.items():
                        metrics_lines.append(f'ats_api_endpoint_requests{{vendor="{vendor}",endpoint="{endpoint}",status_code="{status_code}"}} {count} {timestamp}')

            # Latency metrics
            for vendor, latencies in self.latency_stats.items():
                if latencies:
                    avg_latency = sum(latencies) / len(latencies)
                    max_latency = max(latencies)
                    metrics_lines.append(f'ats_api_latency_avg_ms{{vendor="{vendor}"}} {avg_latency:.2f} {timestamp}')
                    metrics_lines.append(f'ats_api_latency_max_ms{{vendor="{vendor}"}} {max_latency:.2f} {timestamp}')

            # Rate limiting metrics
            for vendor, count in self.rate_limit_events.items():
                metrics_lines.append(f'ats_api_rate_limits_total{{vendor="{vendor}"}} {count} {timestamp}')

            # Success rate metrics
            for vendor, status_counts in self.request_counts.items():
                total_requests = sum(status_counts.values())
                success_requests = sum(count for status, count in status_counts.items() if status < 400)
                success_rate = (success_requests / total_requests) * 100 if total_requests else 0
                metrics_lines.append(f'ats_api_success_rate_percent{{vendor="{vendor}"}} {success_rate:.2f} {timestamp}')

            # Error count metrics
            for vendor, status_counts in self.request_counts.items():
                error_count = sum(count for status, count in status_counts.items() if status >= 400)
                metrics_lines.append(f'ats_api_errors_total{{vendor="{vendor}"}} {error_count} {timestamp}')

            # Add help and type information
            help_lines = [
                "# HELP ats_api_requests_total Total API requests by vendor and status code",
                "# TYPE ats_api_requests_total counter",
                "# HELP ats_api_endpoint_requests API requests by vendor, endpoint and status code",
                "# TYPE ats_api_endpoint_requests counter",
                "# HELP ats_api_latency_avg_ms Average API request latency by vendor",
                "# TYPE ats_api_latency_avg_ms gauge",
                "# HELP ats_api_latency_max_ms Maximum API request latency by vendor",
                "# TYPE ats_api_latency_max_ms gauge",
                "# HELP ats_api_rate_limits_total Total rate limit events by vendor",
                "# TYPE ats_api_rate_limits_total counter",
                "# HELP ats_api_success_rate_percent Success rate percentage by vendor",
                "# TYPE ats_api_success_rate_percent gauge",
                "# HELP ats_api_errors_total Total API errors by vendor",
                "# TYPE ats_api_errors_total counter"
            ]

            result = "\n".join(help_lines + [""] + metrics_lines) + "\n"

            # Update cache
            self.metrics_cache = {'prometheus': result}
            self.cache_expiry = now + self.cache_duration

            return result

        except Exception as e:
            logger.error(f"❌ Failed to generate Prometheus metrics: {e}")
            return ""


# Singleton instance for global tracking
_global_tracker = None

def get_global_tracker() -> APIStatusTracker:
    """Get or create global API status tracker instance."""
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = APIStatusTracker()
    return _global_tracker

async def initialize_global_tracker():
    """Initialize the global API status tracker."""
    tracker = get_global_tracker()
    await tracker.initialize()
    return tracker