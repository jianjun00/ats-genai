#!/usr/bin/env python3
"""
Backfill Framework Utilities

Provides reusable components for data backfill operations including statistics
tracking, progress reporting, and rate limiting.

USAGE:
======

from shared.utils.backfill_framework import BackfillStats, RateLimiter

# Statistics tracking
stats = BackfillStats()
stats.records_fetched += 100
stats.records_inserted += 95
stats.log_progress(logger)

# Rate limiting
rate_limiter = RateLimiter(calls_per_minute=5)  # Polygon free tier
await rate_limiter.wait_if_needed()
"""

import asyncio
import time
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

@dataclass
class BackfillStats:
    """
    Statistics tracking for backfill operations.

    Provides comprehensive metrics and progress reporting for any type of
    data backfill operation (news, prices, fundamentals, etc.).
    """

    # Core metrics
    records_fetched: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    records_failed: int = 0

    # API metrics
    api_calls_made: int = 0
    api_errors: int = 0
    rate_limit_hits: int = 0

    # Timing metrics
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    # Custom metrics (vendor-specific or operation-specific)
    custom_metrics: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.start_time is None:
            self.start_time = datetime.now()

    @property
    def total_processed(self) -> int:
        """Total records processed (inserted + updated + skipped + failed)"""
        return self.records_inserted + self.records_updated + self.records_skipped + self.records_failed

    @property
    def success_rate(self) -> float:
        """Success rate as percentage (inserted + updated) / total_processed"""
        if self.total_processed == 0:
            return 0.0
        return ((self.records_inserted + self.records_updated) / self.total_processed) * 100

    @property
    def duration(self) -> Optional[timedelta]:
        """Duration of the operation"""
        if self.start_time is None:
            return None
        end = self.end_time or datetime.now()
        return end - self.start_time

    @property
    def records_per_minute(self) -> float:
        """Processing rate in records per minute"""
        duration = self.duration
        if not duration or duration.total_seconds() == 0:
            return 0.0
        return (self.total_processed / duration.total_seconds()) * 60

    def mark_complete(self):
        """Mark the operation as complete"""
        self.end_time = datetime.now()

    def log_progress(self, logger: logging.Logger, level: int = logging.INFO):
        """
        Log current progress with comprehensive metrics.

        Args:
            logger: Logger instance to use
            level: Log level (default: INFO)
        """
        duration_str = ""
        if self.duration:
            duration_str = f" in {self.duration}"

        rate_str = ""
        if self.records_per_minute > 0:
            rate_str = f" ({self.records_per_minute:.1f} records/min)"

        logger.log(level,
            f"Progress: {self.records_fetched} fetched, "
            f"{self.records_inserted} inserted, {self.records_updated} updated, "
            f"{self.records_skipped} skipped, {self.records_failed} failed, "
            f"{self.api_calls_made} API calls{duration_str}{rate_str}"
        )

    def log_final_summary(self, logger: logging.Logger):
        """Log final comprehensive summary"""
        self.mark_complete()

        logger.info("=== BACKFILL OPERATION SUMMARY ===")
        logger.info(f"Records fetched: {self.records_fetched:,}")
        logger.info(f"Records inserted: {self.records_inserted:,}")
        logger.info(f"Records updated: {self.records_updated:,}")
        logger.info(f"Records skipped: {self.records_skipped:,}")
        logger.info(f"Records failed: {self.records_failed:,}")
        logger.info(f"Success rate: {self.success_rate:.1f}%")
        logger.info(f"API calls made: {self.api_calls_made:,}")
        logger.info(f"API errors: {self.api_errors:,}")
        logger.info(f"Rate limit hits: {self.rate_limit_hits:,}")

        if self.duration:
            logger.info(f"Duration: {self.duration}")
            logger.info(f"Processing rate: {self.records_per_minute:.1f} records/minute")

        # Log custom metrics
        for key, value in self.custom_metrics.items():
            logger.info(f"{key}: {value}")

    def add_custom_metric(self, key: str, value: Any):
        """Add a custom metric"""
        self.custom_metrics[key] = value

    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary for serialization"""
        return {
            'records_fetched': self.records_fetched,
            'records_inserted': self.records_inserted,
            'records_updated': self.records_updated,
            'records_skipped': self.records_skipped,
            'records_failed': self.records_failed,
            'api_calls_made': self.api_calls_made,
            'api_errors': self.api_errors,
            'rate_limit_hits': self.rate_limit_hits,
            'success_rate': self.success_rate,
            'duration_seconds': self.duration.total_seconds() if self.duration else None,
            'records_per_minute': self.records_per_minute,
            'custom_metrics': self.custom_metrics
        }

class RateLimiter:
    """
    Rate limiter for API calls with support for different vendor limits.

    Examples:
        >>> # Polygon free tier (5 calls/minute)
        >>> rate_limiter = RateLimiter(calls_per_minute=5)
        >>> await rate_limiter.wait_if_needed()

        >>> # Custom rate
        >>> rate_limiter = RateLimiter(calls_per_second=2)
        >>> await rate_limiter.wait_if_needed()
    """

    def __init__(self,
                 calls_per_minute: Optional[int] = None,
                 calls_per_second: Optional[float] = None,
                 burst_allowance: int = 1):
        """
        Initialize rate limiter.

        Args:
            calls_per_minute: Maximum calls per minute (takes precedence)
            calls_per_second: Maximum calls per second
            burst_allowance: Allow burst of N calls before limiting
        """
        if calls_per_minute:
            self.delay_seconds = 60.0 / calls_per_minute
        elif calls_per_second:
            self.delay_seconds = 1.0 / calls_per_second
        else:
            raise ValueError("Must specify either calls_per_minute or calls_per_second")

        self.burst_allowance = burst_allowance
        self.last_call_times = []

    async def wait_if_needed(self):
        """Wait if rate limit would be exceeded"""
        now = time.time()

        # Clean old call times (keep only those within burst window)
        cutoff = now - (self.delay_seconds * self.burst_allowance)
        self.last_call_times = [t for t in self.last_call_times if t > cutoff]

        # Check if we need to wait
        if len(self.last_call_times) >= self.burst_allowance:
            oldest_call = min(self.last_call_times)
            wait_time = oldest_call + self.delay_seconds - now
            if wait_time > 0:
                await asyncio.sleep(wait_time)

        # Record this call
        self.last_call_times.append(time.time())

class ProgressReporter:
    """
    Progress reporting with configurable intervals.

    Example:
        >>> reporter = ProgressReporter(report_every=100)
        >>> for i, item in enumerate(items):
        ...     # Process item
        ...     if reporter.should_report(i):
        ...         stats.log_progress(logger)
    """

    def __init__(self, report_every: int = 100, time_interval: Optional[int] = None):
        """
        Initialize progress reporter.

        Args:
            report_every: Report every N records
            time_interval: Also report every N seconds (optional)
        """
        self.report_every = report_every
        self.time_interval = time_interval
        self.last_report_count = 0
        self.last_report_time = time.time()

    def should_report(self, current_count: int) -> bool:
        """Check if progress should be reported now"""
        now = time.time()

        # Count-based reporting
        if current_count - self.last_report_count >= self.report_every:
            self.last_report_count = current_count
            self.last_report_time = now
            return True

        # Time-based reporting
        if self.time_interval and (now - self.last_report_time) >= self.time_interval:
            self.last_report_time = now
            return True

        return False

# Vendor-specific rate limiters
class VendorRateLimiters:
    """Pre-configured rate limiters for common vendors"""

    @staticmethod
    def polygon_free() -> RateLimiter:
        """Polygon.io free tier: 5 calls/minute"""
        return RateLimiter(calls_per_minute=5)

    @staticmethod
    def polygon_paid() -> RateLimiter:
        """Polygon.io paid tier: More generous"""
        return RateLimiter(calls_per_second=100)  # Adjust based on plan

    @staticmethod
    def alpha_vantage_free() -> RateLimiter:
        """Alpha Vantage free: 5 calls/minute"""
        return RateLimiter(calls_per_minute=5)

    @staticmethod
    def tiingo_free() -> RateLimiter:
        """Tiingo free tier: More generous"""
        return RateLimiter(calls_per_second=1)

    @staticmethod
    def eodhd() -> RateLimiter:
        """EOD Historical Data: Generous limits"""
        return RateLimiter(calls_per_second=10)

    @staticmethod
    def tiingo() -> RateLimiter:
        """Tiingo API rate limiter (alias for tiingo_free)"""
        return VendorRateLimiters.tiingo_free()

    @staticmethod
    def alpha_vantage() -> RateLimiter:
        """Alpha Vantage API rate limiter (alias for alpha_vantage_free)"""
        return VendorRateLimiters.alpha_vantage_free()