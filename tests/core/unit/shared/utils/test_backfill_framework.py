#!/usr/bin/env python3
"""
Comprehensive unit tests for shared.utils.backfill_framework module.

Tests the backfill framework utilities including statistics tracking,
rate limiting, and progress reporting for data backfill operations.
"""

import pytest
import time
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent / 'src'))

from core.shared.utils_core.backfill_framework import (
    BackfillStats,
    RateLimiter,
    ProgressReporter,
    VendorRateLimiters
)


class TestBackfillStats:
    """Test the BackfillStats class"""

    def test_backfill_stats_initialization(self):
        """Test BackfillStats initialization with default values"""
        stats = BackfillStats()

        # Core metrics should start at zero
        assert stats.records_fetched == 0
        assert stats.records_inserted == 0
        assert stats.records_updated == 0
        assert stats.records_skipped == 0
        assert stats.records_failed == 0

        # API metrics should start at zero
        assert stats.api_calls_made == 0
        assert stats.api_errors == 0
        assert stats.rate_limit_hits == 0

        # Timing should be initialized
        assert stats.start_time is not None
        assert stats.end_time is None

        # Custom metrics should be empty dict
        assert stats.custom_metrics == {}

    def test_backfill_stats_custom_start_time(self):
        """Test BackfillStats with custom start time"""
        custom_time = datetime(2025, 1, 1, 12, 0, 0)
        stats = BackfillStats(start_time=custom_time)

        assert stats.start_time == custom_time

    def test_total_processed_property(self):
        """Test the total_processed property calculation"""
        stats = BackfillStats()
        stats.records_inserted = 100
        stats.records_updated = 50
        stats.records_skipped = 25
        stats.records_failed = 10

        assert stats.total_processed == 185

    def test_success_rate_property(self):
        """Test the success_rate property calculation"""
        stats = BackfillStats()
        stats.records_inserted = 80
        stats.records_updated = 20
        stats.records_skipped = 15
        stats.records_failed = 5

        # Success rate = (inserted + updated) / total * 100
        # (80 + 20) / (80 + 20 + 15 + 5) * 100 = 83.33%
        assert stats.success_rate == pytest.approx(83.33, rel=1e-2)

    def test_success_rate_zero_processed(self):
        """Test success_rate when no records processed"""
        stats = BackfillStats()
        assert stats.success_rate == 0.0

    def test_duration_property(self):
        """Test the duration property calculation"""
        start_time = datetime(2025, 1, 1, 12, 0, 0)
        stats = BackfillStats(start_time=start_time)

        # Before marking complete, duration should be calculated from now
        with patch('shared.utils.backfill_framework.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 5, 0)
            duration = stats.duration
            assert duration == timedelta(minutes=5)

    def test_duration_property_with_end_time(self):
        """Test duration property when end_time is set"""
        start_time = datetime(2025, 1, 1, 12, 0, 0)
        end_time = datetime(2025, 1, 1, 12, 10, 0)

        stats = BackfillStats(start_time=start_time, end_time=end_time)

        assert stats.duration == timedelta(minutes=10)

    def test_duration_property_no_start_time(self):
        """Test duration property when start_time is None"""
        stats = BackfillStats()
        stats.start_time = None

        assert stats.duration is None

    def test_records_per_minute_property(self):
        """Test the records_per_minute property calculation"""
        start_time = datetime(2025, 1, 1, 12, 0, 0)
        stats = BackfillStats(start_time=start_time)
        stats.records_inserted = 120
        stats.records_updated = 60

        # Mock duration of 3 minutes
        with patch.object(stats, 'duration', timedelta(minutes=3)):
            # 180 records in 3 minutes = 60 records/minute
            assert stats.records_per_minute == 60.0

    def test_records_per_minute_zero_duration(self):
        """Test records_per_minute when duration is zero"""
        stats = BackfillStats()
        stats.records_inserted = 100

        with patch.object(stats, 'duration', timedelta(seconds=0)):
            assert stats.records_per_minute == 0.0

    def test_records_per_minute_no_duration(self):
        """Test records_per_minute when duration is None"""
        stats = BackfillStats()
        stats.records_inserted = 100

        with patch.object(stats, 'duration', None):
            assert stats.records_per_minute == 0.0

    def test_mark_complete(self):
        """Test the mark_complete method"""
        stats = BackfillStats()

        with patch('shared.utils.backfill_framework.datetime') as mock_datetime:
            mock_end_time = datetime(2025, 1, 1, 12, 10, 0)
            mock_datetime.now.return_value = mock_end_time

            stats.mark_complete()
            assert stats.end_time == mock_end_time

    def test_log_progress_basic(self):
        """Test basic log_progress functionality"""
        stats = BackfillStats()
        stats.records_fetched = 100
        stats.records_inserted = 90
        stats.records_updated = 5
        stats.records_skipped = 3
        stats.records_failed = 2
        stats.api_calls_made = 10

        mock_logger = Mock()
        stats.log_progress(mock_logger)

        mock_logger.log.assert_called_once()
        log_message = mock_logger.log.call_args[0][1]

        assert "100 fetched" in log_message
        assert "90 inserted" in log_message
        assert "5 updated" in log_message
        assert "3 skipped" in log_message
        assert "2 failed" in log_message
        assert "10 API calls" in log_message

    def test_log_progress_with_duration(self):
        """Test log_progress with duration information"""
        stats = BackfillStats()
        stats.records_fetched = 100

        with patch.object(stats, 'duration', timedelta(minutes=2)):
            with patch.object(stats, 'records_per_minute', 50.0):
                mock_logger = Mock()
                stats.log_progress(mock_logger)

                log_message = mock_logger.log.call_args[0][1]
                assert "in 0:02:00" in log_message
                assert "(50.0 records/min)" in log_message

    def test_log_progress_custom_level(self):
        """Test log_progress with custom log level"""
        stats = BackfillStats()
        mock_logger = Mock()

        stats.log_progress(mock_logger, level=30)  # WARNING level

        mock_logger.log.assert_called_once_with(30, mock_logger.log.call_args[0][1])

    def test_log_final_summary(self):
        """Test log_final_summary functionality"""
        stats = BackfillStats()
        stats.records_fetched = 100
        stats.records_inserted = 90
        stats.api_calls_made = 10
        stats.api_errors = 1
        stats.rate_limit_hits = 2
        stats.add_custom_metric("symbols_processed", ["AAPL", "MSFT"])

        mock_logger = Mock()

        with patch.object(stats, 'mark_complete') as mock_mark_complete:
            with patch.object(stats, 'success_rate', 90.0):
                with patch.object(stats, 'duration', timedelta(minutes=5)):
                    with patch.object(stats, 'records_per_minute', 18.0):
                        stats.log_final_summary(mock_logger)

        mock_mark_complete.assert_called_once()

        # Check that multiple info messages were logged
        assert mock_logger.info.call_count >= 8  # At least 8 different metrics

        # Verify specific metrics were logged
        log_messages = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Records fetched: 100" in msg for msg in log_messages)
        assert any("Records inserted: 90" in msg for msg in log_messages)
        assert any("Success rate: 90.0%" in msg for msg in log_messages)
        assert any("symbols_processed:" in msg for msg in log_messages)

    def test_add_custom_metric(self):
        """Test adding custom metrics"""
        stats = BackfillStats()

        stats.add_custom_metric("test_metric", "test_value")
        assert stats.custom_metrics["test_metric"] == "test_value"

        stats.add_custom_metric("numeric_metric", 12345)
        assert stats.custom_metrics["numeric_metric"] == 12345

    def test_to_dict(self):
        """Test converting stats to dictionary"""
        stats = BackfillStats()
        stats.records_fetched = 100
        stats.records_inserted = 90
        stats.api_calls_made = 10
        stats.add_custom_metric("test", "value")

        with patch.object(stats, 'success_rate', 90.0):
            with patch.object(stats, 'duration', timedelta(minutes=5)):
                with patch.object(stats, 'records_per_minute', 18.0):
                    result = stats.to_dict()

        assert result['records_fetched'] == 100
        assert result['records_inserted'] == 90
        assert result['api_calls_made'] == 10
        assert result['success_rate'] == 90.0
        assert result['duration_seconds'] == 300.0  # 5 minutes
        assert result['records_per_minute'] == 18.0
        assert result['custom_metrics'] == {"test": "value"}

    def test_to_dict_no_duration(self):
        """Test to_dict when duration is None"""
        stats = BackfillStats()

        with patch.object(stats, 'duration', None):
            result = stats.to_dict()

        assert result['duration_seconds'] is None


class TestRateLimiter:
    """Test the RateLimiter class"""

    def test_rate_limiter_calls_per_minute_init(self):
        """Test RateLimiter initialization with calls_per_minute"""
        limiter = RateLimiter(calls_per_minute=60)

        # 60 calls/minute = 1 second delay
        assert limiter.delay_seconds == 1.0
        assert limiter.burst_allowance == 1

    def test_rate_limiter_calls_per_second_init(self):
        """Test RateLimiter initialization with calls_per_second"""
        limiter = RateLimiter(calls_per_second=2.0)

        # 2 calls/second = 0.5 second delay
        assert limiter.delay_seconds == 0.5
        assert limiter.burst_allowance == 1

    def test_rate_limiter_custom_burst_allowance(self):
        """Test RateLimiter with custom burst allowance"""
        limiter = RateLimiter(calls_per_minute=60, burst_allowance=5)

        assert limiter.delay_seconds == 1.0
        assert limiter.burst_allowance == 5

    def test_rate_limiter_no_parameters_error(self):
        """Test RateLimiter raises error when no rate parameters provided"""
        with pytest.raises(ValueError, match="Must specify either"):
            RateLimiter()

    def test_rate_limiter_calls_per_minute_precedence(self):
        """Test that calls_per_minute takes precedence over calls_per_second"""
        limiter = RateLimiter(calls_per_minute=60, calls_per_second=10)

        # Should use calls_per_minute (60 calls/min = 1 second delay)
        assert limiter.delay_seconds == 1.0

    @pytest.mark.asyncio
    async def test_rate_limiter_first_call_no_wait(self):
        """Test that first call doesn't require waiting"""
        limiter = RateLimiter(calls_per_second=1)

        start_time = time.time()
        await limiter.wait_if_needed()
        elapsed = time.time() - start_time

        # First call should be immediate (allow for small test variance)
        assert elapsed < 0.1

    @pytest.mark.asyncio
    async def test_rate_limiter_burst_allowance_no_wait(self):
        """Test that calls within burst allowance don't require waiting"""
        limiter = RateLimiter(calls_per_second=1, burst_allowance=3)

        # First 3 calls should be immediate
        for i in range(3):
            start_time = time.time()
            await limiter.wait_if_needed()
            elapsed = time.time() - start_time
            assert elapsed < 0.1

    @pytest.mark.asyncio
    async def test_rate_limiter_enforces_delay(self):
        """Test that rate limiter enforces delays after burst"""
        limiter = RateLimiter(calls_per_second=10, burst_allowance=1)  # 100ms delay

        # First call should be immediate
        await limiter.wait_if_needed()

        # Second call should be delayed
        start_time = time.time()
        await limiter.wait_if_needed()
        elapsed = time.time() - start_time

        # Should wait approximately 100ms (allow for test variance)
        assert 0.05 < elapsed < 0.15

    @pytest.mark.asyncio
    async def test_rate_limiter_call_time_cleanup(self):
        """Test that old call times are cleaned up"""
        limiter = RateLimiter(calls_per_second=1, burst_allowance=2)

        # Add old call times manually
        old_time = time.time() - 10  # 10 seconds ago
        limiter.last_call_times = [old_time, old_time]

        # New call should clean up old times and not require waiting
        start_time = time.time()
        await limiter.wait_if_needed()
        elapsed = time.time() - start_time

        assert elapsed < 0.1
        assert len(limiter.last_call_times) == 1  # Only the new call time


class TestProgressReporter:
    """Test the ProgressReporter class"""

    def test_progress_reporter_initialization(self):
        """Test ProgressReporter initialization"""
        reporter = ProgressReporter(report_every=50, time_interval=30)

        assert reporter.report_every == 50
        assert reporter.time_interval == 30
        assert reporter.last_report_count == 0
        assert reporter.last_report_time <= time.time()

    def test_progress_reporter_default_initialization(self):
        """Test ProgressReporter with default parameters"""
        reporter = ProgressReporter()

        assert reporter.report_every == 100
        assert reporter.time_interval is None

    def test_should_report_count_based(self):
        """Test count-based progress reporting"""
        reporter = ProgressReporter(report_every=10)

        # Should not report for counts below threshold
        assert not reporter.should_report(5)
        assert not reporter.should_report(9)

        # Should report when threshold is reached
        assert reporter.should_report(10)

        # Should not report again until next threshold
        assert not reporter.should_report(15)
        assert not reporter.should_report(19)

        # Should report at next threshold
        assert reporter.should_report(20)

    def test_should_report_time_based(self):
        """Test time-based progress reporting"""
        reporter = ProgressReporter(report_every=1000, time_interval=1)  # 1 second

        # First call should not trigger time-based reporting
        assert not reporter.should_report(5)

        # Wait and test time-based reporting
        time.sleep(1.1)  # Wait just over 1 second
        assert reporter.should_report(6)  # Should trigger time-based reporting

        # Should not report again immediately
        assert not reporter.should_report(7)

    def test_should_report_count_overrides_time(self):
        """Test that count-based reporting takes precedence"""
        reporter = ProgressReporter(report_every=5, time_interval=10)

        # Count-based should trigger first
        assert reporter.should_report(5)

        # Even if time hasn't passed, count-based should work
        assert reporter.should_report(10)

    def test_should_report_updates_tracking(self):
        """Test that should_report updates tracking variables"""
        reporter = ProgressReporter(report_every=10)

        initial_time = reporter.last_report_time
        time.sleep(0.01)  # Small delay

        result = reporter.should_report(10)
        assert result is True
        assert reporter.last_report_count == 10
        assert reporter.last_report_time > initial_time


class TestVendorRateLimiters:
    """Test the VendorRateLimiters class"""

    def test_polygon_free_rate_limiter(self):
        """Test Polygon free tier rate limiter"""
        limiter = VendorRateLimiters.polygon_free()

        # Polygon free: 5 calls/minute = 12 seconds delay
        assert limiter.delay_seconds == 12.0
        assert isinstance(limiter, RateLimiter)

    def test_polygon_paid_rate_limiter(self):
        """Test Polygon paid tier rate limiter"""
        limiter = VendorRateLimiters.polygon_paid()

        # Polygon paid: 100 calls/second = 0.01 second delay
        assert limiter.delay_seconds == 0.01
        assert isinstance(limiter, RateLimiter)

    def test_alpha_vantage_free_rate_limiter(self):
        """Test Alpha Vantage free rate limiter"""
        limiter = VendorRateLimiters.alpha_vantage_free()

        # Alpha Vantage free: 5 calls/minute = 12 seconds delay
        assert limiter.delay_seconds == 12.0
        assert isinstance(limiter, RateLimiter)

    def test_tiingo_free_rate_limiter(self):
        """Test Tiingo free rate limiter"""
        limiter = VendorRateLimiters.tiingo_free()

        # Tiingo free: 1 call/second = 1 second delay
        assert limiter.delay_seconds == 1.0
        assert isinstance(limiter, RateLimiter)

    def test_eodhd_rate_limiter(self):
        """Test EODHD rate limiter"""
        limiter = VendorRateLimiters.eodhd()

        # EODHD: 10 calls/second = 0.1 second delay
        assert limiter.delay_seconds == 0.1
        assert isinstance(limiter, RateLimiter)

    @pytest.mark.asyncio
    async def test_vendor_rate_limiters_functional(self):
        """Test that vendor rate limiters are functional"""
        limiters = [
            VendorRateLimiters.polygon_free(),
            VendorRateLimiters.alpha_vantage_free(),
            VendorRateLimiters.tiingo_free(),
            VendorRateLimiters.eodhd()
        ]

        for limiter in limiters:
            # Should be able to make calls without error
            await limiter.wait_if_needed()
            assert len(limiter.last_call_times) > 0


class TestIntegration:
    """Integration tests combining multiple components"""

    @pytest.mark.asyncio
    async def test_backfill_stats_with_rate_limiter(self):
        """Test BackfillStats working with RateLimiter"""
        stats = BackfillStats()
        limiter = RateLimiter(calls_per_second=10)  # Fast for testing

        # Simulate a backfill operation
        for i in range(5):
            await limiter.wait_if_needed()
            stats.api_calls_made += 1
            stats.records_fetched += 10
            stats.records_inserted += 9
            stats.records_failed += 1

        assert stats.api_calls_made == 5
        assert stats.records_fetched == 50
        assert stats.records_inserted == 45
        assert stats.records_failed == 5
        assert stats.success_rate == 90.0  # 45/(45+0+0+5) * 100

    def test_progress_reporter_with_backfill_stats(self):
        """Test ProgressReporter working with BackfillStats"""
        stats = BackfillStats()
        reporter = ProgressReporter(report_every=25)
        mock_logger = Mock()

        # Simulate processing records
        for i in range(100):
            stats.records_fetched += 1
            stats.records_inserted += 1

            if reporter.should_report(i):
                stats.log_progress(mock_logger)

        # Should have reported 4 times (at 25, 50, 75, 100)
        assert mock_logger.log.call_count >= 3
        assert stats.records_fetched == 100

    def test_custom_backfill_scenario(self):
        """Test a realistic custom backfill scenario"""
        stats = BackfillStats()

        # Add various custom metrics
        stats.add_custom_metric("vendor", "polygon")
        stats.add_custom_metric("symbols_processed", ["AAPL", "MSFT", "GOOGL"])
        stats.add_custom_metric("date_range", "2025-01-01 to 2025-01-31")
        stats.add_custom_metric("data_quality_score", 0.95)

        # Simulate processing
        stats.records_fetched = 1500
        stats.records_inserted = 1450
        stats.records_skipped = 30
        stats.records_failed = 20
        stats.api_calls_made = 15
        stats.api_errors = 1

        # Test comprehensive reporting
        mock_logger = Mock()
        stats.log_final_summary(mock_logger)

        # Verify all metrics are reported
        log_messages = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("1,500" in msg for msg in log_messages)  # Formatted numbers
        assert any("96.7%" in msg for msg in log_messages)  # Success rate
        assert any("polygon" in msg for msg in log_messages)  # Custom metrics
        assert any("0.95" in msg for msg in log_messages)  # Quality score


class TestEdgeCases:
    """Test edge cases and error conditions"""

    def test_backfill_stats_negative_values(self):
        """Test BackfillStats with negative values"""
        stats = BackfillStats()
        stats.records_inserted = -10  # Negative value
        stats.records_failed = 5

        # total_processed should handle negative values
        assert stats.total_processed == -5

        # success_rate should handle negative total
        assert stats.success_rate == 0.0  # Should not crash

    @pytest.mark.asyncio
    async def test_rate_limiter_very_fast_rate(self):
        """Test RateLimiter with very fast rate"""
        limiter = RateLimiter(calls_per_second=1000)  # Very fast

        # Should work without issues
        for _ in range(10):
            await limiter.wait_if_needed()

        assert len(limiter.last_call_times) <= limiter.burst_allowance + 9

    @pytest.mark.asyncio
    async def test_rate_limiter_very_slow_rate(self):
        """Test RateLimiter with very slow rate"""
        limiter = RateLimiter(calls_per_minute=1)  # Very slow: 60 seconds

        # First call should be immediate
        start_time = time.time()
        await limiter.wait_if_needed()
        elapsed = time.time() - start_time

        assert elapsed < 0.1

    def test_progress_reporter_zero_report_every(self):
        """Test ProgressReporter with zero report_every"""
        reporter = ProgressReporter(report_every=0)

        # Should not crash but might behave unexpectedly
        result = reporter.should_report(5)
        assert isinstance(result, bool)

    def test_backfill_stats_very_long_duration(self):
        """Test BackfillStats with very long duration"""
        start_time = datetime(2025, 1, 1)
        end_time = datetime(2025, 12, 31)

        stats = BackfillStats(start_time=start_time, end_time=end_time)
        stats.records_inserted = 1000000

        # Should handle large durations without issues
        duration = stats.duration
        assert duration.days > 300

        rate = stats.records_per_minute
        assert rate > 0


if __name__ == '__main__':
    pytest.main([__file__])