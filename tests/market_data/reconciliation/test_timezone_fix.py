"""
Test cases for timezone handling in cross-vendor reconciliation.

Tests the fix for timezone-naive vs timezone-aware timestamp comparison issues.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import Mock

from domains.market_data.services.reconciliation.cross_vendor_reconciler import (
    CrossVendorReconciler,
    ReconciliationConfig,
    VendorBar
)


class TestTimezoneHandling:
    """Test timezone normalization in reconciler."""

    def setup_method(self):
        """Setup test reconciler."""
        self.reconciler = CrossVendorReconciler(ReconciliationConfig())

    def test_standardize_polygon_timezone_naive(self):
        """Test Polygon data with timezone-naive timestamps."""
        polygon_data = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 8, 16, 13, 30),  # Timezone-naive
                'open': 223.50,
                'high': 224.00,
                'low': 223.00,
                'close': 223.75,
                'volume': 1000
            }
        ]

        bars = self.reconciler._standardize_polygon_data(polygon_data, 'AAPL')

        assert len(bars) == 1
        assert bars[0].vendor == 'polygon'
        assert bars[0].timestamp.tz is not None  # Should be timezone-aware
        assert str(bars[0].timestamp.tz) == 'UTC'

    def test_standardize_polygon_timezone_aware(self):
        """Test Polygon data with timezone-aware timestamps."""
        utc_time = datetime(2024, 8, 16, 13, 30, tzinfo=timezone.utc)
        polygon_data = [
            {
                'symbol': 'AAPL',
                'timestamp': utc_time,
                'open': 223.50,
                'high': 224.00,
                'low': 223.00,
                'close': 223.75,
                'volume': 1000
            }
        ]

        bars = self.reconciler._standardize_polygon_data(polygon_data, 'AAPL')

        assert len(bars) == 1
        assert bars[0].vendor == 'polygon'
        assert bars[0].timestamp.tz is not None  # Should be timezone-aware
        assert str(bars[0].timestamp.tz) == 'UTC'

    def test_standardize_tiingo_timezone_naive(self):
        """Test Tiingo data with timezone-naive timestamps."""
        tiingo_data = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 8, 16, 13, 30),  # Timezone-naive
                'open': 223.50,
                'high': 224.00,
                'low': 223.00,
                'close': 223.75,
                'volume': 0  # Tiingo defaults to 0
            }
        ]

        bars = self.reconciler._standardize_tiingo_data(tiingo_data, 'AAPL')

        assert len(bars) == 1
        assert bars[0].vendor == 'tiingo'
        assert bars[0].timestamp.tz is not None  # Should be timezone-aware
        assert str(bars[0].timestamp.tz) == 'UTC'

    def test_standardize_tiingo_timezone_aware(self):
        """Test Tiingo data with timezone-aware timestamps."""
        utc_time = datetime(2024, 8, 16, 13, 30, tzinfo=timezone.utc)
        tiingo_data = [
            {
                'symbol': 'AAPL',
                'timestamp': utc_time,
                'open': 223.50,
                'high': 224.00,
                'low': 223.00,
                'close': 223.75,
                'volume': 0
            }
        ]

        bars = self.reconciler._standardize_tiingo_data(tiingo_data, 'AAPL')

        assert len(bars) == 1
        assert bars[0].vendor == 'tiingo'
        assert bars[0].timestamp.tz is not None  # Should be timezone-aware
        assert str(bars[0].timestamp.tz) == 'UTC'

    def test_unified_timeline_mixed_timezones(self):
        """Test unified timeline creation with mixed timezone data."""
        # Create mixed timezone bars
        naive_time = datetime(2024, 8, 16, 13, 30)
        aware_time = datetime(2024, 8, 16, 13, 31, tzinfo=timezone.utc)

        polygon_bars = [
            VendorBar(
                symbol='AAPL',
                timestamp=pd.to_datetime(naive_time).tz_localize('UTC'),
                open=223.50, high=224.00, low=223.00, close=223.75,
                volume=1000, vendor='polygon'
            )
        ]

        tiingo_bars = [
            VendorBar(
                symbol='AAPL',
                timestamp=pd.to_datetime(aware_time).tz_convert('UTC'),
                open=223.60, high=224.10, low=223.10, close=223.85,
                volume=0, vendor='tiingo'
            )
        ]

        timeline = self.reconciler._create_unified_timeline(polygon_bars, tiingo_bars)

        assert len(timeline) == 2
        # All timestamps should be timezone-aware and comparable
        for ts in timeline:
            assert ts.tz is not None

    def test_find_bar_by_timestamp_timezone_compatibility(self):
        """Test that bar lookup works with normalized timestamps."""
        base_time = datetime(2024, 8, 16, 13, 30)

        # Create bars with normalized timestamps
        bars = [
            VendorBar(
                symbol='AAPL',
                timestamp=pd.to_datetime(base_time).tz_localize('UTC'),
                open=223.50, high=224.00, low=223.00, close=223.75,
                volume=1000, vendor='polygon'
            ),
            VendorBar(
                symbol='AAPL',
                timestamp=pd.to_datetime(base_time + pd.Timedelta(minutes=1)).tz_localize('UTC'),
                open=223.60, high=224.10, low=223.10, close=223.85,
                volume=1100, vendor='polygon'
            )
        ]

        # Test finding by exact timestamp match
        search_time = pd.to_datetime(base_time).tz_localize('UTC')
        found_bar = self.reconciler._find_bar_by_timestamp(bars, search_time)

        assert found_bar is not None
        assert found_bar.timestamp == search_time
        assert found_bar.volume == 1000

    def test_timestamp_comparison_no_error(self):
        """Test that timezone-normalized timestamps can be compared without error."""
        naive_time = datetime(2024, 8, 16, 13, 30)
        aware_time = datetime(2024, 8, 16, 13, 30, tzinfo=timezone.utc)

        # Normalize both to UTC timezone-aware
        norm_naive = pd.to_datetime(naive_time).tz_localize('UTC')
        norm_aware = pd.to_datetime(aware_time).tz_convert('UTC')

        # These should be equal and comparable without error
        assert norm_naive == norm_aware
        assert norm_naive <= norm_aware
        assert norm_naive >= norm_aware

    def test_standardization_preserves_data_integrity(self):
        """Test that timezone normalization preserves all other data."""
        polygon_data = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 8, 16, 13, 30),
                'open': 223.50,
                'high': 224.00,
                'low': 223.00,
                'close': 223.75,
                'volume': 1000,
                'quality_score': 0.95,
                'vwap': 223.60,
                'trade_count': 150
            }
        ]

        bars = self.reconciler._standardize_polygon_data(polygon_data, 'AAPL')

        assert len(bars) == 1
        bar = bars[0]

        # Check all data is preserved
        assert bar.symbol == 'AAPL'
        assert bar.open == 223.50
        assert bar.high == 224.00
        assert bar.low == 223.00
        assert bar.close == 223.75
        assert bar.volume == 1000
        assert bar.vendor == 'polygon'
        assert bar.quality_score == 0.95
        assert bar.metadata['vwap'] == 223.60
        assert bar.metadata['trade_count'] == 150

        # And timestamp is normalized
        assert bar.timestamp.tz is not None
        assert str(bar.timestamp.tz) == 'UTC'


@pytest.mark.asyncio
class TestTimezoneReconciliationIntegration:
    """Integration tests for timezone handling in reconciliation."""

    @pytest.mark.asyncio

    async def test_reconcile_with_mixed_timezones(self):
        """Test full reconciliation with mixed timezone data."""
        reconciler = CrossVendorReconciler(ReconciliationConfig())

        # Mock data with different timezone formats
        polygon_data = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 8, 16, 13, 30),  # Naive
                'open': 223.50,
                'high': 224.00,
                'low': 223.00,
                'close': 223.75,
                'volume': 1000
            }
        ]

        tiingo_data = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 8, 16, 13, 30, tzinfo=timezone.utc),  # Aware
                'open': 223.55,
                'high': 224.05,
                'low': 223.05,
                'close': 223.80,
                'volume': 0
            }
        ]

        # This should not raise timezone comparison errors
        reconciled_bars = await reconciler.reconcile_minute_data(
            polygon_data, tiingo_data, 'AAPL'
        )

        assert len(reconciled_bars) >= 1
        # Should have reconciled the overlapping timestamp
        assert any(bar.vendor_count == 2 for bar in reconciled_bars)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])