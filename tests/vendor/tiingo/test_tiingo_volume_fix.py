"""
Test cases for Tiingo adapter volume field handling.

Tests the fix for missing 'volume' field in Tiingo API responses.
"""

import pytest
from datetime import datetime

from domains.market_data.services.agent.tiingo_intraday_adapter import TiingoIntradayAdapter, TiingoMinuteBar


class TestTiingoVolumeParsing:
    """Test volume field handling in Tiingo adapter."""

    def setup_method(self):
        """Setup test adapter without API key validation."""
        self.adapter = TiingoIntradayAdapter.__new__(TiingoIntradayAdapter)
        # Skip __init__ to avoid API key requirement

    def test_parse_intraday_data_with_volume(self):
        """Test parsing when volume field is present."""
        mock_data = [
            {
                'date': '2024-08-16T13:30:00.000Z',
                'close': 223.705,
                'high': 224.515,
                'low': 223.705,
                'open': 223.92,
                'volume': 1000
            }
        ]

        bars = self.adapter._parse_intraday_data('AAPL', mock_data)

        assert len(bars) == 1
        assert bars[0].symbol == 'AAPL'
        assert bars[0].volume == 1000
        assert bars[0].open == 223.92
        assert bars[0].close == 223.705

    def test_parse_intraday_data_without_volume(self):
        """Test parsing when volume field is missing (Tiingo reality)."""
        mock_data = [
            {
                'date': '2024-08-16T13:30:00.000Z',
                'close': 223.705,
                'high': 224.515,
                'low': 223.705,
                'open': 223.92
                # No volume field
            }
        ]

        bars = self.adapter._parse_intraday_data('AAPL', mock_data)

        assert len(bars) == 1
        assert bars[0].symbol == 'AAPL'
        assert bars[0].volume == 0  # Default value when missing
        assert bars[0].open == 223.92
        assert bars[0].close == 223.705

    def test_parse_daily_resampled_with_volume(self):
        """Test daily resampled parsing with volume field."""
        mock_data = [
            {
                'date': '2024-08-16T09:30:00.000Z',
                'close': 223.00,
                'high': 224.00,
                'low': 222.50,
                'open': 223.50,
                'volume': 2000
            }
        ]

        bars = self.adapter._parse_daily_resampled('AAPL', mock_data)

        assert len(bars) == 1
        assert bars[0].symbol == 'AAPL'
        assert bars[0].volume == 2000
        assert bars[0].open == 223.50
        assert bars[0].close == 223.00

    def test_parse_daily_resampled_without_volume(self):
        """Test daily resampled parsing without volume field."""
        mock_data = [
            {
                'date': '2024-08-16T09:30:00.000Z',
                'close': 223.00,
                'high': 224.00,
                'low': 222.50,
                'open': 223.50
                # No volume field
            }
        ]

        bars = self.adapter._parse_daily_resampled('AAPL', mock_data)

        assert len(bars) == 1
        assert bars[0].symbol == 'AAPL'
        assert bars[0].volume == 0  # Default value when missing
        assert bars[0].open == 223.50
        assert bars[0].close == 223.00

    def test_parse_multiple_bars_mixed_volume(self):
        """Test parsing multiple bars with mixed volume presence."""
        mock_data = [
            {
                'date': '2024-08-16T13:30:00.000Z',
                'close': 223.705,
                'high': 224.515,
                'low': 223.705,
                'open': 223.92,
                'volume': 1500  # Has volume
            },
            {
                'date': '2024-08-16T13:31:00.000Z',
                'close': 223.80,
                'high': 224.20,
                'low': 223.60,
                'open': 223.70
                # No volume field
            },
            {
                'date': '2024-08-16T13:32:00.000Z',
                'close': 223.90,
                'high': 224.30,
                'low': 223.70,
                'open': 223.80,
                'volume': 2500  # Has volume
            }
        ]

        bars = self.adapter._parse_intraday_data('AAPL', mock_data)

        assert len(bars) == 3
        assert bars[0].volume == 1500  # First bar has volume
        assert bars[1].volume == 0     # Second bar defaults to 0
        assert bars[2].volume == 2500  # Third bar has volume

    def test_parse_empty_data(self):
        """Test parsing empty data array."""
        mock_data = []

        bars = self.adapter._parse_intraday_data('AAPL', mock_data)

        assert len(bars) == 0

    def test_parse_malformed_data_handling(self):
        """Test handling of malformed data entries."""
        mock_data = [
            {
                'date': '2024-08-16T13:30:00.000Z',
                'close': 223.705,
                'high': 224.515,
                'low': 223.705,
                'open': 223.92
                # Valid entry, no volume
            },
            {
                'date': 'invalid-date',
                'close': 'invalid',
                'high': 224.20,
                'low': 223.60,
                'open': 223.70
                # Invalid entry
            },
            {
                'date': '2024-08-16T13:32:00.000Z',
                'close': 223.90,
                'high': 224.30,
                'low': 223.70,
                'open': 223.80
                # Valid entry, no volume
            }
        ]

        bars = self.adapter._parse_intraday_data('AAPL', mock_data)

        # Should have 2 valid bars (malformed entry skipped)
        assert len(bars) == 2
        assert bars[0].volume == 0
        assert bars[1].volume == 0

    def test_tiingo_minute_bar_dataclass(self):
        """Test TiingoMinuteBar dataclass with volume field."""
        timestamp = datetime(2024, 8, 16, 13, 30)

        # Test with explicit volume
        bar_with_volume = TiingoMinuteBar(
            symbol='AAPL',
            timestamp=timestamp,
            open=223.92,
            high=224.515,
            low=223.705,
            close=223.705,
            volume=1000
        )

        assert bar_with_volume.volume == 1000
        assert bar_with_volume.vendor == 'tiingo'

        # Test with zero volume (default case)
        bar_zero_volume = TiingoMinuteBar(
            symbol='AAPL',
            timestamp=timestamp,
            open=223.92,
            high=224.515,
            low=223.705,
            close=223.705,
            volume=0
        )

        assert bar_zero_volume.volume == 0
        assert bar_zero_volume.vendor == 'tiingo'


@pytest.mark.integration
class TestTiingoVolumeIntegration:
    """Integration tests for Tiingo volume parsing."""

    def test_volume_field_consistency(self):
        """Test that volume field handling is consistent across methods."""
        adapter = TiingoIntradayAdapter.__new__(TiingoIntradayAdapter)

        # Same mock data for both methods
        mock_data = [
            {
                'date': '2024-08-16T13:30:00.000Z',
                'close': 223.705,
                'high': 224.515,
                'low': 223.705,
                'open': 223.92
                # No volume field
            }
        ]

        intraday_bars = adapter._parse_intraday_data('AAPL', mock_data)
        daily_bars = adapter._parse_daily_resampled('AAPL', mock_data)

        assert len(intraday_bars) == 1
        assert len(daily_bars) == 1
        assert intraday_bars[0].volume == daily_bars[0].volume == 0

    def test_volume_parsing_performance(self):
        """Test volume parsing performance with large dataset."""
        adapter = TiingoIntradayAdapter.__new__(TiingoIntradayAdapter)

        # Generate large mock dataset with valid timestamps
        mock_data = []
        for i in range(100):  # Reduced size to avoid timestamp issues
            hour = 9 + (i // 60)
            minute = i % 60
            if hour < 24:  # Only create valid hours
                mock_data.append({
                    'date': f'2024-08-16T{hour:02d}:{minute:02d}:00.000Z',
                    'close': 223.00 + i * 0.01,
                    'high': 224.00 + i * 0.01,
                    'low': 222.00 + i * 0.01,
                    'open': 223.50 + i * 0.01
                    # No volume field
                })

        import time
        start_time = time.time()
        bars = adapter._parse_intraday_data('AAPL', mock_data)
        end_time = time.time()

        assert len(bars) == len(mock_data)
        assert all(bar.volume == 0 for bar in bars)
        assert (end_time - start_time) < 1.0  # Should complete within 1 second


if __name__ == '__main__':
    pytest.main([__file__, '-v'])