#!/usr/bin/env python3
"""
Unit tests for FirstRateAdapter

Tests the FirstRate data adapter's ability to:
- Read zip files and extract symbol lists
- Process minute data with timezone conversion (EDT -> UTC)
- Handle date range filtering
- Extract symbol inventory
- Handle edge cases and errors
"""

import pytest
import tempfile
import zipfile
import os
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from domains.market_data.services.agent.firstrate_adapter import FirstRateAdapter, Tick


class TestFirstRateAdapter:
    """Test FirstRateAdapter functionality."""

    @pytest.fixture
    def temp_data_dir(self):
        """Create temporary directory with sample FirstRate data."""
        temp_dir = tempfile.mkdtemp()
        stock_dir = Path(temp_dir) / 'stock'
        stock_dir.mkdir(parents=True)

        # Create sample zip file with mock data
        zip_path = stock_dir / 'stock_A_test.zip'

        sample_data = {
            'AAPL_full_1min_adjsplitdiv.txt': [
                '2023-01-03 09:30:00,150.00,151.00,149.50,150.50,1000000',
                '2023-01-03 09:31:00,150.50,151.25,150.00,150.75,800000',
                '2023-01-03 09:32:00,150.75,152.00,150.50,151.50,1200000'
            ],
            'AMZN_full_1min_adjsplitdiv.txt': [
                '2023-01-03 09:30:00,90.00,91.00,89.50,90.50,500000',
                '2023-01-03 09:31:00,90.50,91.25,90.00,90.75,400000'
            ]
        }

        with zipfile.ZipFile(zip_path, 'w') as zf:
            for filename, lines in sample_data.items():
                zf.writestr(filename, '\n'.join(lines))

        yield temp_dir

        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)

    def test_adapter_initialization(self, temp_data_dir):
        """Test FirstRateAdapter initialization."""
        adapter = FirstRateAdapter(temp_data_dir)

        assert adapter.vendor_name == "firstrate"
        assert adapter.data_path == Path(temp_data_dir)
        assert adapter.edt_tz == ZoneInfo("America/New_York")
        assert adapter.utc_tz == ZoneInfo("UTC")
        assert 'stock' in adapter.data_types

    def test_get_available_zip_files(self, temp_data_dir):
        """Test getting available zip files."""
        adapter = FirstRateAdapter(temp_data_dir)
        zip_files = adapter.get_available_zip_files('stock')

        assert len(zip_files) == 1
        assert zip_files[0].name == 'stock_A_test.zip'

    def test_extract_symbols_from_zip(self, temp_data_dir):
        """Test extracting symbols from zip file."""
        adapter = FirstRateAdapter(temp_data_dir)
        zip_files = adapter.get_available_zip_files('stock')
        symbols = adapter.extract_symbols_from_zip(zip_files[0])

        assert len(symbols) == 2
        assert 'AAPL' in symbols
        assert 'AMZN' in symbols

    def test_timezone_conversion(self, temp_data_dir):
        """Test EDT to UTC timezone conversion."""
        adapter = FirstRateAdapter(temp_data_dir)
        zip_files = adapter.get_available_zip_files('stock')

        # Process AAPL data
        ticks = list(adapter.process_minute_data_from_zip(zip_files[0], 'AAPL'))

        assert len(ticks) == 3

        # First tick should be 09:30 EST -> 14:30 UTC (winter) or 13:30 UTC (summer)
        first_tick = ticks[0]
        assert first_tick.symbol == 'AAPL'
        assert first_tick.timestamp.tzinfo == ZoneInfo("UTC")

        # Verify the conversion (January 3, 2023 is EST, so +5 hours to UTC)
        assert first_tick.timestamp.hour == 14  # 9:30 EST + 5 = 14:30 UTC
        assert first_tick.timestamp.minute == 30

        # Verify OHLCV data
        assert first_tick.open == 150.00
        assert first_tick.high == 151.00
        assert first_tick.low == 149.50
        assert first_tick.close == 150.50
        assert first_tick.volume == 1000000
        assert first_tick.vendor == "firstrate"

    def test_date_range_filtering(self, temp_data_dir):
        """Test date range filtering functionality."""
        adapter = FirstRateAdapter(temp_data_dir)
        zip_files = adapter.get_available_zip_files('stock')

        # Filter to only January 3, 2023
        start_date = date(2023, 1, 3)
        end_date = date(2023, 1, 3)

        ticks = list(adapter.process_minute_data_from_zip(
            zip_files[0], 'AAPL', start_date, end_date
        ))

        assert len(ticks) == 3  # All sample data is from Jan 3

        # Filter to exclude all data
        start_date = date(2023, 1, 4)
        end_date = date(2023, 1, 4)

        ticks = list(adapter.process_minute_data_from_zip(
            zip_files[0], 'AAPL', start_date, end_date
        ))

        assert len(ticks) == 0  # No data should match

    def test_get_date_range_for_symbol(self, temp_data_dir):
        """Test getting date range for a symbol."""
        adapter = FirstRateAdapter(temp_data_dir)
        zip_files = adapter.get_available_zip_files('stock')

        min_date, max_date = adapter.get_date_range_for_symbol(zip_files[0], 'AAPL')

        assert min_date == date(2023, 1, 3)
        assert max_date == date(2023, 1, 3)

    def test_get_symbol_inventory(self, temp_data_dir):
        """Test building symbol inventory."""
        adapter = FirstRateAdapter(temp_data_dir)
        inventory = adapter.get_symbol_inventory('stock')

        assert len(inventory) == 2
        assert 'AAPL' in inventory
        assert 'AMZN' in inventory

        aapl_info = inventory['AAPL']
        assert aapl_info['min_date'] == date(2023, 1, 3)
        assert aapl_info['max_date'] == date(2023, 1, 3)
        assert aapl_info['total_files'] == 1
        assert len(aapl_info['zip_files']) == 1

    def test_missing_symbol_handling(self, temp_data_dir):
        """Test handling of missing symbols."""
        adapter = FirstRateAdapter(temp_data_dir)
        zip_files = adapter.get_available_zip_files('stock')

        # Try to process a symbol that doesn't exist
        ticks = list(adapter.process_minute_data_from_zip(zip_files[0], 'NONEXISTENT'))
        assert len(ticks) == 0

    def test_invalid_data_handling(self, temp_data_dir):
        """Test handling of invalid data lines."""
        # Create zip with invalid data
        stock_dir = Path(temp_data_dir) / 'stock'
        zip_path = stock_dir / 'stock_B_invalid.zip'

        invalid_data = {
            'TEST_full_1min_adjsplitdiv.txt': [
                '2023-01-03 09:30:00,150.00,151.00,149.50,150.50,1000000',  # Valid
                'invalid line with no commas',  # Invalid
                '2023-01-03,150.00',  # Invalid - too few fields
                '2023-01-03 09:32:00,150.00,151.00,149.50,150.50,invalid_volume',  # Invalid volume
                '2023-01-03 09:33:00,150.00,151.00,149.50,150.50,2000000'  # Valid
            ]
        }

        with zipfile.ZipFile(zip_path, 'w') as zf:
            for filename, lines in invalid_data.items():
                zf.writestr(filename, '\n'.join(lines))

        adapter = FirstRateAdapter(temp_data_dir)
        ticks = list(adapter.process_minute_data_from_zip(zip_path, 'TEST'))

        # Should only get 2 valid ticks
        assert len(ticks) == 2
        assert ticks[0].timestamp.minute == 30
        assert ticks[1].timestamp.minute == 33

    def test_fetch_instruments(self, temp_data_dir):
        """Test fetch_instruments method."""
        adapter = FirstRateAdapter(temp_data_dir)
        instruments = adapter.fetch_instruments()

        assert len(instruments) == 2
        assert 'AAPL' in instruments
        assert 'AMZN' in instruments

    def test_abstract_methods_not_implemented(self, temp_data_dir):
        """Test that abstract methods raise NotImplementedError."""
        adapter = FirstRateAdapter(temp_data_dir)

        with pytest.raises(NotImplementedError):
            adapter.fetch_eod([], None, None)

        with pytest.raises(NotImplementedError):
            adapter.fetch_ticks('AAPL', None, None)

        with pytest.raises(NotImplementedError):
            adapter.fetch_interval('AAPL', '1m', None, None)


class TestFirstRateTimezoneConversion:
    """Test timezone conversion edge cases."""

    def test_dst_transition(self):
        """Test timezone conversion during DST transitions."""
        # Create adapter
        adapter = FirstRateAdapter()

        # Test EST (Standard Time) - January
        est_time = datetime(2023, 1, 15, 9, 30, 0, tzinfo=ZoneInfo("America/New_York"))
        utc_time = est_time.astimezone(ZoneInfo("UTC"))
        assert utc_time.hour == 14  # 9:30 EST + 5 = 14:30 UTC

        # Test EDT (Daylight Time) - July
        edt_time = datetime(2023, 7, 15, 9, 30, 0, tzinfo=ZoneInfo("America/New_York"))
        utc_time = edt_time.astimezone(ZoneInfo("UTC"))
        assert utc_time.hour == 13  # 9:30 EDT + 4 = 13:30 UTC


class TestTickDataStructure:
    """Test the Tick data structure."""

    def test_tick_creation(self):
        """Test creating Tick objects."""
        timestamp = datetime(2023, 1, 3, 14, 30, 0, tzinfo=ZoneInfo("UTC"))

        tick = Tick(
            symbol="AAPL",
            timestamp=timestamp,
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000000,
            vendor="firstrate"
        )

        assert tick.symbol == "AAPL"
        assert tick.timestamp == timestamp
        assert tick.open == 150.00
        assert tick.high == 151.00
        assert tick.low == 149.50
        assert tick.close == 150.50
        assert tick.volume == 1000000
        assert tick.vendor == "firstrate"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])