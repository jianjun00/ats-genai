#!/usr/bin/env python3
"""
Integration tests for FirstRate backfill processor

Tests the complete FirstRate backfill system:
- Monthly processing logic
- Checkpoint functionality
- Error recovery
- Storage integration
- Progress tracking
"""

import pytest
import tempfile
import zipfile
import asyncio
import json
import os
from datetime import datetime, date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))

from populate_firstrate_minute_bars import FirstRateBackfillProcessor
from storage.file_based_minute_manager import MinuteBar


class TestFirstRateBackfillProcessor:
    """Test FirstRateBackfillProcessor functionality."""
    
    @pytest.fixture
    def temp_setup(self):
        """Set up temporary directories and mock data."""
        temp_dir = tempfile.mkdtemp()
        data_path = Path(temp_dir) / 'firstrate-data'
        output_path = Path(temp_dir) / 'minute-bars' / 'firstrate'
        checkpoint_file = Path(temp_dir) / 'test_checkpoint.json'
        
        # Create data directory structure
        stock_dir = data_path / 'stock'
        stock_dir.mkdir(parents=True)
        output_path.mkdir(parents=True)
        
        # Create sample zip files
        zip_data = {
            'stock_A_test.zip': {
                'AAPL_full_1min_adjsplitdiv.txt': [
                    '2023-01-03 09:30:00,150.00,151.00,149.50,150.50,1000000',
                    '2023-01-03 09:31:00,150.50,151.25,150.00,150.75,800000',
                    '2023-02-01 09:30:00,155.00,156.00,154.50,155.50,1200000'
                ],
                'AMZN_full_1min_adjsplitdiv.txt': [
                    '2023-01-03 09:30:00,90.00,91.00,89.50,90.50,500000',
                    '2023-02-01 09:30:00,92.00,93.00,91.50,92.50,600000'
                ]
            }
        }
        
        for zip_name, file_data in zip_data.items():
            zip_path = stock_dir / zip_name
            with zipfile.ZipFile(zip_path, 'w') as zf:
                for filename, lines in file_data.items():
                    zf.writestr(filename, '\n'.join(lines))
        
        yield {
            'temp_dir': temp_dir,
            'data_path': str(data_path),
            'output_path': str(output_path),
            'checkpoint_file': str(checkpoint_file)
        }
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_processor_initialization(self, temp_setup):
        """Test FirstRateBackfillProcessor initialization."""
        processor = FirstRateBackfillProcessor(
            data_path=temp_setup['data_path'],
            output_path=temp_setup['output_path'],
            checkpoint_file=temp_setup['checkpoint_file']
        )
        
        assert processor.adapter.vendor_name == "firstrate"
        assert processor.checkpoint_file == Path(temp_setup['checkpoint_file'])
        assert processor.asset_type == "stock"
        assert processor.checkpoint['completed_months'] == {}
    
    def test_checkpoint_loading_and_saving(self, temp_setup):
        """Test checkpoint loading and saving."""
        # Create initial checkpoint
        checkpoint_data = {
            'completed_months': {'AAPL': ['2023-01']},
            'failed_months': {},
            'last_processed': '2023-01-01T00:00:00',
            'total_symbols': 1
        }
        
        with open(temp_setup['checkpoint_file'], 'w') as f:
            json.dump(checkpoint_data, f)
        
        # Initialize processor and verify checkpoint loading
        processor = FirstRateBackfillProcessor(
            data_path=temp_setup['data_path'],
            output_path=temp_setup['output_path'],
            checkpoint_file=temp_setup['checkpoint_file']
        )
        
        assert processor.checkpoint['completed_months']['AAPL'] == ['2023-01']
        
        # Test saving
        processor.checkpoint['completed_months']['AAPL'].append('2023-02')
        processor._save_checkpoint()
        
        # Verify saved data
        with open(temp_setup['checkpoint_file'], 'r') as f:
            saved_data = json.load(f)
        
        assert '2023-02' in saved_data['completed_months']['AAPL']
    
    def test_generate_monthly_date_ranges(self, temp_setup):
        """Test monthly date range generation."""
        processor = FirstRateBackfillProcessor(
            data_path=temp_setup['data_path'],
            output_path=temp_setup['output_path']
        )
        
        start_date = date(2023, 1, 15)
        end_date = date(2023, 3, 10)
        
        ranges = processor.generate_monthly_date_ranges(start_date, end_date)
        
        assert len(ranges) == 3
        assert ranges[0] == (date(2023, 1, 1), date(2023, 1, 31))
        assert ranges[1] == (date(2023, 2, 1), date(2023, 2, 28))
        assert ranges[2] == (date(2023, 3, 1), date(2023, 3, 10))
    
    @patch('storage.file_based_minute_manager.FileBasedMinuteManager.store_minute_data')
    async def test_process_symbol_month_success(self, mock_store, temp_setup):
        """Test successful month processing."""
        mock_store.return_value = {'records_stored': 2}
        
        processor = FirstRateBackfillProcessor(
            data_path=temp_setup['data_path'],
            output_path=temp_setup['output_path']
        )
        
        zip_files = [str(Path(temp_setup['data_path']) / 'stock' / 'stock_A_test.zip')]
        
        result = await processor.process_symbol_month(
            'AAPL',
            zip_files,
            date(2023, 1, 1),
            date(2023, 1, 31)
        )
        
        assert result['success'] is True
        assert result['records'] == 2
        assert result['month'] == '2023-01'
        
        # Verify store_minute_data was called
        mock_store.assert_called_once()
        args, kwargs = mock_store.call_args
        assert kwargs['symbol'] == 'AAPL'
        assert len(kwargs['bars']) == 2  # 2 January records
        assert all(isinstance(bar, MinuteBar) for bar in kwargs['bars'])
    
    @patch('storage.file_based_minute_manager.FileBasedMinuteManager.store_minute_data')
    async def test_process_symbol_month_no_data(self, mock_store, temp_setup):
        """Test month processing with no data."""
        processor = FirstRateBackfillProcessor(
            data_path=temp_setup['data_path'],
            output_path=temp_setup['output_path']
        )
        
        zip_files = [str(Path(temp_setup['data_path']) / 'stock' / 'stock_A_test.zip')]
        
        # Request data for a month with no data
        result = await processor.process_symbol_month(
            'AAPL',
            zip_files,
            date(2023, 12, 1),
            date(2023, 12, 31)
        )
        
        assert result['success'] is True
        assert result['records'] == 0
        assert result['month'] == '2023-12'
        
        # Storage should not be called for empty data
        mock_store.assert_not_called()
    
    @patch('storage.file_based_minute_manager.FileBasedMinuteManager.store_minute_data')
    async def test_process_symbol_month_error(self, mock_store, temp_setup):
        """Test month processing with storage error."""
        mock_store.side_effect = Exception("Storage error")
        
        processor = FirstRateBackfillProcessor(
            data_path=temp_setup['data_path'],
            output_path=temp_setup['output_path']
        )
        
        zip_files = [str(Path(temp_setup['data_path']) / 'stock' / 'stock_A_test.zip')]
        
        result = await processor.process_symbol_month(
            'AAPL',
            zip_files,
            date(2023, 1, 1),
            date(2023, 1, 31)
        )
        
        assert result['success'] is False
        assert 'error' in result
        assert result['month'] == '2023-01'
    
    @patch('storage.file_based_minute_manager.FileBasedMinuteManager.store_minute_data')
    async def test_process_symbol_complete(self, mock_store, temp_setup):
        """Test complete symbol processing."""
        mock_store.return_value = {'records_stored': 1}
        
        processor = FirstRateBackfillProcessor(
            data_path=temp_setup['data_path'],
            output_path=temp_setup['output_path']
        )
        
        # Mock symbol info
        symbol_info = {
            'min_date': date(2023, 1, 3),
            'max_date': date(2023, 2, 1),
            'zip_files': [str(Path(temp_setup['data_path']) / 'stock' / 'stock_A_test.zip')]
        }
        
        stats = await processor.process_symbol('AAPL', symbol_info)
        
        assert stats['months_total'] == 2  # January and February
        assert stats['months_completed'] == 2
        assert stats['months_failed'] == 0
        assert stats['total_records'] == 2  # 1 record per month
        
        # Verify checkpoint was updated
        assert len(processor.checkpoint['completed_months']['AAPL']) == 2
        assert '2023-01' in processor.checkpoint['completed_months']['AAPL']
        assert '2023-02' in processor.checkpoint['completed_months']['AAPL']
    
    @patch('storage.file_based_minute_manager.FileBasedMinuteManager.store_minute_data')
    async def test_checkpoint_resume(self, mock_store, temp_setup):
        """Test resuming from checkpoint."""
        mock_store.return_value = {'records_stored': 1}
        
        # Create checkpoint with one completed month
        checkpoint_data = {
            'completed_months': {'AAPL': ['2023-01']},
            'failed_months': {},
            'last_processed': None,
            'total_symbols': 0
        }
        
        with open(temp_setup['checkpoint_file'], 'w') as f:
            json.dump(checkpoint_data, f)
        
        processor = FirstRateBackfillProcessor(
            data_path=temp_setup['data_path'],
            output_path=temp_setup['output_path'],
            checkpoint_file=temp_setup['checkpoint_file']
        )
        
        symbol_info = {
            'min_date': date(2023, 1, 3),
            'max_date': date(2023, 2, 1),
            'zip_files': [str(Path(temp_setup['data_path']) / 'stock' / 'stock_A_test.zip')]
        }
        
        stats = await processor.process_symbol('AAPL', symbol_info)
        
        # Should process both months but skip January (already completed)
        # The stats show total completed for this run, which includes skipped months
        assert stats['months_completed'] == 2  # Both months marked as completed
        assert stats['total_records'] == 1  # Only February actually processed
        
        # Storage should only be called once (for February)
        assert mock_store.call_count == 1
    
    def test_get_symbol_inventory(self, temp_setup):
        """Test symbol inventory building."""
        processor = FirstRateBackfillProcessor(
            data_path=temp_setup['data_path'],
            output_path=temp_setup['output_path']
        )
        
        inventory = processor.get_symbol_inventory()
        
        assert len(inventory) == 2
        assert 'AAPL' in inventory
        assert 'AMZN' in inventory
        
        aapl_info = inventory['AAPL']
        assert aapl_info['min_date'] == date(2023, 1, 3)
        assert aapl_info['max_date'] == date(2023, 2, 1)  # Latest data
        assert aapl_info['total_files'] == 1


class TestFirstRateMonthlyProcessing:
    """Test monthly processing logic specifically."""
    
    def test_month_boundary_handling(self):
        """Test proper month boundary handling."""
        from populate_firstrate_minute_bars import FirstRateBackfillProcessor
        
        processor = FirstRateBackfillProcessor()
        
        # Test year boundary
        ranges = processor.generate_monthly_date_ranges(
            date(2022, 12, 15), 
            date(2023, 2, 10)
        )
        
        assert len(ranges) == 3
        assert ranges[0] == (date(2022, 12, 1), date(2022, 12, 31))
        assert ranges[1] == (date(2023, 1, 1), date(2023, 1, 31))
        assert ranges[2] == (date(2023, 2, 1), date(2023, 2, 10))
        
        # Test leap year
        ranges = processor.generate_monthly_date_ranges(
            date(2024, 2, 1),
            date(2024, 2, 29)
        )
        
        assert len(ranges) == 1
        assert ranges[0] == (date(2024, 2, 1), date(2024, 2, 29))


class TestFirstRateTimezoneIntegration:
    """Test timezone handling in the complete system."""
    
    @pytest.fixture
    def temp_timezone_data(self):
        """Create test data with specific timezone scenarios."""
        temp_dir = tempfile.mkdtemp()
        data_path = Path(temp_dir) / 'firstrate-data'
        stock_dir = data_path / 'stock'
        stock_dir.mkdir(parents=True)
        
        # Create data spanning EST/EDT transition
        zip_path = stock_dir / 'stock_TZ_test.zip'
        timezone_data = {
            'TEST_full_1min_adjsplitdiv.txt': [
                # EST data (January)
                '2023-01-15 09:30:00,100.00,101.00,99.50,100.50,1000',
                # EDT data (July) 
                '2023-07-15 09:30:00,105.00,106.00,104.50,105.50,1000',
            ]
        }
        
        with zipfile.ZipFile(zip_path, 'w') as zf:
            for filename, lines in timezone_data.items():
                zf.writestr(filename, '\n'.join(lines))
        
        yield str(data_path)
        
        import shutil
        shutil.rmtree(temp_dir)
    
    def test_timezone_conversion_integration(self, temp_timezone_data):
        """Test timezone conversion in full processing pipeline."""
        from market_data.agent.firstrate_adapter import FirstRateAdapter
        
        adapter = FirstRateAdapter(temp_timezone_data)
        zip_files = adapter.get_available_zip_files('stock')
        
        # Process the test symbol
        ticks = list(adapter.process_minute_data_from_zip(zip_files[0], 'TEST'))
        
        assert len(ticks) == 2
        
        # EST tick (January): 9:30 EST -> 14:30 UTC
        est_tick = ticks[0]
        assert est_tick.timestamp.month == 1
        assert est_tick.timestamp.hour == 14  # 9:30 + 5 hours
        assert est_tick.timestamp.minute == 30
        
        # EDT tick (July): 9:30 EDT -> 13:30 UTC
        edt_tick = ticks[1]
        assert edt_tick.timestamp.month == 7
        assert edt_tick.timestamp.hour == 13  # 9:30 + 4 hours
        assert edt_tick.timestamp.minute == 30


if __name__ == "__main__":
    pytest.main([__file__, "-v"])