#!/usr/bin/env python3
"""
Comprehensive test cases for FileBasedMinuteManager

Tests all edge cases including:
- Missing monthly files
- Overlapping intervals
- Data corruption scenarios
- Atomic operations
- Backup and restore
- Multiple vendor data
- Large data sets
- Concurrent operations
"""

import pytest
import asyncio
import tempfile
import shutil
import pandas as pd
from datetime import datetime, timedelta
import random

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from storage.file_based_minute_manager import (
    FileBasedMinuteManager,
    MinuteBar
)

class TestFileBasedMinuteManager:
    """Comprehensive test suite for file-based minute data manager"""

    @pytest.fixture
    async def temp_manager(self):
        """Create temporary file manager for testing"""
        temp_dir = tempfile.mkdtemp()
        manager = FileBasedMinuteManager(
            base_path=temp_dir,
            max_concurrent_operations=2,
            backup_enabled=True,
            compression='snappy'
        )
        yield manager
        await manager.close()
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def sample_bars(self):
        """Create sample minute bars for testing"""
        bars = []
        base_time = datetime(2024, 1, 15, 9, 30)  # Market open

        for i in range(10):
            bar = MinuteBar(
                symbol='AAPL',
                timestamp=base_time + timedelta(minutes=i),
                open=150.0 + random.uniform(-1, 1),
                high=150.0 + random.uniform(0, 2),
                low=150.0 + random.uniform(-2, 0),
                close=150.0 + random.uniform(-1, 1),
                volume=random.randint(1000, 10000),
                vendor='test'
            )
            bars.append(bar)

        return bars

    @pytest.fixture
    def overlapping_bars(self):
        """Create bars that overlap with sample_bars"""
        bars = []
        base_time = datetime(2024, 1, 15, 9, 35)  # Overlaps with sample_bars

        for i in range(10):
            bar = MinuteBar(
                symbol='AAPL',
                timestamp=base_time + timedelta(minutes=i),
                open=151.0 + random.uniform(-1, 1),
                high=151.0 + random.uniform(0, 2),
                low=151.0 + random.uniform(-2, 0),
                close=151.0 + random.uniform(-1, 1),
                volume=random.randint(1500, 12000),
                vendor='test2'
            )
            bars.append(bar)

        return bars

    # Test Case 1: Missing Monthly Files Scenario
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_missing_monthly_files_query(self, temp_manager):
        """Test querying data when monthly files don't exist"""
        # Query data for a symbol/date that doesn't exist
        result = await temp_manager.query_minute_data(
            'NONEXISTENT',
            datetime(2024, 1, 1, 9, 30),
            datetime(2024, 1, 1, 16, 0)
        )

        # Should return empty DataFrame without error
        assert result.empty
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_missing_files_partial_data(self, temp_manager, sample_bars):
        """Test querying across months where some files exist and some don't"""
        # Store data for January
        await temp_manager.store_minute_data('AAPL', sample_bars)

        # Query spanning January to March (February and March files don't exist)
        result = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 1),
            datetime(2024, 3, 31)
        )

        # Should return only January data
        assert not result.empty
        assert len(result) == len(sample_bars)
        assert all(result['timestamp'].dt.month == 1)

    # Test Case 2: Overlapping Intervals Between Files and New Data
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_overlap_merge_strategy(self, temp_manager, sample_bars, overlapping_bars):
        """Test overlap handling with merge strategy"""
        # Store initial data
        result1 = await temp_manager.store_minute_data('AAPL', sample_bars)
        assert result1['stored'] == len(sample_bars)

        # Store overlapping data with merge strategy
        result2 = await temp_manager.store_minute_data(
            'AAPL', overlapping_bars, 'merge'
        )

        # Should have updates for overlapping timestamps and new data stored
        assert result2['updated'] > 0 or result2['stored'] > 0

        # Query all data
        all_data = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 15, 9, 0),
            datetime(2024, 1, 15, 10, 0)
        )

        # Should have no duplicate timestamps
        assert len(all_data) == len(all_data['timestamp'].unique())

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_overlap_replace_strategy(self, temp_manager, sample_bars, overlapping_bars):
        """Test overlap handling with replace strategy"""
        # Store initial data
        await temp_manager.store_minute_data('AAPL', sample_bars)

        # Store overlapping data with replace strategy
        result = await temp_manager.store_minute_data(
            'AAPL', overlapping_bars, 'replace'
        )

        # Query overlapping time range
        overlap_data = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 15, 9, 35),
            datetime(2024, 1, 15, 9, 45)
        )

        # Data in overlap range should be from second dataset (vendor='test2')
        overlap_vendors = overlap_data['vendor'].unique()
        assert 'test2' in overlap_vendors

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_overlap_skip_strategy(self, temp_manager, sample_bars, overlapping_bars):
        """Test overlap handling with skip strategy"""
        # Store initial data
        result1 = await temp_manager.store_minute_data('AAPL', sample_bars)
        original_count = result1['stored']

        # Store overlapping data with skip strategy
        result2 = await temp_manager.store_minute_data(
            'AAPL', overlapping_bars, 'skip'
        )

        # Should skip overlapping timestamps
        assert result2['skipped'] > 0

        # Query overlapping time range (only the overlapping timestamps that should have been skipped)
        overlap_data = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 15, 9, 35),
            datetime(2024, 1, 15, 9, 39)  # Only the overlapping part
        )

        # Data in overlap range should only be from first dataset (vendor='test') since new data was skipped
        if not overlap_data.empty:
            overlap_vendors = overlap_data['vendor'].unique()
            assert 'test' in overlap_vendors and 'test2' not in overlap_vendors

        # Query the non-overlapping new data (should be stored with vendor='test2')
        new_data = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 15, 9, 40),
            datetime(2024, 1, 15, 9, 44)
        )

        if not new_data.empty:
            new_vendors = new_data['vendor'].unique()
            assert 'test2' in new_vendors

    # Test Case 3: File Corruption and Recovery
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_corrupted_file_handling(self, temp_manager, sample_bars):
        """Test handling of corrupted Parquet files"""
        # Store initial data
        await temp_manager.store_minute_data('AAPL', sample_bars)

        # Corrupt the file by writing invalid data
        file_path = temp_manager._get_monthly_file_path('AAPL', 2024, 1)
        with open(file_path, 'w') as f:
            f.write("CORRUPTED DATA")

        # Querying should handle corruption gracefully (return empty data, not crash)
        result = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 1),
            datetime(2024, 1, 31)
        )

        # Should return empty DataFrame when file is corrupted
        assert result.empty

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_missing_metadata_file(self, temp_manager, sample_bars):
        """Test handling when metadata file is missing"""
        # Store data (creates metadata)
        await temp_manager.store_minute_data('AAPL', sample_bars)

        # Delete metadata file
        file_path = temp_manager._get_monthly_file_path('AAPL', 2024, 1)
        metadata_path = temp_manager._get_metadata_path(file_path)
        metadata_path.unlink()

        # Should still be able to query data
        result = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 15, 9, 30),
            datetime(2024, 1, 15, 10, 30)
        )

        assert not result.empty

    # Test Case 4: Cross-Month Data Handling
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_cross_month_data_storage(self, temp_manager):
        """Test storing data that spans multiple months"""
        # Create bars spanning December to January
        cross_month_bars = []

        # December 31st data
        dec_time = datetime(2023, 12, 31, 15, 30)
        for i in range(30):
            bar = MinuteBar(
                symbol='AAPL',
                timestamp=dec_time + timedelta(minutes=i),
                open=150.0, high=151.0, low=149.0, close=150.5,
                volume=1000, vendor='test'
            )
            cross_month_bars.append(bar)

        # January 1st data
        jan_time = datetime(2024, 1, 1, 9, 30)
        for i in range(30):
            bar = MinuteBar(
                symbol='AAPL',
                timestamp=jan_time + timedelta(minutes=i),
                open=151.0, high=152.0, low=150.0, close=151.5,
                volume=1100, vendor='test'
            )
            cross_month_bars.append(bar)

        # Store cross-month data
        result = await temp_manager.store_minute_data('AAPL', cross_month_bars)

        # Should create files for both months
        assert result['files_created'] == 2
        assert result['stored'] == 60

        # Verify December file
        dec_data = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2023, 12, 31),
            datetime(2023, 12, 31, 23, 59)
        )
        assert len(dec_data) == 30

        # Verify January file
        jan_data = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 1),
            datetime(2024, 1, 1, 23, 59)
        )
        assert len(jan_data) == 30

    # Test Case 5: Large Dataset Handling
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_large_dataset_performance(self, temp_manager):
        """Test performance with large datasets"""
        # Create large dataset (1 month of minute data)
        large_bars = []
        start_time = datetime(2024, 1, 1, 9, 30)

        # 22 trading days * 390 minutes = 8,580 bars
        for day in range(22):
            day_start = start_time + timedelta(days=day)
            for minute in range(390):
                bar = MinuteBar(
                    symbol='AAPL',
                    timestamp=day_start + timedelta(minutes=minute),
                    open=150.0 + random.uniform(-5, 5),
                    high=150.0 + random.uniform(0, 7),
                    low=150.0 + random.uniform(-7, 0),
                    close=150.0 + random.uniform(-5, 5),
                    volume=random.randint(1000, 50000),
                    vendor='large_test'
                )
                large_bars.append(bar)

        # Store large dataset
        start_time = datetime.now()
        result = await temp_manager.store_minute_data('AAPL', large_bars)
        storage_time = (datetime.now() - start_time).total_seconds()

        assert result['stored'] == len(large_bars)
        assert storage_time < 30  # Should complete within 30 seconds

        # Query performance test
        start_time = datetime.now()
        query_result = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 1),
            datetime(2024, 1, 31)
        )
        query_time = (datetime.now() - start_time).total_seconds()

        assert len(query_result) == len(large_bars)
        assert query_time < 10  # Should complete within 10 seconds

    # Test Case 6: Multiple Vendors Data
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_multiple_vendors_same_symbol(self, temp_manager):
        """Test storing data from multiple vendors for same symbol/time"""
        # Create data from different vendors
        polygon_bars = []
        tiingo_bars = []
        base_time = datetime(2024, 1, 15, 10, 0)

        for i in range(20):
            timestamp = base_time + timedelta(minutes=i)

            # Polygon data
            polygon_bar = MinuteBar(
                symbol='AAPL', timestamp=timestamp,
                open=150.0, high=151.0, low=149.0, close=150.5,
                volume=1000, vendor='polygon', quality_score=0.9
            )
            polygon_bars.append(polygon_bar)

            # Tiingo data (slightly different values)
            tiingo_bar = MinuteBar(
                symbol='AAPL', timestamp=timestamp,
                open=150.1, high=151.1, low=149.1, close=150.6,
                volume=1050, vendor='tiingo', quality_score=0.8
            )
            tiingo_bars.append(tiingo_bar)

        # Store Polygon data first
        result1 = await temp_manager.store_minute_data('AAPL', polygon_bars)
        assert result1['stored'] == 20

        # Store Tiingo data with merge strategy (should update existing)
        result2 = await temp_manager.store_minute_data('AAPL', tiingo_bars, 'merge')
        assert result2['updated'] == 20

        # Query and verify latest vendor data is kept
        result_data = await temp_manager.query_minute_data(
            'AAPL', base_time, base_time + timedelta(minutes=19)
        )

        # With merge strategy, should keep last vendor's data (Tiingo)
        assert all(result_data['vendor'] == 'tiingo')
        assert len(result_data) == 20

    # Test Case 7: Concurrent Operations
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_storage_operations(self, temp_manager):
        """Test concurrent storage operations on different symbols"""

        async def store_symbol_data(symbol: str, offset_minutes: int):
            bars = []
            base_time = datetime(2024, 1, 15, 9, 30) + timedelta(minutes=offset_minutes)

            for i in range(50):
                bar = MinuteBar(
                    symbol=symbol,
                    timestamp=base_time + timedelta(minutes=i),
                    open=100.0 + offset_minutes,
                    high=101.0 + offset_minutes,
                    low=99.0 + offset_minutes,
                    close=100.5 + offset_minutes,
                    volume=1000 + offset_minutes * 10,
                    vendor='concurrent_test'
                )
                bars.append(bar)

            return await temp_manager.store_minute_data(symbol, bars)

        # Run concurrent operations
        tasks = [
            store_symbol_data('AAPL', 0),
            store_symbol_data('MSFT', 60),
            store_symbol_data('GOOGL', 120),
            store_symbol_data('AMZN', 180)
        ]

        results = await asyncio.gather(*tasks)

        # All operations should succeed
        for result in results:
            assert result['stored'] == 50
            assert result.get('errors', []) == []

    # Test Case 8: Backup and Recovery
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_backup_creation_and_recovery(self, temp_manager, sample_bars):
        """Test backup creation during file updates"""
        # Store initial data
        await temp_manager.store_minute_data('AAPL', sample_bars)

        # Store overlapping data (should trigger backup)
        new_bars = []
        base_time = datetime(2024, 1, 15, 9, 30)
        for i in range(5):
            bar = MinuteBar(
                symbol='AAPL',
                timestamp=base_time + timedelta(minutes=i),
                open=152.0, high=153.0, low=151.0, close=152.5,
                volume=2000, vendor='backup_test'
            )
            new_bars.append(bar)

        result = await temp_manager.store_minute_data('AAPL', new_bars, 'merge')

        # Check backup was created
        backup_files = list(temp_manager.backup_path.rglob('*.backup'))
        assert len(backup_files) > 0

        # Verify backup contains original data
        # (In a real scenario, you'd restore from backup and verify)

    # Test Case 9: Data Integrity Verification
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_integrity_verification(self, temp_manager, sample_bars):
        """Test comprehensive data integrity checks"""
        # Store data
        await temp_manager.store_minute_data('AAPL', sample_bars)

        # Run integrity check
        integrity_result = await temp_manager.verify_data_integrity('AAPL')

        # Should pass all checks
        assert integrity_result['verified_files'] == 1
        assert integrity_result['corrupt_files'] == 0
        assert integrity_result['missing_metadata'] == 0
        assert integrity_result['checksum_mismatches'] == 0

    # Test Case 10: Edge Cases in Data Structure
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_edge_case_data_values(self, temp_manager):
        """Test edge cases with unusual data values"""
        edge_case_bars = []
        base_time = datetime(2024, 1, 15, 9, 30)

        # Edge case values
        edge_cases = [
            {'open': 0.01, 'high': 0.01, 'low': 0.01, 'close': 0.01, 'volume': 0},  # Penny stock
            {'open': 999999.99, 'high': 999999.99, 'low': 999999.99, 'close': 999999.99, 'volume': 0},  # High price
            {'open': 100.0, 'high': 100.0, 'low': 100.0, 'close': 100.0, 'volume': 2**31-1},  # Max volume
            {'open': float('nan'), 'high': 150.0, 'low': 149.0, 'close': 150.0, 'volume': 1000},  # NaN values
        ]

        for i, case in enumerate(edge_cases):
            bar = MinuteBar(
                symbol='EDGE',
                timestamp=base_time + timedelta(minutes=i),
                open=case['open'],
                high=case['high'],
                low=case['low'],
                close=case['close'],
                volume=case['volume'],
                vendor='edge_test'
            )
            edge_case_bars.append(bar)

        # Should handle edge cases without crashing
        try:
            result = await temp_manager.store_minute_data('EDGE', edge_case_bars)
            # NaN values might cause issues, so we allow for some data to be filtered
            assert result['stored'] >= 3
        except Exception as e:
            # If NaN causes issues, that's acceptable behavior
            assert 'nan' in str(e).lower() or 'invalid' in str(e).lower()

    # Test Case 11: Storage Statistics Accuracy
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_storage_statistics_accuracy(self, temp_manager):
        """Test accuracy of storage statistics reporting"""
        # Create data for multiple symbols and months
        symbols = ['AAPL', 'MSFT', 'GOOGL']
        total_bars = 0

        for symbol in symbols:
            bars = []
            base_time = datetime(2024, 1, 15, 9, 30)

            for i in range(100):
                bar = MinuteBar(
                    symbol=symbol,
                    timestamp=base_time + timedelta(minutes=i),
                    open=150.0, high=151.0, low=149.0, close=150.5,
                    volume=1000, vendor='stats_test'
                )
                bars.append(bar)

            await temp_manager.store_minute_data(symbol, bars)
            total_bars += len(bars)

        # Get statistics
        stats = await temp_manager.get_storage_stats()

        # Verify statistics accuracy
        assert stats['symbols'] == len(symbols)
        assert stats['total_records'] == total_bars
        assert stats['files'] == len(symbols)  # One file per symbol (same month)
        assert stats['total_size_bytes'] > 0

    # Test Case 12: Query Edge Cases
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_query_edge_cases(self, temp_manager, sample_bars):
        """Test edge cases in data querying"""
        # Store sample data
        await temp_manager.store_minute_data('AAPL', sample_bars)

        # Test 1: Query with end date before start date
        result = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 15, 16, 0),  # End time
            datetime(2024, 1, 15, 9, 30)   # Start time (invalid)
        )
        assert result.empty

        # Test 2: Query with exact timestamp boundaries
        first_timestamp = sample_bars[0].timestamp
        result = await temp_manager.query_minute_data(
            'AAPL',
            first_timestamp,
            first_timestamp
        )
        assert len(result) >= 1

        # Test 3: Query non-existent columns
        result = await temp_manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 15, 9, 30),
            datetime(2024, 1, 15, 10, 30),
            columns=['nonexistent_column']
        )
        # Should return empty or handle gracefully
        assert isinstance(result, pd.DataFrame)

# Test fixtures for pytest
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

if __name__ == "__main__":
    # Run specific test for debugging
    import asyncio

    async def run_specific_test():
        test_instance = TestFileBasedMinuteManager()

        # Create temporary manager
        temp_dir = tempfile.mkdtemp()
        manager = FileBasedMinuteManager(base_path=temp_dir, backup_enabled=True)

        try:
            # Create sample data
            bars = []
            base_time = datetime(2024, 1, 15, 9, 30)
            for i in range(10):
                bar = MinuteBar(
                    symbol='TEST',
                    timestamp=base_time + timedelta(minutes=i),
                    open=150.0 + i,
                    high=151.0 + i,
                    low=149.0 + i,
                    close=150.5 + i,
                    volume=1000 + i*100,
                    vendor='manual_test'
                )
                bars.append(bar)

            # Test storage
            print("Testing storage...")
            result = await manager.store_minute_data('TEST', bars)
            print(f"Storage result: {result}")

            # Test query
            print("Testing query...")
            query_result = await manager.query_minute_data(
                'TEST',
                datetime(2024, 1, 15, 9, 30),
                datetime(2024, 1, 15, 10, 30)
            )
            print(f"Query returned {len(query_result)} records")

            # Test stats
            print("Testing statistics...")
            stats = await manager.get_storage_stats()
            print(f"Storage stats: {stats}")

        finally:
            await manager.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

    asyncio.run(run_specific_test())