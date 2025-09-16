#!/usr/bin/env python3
"""
Comprehensive Test Suite for File-Based Time-Series Storage

Tests all components of the file-based storage system:
- TimeSeriesFileManager
- TimeSeriesQueryEngine
- DualWriteTimeSeriesManager
- Data migration functionality
- Performance benchmarking

Run with:
    PYTHONPATH=src pytest tests/storage/test_file_based_storage.py -v
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, date, timedelta
import sys
import random

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from storage.time_series_file_manager import (
    TimeSeriesFileManager,
    TimeSeriesQueryEngine,
    MinuteRecord,
    FileMetadata
)
from storage.dual_write_manager import (
    DualWriteTimeSeriesManager,
    DualWriteConfig,
    WriteMode,
    ReadMode
)

@pytest.fixture
def temp_storage_dir():
    """Create temporary directory for storage tests"""
    temp_dir = tempfile.mkdtemp(prefix="timeseries_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.fixture
def sample_records():
    """Generate sample minute records for testing"""
    records = []
    base_time = datetime(2024, 6, 15, 9, 30)  # Market open
    base_price = 100.0

    for i in range(100):  # 100 minutes of data
        timestamp = base_time + timedelta(minutes=i)
        price_change = random.uniform(-0.5, 0.5)
        current_price = base_price + price_change * i * 0.1

        record = MinuteRecord(
            timestamp=timestamp,
            open_price=current_price,
            high_price=current_price + random.uniform(0, 0.5),
            low_price=current_price - random.uniform(0, 0.5),
            close_price=current_price + random.uniform(-0.25, 0.25),
            volume=random.randint(1000, 10000)
        )
        records.append(record)

    return records

@pytest.fixture
def file_manager(temp_storage_dir):
    """Create file manager instance with temp directory"""
    return TimeSeriesFileManager(temp_storage_dir)

@pytest.fixture
def query_engine(file_manager):
    """Create query engine instance"""
    return TimeSeriesQueryEngine(file_manager)

class TestMinuteRecord:
    """Test MinuteRecord binary serialization"""

    def test_binary_serialization_roundtrip(self):
        """Test that records can be serialized and deserialized correctly"""
        original = MinuteRecord(
            timestamp=datetime(2024, 6, 15, 10, 30),
            open_price=100.50,
            high_price=101.25,
            low_price=99.75,
            close_price=100.80,
            volume=5000
        )

        # Serialize to binary
        binary_data = original.to_binary()
        assert len(binary_data) == 32  # Expected size

        # Deserialize back
        restored = MinuteRecord.from_binary(binary_data)

        # Compare values (timestamps should match exactly)
        assert restored.timestamp == original.timestamp
        assert abs(restored.open_price - original.open_price) < 0.01
        assert abs(restored.high_price - original.high_price) < 0.01
        assert abs(restored.low_price - original.low_price) < 0.01
        assert abs(restored.close_price - original.close_price) < 0.01
        assert restored.volume == original.volume

    def test_binary_format_consistency(self):
        """Test that binary format is consistent across multiple records"""
        records = []
        for i in range(10):
            record = MinuteRecord(
                timestamp=datetime(2024, 1, 1, 9, 30 + i),
                open_price=100.0 + i,
                high_price=101.0 + i,
                low_price=99.0 + i,
                close_price=100.5 + i,
                volume=1000 + i * 100
            )
            records.append(record)

        # Serialize all records
        binary_data = b''.join(record.to_binary() for record in records)
        assert len(binary_data) == 32 * 10  # 10 records * 32 bytes each

        # Deserialize and verify
        for i in range(10):
            offset = i * 32
            record_binary = binary_data[offset:offset + 32]
            restored = MinuteRecord.from_binary(record_binary)

            assert restored.timestamp == records[i].timestamp
            assert abs(restored.open_price - records[i].open_price) < 0.01

class TestFileMetadata:
    """Test FileMetadata binary serialization"""

    def test_metadata_serialization_roundtrip(self):
        """Test metadata serialization and deserialization"""
        original = FileMetadata(
            instrument_id=12345,
            year=2024,
            month=6,
            record_count=1000,
            first_timestamp=datetime(2024, 6, 1, 9, 30),
            last_timestamp=datetime(2024, 6, 30, 16, 0),
            file_version=1
        )

        # Serialize to binary
        binary_data = original.to_binary()
        assert len(binary_data) == 48  # Expected size

        # Deserialize back
        restored = FileMetadata.from_binary(binary_data)

        # Compare values
        assert restored.instrument_id == original.instrument_id
        assert restored.year == original.year
        assert restored.month == original.month
        assert restored.record_count == original.record_count
        assert restored.first_timestamp == original.first_timestamp
        assert restored.last_timestamp == original.last_timestamp
        assert restored.file_version == original.file_version

class TestTimeSeriesFileManager:
    """Test TimeSeriesFileManager functionality"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_file_path_generation(self, file_manager):
        """Test file path generation with sharding"""
        instrument_id = 12345
        year = 2024
        month = 6

        file_path = file_manager.get_file_path(instrument_id, year, month)

        # Check path structure
        expected_shard = instrument_id % 100  # 45
        expected_path = Path(file_manager.base_path) / str(year) / f"{month:02d}" / f"{expected_shard:02d}" / f"{instrument_id}_{year}_{month:02d}.record"

        assert file_path == expected_path
        assert file_path.parent.exists()  # Directory should be created

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_write_and_read_monthly_file(self, file_manager, sample_records):
        """Test writing and reading monthly files"""
        instrument_id = 12345
        year = 2024
        month = 6

        # Write file
        success = await file_manager.write_monthly_file(instrument_id, year, month, sample_records)
        assert success

        # Check that compressed file exists
        file_path = file_manager.get_file_path(instrument_id, year, month)
        compressed_file = file_path.with_suffix('.record.gz')
        assert compressed_file.exists()
        assert compressed_file.stat().st_size > 0

        # Read file back
        read_records = await file_manager.read_monthly_file(instrument_id, year, month)

        # Verify record count
        assert len(read_records) == len(sample_records)

        # Verify first and last records
        assert read_records[0].timestamp == sample_records[0].timestamp
        assert read_records[-1].timestamp == sample_records[-1].timestamp

        # Verify data integrity for a few sample records
        for i in [0, len(sample_records)//2, len(sample_records)-1]:
            original = sample_records[i]
            restored = read_records[i]

            assert restored.timestamp == original.timestamp
            assert abs(restored.open_price - original.open_price) < 0.01
            assert abs(restored.close_price - original.close_price) < 0.01
            assert restored.volume == original.volume

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_read_with_time_filtering(self, file_manager, sample_records):
        """Test reading files with time range filtering"""
        instrument_id = 12345
        year = 2024
        month = 6

        # Write file
        await file_manager.write_monthly_file(instrument_id, year, month, sample_records)

        # Read with time filtering (middle 50 records)
        start_time = sample_records[25].timestamp
        end_time = sample_records[75].timestamp

        filtered_records = await file_manager.read_monthly_file(
            instrument_id, year, month, start_time, end_time
        )

        # Should get approximately 50 records (inclusive range)
        assert 45 <= len(filtered_records) <= 55

        # All records should be within the time range
        for record in filtered_records:
            assert start_time <= record.timestamp <= end_time

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_file_metadata_reading(self, file_manager, sample_records):
        """Test reading file metadata"""
        instrument_id = 12345
        year = 2024
        month = 6

        # Write file
        await file_manager.write_monthly_file(instrument_id, year, month, sample_records)

        # Read metadata
        metadata = await file_manager.get_file_metadata(instrument_id, year, month)

        assert metadata is not None
        assert metadata.instrument_id == instrument_id
        assert metadata.year == year
        assert metadata.month == month
        assert metadata.record_count == len(sample_records)
        assert metadata.first_timestamp == sample_records[0].timestamp
        assert metadata.last_timestamp == sample_records[-1].timestamp

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_list_available_data(self, file_manager, sample_records):
        """Test listing available data for an instrument"""
        instrument_id = 12345

        # Write data for multiple months
        months_data = [
            (2024, 5, sample_records[:30]),
            (2024, 6, sample_records[30:70]),
            (2024, 7, sample_records[70:])
        ]

        for year, month, records in months_data:
            await file_manager.write_monthly_file(instrument_id, year, month, records)

        # List available data
        available = await file_manager.list_available_data(instrument_id, 2024, 2024)

        expected_months = [(2024, 5), (2024, 6), (2024, 7)]
        assert set(available) == set(expected_months)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_storage_statistics(self, file_manager, sample_records):
        """Test storage statistics calculation"""
        # Write data for multiple instruments and months
        instruments = [12345, 12346, 12347]
        months = [(2024, 5), (2024, 6)]

        for instrument_id in instruments:
            for year, month in months:
                # Use subset of records for each file
                records = sample_records[:(10 + instrument_id % 20)]
                await file_manager.write_monthly_file(instrument_id, year, month, records)

        # Get storage statistics
        stats = await file_manager.get_storage_stats()

        assert stats['total_files'] == len(instruments) * len(months)  # 6 files
        assert stats['total_size_bytes'] > 0
        assert stats['years_covered'] == 1  # 2024
        assert stats['instruments_count'] == len(instruments)
        assert 0 < stats['compression_ratio'] < 1  # Should have some compression

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_empty_records_handling(self, file_manager):
        """Test handling of empty record lists"""
        instrument_id = 12345
        year = 2024
        month = 6

        # Write empty records
        success = await file_manager.write_monthly_file(instrument_id, year, month, [])
        assert not success  # Should return False for empty records

        # Read non-existent file
        records = await file_manager.read_monthly_file(instrument_id, year, month)
        assert len(records) == 0

class TestTimeSeriesQueryEngine:
    """Test TimeSeriesQueryEngine functionality"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_query_single_instrument_single_month(self, query_engine, sample_records):
        """Test querying single instrument for single month"""
        instrument_id = 12345

        # Write data
        await query_engine.file_manager.write_monthly_file(instrument_id, 2024, 6, sample_records)

        # Query data
        start_time = datetime(2024, 6, 1)
        end_time = datetime(2024, 6, 30, 23, 59, 59)

        results = await query_engine.query_range([instrument_id], start_time, end_time)

        assert instrument_id in results
        assert len(results[instrument_id]) == len(sample_records)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_query_multiple_instruments(self, query_engine, sample_records):
        """Test querying multiple instruments"""
        instruments = [12345, 12346, 12347]

        # Write data for each instrument
        for instrument_id in instruments:
            # Use different subsets of records
            records = sample_records[:(50 + instrument_id % 10)]
            await query_engine.file_manager.write_monthly_file(instrument_id, 2024, 6, records)

        # Query all instruments
        start_time = datetime(2024, 6, 1)
        end_time = datetime(2024, 6, 30, 23, 59, 59)

        results = await query_engine.query_range(instruments, start_time, end_time)

        # Should have results for all instruments
        for instrument_id in instruments:
            assert instrument_id in results
            assert len(results[instrument_id]) > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_query_across_multiple_months(self, query_engine, sample_records):
        """Test querying across multiple months"""
        instrument_id = 12345

        # Write data for multiple months
        months_data = [
            (2024, 5, sample_records[:30]),
            (2024, 6, sample_records[30:70]),
            (2024, 7, sample_records[70:])
        ]

        for year, month, records in months_data:
            await query_engine.file_manager.write_monthly_file(instrument_id, year, month, records)

        # Query across all months
        start_time = datetime(2024, 5, 1)
        end_time = datetime(2024, 7, 31, 23, 59, 59)

        results = await query_engine.query_range([instrument_id], start_time, end_time)

        assert instrument_id in results
        assert len(results[instrument_id]) == len(sample_records)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_daily_ohlc_aggregation(self, query_engine):
        """Test daily OHLC aggregation from minute data"""
        instrument_id = 12345

        # Create minute data spanning multiple days
        records = []
        base_date = date(2024, 6, 15)

        for day_offset in range(3):  # 3 days
            current_date = base_date + timedelta(days=day_offset)

            for minute_offset in range(390):  # 6.5 hours * 60 minutes
                timestamp = datetime.combine(current_date, datetime.min.time()) + timedelta(minutes=9*60 + 30 + minute_offset)

                # Create realistic OHLC data
                base_price = 100.0 + day_offset * 2 + minute_offset * 0.01

                record = MinuteRecord(
                    timestamp=timestamp,
                    open_price=base_price,
                    high_price=base_price + random.uniform(0, 0.5),
                    low_price=base_price - random.uniform(0, 0.5),
                    close_price=base_price + random.uniform(-0.25, 0.25),
                    volume=random.randint(100, 1000)
                )
                records.append(record)

        # Write data
        await query_engine.file_manager.write_monthly_file(instrument_id, 2024, 6, records)

        # Get daily OHLC
        start_date = date(2024, 6, 15)
        end_date = date(2024, 6, 17)

        daily_ohlc = await query_engine.get_daily_ohlc(instrument_id, start_date, end_date)

        assert len(daily_ohlc) == 3  # 3 days

        for day_data in daily_ohlc:
            assert 'date' in day_data
            assert 'open' in day_data
            assert 'high' in day_data
            assert 'low' in day_data
            assert 'close' in day_data
            assert 'volume' in day_data
            assert 'record_count' in day_data

            # Validate OHLC relationships
            assert day_data['low'] <= day_data['open']
            assert day_data['low'] <= day_data['close']
            assert day_data['high'] >= day_data['open']
            assert day_data['high'] >= day_data['close']
            assert day_data['volume'] > 0
            assert day_data['record_count'] == 390  # Minutes per trading day

class TestDualWriteTimeSeriesManager:
    """Test DualWriteTimeSeriesManager functionality"""

    @pytest.fixture
    def dual_write_config(self, temp_storage_dir):
        """Create dual-write configuration for testing"""
        return DualWriteConfig(
            file_base_path=temp_storage_dir,
            write_mode=WriteMode.FILES_ONLY,  # Use files only for testing
            read_mode=ReadMode.FILES_ONLY,
            enable_metrics=True,
            log_write_stats=False  # Reduce log noise in tests
        )

    @pytest.fixture
    def dual_write_manager(self, dual_write_config):
        """Create dual-write manager instance"""
        return DualWriteTimeSeriesManager(dual_write_config)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_write_minute_data_files_only(self, dual_write_manager, sample_records):
        """Test writing minute data in files-only mode"""
        instrument_id = 12345

        result = await dual_write_manager.write_minute_data(instrument_id, sample_records)

        assert result.success
        assert result.file_success
        assert not result.db_success  # DB not used in files-only mode
        assert result.records_written == len(sample_records)
        assert result.write_time_seconds > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_read_minute_data_files_only(self, dual_write_manager, sample_records):
        """Test reading minute data in files-only mode"""
        instrument_id = 12345

        # Write data first
        await dual_write_manager.write_minute_data(instrument_id, sample_records)

        # Read data back
        start_time = sample_records[0].timestamp - timedelta(minutes=1)
        end_time = sample_records[-1].timestamp + timedelta(minutes=1)

        read_data = await dual_write_manager.read_minute_data([instrument_id], start_time, end_time)

        assert instrument_id in read_data
        assert len(read_data[instrument_id]) == len(sample_records)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_metrics_collection(self, dual_write_manager, sample_records):
        """Test metrics collection during writes"""
        instrument_id = 12345

        # Perform multiple writes
        for i in range(3):
            records_subset = sample_records[i*10:(i+1)*10]
            await dual_write_manager.write_minute_data(instrument_id, records_subset)

        # Get metrics
        metrics = dual_write_manager.get_metrics_summary()

        assert metrics['total_writes'] == 3
        assert metrics['successful_writes'] == 3
        assert metrics['success_rate'] == 1.0
        assert metrics['total_records'] == 30  # 3 writes * 10 records each
        assert metrics['avg_write_time'] > 0
        assert metrics['files']['writes'] == 3
        assert metrics['files']['failures'] == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_empty_records_handling(self, dual_write_manager):
        """Test handling of empty record lists"""
        instrument_id = 12345

        result = await dual_write_manager.write_minute_data(instrument_id, [])

        assert result.success
        assert result.records_written == 0

class TestPerformanceBenchmarks:
    """Performance benchmarks for the file-based system"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_write_performance_benchmark(self, file_manager):
        """Benchmark write performance"""
        instrument_id = 12345

        # Generate larger dataset for benchmarking
        records = []
        base_time = datetime(2024, 6, 1, 9, 30)

        for i in range(10000):  # 10k records (about 1 week of minute data)
            record = MinuteRecord(
                timestamp=base_time + timedelta(minutes=i),
                open_price=100.0 + random.uniform(-5, 5),
                high_price=100.0 + random.uniform(0, 10),
                low_price=100.0 - random.uniform(0, 10),
                close_price=100.0 + random.uniform(-5, 5),
                volume=random.randint(1000, 50000)
            )
            records.append(record)

        # Benchmark write
        start_time = datetime.now()
        success = await file_manager.write_monthly_file(instrument_id, 2024, 6, records)
        write_time = (datetime.now() - start_time).total_seconds()

        assert success

        # Calculate performance metrics
        records_per_second = len(records) / write_time
        mb_per_second = (len(records) * 32) / (1024 * 1024) / write_time

        print(f"\nWrite Performance:")
        print(f"  Records: {len(records):,}")
        print(f"  Time: {write_time:.2f}s")
        print(f"  Records/sec: {records_per_second:,.0f}")
        print(f"  MB/sec: {mb_per_second:.1f}")

        # Basic performance assertions
        assert records_per_second > 1000  # Should write at least 1k records/sec
        assert write_time < 30  # Should complete within 30 seconds

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_read_performance_benchmark(self, file_manager):
        """Benchmark read performance"""
        instrument_id = 12345

        # First create test data (reuse from write benchmark)
        records = []
        base_time = datetime(2024, 6, 1, 9, 30)

        for i in range(10000):
            record = MinuteRecord(
                timestamp=base_time + timedelta(minutes=i),
                open_price=100.0 + random.uniform(-5, 5),
                high_price=100.0 + random.uniform(0, 10),
                low_price=100.0 - random.uniform(0, 10),
                close_price=100.0 + random.uniform(-5, 5),
                volume=random.randint(1000, 50000)
            )
            records.append(record)

        await file_manager.write_monthly_file(instrument_id, 2024, 6, records)

        # Benchmark read
        start_time = datetime.now()
        read_records = await file_manager.read_monthly_file(instrument_id, 2024, 6)
        read_time = (datetime.now() - start_time).total_seconds()

        assert len(read_records) == len(records)

        # Calculate performance metrics
        records_per_second = len(read_records) / read_time
        mb_per_second = (len(read_records) * 32) / (1024 * 1024) / read_time

        print(f"\nRead Performance:")
        print(f"  Records: {len(read_records):,}")
        print(f"  Time: {read_time:.2f}s")
        print(f"  Records/sec: {records_per_second:,.0f}")
        print(f"  MB/sec: {mb_per_second:.1f}")

        # Basic performance assertions
        assert records_per_second > 5000  # Should read at least 5k records/sec
        assert read_time < 10  # Should complete within 10 seconds

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_compression_efficiency(self, file_manager):
        """Test compression efficiency"""
        instrument_id = 12345

        # Create records with realistic patterns
        records = []
        base_time = datetime(2024, 6, 1, 9, 30)
        base_price = 100.0

        for i in range(5000):
            # Create somewhat realistic price patterns
            price_trend = base_price + (i * 0.001)
            noise = random.uniform(-0.1, 0.1)

            record = MinuteRecord(
                timestamp=base_time + timedelta(minutes=i),
                open_price=price_trend + noise,
                high_price=price_trend + noise + random.uniform(0, 0.05),
                low_price=price_trend + noise - random.uniform(0, 0.05),
                close_price=price_trend + noise + random.uniform(-0.02, 0.02),
                volume=random.randint(1000, 10000)
            )
            records.append(record)

        # Write and measure file sizes
        await file_manager.write_monthly_file(instrument_id, 2024, 6, records)

        file_path = file_manager.get_file_path(instrument_id, 2024, 6)
        compressed_file = file_path.with_suffix('.record.gz')

        compressed_size = compressed_file.stat().st_size
        uncompressed_size = (48 + len(records) * 32)  # metadata + records
        compression_ratio = compressed_size / uncompressed_size

        print(f"\nCompression Efficiency:")
        print(f"  Records: {len(records):,}")
        print(f"  Uncompressed: {uncompressed_size:,} bytes")
        print(f"  Compressed: {compressed_size:,} bytes")
        print(f"  Compression ratio: {compression_ratio:.1%}")
        print(f"  Space saved: {(1-compression_ratio)*100:.1f}%")

        # Assert reasonable compression
        assert compression_ratio < 0.8  # Should achieve at least 20% compression
        assert compression_ratio > 0.2  # But not impossibly good compression

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_integration_scenario():
    """Integration test simulating real-world usage scenario"""

    with tempfile.TemporaryDirectory(prefix="integration_test_") as temp_dir:
        # Initialize components
        file_manager = TimeSeriesFileManager(temp_dir)
        query_engine = TimeSeriesQueryEngine(file_manager)

        # Simulate data for multiple instruments and months
        instruments = [12345, 12346, 12347]
        months = [(2024, 4), (2024, 5), (2024, 6)]

        total_records_written = 0

        # Phase 1: Write historical data
        print("\n🔄 Phase 1: Writing historical data...")
        for instrument_id in instruments:
            for year, month in months:
                # Generate month's worth of minute data
                records = []
                base_time = datetime(year, month, 1, 9, 30)

                # Approximate trading minutes in a month (22 trading days * 390 minutes)
                num_records = 22 * 390

                for i in range(num_records):
                    timestamp = base_time + timedelta(minutes=i*2)  # Every 2 minutes for testing

                    record = MinuteRecord(
                        timestamp=timestamp,
                        open_price=100.0 + instrument_id * 0.01 + random.uniform(-2, 2),
                        high_price=100.0 + instrument_id * 0.01 + random.uniform(0, 3),
                        low_price=100.0 + instrument_id * 0.01 - random.uniform(0, 3),
                        close_price=100.0 + instrument_id * 0.01 + random.uniform(-2, 2),
                        volume=random.randint(1000, 20000)
                    )
                    records.append(record)

                success = await file_manager.write_monthly_file(instrument_id, year, month, records)
                assert success
                total_records_written += len(records)

                print(f"  ✅ {instrument_id} {year}-{month:02d}: {len(records):,} records")

        print(f"  📊 Total records written: {total_records_written:,}")

        # Phase 2: Query data in various patterns
        print("\n🔍 Phase 2: Testing query patterns...")

        # Single instrument, single month
        start_time = datetime(2024, 5, 1)
        end_time = datetime(2024, 5, 31, 23, 59, 59)

        results = await query_engine.query_range([12345], start_time, end_time)
        print(f"  ✅ Single instrument query: {len(results[12345]):,} records")

        # Multiple instruments, single month
        results = await query_engine.query_range(instruments, start_time, end_time)
        total_read = sum(len(records) for records in results.values())
        print(f"  ✅ Multi-instrument query: {total_read:,} records")

        # Single instrument, multi-month
        start_time = datetime(2024, 4, 1)
        end_time = datetime(2024, 6, 30, 23, 59, 59)

        results = await query_engine.query_range([12345], start_time, end_time)
        print(f"  ✅ Multi-month query: {len(results[12345]):,} records")

        # Daily OHLC aggregation
        daily_ohlc = await query_engine.get_daily_ohlc(
            12345,
            date(2024, 5, 1),
            date(2024, 5, 31)
        )
        print(f"  ✅ Daily OHLC aggregation: {len(daily_ohlc)} days")

        # Phase 3: Validate storage statistics
        print("\n📊 Phase 3: Storage statistics...")
        stats = await file_manager.get_storage_stats()

        expected_files = len(instruments) * len(months)
        assert stats['total_files'] == expected_files

        print(f"  📁 Total files: {stats['total_files']}")
        print(f"  💾 Total size: {stats['total_size_bytes'] / (1024**2):.1f} MB")
        print(f"  🗜️ Compression ratio: {stats['compression_ratio']:.1%}")
        print(f"  📅 Years covered: {stats['years_covered']}")
        print(f"  🎯 Instruments: {stats['instruments_count']}")

        print("\n✅ Integration test completed successfully!")

if __name__ == "__main__":
    # Run integration test standalone
    asyncio.run(test_integration_scenario())