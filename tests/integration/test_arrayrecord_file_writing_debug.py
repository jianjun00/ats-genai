"""
Focused Test Cases for ArrayRecord File Writing Debug

Specifically targets the AAPL ArrayRecord file writing failure.
Tests the exact conditions that caused database success but zero file size.
"""

import pytest
import asyncio
import os
import tempfile
import struct
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch, MagicMock

# Import components for testing
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.storage.sequence_storage_manager import ArrayRecordStorageManager
from domains.ml.services.training_data.schemas.binary_record_schema import SchemaTemplates


class TestArrayRecordFileWritingDebug:
    """Debug ArrayRecord file writing failures step by step."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test outputs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def mock_callback_with_writers(self, temp_dir):
        """Create callback with initialized ArrayRecord writers."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=None,
            storage_format='arrayrecord',
            output_dir=temp_dir,
            start_date='2025-09-01',
            end_date='2025-09-01'
        )
        callback.dataset_id = "test_debug_dataset"
        return callback

    def test_arrayrecord_import_availability(self):
        """
        TEST 1: Verify ArrayRecord module can be imported.
        
        This is the most likely root cause of file writing failures.
        """
        try:
            import array_record.python.array_record_module as array_record
            assert array_record is not None
            
            # Test that ArrayRecordWriter class exists
            assert hasattr(array_record, 'ArrayRecordWriter')
            assert hasattr(array_record, 'ArrayRecordReader')
            
            print(f"✅ ArrayRecord module imported successfully")
            print(f"   Writer class: {array_record.ArrayRecordWriter}")
            print(f"   Reader class: {array_record.ArrayRecordReader}")
            
        except ImportError as e:
            pytest.fail(f"❌ CRITICAL: ArrayRecord not available - this is the root cause: {e}")

    def test_arrayrecord_writer_creation(self, temp_dir):
        """
        TEST 2: Test ArrayRecord writer creation with exact parameters.
        
        Tests the exact writer creation used in the callback.
        """
        try:
            import array_record.python.array_record_module as array_record
        except ImportError:
            pytest.skip("ArrayRecord not available")

        # Test file path creation
        dataset_id = "test_dataset_123"
        symbol = "AAPL"
        timeframe = "5m"
        
        # Replicate exact directory structure from callback
        symbol_datetime_str = f"{symbol}_20250901_000000_20250901_235959"
        dataset_dir = Path(temp_dir) / dataset_id / symbol_datetime_str
        timeframe_dir = dataset_dir / timeframe
        timeframe_dir.mkdir(parents=True, exist_ok=True)
        
        arrayrecord_file = timeframe_dir / f"{symbol_datetime_str}.arrayrecord"
        
        print(f"🔍 Testing ArrayRecord writer creation:")
        print(f"   File path: {arrayrecord_file}")
        print(f"   Directory exists: {timeframe_dir.exists()}")
        
        # Test writer creation with exact parameters from callback
        try:
            writer = array_record.ArrayRecordWriter(str(arrayrecord_file), 'group_size:1')
            assert writer is not None
            print(f"✅ ArrayRecord writer created successfully")
            
            # Immediately test if we can close it
            writer.close()
            print(f"✅ ArrayRecord writer closed successfully")
            
            # Check if file was created
            file_exists = arrayrecord_file.exists()
            file_size = arrayrecord_file.stat().st_size if file_exists else 0
            
            print(f"   File exists after close: {file_exists}")
            print(f"   File size after close: {file_size} bytes")
            
            if file_size == 0:
                pytest.fail("❌ CRITICAL: ArrayRecord file created but has zero size after close")
                
        except Exception as e:
            pytest.fail(f"❌ CRITICAL: ArrayRecord writer creation failed: {e}")

    def test_arrayrecord_binary_data_writing(self, temp_dir):
        """
        TEST 3: Test writing binary data to ArrayRecord files.
        
        Tests the exact binary data writing process used in the callback.
        """
        try:
            import array_record.python.array_record_module as array_record
        except ImportError:
            pytest.skip("ArrayRecord not available")

        test_file = os.path.join(temp_dir, "binary_write_test.arrayrecord")
        
        # Create test binary data similar to what the callback would generate
        test_records = []
        
        # Simulate OHLCV data record
        timestamp = datetime.now().timestamp()
        symbol = "AAPL"
        ohlcv_data = {
            'open': 150.25,
            'high': 152.75, 
            'low': 149.50,
            'close': 151.80,
            'volume': 1000000.0
        }
        
        # Create binary record similar to binary_record_schema.pack_interval
        symbol_bytes = symbol.encode('utf-8')
        symbol_len = len(symbol_bytes)
        
        # Pack as binary (mimicking what the schema would do)
        binary_record = struct.pack(
            f'>dI{symbol_len}sfffff',  # Big-endian: double, uint32, string, 5 floats
            timestamp,                 # timestamp 
            symbol_len,               # symbol length
            symbol_bytes,             # symbol
            float(ohlcv_data['open']),
            float(ohlcv_data['high']), 
            float(ohlcv_data['low']),
            float(ohlcv_data['close']),
            float(ohlcv_data['volume'])
        )
        
        test_records.append(binary_record)
        
        print(f"🔍 Testing binary data writing:")
        print(f"   Test file: {test_file}")
        print(f"   Binary record size: {len(binary_record)} bytes")
        print(f"   Record count: {len(test_records)}")
        
        # Test writing process
        try:
            writer = array_record.ArrayRecordWriter(test_file, 'group_size:1')
            
            for i, record in enumerate(test_records):
                print(f"   Writing record {i+1}: {len(record)} bytes")
                writer.write(record)
            
            writer.close()
            print(f"✅ All records written and writer closed")
            
            # Verify file after writing
            assert os.path.exists(test_file)
            file_size = os.path.getsize(test_file)
            print(f"   Final file size: {file_size} bytes")
            
            if file_size == 0:
                pytest.fail("❌ CRITICAL: File written but has zero size - data not persisted")
            
            # Test reading back the data
            reader = array_record.ArrayRecordReader(test_file)
            read_records = list(reader)
            reader.close()
            
            print(f"   Records read back: {len(read_records)}")
            
            if len(read_records) != len(test_records):
                pytest.fail(f"❌ CRITICAL: Record count mismatch - wrote {len(test_records)}, read {len(read_records)}")
            
            print(f"✅ Binary data writing test passed")
            
        except Exception as e:
            pytest.fail(f"❌ CRITICAL: Binary data writing failed: {e}")

    @pytest.mark.asyncio
    async def test_callback_writer_initialization_exact_replication(self, temp_dir):
        """
        TEST 4: Replicate exact callback writer initialization process.
        
        This replicates the exact _initialize_dataset_structure method
        that failed for AAPL generation.
        """
        # Skip if ArrayRecord not available
        try:
            import array_record.python.array_record_module as array_record
        except ImportError:
            pytest.skip("ArrayRecord not available")

        # Create callback exactly as done in the runner
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=None,
            storage_format='arrayrecord',
            output_dir=temp_dir,
            start_date='2025-09-01',
            end_date='2025-09-01'
        )
        
        # Set dataset_id as done in runner
        dataset_id = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        callback.dataset_id = dataset_id
        
        print(f"🔍 Testing exact callback initialization:")
        print(f"   Dataset ID: {dataset_id}")
        print(f"   Symbols: {callback.symbols}")
        print(f"   Output dir: {callback.output_dir}")
        
        # Test the exact initialization process
        try:
            # This calls the exact method that failed for AAPL
            await callback._initialize_dataset_structure()
            
            print(f"✅ Dataset structure initialized")
            print(f"   Writers created: {len(callback.array_record_writers)}")
            
            # Verify writers were created for all timeframes
            timeframes = ['5m', '15m', '1h', '1d']
            expected_writers = len(callback.symbols) * len(timeframes)
            actual_writers = len(callback.array_record_writers)
            
            print(f"   Expected writers: {expected_writers}")
            print(f"   Actual writers: {actual_writers}")
            
            if actual_writers != expected_writers:
                pytest.fail(f"❌ Writer count mismatch: expected {expected_writers}, got {actual_writers}")
            
            # Test that we can write to each writer
            test_data = b"test_data_12345"
            successful_writes = 0
            
            for writer_key, writer in callback.array_record_writers.items():
                try:
                    writer.write(test_data)
                    successful_writes += 1
                    print(f"   ✅ Write successful to {writer_key}")
                except Exception as e:
                    print(f"   ❌ Write failed to {writer_key}: {e}")
            
            print(f"   Successful writes: {successful_writes}/{actual_writers}")
            
            # Close all writers (as done in handleEnd)
            for writer_key, writer in callback.array_record_writers.items():
                try:
                    writer.close()
                    print(f"   ✅ Closed writer {writer_key}")
                except Exception as e:
                    print(f"   ❌ Failed to close writer {writer_key}: {e}")
            
            # Verify files were actually created and have content
            dataset_dir = callback.output_dir / dataset_id
            created_files = []
            zero_size_files = []
            
            for arrayrecord_file in dataset_dir.rglob("*.arrayrecord"):
                created_files.append(str(arrayrecord_file))
                file_size = arrayrecord_file.stat().st_size
                print(f"   File: {arrayrecord_file.name}, Size: {file_size} bytes")
                
                if file_size == 0:
                    zero_size_files.append(str(arrayrecord_file))
            
            print(f"   Total files created: {len(created_files)}")
            print(f"   Zero-size files: {len(zero_size_files)}")
            
            if len(zero_size_files) > 0:
                pytest.fail(f"❌ CRITICAL: {len(zero_size_files)} files have zero size: {zero_size_files}")
            
            if len(created_files) != expected_writers:
                pytest.fail(f"❌ CRITICAL: Expected {expected_writers} files, created {len(created_files)}")
                
            print(f"✅ Callback initialization test passed - all files created with content")
            
        except Exception as e:
            import traceback
            pytest.fail(f"❌ CRITICAL: Callback initialization failed: {e}\nTraceback: {traceback.format_exc()}")

    def test_writer_persistence_and_flushing(self, temp_dir):
        """
        TEST 5: Test writer persistence and data flushing.
        
        Tests whether ArrayRecord writers properly flush data to disk.
        """
        try:
            import array_record.python.array_record_module as array_record
        except ImportError:
            pytest.skip("ArrayRecord not available")

        test_file = os.path.join(temp_dir, "persistence_test.arrayrecord")
        
        # Test data persistence without explicit close
        print(f"🔍 Testing writer persistence and flushing:")
        
        writer = array_record.ArrayRecordWriter(test_file, 'group_size:1')
        
        # Write data
        test_data = b"persistence_test_data_12345"
        writer.write(test_data)
        
        # Check file size before close
        file_size_before = os.path.getsize(test_file) if os.path.exists(test_file) else 0
        print(f"   File size before close: {file_size_before} bytes")
        
        # Close writer
        writer.close()
        
        # Check file size after close
        file_size_after = os.path.getsize(test_file) if os.path.exists(test_file) else 0
        print(f"   File size after close: {file_size_after} bytes")
        
        if file_size_after == 0:
            pytest.fail("❌ CRITICAL: Data not persisted to disk even after close()")
        
        if file_size_before == 0 and file_size_after > 0:
            print("   ⚠️ Data only written after close() - writers must be closed to persist data")
        
        print(f"✅ Writer persistence test passed")

    @pytest.mark.asyncio 
    async def test_streaming_write_pattern(self, temp_dir):
        """
        TEST 6: Test the streaming write pattern used in the callback.
        
        This tests the pattern where writers are created once and data is streamed
        over multiple intervals, which is how the callback works.
        """
        try:
            import array_record.python.array_record_module as array_record
        except ImportError:
            pytest.skip("ArrayRecord not available")

        print(f"🔍 Testing streaming write pattern:")
        
        # Create writers (as done in _initialize_dataset_structure)
        writers = {}
        timeframes = ['5m', '15m', '1h', '1d']
        symbol = 'AAPL'
        
        for timeframe in timeframes:
            file_path = os.path.join(temp_dir, f"{symbol}_{timeframe}_streaming.arrayrecord")
            writer = array_record.ArrayRecordWriter(file_path, 'group_size:1')
            writers[f"{symbol}_{timeframe}"] = writer
            print(f"   Created writer for {symbol}_{timeframe}")
        
        # Simulate multiple intervals (as done in _stream_intervals_to_writers)
        intervals_to_simulate = 5
        
        for interval in range(intervals_to_simulate):
            print(f"   Processing interval {interval + 1}...")
            
            # Create test data for this interval
            timestamp = datetime.now().timestamp() + interval * 60
            test_data = struct.pack('>df', timestamp, 150.0 + interval)  # Simple timestamp + price
            
            # Write to all timeframes
            for writer_key, writer in writers.items():
                try:
                    writer.write(test_data)
                    print(f"     ✅ Wrote to {writer_key}")
                except Exception as e:
                    print(f"     ❌ Failed to write to {writer_key}: {e}")
        
        print(f"   Completed {intervals_to_simulate} intervals")
        
        # Close all writers (as done in handleEnd)
        file_sizes = {}
        for writer_key, writer in writers.items():
            writer.close()
            
            # Check final file size
            file_path = os.path.join(temp_dir, f"{symbol}_{writer_key.split('_')[1]}_streaming.arrayrecord")
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            file_sizes[writer_key] = file_size
            
            print(f"   {writer_key}: {file_size} bytes")
        
        # Verify all files have content
        zero_size_files = [k for k, v in file_sizes.items() if v == 0]
        
        if zero_size_files:
            pytest.fail(f"❌ CRITICAL: Streaming write failed - zero size files: {zero_size_files}")
        
        print(f"✅ Streaming write pattern test passed - all files have content")

    def test_binary_record_schema_integration(self, temp_dir):
        """
        TEST 7: Test binary record schema integration.
        
        Tests whether the binary_record_schema.pack_interval method
        produces valid data for ArrayRecord writing.
        """
        try:
            import array_record.python.array_record_module as array_record
        except ImportError:
            pytest.skip("ArrayRecord not available")

        # Test schema creation
        try:
            schema = SchemaTemplates.auto_detect()
            assert schema is not None
            print(f"✅ Binary record schema created")
        except Exception as e:
            pytest.skip(f"Binary record schema not available: {e}")

        # Test data packing
        symbol = "AAPL"
        test_interval = {
            'timestamp': datetime.now(),
            'open': 150.25,
            'high': 152.75,
            'low': 149.50,
            'close': 151.80,
            'volume': 1000000.0
        }
        
        print(f"🔍 Testing binary record schema integration:")
        print(f"   Test interval: {test_interval}")
        
        try:
            binary_record = schema.pack_interval(symbol, test_interval)
            assert isinstance(binary_record, bytes)
            assert len(binary_record) > 0
            
            print(f"   ✅ Binary record packed: {len(binary_record)} bytes")
            
            # Test writing the packed record
            test_file = os.path.join(temp_dir, "schema_integration_test.arrayrecord")
            writer = array_record.ArrayRecordWriter(test_file, 'group_size:1')
            writer.write(binary_record)
            writer.close()
            
            file_size = os.path.getsize(test_file)
            print(f"   ✅ Packed record written to file: {file_size} bytes")
            
            if file_size == 0:
                pytest.fail("❌ CRITICAL: Schema-packed record resulted in zero-size file")
            
        except Exception as e:
            pytest.fail(f"❌ CRITICAL: Binary record schema integration failed: {e}")

    def test_directory_structure_and_permissions(self, temp_dir):
        """
        TEST 8: Test directory structure creation and permissions.
        
        Tests whether directory creation issues could cause file writing failures.
        """
        print(f"🔍 Testing directory structure and permissions:")
        
        # Replicate exact directory structure from callback
        dataset_id = "test_permission_check"
        symbol = "AAPL"
        symbol_datetime_str = f"{symbol}_20250901_000000_20250901_235959"
        
        # Create nested structure
        dataset_dir = Path(temp_dir) / dataset_id / symbol_datetime_str
        
        timeframes = ['5m', '15m', '1h', '1d']
        
        for timeframe in timeframes:
            timeframe_dir = dataset_dir / timeframe
            
            print(f"   Creating directory: {timeframe_dir}")
            
            try:
                timeframe_dir.mkdir(parents=True, exist_ok=True)
                assert timeframe_dir.exists()
                print(f"   ✅ Directory created: {timeframe_dir}")
                
                # Test file creation in directory
                test_file = timeframe_dir / f"{symbol_datetime_str}.arrayrecord"
                test_file.touch()
                
                assert test_file.exists()
                print(f"   ✅ File creation test passed: {test_file.name}")
                
                # Test write permissions
                test_content = b"permission_test_data"
                with open(test_file, 'wb') as f:
                    f.write(test_content)
                
                # Verify content was written
                with open(test_file, 'rb') as f:
                    read_content = f.read()
                
                if read_content != test_content:
                    pytest.fail(f"❌ Write permission issue - content mismatch")
                
                print(f"   ✅ Write permissions verified")
                
            except Exception as e:
                pytest.fail(f"❌ CRITICAL: Directory/permission issue: {e}")

    def test_error_conditions_and_recovery(self, temp_dir):
        """
        TEST 9: Test error conditions and recovery mechanisms.
        
        Tests how the system handles various error conditions that might
        cause silent failures.
        """
        try:
            import array_record.python.array_record_module as array_record
        except ImportError:
            pytest.skip("ArrayRecord not available")

        print(f"🔍 Testing error conditions and recovery:")
        
        # Test 1: Invalid file path
        invalid_path = "/invalid/path/that/does/not/exist.arrayrecord"
        try:
            writer = array_record.ArrayRecordWriter(invalid_path, 'group_size:1')
            pytest.fail("❌ Should have failed with invalid path")
        except Exception as e:
            print(f"   ✅ Invalid path correctly rejected: {type(e).__name__}")
        
        # Test 2: Invalid writer options
        valid_file = os.path.join(temp_dir, "error_test.arrayrecord")
        try:
            writer = array_record.ArrayRecordWriter(valid_file, 'invalid_option:123')
            # If this doesn't fail, it might cause silent issues
            writer.close()
            print(f"   ⚠️ Invalid options accepted - might cause issues")
        except Exception as e:
            print(f"   ✅ Invalid options correctly rejected: {type(e).__name__}")
        
        # Test 3: Writing after close
        writer = array_record.ArrayRecordWriter(valid_file, 'group_size:1')
        writer.close()
        
        try:
            writer.write(b"test_after_close")
            pytest.fail("❌ Should not be able to write after close")
        except Exception as e:
            print(f"   ✅ Write after close correctly rejected: {type(e).__name__}")
        
        # Test 4: Double close
        writer2 = array_record.ArrayRecordWriter(valid_file, 'group_size:1')
        writer2.close()
        try:
            writer2.close()  # Should not crash
            print(f"   ✅ Double close handled gracefully")
        except Exception as e:
            print(f"   ⚠️ Double close caused error: {e}")

    @pytest.mark.asyncio
    async def test_aapl_specific_reproduction_attempt(self, temp_dir):
        """
        TEST 10: Attempt to reproduce the exact AAPL failure scenario.
        
        This test tries to reproduce the exact conditions that led to
        AAPL showing "completed" in database but zero file size.
        """
        print(f"🔍 REPRODUCING EXACT AAPL FAILURE SCENARIO:")
        
        try:
            import array_record.python.array_record_module as array_record
        except ImportError:
            pytest.fail("❌ CRITICAL: ArrayRecord not available - this explains AAPL failure")

        # Exact parameters from AAPL failure
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=None,
            storage_format='arrayrecord',
            output_dir=temp_dir,
            start_date='2025-07-01',  # Same as AAPL failure
            end_date='2025-09-10'     # Same as AAPL failure
        )
        
        callback.dataset_id = "dataset_20250910_074440"  # Same as failure case
        
        print(f"   Dataset ID: {callback.dataset_id}")
        print(f"   Symbols: {callback.symbols}")
        print(f"   Date range: {callback.start_date} to {callback.end_date}")
        
        # Step 1: Initialize dataset structure (this worked in AAPL case)
        try:
            await callback._initialize_dataset_structure()
            print(f"   ✅ Dataset structure initialized")
            print(f"   Writers created: {len(callback.array_record_writers)}")
        except Exception as e:
            pytest.fail(f"❌ Dataset structure initialization failed: {e}")
        
        # Step 2: Simulate interval processing (this might have failed silently)
        mock_examples = [
            Mock(symbol='AAPL', prediction_timestamp=datetime.now())
        ]
        
        # Mock the minute data access (this could have been the failure point)
        with patch.object(callback, '_get_current_interval_minute_data') as mock_data:
            mock_data.return_value = []  # Empty data - this might be the issue!
            
            print(f"   Testing with empty minute data (potential failure cause)...")
            
            try:
                await callback._stream_intervals_to_writers(mock_examples, datetime.now())
                print(f"   ⚠️ Streaming completed with empty data - no files written")
            except Exception as e:
                print(f"   ❌ Streaming failed with empty data: {e}")
        
        # Step 3: Test with actual data
        with patch.object(callback, '_get_current_interval_minute_data') as mock_data:
            # Provide actual minute data
            mock_data.return_value = [
                {'timestamp': datetime.now(), 'open': 150.0, 'high': 151.0, 'low': 149.0, 'close': 150.5, 'volume': 1000}
            ]
            
            print(f"   Testing with actual minute data...")
            
            try:
                await callback._stream_intervals_to_writers(mock_examples, datetime.now())
                print(f"   ✅ Streaming completed with actual data")
            except Exception as e:
                print(f"   ❌ Streaming failed even with actual data: {e}")
        
        # Step 4: Close writers and check files (critical step)
        await callback.handleEnd(Mock(), datetime.now())
        
        # Step 5: Verify final state
        dataset_dir = Path(temp_dir) / callback.dataset_id
        arrayrecord_files = list(dataset_dir.rglob("*.arrayrecord"))
        
        print(f"   ArrayRecord files found: {len(arrayrecord_files)}")
        
        zero_size_files = []
        for file_path in arrayrecord_files:
            file_size = file_path.stat().st_size
            print(f"     {file_path.name}: {file_size} bytes")
            if file_size == 0:
                zero_size_files.append(str(file_path))
        
        if zero_size_files:
            print(f"   ❌ REPRODUCED AAPL FAILURE: {len(zero_size_files)} zero-size files")
            print(f"     Zero-size files: {[Path(f).name for f in zero_size_files]}")
            
            # This is the exact failure condition from AAPL case
            assert len(zero_size_files) > 0, "Successfully reproduced AAPL failure condition"
        else:
            print(f"   ✅ Could not reproduce failure - all files have content")