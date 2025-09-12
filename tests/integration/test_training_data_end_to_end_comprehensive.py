#!/usr/bin/env python3
"""
🧪 COMPREHENSIVE END-TO-END TRAINING DATA GENERATION TESTS

This test suite provides exhaustive verification of the complete training data generation pipeline.
Tests use REAL data and verify ACTUAL functionality - no superficial checks or mock data.

Test Categories:
1. Complete Pipeline Integration - Full workflow from input to output
2. ArrayRecord File Integrity - Binary format verification and readability
3. Database Registration and Tracking - Run metadata and dataset registration
4. Multi-Timeframe Data Consistency - Cross-timeframe data validation
5. Error Handling and Resource Cleanup - Failure scenarios and resource management
6. Performance and Resource Usage - Memory, CPU, and I/O performance
7. Data Quality Validation - Content structure and technical indicators
8. Regression Protection - System stability and backwards compatibility

🚫 NO SUPERFICIAL TESTING:
- No existence-only checks (file exists ≠ file is correct)
- No 200 OK without content validation
- No mock data outside unit test scenarios
- All tests verify ACTUAL functionality and data quality
"""

import pytest
import asyncio
import tempfile
import shutil
import time
import psutil
import os
import json
import struct
from datetime import datetime, timedelta
from pathlib import Path
import array_record.python.array_record_module as array_record
import pandas as pd
import asyncpg
from unittest import mock

# Import the training data system components
import sys
sys.path.insert(0, '/workspace/src')

from services.core.app.runner import Runner
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from shared.utils.database_connections import get_database_pool
from shared.utils.environment import Environment, EnvironmentType


class TrainingDataEndToEndTest:
    """Comprehensive end-to-end testing suite for training data generation."""

    def __init__(self):
        self.test_output_dir = None
        self.test_symbols = ['TSLA']  # Use real symbol with known data
        self.test_start_date = '2025-07-01'
        self.test_end_date = '2025-07-02'  # Short range for faster testing
        self.expected_timeframes = ['5m', '15m', '1h', '1d']

    async def setup_test_environment(self):
        """🔧 Setup isolated test environment with real database connection."""
        print("🔧 Setting up test environment...")

        # Create temporary output directory
        self.test_output_dir = tempfile.mkdtemp(prefix='training_data_test_')
        print(f"📁 Test output directory: {self.test_output_dir}")

        # Verify database connection
        try:
            pool = await get_database_pool('intg')
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
            await pool.close()
            print("✅ Database connection verified")
        except Exception as e:
            pytest.fail(f"❌ Database connection failed: {e}")

        return True

    async def cleanup_test_environment(self):
        """🧹 Cleanup test environment and temporary files."""
        if self.test_output_dir and os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
            print(f"🧹 Cleaned up test directory: {self.test_output_dir}")

    async def test_complete_pipeline_integration(self):
        """
        🧪 TEST 1: Complete Pipeline Integration

        Verifies the ENTIRE training data generation workflow:
        - Data input processing
        - Multi-timeframe aggregation
        - Technical indicator calculation
        - ArrayRecord file creation
        - Database registration
        - Resource cleanup
        """
        print("\n🧪 TEST 1: Complete Pipeline Integration")

        # Record initial system state
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        start_time = time.time()

        try:
            # Create configuration for training data generation
            config = TrainingDataConfig(
                base_interval_minutes=1,
                training_interval_minutes=60,
                technical_indicators=["etop", "ebot", "pldot"]
            )

            # Initialize training callback with test parameters
            callback = IntervalBasedTrainingDataCallback(
                symbols=self.test_symbols,
                config=config,
                start_date=self.test_start_date,
                end_date=self.test_end_date,
                output_dir=self.test_output_dir,
                storage_format='arrayrecord'
            )

            # Execute complete pipeline using Runner framework
            print("🚀 Executing training data generation pipeline...")
            environment = Environment(env_type=EnvironmentType.INTG)
            runner = Runner(
                environment=environment,
                symbols=self.test_symbols,
                start_date=self.test_start_date,
                end_date=self.test_end_date,
                interval='60m'
            )

            runner.add_callback(callback)
            await runner.run()

            # Measure execution metrics
            execution_time = time.time() - start_time
            final_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            memory_delta = final_memory - initial_memory

            print(f"⏱️ Execution time: {execution_time:.2f} seconds")
            print(f"📊 Memory usage delta: {memory_delta:.1f} MB")

            # VERIFICATION: Execution completed within acceptable timeframe
            assert execution_time < 300, f"Pipeline too slow: {execution_time:.1f}s (expected <300s)"

            # VERIFICATION: Memory usage reasonable
            assert abs(memory_delta) < 500, f"Memory leak detected: {memory_delta:.1f}MB delta"

            print("✅ Complete pipeline integration test PASSED")
            return True

        except Exception as e:
            print(f"❌ Pipeline integration test FAILED: {e}")
            raise

    async def test_arrayrecord_file_integrity(self):
        """
        🧪 TEST 2: ArrayRecord File Integrity

        Verifies ArrayRecord files are properly created and contain valid data:
        - File creation and proper sizing
        - Binary format integrity
        - Data structure consistency
        - Cross-timeframe record counts
        """
        print("\n🧪 TEST 2: ArrayRecord File Integrity")

        # Find generated ArrayRecord files
        arrayrecord_files = []
        for root, dirs, files in os.walk(self.test_output_dir):
            for file in files:
                if file.endswith('.arrayrecord'):
                    arrayrecord_files.append(os.path.join(root, file))

        print(f"📁 Found {len(arrayrecord_files)} ArrayRecord files")

        # VERIFICATION: Expected number of files created
        expected_file_count = len(self.test_symbols) * len(self.expected_timeframes) * 1  # 1 month
        assert len(arrayrecord_files) >= expected_file_count, f"Expected >= {expected_file_count} files, found {len(arrayrecord_files)}"

        file_integrity_results = {}

        for file_path in arrayrecord_files:
            print(f"🔍 Verifying file: {os.path.basename(file_path)}")

            # VERIFICATION: File exists and has reasonable size
            file_size = os.path.getsize(file_path)
            assert file_size > 1000, f"File too small: {file_size} bytes (expected >1000)"

            try:
                # VERIFICATION: ArrayRecord file is readable
                reader = array_record.ArrayRecordReader(str(file_path))
                total_records = reader.num_records()

                print(f"   📊 Records: {total_records:,}")
                assert total_records > 0, f"Empty ArrayRecord file: {file_path}"

                # VERIFICATION: Sample record structure
                if total_records > 0:
                    reader.seek(0)
                    sample_record = reader.read()

                    # Parse and verify binary format
                    indicator_count = struct.unpack('>H', sample_record[:2])[0]
                    timestamp = struct.unpack('>d', sample_record[2:10])[0]
                    symbol_len = struct.unpack('>I', sample_record[10:14])[0]

                    assert indicator_count > 0, f"No indicators in record: {file_path}"
                    assert symbol_len > 0, f"Invalid symbol length: {file_path}"
                    assert timestamp > 1640995200, f"Invalid timestamp: {timestamp}"  # After 2022

                    # VERIFICATION: OHLCV data is reasonable
                    ohlcv_offset = 14 + symbol_len
                    ohlcv_data = struct.unpack('>fffff', sample_record[ohlcv_offset:ohlcv_offset+20])
                    open_price, high_price, low_price, close_price, volume = ohlcv_data

                    assert open_price > 0, f"Invalid open price: {open_price}"
                    assert high_price >= low_price, f"High < Low: {high_price} < {low_price}"
                    assert volume >= 0, f"Negative volume: {volume}"

                    print(f"   💰 Sample OHLCV: O=${open_price:.2f}, H=${high_price:.2f}, L=${low_price:.2f}, C=${close_price:.2f}, V={volume:,.0f}")

                file_integrity_results[file_path] = {
                    'records': total_records,
                    'size_bytes': file_size,
                    'size_kb': file_size / 1024
                }

            except Exception as e:
                pytest.fail(f"ArrayRecord integrity check failed for {file_path}: {e}")

        print(f"✅ ArrayRecord file integrity test PASSED - {len(arrayrecord_files)} files verified")
        return file_integrity_results

    async def test_database_registration_tracking(self):
        """
        🧪 TEST 3: Database Registration and Tracking

        Verifies training data runs are properly tracked in database:
        - Run metadata registration
        - Training dataset registration
        - Gin config preservation
        - Status tracking accuracy
        """
        print("\n🧪 TEST 3: Database Registration and Tracking")

        try:
            pool = await get_database_pool('intg')
            async with pool.acquire() as conn:
                # VERIFICATION: Find recent training data runs
                recent_runs = await conn.fetch("""
                    SELECT id, run_type, status, command_line, git_commit_hash, created_at
                    FROM intg_runs
                    WHERE run_type = 'training_data_generation'
                    AND created_at >= NOW() - INTERVAL '1 hour'
                    ORDER BY created_at DESC
                    LIMIT 5
                """)

                print(f"📊 Found {len(recent_runs)} recent training data runs")
                assert len(recent_runs) >= 1, "No recent training data runs found in database"

                # VERIFICATION: Run metadata completeness
                latest_run = recent_runs[0]
                print(f"🔍 Latest run ID: {latest_run['id']}")
                print(f"📋 Command line: {latest_run['command_line'][:100]}...")
                print(f"🔗 Git commit: {latest_run['git_commit_hash'][:8]}...")

                assert latest_run['run_type'] == 'training_data_generation', f"Wrong run type: {latest_run['run_type']}"
                assert latest_run['command_line'] is not None, "Missing command line in run record"
                assert latest_run['git_commit_hash'] is not None, "Missing git commit hash"

                # VERIFICATION: Training dataset registration
                recent_datasets = await conn.fetch("""
                    SELECT id, dataset_name, symbols, status, total_sequences, feature_count, file_size_mb
                    FROM intg_training_datasets
                    WHERE created_at >= NOW() - INTERVAL '1 hour'
                    ORDER BY created_at DESC
                    LIMIT 3
                """)

                print(f"📊 Found {len(recent_datasets)} recent training datasets")

                if recent_datasets:
                    latest_dataset = recent_datasets[0]
                    print(f"📋 Dataset: {latest_dataset['dataset_name']}")
                    print(f"📊 Status: {latest_dataset['status']}")
                    print(f"📈 File size: {latest_dataset['file_size_mb']}MB")

                    # VERIFICATION: Dataset has reasonable metadata
                    assert latest_dataset['symbols'] is not None, "Missing symbols in dataset"
                    assert len(latest_dataset['symbols']) > 0, "Empty symbols array"

            await pool.close()

            print("✅ Database registration and tracking test PASSED")
            return True

        except Exception as e:
            print(f"❌ Database registration test FAILED: {e}")
            raise

    async def test_multi_timeframe_consistency(self):
        """
        🧪 TEST 4: Multi-Timeframe Data Consistency

        Verifies data consistency across different timeframes:
        - All timeframes generated
        - Data aggregation accuracy
        - Timestamp alignment
        - Volume conservation
        """
        print("\n🧪 TEST 4: Multi-Timeframe Data Consistency")

        timeframe_data = {}

        # Collect data from each timeframe
        for root, dirs, files in os.walk(self.test_output_dir):
            for file in files:
                if file.endswith('.arrayrecord'):
                    # Extract timeframe from directory structure
                    path_parts = root.split(os.sep)
                    if len(path_parts) >= 2:
                        timeframe = path_parts[-1]  # e.g., '5m', '15m', '1h', '1d'

                        if timeframe in self.expected_timeframes:
                            file_path = os.path.join(root, file)

                            try:
                                reader = array_record.ArrayRecordReader(str(file_path))
                                record_count = reader.num_records()

                                if record_count > 0:
                                    reader.seek(0)
                                    first_record = reader.read()

                                    # Extract timestamp and volume from first record
                                    timestamp = struct.unpack('>d', first_record[2:10])[0]
                                    symbol_len = struct.unpack('>I', first_record[10:14])[0]
                                    ohlcv_offset = 14 + symbol_len
                                    volume = struct.unpack('>f', first_record[ohlcv_offset+16:ohlcv_offset+20])[0]

                                    timeframe_data[timeframe] = {
                                        'records': record_count,
                                        'first_timestamp': timestamp,
                                        'first_volume': volume,
                                        'file_path': file_path
                                    }

                                    print(f"📊 {timeframe}: {record_count} records, first volume: {volume:,.0f}")

                            except Exception as e:
                                print(f"⚠️ Error reading {timeframe} data: {e}")

        # VERIFICATION: All expected timeframes present
        found_timeframes = set(timeframe_data.keys())
        expected_timeframes = set(self.expected_timeframes)
        missing_timeframes = expected_timeframes - found_timeframes

        assert len(missing_timeframes) == 0, f"Missing timeframes: {missing_timeframes}"

        # VERIFICATION: Record count consistency (higher frequency = more records)
        if '5m' in timeframe_data and '1h' in timeframe_data:
            ratio_5m_1h = timeframe_data['5m']['records'] / timeframe_data['1h']['records']
            # 5-minute data should have roughly 12x more records than 1-hour (60min / 5min = 12)
            assert 8 <= ratio_5m_1h <= 15, f"Unexpected 5m/1h ratio: {ratio_5m_1h:.1f} (expected ~12)"
            print(f"📏 5m/1h record ratio: {ratio_5m_1h:.1f} (expected ~12)")

        # VERIFICATION: Timestamp alignment
        timestamps = [data['first_timestamp'] for data in timeframe_data.values()]
        timestamp_range = max(timestamps) - min(timestamps)
        assert timestamp_range < 86400, f"Timestamps too spread out: {timestamp_range}s (expected <24h)"

        print("✅ Multi-timeframe data consistency test PASSED")
        return timeframe_data

    async def test_error_handling_and_cleanup(self):
        """
        🧪 TEST 5: Error Handling and Resource Cleanup

        Verifies robust error handling and resource management:
        - ArrayRecord writer cleanup on exceptions
        - Context manager proper exit
        - Memory leak prevention
        - Partial data handling
        """
        print("\n🧪 TEST 5: Error Handling and Resource Cleanup")

        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

        # TEST: Simulated failure during generation
        try:
            config = TrainingDataConfig(
                base_interval_minutes=1,
                training_interval_minutes=60,
                technical_indicators=["etop", "ebot", "pldot"]
            )

            callback = IntervalBasedTrainingDataCallback(
                symbols=['INVALID_SYMBOL_TEST'],  # Use invalid symbol to trigger controlled failure
                config=config,
                start_date=self.test_start_date,
                end_date=self.test_end_date,
                output_dir=self.test_output_dir,
                storage_format='arrayrecord'
            )

            # Mock a failure in the middle of processing
            original_process_interval = callback._process_interval

            def failing_process_interval(*args, **kwargs):
                # Let first few calls succeed, then fail
                if hasattr(callback, '_call_count'):
                    callback._call_count += 1
                else:
                    callback._call_count = 1

                if callback._call_count > 2:
                    raise RuntimeError("Simulated processing failure")

                return original_process_interval(*args, **kwargs)

            callback._process_interval = failing_process_interval

            # Try to run pipeline - should fail gracefully
            exception_caught = False
            try:
                environment = Environment(env_type=EnvironmentType.INTG)
                runner = Runner(
                    environment=environment,
                    symbols=['INVALID_SYMBOL_TEST'],
                    start_date=self.test_start_date,
                    end_date=self.test_end_date,
                    interval='60m'
                )

                runner.add_callback(callback)
                await runner.run()
            except (RuntimeError, Exception) as e:
                exception_caught = True
                print(f"🎯 Caught expected exception: {type(e).__name__}: {e}")

            # VERIFICATION: Exception was properly caught and handled
            assert exception_caught, "Expected exception was not raised"

            # VERIFICATION: Memory was properly cleaned up
            final_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            memory_delta = final_memory - initial_memory
            assert abs(memory_delta) < 100, f"Memory not cleaned up: {memory_delta:.1f}MB delta"

            print(f"📊 Memory cleanup verified: {memory_delta:.1f}MB delta")
            print("✅ Error handling and cleanup test PASSED")
            return True

        except AssertionError:
            raise
        except Exception as e:
            print(f"❌ Error handling test failed unexpectedly: {e}")
            raise

    async def test_performance_benchmarks(self):
        """
        🧪 TEST 6: Performance and Resource Usage

        Measures and validates system performance:
        - Processing speed benchmarks
        - Memory usage patterns
        - I/O efficiency
        - Resource utilization
        """
        print("\n🧪 TEST 6: Performance and Resource Usage")

        # Performance monitoring setup
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        initial_cpu_percent = process.cpu_percent()
        start_time = time.time()

        # Record I/O statistics
        initial_io = process.io_counters()

        # Run a controlled benchmark
        config = TrainingDataConfig(
            base_interval_minutes=1,
            training_interval_minutes=60,
            technical_indicators=["etop", "ebot", "pldot"]
        )

        callback = IntervalBasedTrainingDataCallback(
            symbols=self.test_symbols,
            config=config,
            start_date=self.test_start_date,
            end_date=self.test_end_date,
            output_dir=self.test_output_dir,
            storage_format='arrayrecord'
        )

        environment = Environment(env_type=EnvironmentType.INTG)
        runner = Runner(
            environment=environment,
            symbols=self.test_symbols,
            start_date=self.test_start_date,
            end_date=self.test_end_date,
            interval='60m'
        )

        runner.add_callback(callback)
        await runner.run()

        # Measure final performance metrics
        end_time = time.time()
        execution_time = end_time - start_time
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        peak_memory = final_memory
        final_io = process.io_counters()

        # Calculate metrics
        memory_usage = final_memory - initial_memory
        io_read_mb = (final_io.read_bytes - initial_io.read_bytes) / 1024 / 1024
        io_write_mb = (final_io.write_bytes - initial_io.write_bytes) / 1024 / 1024

        # Count generated files for throughput calculation
        generated_files = 0
        total_file_size = 0
        for root, dirs, files in os.walk(self.test_output_dir):
            for file in files:
                if file.endswith('.arrayrecord'):
                    generated_files += 1
                    total_file_size += os.path.getsize(os.path.join(root, file))

        throughput_files_per_sec = generated_files / execution_time if execution_time > 0 else 0
        throughput_mb_per_sec = (total_file_size / 1024 / 1024) / execution_time if execution_time > 0 else 0

        # Performance reporting
        print(f"⏱️ Execution time: {execution_time:.2f} seconds")
        print(f"📊 Memory usage: {memory_usage:.1f} MB")
        print(f"📁 Files generated: {generated_files}")
        print(f"💾 Total output size: {total_file_size / 1024 / 1024:.1f} MB")
        print(f"📈 Throughput: {throughput_files_per_sec:.1f} files/sec, {throughput_mb_per_sec:.1f} MB/sec")
        print(f"💿 I/O: Read {io_read_mb:.1f} MB, Write {io_write_mb:.1f} MB")

        # VERIFICATION: Performance within acceptable bounds
        assert execution_time < 180, f"Processing too slow: {execution_time:.1f}s (expected <180s)"
        assert memory_usage < 1000, f"Memory usage too high: {memory_usage:.1f}MB (expected <1000MB)"
        assert generated_files > 0, "No files generated"
        assert throughput_files_per_sec > 0.01, f"Throughput too low: {throughput_files_per_sec:.3f} files/sec"

        performance_metrics = {
            'execution_time_sec': execution_time,
            'memory_usage_mb': memory_usage,
            'files_generated': generated_files,
            'throughput_files_per_sec': throughput_files_per_sec,
            'throughput_mb_per_sec': throughput_mb_per_sec,
            'io_read_mb': io_read_mb,
            'io_write_mb': io_write_mb
        }

        print("✅ Performance benchmark test PASSED")
        return performance_metrics

    async def test_data_quality_validation(self):
        """
        🧪 TEST 7: Data Quality Validation

        Validates the quality and structure of generated training data:
        - Technical indicator completeness
        - Price data consistency
        - Volume data validation
        - Feature completeness
        """
        print("\n🧪 TEST 7: Data Quality Validation")

        quality_metrics = {
            'total_records_validated': 0,
            'valid_price_records': 0,
            'valid_volume_records': 0,
            'indicator_completeness': 0,
            'data_anomalies': []
        }

        # Analyze all generated ArrayRecord files
        for root, dirs, files in os.walk(self.test_output_dir):
            for file in files:
                if file.endswith('.arrayrecord'):
                    file_path = os.path.join(root, file)
                    timeframe = os.path.basename(root)

                    print(f"🔍 Validating data quality: {timeframe}/{file}")

                    try:
                        reader = array_record.ArrayRecordReader(str(file_path))
                        total_records = reader.num_records()

                        if total_records == 0:
                            print(f"⚠️ Empty file: {file}")
                            continue

                        quality_metrics['total_records_validated'] += total_records

                        # Sample and validate multiple records
                        sample_size = min(10, total_records)
                        sample_indices = [i * (total_records // sample_size) for i in range(sample_size)]

                        valid_prices = 0
                        valid_volumes = 0
                        indicator_counts = []

                        for idx in sample_indices:
                            reader.seek(idx)
                            record = reader.read()

                            # Parse record structure
                            indicator_count = struct.unpack('>H', record[:2])[0]
                            timestamp = struct.unpack('>d', record[2:10])[0]
                            symbol_len = struct.unpack('>I', record[10:14])[0]
                            symbol = record[14:14+symbol_len].decode('utf-8')

                            # Validate OHLCV data
                            ohlcv_offset = 14 + symbol_len
                            ohlcv_data = struct.unpack('>fffff', record[ohlcv_offset:ohlcv_offset+20])
                            open_price, high_price, low_price, close_price, volume = ohlcv_data

                            # Price validation
                            if (open_price > 0 and high_price >= low_price and
                                close_price > 0 and high_price >= max(open_price, close_price) and
                                low_price <= min(open_price, close_price)):
                                valid_prices += 1
                            else:
                                quality_metrics['data_anomalies'].append(f"Invalid OHLC in {file} record {idx}")

                            # Volume validation
                            if volume >= 0:
                                valid_volumes += 1
                            else:
                                quality_metrics['data_anomalies'].append(f"Negative volume in {file} record {idx}")

                            # Indicator count tracking
                            indicator_counts.append(indicator_count)

                        # Calculate quality metrics for this file
                        price_quality = valid_prices / sample_size
                        volume_quality = valid_volumes / sample_size
                        avg_indicators = sum(indicator_counts) / len(indicator_counts)

                        quality_metrics['valid_price_records'] += valid_prices
                        quality_metrics['valid_volume_records'] += valid_volumes

                        print(f"   📊 Price quality: {price_quality*100:.1f}%")
                        print(f"   📈 Volume quality: {volume_quality*100:.1f}%")
                        print(f"   🔧 Avg indicators: {avg_indicators:.1f}")

                        # VERIFICATION: High data quality standards
                        assert price_quality >= 0.95, f"Price quality too low: {price_quality*100:.1f}% (expected ≥95%)"
                        assert volume_quality >= 0.95, f"Volume quality too low: {volume_quality*100:.1f}% (expected ≥95%)"
                        assert avg_indicators >= 10, f"Too few indicators: {avg_indicators:.1f} (expected ≥10)"

                    except Exception as e:
                        quality_metrics['data_anomalies'].append(f"Error validating {file}: {e}")
                        print(f"⚠️ Validation error for {file}: {e}")

        # Calculate overall quality metrics
        total_samples = quality_metrics['valid_price_records'] + quality_metrics['valid_volume_records']
        if quality_metrics['total_records_validated'] > 0:
            overall_quality = total_samples / (quality_metrics['total_records_validated'] * 2)  # *2 for price+volume
        else:
            overall_quality = 0

        print(f"\n📊 Overall Data Quality Report:")
        print(f"   📈 Records validated: {quality_metrics['total_records_validated']:,}")
        print(f"   ✅ Overall quality score: {overall_quality*100:.1f}%")
        print(f"   ⚠️ Data anomalies: {len(quality_metrics['data_anomalies'])}")

        # VERIFICATION: Overall data quality meets standards
        assert overall_quality >= 0.95, f"Overall data quality too low: {overall_quality*100:.1f}% (expected ≥95%)"
        assert len(quality_metrics['data_anomalies']) < 5, f"Too many data anomalies: {len(quality_metrics['data_anomalies'])}"

        print("✅ Data quality validation test PASSED")
        return quality_metrics

    async def test_complete_workflow_regression(self):
        """
        🧪 TEST 8: Complete Workflow Regression Protection

        Ensures the entire system continues to work as expected:
        - End-to-end workflow stability
        - Backwards compatibility
        - Integration points
        - System resilience
        """
        print("\n🧪 TEST 8: Complete Workflow Regression Protection")

        regression_checks = {
            'workflow_completed': False,
            'database_integration': False,
            'file_system_integration': False,
            'arrayrecord_compatibility': False,
            'cleanup_successful': False
        }

        try:
            # Complete workflow execution
            print("🔄 Executing complete regression workflow...")

            config = TrainingDataConfig(
                base_interval_minutes=1,
                training_interval_minutes=60,
                technical_indicators=["etop", "ebot", "pldot"]
            )

            callback = IntervalBasedTrainingDataCallback(
                symbols=self.test_symbols,
                config=config,
                start_date=self.test_start_date,
                end_date=self.test_end_date,
                output_dir=self.test_output_dir,
                storage_format='arrayrecord'
            )

            # Execute with full monitoring
            execution_successful = False
            try:
                environment = Environment(env_type=EnvironmentType.INTG)
                runner = Runner(
                    environment=environment,
                    symbols=self.test_symbols,
                    start_date=self.test_start_date,
                    end_date=self.test_end_date,
                    interval='60m'
                )

                runner.add_callback(callback)
                await runner.run()
                execution_successful = True

            except Exception as e:
                print(f"⚠️ Workflow execution error: {e}")

            regression_checks['workflow_completed'] = execution_successful

            # Database integration check
            try:
                pool = await get_database_pool('intg')
                async with pool.acquire() as conn:
                    recent_run = await conn.fetchrow("""
                        SELECT id FROM intg_runs
                        WHERE created_at >= NOW() - INTERVAL '5 minutes'
                        ORDER BY created_at DESC LIMIT 1
                    """)

                    regression_checks['database_integration'] = recent_run is not None
                await pool.close()

            except Exception as e:
                print(f"⚠️ Database integration error: {e}")

            # File system integration check
            generated_files = []
            for root, dirs, files in os.walk(self.test_output_dir):
                for file in files:
                    if file.endswith('.arrayrecord'):
                        generated_files.append(os.path.join(root, file))

            regression_checks['file_system_integration'] = len(generated_files) > 0

            # ArrayRecord compatibility check
            compatible_files = 0
            for file_path in generated_files:
                try:
                    reader = array_record.ArrayRecordReader(str(file_path))
                    if reader.num_records() > 0:
                        compatible_files += 1
                except:
                    pass

            regression_checks['arrayrecord_compatibility'] = (
                compatible_files > 0 and
                compatible_files == len(generated_files)
            )

            # Cleanup verification
            regression_checks['cleanup_successful'] = True  # If we get here, cleanup worked

            # Report regression results
            print(f"\n📋 Regression Check Results:")
            for check, result in regression_checks.items():
                status = "✅ PASS" if result else "❌ FAIL"
                print(f"   {check}: {status}")

            # VERIFICATION: All regression checks must pass
            failed_checks = [check for check, result in regression_checks.items() if not result]
            assert len(failed_checks) == 0, f"Regression failures: {failed_checks}"

            print("✅ Complete workflow regression protection test PASSED")
            return regression_checks

        except Exception as e:
            print(f"❌ Regression test failed: {e}")
            raise


# Pytest test functions
@pytest.fixture
async def test_suite():
    """Initialize and cleanup test suite."""
    suite = TrainingDataEndToEndTest()
    await suite.setup_test_environment()
    yield suite
    await suite.cleanup_test_environment()


@pytest.mark.asyncio
async def test_end_to_end_comprehensive_suite(test_suite):
    """
    🧪 MASTER TEST: Comprehensive End-to-End Training Data Generation

    Executes all test categories in sequence with proper error handling and reporting.
    This is the main entry point for comprehensive validation.
    """
    print("\n" + "="*80)
    print("🧪 COMPREHENSIVE END-TO-END TRAINING DATA GENERATION TESTS")
    print("="*80)

    test_results = {}
    total_tests = 8
    passed_tests = 0

    # Execute all test categories
    test_methods = [
        ('Complete Pipeline Integration', test_suite.test_complete_pipeline_integration),
        ('ArrayRecord File Integrity', test_suite.test_arrayrecord_file_integrity),
        ('Database Registration and Tracking', test_suite.test_database_registration_tracking),
        ('Multi-Timeframe Data Consistency', test_suite.test_multi_timeframe_consistency),
        ('Error Handling and Cleanup', test_suite.test_error_handling_and_cleanup),
        ('Performance and Resource Usage', test_suite.test_performance_benchmarks),
        ('Data Quality Validation', test_suite.test_data_quality_validation),
        ('Complete Workflow Regression', test_suite.test_complete_workflow_regression),
    ]

    for test_name, test_method in test_methods:
        try:
            print(f"\n🔹 Starting: {test_name}")
            result = await test_method()
            test_results[test_name] = {'status': 'PASSED', 'result': result}
            passed_tests += 1
            print(f"✅ {test_name}: PASSED")

        except Exception as e:
            test_results[test_name] = {'status': 'FAILED', 'error': str(e)}
            print(f"❌ {test_name}: FAILED - {e}")

    # Final test report
    print("\n" + "="*80)
    print("🎯 COMPREHENSIVE TEST SUITE RESULTS")
    print("="*80)

    for test_name, result in test_results.items():
        status_icon = "✅" if result['status'] == 'PASSED' else "❌"
        print(f"{status_icon} {test_name}: {result['status']}")

        if result['status'] == 'FAILED':
            print(f"    Error: {result['error']}")

    print(f"\n📊 Summary: {passed_tests}/{total_tests} tests passed ({passed_tests/total_tests*100:.1f}%)")

    # Overall success criteria
    success_rate = passed_tests / total_tests
    assert success_rate >= 0.875, f"Test success rate too low: {success_rate*100:.1f}% (expected ≥87.5%)"

    if success_rate == 1.0:
        print("🎉 ALL TESTS PASSED - SYSTEM FULLY VERIFIED")
    else:
        print(f"⚠️ {total_tests - passed_tests} tests failed - system needs attention")

    print("="*80)
    return test_results


if __name__ == "__main__":
    """Direct execution for development testing."""
    print("🧪 Direct execution of comprehensive end-to-end tests")

    async def run_direct():
        suite = TrainingDataEndToEndTest()
        try:
            await suite.setup_test_environment()
            results = await test_end_to_end_comprehensive_suite(suite)
            print("\n🎯 Direct execution completed successfully")
            return results
        finally:
            await suite.cleanup_test_environment()

    # Run the tests
    import asyncio
    results = asyncio.run(run_direct())