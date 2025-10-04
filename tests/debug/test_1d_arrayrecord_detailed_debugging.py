#!/usr/bin/env python3
"""
Ultra-detailed debugging for 1d ArrayRecord missing data issue.

This test provides step-by-step debugging with extensive logging to identify 
exactly where the 1d timeframe data gets lost, corrupted, or filtered out
during the training data generation process.

Focus Areas:
1. Date range filtering logic for 1d vs other timeframes
2. ArrayRecord writer initialization for 1d files
3. Data flow through the callback system
4. Schema generation differences between timeframes
5. File writing vs reading discrepancies
"""

import pytest
import tempfile
import asyncio
import logging
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, Any, List

# Skip entire module if array_record is not available
array_record = pytest.importorskip("array_record.python.array_record_module", reason="array_record module not installed")

from core.platform.config_env.environment import Environment, EnvironmentType
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


# Setup ultra-detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/tmp/1d_debug.log', mode='w')
    ]
)

logger = logging.getLogger("1d_arrayrecord_debug")
logger.setLevel(logging.DEBUG)


class Test1dArrayRecordDetailedDebugging:
    """Ultra-detailed debugging tests for 1d ArrayRecord issue."""

    @pytest.fixture
    def debug_output_dir(self):
        """Create debug output directory with detailed logging."""
        with tempfile.TemporaryDirectory() as temp_dir:
            debug_dir = Path(temp_dir)
            logger.info(f"🔧 Debug output directory: {debug_dir}")
            yield debug_dir

    @pytest.fixture
    def debug_runner_with_extensive_logging(self):
        """Create mock runner with maximum logging detail."""
        from unittest.mock import Mock, AsyncMock
        
        runner = Mock()
        env = Mock(spec=Environment)
        env.env_type = EnvironmentType.TEST
        env.get_connection = AsyncMock()
        runner.get_environment.return_value = env
        
        # Setup runner logger
        runner_logger = logging.getLogger("debug_runner")
        runner_logger.setLevel(logging.DEBUG)
        runner.logger = runner_logger
        
        return runner

    def create_debug_callback_with_logging(self, output_dir: str) -> IntervalBasedTrainingDataCallback:
        """Create callback with extensive debugging enabled."""
        logger.info("🔧 Creating debug callback with extensive logging")
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=["AAPL"],
            start_date="2025-07-15",
            end_date="2025-07-15",
            output_dir=output_dir
        )
        
        # Enable detailed logging
        callback.logger.setLevel(logging.DEBUG)
        callback.dataset_id = "debug_1d_issue"
        
        # Override key methods to add debugging
        original_categorize = callback._categorize_features_by_group
        original_stream = callback._stream_training_examples_to_writers
        
        async def debug_categorize_features(features: Dict[str, Any], timeframe: str, feature_dao):
            logger.debug(f"🔍 CATEGORIZING FEATURES FOR {timeframe}")
            logger.debug(f"   📊 Input features count: {len(features)}")
            logger.debug(f"   🔑 Feature keys: {list(features.keys())}")
            logger.debug(f"   💾 Feature values sample: {dict(list(features.items())[:3])}")
            
            result = await original_categorize(features, timeframe, feature_dao)
            
            logger.debug(f"   ✅ Output feature groups: {list(result.keys())}")
            for group, group_features in result.items():
                logger.debug(f"      {group}: {len(group_features)} features")
            
            return result
        
        async def debug_stream_examples(examples: List[Dict], current_time: datetime, runner):
            logger.debug(f"🌊 STREAMING {len(examples)} EXAMPLES AT {current_time}")
            
            # Log current time vs date range
            logger.debug(f"   📅 Current date: {current_time.date()}")
            logger.debug(f"   📅 Start date: {callback.start_date}")
            logger.debug(f"   📅 End date: {callback.end_date}")
            logger.debug(f"   ✅ In range: {callback.start_date <= current_time.date() <= callback.end_date}")
            
            # Process each example with detailed logging
            for idx, example in enumerate(examples):
                logger.debug(f"   📋 Example {idx}: symbol={example.get('symbol', 'UNKNOWN')}")
                
                timeframe_features = example.get('timeframe_features', {})
                logger.debug(f"      🔢 Timeframes found: {list(timeframe_features.keys())}")
                
                for timeframe, features in timeframe_features.items():
                    if timeframe == '1d':
                        logger.debug(f"      🎯 1D FEATURES DETECTED:")
                        logger.debug(f"         📊 Feature count: {len(features) if features else 0}")
                        logger.debug(f"         🔑 Keys: {list(features.keys()) if features else []}")
                        logger.debug(f"         💾 Values: {features if features else 'EMPTY'}")
            
            # Call original method
            await original_stream(examples, current_time, runner)
            
            logger.debug(f"   ✅ Streaming completed")
        
        # Replace methods with debug versions
        callback._categorize_features_by_group = debug_categorize_features
        callback._stream_training_examples_to_writers = debug_stream_examples
        
        return callback

    def create_comprehensive_test_data(self) -> List[Dict[str, Any]]:
        """Create comprehensive test data that should definitely generate 1d records."""
        logger.info("📊 Creating comprehensive test data for 1d debugging")
        
        base_date = datetime(2025, 7, 15)
        examples = []
        
        # Create multiple examples throughout the day to ensure daily aggregation
        time_points = [
            (9, 30),   # Market open
            (12, 0),   # Midday
            (15, 30),  # Near market close
        ]
        
        for hour, minute in time_points:
            current_time = base_date.replace(hour=hour, minute=minute)
            
            example = {
                'symbol': 'AAPL',
                'timeframe_features': {
                    '5m': {
                        '5m_timestamp': current_time.timestamp(),
                        '5m_symbol': 'AAPL',
                        '5m_open': 207.50 + (hour * 0.1),
                        '5m_high': 207.60 + (hour * 0.1),
                        '5m_low': 207.40 + (hour * 0.1),
                        '5m_close': 207.55 + (hour * 0.1),
                        '5m_volume': 1000 + (hour * 100),
                        '5m_vwap': 207.52 + (hour * 0.1),
                    },
                    '1d': {
                        '1d_timestamp': base_date.timestamp(),  # Same daily timestamp for all
                        '1d_symbol': 'AAPL',
                        '1d_open': 207.50,
                        '1d_high': 208.20,
                        '1d_low': 207.30,
                        '1d_close': 207.95,
                        '1d_volume': 850000,
                        '1d_vwap': 207.75,
                    }
                }
            }
            examples.append(example)
            
            logger.debug(f"   ⏰ Created example for {current_time}")
        
        logger.info(f"📊 Created {len(examples)} examples with 1d data")
        return examples

    async def test_step_by_step_1d_data_flow_debugging(self, debug_output_dir, 
                                                      debug_runner_with_extensive_logging):
        """Ultra-detailed step-by-step debugging of 1d data flow."""
        logger.info("🔍 STARTING STEP-BY-STEP 1D DATA FLOW DEBUGGING")
        
        # Step 1: Create callback with debug logging
        callback = self.create_debug_callback_with_logging(str(debug_output_dir))
        logger.info("✅ Step 1: Created debug callback")
        
        # Step 2: Create test data
        test_examples = self.create_comprehensive_test_data()
        logger.info(f"✅ Step 2: Created {len(test_examples)} test examples")
        
        # Step 3: Mock feature DAO to avoid database dependency
        from unittest.mock import AsyncMock
        from domains.ml.services.training_data.dao.feature_extraction_dao import FeatureMappingResult
        
        mock_feature_dao = AsyncMock()
        mock_mappings = [
            FeatureMappingResult("timestamp", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("symbol", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("open", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("high", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("low", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("close", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("volume", "ohlcv_basic", 1, "exact", confidence=1.0),
            FeatureMappingResult("vwap", "ohlcv_basic", 1, "exact", confidence=1.0),
        ]
        mock_feature_dao.get_feature_mappings_batch.return_value = mock_mappings
        callback._get_feature_dao = AsyncMock(return_value=mock_feature_dao)
        logger.info("✅ Step 3: Mocked feature DAO")
        
        # Step 4: Initialize dataset structure
        logger.info("🔧 Step 4: Initializing dataset structure")
        await callback._initialize_dataset_structure()
        logger.info("✅ Step 4: Dataset structure initialized")
        
        # Log the writers that were created
        logger.info(f"📝 ArrayRecord writers created: {len(callback.array_record_writers)}")
        for writer_key in callback.array_record_writers.keys():
            logger.info(f"   📄 Writer: {writer_key}")
        
        # Step 5: Process examples with detailed tracking
        current_time = datetime(2025, 7, 15, 12, 0, 0)
        logger.info(f"🌊 Step 5: Processing examples at {current_time}")
        
        await callback._stream_training_examples_to_writers(
            test_examples,
            current_time,
            debug_runner_with_extensive_logging
        )
        logger.info("✅ Step 5: Examples processed")
        
        # Step 6: Close writers and analyze results
        logger.info("🔒 Step 6: Closing writers")
        callback._ensure_writers_closed()
        logger.info("✅ Step 6: Writers closed")
        
        # Step 7: Detailed file analysis
        logger.info("🔍 Step 7: Analyzing generated files")
        
        dataset_dir = debug_output_dir / "debug_1d_issue" / "AAPL_2025_07"
        logger.info(f"📁 Dataset directory: {dataset_dir}")
        
        # Check each timeframe
        file_analysis = {}
        timeframes = ['5m', '1d']
        
        for timeframe in timeframes:
            logger.info(f"\n🔍 ANALYZING {timeframe.upper()} TIMEFRAME:")
            
            timeframe_dir = dataset_dir / timeframe
            arrayrecord_file = timeframe_dir / "AAPL_2025_07.arrayrecord"
            
            logger.info(f"   📁 Directory: {timeframe_dir}")
            logger.info(f"   📄 File: {arrayrecord_file}")
            logger.info(f"   ✅ Directory exists: {timeframe_dir.exists()}")
            logger.info(f"   ✅ File exists: {arrayrecord_file.exists()}")
            
            if arrayrecord_file.exists():
                file_size = arrayrecord_file.stat().st_size
                logger.info(f"   📏 File size: {file_size:,} bytes")
                
                # Read file with detailed error handling
                logger.info(f"   📖 Attempting to read ArrayRecord...")
                reader = array_record.ArrayRecordReader()
                reader.OpenFromFile(str(arrayrecord_file))
                
                records = []
                record_count = 0
                
                for record in reader:
                    records.append(record)
                    record_count += 1
                    if record_count <= 3:  # Log first few records
                        logger.info(f"      📋 Record {record_count}: {record}")
                    if record_count >= 10:  # Limit for debugging
                        break
                
                logger.info(f"   ✅ Successfully read {record_count} records")
                
                file_analysis[timeframe] = {
                    'exists': True,
                    'file_size': file_size,
                    'record_count': record_count,
                    'readable': True,
                    'first_record': records[0] if records else None
                }
                
                logger.warning(f"   ❌ File not found: {arrayrecord_file}")
                file_analysis[timeframe] = {
                    'exists': False,
                    'file_size': 0,
                    'record_count': 0,
                    'readable': False,
                    'error': 'File not found'
                }
        
        # Step 8: Summary and conclusions
        logger.info("\n📊 DEBUGGING SUMMARY:")
        for timeframe, analysis in file_analysis.items():
            status = "✅ READABLE" if analysis.get('readable') else "❌ UNREADABLE"
            logger.info(f"   {timeframe:>3s}: {analysis.get('file_size', 0):>8,} bytes, {analysis.get('record_count', 0):>4} records {status}")
            
            if not analysis.get('readable') and analysis.get('error'):
                logger.error(f"        Error: {analysis['error']}")
        
        # Log conclusion
        if file_analysis.get('1d', {}).get('readable', False):
            logger.info("🎉 1D ARRAYRECORD ISSUE RESOLVED!")
        else:
            logger.warning("🔥 1D ARRAYRECORD ISSUE PERSISTS - DETAILED LOGS AVAILABLE")
            logger.warning("    Check /tmp/1d_debug.log for complete debugging trace")
        
        return file_analysis

    async def test_direct_arrayrecord_writer_debugging(self, debug_output_dir):
        """Test ArrayRecord writer directly to isolate callback vs writer issues."""
        logger.info("🔧 DIRECT ARRAYRECORD WRITER DEBUGGING")
        
        # Test data for both timeframes
        test_records = {
            '5m': [
                {
                    'timestamp': 1689422100.0,
                    'symbol': 'AAPL',
                    'open': 207.50,
                    'high': 207.60,
                    'low': 207.40,
                    'close': 207.55,
                    'volume': 1000.0
                }
            ],
            '1d': [
                {
                    'timestamp': 1689422100.0,
                    'symbol': 'AAPL',
                    'open': 207.50,
                    'high': 208.20,
                    'low': 207.30,
                    'close': 207.95,
                    'volume': 850000.0
                }
            ]
        }
        
        results = {}
        
        for timeframe, records in test_records.items():
            logger.info(f"\n🔧 Testing direct ArrayRecord writing for {timeframe}")
            
            output_file = debug_output_dir / f"direct_{timeframe}_test.arrayrecord"
            
            # Write records
            logger.info(f"   ✍️ Writing {len(records)} records to {output_file}")
            writer = array_record.ArrayRecordWriter()
            writer.OpenFromFile(str(output_file))
            
            for idx, record in enumerate(records):
                logger.debug(f"      📋 Writing record {idx}: {record}")
                writer.WriteRecord(record)
            
            writer.Close()
            logger.info(f"   ✅ Writing completed")
            
            # Verify by reading
            logger.info(f"   📖 Reading back from {output_file}")
            reader = array_record.ArrayRecordReader()
            reader.OpenFromFile(str(output_file))
            
            read_records = []
            for idx, record in enumerate(reader):
                read_records.append(record)
                logger.debug(f"      📋 Read record {idx}: {record}")
            
            logger.info(f"   ✅ Read {len(read_records)} records successfully")
            
            results[timeframe] = {
                'write_success': True,
                'read_success': True,
                'records_written': len(records),
                'records_read': len(read_records),
                'data_matches': records[0] == dict(read_records[0]) if read_records else False
            }
            
        for timeframe, result in results.items():
            if result.get('write_success') and result.get('read_success'):
                logger.info(f"✅ {timeframe}: Direct ArrayRecord writing/reading works")
            else:
                logger.error(f"❌ {timeframe}: Direct ArrayRecord failed - {result.get('error', 'Unknown error')}")
        
        return results


if __name__ == "__main__":
    # Run individual tests for debugging
    import asyncio
    
    async def run_debug_test():
        test_instance = Test1dArrayRecordDetailedDebugging()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            debug_dir = Path(temp_dir)
            
            from unittest.mock import Mock, AsyncMock
            runner = Mock()
            env = Mock(spec=Environment)
            env.env_type = EnvironmentType.TEST
            env.get_connection = AsyncMock()
            runner.get_environment.return_value = env
            
            # Run the main debugging test
            await test_instance.test_step_by_step_1d_data_flow_debugging(debug_dir, runner)
            
            # Run direct writer test
            await test_instance.test_direct_arrayrecord_writer_debugging(debug_dir)
    
    asyncio.run(run_debug_test())