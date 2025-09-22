#!/usr/bin/env python3
"""
TEST-FIRST REPRODUCTION: 1d ArrayRecord empty while 5m has data

ISSUE OBSERVED (2025-09-21):
- Dataset: /data/training_data/dataset_20250921_072901/AAPL_2025_07/
- 5m/AAPL_2025_07.arrayrecord: 624 records (✅ WORKING)  
- 1d/AAPL_2025_07.arrayrecord: 0 records (❌ BROKEN - "Empty ArrayRecord file")

ROOT CAUSE ANALYSIS APPROACH:
1. First log actual inputs in the failing training data generation method
2. Write test to reproduce exact failure scenario with logged inputs
3. Identify specific coding error causing 1d timeframe data loss
4. Fix root cause and verify with test

CRITICAL: This follows debug-first methodology - NO WORKAROUNDS without understanding WHY.
"""

import pytest
import tempfile
import asyncio
import logging
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List

from core.platform.config.environment import Environment, EnvironmentType
from core.market_data.unified_manager import UnifiedMarketDataManager
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


class Test1dArrayRecordMissingIssue:
    """
    TEST-FIRST REPRODUCTION: 1d ArrayRecord empty while 5m has data
    
    This test reproduces the exact production issue using real system components
    and follows the mandatory debug-first methodology from CLAUDE.md.
    """

    @pytest.fixture
    async def test_environment(self):
        """Real test environment with database connection."""
        return Environment(
            env_type=EnvironmentType.DEV,
            db_host="localhost",
            db_port=3432,
            db_user="postgres",
            db_password="dev_password",
            db_name="dev_db"
        )

    @pytest.fixture
    def test_output_dir(self):
        """Create temporary directory for test output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture 
    async def real_market_data_manager(self, test_environment):
        """Real market data manager with test data access."""
        manager = UnifiedMarketDataManager(
            environment=test_environment,
            data_path="/mnt/d/ats-data"
        )
        await manager.initialize()
        return manager

    def test_analyze_existing_1d_arrayrecord_issue(self):
        """
        REPRODUCTION TEST: Analyze the existing empty 1d ArrayRecord.
        
        This test analyzes the actual production issue:
        - Check the existing dataset_20250921_072901 that has the problem
        - Verify 5m timeframe has data (should work)
        - Verify 1d timeframe is empty (currently broken)
        - Document the exact issue for debugging
        """
        print("🔍 ANALYZING EXISTING 1D ARRAYRECORD ISSUE")
        
        # Reference the existing problematic dataset
        dataset_path = Path("/data/training_data/dataset_20250921_072901/AAPL_2025_07")
        
        print(f"📊 ANALYZING EXISTING DATASET:")
        print(f"   Dataset path: {dataset_path}")
        
        # Check all timeframes in the existing dataset
        expected_timeframes = ["5m", "15m", "1h", "1d", "1w"]
        timeframe_results = {}
        
        for timeframe in expected_timeframes:
            timeframe_dir = dataset_path / timeframe
            
            if timeframe_dir.exists():
                arrayrecord_files = list(timeframe_dir.glob("*.arrayrecord"))
                
                if arrayrecord_files:
                    arrayrecord_file = arrayrecord_files[0]
                    file_size = arrayrecord_file.stat().st_size
                    
                    # Try to read the file to count records using run_dev command
                    try:
                        import subprocess
                        result = subprocess.run([
                            "python3", "scripts/run_dev.py", "arrayrecord", 
                            "-f", str(arrayrecord_file)
                        ], capture_output=True, text=True, cwd="/home/jianjun/ats-genai-admin")
                        
                        if result.returncode == 0:
                            # Extract record count from output - look for "Total records: N"
                            record_count = 0
                            for line in result.stdout.split('\n'):
                                if "Total records:" in line:
                                    record_count = int(line.split(":")[1].strip())
                                    break
                                elif "❌ Empty ArrayRecord file" in line:
                                    record_count = 0
                                    break
                        else:
                            record_count = 0
                        
                        timeframe_results[timeframe] = {
                            "file_exists": True,
                            "file_size": file_size,
                            "record_count": record_count,
                            "file_path": str(arrayrecord_file),
                            "readable": record_count > 0,
                            "command_output": result.stdout
                        }
                    except Exception as e:
                        timeframe_results[timeframe] = {
                            "file_exists": True,
                            "file_size": file_size,
                            "record_count": 0,
                            "file_path": str(arrayrecord_file),
                            "readable": False,
                            "error": str(e)
                        }
                else:
                    timeframe_results[timeframe] = {
                        "file_exists": False,
                        "file_size": 0,
                        "record_count": 0,
                        "file_path": None,
                        "readable": False,
                        "error": "No ArrayRecord files found"
                    }
            else:
                timeframe_results[timeframe] = {
                    "file_exists": False,
                    "file_size": 0,
                    "record_count": 0,
                    "file_path": None,
                    "readable": False,
                    "error": "Timeframe directory missing"
                }
        
        # DEBUG: Print analysis results
        print(f"\n🔍 PRODUCTION ISSUE ANALYSIS:")
        for timeframe, result in timeframe_results.items():
            status = "✅" if result["readable"] and result["record_count"] > 0 else "❌"
            print(f"   {status} {timeframe:>3s}: {result['record_count']:>4} records, {result['file_size']:>8,} bytes")
            if not result["readable"] and "error" in result:
                print(f"       Error: {result['error']}")
        
        # DOCUMENT THE ISSUE
        
        # 5m should work (this should pass)
        assert timeframe_results["5m"]["file_exists"], "❌ 5m ArrayRecord file missing from production dataset"
        assert timeframe_results["5m"]["record_count"] > 0, "❌ 5m ArrayRecord file is empty in production dataset"
        print(f"✅ 5m timeframe working: {timeframe_results['5m']['record_count']} records")
        
        # 1d should work but currently fails (this assertion will document the bug)
        if timeframe_results["1d"]["record_count"] == 0:
            print(f"\n🚨 PRODUCTION BUG CONFIRMED: 1d ArrayRecord Issue")
            print(f"   📊 5m records: {timeframe_results['5m']['record_count']}")
            print(f"   📊 1d records: {timeframe_results['1d']['record_count']}")
            print(f"   📁 1d file exists: {timeframe_results['1d']['file_exists']}")
            print(f"   📏 1d file size: {timeframe_results['1d']['file_size']} bytes")
            print(f"   🎯 ISSUE: 1d ArrayRecord exists but reports as empty")
            
            # This assertion will fail, documenting the exact production bug
            assert False, f"❌ PRODUCTION BUG: 1d ArrayRecord empty ({timeframe_results['1d']['record_count']} records) while 5m has {timeframe_results['5m']['record_count']} records"
        
        # If we reach here, the issue is resolved
        assert timeframe_results["1d"]["record_count"] > 0, "❌ 1d ArrayRecord file is empty"
        print(f"✅ SUCCESS: 1d timeframe working: {timeframe_results['1d']['record_count']} records")

    async def test_data_availability_analysis_5m_vs_1d(
        self,
        test_environment,
        real_market_data_manager
    ):
        """
        ROOT CAUSE ANALYSIS: Check underlying minute data availability for 1d aggregation.
        
        This test determines if the issue is:
        1. Missing minute-level source data for July 2025 AAPL
        2. Timeframe aggregation logic bug (5m works, 1d fails)
        3. ArrayRecord writing logic bug specific to 1d timeframe
        """
        print("🔍 DATA AVAILABILITY ANALYSIS FOR 1D vs 5M ISSUE")
        
        symbol = "AAPL"
        start_date = datetime(2025, 7, 1, tzinfo=timezone.utc)
        end_date = datetime(2025, 7, 31, tzinfo=timezone.utc)
        
        print(f"📊 Analyzing {symbol} data from {start_date.date()} to {end_date.date()}")
        
        # Check minute-level source data availability
        try:
            minute_data = await real_market_data_manager.get_minute_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )
            minute_count = len(minute_data) if minute_data is not None else 0
            print(f"   📊 Raw minute data: {minute_count:,} records")
        except Exception as e:
            minute_count = 0
            print(f"   ❌ Raw minute data error: {e}")
        
        # Check 5m aggregated data (known working in production)
        try:
            data_5m = await real_market_data_manager.get_aggregated_data(
                symbol=symbol,
                timeframe="5m",
                start_date=start_date,
                end_date=end_date
            )
            count_5m = len(data_5m) if data_5m is not None else 0
            print(f"   📊 5m aggregated data: {count_5m:,} records")
        except Exception as e:
            count_5m = 0
            print(f"   ❌ 5m aggregated data error: {e}")
        
        # Check 1d aggregated data (potentially broken in production)
        try:
            data_1d = await real_market_data_manager.get_aggregated_data(
                symbol=symbol,
                timeframe="1d",
                start_date=start_date,
                end_date=end_date
            )
            count_1d = len(data_1d) if data_1d is not None else 0
            print(f"   📊 1d aggregated data: {count_1d:,} records")
        except Exception as e:
            count_1d = 0
            print(f"   ❌ 1d aggregated data error: {e}")
        
        # ANALYSIS RESULTS
        print(f"\n🔍 ROOT CAUSE ANALYSIS:")
        
        if minute_count == 0:
            print(f"   🚨 ROOT CAUSE: No minute-level source data available for {symbol}")
            assert False, f"Cannot reproduce issue - no source data for {symbol} in July 2025"
        
        if count_5m == 0:
            print(f"   🚨 UNEXPECTED: 5m aggregation also failing (should work in production)")
            assert False, f"5m aggregation failed - unexpected deviation from production"
        
        if count_1d == 0:
            print(f"   🚨 ROOT CAUSE CONFIRMED: 1d aggregation returns no data")
            print(f"      - {minute_count:,} minute records available as source")
            print(f"      - {count_5m:,} records successfully aggregated to 5m")
            print(f"      - 0 records aggregated to 1d (BUG)")
            print(f"   🎯 ISSUE: 1d timeframe aggregation logic broken")
        else:
            print(f"   ✅ 1d aggregation working: {count_1d} records")
            print(f"   🤔 Issue may be in ArrayRecord writing stage, not data aggregation")
        
        # Assert expected behavior based on production observation
        assert minute_count > 0, f"Source data required for analysis: found {minute_count} minute records"
        assert count_5m > 0, f"5m aggregation should work: found {count_5m} records"
        
        # This assertion should fail if the issue is in aggregation logic
        if count_1d == 0:
            assert False, f"❌ BUG CONFIRMED: 1d aggregation failed - {count_1d} records from {minute_count:,} minute records"

    def test_debug_training_data_generation_with_logging(self):
        """
        DEBUG TEST: Run training data generation with detailed logging.
        
        This test will run a small training data generation to capture the exact
        inputs being passed to the ArrayRecord writing logic for both working (5m)
        and failing (1d) timeframes.
        """
        print("🔍 RUNNING TRAINING DATA GENERATION WITH DEBUG LOGGING")
        
        # Run training data generation for a single day to minimize data
        # but capture both 5m and 1d writes with debug logging
        import subprocess
        import tempfile
        from pathlib import Path
        
        with tempfile.TemporaryDirectory() as temp_dir:
            print(f"📁 Using temporary output directory: {temp_dir}")
            
            # Run training data generation command with debug logging
            cmd = [
                "python3", "src/domains/ml/services/training_data/runners/training_data_callback_runner.py",
                "--symbols", "AAPL", 
                "--start-date", "2025-07-01",
                "--end-date", "2025-07-01", 
                "--environment", "dev",
                "--storage-format", "arrayrecord",
                "--output-dir", temp_dir,
                "--debug",
                "--gin-config", "config/training_data.gin",
                "--base-duration", "60m"
            ]
            
            print(f"🔧 Running command: {' '.join(cmd)}")
            
            # Set environment variables for database connection and logging
            env_vars = {
                **dict(os.environ),
                "PYTHONPATH": "src",
                "DB_HOST": "localhost",
                "DB_PORT": "3432", 
                "DB_USER": "postgres",
                "DB_PASSWORD": "dev_password",
                "DB_NAME": "dev_db",
                "ENVIRONMENT_TYPE": "dev",
                "LOGGING_LEVEL": "DEBUG"  # Enable debug logging
            }
            
            # Run the command and capture output
            try:
                result = subprocess.run(
                    cmd,
                    cwd="/home/jianjun/ats-genai-admin",
                    env=env_vars,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout
                )
                
                print(f"📊 Command return code: {result.returncode}")
                print(f"📊 Command stdout length: {len(result.stdout)} chars")
                print(f"📊 Command stderr length: {len(result.stderr)} chars")
                
                # Extract debug logging related to ArrayRecord writes
                debug_lines = []
                all_output = result.stdout + "\n" + result.stderr
                
                for line in all_output.split('\n'):
                    if "ARRAYRECORD WRITE DEBUG" in line or "ARRAYRECORD WRITE SUCCESS" in line or "ARRAYRECORD WRITE FAILURE" in line:
                        debug_lines.append(line)
                
                print(f"\n🔍 EXTRACTED DEBUG LINES ({len(debug_lines)} total):")
                for line in debug_lines:
                    print(f"   {line}")
                
                # Analyze the debug output to identify differences
                writes_5m = [line for line in debug_lines if "[5m]" in line]
                writes_1d = [line for line in debug_lines if "[1d]" in line]
                
                print(f"\n📊 ANALYSIS:")
                print(f"   5m writes captured: {len(writes_5m)}")
                print(f"   1d writes captured: {len(writes_1d)}")
                
                if writes_5m:
                    print(f"\n✅ 5M WRITES (first 3):")
                    for line in writes_5m[:3]:
                        print(f"   {line}")
                
                if writes_1d:
                    print(f"\n🔍 1D WRITES (first 3):")
                    for line in writes_1d[:3]:
                        print(f"   {line}")
                else:
                    print(f"\n❌ NO 1D WRITES CAPTURED - This indicates the issue occurs before writing")
                
                # Check if any files were actually created
                dataset_dirs = list(Path(temp_dir).glob("dataset_*"))
                if dataset_dirs:
                    dataset_dir = dataset_dirs[0]
                    aapl_dirs = list(dataset_dir.glob("*AAPL*"))
                    if aapl_dirs:
                        aapl_dir = aapl_dirs[0]
                        timeframe_dirs = [d for d in aapl_dir.iterdir() if d.is_dir()]
                        print(f"\n📁 Generated timeframe directories: {[d.name for d in timeframe_dirs]}")
                        
                        for tf_dir in timeframe_dirs:
                            arrayrecord_files = list(tf_dir.glob("*.arrayrecord"))
                            if arrayrecord_files:
                                file_size = arrayrecord_files[0].stat().st_size
                                print(f"   {tf_dir.name}: {file_size} bytes")
                
                # Save full output for manual analysis
                debug_log_file = Path(temp_dir) / "debug_output.log"
                with open(debug_log_file, 'w') as f:
                    f.write("STDOUT:\n")
                    f.write(result.stdout)
                    f.write("\n\nSTDERR:\n")
                    f.write(result.stderr)
                
                print(f"\n💾 Full debug output saved to: {debug_log_file}")
                
                return {
                    "return_code": result.returncode,
                    "debug_lines": debug_lines,
                    "writes_5m": writes_5m,
                    "writes_1d": writes_1d,
                    "full_output": all_output
                }
                
            except subprocess.TimeoutExpired:
                print("❌ Command timed out after 5 minutes")
                assert False, "Training data generation timed out"
            except Exception as e:
                print(f"❌ Error running training data generation: {e}")
                assert False, f"Failed to run training data generation: {e}"

    def test_timeframe_selection_logic_fix(self):
        """Test that the fix correctly generates 1d timeframes in historical training mode."""
        from datetime import datetime
        from unittest.mock import Mock
        
        print("🔍 TESTING TIMEFRAME SELECTION LOGIC FIX")
        
        # Import the actual method from the training callback
        import sys
        sys.path.insert(0, 'src')
        from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
        
        # Create real callback instance for testing (historical mode)
        real_callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 12),
            output_dir='/tmp/test'
        )
        
        # Test historical training mode detection
        is_historical = real_callback._is_historical_training_mode()
        print(f"📊 Historical training mode detected: {is_historical}")
        assert is_historical == True, "Should detect historical training mode when start_date and end_date are set"
        
        # Test timeframe selection at 7:30 AM (5-minute aligned, like production time)
        test_time = datetime(2025, 7, 1, 7, 30, 0)  # 7:30 AM, 5-minute aligned
        print(f"📊 Testing timeframe selection at: {test_time}")
        
        # Test that 1d timeframe is now included in historical mode at any time
        # Pass datetime directly to the method, not a dictionary
        timeframes = real_callback._get_target_timeframes_for_interval(test_time)
        print(f"📊 Generated timeframes: {timeframes}")
        
        # Verify 1d is included (this was the bug - it was missing in historical mode)
        assert '1d' in timeframes, f"❌ 1d timeframe should be included in historical training mode at 7:30 AM. Got timeframes: {timeframes}"
        
        # Verify other timeframes are still included at 5-minute aligned time
        assert '5m' in timeframes, "❌ 5m timeframe should be included at 5-minute aligned time"
        
        print(f"✅ FIX VERIFIED: 1d timeframe now generated in historical mode at 7:30 AM")
        print(f"✅ Generated timeframes: {timeframes}")
        
        # Test real-time mode (start_date/end_date = None)
        realtime_callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date=None,
            end_date=None,
            output_dir='/tmp/test'
        )
        
        is_realtime = realtime_callback._is_historical_training_mode()
        print(f"📊 Real-time mode detected: {not is_realtime}")
        assert is_realtime == False, "Should detect real-time mode when start_date and end_date are None"
        
        # Test that 1d timeframe is NOT included at 7:30 AM in real-time mode
        realtime_timeframes = realtime_callback._get_target_timeframes_for_interval(test_time)
        print(f"📊 Real-time generated timeframes at 7:30 AM: {realtime_timeframes}")
        assert '1d' not in realtime_timeframes, f"❌ 1d timeframe should NOT be included in real-time mode at 7:30 AM. Got timeframes: {realtime_timeframes}"
        
        # Test that 1d timeframe IS included at midnight in real-time mode
        midnight_time = datetime(2025, 7, 1, 0, 0, 0)  # Midnight
        midnight_timeframes = realtime_callback._get_target_timeframes_for_interval(midnight_time)
        print(f"📊 Real-time generated timeframes at midnight: {midnight_timeframes}")
        assert '1d' in midnight_timeframes, f"❌ 1d timeframe SHOULD be included in real-time mode at midnight. Got timeframes: {midnight_timeframes}"
        
        print(f"✅ COMPLETE FIX VERIFIED:")
        print(f"   ✅ Historical mode: 1d timeframe generated at any time")
        print(f"   ✅ Real-time mode: 1d timeframe only at midnight")


if __name__ == "__main__":
    """
    Run this test to reproduce the 1d ArrayRecord empty issue.
    
    Expected outcome: Test will fail, confirming the bug exists.
    The failure will pinpoint whether the issue is in:
    1. Data aggregation (1d timeframe logic)
    2. ArrayRecord file writing 
    3. Source data availability
    """
    print("🔍 RUNNING 1D ARRAYRECORD REPRODUCTION TEST")
    print("=" * 60)
    print("Expected: This test WILL FAIL, reproducing the production bug")
    print("Goal: Identify root cause for debugging and fixing")
    print("=" * 60)
    
    # Run with verbose output for debugging
    import sys
    sys.exit(pytest.main([__file__, "-v", "--tb=long", "-s"]))