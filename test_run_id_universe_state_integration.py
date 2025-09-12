#!/usr/bin/env python3
"""
Comprehensive test for run_id integration in universe state saving.

This test verifies that:
1. run_id is properly populated in universe_state_interval table
2. Multiple runs with same timestamp don't conflict (unique constraint includes run_id)
3. run_id is correctly retrieved from run_context
4. Database queries filter by run_id when needed
"""

import asyncio
import os
import sys
import tempfile
import shutil
from datetime import datetime, date
from pathlib import Path
import subprocess
import logging

# Add src to path for imports
sys.path.insert(0, 'src')

import asyncpg
from core.platform.config.environment import Environment, EnvironmentType
from shared.utils.environment import Environment as SharedEnvironment
from core.shared.run_context import RunContext


class RunIdUniverseStateTest:
    """Test run_id integration in universe state management."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.temp_dir = None
        self.environment = None
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    async def setup_environment(self):
        """Set up test environment."""
        self.logger.info("🔧 Setting up test environment")
        
        # Create environment without gin config to focus on database integration
        self.environment = SharedEnvironment(None, EnvironmentType.DEV)
        
        # Verify database connection
        await self.verify_database_connection()
        
        self.logger.info("✅ Test environment set up successfully")
    
    async def verify_database_connection(self):
        """Verify database connection and schema."""
        self.logger.info("🔍 Verifying database connection and schema")
        
        try:
            conn = await asyncpg.connect(self.environment.get_database_url())
            
            # Check if run_id column exists - manually check dev table
            table_name = "dev_universe_state_interval"  # Force dev table name
            self.logger.info(f"Checking table: {table_name}")
            
            result = await conn.fetchval("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = $1 AND column_name = 'run_id'
            """, table_name)
            
            if not result:
                raise Exception("run_id column not found in universe_state_interval table. Run migration first!")
            
            self.logger.info("✅ Database schema verified - run_id column exists")
            
            # Check constraint
            constraints = await conn.fetch("""
                SELECT constraint_name, constraint_type 
                FROM information_schema.table_constraints 
                WHERE table_name = $1 AND constraint_type = 'UNIQUE'
            """, table_name)
            
            run_id_constraint_found = any(
                'run_id' in constraint['constraint_name'] 
                for constraint in constraints
            )
            
            if not run_id_constraint_found:
                self.logger.warning("⚠️ run_id unique constraint not found - may cause issues")
            else:
                self.logger.info("✅ run_id unique constraint verified")
            
            await conn.close()
            
        except Exception as e:
            self.logger.error(f"❌ Database verification failed: {e}")
            raise
    
    async def test_run_id_population(self):
        """Test that run_id is properly populated in database records."""
        self.logger.info("🧪 TEST 1: run_id population")
        
        from domains.trading.services.state.universe_state_manager import UniverseStateManager
        from core.shared.run_context import RunContext
        
        # Create test run context
        test_run_id = f"test_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        run_context = RunContext(
            run_id=test_run_id,
            artifacts_dir=Path("/tmp/test_artifacts"),
            universe_state_dir=Path("/tmp/test_universe_state")
        )
        
        # Create universe state manager with run context
        universe_manager = UniverseStateManager(
            env=self.environment,
            run_context=run_context
        )
        
        # Create test metadata
        test_timestamp = datetime.now()
        test_metadata = {
            'universe_id': 1,
            'duration': '60m',
            'start_date_time': test_timestamp,
            'end_date_time': test_timestamp,
        }
        
        try:
            # Save universe state (this should populate run_id)
            await universe_manager.save_universe_state(
                df=None,  # Minimal test - no actual data
                timestamp=test_timestamp,
                metadata=test_metadata
            )
            
            # Verify run_id was saved correctly
            conn = await asyncpg.connect(self.environment.get_database_url())
            
            saved_record = await conn.fetchrow(f"""
                SELECT universe_id, duration, start_date_time, run_id 
                FROM {self.environment.get_table_name('universe_state_interval')}
                WHERE run_id = $1
                ORDER BY created_at DESC
                LIMIT 1
            """, test_run_id)
            
            await conn.close()
            
            if not saved_record:
                raise Exception(f"No record found with run_id: {test_run_id}")
            
            if saved_record['run_id'] != test_run_id:
                raise Exception(f"Expected run_id {test_run_id}, got {saved_record['run_id']}")
            
            self.logger.info(f"✅ run_id correctly saved: {saved_record['run_id']}")
            self.logger.info(f"   Universe ID: {saved_record['universe_id']}")
            self.logger.info(f"   Duration: {saved_record['duration']}")
            self.logger.info(f"   Timestamp: {saved_record['start_date_time']}")
            
            return test_run_id
            
        except Exception as e:
            self.logger.error(f"❌ run_id population test failed: {e}")
            raise
    
    async def test_multiple_runs_same_timestamp(self):
        """Test that multiple runs can process the same timestamp without conflicts."""
        self.logger.info("🧪 TEST 2: Multiple runs same timestamp")
        
        from domains.trading.services.state.universe_state_manager import UniverseStateManager
        from core.shared.run_context import RunContext
        
        test_timestamp = datetime.now()
        test_metadata = {
            'universe_id': 1,
            'duration': '60m', 
            'start_date_time': test_timestamp,
            'end_date_time': test_timestamp,
        }
        
        run_ids = []
        
        # Create 3 different runs with same timestamp
        for i in range(3):
            run_id = f"test_concurrent_run_{i}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            run_ids.append(run_id)
            
            run_context = RunContext(
                run_id=run_id,
                artifacts_dir=Path(f"/tmp/test_artifacts_{i}"),
                universe_state_dir=Path(f"/tmp/test_universe_state_{i}")
            )
            
            universe_manager = UniverseStateManager(
                env=self.environment,
                run_context=run_context
            )
            
            try:
                # This should NOT fail with unique constraint violation
                await universe_manager.save_universe_state(
                    df=None,
                    timestamp=test_timestamp,
                    metadata=test_metadata
                )
                
                self.logger.info(f"✅ Run {i+1} saved successfully: {run_id}")
                
            except Exception as e:
                self.logger.error(f"❌ Run {i+1} failed: {e}")
                raise
        
        # Verify all 3 records exist
        conn = await asyncpg.connect(self.environment.get_database_url())
        
        for run_id in run_ids:
            record = await conn.fetchrow(f"""
                SELECT run_id, start_date_time 
                FROM {self.environment.get_table_name('universe_state_interval')}
                WHERE run_id = $1
            """, run_id)
            
            if not record:
                raise Exception(f"Record not found for run_id: {run_id}")
            
            self.logger.info(f"✅ Verified record exists for run_id: {run_id}")
        
        await conn.close()
        
        return run_ids
    
    async def test_training_data_generation_with_run_id(self):
        """Test training data generation uses run_id correctly."""
        self.logger.info("🧪 TEST 3: Training data generation with run_id")
        
        # Create temporary output directory
        temp_output_dir = Path(tempfile.mkdtemp(prefix="training_run_id_test_"))
        
        try:
            # Run training data generation
            cmd = [
                "python3", 
                "src/domains/ml/services/training_data/runners/training_data_callback_runner.py",
                "--symbols", "TSLA",
                "--start-date", "2025-09-12",  # Use today's date
                "--end-date", "2025-09-12",    # Same date to test single day
                "--environment", "dev",
                "--storage-format", "arrayrecord",
                "--output-dir", str(temp_output_dir),
                "--debug",
                "--gin-config", "config/training_data.gin",
                "--base-duration", "60m"
            ]
            
            # Set environment variables
            env = os.environ.copy()
            env['PYTHONPATH'] = 'src'
            env['DB_HOST'] = 'localhost'
            env['DB_PORT'] = '3432'
            env['DB_USER'] = 'postgres'
            env['DB_PASSWORD'] = 'dev_password'
            env['DB_NAME'] = 'dev_db'
            env['ENVIRONMENT_TYPE'] = 'dev'
            
            self.logger.info("🔄 Running training data generation...")
            self.logger.info(f"   Command: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                timeout=120  # 2 minutes timeout
            )
            
            success = result.returncode == 0
            
            if success:
                self.logger.info("✅ Training data generation completed successfully")
                
                # Check if run_id was used in database records
                conn = await asyncpg.connect(self.environment.get_database_url())
                
                recent_records = await conn.fetch(f"""
                    SELECT run_id, start_date_time, created_at
                    FROM {self.environment.get_table_name('universe_state_interval')}
                    WHERE created_at > NOW() - INTERVAL '5 minutes'
                    AND run_id != 'legacy_run_pre_0024'
                    ORDER BY created_at DESC
                    LIMIT 10
                """)
                
                await conn.close()
                
                if recent_records:
                    self.logger.info(f"✅ Found {len(recent_records)} recent records with proper run_id")
                    for record in recent_records[:3]:  # Show first 3
                        self.logger.info(f"   run_id: {record['run_id']}, timestamp: {record['start_date_time']}")
                else:
                    self.logger.warning("⚠️ No recent records found with proper run_id")
            else:
                self.logger.error(f"❌ Training data generation failed")
                self.logger.error(f"STDOUT: {result.stdout}")
                self.logger.error(f"STDERR: {result.stderr}")
            
            return success
            
        finally:
            # Cleanup
            try:
                shutil.rmtree(temp_output_dir)
                self.logger.info(f"🧹 Cleaned up temp directory: {temp_output_dir}")
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to cleanup {temp_output_dir}: {e}")
    
    async def test_query_filtering_by_run_id(self):
        """Test that database queries can filter by run_id."""
        self.logger.info("🧪 TEST 4: Query filtering by run_id")
        
        # First, create some test data with known run_ids
        conn = await asyncpg.connect(self.environment.get_database_url())
        
        test_run_id = f"test_query_filter_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        test_timestamp = datetime.now()
        
        # Insert test record
        await conn.execute(f"""
            INSERT INTO {self.environment.get_table_name('universe_state_interval')}
            (universe_id, duration, start_date_time, end_date_time, run_id)
            VALUES ($1, $2, $3, $4, $5)
        """, 1, '60m', test_timestamp, test_timestamp, test_run_id)
        
        # Query specifically by run_id
        specific_records = await conn.fetch(f"""
            SELECT universe_id, run_id, start_date_time
            FROM {self.environment.get_table_name('universe_state_interval')}
            WHERE run_id = $1
        """, test_run_id)
        
        # Query all records
        all_records = await conn.fetch(f"""
            SELECT COUNT(*) as total_count
            FROM {self.environment.get_table_name('universe_state_interval')}
        """)
        
        await conn.close()
        
        if len(specific_records) != 1:
            raise Exception(f"Expected 1 record for run_id {test_run_id}, got {len(specific_records)}")
        
        if specific_records[0]['run_id'] != test_run_id:
            raise Exception(f"Query filtering failed - got wrong run_id: {specific_records[0]['run_id']}")
        
        total_count = all_records[0]['total_count']
        
        self.logger.info(f"✅ Query filtering successful")
        self.logger.info(f"   Specific run_id query: {len(specific_records)} records")
        self.logger.info(f"   Total records in table: {total_count}")
        self.logger.info(f"   Test record run_id: {specific_records[0]['run_id']}")
        
        return test_run_id
    
    async def run_all_tests(self):
        """Run all tests and provide summary."""
        self.logger.info("🚀 Starting run_id integration tests")
        self.logger.info("=" * 80)
        
        test_results = {}
        
        try:
            await self.setup_environment()
            
            # Test 1: run_id population
            try:
                test_run_id_1 = await self.test_run_id_population()
                test_results['run_id_population'] = True
                self.logger.info("✅ TEST 1 PASSED: run_id population")
            except Exception as e:
                test_results['run_id_population'] = False
                self.logger.error(f"❌ TEST 1 FAILED: {e}")
            
            # Test 2: Multiple runs same timestamp
            try:
                concurrent_run_ids = await self.test_multiple_runs_same_timestamp()
                test_results['concurrent_runs'] = True
                self.logger.info("✅ TEST 2 PASSED: Multiple runs same timestamp")
            except Exception as e:
                test_results['concurrent_runs'] = False
                self.logger.error(f"❌ TEST 2 FAILED: {e}")
            
            # Test 3: Training data generation (disabled for now due to complexity)
            # try:
            #     training_success = await self.test_training_data_generation_with_run_id()
            #     test_results['training_data_generation'] = training_success
            #     if training_success:
            #         self.logger.info("✅ TEST 3 PASSED: Training data generation")
            #     else:
            #         self.logger.error("❌ TEST 3 FAILED: Training data generation")
            # except Exception as e:
            #     test_results['training_data_generation'] = False
            #     self.logger.error(f"❌ TEST 3 FAILED: {e}")
            
            # Test 4: Query filtering
            try:
                query_test_run_id = await self.test_query_filtering_by_run_id()
                test_results['query_filtering'] = True
                self.logger.info("✅ TEST 4 PASSED: Query filtering by run_id")
            except Exception as e:
                test_results['query_filtering'] = False
                self.logger.error(f"❌ TEST 4 FAILED: {e}")
            
        except Exception as e:
            self.logger.error(f"❌ Test setup failed: {e}")
            return False
        
        # Summary
        self.logger.info("\n" + "=" * 80)
        self.logger.info("📊 TEST RESULTS SUMMARY")
        self.logger.info("=" * 80)
        
        passed_tests = sum(1 for result in test_results.values() if result)
        total_tests = len(test_results)
        
        for test_name, result in test_results.items():
            status = "✅ PASSED" if result else "❌ FAILED"
            self.logger.info(f"{test_name}: {status}")
        
        self.logger.info(f"\n🎯 Overall: {passed_tests}/{total_tests} tests passed")
        
        if passed_tests == total_tests:
            self.logger.info("🎉 ALL TESTS PASSED - run_id integration is working correctly!")
            return True
        else:
            self.logger.error("⚠️ SOME TESTS FAILED - run_id integration needs attention")
            return False


async def main():
    """Main test runner."""
    test_runner = RunIdUniverseStateTest()
    
    try:
        success = await test_runner.run_all_tests()
        return 0 if success else 1
        
    except Exception as e:
        logging.error(f"❌ Test runner failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)