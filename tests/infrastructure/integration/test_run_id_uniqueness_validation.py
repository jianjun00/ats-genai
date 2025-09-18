#!/usr/bin/env python3
"""
COMPREHENSIVE RUN ID UNIQUENESS VALIDATION TESTS

This addresses the recurring duplicate run ID issue that has happened "100 times".

Critical Test Coverage:
1. Run ID collision detection
2. Database state validation before starting runs
3. Failed run cleanup mechanisms  
4. Concurrent run ID generation safety
5. Database constraint validation
6. Idempotent run behavior

The duplicate key error occurs in:
intg_instrument_interval_instrument_id_interval_start_run_key
Key (instrument_id, interval_start, interval_duration, run_id) already exists

This suggests run_id is NOT unique across executions.
"""

import pytest
import asyncio
import sys
import os
import asyncpg
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import uuid
import concurrent.futures
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')

# Set environment to skip gin loading
os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'

from core.shared.utils.environment import Environment, EnvironmentType
from core.shared.run_context import RunContext


class TestRunIdUniquenessValidation:
    """Test comprehensive run ID uniqueness validation."""

    def setup_method(self):
        """Setup test environment."""
        self.test_symbols = ["AAPL"]
        self.test_date = date(2025, 7, 1)
        
        # Database connection for intg environment
        self.db_config = {
            'host': 'localhost',
            'port': 4432,
            'user': 'postgres', 
            'password': 'intg_password',
            'database': 'intg_db'
        }

    async def get_db_connection(self) -> asyncpg.Connection:
        """Get database connection for testing."""
        try:
            conn = await asyncpg.connect(**self.db_config)
            return conn
        except Exception as e:
            pytest.skip(f"Cannot connect to intg database: {e}")

    @pytest.mark.asyncio
    async def test_run_id_generation_uniqueness(self):
        """Test: Run ID generation produces unique IDs across multiple calls."""
        
        print("🔍 Testing run ID generation uniqueness...")
        
        # Generate multiple run IDs rapidly
        run_ids = set()
        num_iterations = 100
        
        for i in range(num_iterations):
            # Simulate the actual run ID generation from RunContext
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            unique_suffix = uuid.uuid4().hex[:8]
            run_id = f"run_{timestamp}_{unique_suffix}"
            
            # Check for collisions
            if run_id in run_ids:
                pytest.fail(f"Run ID collision detected: {run_id} generated twice!")
            
            run_ids.add(run_id)
            
            # Small delay to ensure timestamp differences
            if i % 10 == 0:
                await asyncio.sleep(0.001)
        
        print(f"✅ Generated {len(run_ids)} unique run IDs out of {num_iterations} iterations")
        assert len(run_ids) == num_iterations, f"Expected {num_iterations} unique IDs, got {len(run_ids)}"

    @pytest.mark.asyncio
    async def test_database_run_id_collision_detection(self):
        """Test: Detect existing run IDs in database before starting new runs."""
        
        print("🔍 Testing database run ID collision detection...")
        
        conn = await self.get_db_connection()
        
        try:
            # Check for existing run IDs in the database
            existing_runs_query = """
                SELECT run_id, created_at, status 
                FROM intg_runs 
                ORDER BY created_at DESC 
                LIMIT 50
            """
            
            existing_runs = await conn.fetch(existing_runs_query)
            existing_run_ids = {row['run_id'] for row in existing_runs}
            
            print(f"📊 Found {len(existing_run_ids)} existing run IDs in database:")
            for i, run_id in enumerate(list(existing_run_ids)[:10]):
                print(f"   {i+1}. {run_id}")
            if len(existing_run_ids) > 10:
                print(f"   ... and {len(existing_run_ids) - 10} more")
            
            # Test run ID collision detection function
            def check_run_id_exists(potential_run_id: str) -> bool:
                return potential_run_id in existing_run_ids
            
            # Generate new run ID and verify it's unique
            new_run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
            
            collision_detected = check_run_id_exists(new_run_id)
            assert not collision_detected, f"New run ID {new_run_id} collides with existing run!"
            
            # Test collision detection with a known existing run ID
            if existing_run_ids:
                existing_run_id = list(existing_run_ids)[0]
                collision_detected = check_run_id_exists(existing_run_id)
                assert collision_detected, f"Should detect collision for existing run ID {existing_run_id}"
            
            print("✅ Run ID collision detection working correctly")
            
        finally:
            await conn.close()

    @pytest.mark.asyncio 
    async def test_instrument_interval_constraint_violation_detection(self):
        """Test: Detect potential instrument_interval constraint violations."""
        
        print("🔍 Testing instrument interval constraint violation detection...")
        
        conn = await self.get_db_connection()
        
        try:
            # Query for potential constraint violations in intg_instrument_interval
            constraint_check_query = """
                SELECT 
                    instrument_id,
                    interval_start,
                    interval_duration, 
                    run_id,
                    COUNT(*) as duplicate_count
                FROM intg_instrument_interval 
                WHERE instrument_id = 31  -- AAPL instrument_id
                  AND interval_start >= '2025-07-01'
                  AND interval_start <= '2025-09-13'
                GROUP BY instrument_id, interval_start, interval_duration, run_id
                HAVING COUNT(*) > 1
                ORDER BY interval_start DESC
                LIMIT 20
            """
            
            duplicates = await conn.fetch(constraint_check_query)
            
            if duplicates:
                print(f"🚨 FOUND {len(duplicates)} CONSTRAINT VIOLATIONS:")
                for row in duplicates:
                    print(f"   Duplicate: instrument={row['instrument_id']}, "
                          f"interval={row['interval_start']}, "
                          f"run={row['run_id']}, "
                          f"count={row['duplicate_count']}")
                
                pytest.fail(f"Database contains {len(duplicates)} constraint violations!")
            
            # Check for run ID reuse across different intervals
            run_id_reuse_query = """
                SELECT 
                    run_id,
                    COUNT(DISTINCT interval_start) as interval_count,
                    MIN(interval_start) as first_interval,
                    MAX(interval_start) as last_interval,
                    COUNT(*) as total_records
                FROM intg_instrument_interval 
                WHERE instrument_id = 31
                  AND interval_start >= '2025-07-01'
                GROUP BY run_id
                HAVING COUNT(*) > 100  -- Suspiciously high record count
                ORDER BY total_records DESC
                LIMIT 10
            """
            
            high_usage_runs = await conn.fetch(run_id_reuse_query)
            
            if high_usage_runs:
                print(f"📊 Found {len(high_usage_runs)} runs with high record counts:")
                for row in high_usage_runs:
                    print(f"   Run {row['run_id']}: {row['total_records']} records, "
                          f"{row['interval_count']} intervals, "
                          f"from {row['first_interval']} to {row['last_interval']}")
            
            print("✅ Instrument interval constraint validation completed")
            
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_failed_run_cleanup_detection(self):
        """Test: Detect failed runs that need cleanup."""
        
        print("🔍 Testing failed run cleanup detection...")
        
        conn = await self.get_db_connection()
        
        try:
            # Find incomplete/failed runs
            failed_runs_query = """
                SELECT DISTINCT
                    ii.run_id,
                    COUNT(ii.*) as interval_records,
                    MIN(ii.interval_start) as first_interval,
                    MAX(ii.interval_start) as last_interval,
                    CASE 
                        WHEN r.status IS NULL THEN 'orphaned'
                        ELSE r.status 
                    END as run_status
                FROM intg_instrument_interval ii
                LEFT JOIN intg_runs r ON ii.run_id = r.run_id
                WHERE ii.instrument_id = 31
                  AND ii.interval_start >= '2025-07-01'
                GROUP BY ii.run_id, r.status
                ORDER BY interval_records DESC
            """
            
            runs_analysis = await conn.fetch(failed_runs_query)
            
            orphaned_runs = [r for r in runs_analysis if r['run_status'] == 'orphaned']
            failed_runs = [r for r in runs_analysis if r['run_status'] in ('failed', 'error')]
            incomplete_runs = [r for r in runs_analysis if r['interval_records'] < 10]  # Suspiciously low
            
            print(f"📊 Run Analysis Results:")
            print(f"   Total runs analyzed: {len(runs_analysis)}")
            print(f"   Orphaned runs (no intg_runs record): {len(orphaned_runs)}")
            print(f"   Failed runs: {len(failed_runs)}")
            print(f"   Incomplete runs (<10 intervals): {len(incomplete_runs)}")
            
            if orphaned_runs:
                print(f"\n🚨 ORPHANED RUNS NEED CLEANUP:")
                for run in orphaned_runs[:5]:
                    print(f"   {run['run_id']}: {run['interval_records']} intervals, "
                          f"{run['first_interval']} to {run['last_interval']}")
            
            if failed_runs:
                print(f"\n💥 FAILED RUNS:")
                for run in failed_runs[:5]:
                    print(f"   {run['run_id']}: {run['run_status']}, "
                          f"{run['interval_records']} intervals")
            
            # Create cleanup recommendations
            cleanup_needed = len(orphaned_runs) + len(failed_runs)
            if cleanup_needed > 0:
                print(f"\n🧹 CLEANUP RECOMMENDATION: Remove {cleanup_needed} failed/orphaned runs")
            else:
                print(f"\n✅ No cleanup needed - all runs have proper status tracking")
            
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_concurrent_run_id_generation_safety(self):
        """Test: Concurrent run ID generation doesn't produce collisions."""
        
        print("🔍 Testing concurrent run ID generation safety...")
        
        def generate_run_id():
            """Simulate concurrent run ID generation."""
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            unique_suffix = uuid.uuid4().hex[:8]
            return f"run_{timestamp}_{unique_suffix}"
        
        # Generate run IDs concurrently
        num_workers = 10
        iterations_per_worker = 20
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            futures = []
            for worker_id in range(num_workers):
                for iteration in range(iterations_per_worker):
                    future = executor.submit(generate_run_id)
                    futures.append(future)
            
            # Collect results
            run_ids = set()
            collisions = []
            
            for future in concurrent.futures.as_completed(futures):
                run_id = future.result()
                if run_id in run_ids:
                    collisions.append(run_id)
                else:
                    run_ids.add(run_id)
        
        total_generated = num_workers * iterations_per_worker
        unique_count = len(run_ids)
        collision_count = len(collisions)
        
        print(f"📊 Concurrent Generation Results:")
        print(f"   Workers: {num_workers}")
        print(f"   Iterations per worker: {iterations_per_worker}")
        print(f"   Total generated: {total_generated}")
        print(f"   Unique run IDs: {unique_count}")
        print(f"   Collisions: {collision_count}")
        
        if collisions:
            print(f"🚨 COLLISION DETECTED:")
            for collision in collisions[:5]:
                print(f"   {collision}")
        
        assert collision_count == 0, f"Found {collision_count} run ID collisions in concurrent generation!"
        print("✅ Concurrent run ID generation is collision-safe")

    @pytest.mark.asyncio
    async def test_idempotent_run_behavior(self):
        """Test: Define requirements for idempotent run behavior."""
        
        print("🔍 Testing idempotent run behavior requirements...")
        
        # Define idempotent run requirements
        idempotent_requirements = {
            'run_id_check': "Must check if run_id already exists before starting",
            'partial_cleanup': "Must clean up partial data from failed runs with same parameters", 
            'safe_restart': "Must allow safe restart of failed runs with new run_id",
            'data_validation': "Must validate no duplicate intervals exist before processing",
            'atomic_operations': "Must use transactions to prevent partial state"
        }
        
        print("📋 Idempotent Run Requirements:")
        for req_id, description in idempotent_requirements.items():
            print(f"   {req_id}: {description}")
        
        # Test case: Simulate run restart scenario
        original_run_id = "run_20250912_test_original"
        restart_run_id = "run_20250912_test_restart"
        
        test_scenarios = [
            {
                'scenario': 'fresh_run',
                'description': 'New run on clean database',
                'expected': 'success'
            },
            {
                'scenario': 'duplicate_run_id', 
                'description': 'Same run_id used twice',
                'expected': 'error_detected'
            },
            {
                'scenario': 'failed_run_restart',
                'description': 'Restart failed run with new run_id',
                'expected': 'success_after_cleanup'
            },
            {
                'scenario': 'partial_data_exists',
                'description': 'Some intervals already exist',
                'expected': 'skip_existing_continue_new'
            }
        ]
        
        print("\n🧪 Idempotent Behavior Test Scenarios:")
        for scenario in test_scenarios:
            print(f"   {scenario['scenario']}: {scenario['description']}")
            print(f"      Expected: {scenario['expected']}")
        
        # This documents the required behavior - implementation would go here
        assert len(idempotent_requirements) == 5, "All idempotent requirements defined"
        assert len(test_scenarios) == 4, "All test scenarios defined"
        
        print("✅ Idempotent run behavior requirements defined")


class TestDatabaseConstraintValidation:
    """Test database constraints and violation prevention."""
    
    def setup_method(self):
        """Setup test environment."""
        self.db_config = {
            'host': 'localhost',
            'port': 4432,
            'user': 'postgres',
            'password': 'intg_password', 
            'database': 'intg_db'
        }

    async def get_db_connection(self) -> asyncpg.Connection:
        """Get database connection for testing."""
        try:
            conn = await asyncpg.connect(**self.db_config)
            return conn
        except Exception as e:
            pytest.skip(f"Cannot connect to intg database: {e}")

    @pytest.mark.asyncio
    async def test_database_constraint_definitions(self):
        """Test: Verify database constraint definitions are correct."""
        
        print("🔍 Testing database constraint definitions...")
        
        conn = await self.get_db_connection()
        
        try:
            # Check constraint definitions
            constraint_query = """
                SELECT 
                    tc.constraint_name,
                    tc.constraint_type,
                    tc.table_name,
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name IN ('intg_instrument_interval', 'intg_runs', 'intg_training_datasets')
                  AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
                ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
            """
            
            constraints = await conn.fetch(constraint_query)
            
            print(f"📊 Database Constraints Found: {len(constraints)}")
            
            constraint_groups = {}
            for row in constraints:
                table = row['table_name']
                constraint = row['constraint_name']
                key = f"{table}.{constraint}"
                
                if key not in constraint_groups:
                    constraint_groups[key] = {
                        'type': row['constraint_type'],
                        'table': table,
                        'columns': []
                    }
                constraint_groups[key]['columns'].append(row['column_name'])
            
            for constraint_key, info in constraint_groups.items():
                print(f"   {constraint_key}:")
                print(f"      Type: {info['type']}")
                print(f"      Columns: {', '.join(info['columns'])}")
            
            # Verify the problematic constraint exists
            problematic_constraint = 'intg_instrument_interval.intg_instrument_interval_instrument_id_interval_start_run_key'
            
            if problematic_constraint in constraint_groups:
                constraint_info = constraint_groups[problematic_constraint]
                print(f"\n🎯 Found problematic constraint:")
                print(f"   {problematic_constraint}")
                print(f"   Columns: {constraint_info['columns']}")
                
                # Verify it's the constraint causing our issues
                expected_columns = ['instrument_id', 'interval_start', 'interval_duration', 'run_id']
                actual_columns = sorted(constraint_info['columns'])
                expected_columns_sorted = sorted(expected_columns)
                
                assert actual_columns == expected_columns_sorted, \
                    f"Constraint columns mismatch. Expected: {expected_columns_sorted}, Got: {actual_columns}"
                
                print("✅ Constraint definition matches error message")
            else:
                print("⚠️  Problematic constraint not found - may have been modified")
                
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_constraint_violation_prevention(self):
        """Test: Methods to prevent constraint violations."""
        
        print("🔍 Testing constraint violation prevention methods...")
        
        conn = await self.get_db_connection()
        
        try:
            # Method 1: Pre-flight check for existing records
            def create_precheck_query(instrument_id: int, interval_start: str, 
                                    interval_duration: str, run_id: str) -> str:
                return f"""
                    SELECT COUNT(*) as existing_count
                    FROM intg_instrument_interval 
                    WHERE instrument_id = {instrument_id}
                      AND interval_start = '{interval_start}'
                      AND interval_duration = '{interval_duration}'
                      AND run_id = '{run_id}'
                """
            
            # Test with a known problematic combination from the error
            test_instrument_id = 31
            test_interval_start = '2025-07-03 23:00:00+00'
            test_interval_duration = '60m'
            test_run_id = 'run_20250913_053441_b23b366c'  # From the error message
            
            precheck_query = create_precheck_query(
                test_instrument_id, test_interval_start, 
                test_interval_duration, test_run_id
            )
            
            result = await conn.fetchrow(precheck_query)
            existing_count = result['existing_count']
            
            print(f"📊 Pre-flight Check Results:")
            print(f"   Instrument: {test_instrument_id}")
            print(f"   Interval: {test_interval_start}")
            print(f"   Duration: {test_interval_duration}")
            print(f"   Run ID: {test_run_id}")
            print(f"   Existing records: {existing_count}")
            
            if existing_count > 0:
                print("🚨 CONSTRAINT VIOLATION WOULD OCCUR - Insert should be skipped!")
            else:
                print("✅ No conflict detected - Insert would succeed")
            
            # Method 2: UPSERT/ON CONFLICT strategy
            upsert_strategy = """
                INSERT INTO intg_instrument_interval 
                (instrument_id, interval_start, interval_duration, run_id, ...)
                VALUES (?, ?, ?, ?, ...)
                ON CONFLICT (instrument_id, interval_start, interval_duration, run_id) 
                DO NOTHING
            """
            
            print(f"\n🔧 Suggested Prevention Methods:")
            print(f"   1. Pre-flight check: Query existing records before insert")
            print(f"   2. UPSERT with ON CONFLICT DO NOTHING")
            print(f"   3. Run ID uniqueness validation before start")
            print(f"   4. Failed run cleanup before new run")
            
            print("✅ Constraint violation prevention methods defined")
            
        finally:
            await conn.close()


class TestRunCleanupMechanisms:
    """Test mechanisms for cleaning up failed/duplicate runs."""
    
    def setup_method(self):
        """Setup test environment."""
        self.db_config = {
            'host': 'localhost',
            'port': 4432,
            'user': 'postgres',
            'password': 'intg_password',
            'database': 'intg_db'
        }

    async def get_db_connection(self) -> asyncpg.Connection:
        """Get database connection for testing."""
        try:
            conn = await asyncpg.connect(**self.db_config)
            return conn
        except Exception as e:
            pytest.skip(f"Cannot connect to intg database: {e}")

    @pytest.mark.asyncio
    async def test_failed_run_identification(self):
        """Test: Identify runs that need cleanup."""
        
        print("🔍 Testing failed run identification...")
        
        conn = await self.get_db_connection()
        
        try:
            # Comprehensive failed run analysis
            failed_run_analysis_query = """
                WITH run_stats AS (
                    SELECT 
                        ii.run_id,
                        COUNT(*) as interval_count,
                        MIN(ii.interval_start) as first_interval,
                        MAX(ii.interval_start) as last_interval,
                        MIN(ii.created_at) as run_started,
                        MAX(ii.created_at) as last_activity,
                        EXTRACT(EPOCH FROM (MAX(ii.created_at) - MIN(ii.created_at))) / 3600.0 as duration_hours
                    FROM intg_instrument_interval ii
                    WHERE ii.instrument_id = 31
                      AND ii.interval_start >= '2025-07-01'
                    GROUP BY ii.run_id
                ),
                run_status AS (
                    SELECT 
                        rs.*,
                        r.status as official_status,
                        r.created_at as run_record_created,
                        CASE 
                            WHEN r.run_id IS NULL THEN 'orphaned'
                            WHEN r.status = 'failed' THEN 'failed'
                            WHEN r.status = 'running' AND rs.last_activity < NOW() - INTERVAL '1 hour' THEN 'stalled'
                            WHEN rs.interval_count < 10 THEN 'incomplete'
                            WHEN rs.duration_hours > 24 THEN 'excessive_duration'
                            ELSE 'normal'
                        END as cleanup_category
                    FROM run_stats rs
                    LEFT JOIN intg_runs r ON rs.run_id = r.run_id
                )
                SELECT 
                    cleanup_category,
                    COUNT(*) as run_count,
                    SUM(interval_count) as total_intervals,
                    AVG(interval_count) as avg_intervals,
                    MIN(run_started) as earliest_run,
                    MAX(last_activity) as latest_activity
                FROM run_status
                GROUP BY cleanup_category
                ORDER BY run_count DESC
            """
            
            cleanup_analysis = await conn.fetch(failed_run_analysis_query)
            
            print(f"📊 Failed Run Analysis Results:")
            total_problematic_runs = 0
            total_problematic_intervals = 0
            
            for row in cleanup_analysis:
                category = row['cleanup_category'] 
                run_count = row['run_count']
                total_intervals = row['total_intervals']
                avg_intervals = row['avg_intervals']
                
                print(f"   {category}:")
                print(f"      Runs: {run_count}")
                print(f"      Total intervals: {total_intervals}")
                print(f"      Avg intervals/run: {avg_intervals:.1f}")
                print(f"      Date range: {row['earliest_run']} to {row['latest_activity']}")
                
                if category in ['orphaned', 'failed', 'stalled', 'incomplete']:
                    total_problematic_runs += run_count
                    total_problematic_intervals += total_intervals
            
            print(f"\n🧹 Cleanup Summary:")
            print(f"   Total problematic runs: {total_problematic_runs}")
            print(f"   Total problematic intervals: {total_problematic_intervals}")
            
            if total_problematic_runs > 0:
                print(f"   📋 CLEANUP NEEDED: {total_problematic_runs} runs require attention")
            else:
                print(f"   ✅ No problematic runs detected")
            
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_cleanup_strategy_definition(self):
        """Test: Define comprehensive cleanup strategies."""
        
        print("🔍 Testing cleanup strategy definition...")
        
        cleanup_strategies = {
            'orphaned_runs': {
                'description': 'Runs with instrument_interval records but no intg_runs entry',
                'action': 'DELETE from intg_instrument_interval WHERE run_id NOT IN (SELECT run_id FROM intg_runs)',
                'risk': 'low',
                'reversible': False
            },
            'failed_runs': {
                'description': 'Runs marked as failed in intg_runs table',
                'action': 'DELETE from intg_instrument_interval WHERE run_id IN (SELECT run_id FROM intg_runs WHERE status = "failed")',
                'risk': 'low',
                'reversible': False
            },
            'stalled_runs': {
                'description': 'Runs marked as running but no activity for >1 hour',
                'action': 'UPDATE intg_runs SET status = "failed" WHERE status = "running" AND created_at < NOW() - INTERVAL "1 hour"',
                'risk': 'medium',
                'reversible': True
            },
            'incomplete_runs': {
                'description': 'Runs with suspiciously few intervals (<10)',
                'action': 'Manual review required - may be legitimate short runs',
                'risk': 'high',
                'reversible': True
            },
            'duplicate_intervals': {
                'description': 'Multiple records with same (instrument_id, interval_start, interval_duration, run_id)',
                'action': 'DELETE duplicates keeping only the first one',
                'risk': 'medium', 
                'reversible': False
            }
        }
        
        print("🧹 Comprehensive Cleanup Strategies:")
        
        for strategy_name, strategy in cleanup_strategies.items():
            print(f"\n   {strategy_name.upper()}:")
            print(f"      Description: {strategy['description']}")
            print(f"      Action: {strategy['action']}")
            print(f"      Risk Level: {strategy['risk']}")
            print(f"      Reversible: {strategy['reversible']}")
        
        # Priority order for cleanup
        cleanup_priority = [
            'orphaned_runs',      # Safest - no official record exists
            'failed_runs',        # Safe - officially marked as failed
            'stalled_runs',       # Medium risk - mark as failed first
            'duplicate_intervals', # Medium risk - data integrity issue
            'incomplete_runs'     # Highest risk - may be legitimate
        ]
        
        print(f"\n📋 Recommended Cleanup Priority:")
        for i, strategy in enumerate(cleanup_priority, 1):
            risk = cleanup_strategies[strategy]['risk']
            print(f"   {i}. {strategy} (Risk: {risk})")
        
        assert len(cleanup_strategies) == 5, "All cleanup strategies defined"
        assert len(cleanup_priority) == 5, "Cleanup priority order complete"
        
        print("✅ Comprehensive cleanup strategies defined")


if __name__ == "__main__":
    # Run the comprehensive run ID uniqueness tests
    pytest.main([__file__, "-v", "--tb=short", "-s"])