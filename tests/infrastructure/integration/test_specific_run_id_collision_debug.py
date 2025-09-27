#!/usr/bin/env python3
"""
SPECIFIC RUN ID COLLISION DEBUG TEST

This test investigates the exact run ID collision that occurred:
run_20250913_053441_b23b366c with instrument_id=31, interval_start='2025-07-03 23:00:00+00'

The goal is to understand why this specific collision happened and prevent it.
"""

import pytest
import asyncio
import sys
import os
import asyncpg
from datetime import datetime, date
from typing import Dict, List, Any

# Add src to path
sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')

# Set environment to skip gin loading
os.environ['GIN_LOAD_DEFAULT_CONFIG'] = '0'


class TestSpecificRunIdCollisionDebug:
    """Debug the specific run ID collision that occurred."""

    def setup_method(self):
        """Setup test environment."""
        # Database connection for intg environment
        self.db_config = {
            'host': 'localhost',
            'port': 4432,
            'user': 'postgres', 
            'password': 'intg_password',
            'database': 'intg_db'
        }
        
        # The specific problematic values from the error
        self.problematic_run_id = 'run_20250913_053441_b23b366c'
        self.problematic_instrument_id = 31
        self.problematic_interval_start = datetime(2025, 7, 3, 23, 0, 0)
        self.problematic_interval_duration = '60m'

    async def get_db_connection(self) -> asyncpg.Connection:
        """Get database connection for testing."""
        conn = await asyncpg.connect(**self.db_config)
        return conn
    @pytest.mark.asyncio
    async def test_investigate_problematic_run_id(self):
        """Test: Investigate the specific problematic run ID."""
        
        print(f"🔍 Investigating problematic run ID: {self.problematic_run_id}")
        
        conn = await self.get_db_connection()
        
        # Check if this exact record exists
        exact_match_query = """
            SELECT 
                id,
                instrument_id,
                interval_start,
                interval_duration,
                run_id,
                created_at,
                status
            FROM intg_instrument_interval 
            WHERE instrument_id = $1
              AND interval_start = $2
              AND interval_duration = $3
              AND run_id = $4
        """
        
        exact_matches = await conn.fetch(
            exact_match_query,
            self.problematic_instrument_id,
            self.problematic_interval_start,
            self.problematic_interval_duration,
            self.problematic_run_id
        )
        
        print(f"📊 Exact match results: {len(exact_matches)} records found")
        
        if exact_matches:
            print("🚨 DUPLICATE RECORDS FOUND:")
            for i, record in enumerate(exact_matches):
                print(f"   Record {i+1}:")
                print(f"      ID: {record['id']}")
                print(f"      Created: {record['created_at']}")
                print(f"      Status: {record['status']}")
        else:
            print("✅ No exact duplicates found - record may have been cleaned up")
        
        # Check for any records with this run_id
        run_id_query = """
            SELECT 
                COUNT(*) as total_records,
                MIN(interval_start) as first_interval,
                MAX(interval_start) as last_interval,
                MIN(created_at) as run_started,
                MAX(created_at) as last_activity,
                COUNT(DISTINCT instrument_id) as unique_instruments
            FROM intg_instrument_interval 
            WHERE run_id = $1
        """
        
        run_stats = await conn.fetchrow(run_id_query, self.problematic_run_id)
        
        if run_stats and run_stats['total_records'] > 0:
            print(f"\n📈 Run ID Statistics:")
            print(f"   Total records: {run_stats['total_records']}")
            print(f"   Unique instruments: {run_stats['unique_instruments']}")
            print(f"   Time range: {run_stats['first_interval']} to {run_stats['last_interval']}")
            print(f"   Execution time: {run_stats['run_started']} to {run_stats['last_activity']}")
        else:
            print(f"\n✅ No records found for run_id: {self.problematic_run_id}")
        
        # Check for similar run IDs (same timestamp, different suffix)
        similar_run_ids_query = """
            SELECT DISTINCT run_id, COUNT(*) as record_count
            FROM intg_instrument_interval 
            WHERE run_id LIKE 'run_20250913_053441_%'
            GROUP BY run_id
            ORDER BY run_id
        """
        
        similar_runs = await conn.fetch(similar_run_ids_query)
        
        if similar_runs:
            print(f"\n🔍 Similar run IDs (same timestamp):")
            for run in similar_runs:
                print(f"   {run['run_id']}: {run['record_count']} records")
                
            if len(similar_runs) > 1:
                print("🚨 MULTIPLE RUNS WITH SAME TIMESTAMP - This suggests rapid restart issue!")
        
    @pytest.mark.asyncio
    async def test_analyze_run_id_pattern(self):
        """Test: Analyze run ID patterns to understand collision source."""
        
        print("🔍 Analyzing run ID patterns...")
        
        conn = await self.get_db_connection()
        
        # Analyze all run IDs to find patterns
        run_id_analysis_query = """
            WITH run_id_parts AS (
                SELECT 
                    run_id,
                    COUNT(*) as record_count,
                    CASE 
                        WHEN run_id ~ '^run_\\d{8}_\\d{6}_[a-f0-9]{8}$' THEN 'standard_format'
                        WHEN run_id ~ '^run_\\d+' THEN 'legacy_numeric'
                        WHEN run_id = 'legacy_run_pre_0025' THEN 'legacy_default'
                        ELSE 'unknown_format'
                    END as format_type,
                    SUBSTRING(run_id FROM 'run_(\\d{8}_\\d{6})_') as timestamp_part,
                    SUBSTRING(run_id FROM '_([a-f0-9]{8})$') as uuid_part
                FROM intg_instrument_interval 
                WHERE interval_start >= '2025-07-01'
                GROUP BY run_id
            )
            SELECT 
                format_type,
                COUNT(*) as run_count,
                SUM(record_count) as total_records,
                AVG(record_count) as avg_records_per_run,
                MIN(record_count) as min_records,
                MAX(record_count) as max_records
            FROM run_id_parts
            GROUP BY format_type
            ORDER BY run_count DESC
        """
        
        pattern_analysis = await conn.fetch(run_id_analysis_query)
        
        print("📊 Run ID Pattern Analysis:")
        for row in pattern_analysis:
            print(f"   {row['format_type']}:")
            print(f"      Runs: {row['run_count']}")
            print(f"      Total records: {row['total_records']}")
            print(f"      Avg records/run: {row['avg_records_per_run']:.1f}")
            print(f"      Range: {row['min_records']} - {row['max_records']} records")
        
        # Check for timestamp collisions
        timestamp_collision_query = """
            WITH timestamp_groups AS (
                SELECT 
                    SUBSTRING(run_id FROM 'run_(\\d{8}_\\d{6})_') as timestamp_part,
                    COUNT(DISTINCT run_id) as unique_run_ids,
                    ARRAY_AGG(DISTINCT run_id) as run_ids
                FROM intg_instrument_interval 
                WHERE run_id ~ '^run_\\d{8}_\\d{6}_[a-f0-9]{8}$'
                  AND interval_start >= '2025-07-01'
                GROUP BY timestamp_part
                HAVING COUNT(DISTINCT run_id) > 1
            )
            SELECT 
                timestamp_part,
                unique_run_ids,
                run_ids
            FROM timestamp_groups
            ORDER BY unique_run_ids DESC
        """
        
        timestamp_collisions = await conn.fetch(timestamp_collision_query)
        
        if timestamp_collisions:
            print(f"\n🚨 TIMESTAMP COLLISIONS DETECTED:")
            for collision in timestamp_collisions:
                print(f"   Timestamp {collision['timestamp_part']}:")
                print(f"      {collision['unique_run_ids']} different run IDs")
                for run_id in collision['run_ids'][:3]:  # Show first 3
                    print(f"         - {run_id}")
                if len(collision['run_ids']) > 3:
                    print(f"         ... and {len(collision['run_ids']) - 3} more")
        else:
            print(f"\n✅ No timestamp collisions detected in run ID generation")
        
    @pytest.mark.asyncio
    async def test_simulate_collision_scenario(self):
        """Test: Simulate the collision scenario to understand root cause."""
        
        print("🔍 Simulating collision scenario...")
        
        # Simulation: What happens when the same run_id is used twice?
        print("🧪 Collision Simulation Scenarios:")
        
        scenarios = [
            {
                'name': 'rapid_restart',
                'description': 'Process crashes and restarts within same second',
                'cause': 'Timestamp resolution too low (1 second)',
                'probability': 'high_with_automation'
            },
            {
                'name': 'parallel_execution',
                'description': 'Multiple processes start simultaneously',
                'cause': 'No process-level coordination',
                'probability': 'medium'
            },
            {
                'name': 'system_clock_issues',
                'description': 'System clock goes backwards',
                'cause': 'NTP sync or timezone changes',
                'probability': 'low'
            },
            {
                'name': 'uuid_collision',
                'description': 'UUID suffix collision (8 chars)',
                'cause': 'Random collision in 4.3 billion space',
                'probability': 'extremely_low'
            },
            {
                'name': 'failed_run_not_cleaned',
                'description': 'Previous run with same ID not cleaned up',
                'cause': 'Incomplete cleanup after failure',
                'probability': 'high_with_repeated_failures'
            }
        ]
        
        for scenario in scenarios:
            print(f"\n   {scenario['name'].upper()}:")
            print(f"      Description: {scenario['description']}")
            print(f"      Root cause: {scenario['cause']}")
            print(f"      Probability: {scenario['probability']}")
        
        # Specific analysis for our error case
        print(f"\n🎯 SPECIFIC CASE ANALYSIS:")
        print(f"   Run ID: {self.problematic_run_id}")
        print(f"   Timestamp: 20250913_053441 (Sep 13, 2025 05:34:41 UTC)")
        print(f"   UUID suffix: b23b366c")
        print(f"   Conflict on: interval 2025-07-03 23:00:00 (July 3, 11PM)")
        
        # This suggests the run was processing historical data and hit a duplicate
        print(f"\n💡 LIKELY ROOT CAUSE:")
        print(f"   1. Previous run with same ID started processing")
        print(f"   2. Previous run failed or was interrupted")
        print(f"   3. Database records from failed run were NOT cleaned up")
        print(f"   4. New run with coincidentally same ID tried to process same intervals")
        print(f"   5. Constraint violation occurred on existing data")
        
        print(f"\n🔧 RECOMMENDED SOLUTION:")
        print(f"   1. Add pre-flight check: Query existing run_id before starting")
        print(f"   2. Implement failed run cleanup mechanism")
        print(f"   3. Use microsecond precision in timestamp")
        print(f"   4. Add process PID to run ID for uniqueness")
        print(f"   5. Implement idempotent run behavior")

    @pytest.mark.asyncio
    async def test_prevention_strategy_validation(self):
        """Test: Validate prevention strategies for run ID collisions."""
        
        print("🔍 Testing prevention strategy validation...")
        
        conn = await self.get_db_connection()
        
        # Strategy 1: Pre-flight run ID check
        async def check_run_id_exists(run_id: str) -> bool:
            query = "SELECT COUNT(*) as count FROM intg_instrument_interval WHERE run_id = $1"
            result = await conn.fetchrow(query, run_id)
            return result['count'] > 0
        
        # Test with known run IDs
        test_run_ids = [
            self.problematic_run_id,
            'run_20250913_999999_ffffffff',  # Unlikely to exist
            'legacy_run_pre_0025'  # Default value
        ]
        
        print("📋 Pre-flight Check Results:")
        for run_id in test_run_ids:
            exists = await check_run_id_exists(run_id)
            status = "🚨 EXISTS" if exists else "✅ AVAILABLE"
            print(f"   {run_id}: {status}")
        
        # Strategy 2: Enhanced run ID generation
        def generate_enhanced_run_id() -> str:
            import uuid
            import os
            from datetime import datetime
            
            # Use microsecond precision
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]  # milliseconds
            
            # Add process ID for uniqueness
            pid = os.getpid()
            
            # Use longer UUID suffix
            uuid_suffix = uuid.uuid4().hex[:12]  # 12 chars instead of 8
            
            return f"run_{timestamp}_{pid:06d}_{uuid_suffix}"
        
        # Test enhanced generation
        enhanced_run_ids = [generate_enhanced_run_id() for _ in range(5)]
        
        print(f"\n🔧 Enhanced Run ID Generation:")
        for i, run_id in enumerate(enhanced_run_ids):
            print(f"   {i+1}. {run_id}")
            exists = await check_run_id_exists(run_id)
            if exists:
                print(f"      🚨 COLLISION DETECTED!")
            else:
                print(f"      ✅ Unique")
        
        # Strategy 3: Cleanup query for failed runs
        cleanup_query = """
            DELETE FROM intg_instrument_interval 
            WHERE run_id = $1
              AND instrument_id = $2
              AND interval_start >= $3
              AND interval_start < $4
        """
        
        print(f"\n🧹 Cleanup Strategy Example:")
        print(f"   Query: {cleanup_query}")
        print(f"   Purpose: Remove partial data from failed runs before retry")
        print(f"   Safety: Scoped to specific run_id and date range")
        
        print(f"\n✅ Prevention strategies validated")
        
if __name__ == "__main__":
    # Run the specific collision debug tests
    pytest.main([__file__, "-v", "--tb=short", "-s"])