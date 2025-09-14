#!/usr/bin/env python3
"""
DUPLICATE INTERVAL DETECTION TEST

This test finds the exact cause of the constraint violation by looking for
existing intervals that conflict with new runs.
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


class TestDuplicateIntervalDetection:
    """Detect duplicate intervals causing constraint violations."""

    def setup_method(self):
        """Setup test environment."""
        self.db_config = {
            'host': 'localhost',
            'port': 4432,
            'user': 'postgres',
            'password': 'intg_password',
            'database': 'intg_db'
        }

        # The problematic interval from the error
        self.problematic_instrument_id = 31
        self.problematic_interval_start = datetime(2025, 7, 3, 23, 0, 0)
        self.problematic_interval_duration = '60m'

    async def get_db_connection(self) -> asyncpg.Connection:
        """Get database connection for testing."""
        try:
            conn = await asyncpg.connect(**self.db_config)
            return conn
        except Exception as e:
            pytest.skip(f"Cannot connect to intg database: {e}")

    @pytest.mark.asyncio
    async def test_find_conflicting_interval(self):
        """Test: Find the existing interval that conflicts with the new run."""

        print("🔍 Searching for conflicting interval...")

        conn = await self.get_db_connection()

        try:
            # Find ALL records with the problematic interval details (regardless of run_id)
            conflict_search_query = """
                SELECT
                    id,
                    instrument_id,
                    interval_start,
                    interval_duration,
                    run_id,
                    created_at,
                    status,
                    open,
                    high,
                    low,
                    close,
                    traded_volume
                FROM intg_instrument_interval
                WHERE instrument_id = $1
                  AND interval_start = $2
                  AND interval_duration = $3
                ORDER BY created_at ASC
            """

            conflicts = await conn.fetch(
                conflict_search_query,
                self.problematic_instrument_id,
                self.problematic_interval_start,
                self.problematic_interval_duration
            )

            print(f"📊 Found {len(conflicts)} records for problematic interval:")
            print(f"   Instrument: {self.problematic_instrument_id}")
            print(f"   Interval: {self.problematic_interval_start}")
            print(f"   Duration: {self.problematic_interval_duration}")

            if len(conflicts) > 1:
                print(f"\n🚨 CONSTRAINT VIOLATION CAUSE FOUND:")
                print(f"   Multiple records exist for same (instrument_id, interval_start, interval_duration)")

                for i, record in enumerate(conflicts):
                    print(f"\n   Record {i+1}:")
                    print(f"      ID: {record['id']}")
                    print(f"      Run ID: {record['run_id']}")
                    print(f"      Created: {record['created_at']}")
                    print(f"      Status: {record['status']}")
                    print(f"      OHLC: {record['open']}/{record['high']}/{record['low']}/{record['close']}")
                    print(f"      Volume: {record['traded_volume']}")

                # Identify which run_ids are involved
                unique_run_ids = set(record['run_id'] for record in conflicts)
                print(f"\n🎯 CONFLICTING RUN IDs:")
                for run_id in unique_run_ids:
                    count = sum(1 for r in conflicts if r['run_id'] == run_id)
                    print(f"   {run_id}: {count} records")

            elif len(conflicts) == 1:
                record = conflicts[0]
                print(f"\n✅ Single record found (no current duplication):")
                print(f"   ID: {record['id']}")
                print(f"   Run ID: {record['run_id']}")
                print(f"   Created: {record['created_at']}")
                print(f"   Status: {record['status']}")

                print(f"\n💡 LIKELY SCENARIO:")
                print(f"   1. Previous run created this interval record")
                print(f"   2. New run tried to create same interval")
                print(f"   3. Database constraint prevented duplicate")
                print(f"   4. One of the duplicate attempts may have been cleaned up")

            else:
                print(f"\n❓ No records found - interval may have been cleaned up entirely")

        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_comprehensive_duplicate_scan(self):
        """Test: Comprehensive scan for ALL duplicate intervals in database."""

        print("🔍 Performing comprehensive duplicate interval scan...")

        conn = await self.get_db_connection()

        try:
            # Find ALL duplicate intervals in the database
            duplicate_scan_query = """
                SELECT
                    instrument_id,
                    interval_start,
                    interval_duration,
                    COUNT(*) as duplicate_count,
                    ARRAY_AGG(DISTINCT run_id) as run_ids,
                    ARRAY_AGG(DISTINCT id) as record_ids,
                    MIN(created_at) as first_created,
                    MAX(created_at) as last_created
                FROM intg_instrument_interval
                WHERE interval_start >= '2025-07-01'
                GROUP BY instrument_id, interval_start, interval_duration
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC, interval_start DESC
                LIMIT 20
            """

            duplicates = await conn.fetch(duplicate_scan_query)

            print(f"📊 Comprehensive Duplicate Scan Results:")
            print(f"   Total duplicate interval groups: {len(duplicates)}")

            if duplicates:
                print(f"\n🚨 DUPLICATE INTERVALS FOUND:")

                total_duplicate_records = 0
                for i, dup in enumerate(duplicates):
                    duplicate_count = dup['duplicate_count']
                    total_duplicate_records += duplicate_count

                    print(f"\n   Group {i+1}:")
                    print(f"      Instrument: {dup['instrument_id']}")
                    print(f"      Interval: {dup['interval_start']}")
                    print(f"      Duration: {dup['interval_duration']}")
                    print(f"      Duplicates: {duplicate_count} records")
                    print(f"      Run IDs: {list(dup['run_ids'])}")
                    print(f"      Time span: {dup['first_created']} to {dup['last_created']}")

                print(f"\n📈 SUMMARY:")
                print(f"   Duplicate groups: {len(duplicates)}")
                print(f"   Total duplicate records: {total_duplicate_records}")
                print(f"   This explains the constraint violations!")

            else:
                print(f"\n✅ No duplicate intervals found in database")
                print(f"   All constraint violations have been resolved or cleaned up")

        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_run_id_overlap_analysis(self):
        """Test: Analyze which run IDs have overlapping intervals."""

        print("🔍 Analyzing run ID interval overlaps...")

        conn = await self.get_db_connection()

        try:
            # Find run IDs that have created overlapping intervals
            overlap_analysis_query = """
                WITH interval_run_mapping AS (
                    SELECT DISTINCT
                        instrument_id,
                        interval_start,
                        interval_duration,
                        run_id
                    FROM intg_instrument_interval
                    WHERE interval_start >= '2025-07-01'
                ),
                interval_counts AS (
                    SELECT
                        instrument_id,
                        interval_start,
                        interval_duration,
                        COUNT(DISTINCT run_id) as run_count,
                        ARRAY_AGG(DISTINCT run_id) as run_ids
                    FROM interval_run_mapping
                    GROUP BY instrument_id, interval_start, interval_duration
                    HAVING COUNT(DISTINCT run_id) > 1
                )
                SELECT
                    run_ids[1] as run_id_1,
                    run_ids[2] as run_id_2,
                    COUNT(*) as overlap_count,
                    MIN(interval_start) as first_overlap,
                    MAX(interval_start) as last_overlap
                FROM interval_counts
                WHERE array_length(run_ids, 1) = 2  -- Focus on pairs
                GROUP BY run_ids[1], run_ids[2]
                ORDER BY overlap_count DESC
            """

            overlaps = await conn.fetch(overlap_analysis_query)

            print(f"📊 Run ID Overlap Analysis:")
            print(f"   Overlapping run ID pairs: {len(overlaps)}")

            if overlaps:
                print(f"\n🔄 RUN ID OVERLAPS:")

                for i, overlap in enumerate(overlaps):
                    print(f"\n   Overlap {i+1}:")
                    print(f"      Run ID 1: {overlap['run_id_1']}")
                    print(f"      Run ID 2: {overlap['run_id_2']}")
                    print(f"      Shared intervals: {overlap['overlap_count']}")
                    print(f"      Date range: {overlap['first_overlap']} to {overlap['last_overlap']}")

                print(f"\n💡 ROOT CAUSE IDENTIFIED:")
                print(f"   Multiple runs are processing the same time intervals")
                print(f"   This creates constraint violations when they try to insert the same data")

            else:
                print(f"\n✅ No run ID overlaps detected")
                print(f"   Each run is processing unique intervals")

        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_cleanup_strategy_for_duplicates(self):
        """Test: Define cleanup strategy for duplicate intervals."""

        print("🔍 Defining cleanup strategy for duplicate intervals...")

        conn = await self.get_db_connection()

        try:
            # Get specific duplicate details for cleanup planning
            cleanup_planning_query = """
                SELECT
                    instrument_id,
                    interval_start,
                    interval_duration,
                    run_id,
                    id,
                    created_at,
                    status,
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument_id, interval_start, interval_duration
                        ORDER BY created_at ASC
                    ) as creation_order
                FROM intg_instrument_interval
                WHERE interval_start >= '2025-07-01'
                  AND (instrument_id, interval_start, interval_duration) IN (
                      SELECT instrument_id, interval_start, interval_duration
                      FROM intg_instrument_interval
                      WHERE interval_start >= '2025-07-01'
                      GROUP BY instrument_id, interval_start, interval_duration
                      HAVING COUNT(*) > 1
                  )
                ORDER BY instrument_id, interval_start, creation_order
            """

            cleanup_candidates = await conn.fetch(cleanup_planning_query)

            print(f"📊 Cleanup Planning Analysis:")
            print(f"   Records requiring cleanup decision: {len(cleanup_candidates)}")

            if cleanup_candidates:
                # Group by interval for cleanup decisions
                interval_groups = {}
                for record in cleanup_candidates:
                    key = (record['instrument_id'], record['interval_start'], record['interval_duration'])
                    if key not in interval_groups:
                        interval_groups[key] = []
                    interval_groups[key].append(record)

                print(f"\n🧹 CLEANUP RECOMMENDATIONS:")

                total_to_delete = 0
                for interval_key, records in interval_groups.items():
                    if len(records) > 1:
                        instrument_id, interval_start, interval_duration = interval_key

                        print(f"\n   Interval: {instrument_id}, {interval_start}, {interval_duration}")
                        print(f"   Duplicates: {len(records)} records")

                        # Recommendation: Keep the first created, delete the rest
                        keep_record = records[0]
                        delete_records = records[1:]

                        print(f"   KEEP: ID {keep_record['id']} (run_id: {keep_record['run_id']}, created: {keep_record['created_at']})")

                        for delete_record in delete_records:
                            print(f"   DELETE: ID {delete_record['id']} (run_id: {delete_record['run_id']}, created: {delete_record['created_at']})")
                            total_to_delete += 1

                print(f"\n📋 CLEANUP SUMMARY:")
                print(f"   Total records to delete: {total_to_delete}")
                print(f"   Cleanup method: DELETE FROM intg_instrument_interval WHERE id IN (...)")
                print(f"   Safety: Keep earliest created record for each interval")

                # Generate actual cleanup SQL
                if total_to_delete > 0:
                    delete_ids = []
                    for interval_key, records in interval_groups.items():
                        if len(records) > 1:
                            delete_ids.extend([str(r['id']) for r in records[1:]])

                    cleanup_sql = f"DELETE FROM intg_instrument_interval WHERE id IN ({', '.join(delete_ids)});"
                    print(f"\n🔧 CLEANUP SQL:")
                    print(f"   {cleanup_sql}")

            else:
                print(f"\n✅ No duplicates found - database is clean")

        finally:
            await conn.close()


if __name__ == "__main__":
    # Run the duplicate interval detection tests
    pytest.main([__file__, "-v", "--tb=short", "-s"])