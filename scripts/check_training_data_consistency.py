#!/usr/bin/env python3
"""
Production Training Data Consistency Checker

This script detects and reports inconsistencies between run status and dataset status.
It should be run regularly to catch issues like datasets stuck in 'generating' status
when their corresponding runs have failed.

Usage:
    python scripts/check_training_data_consistency.py [--fix]
    
    --fix: Automatically apply suggested fixes (use with caution)
"""

import asyncio
import asyncpg
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any


async def main():
    parser = argparse.ArgumentParser(description="Check training data consistency")
    parser.add_argument('--fix', action='store_true', help='Automatically apply fixes')
    parser.add_argument('--env', default='dev', help='Environment (dev/intg/prod)')
    args = parser.parse_args()
    
    print("🔍 TRAINING DATA CONSISTENCY CHECKER")
    print("=" * 60)
    
    # Connect to database
    if args.env == 'dev':
        db_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"
    else:
        print(f"❌ Unsupported environment: {args.env}")
        return
    
    try:
        conn = await asyncpg.connect(db_url)
        
        # Run comprehensive consistency check
        inconsistencies = await run_consistency_check(conn)
        
        # Report results
        print(f"Environment: {args.env}")
        print(f"Check time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total inconsistencies found: {len(inconsistencies)}")
        
        if not inconsistencies:
            print("\n✅ No inconsistencies detected - all systems healthy!")
            return
        
        # Group inconsistencies by type
        by_type = {}
        for inc in inconsistencies:
            inc_type = inc['type']
            if inc_type not in by_type:
                by_type[inc_type] = []
            by_type[inc_type].append(inc)
        
        print(f"\n❌ INCONSISTENCIES DETECTED:")
        for inc_type, incidents in by_type.items():
            print(f"\n🚨 {inc_type.upper().replace('_', ' ')} ({len(incidents)} cases):")
            for inc in incidents:
                print(f"   - {inc['description']}")
        
        # Generate and show fixes
        fixes = generate_fix_sql(inconsistencies)
        print(f"\n🔧 SUGGESTED FIXES:")
        for fix in fixes:
            print(f"   {fix}")
        
        # Apply fixes if requested
        if args.fix:
            print(f"\n⚡ APPLYING FIXES...")
            for fix in fixes:
                try:
                    result = await conn.execute(fix)
                    print(f"   ✅ {fix} - {result}")
                except Exception as e:
                    print(f"   ❌ {fix} - Error: {e}")
            
            # Re-run check to verify fixes
            print(f"\n🔄 VERIFYING FIXES...")
            remaining = await run_consistency_check(conn)
            if remaining:
                print(f"   ⚠️ {len(remaining)} inconsistencies remain")
            else:
                print(f"   ✅ All inconsistencies resolved!")
        else:
            print(f"\n💡 To apply fixes automatically, run with --fix flag")
            print(f"   CAUTION: Review fixes carefully before applying!")
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")


async def run_consistency_check(conn) -> List[Dict[str, Any]]:
    """Run comprehensive consistency checks."""
    inconsistencies = []
    
    # Check 1: Failed runs with generating datasets
    failed_run_inconsistencies = await conn.fetch("""
        SELECT 
            r.id as run_id,
            r.status as run_status,
            r.run_type,
            r.start_time,
            r.end_time,
            d.id as dataset_id,
            d.status as dataset_status,
            d.dataset_name,
            EXTRACT(EPOCH FROM (NOW() - COALESCE(r.end_time, r.start_time)))/3600 as hours_since
        FROM dev_runs r
        JOIN dev_training_datasets d ON d.run_id = r.id
        WHERE r.status IN ('failed', 'completed_with_errors') 
        AND d.status = 'generating'
        AND r.run_type LIKE '%training_data%'
    """)
    
    for row in failed_run_inconsistencies:
        inconsistencies.append({
            'type': 'failed_run_generating_dataset',
            'run_id': row['run_id'],
            'dataset_id': row['dataset_id'],
            'run_status': row['run_status'],
            'dataset_status': row['dataset_status'],
            'hours_since': float(row['hours_since']) if row['hours_since'] else 0,
            'description': f"Run {row['run_id']} {row['run_status']} {row['hours_since']:.1f}h ago, but dataset {row['dataset_id']} still generating"
        })
    
    # Check 2: Completed runs with generating datasets (possible timeout/incomplete)
    completed_inconsistencies = await conn.fetch("""
        SELECT 
            r.id as run_id,
            r.status as run_status,
            r.end_time,
            d.id as dataset_id,  
            d.status as dataset_status,
            d.dataset_name,
            EXTRACT(EPOCH FROM (NOW() - r.end_time))/3600 as hours_since_completion
        FROM dev_runs r
        JOIN dev_training_datasets d ON d.run_id = r.id
        WHERE r.status = 'completed'
        AND d.status = 'generating'
        AND r.run_type LIKE '%training_data%'
        AND r.end_time < NOW() - INTERVAL '1 hour'  -- Completed more than 1 hour ago
    """)
    
    for row in completed_inconsistencies:
        inconsistencies.append({
            'type': 'completed_run_generating_dataset',
            'run_id': row['run_id'],
            'dataset_id': row['dataset_id'],
            'hours_since': float(row['hours_since_completion']) if row['hours_since_completion'] else 0,
            'description': f"Run {row['run_id']} completed {row['hours_since_completion']:.1f}h ago, but dataset {row['dataset_id']} still generating"
        })
    
    # Check 3: Orphaned datasets (NULL run_id)
    orphaned_datasets = await conn.fetch("""
        SELECT 
            id, 
            dataset_name, 
            status,
            created_at,
            EXTRACT(EPOCH FROM (NOW() - created_at))/3600 as hours_since_creation
        FROM dev_training_datasets
        WHERE run_id IS NULL 
        AND status = 'generating'
    """)
    
    for row in orphaned_datasets:
        inconsistencies.append({
            'type': 'orphaned_dataset',
            'dataset_id': row['id'],
            'dataset_name': row['dataset_name'],
            'hours_since': float(row['hours_since_creation']) if row['hours_since_creation'] else 0,
            'description': f"Dataset {row['id']} orphaned (no run_id) for {row['hours_since_creation']:.1f}h, status still generating"
        })
    
    # Check 4: Long-running generations (possible stuck processes)
    long_running = await conn.fetch("""
        SELECT 
            r.id as run_id,
            r.status as run_status,
            r.start_time,
            d.id as dataset_id,
            d.status as dataset_status,
            d.dataset_name,
            EXTRACT(EPOCH FROM (NOW() - r.start_time))/3600 as hours_running
        FROM dev_runs r
        JOIN dev_training_datasets d ON d.run_id = r.id
        WHERE r.status = 'running'
        AND d.status = 'generating'  
        AND r.run_type LIKE '%training_data%'
        AND r.start_time < NOW() - INTERVAL '6 hours'  -- Running for more than 6 hours
    """)
    
    for row in long_running:
        inconsistencies.append({
            'type': 'long_running_generation',
            'run_id': row['run_id'],
            'dataset_id': row['dataset_id'],
            'hours_running': float(row['hours_running']),
            'description': f"Run {row['run_id']} and dataset {row['dataset_id']} have been running for {row['hours_running']:.1f}h (possible stuck process)"
        })
    
    return inconsistencies


def generate_fix_sql(inconsistencies: List[Dict[str, Any]]) -> List[str]:
    """Generate SQL commands to fix detected inconsistencies."""
    fixes = []
    
    for inc in inconsistencies:
        if inc['type'] == 'failed_run_generating_dataset':
            fixes.append(
                f"UPDATE dev_training_datasets SET status = 'failed' WHERE id = {inc['dataset_id']};"
            )
        elif inc['type'] == 'orphaned_dataset':
            fixes.append(
                f"UPDATE dev_training_datasets SET status = 'failed' WHERE id = {inc['dataset_id']};"  
            )
        elif inc['type'] == 'completed_run_generating_dataset':
            # This could be completed or failed depending on if files exist
            fixes.append(
                f"-- MANUAL CHECK REQUIRED: UPDATE dev_training_datasets SET status = 'completed' WHERE id = {inc['dataset_id']}; -- Verify files exist first"
            )
        elif inc['type'] == 'long_running_generation':
            fixes.append(
                f"-- MANUAL CHECK REQUIRED: Long running process - investigate run {inc['run_id']} and dataset {inc['dataset_id']}"
            )
    
    return fixes


if __name__ == "__main__":
    asyncio.run(main())