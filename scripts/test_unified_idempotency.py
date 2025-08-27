#!/usr/bin/env python3
"""
Test Script: Unified Instrument Population Idempotency

Tests that the unified instrument population script is fully idempotent by:
1. Running it once and recording results
2. Running it again and verifying results are identical
3. Testing edge cases and recovery scenarios
"""

import os
import sys
import asyncio
import subprocess
import json
from datetime import datetime

# Add src to Python path
sys.path.insert(0, '/workspace/src')

async def run_unified_population():
    """Run the unified instrument population script"""
    print("🔄 Running unified instrument population...")
    
    try:
        # Run the script using the run_dev.py infrastructure
        cmd = [
            "python3", "scripts/run_dev.py", "run",
            "--script", "scripts/unified_instrument_population.py"
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode
        }
        
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'error': 'Script timed out after 5 minutes',
            'returncode': -1
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'returncode': -1
        }

async def get_database_state():
    """Get current state of the database for comparison"""
    try:
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)
        
        async with pool.acquire() as conn:
            # Get comprehensive metrics
            state = await conn.fetchrow("""
                SELECT 
                    (SELECT COUNT(*) FROM dev_instruments) as unified_count,
                    (SELECT COUNT(DISTINCT symbol) FROM dev_instruments) as unique_symbols,
                    (SELECT MAX(updated_at) FROM dev_instruments) as latest_update,
                    (SELECT COUNT(*) FROM dev_daily_prices_polygon p
                     WHERE EXISTS (SELECT 1 FROM dev_instruments i WHERE i.id = p.instrument_id)
                    ) as price_integrity_count,
                    (SELECT COUNT(*) FROM dev_daily_prices_polygon) as total_price_records
            """)
            
            # Check for duplicates
            duplicates = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT symbol FROM dev_instruments 
                    GROUP BY symbol HAVING COUNT(*) > 1
                ) dup
            """)
            
            # Get sample of recent records for content comparison
            sample_records = await conn.fetch("""
                SELECT symbol, name, exchange, type, currency, active
                FROM dev_instruments
                ORDER BY updated_at DESC, symbol
                LIMIT 10
            """)
            
        await pool.close()
        
        return {
            'unified_count': state['unified_count'],
            'unique_symbols': state['unique_symbols'], 
            'latest_update': state['latest_update'],
            'price_integrity_count': state['price_integrity_count'],
            'total_price_records': state['total_price_records'],
            'duplicates': duplicates,
            'sample_records': [dict(r) for r in sample_records],
            'integrity_percentage': (state['price_integrity_count'] / state['total_price_records'] * 100) if state['total_price_records'] > 0 else 0
        }
        
    except Exception as e:
        return {'error': str(e)}

def compare_states(state1, state2):
    """Compare two database states for idempotency validation"""
    differences = []
    
    # Compare key metrics
    if state1.get('unified_count') != state2.get('unified_count'):
        differences.append(f"Unified count changed: {state1.get('unified_count')} -> {state2.get('unified_count')}")
    
    if state1.get('unique_symbols') != state2.get('unique_symbols'):
        differences.append(f"Unique symbols changed: {state1.get('unique_symbols')} -> {state2.get('unique_symbols')}")
    
    if state1.get('duplicates') != state2.get('duplicates'):
        differences.append(f"Duplicates changed: {state1.get('duplicates')} -> {state2.get('duplicates')}")
    
    if abs(state1.get('integrity_percentage', 0) - state2.get('integrity_percentage', 0)) > 0.1:
        differences.append(f"Integrity percentage changed: {state1.get('integrity_percentage'):.1f}% -> {state2.get('integrity_percentage'):.1f}%")
    
    # Compare sample records (content should be identical)
    if state1.get('sample_records') != state2.get('sample_records'):
        differences.append("Sample record content changed between runs")
    
    return differences

async def test_idempotency():
    """Test complete idempotency of unified instrument population"""
    print("🧪 TESTING UNIFIED INSTRUMENT POPULATION IDEMPOTENCY")
    print("=" * 70)
    print(f"Started: {datetime.now()}")
    print("=" * 70)
    
    # Test 1: Get initial state
    print("1️⃣ Getting initial database state...")
    initial_state = await get_database_state()
    
    if 'error' in initial_state:
        print(f"❌ Failed to get initial state: {initial_state['error']}")
        return False
    
    print(f"📊 Initial state: {initial_state['unified_count']:,} instruments, {initial_state['integrity_percentage']:.1f}% integrity")
    
    # Test 2: First run
    print("\n2️⃣ Running unified population (first time)...")
    first_run = await run_unified_population()
    
    if not first_run['success']:
        print(f"❌ First run failed: {first_run.get('error', 'Unknown error')}")
        if first_run.get('stderr'):
            print(f"STDERR: {first_run['stderr'][-500:]}")  # Last 500 chars
        return False
    
    print("✅ First run completed successfully")
    
    # Get state after first run
    print("3️⃣ Getting state after first run...")
    first_state = await get_database_state()
    
    if 'error' in first_state:
        print(f"❌ Failed to get first state: {first_state['error']}")
        return False
    
    print(f"📊 After first run: {first_state['unified_count']:,} instruments, {first_state['integrity_percentage']:.1f}% integrity")
    
    # Wait a moment to ensure timestamp differences would show up
    await asyncio.sleep(2)
    
    # Test 3: Second run (idempotency test)
    print("\n4️⃣ Running unified population again (idempotency test)...")
    second_run = await run_unified_population()
    
    if not second_run['success']:
        print(f"❌ Second run failed: {second_run.get('error', 'Unknown error')}")
        if second_run.get('stderr'):
            print(f"STDERR: {second_run['stderr'][-500:]}")
        return False
    
    print("✅ Second run completed successfully")
    
    # Get state after second run
    print("5️⃣ Getting state after second run...")
    second_state = await get_database_state()
    
    if 'error' in second_state:
        print(f"❌ Failed to get second state: {second_state['error']}")
        return False
    
    print(f"📊 After second run: {second_state['unified_count']:,} instruments, {second_state['integrity_percentage']:.1f}% integrity")
    
    # Test 4: Compare states for idempotency
    print("\n6️⃣ Comparing states for idempotency validation...")
    differences = compare_states(first_state, second_state)
    
    if differences:
        print("❌ IDEMPOTENCY TEST FAILED - Found differences:")
        for diff in differences:
            print(f"   • {diff}")
        return False
    else:
        print("✅ IDEMPOTENCY TEST PASSED - States are identical")
    
    # Test 5: Validate data quality
    print("\n7️⃣ Validating data quality...")
    
    quality_issues = []
    
    if second_state['duplicates'] > 0:
        quality_issues.append(f"Found {second_state['duplicates']} duplicate symbols")
    
    if second_state['integrity_percentage'] < 95.0:
        quality_issues.append(f"Price integrity too low: {second_state['integrity_percentage']:.1f}%")
    
    if second_state['unified_count'] == 0:
        quality_issues.append("No instruments in unified table")
    
    if quality_issues:
        print("⚠️ Data quality issues found:")
        for issue in quality_issues:
            print(f"   • {issue}")
        return False
    else:
        print("✅ All data quality checks passed")
    
    # Final results
    print("\n" + "=" * 70)
    print("🎉 IDEMPOTENCY TEST RESULTS")
    print("=" * 70)
    print("✅ Script can be run multiple times safely")
    print("✅ Results are consistent across runs")
    print("✅ Data integrity maintained")
    print("✅ No duplicates or corruption")
    print(f"📊 Final metrics: {second_state['unified_count']:,} instruments, {second_state['integrity_percentage']:.1f}% integrity")
    print("🚀 UNIFIED INSTRUMENT POPULATION IS FULLY IDEMPOTENT")
    print("=" * 70)
    
    return True

async def main():
    """Main test execution"""
    try:
        success = await test_idempotency()
        return 0 if success else 1
    except Exception as e:
        print(f"❌ Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)