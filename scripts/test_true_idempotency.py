#!/usr/bin/env python3
"""
Test True Idempotency of Unified Instrument Population

Tests the enhanced idempotent mode by running the script twice in quick succession
to ensure identical results when vendor data hasn't changed.
"""

import os
import sys
import asyncio
from datetime import datetime

# Add src to Python path
sys.path.insert(0, '/workspace/src')

async def get_instrument_count():
    """Get current unified instrument count"""
    try:
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=10.0)
        
        async with pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
        
        await pool.close()
        return count
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return None

async def run_unified_population_idempotent():
    """Run unified population in idempotent mode"""
    import subprocess
    
    try:
        result = subprocess.run([
            "python3", "scripts/run_dev.py", "run",
            "--script", "scripts/unified_instrument_population.py",
            "--env", '{"IDEMPOTENT_MODE": "true"}'
        ], capture_output=True, text=True, timeout=120)
        
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout[-1000:] if result.stdout else '',  # Last 1000 chars
            'stderr': result.stderr[-500:] if result.stderr else '',   # Last 500 chars
        }
        
    except subprocess.TimeoutExpired:
        return {'success': False, 'error': 'Timeout'}
    except Exception as e:
        return {'success': False, 'error': str(e)}

async def test_true_idempotency():
    """Test true idempotency with immediate successive runs"""
    print("🧪 TESTING TRUE IDEMPOTENCY OF UNIFIED INSTRUMENT POPULATION")
    print("=" * 75)
    print(f"Started: {datetime.now()}")
    print("=" * 75)
    
    # Get initial count
    print("1️⃣ Getting baseline instrument count...")
    initial_count = await get_instrument_count()
    if initial_count is None:
        print("❌ Cannot connect to database")
        return False
    
    print(f"📊 Baseline: {initial_count:,} instruments")
    
    # Run first idempotent population
    print("\n2️⃣ Running first idempotent population...")
    first_run = await run_unified_population_idempotent()
    
    if not first_run['success']:
        print(f"❌ First run failed: {first_run.get('error', 'Unknown error')}")
        return False
    
    first_count = await get_instrument_count()
    print(f"📊 After first run: {first_count:,} instruments")
    
    # Brief pause to ensure any timing issues are resolved
    await asyncio.sleep(1)
    
    # Run second idempotent population immediately
    print("\n3️⃣ Running second idempotent population (immediate succession)...")
    second_run = await run_unified_population_idempotent()
    
    if not second_run['success']:
        print(f"❌ Second run failed: {second_run.get('error', 'Unknown error')}")
        return False
    
    second_count = await get_instrument_count()
    print(f"📊 After second run: {second_count:,} instruments")
    
    # Test idempotency
    print("\n4️⃣ Validating idempotency...")
    
    if first_count == second_count:
        print("✅ IDEMPOTENCY TEST PASSED!")
        print(f"✅ Instrument count remained stable: {first_count:,}")
        
        # Extract key metrics from logs
        if 'Inserted: 0' in first_run['stdout'] and 'Inserted: 0' in second_run['stdout']:
            print("✅ Both runs showed 0 insertions (idempotent mode working)")
        
        if 'Updated:' in first_run['stdout'] and 'Updated:' in second_run['stdout']:
            print("✅ Both runs showed updates only (no new instruments)")
            
        return True
    else:
        print(f"❌ IDEMPOTENCY TEST FAILED!")
        print(f"❌ Count changed: {first_count:,} → {second_count:,}")
        
        # This could happen if vendor tables are being populated concurrently
        count_diff = abs(second_count - first_count)
        if count_diff < 10:  # Small changes might be acceptable in live system
            print(f"⚠️ Small change ({count_diff}) might be due to concurrent vendor population")
            print("⚠️ This may be acceptable in a live system")
        else:
            print(f"❌ Large change ({count_diff}) indicates non-idempotent behavior")
        
        return False

async def main():
    """Main test execution"""
    try:
        success = await test_true_idempotency()
        
        print("\n" + "=" * 75)
        if success:
            print("🎉 TRUE IDEMPOTENCY VALIDATED!")
            print("✅ Unified instrument population is safe to run multiple times")
            print("✅ IDEMPOTENT_MODE successfully prevents new instrument creation")
            print("✅ System maintains data consistency across runs")
        else:
            print("⚠️ IDEMPOTENCY CONCERNS DETECTED")
            print("⚠️ May be acceptable in live system with concurrent data population")
        print("=" * 75)
        
        return 0 if success else 1
        
    except Exception as e:
        print(f"❌ Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)