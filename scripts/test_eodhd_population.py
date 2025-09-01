#!/usr/bin/env python3
"""
Test EODHD Population Script

Tests the 30-year population script with a small sample to verify:
- EODHD API connectivity
- D: drive storage functionality  
- File-based storage operations
- Checkpoint system
- Error handling

Run this before attempting the full 30-year population.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
import json
import tempfile

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from populate_30year_eodhd_minute_bars import EODHD30YearPopulator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def test_api_connection():
    """Test EODHD API connection"""
    
    logger.info("Testing EODHD API connection...")
    
    api_key = os.getenv('EODHD_API_KEY')
    if not api_key:
        logger.error("EODHD_API_KEY not set")
        return False
    
    try:
        from market_data.agent.eodhd_minute_adapter import EODHDMinuteAdapter
        
        adapter = EODHDMinuteAdapter(api_key)
        
        # Test instrument fetch
        instruments = adapter.fetch_instruments()
        logger.info(f"✅ API connection successful - {len(instruments)} instruments available")
        
        # Test minute data fetch for one symbol (small date range)
        async with adapter:
            test_date = datetime.now() - timedelta(days=1)
            bars = await adapter.fetch_minute_bars_async(
                'AAPL',
                test_date,
                test_date
            )
            logger.info(f"✅ Minute data fetch test - {len(bars)} bars retrieved for AAPL")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ API connection failed: {e}")
        return False

async def test_storage_setup():
    """Test D: drive storage setup"""
    
    logger.info("Testing D: drive storage setup...")
    
    # Check if D: drive is accessible
    d_drive_paths = ["/mnt/d", "/mnt/d/ats-data"]
    
    for path in d_drive_paths:
        if os.path.exists(path):
            logger.info(f"✅ Found D: drive path: {path}")
            
            # Test write permissions
            test_file = Path(path) / "test_write.tmp"
            try:
                with open(test_file, 'w') as f:
                    f.write("test")
                test_file.unlink()
                logger.info(f"✅ Write permissions verified: {path}")
                return True
            except Exception as e:
                logger.error(f"❌ Write permission test failed: {e}")
                return False
    
    logger.error("❌ D: drive not accessible")
    return False

async def test_populator_initialization():
    """Test populator initialization"""
    
    logger.info("Testing populator initialization...")
    
    # Use temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            populator = EODHD30YearPopulator(
                storage_path=temp_dir,
                checkpoint_file="test_checkpoint.json",
                max_concurrent=1,
                debug=True
            )
            
            await populator.initialize()
            logger.info("✅ Populator initialization successful")
            
            # Test universe loading
            if populator.universe_symbols:
                logger.info(f"✅ Universe loaded: {len(populator.universe_symbols)} symbols")
            else:
                logger.warning("⚠️ Universe loading returned no symbols")
            
            await populator.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Populator initialization failed: {e}")
            return False

async def test_checkpoint_system():
    """Test checkpoint save/load functionality"""
    
    logger.info("Testing checkpoint system...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            populator = EODHD30YearPopulator(
                storage_path=temp_dir,
                checkpoint_file=os.path.join(temp_dir, "test_checkpoint.json"),
                debug=True
            )
            
            await populator.initialize()
            
            # Create test checkpoint
            start_date = date(2024, 1, 1)
            end_date = date(2024, 1, 2)
            test_symbols = {'AAPL', 'MSFT'}
            
            checkpoint = await populator.create_checkpoint(start_date, end_date, test_symbols)
            logger.info("✅ Checkpoint creation successful")
            
            # Test save
            await populator.save_checkpoint(checkpoint)
            logger.info("✅ Checkpoint save successful")
            
            # Test load
            loaded_checkpoint = await populator.load_checkpoint()
            if loaded_checkpoint:
                logger.info("✅ Checkpoint load successful")
                logger.info(f"   Loaded: {loaded_checkpoint.total_symbols} symbols, {loaded_checkpoint.start_date} to {loaded_checkpoint.end_date}")
            else:
                logger.error("❌ Checkpoint load failed")
                return False
            
            await populator.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Checkpoint system test failed: {e}")
            return False

async def test_sample_population():
    """Test actual data population with a tiny sample"""
    
    logger.info("Testing sample data population...")
    
    # Use /tmp for testing to avoid D: drive issues during testing
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            populator = EODHD30YearPopulator(
                storage_path=temp_dir,
                checkpoint_file=os.path.join(temp_dir, "sample_checkpoint.json"),
                debug=True
            )
            
            await populator.initialize()
            
            # Test with very small date range and one symbol
            test_symbols = ['AAPL']
            start_date = datetime.now().date() - timedelta(days=2)  # 2 days ago
            end_date = datetime.now().date() - timedelta(days=1)    # 1 day ago
            
            logger.info(f"Testing population: {test_symbols[0]} from {start_date} to {end_date}")
            
            # Run mini population
            await populator.run_full_population(
                start_date=start_date,
                end_date=end_date,
                limit=1,
                symbols=test_symbols
            )
            
            logger.info("✅ Sample population completed")
            
            # Check results
            if populator.stats['symbols_processed'] > 0:
                logger.info(f"✅ Processing stats: {populator.stats}")
            else:
                logger.warning("⚠️ No symbols were processed")
            
            await populator.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Sample population failed: {e}")
            return False

async def run_all_tests():
    """Run all tests in sequence"""
    
    print("🧪 EODHD 30-YEAR POPULATION TEST SUITE")
    print("=" * 50)
    
    tests = [
        ("API Connection", test_api_connection),
        ("Storage Setup", test_storage_setup),
        ("Populator Initialization", test_populator_initialization),
        ("Checkpoint System", test_checkpoint_system),
        ("Sample Population", test_sample_population)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n🔍 Running: {test_name}")
        print("-" * 30)
        
        try:
            success = await test_func()
            results[test_name] = success
            
            if success:
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
                
        except Exception as e:
            results[test_name] = False
            print(f"💥 {test_name}: ERROR - {e}")
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name:.<30} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Ready for 30-year population.")
        print("\nTo run the full population:")
        print("  1. First set up D: drive storage:")
        print("     python scripts/setup_d_drive_storage.py")
        print("  2. Then run with debug mode:")
        print("     python scripts/populate_30year_eodhd_minute_bars.py --debug --limit 5")
        print("  3. After testing, run full population:")
        print("     python scripts/populate_30year_eodhd_minute_bars.py --mode full")
    else:
        print(f"\n⚠️ {total - passed} tests failed. Fix issues before running full population.")
    
    return passed == total

async def main():
    """Main test runner"""
    
    # Check prerequisites
    if not os.getenv('EODHD_API_KEY'):
        print("❌ EODHD_API_KEY environment variable not set")
        print("Please set your EODHD API key:")
        print("  export EODHD_API_KEY='your-api-key-here'")
        return 1
    
    try:
        success = await run_all_tests()
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
        return 1
    except Exception as e:
        print(f"\n💥 Test suite failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)