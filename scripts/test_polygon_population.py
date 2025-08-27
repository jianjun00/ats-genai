#!/usr/bin/env python3
"""
Test Polygon Population Script

Tests the 30-year Polygon population script with a small sample to verify:
- Polygon API connectivity and rate limits
- D: drive storage functionality  
- File-based storage operations
- Checkpoint system functionality
- Quality validation
- Error handling and recovery

Run this before attempting the full 30-year Polygon population.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
import json
import tempfile
import time

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from populate_30year_polygon_minute_bars import Polygon30YearPopulator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def test_polygon_api_connection():
    """Test Polygon API connection and rate limits"""
    
    logger.info("Testing Polygon API connection...")
    
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        logger.error("POLYGON_API_KEY not set")
        return False
    
    try:
        from market_data.agent.polygon_minute_adapter import PolygonMinuteAdapter
        from market_data.agent.polygon_adapter import PolygonAdapter
        
        # Test instrument fetch
        polygon_adapter = PolygonAdapter(api_key)
        instruments = polygon_adapter.fetch_instruments()
        logger.info(f"✅ Instruments API - {len(instruments)} instruments available")
        
        # Test minute data fetch for small sample
        async with PolygonMinuteAdapter(api_key) as minute_adapter:
            test_date = datetime.now() - timedelta(days=2)  # 2 days ago
            
            start_time = time.time()
            bars = await minute_adapter.fetch_minute_bars_async(
                'AAPL',
                test_date,
                test_date
            )
            request_time = time.time() - start_time
            
            logger.info(f"✅ Minute data API - {len(bars)} bars retrieved for AAPL")
            logger.info(f"   Request time: {request_time:.2f}s")
            
            # Test data quality validation
            if bars:
                quality_metrics = minute_adapter.validate_data_quality(bars)
                logger.info(f"✅ Data quality validation - Valid: {quality_metrics['valid']}")
                logger.info(f"   Completeness: {quality_metrics['data_completeness']:.2%}")
                logger.info(f"   Time gaps: {quality_metrics['time_gaps']}")
                logger.info(f"   Avg volume: {quality_metrics['avg_volume']:,.0f}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Polygon API test failed: {e}")
        return False

async def test_rate_limiting():
    """Test rate limiting behavior"""
    
    logger.info("Testing rate limiting behavior...")
    
    try:
        from market_data.agent.polygon_minute_adapter import PolygonMinuteAdapter
        
        async with PolygonMinuteAdapter() as adapter:
            # Make multiple quick requests to test rate limiting
            symbols = ['AAPL', 'MSFT', 'GOOGL']
            test_date = datetime.now() - timedelta(days=1)
            
            start_time = time.time()
            
            for symbol in symbols:
                logger.info(f"Testing rate limit with {symbol}...")
                bars = await adapter.fetch_minute_bars_async(
                    symbol,
                    test_date,
                    test_date
                )
                logger.info(f"   {symbol}: {len(bars)} bars")
                
                # Don't add artificial delay - let adapter handle it
            
            total_time = time.time() - start_time
            avg_time_per_request = total_time / len(symbols)
            
            logger.info(f"✅ Rate limiting test complete")
            logger.info(f"   Total time: {total_time:.1f}s")
            logger.info(f"   Avg per request: {avg_time_per_request:.1f}s")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Rate limiting test failed: {e}")
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
            test_file = Path(path) / "polygon_test_write.tmp"
            try:
                with open(test_file, 'w') as f:
                    f.write("polygon test")
                test_file.unlink()
                logger.info(f"✅ Write permissions verified: {path}")
                return True
            except Exception as e:
                logger.error(f"❌ Write permission test failed: {e}")
                return False
    
    logger.error("❌ D: drive not accessible")
    return False

async def test_populator_initialization():
    """Test Polygon populator initialization"""
    
    logger.info("Testing Polygon populator initialization...")
    
    # Use temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            populator = Polygon30YearPopulator(
                storage_path=temp_dir,
                checkpoint_file="test_polygon_checkpoint.json",
                max_concurrent=1,
                premium_plan=False,
                debug=True
            )
            
            await populator.initialize()
            logger.info("✅ Polygon populator initialization successful")
            
            # Test universe loading
            if populator.universe_symbols:
                logger.info(f"✅ Universe loaded: {len(populator.universe_symbols)} symbols")
                
                # Log some sample symbols
                sample_symbols = list(populator.universe_symbols)[:10]
                logger.info(f"   Sample symbols: {sample_symbols}")
            else:
                logger.warning("⚠️ Universe loading returned no symbols")
            
            # Test rate limiting configuration
            logger.info(f"✅ Rate limiting configured:")
            logger.info(f"   Requests per minute: {populator.requests_per_minute}")
            logger.info(f"   Delay between requests: {populator.delay_between_requests}s")
            logger.info(f"   Premium plan: {populator.premium_plan}")
            
            await populator.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Polygon populator initialization failed: {e}")
            return False

async def test_checkpoint_system():
    """Test checkpoint save/load functionality"""
    
    logger.info("Testing Polygon checkpoint system...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            populator = Polygon30YearPopulator(
                storage_path=temp_dir,
                checkpoint_file=os.path.join(temp_dir, "test_polygon_checkpoint.json"),
                debug=True
            )
            
            await populator.initialize()
            
            # Create test checkpoint
            start_date = date(2024, 1, 1)
            end_date = date(2024, 1, 2)
            test_symbols = {'AAPL', 'MSFT', 'GOOGL'}
            
            checkpoint = await populator.create_checkpoint(start_date, end_date, test_symbols)
            logger.info("✅ Polygon checkpoint creation successful")
            
            # Verify checkpoint structure
            assert checkpoint.total_symbols == 3
            assert checkpoint.start_date == "2024-01-01"
            assert checkpoint.end_date == "2024-01-02"
            logger.info("✅ Checkpoint structure validated")
            
            # Test save
            await populator.save_checkpoint(checkpoint)
            logger.info("✅ Polygon checkpoint save successful")
            
            # Test load
            loaded_checkpoint = await populator.load_checkpoint()
            if loaded_checkpoint:
                logger.info("✅ Polygon checkpoint load successful")
                logger.info(f"   Loaded: {loaded_checkpoint.total_symbols} symbols")
                logger.info(f"   Date range: {loaded_checkpoint.start_date} to {loaded_checkpoint.end_date}")
                logger.info(f"   Progress: {loaded_checkpoint.processed_symbols}/{loaded_checkpoint.total_symbols}")
            else:
                logger.error("❌ Polygon checkpoint load failed")
                return False
            
            await populator.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Polygon checkpoint system test failed: {e}")
            return False

async def test_sample_population():
    """Test actual data population with a tiny sample"""
    
    logger.info("Testing Polygon sample data population...")
    
    # Use /tmp for testing to avoid D: drive issues during testing
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            populator = Polygon30YearPopulator(
                storage_path=temp_dir,
                checkpoint_file=os.path.join(temp_dir, "sample_polygon_checkpoint.json"),
                max_concurrent=1,
                premium_plan=False,  # Test with free tier constraints
                debug=True
            )
            
            await populator.initialize()
            
            # Test with very small date range and one symbol
            test_symbols = ['AAPL']
            start_date = datetime.now().date() - timedelta(days=3)  # 3 days ago
            end_date = datetime.now().date() - timedelta(days=2)    # 2 days ago
            
            logger.info(f"Testing Polygon population: {test_symbols[0]} from {start_date} to {end_date}")
            
            # Run mini population
            await populator.run_full_population(
                start_date=start_date,
                end_date=end_date,
                limit=1,
                symbols=test_symbols
            )
            
            logger.info("✅ Polygon sample population completed")
            
            # Check results
            if populator.stats['symbols_processed'] > 0:
                logger.info(f"✅ Processing stats:")
                logger.info(f"   Symbols processed: {populator.stats['symbols_processed']}")
                logger.info(f"   Bars collected: {populator.stats['total_bars_collected']}")
                logger.info(f"   Bars stored: {populator.stats['total_bars_stored']}")
                logger.info(f"   Files created: {populator.stats['total_files_created']}")
                logger.info(f"   API calls: {populator.stats['total_api_calls']}")
                logger.info(f"   Rate limit delays: {populator.stats['rate_limit_delays']}")
                logger.info(f"   Average quality: {populator.stats['average_quality_score']:.3f}")
            else:
                logger.warning("⚠️ No symbols were processed")
            
            await populator.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Polygon sample population failed: {e}")
            return False

async def test_premium_vs_free_detection():
    """Test detection of premium vs free Polygon plan"""
    
    logger.info("Testing Polygon plan detection...")
    
    try:
        # Test free tier settings
        free_populator = Polygon30YearPopulator(
            storage_path="/tmp",
            premium_plan=False,
            debug=True
        )
        
        logger.info("✅ Free tier configuration:")
        logger.info(f"   Requests per minute: {free_populator.requests_per_minute}")
        logger.info(f"   Delay between requests: {free_populator.delay_between_requests}s")
        
        # Test premium tier settings
        premium_populator = Polygon30YearPopulator(
            storage_path="/tmp",
            premium_plan=True,
            debug=True
        )
        
        logger.info("✅ Premium tier configuration:")
        logger.info(f"   Requests per minute: {premium_populator.requests_per_minute}")
        logger.info(f"   Delay between requests: {premium_populator.delay_between_requests}s")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Plan detection test failed: {e}")
        return False

async def run_all_tests():
    """Run all tests in sequence"""
    
    print("🧪 POLYGON 30-YEAR POPULATION TEST SUITE")
    print("=" * 55)
    
    tests = [
        ("Polygon API Connection", test_polygon_api_connection),
        ("Rate Limiting", test_rate_limiting),
        ("Storage Setup", test_storage_setup),
        ("Populator Initialization", test_populator_initialization),
        ("Checkpoint System", test_checkpoint_system),
        ("Premium vs Free Detection", test_premium_vs_free_detection),
        ("Sample Population", test_sample_population)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n🔍 Running: {test_name}")
        print("-" * 35)
        
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
    print("\n" + "=" * 55)
    print("📊 POLYGON TEST RESULTS SUMMARY")
    print("=" * 55)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name:.<35} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Ready for Polygon 30-year population.")
        print("\nTo run the full population:")
        print("  1. First set up D: drive storage:")
        print("     python scripts/setup_polygon_d_drive_storage.py")
        print("  2. Test with debug mode:")
        print("     python scripts/populate_30year_polygon_minute_bars.py --debug --limit 5")
        print("  3. Run full population (free tier):")
        print("     python scripts/populate_30year_polygon_minute_bars.py --mode full --concurrent 1")
        print("  4. Run full population (premium tier):")
        print("     python scripts/populate_30year_polygon_minute_bars.py --mode full --premium --concurrent 3")
    else:
        print(f"\n⚠️ {total - passed} tests failed. Fix issues before running full population.")
        
        # Specific recommendations based on failures
        if not results.get("Polygon API Connection", True):
            print("   - Check your POLYGON_API_KEY environment variable")
            print("   - Verify your Polygon API subscription is active")
        
        if not results.get("Storage Setup", True):
            print("   - Ensure D: drive is mounted and accessible")
            print("   - Check write permissions on D: drive")
        
        if not results.get("Rate Limiting", True):
            print("   - Your API key may have rate limit issues")
            print("   - Consider using --premium flag if you have premium plan")
    
    return passed == total

async def main():
    """Main test runner"""
    
    # Check prerequisites
    if not os.getenv('POLYGON_API_KEY'):
        print("❌ POLYGON_API_KEY environment variable not set")
        print("Please set your Polygon API key:")
        print("  export POLYGON_API_KEY='your-polygon-api-key-here'")
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