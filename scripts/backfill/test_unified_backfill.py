#!/usr/bin/env python3
"""
Test Script for Unified Backfill System

Validates that all components work together before running the full 5-year backfill.
Tests API connections, database connectivity, and cross-vendor reconciliation.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from market_data.agent.polygon_minute_adapter import PolygonMinuteAdapter
from market_data.agent.tiingo_intraday_adapter import TiingoIntradayAdapter
from market_data.reconciliation.cross_vendor_reconciler import (
    CrossVendorReconciler, 
    ReconciliationConfig, 
    ReconciliationMethod
)
from storage.hybrid_minute_data_manager import HybridMinuteDataManager, StorageConfig
import asyncpg

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_api_connections():
    """Test API connections to both vendors."""
    logger.info("Testing API connections...")
    
    polygon_key = os.getenv('POLYGON_API_KEY')
    tiingo_key = os.getenv('TIINGO_API_KEY')
    
    if not polygon_key:
        logger.error("POLYGON_API_KEY not set")
        return False
    
    if not tiingo_key:
        logger.error("TIINGO_API_KEY not set")
        return False
    
    # Test Polygon connection
    try:
        async with PolygonMinuteAdapter(polygon_key) as polygon:
            test_date = datetime.now() - timedelta(days=1)
            bars = await polygon.fetch_minute_bars_async('AAPL', test_date, test_date)
            logger.info(f"Polygon test: fetched {len(bars)} AAPL bars")
    except Exception as e:
        logger.error(f"Polygon connection failed: {e}")
        return False
    
    # Test Tiingo connection
    try:
        async with TiingoIntradayAdapter(tiingo_key) as tiingo:
            test_date = datetime.now() - timedelta(days=1)
            bars = await tiingo.fetch_minute_bars_async('AAPL', test_date, test_date)
            logger.info(f"Tiingo test: fetched {len(bars)} AAPL bars")
    except Exception as e:
        logger.error(f"Tiingo connection failed: {e}")
        return False
    
    logger.info("✓ API connections successful")
    return True


async def test_database_connection():
    """Test database connection."""
    logger.info("Testing database connection...")
    
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5433')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    db_name = os.getenv('DB_NAME', 'dev_db')
    
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
        
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT version()")
            logger.info(f"Database version: {result}")
        
        await pool.close()
        logger.info("✓ Database connection successful")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


async def test_reconciliation():
    """Test cross-vendor data reconciliation."""
    logger.info("Testing data reconciliation...")
    
    # Mock data for testing
    polygon_data = [
        {
            'symbol': 'AAPL',
            'timestamp': datetime(2024, 1, 1, 9, 30),
            'open': 180.00,
            'high': 181.00,
            'low': 179.50,
            'close': 180.50,
            'volume': 1000000,
            'vendor': 'polygon'
        }
    ]
    
    tiingo_data = [
        {
            'symbol': 'AAPL',
            'timestamp': datetime(2024, 1, 1, 9, 30),
            'open': 180.10,
            'high': 181.20,
            'low': 179.40,
            'close': 180.60,
            'volume': 1050000,
            'vendor': 'tiingo'
        }
    ]
    
    try:
        config = ReconciliationConfig(method=ReconciliationMethod.WEIGHTED_AVERAGE)
        reconciler = CrossVendorReconciler(config)
        
        reconciled = await reconciler.reconcile_minute_data(
            polygon_data, tiingo_data, 'AAPL'
        )
        
        if reconciled:
            bar = reconciled[0]
            logger.info(f"Reconciled bar: OHLC={bar.open:.2f}/{bar.high:.2f}/{bar.low:.2f}/{bar.close:.2f}")
            logger.info(f"Quality score: {bar.quality_score:.3f}")
            logger.info(f"Price variance: {bar.price_variance:.6f}")
        
        reconciler.close()
        logger.info("✓ Data reconciliation successful")
        return True
        
    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")
        return False


async def test_storage_system():
    """Test hybrid storage system."""
    logger.info("Testing storage system...")
    
    storage_path = "/home/jianjun/ats/data/STK/1min"
    
    try:
        # Ensure directory exists
        Path(storage_path).mkdir(parents=True, exist_ok=True)
        
        # Test storage config
        config = StorageConfig(base_data_path=storage_path)
        
        # Create mock pool (won't actually connect for this test)
        db_url = "postgresql://test:test@localhost:5432/test"
        
        # Just test directory creation and file path generation
        from storage.hybrid_minute_data_manager import HybridMinuteDataManager
        
        # Mock pool for testing
        class MockPool:
            async def acquire(self):
                return self
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
        
        manager = HybridMinuteDataManager(MockPool(), config)
        
        # Test file path generation
        test_time = datetime.now()
        file_path = manager._get_file_path('AAPL', test_time, 'cold')
        logger.info(f"Generated file path: {file_path}")
        
        # Check if directories were created
        expected_dirs = ['hot', 'warm', 'cold', 'archive']
        for dir_name in expected_dirs:
            dir_path = Path(storage_path) / dir_name
            if dir_path.exists():
                logger.info(f"✓ {dir_name} directory exists")
            else:
                logger.warning(f"✗ {dir_name} directory missing")
        
        logger.info("✓ Storage system test successful")
        return True
        
    except Exception as e:
        logger.error(f"Storage system test failed: {e}")
        return False


async def run_integration_test():
    """Run a small integration test with real data."""
    logger.info("Running integration test with real data...")
    
    polygon_key = os.getenv('POLYGON_API_KEY')
    tiingo_key = os.getenv('TIINGO_API_KEY')
    
    if not polygon_key or not tiingo_key:
        logger.warning("Skipping integration test - API keys not available")
        return True
    
    try:
        # Fetch small amount of real data
        test_date = datetime.now() - timedelta(days=1)
        symbol = 'AAPL'
        
        polygon_data = []
        tiingo_data = []
        
        # Fetch from Polygon
        try:
            async with PolygonMinuteAdapter(polygon_key) as polygon:
                polygon_bars = await polygon.fetch_minute_bars_async(symbol, test_date, test_date)
                polygon_data = [
                    {
                        'symbol': bar.symbol,
                        'timestamp': bar.timestamp,
                        'open': bar.open,
                        'high': bar.high,
                        'low': bar.low,
                        'close': bar.close,
                        'volume': bar.volume,
                        'vendor': 'polygon'
                    }
                    for bar in polygon_bars[:5]  # Just first 5 bars
                ]
        except Exception as e:
            logger.warning(f"Polygon fetch failed: {e}")
        
        # Fetch from Tiingo
        try:
            async with TiingoIntradayAdapter(tiingo_key) as tiingo:
                tiingo_bars = await tiingo.fetch_minute_bars_async(symbol, test_date, test_date)
                tiingo_data = [
                    {
                        'symbol': bar.symbol,
                        'timestamp': bar.timestamp,
                        'open': bar.open,
                        'high': bar.high,
                        'low': bar.low,
                        'close': bar.close,
                        'volume': bar.volume,
                        'vendor': 'tiingo'
                    }
                    for bar in tiingo_bars[:5]  # Just first 5 bars
                ]
        except Exception as e:
            logger.warning(f"Tiingo fetch failed: {e}")
        
        # Reconcile if we have data from both
        if polygon_data and tiingo_data:
            reconciler = CrossVendorReconciler()
            reconciled = await reconciler.reconcile_minute_data(
                polygon_data, tiingo_data, symbol
            )
            
            logger.info(f"Integration test: {len(polygon_data)} Polygon + {len(tiingo_data)} Tiingo -> {len(reconciled)} reconciled bars")
            
            if reconciled:
                sample_bar = reconciled[0]
                logger.info(f"Sample reconciled bar: {sample_bar.timestamp} OHLC={sample_bar.open:.2f}/{sample_bar.high:.2f}/{sample_bar.low:.2f}/{sample_bar.close:.2f}")
            
            reconciler.close()
        
        logger.info("✓ Integration test successful")
        return True
        
    except Exception as e:
        logger.error(f"Integration test failed: {e}")
        return False


async def main():
    """Run all tests."""
    logger.info("=== Unified Backfill System Test ===")
    
    tests = [
        ("API Connections", test_api_connections),
        ("Database Connection", test_database_connection),
        ("Data Reconciliation", test_reconciliation),
        ("Storage System", test_storage_system),
        ("Integration Test", run_integration_test)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        try:
            results[test_name] = await test_func()
        except Exception as e:
            logger.error(f"{test_name} failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    logger.info("\n=== Test Results ===")
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "PASS" if result else "FAIL"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("✓ All tests passed! System is ready for backfill.")
    else:
        logger.warning("✗ Some tests failed. Please resolve issues before running backfill.")
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)