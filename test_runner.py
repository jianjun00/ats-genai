#!/usr/bin/env python3
"""
Test Runner for Real-time Collection System

Runs a basic validation of the test suite and collector functionality
without requiring all external dependencies.
"""

import sys
import os
import asyncio
import logging
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_collector_initialization():
    """Test collector initialization without database"""
    try:
        from src.market_data.realtime.aapl_tsla_synthetic_collector import AAPLTSLASyntheticCollector
        
        collector = AAPLTSLASyntheticCollector()
        
        # Test basic properties
        assert collector.symbols == ['AAPL', 'TSLA']
        assert collector.collection_interval == 60
        assert collector.base_prices['AAPL'] == 225.0
        assert collector.base_prices['TSLA'] == 330.0
        assert not collector.running
        
        logger.info("✅ Collector initialization test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Collector initialization test failed: {e}")
        return False

def test_data_generation():
    """Test synthetic data generation"""
    try:
        from src.market_data.realtime.aapl_tsla_synthetic_collector import AAPLTSLASyntheticCollector
        
        collector = AAPLTSLASyntheticCollector()
        timestamp = datetime.now()
        
        # Test Tiingo bar generation
        tiingo_bar = collector.generate_minute_bar('AAPL', timestamp, 'tiingo')
        
        assert tiingo_bar['symbol'] == 'AAPL'
        assert tiingo_bar['timestamp'] == timestamp
        assert tiingo_bar['vendor'] == 'tiingo'
        assert tiingo_bar['open_price'] > 0
        assert tiingo_bar['high_price'] >= tiingo_bar['low_price']
        assert tiingo_bar['high_price'] >= tiingo_bar['open_price']
        assert tiingo_bar['high_price'] >= tiingo_bar['close_price']
        assert tiingo_bar['low_price'] <= tiingo_bar['open_price']
        assert tiingo_bar['low_price'] <= tiingo_bar['close_price']
        assert tiingo_bar['volume'] > 0
        assert 0 <= tiingo_bar['quality_score'] <= 1
        
        # Test Polygon bar generation
        polygon_bar = collector.generate_minute_bar('TSLA', timestamp, 'polygon')
        
        assert polygon_bar['symbol'] == 'TSLA'
        assert polygon_bar['vendor'] == 'polygon'
        assert 'vwap' in polygon_bar
        assert 'trade_count' in polygon_bar
        assert polygon_bar['vwap'] > 0
        assert polygon_bar['trade_count'] > 0
        
        # Test price ranges
        assert 200 <= tiingo_bar['close_price'] <= 250  # AAPL range
        assert 300 <= polygon_bar['close_price'] <= 360  # TSLA range
        
        logger.info("✅ Data generation test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Data generation test failed: {e}")
        return False

def test_price_relationships():
    """Test OHLC price relationships"""
    try:
        from src.market_data.realtime.aapl_tsla_synthetic_collector import AAPLTSLASyntheticCollector
        
        collector = AAPLTSLASyntheticCollector()
        timestamp = datetime.now()
        
        # Generate multiple bars and test relationships
        violations = 0
        for _ in range(100):
            bar = collector.generate_minute_bar('AAPL', timestamp, 'tiingo')
            
            # Check OHLC relationships
            if not (bar['high_price'] >= bar['open_price'] and
                   bar['high_price'] >= bar['close_price'] and
                   bar['high_price'] >= bar['low_price'] and
                   bar['low_price'] <= bar['open_price'] and
                   bar['low_price'] <= bar['close_price']):
                violations += 1
        
        violation_rate = violations / 100
        assert violation_rate < 0.01, f"OHLC violation rate {violation_rate:.2%} too high"
        
        logger.info(f"✅ Price relationships test passed (violation rate: {violation_rate:.2%})")
        return True
        
    except Exception as e:
        logger.error(f"❌ Price relationships test failed: {e}")
        return False

def test_data_quality_metrics():
    """Test data quality scoring"""
    try:
        from src.market_data.realtime.aapl_tsla_synthetic_collector import AAPLTSLASyntheticCollector
        
        collector = AAPLTSLASyntheticCollector()
        timestamp = datetime.now()
        
        quality_scores = []
        volume_values = []
        latency_values = []
        
        for symbol in ['AAPL', 'TSLA']:
            for vendor in ['tiingo', 'polygon']:
                bar = collector.generate_minute_bar(symbol, timestamp, vendor)
                
                quality_scores.append(bar['quality_score'])
                volume_values.append(bar['volume'])
                latency_values.append(bar['data_latency_ms'])
        
        # Test quality score distribution
        avg_quality = sum(quality_scores) / len(quality_scores)
        assert 0.8 <= avg_quality <= 1.0, f"Average quality {avg_quality:.3f} outside expected range"
        
        # Test volume realism
        avg_volume = sum(volume_values) / len(volume_values)
        assert avg_volume > 20000, f"Average volume {avg_volume} too low"
        
        # Test latency values
        avg_latency = sum(latency_values) / len(latency_values)
        assert 0 <= avg_latency <= 5000, f"Average latency {avg_latency}ms outside expected range"
        
        logger.info(f"✅ Data quality metrics test passed (quality: {avg_quality:.3f}, volume: {avg_volume:.0f}, latency: {avg_latency:.0f}ms)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Data quality metrics test failed: {e}")
        return False

def run_test_suite():
    """Run the complete test suite"""
    logger.info("🧪 Starting Real-time Collection Test Suite")
    logger.info("=" * 60)
    
    tests = [
        ("Collector Initialization", test_collector_initialization),
        ("Data Generation", test_data_generation),
        ("Price Relationships", test_price_relationships),
        ("Data Quality Metrics", test_data_quality_metrics)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        logger.info(f"\n🔬 Running: {test_name}")
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(f"❌ {test_name} failed with exception: {e}")
            failed += 1
    
    logger.info("\n" + "=" * 60)
    logger.info("📊 Test Suite Summary:")
    logger.info(f"   ✅ Passed: {passed}")
    logger.info(f"   ❌ Failed: {failed}")
    logger.info(f"   📈 Success Rate: {passed/(passed+failed)*100:.1f}%")
    
    if failed == 0:
        logger.info("🎉 All tests passed! Real-time collection system is working correctly.")
    else:
        logger.warning(f"⚠️  {failed} tests failed. Please review the errors above.")
    
    return failed == 0

if __name__ == "__main__":
    success = run_test_suite()
    sys.exit(0 if success else 1)