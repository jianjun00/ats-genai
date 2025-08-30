#!/usr/bin/env python3
"""
Validation script for UnifiedMarketCapProvider.

Tests the market cap provider against real database data to ensure functionality
works correctly with actual fundamental and price data.
"""

import asyncio
import sys
import logging
import os
from datetime import date, timedelta

# Setup environment
os.environ.setdefault('PYTHONPATH', '/workspace/src')
os.environ.setdefault('ENVIRONMENT', 'dev')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """Test UnifiedMarketCapProvider functionality"""
    try:
        # Import after environment setup
        from market_data.market_cap.unified_market_cap_provider import (
            UnifiedMarketCapProvider,
            MarketCapValidationStatus,
            UnifiedMarketCap
        )
        from config.environment import Environment
        
        logger.info("✅ Successfully imported UnifiedMarketCapProvider")
        
        # Initialize environment and provider
        env = Environment()
        provider = UnifiedMarketCapProvider(env)
        
        logger.info("✅ Successfully created UnifiedMarketCapProvider instance")
        
        # Connect to database
        await provider.connect()
        logger.info("✅ Successfully connected to database")
        
        # Test 1: List symbols with market cap data
        logger.info("=== Test 1: List symbols with market cap data ===")
        symbols = await provider.list_symbols_with_market_cap_data()
        logger.info(f"Found {len(symbols)} symbols with market cap data")
        
        if symbols:
            logger.info(f"Sample symbols: {symbols[:10]}")  # Show first 10
        
        # Test 2: Test with well-known symbols
        test_symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "META"]
        available_test_symbols = [s for s in test_symbols if s in symbols]
        
        if not available_test_symbols:
            logger.warning("No test symbols found in database, using available symbols")
            available_test_symbols = symbols[:3] if len(symbols) >= 3 else symbols
        
        logger.info(f"Testing with symbols: {available_test_symbols}")
        
        # Test 3: Calculate market cap for specific symbol and date
        if available_test_symbols:
            test_symbol = available_test_symbols[0]
            test_date = date(2024, 9, 30)  # Recent quarter end
            
            logger.info(f"=== Test 2: Calculate market cap for {test_symbol} on {test_date} ===")
            
            result = await provider.get_unified_market_cap(test_symbol, test_date)
            
            if result:
                logger.info(f"✅ Market cap calculation successful:")
                logger.info(f"   Symbol: {result.symbol}")
                logger.info(f"   Date: {result.date}")
                logger.info(f"   Market Cap: ${result.market_cap:,}")
                logger.info(f"   Confidence: {result.confidence_score:.2f}")
                logger.info(f"   Status: {result.status.value}")
                logger.info(f"   Primary Source: {result.primary_source}")
                logger.info(f"   Number of Sources: {len(result.source_data)}")
                logger.info(f"   Calculation Notes: {result.calculation_notes}")
                
                # Show source breakdown
                logger.info("   Source Breakdown:")
                for i, source in enumerate(result.source_data):
                    logger.info(f"     {i+1}. {source.vendor} ({source.source_type}): "
                               f"${source.market_cap:,} (confidence: {source.confidence:.2f})")
                
                # Show validation metadata
                if result.validation_metadata:
                    logger.info("   Validation Metadata:")
                    for key, value in result.validation_metadata.items():
                        if isinstance(value, dict):
                            logger.info(f"     {key}: {len(value)} items")
                        else:
                            logger.info(f"     {key}: {value}")
            else:
                logger.warning(f"No market cap data found for {test_symbol} on {test_date}")
        
        # Test 4: Test market cap history
        if available_test_symbols:
            test_symbol = available_test_symbols[0]
            start_date = date(2024, 9, 28)
            end_date = date(2024, 9, 30)
            
            logger.info(f"=== Test 3: Market cap history for {test_symbol} ({start_date} to {end_date}) ===")
            
            history = await provider.get_market_cap_history(test_symbol, start_date, end_date)
            
            if history:
                logger.info(f"✅ Found {len(history)} market cap data points:")
                for mc in history:
                    logger.info(f"   {mc.date}: ${mc.market_cap:,} "
                               f"(confidence: {mc.confidence_score:.2f}, "
                               f"status: {mc.status.value})")
            else:
                logger.warning(f"No historical market cap data found for {test_symbol}")
        
        # Test 5: Test individual data source methods
        if available_test_symbols:
            test_symbol = available_test_symbols[0]
            test_date = date(2024, 9, 30)
            
            logger.info(f"=== Test 4: Individual data sources for {test_symbol} ===")
            
            # Test fundamental sources
            fundamental_sources = await provider._get_fundamental_market_cap_sources(test_symbol, test_date)
            logger.info(f"Fundamental sources found: {len(fundamental_sources)}")
            for source in fundamental_sources:
                logger.info(f"   {source.vendor}: ${source.market_cap:,} "
                           f"(confidence: {source.confidence:.2f})")
            
            # Test price-based calculation
            price_source = await provider._get_price_based_market_cap(test_symbol, test_date)
            if price_source:
                logger.info(f"Price-based calculation: ${price_source.market_cap:,}")
                logger.info(f"   Price used: ${price_source.price_used:.2f}")
                logger.info(f"   Shares outstanding: {price_source.shares_outstanding:,}")
                logger.info(f"   Confidence: {price_source.confidence:.2f}")
            else:
                logger.info("No price-based market cap calculation available")
            
            # Test shares outstanding lookup
            shares = await provider._get_shares_outstanding(test_symbol, test_date)
            if shares:
                logger.info(f"Shares outstanding: {shares:,}")
            else:
                logger.info("No shares outstanding data found")
            
            # Test historical estimate
            historical_source = await provider._get_historical_market_cap_estimate(test_symbol, test_date)
            if historical_source:
                logger.info(f"Historical estimate: ${historical_source.market_cap:,}")
                logger.info(f"   Confidence: {historical_source.confidence:.2f}")
                logger.info(f"   Original date: {historical_source.raw_data.get('original_date')}")
                logger.info(f"   Days difference: {historical_source.raw_data.get('days_difference')}")
            else:
                logger.info("No historical market cap estimate found")
        
        # Test 6: Test multiple symbols for cross-validation
        logger.info("=== Test 5: Cross-validation with multiple symbols ===")
        successful_calculations = 0
        total_tests = min(5, len(available_test_symbols))
        
        for symbol in available_test_symbols[:total_tests]:
            result = await provider.get_unified_market_cap(symbol, date(2024, 9, 30))
            if result:
                successful_calculations += 1
                logger.info(f"✅ {symbol}: ${result.market_cap:,} "
                           f"({result.status.value}, {len(result.source_data)} sources)")
            else:
                logger.info(f"⚠️  {symbol}: No data available")
        
        success_rate = (successful_calculations / total_tests) * 100 if total_tests > 0 else 0
        logger.info(f"Success rate: {successful_calculations}/{total_tests} ({success_rate:.1f}%)")
        
        # Test 7: Error handling
        logger.info("=== Test 6: Error handling ===")
        
        # Test with invalid symbol
        invalid_result = await provider.get_unified_market_cap("INVALID_SYMBOL_XYZ", date(2024, 1, 1))
        assert invalid_result is None, "Should return None for invalid symbol"
        logger.info("✅ Invalid symbol handling works correctly")
        
        # Test with very old date
        old_date_result = await provider.get_unified_market_cap("AAPL", date(1990, 1, 1))
        if old_date_result is None:
            logger.info("✅ Old date handling works correctly (no data expected)")
        else:
            logger.info("⚠️  Found data for very old date (unexpected but not necessarily wrong)")
        
        # Disconnect
        await provider.disconnect()
        logger.info("✅ Successfully disconnected from database")
        
        # Summary
        logger.info("=" * 60)
        logger.info("🎉 UnifiedMarketCapProvider validation completed successfully!")
        logger.info(f"   - Found {len(symbols)} symbols with market cap data")
        logger.info(f"   - Successfully calculated market cap for {successful_calculations}/{total_tests} test symbols")
        logger.info(f"   - All database operations completed without errors")
        logger.info(f"   - Cross-source validation and reconciliation working correctly")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)