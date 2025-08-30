#!/usr/bin/env python3
"""
Validation script for Fundamental DAOs implementation.
Tests against real database data to ensure functionality works correctly.
"""

import asyncio
import sys
import logging
import os

# Setup environment
os.environ.setdefault('PYTHONPATH', '/workspace/src')
os.environ.setdefault('ENVIRONMENT', 'dev')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """Validate fundamental DAO implementation"""
    try:
        # Import after environment setup
        from dao.fundamentals_fmp_dao import FundamentalsFMPDAO
        from dao.fundamentals_polygon_dao import FundamentalsPolygonDAO
        from dao.fundamentals_tiingo_dao import FundamentalsTiingoDAO
        from market_data.fundamentals.unified_fundamental_provider import UnifiedFundamentalProvider
        from config.environment import Environment
        
        logger.info("✅ Successfully imported all fundamental DAO classes")
        
        # Initialize environment and components
        env = Environment()
        fmp_dao = FundamentalsFMPDAO(env)
        polygon_dao = FundamentalsPolygonDAO(env)  
        tiingo_dao = FundamentalsTiingoDAO(env)
        provider = UnifiedFundamentalProvider(env)
        
        logger.info("✅ Successfully created all DAO instances")
        
        # Test with a real symbol that has data
        test_symbol = "A"  # From our sample data query
        
        # Check symbols available from each DAO
        logger.info("Testing individual DAO symbol listings...")
        
        fmp_symbols = await fmp_dao.get_symbols_with_data()
        polygon_symbols = await polygon_dao.get_symbols_with_data()
        tiingo_symbols = await tiingo_dao.get_symbols_with_data()
        
        logger.info(f"FMP symbols: {len(fmp_symbols) if fmp_symbols else 0}")
        logger.info(f"Polygon symbols: {len(polygon_symbols) if polygon_symbols else 0}")
        logger.info(f"Tiingo symbols: {len(tiingo_symbols) if tiingo_symbols else 0}")
        
        # Test UnifiedFundamentalProvider symbol listing
        logger.info("Testing UnifiedFundamentalProvider symbol listing...")
        unified_symbols = await provider.list_symbols_with_data()
        logger.info(f"Unified symbols: {len(unified_symbols) if unified_symbols else 0}")
        
        # Test retrieving data for a real symbol
        if polygon_symbols and len(polygon_symbols) > 0:
            test_symbol = polygon_symbols[0]
            logger.info(f"Testing with real symbol: {test_symbol}")
            
            # Get latest fundamental data for the symbol
            latest_polygon = await polygon_dao.get_latest_fundamental(test_symbol)
            if latest_polygon:
                logger.info(f"✅ Retrieved Polygon data for {test_symbol}: revenue={latest_polygon.revenue}")
                
                # Test unified provider with this real data
                unified_result = await provider.get_unified_fundamental(test_symbol, latest_polygon.date)
                if unified_result:
                    logger.info(f"✅ Unified provider result for {test_symbol}:")
                    logger.info(f"   - Status: {unified_result.status}")
                    logger.info(f"   - Confidence: {unified_result.confidence_score}")
                    logger.info(f"   - Vendor count: {len(unified_result.vendor_data)}")
                    logger.info(f"   - Vendors: {[vd.vendor for vd in unified_result.vendor_data]}")
                else:
                    logger.info(f"⚠️ No unified result for {test_symbol} on {latest_polygon.date}")
            else:
                logger.info(f"⚠️ No Polygon data found for {test_symbol}")
        
        logger.info("✅ Fundamental DAO implementation validation completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)