#!/usr/bin/env python3
"""
Integration test script for Fundamental DAOs and UnifiedFundamentalProvider.
This script tests the implementation against the real database schema.
"""

import asyncio
from datetime import date
import sys
import logging
import os

# Setup environment for K8s
os.environ.setdefault('PYTHONPATH', '/workspace/src')
os.environ.setdefault('ENVIRONMENT', 'dev')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """Test fundamental DAOs and unified provider integration"""
    try:
        from dao.fundamentals_fmp_dao import FundamentalsFMPDAO, FMPFundamental
        from dao.fundamentals_polygon_dao import FundamentalsPolygonDAO, PolygonFundamental
        from dao.fundamentals_tiingo_dao import FundamentalsTiingoDAO, TiingoFundamental
        from market_data.fundamentals.unified_fundamental_provider import UnifiedFundamentalProvider
        from config.environment import Environment
        
        logger.info("Successfully imported all DAO classes")
        
        # Initialize environment and DAOs
        env = Environment()
        fmp_dao = FundamentalsFMPDAO(env)
        polygon_dao = FundamentalsPolygonDAO(env)
        tiingo_dao = FundamentalsTiingoDAO(env)
        provider = UnifiedFundamentalProvider(env)
        
        logger.info("Successfully created DAO instances")
        
        # Test symbol for integration
        test_symbol = "TEST_INTEGRATION_DAO"
        test_date = date(2023, 12, 31)
        
        # Create test data
        fmp_test_data = FMPFundamental(
            symbol=test_symbol,
            date=test_date,
            vendor="fmp",
            fiscal_period="Q4",
            revenue=1000000000,
            net_income=100000000,
            eps=2.50,
            raw_data={"test": "integration"}
        )
        
        polygon_test_data = PolygonFundamental(
            symbol=test_symbol,
            date=test_date,
            vendor="polygon",
            fiscal_period="Q4",
            revenue=1001000000,
            net_income=99500000,
            eps=2.49,
            raw_data={"test": "integration"}
        )
        
        tiingo_test_data = TiingoFundamental(
            symbol=test_symbol,
            date=test_date,
            vendor="tiingo",
            fiscal_period="Q4",
            revenue=999500000,
            net_income=100200000,
            eps=2.51,
            raw_data={"test": "integration"}
        )
        
        logger.info("Created test data objects")
        
        # Cleanup any existing test data
        await fmp_dao.delete_fundamental(test_symbol, test_date)
        await polygon_dao.delete_fundamental(test_symbol, test_date)
        await tiingo_dao.delete_fundamental(test_symbol, test_date)
        
        logger.info("Cleaned up any existing test data")
        
        # Test individual DAO operations
        logger.info("Testing FMP DAO operations...")
        fmp_insert = await fmp_dao.insert_fundamental(fmp_test_data)
        logger.info(f"FMP insert result: {fmp_insert}")
        
        fmp_retrieve = await fmp_dao.get_fundamental(test_symbol, test_date)
        logger.info(f"FMP retrieve success: {fmp_retrieve is not None}")
        assert fmp_retrieve is not None, "Failed to retrieve FMP data"
        assert fmp_retrieve.vendor == "fmp", f"Expected vendor 'fmp', got {fmp_retrieve.vendor}"
        
        logger.info("Testing Polygon DAO operations...")
        polygon_insert = await polygon_dao.insert_fundamental(polygon_test_data)
        logger.info(f"Polygon insert result: {polygon_insert}")
        
        polygon_retrieve = await polygon_dao.get_fundamental(test_symbol, test_date)
        logger.info(f"Polygon retrieve success: {polygon_retrieve is not None}")
        assert polygon_retrieve is not None, "Failed to retrieve Polygon data"
        assert polygon_retrieve.vendor == "polygon", f"Expected vendor 'polygon', got {polygon_retrieve.vendor}"
        
        logger.info("Testing Tiingo DAO operations...")
        tiingo_insert = await tiingo_dao.insert_fundamental(tiingo_test_data)
        logger.info(f"Tiingo insert result: {tiingo_insert}")
        
        tiingo_retrieve = await tiingo_dao.get_fundamental(test_symbol, test_date)
        logger.info(f"Tiingo retrieve success: {tiingo_retrieve is not None}")
        assert tiingo_retrieve is not None, "Failed to retrieve Tiingo data"
        assert tiingo_retrieve.vendor == "tiingo", f"Expected vendor 'tiingo', got {tiingo_retrieve.vendor}"
        
        # Test UnifiedFundamentalProvider
        logger.info("Testing UnifiedFundamentalProvider...")
        unified_result = await provider.get_unified_fundamental(test_symbol, test_date)
        logger.info(f"Unified result success: {unified_result is not None}")
        
        if unified_result:
            logger.info(f"Unified result vendor count: {len(unified_result.vendor_data)}")
            logger.info(f"Unified result status: {unified_result.status}")
            logger.info(f"Unified result confidence: {unified_result.confidence_score}")
            
            vendors = {vd.vendor for vd in unified_result.vendor_data}
            logger.info(f"Vendors in unified result: {vendors}")
            
            assert len(unified_result.vendor_data) == 3, f"Expected 3 vendors, got {len(unified_result.vendor_data)}"
            assert vendors == {"fmp", "polygon", "tiingo"}, f"Expected all vendors, got {vendors}"
        
        # Test symbol listing
        logger.info("Testing symbol listing...")
        symbols = await provider.list_symbols_with_data()
        logger.info(f"Symbols list length: {len(symbols) if symbols else 0}")
        assert test_symbol in symbols, f"Test symbol {test_symbol} not found in symbols list"
        
        # Cleanup test data
        logger.info("Cleaning up test data...")
        await fmp_dao.delete_fundamental(test_symbol, test_date)
        await polygon_dao.delete_fundamental(test_symbol, test_date)
        await tiingo_dao.delete_fundamental(test_symbol, test_date)
        
        logger.info("✅ All integration tests passed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Integration test failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)