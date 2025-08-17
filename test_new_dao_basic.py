#!/usr/bin/env python3
"""
Basic test to verify new DAO structure works correctly without complex database setup.
"""

import os
import sys
sys.path.append('src')

def test_dao_imports():
    """Test that all new DAO imports work correctly."""
    try:
        from dao.vendors.polygon_dao import PolygonDAO
        from dao.vendors.tiingo_dao import TiingoDAO
        from dao.market_data.daily_prices_dao import DailyPricesDAO
        from dao.corporate_actions.dividends_dao import DividendsDAO
        from dao.corporate_actions.stock_splits_dao import StockSplitsDAO
        from dao.instruments.instruments_dao import InstrumentsDAO
        print("✅ All DAO imports successful")
        return True
    except Exception as e:
        print(f"❌ DAO import failed: {e}")
        return False

def test_dao_instantiation():
    """Test that all DAOs can be instantiated."""
    try:
        from dao.vendors.polygon_dao import PolygonDAO
        from dao.vendors.tiingo_dao import TiingoDAO
        from dao.market_data.daily_prices_dao import DailyPricesDAO
        from dao.corporate_actions.dividends_dao import DividendsDAO
        from dao.corporate_actions.stock_splits_dao import StockSplitsDAO
        from dao.instruments.instruments_dao import InstrumentsDAO
        
        # Instantiate all DAOs
        polygon_dao = PolygonDAO()
        tiingo_dao = TiingoDAO()
        daily_dao = DailyPricesDAO()
        dividends_dao = DividendsDAO()
        splits_dao = StockSplitsDAO()
        instruments_dao = InstrumentsDAO()
        
        print("✅ All DAO instantiation successful")
        
        # Test basic methods exist
        assert hasattr(polygon_dao, 'insert_daily_price')
        assert hasattr(polygon_dao, 'insert_dividend')
        assert hasattr(polygon_dao, 'insert_stock_split')
        assert hasattr(tiingo_dao, 'insert_daily_price')
        assert hasattr(daily_dao, 'get_price_by_symbol_date')
        assert hasattr(dividends_dao, 'get_dividends_by_symbol')
        assert hasattr(splits_dao, 'get_splits_by_symbol')
        assert hasattr(instruments_dao, 'get_by_symbol')
        
        print("✅ All required methods exist")
        return True
    except Exception as e:
        print(f"❌ DAO instantiation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_dao_schemas():
    """Test that DAO schemas are properly defined."""
    try:
        from dao.vendors.polygon_dao import PolygonDAO
        from dao.market_data.daily_prices_dao import DailyPricesDAO
        
        polygon_dao = PolygonDAO()
        daily_dao = DailyPricesDAO()
        
        # Test schema methods
        polygon_schema = polygon_dao.get_schema()
        daily_schema = daily_dao.get_schema()
        
        assert isinstance(polygon_schema, dict)
        assert isinstance(daily_schema, dict)
        assert 'id' in polygon_schema
        assert 'id' in daily_schema
        
        print("✅ DAO schemas properly defined")
        return True
    except Exception as e:
        print(f"❌ DAO schema test failed: {e}")
        return False

def test_vendor_configs():
    """Test that vendor configurations work."""
    try:
        from dao.vendors.polygon_dao import PolygonDAO
        from dao.vendors.tiingo_dao import TiingoDAO
        
        polygon_dao = PolygonDAO()
        tiingo_dao = TiingoDAO()
        
        # Test vendor configs
        polygon_config = polygon_dao.get_vendor_config()
        tiingo_config = tiingo_dao.get_vendor_config()
        
        assert isinstance(polygon_config, dict)
        assert isinstance(tiingo_config, dict)
        assert 'api_base_url' in polygon_config
        assert 'api_base_url' in tiingo_config
        
        print("✅ Vendor configurations work")
        return True
    except Exception as e:
        print(f"❌ Vendor config test failed: {e}")
        return False

def test_data_validation():
    """Test data validation functionality."""
    try:
        from dao.vendors.polygon_dao import PolygonDAO
        
        # Setup basic logging to avoid logger issues
        import logging
        logging.basicConfig(level=logging.INFO)
        
        polygon_dao = PolygonDAO()
        
        # Test valid price data
        valid_data = {
            'symbol': 'AAPL',
            'date': '2024-01-01',
            'open': 100.0,
            'high': 105.0,
            'low': 99.0,
            'close': 104.0,
            'volume': 1000000
        }
        
        is_valid = polygon_dao.validate_price_data(valid_data)
        assert is_valid, "Valid price data should pass validation"
        
        # Test invalid price data (high < low)
        invalid_data = {
            'symbol': 'AAPL',
            'date': '2024-01-01',
            'open': 100.0,
            'high': 98.0,  # Invalid: high < low
            'low': 99.0,
            'close': 104.0,
            'volume': 1000000
        }
        
        is_invalid = polygon_dao.validate_price_data(invalid_data)
        assert not is_invalid, "Invalid price data should fail validation"
        
        print("✅ Data validation works correctly")
        return True
    except Exception as e:
        print(f"❌ Data validation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("🧪 Testing New DAO Structure")
    print("=" * 50)
    
    tests = [
        test_dao_imports,
        test_dao_instantiation,
        test_dao_schemas,
        test_vendor_configs,
        test_data_validation
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        print(f"\n🔧 Running {test.__name__}...")
        if test():
            passed += 1
        else:
            print(f"❌ Test {test.__name__} failed")
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All new DAO structure tests passed!")
        return True
    else:
        print("❌ Some tests failed")
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)