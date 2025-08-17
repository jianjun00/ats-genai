#!/usr/bin/env python3
"""
Test script for core infrastructure components.

This script validates that the core infrastructure is working correctly
and demonstrates the usage patterns.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import pandas as pd
from datetime import datetime, date
import numpy as np

def test_settings():
    """Test centralized settings."""
    print("🔧 Testing Settings...")
    
    try:
        from core.config.settings import get_settings
        
        settings = get_settings()
        print(f"  ✅ Settings loaded: {settings.environment}")
        print(f"  ✅ Table prefix: {settings.table_prefix}")
        print(f"  ✅ Database URL configured: {bool(settings.database_url)}")
        
        # Test table naming
        table_name = settings.get_table_name("daily_prices")
        print(f"  ✅ Table naming works: {table_name}")
        
        # Test validation
        errors = settings.validate_required_settings()
        if errors:
            print(f"  ⚠️  Configuration warnings: {errors}")
        else:
            print("  ✅ Configuration validation passed")
            
    except Exception as e:
        print(f"  ❌ Settings test failed: {e}")
        return False
    
    return True


def test_logging():
    """Test structured logging."""
    print("\n📊 Testing Logging...")
    
    try:
        from core.logging.logger_config import setup_logging, get_logger
        
        # Setup logging
        setup_logging()
        logger = get_logger(__name__)
        
        # Test basic logging
        logger.logger.info("Test log message", extra={"test_field": "test_value"})
        print("  ✅ Basic logging works")
        
        # Test timing
        with logger.timer("test_operation"):
            import time
            time.sleep(0.1)  # Simulate work
        print("  ✅ Timing logger works")
        
    except Exception as e:
        print(f"  ❌ Logging test failed: {e}")
        return False
    
    return True


def test_exceptions():
    """Test custom exceptions."""
    print("\n🚨 Testing Exceptions...")
    
    try:
        from core.exceptions.custom_exceptions import (
            DatabaseError, create_error_context, handle_database_error
        )
        
        # Test error context creation
        context = create_error_context(
            operation="test_operation",
            component="test_component",
            test_data="test_value"
        )
        print("  ✅ Error context creation works")
        
        # Test custom exception
        try:
            raise DatabaseError("Test database error", context=context)
        except DatabaseError as e:
            error_dict = e.to_dict()
            assert "error_type" in error_dict
            assert "context" in error_dict
            print("  ✅ Custom exception handling works")
        
    except Exception as e:
        print(f"  ❌ Exceptions test failed: {e}")
        return False
    
    return True


def test_validation():
    """Test data validation framework."""
    print("\n✅ Testing Validation...")
    
    try:
        from core.validation.data_validators import (
            FieldValidator, MarketDataValidator, create_price_validator
        )
        
        # Test field validator
        price_validator = create_price_validator()
        result = price_validator.validate(185.50)
        assert result.is_valid
        print("  ✅ Field validation works")
        
        # Test invalid price
        result = price_validator.validate(-10.0)
        assert not result.is_valid
        print("  ✅ Invalid data detection works")
        
        # Test market data validator with sample data
        sample_data = pd.DataFrame({
            "symbol": ["AAPL", "MSFT"],
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "open": [180.0, 290.0],
            "high": [185.0, 295.0],
            "low": [178.0, 288.0],
            "close": [183.0, 292.0],
            "volume": [50000000, 30000000]
        })
        
        validator = MarketDataValidator()
        result = validator.validate(sample_data)
        print(f"  ✅ Market data validation: {result.is_valid}")
        
    except Exception as e:
        print(f"  ❌ Validation test failed: {e}")
        return False
    
    return True


def test_datetime_utils():
    """Test datetime utilities."""
    print("\n🕐 Testing DateTime Utils...")
    
    try:
        from core.utils.datetime_utils import (
            get_current_market_time, is_market_hours, get_trading_session,
            generate_business_days, format_datetime_for_api
        )
        
        # Test market time
        market_time = get_current_market_time()
        print(f"  ✅ Market time: {market_time}")
        
        # Test market hours
        session = get_trading_session()
        print(f"  ✅ Trading session: {session}")
        
        # Test business days
        business_days = generate_business_days(
            date(2024, 1, 1), date(2024, 1, 7)
        )
        print(f"  ✅ Business days generated: {len(business_days)} days")
        
        # Test API formatting
        api_date = format_datetime_for_api(datetime.now(), "polygon")
        print(f"  ✅ API date formatting: {api_date}")
        
    except Exception as e:
        print(f"  ❌ DateTime utils test failed: {e}")
        return False
    
    return True


def test_data_utils():
    """Test data utilities."""
    print("\n📈 Testing Data Utils...")
    
    try:
        from core.utils.data_utils import (
            normalize_symbol, standardize_price_data, calculate_returns,
            clean_numeric_data
        )
        
        # Test symbol normalization
        symbol = normalize_symbol("  aapl  ")
        assert symbol == "AAPL"
        print("  ✅ Symbol normalization works")
        
        # Test price data standardization
        raw_data = pd.DataFrame({
            "ticker": ["AAPL", "MSFT"],
            "dt": [date(2024, 1, 1), date(2024, 1, 2)],
            "o": [180.0, 290.0],
            "h": [185.0, 295.0],
            "l": [178.0, 288.0],
            "c": [183.0, 292.0],
            "v": [50000000, 30000000]
        })
        
        standardized = standardize_price_data(raw_data)
        expected_columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
        assert all(col in standardized.columns for col in expected_columns)
        print("  ✅ Price data standardization works")
        
        # Test returns calculation
        prices = pd.Series([100, 105, 102, 108])
        returns = calculate_returns(prices)
        assert len(returns) == len(prices)
        print("  ✅ Returns calculation works")
        
        # Test data cleaning
        messy_data = pd.Series([1, 2, np.nan, 4, 100])  # 100 is outlier
        cleaned = clean_numeric_data(messy_data, remove_outliers=True)
        print(f"  ✅ Data cleaning works: {len(cleaned)} values")
        
    except Exception as e:
        print(f"  ❌ Data utils test failed: {e}")
        return False
    
    return True


def main():
    """Run all tests."""
    print("🧪 Testing Core Infrastructure Components\n")
    
    tests = [
        test_settings,
        test_logging,
        test_exceptions,
        test_validation,
        test_datetime_utils,
        test_data_utils
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All core infrastructure tests passed!")
        print("\n✅ Phase 1: Core Infrastructure - COMPLETED")
        return True
    else:
        print("❌ Some tests failed. Please check the implementation.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)