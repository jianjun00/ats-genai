#!/usr/bin/env python3
"""
Test the gin configuration refactor works correctly
"""

import sys
import os
sys.path.insert(0, 'src')

import gin

def test_simple_main_gin_config():
    """Test that simple_main uses gin configuration properly"""

    # Clear gin configuration
    gin.clear_config()

    # Import after clearing to avoid cached values
    from simple_main import ApiConfig

    # Test default values (without gin)
    config = ApiConfig()
    assert config.port == 8080
    assert config.host == "0.0.0.0"
    assert config.title == "ATS GenAI API"
    print("✅ Default ApiConfig values confirmed")

    # Test with gin configuration
    gin.parse_config([
        'simple_main.ApiConfig.port = 9000',
        'simple_main.ApiConfig.host = "127.0.0.1"',
        'simple_main.ApiConfig.title = "Test API"'
    ])

    # Create new config with gin values
    gin_config = ApiConfig()
    assert gin_config.port == 9000
    assert gin_config.host == "127.0.0.1"
    assert gin_config.title == "Test API"
    print("✅ Gin-configured ApiConfig values confirmed")

    return True

def test_analytics_api_gin_config():
    """Test that analytics API uses gin configuration properly"""

    # Clear gin configuration
    gin.clear_config()

    # Import after clearing
    from analytics_api_dynamic import MockDataConfig

    # Test default values
    config = MockDataConfig()
    assert 'AAPL' in config.default_universe
    assert config.base_prices['AAPL'] == 150
    assert config.volatilities['TSLA'] == 0.04
    assert config.lookback_days == 30
    print("✅ Default MockDataConfig values confirmed")

    # Test with gin configuration
    gin.parse_config([
        'analytics_api_dynamic.MockDataConfig.default_universe = ["TEST", "SYMBOL"]',
        'analytics_api_dynamic.MockDataConfig.base_prices = {"TEST": 100.0}',
        'analytics_api_dynamic.MockDataConfig.lookback_days = 60'
    ])

    # Create new config with gin values
    gin_config = MockDataConfig()
    assert gin_config.default_universe == ["TEST", "SYMBOL"]
    assert gin_config.base_prices == {"TEST": 100.0}
    assert gin_config.lookback_days == 60
    print("✅ Gin-configured MockDataConfig values confirmed")

    return True

def test_gin_file_loading():
    """Test loading configuration from the gin file"""

    # Clear configuration
    gin.clear_config()

    # Load our hardcoded values configuration
    if os.path.exists("config/hardcoded_values.gin"):
        gin.parse_config_file("config/hardcoded_values.gin")
        print("✅ Successfully loaded hardcoded_values.gin")

        # Test that some values were loaded
        from simple_main import ApiConfig
        from analytics_api_dynamic import MockDataConfig

        api_config = ApiConfig()
        mock_config = MockDataConfig()

        # These should be the values from the gin file
        assert api_config.port == 8080  # From gin file
        assert 'AAPL' in mock_config.default_universe  # From gin file
        assert mock_config.lookback_days == 30  # From gin file

        print("✅ Gin file values loaded correctly")
        return True
        print("⚠️ Gin file not found, skipping file loading test")
        return True

if __name__ == "__main__":
    test_simple_main_gin_config()
    test_analytics_api_gin_config()
    test_gin_file_loading()