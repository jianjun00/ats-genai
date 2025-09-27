#!/usr/bin/env python3
"""
Simple standalone test for our hardcoded values without complex gin dependencies
"""

import sys
sys.path.insert(0, 'src')

def test_simple_main_gin_standalone():
    """Test simple_main gin configuration without complex dependencies"""
    import gin
    gin.clear_config()

    # Test that our ApiConfig class can be imported and instantiated
    from simple_main import ApiConfig

    # Test default values
    config = ApiConfig()
    assert config.port == 8080
    assert config.host == "0.0.0.0"
    assert config.title == "ATS GenAI API"
    print("✅ simple_main.ApiConfig default values work")

    # Test gin configuration
    gin.parse_config([
        'simple_main.ApiConfig.port = 9000',
        'simple_main.ApiConfig.host = "127.0.0.1"',
        'simple_main.ApiConfig.title = "Test API"'
    ])

    gin_config = ApiConfig()
    assert gin_config.port == 9000
    assert gin_config.host == "127.0.0.1"
    assert gin_config.title == "Test API"
    print("✅ simple_main.ApiConfig gin configuration works")
    return True

def test_analytics_api_gin_standalone():
    """Test analytics_api_dynamic gin configuration without complex dependencies"""
    import gin
    gin.clear_config()

    # Test that our MockDataConfig class can be imported and instantiated
    from analytics_api_dynamic import MockDataConfig

    # Test default values
    config = MockDataConfig()
    assert 'AAPL' in config.default_universe
    assert config.base_prices['AAPL'] == 150
    assert config.volatilities['TSLA'] == 0.04
    assert config.lookback_days == 30
    print("✅ analytics_api_dynamic.MockDataConfig default values work")

    # Test gin configuration
    gin.parse_config([
        'analytics_api_dynamic.MockDataConfig.default_universe = ["TEST", "SYMBOL"]',
        'analytics_api_dynamic.MockDataConfig.base_prices = {"TEST": 100.0}',
        'analytics_api_dynamic.MockDataConfig.lookback_days = 60'
    ])

    gin_config = MockDataConfig()
    assert gin_config.default_universe == ["TEST", "SYMBOL"]
    assert gin_config.base_prices == {"TEST": 100.0}
    assert gin_config.lookback_days == 60
    print("✅ analytics_api_dynamic.MockDataConfig gin configuration works")
    return True

def test_polygon_adapter_gin_standalone():
    """Test polygon_adapter gin configuration without complex dependencies"""
    import gin
    gin.clear_config()

    # Test that our PolygonAdapterConfig class can be imported and instantiated
    from domains.market_data.services.vendor_adapters.market_cap.unified_market_cap_provider import PolygonAdapterConfig

    # Test default values
    config = PolygonAdapterConfig()
    assert config.api_limit == 50000
    assert "AAPL" in config.debug_tickers
    assert "TSLA" in config.debug_tickers
    assert config.debug_start_date == "2020-01-10"
    assert config.debug_end_date == "2024-12-31"
    assert config.debug_log_path == "tests/data"
    print("✅ polygon_adapter.PolygonAdapterConfig default values work")

    # Test gin configuration
    gin.parse_config([
        'market_data.agent.polygon_adapter.PolygonAdapterConfig.api_limit = 25000',
        'market_data.agent.polygon_adapter.PolygonAdapterConfig.debug_tickers = ["GOOGL", "META"]',
        'market_data.agent.polygon_adapter.PolygonAdapterConfig.debug_start_date = "2021-01-01"',
        'market_data.agent.polygon_adapter.PolygonAdapterConfig.debug_log_path = "custom/debug/path"'
    ])

    gin_config = PolygonAdapterConfig()
    assert gin_config.api_limit == 25000
    assert gin_config.debug_tickers == ["GOOGL", "META"]
    assert gin_config.debug_start_date == "2021-01-01"
    assert gin_config.debug_log_path == "custom/debug/path"
    print("✅ polygon_adapter.PolygonAdapterConfig gin configuration works")
    return True

if __name__ == "__main__":
    print("🧪 Testing Standalone Gin Refactoring (Without Complex Dependencies)")
    print("=" * 75)

    test_simple_main_gin_standalone()
    test_analytics_api_gin_standalone()
    test_polygon_adapter_gin_standalone()

    print("\n🎉 All standalone gin configuration tests passed!")
    print("✅ Hardcoded values refactoring is working correctly!")
    print("\n📋 Successfully tested:")
    print("  • simple_main.ApiConfig - gin configurable API settings")
    print("  • analytics_api_dynamic.MockDataConfig - gin configurable mock data")
    print("  • polygon_adapter.PolygonAdapterConfig - gin configurable adapter settings")
    print("\n🚀 Our refactoring changes work independently of other system issues")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)