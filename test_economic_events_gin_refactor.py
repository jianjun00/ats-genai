#!/usr/bin/env python3
"""
Test the refactored economic events clients with gin configuration
"""

import sys
import os
sys.path.insert(0, 'src')

def test_fred_client_gin_refactor():
    """Test FRED client gin configuration refactoring"""
    import gin
    gin.clear_config()
    
    from economic_events.fred_client import FREDEconomicConfig, FREDEconomicClient
    
    # Test default configuration
    config = FREDEconomicConfig()
    assert config.base_url == "https://api.stlouisfed.org/fred"
    assert config.timeout_seconds == 30
    assert config.search_limit_default == 20
    assert config.observations_limit == 100000
    
    client = FREDEconomicClient("test_key")
    assert client.config.base_url == "https://api.stlouisfed.org/fred"
    print("✅ FRED client default configuration works")
    
    # Test gin configuration
    gin.parse_config([
        'economic_events.fred_client.FREDEconomicConfig.base_url = "https://custom.fred.api"',
        'economic_events.fred_client.FREDEconomicConfig.timeout_seconds = 60',
        'economic_events.fred_client.FREDEconomicConfig.search_limit_default = 50'
    ])
    
    gin_config = FREDEconomicConfig()
    assert gin_config.base_url == "https://custom.fred.api"
    assert gin_config.timeout_seconds == 60
    assert gin_config.search_limit_default == 50
    
    gin_client = FREDEconomicClient("test_key")
    assert gin_client.config.base_url == "https://custom.fred.api"
    print("✅ FRED client gin configuration works")
    return True

def test_polygon_client_gin_refactor():
    """Test Polygon client gin configuration refactoring"""
    import gin
    gin.clear_config()
    
    from economic_events.polygon_client import PolygonEconomicEventsConfig, PolygonEconomicEventsClient
    
    # Test default configuration
    config = PolygonEconomicEventsConfig()
    assert config.base_url == "https://api.polygon.io"
    assert config.timeout_seconds == 30
    assert config.api_limit == 1000
    assert config.rate_limit_sleep_seconds == 12
    
    client = PolygonEconomicEventsClient("test_key")
    assert client.config.base_url == "https://api.polygon.io"
    print("✅ Polygon client default configuration works")
    
    # Test gin configuration
    gin.parse_config([
        'economic_events.polygon_client.PolygonEconomicEventsConfig.base_url = "https://custom.polygon.api"',
        'economic_events.polygon_client.PolygonEconomicEventsConfig.timeout_seconds = 45',
        'economic_events.polygon_client.PolygonEconomicEventsConfig.api_limit = 2000',
        'economic_events.polygon_client.PolygonEconomicEventsConfig.rate_limit_sleep_seconds = 20'
    ])
    
    gin_config = PolygonEconomicEventsConfig()
    assert gin_config.base_url == "https://custom.polygon.api"
    assert gin_config.timeout_seconds == 45
    assert gin_config.api_limit == 2000
    assert gin_config.rate_limit_sleep_seconds == 20
    
    gin_client = PolygonEconomicEventsClient("test_key")
    assert gin_client.config.base_url == "https://custom.polygon.api"
    print("✅ Polygon client gin configuration works")
    return True

def test_alpha_vantage_client_gin_refactor():
    """Test Alpha Vantage client gin configuration refactoring"""
    import gin
    gin.clear_config()
    
    from economic_events.alpha_vantage_client import AlphaVantageEconomicConfig, AlphaVantageEconomicClient
    
    # Test default configuration
    config = AlphaVantageEconomicConfig()
    assert config.base_url == "https://www.alphavantage.co/query"
    assert config.timeout_seconds == 30
    assert config.rate_limit_delay_seconds == 15
    
    client = AlphaVantageEconomicClient("test_key")
    assert client.config.base_url == "https://www.alphavantage.co/query"
    print("✅ Alpha Vantage client default configuration works")
    
    # Test gin configuration
    gin.parse_config([
        'economic_events.alpha_vantage_client.AlphaVantageEconomicConfig.base_url = "https://custom.alphavantage.api"',
        'economic_events.alpha_vantage_client.AlphaVantageEconomicConfig.timeout_seconds = 45',
        'economic_events.alpha_vantage_client.AlphaVantageEconomicConfig.rate_limit_delay_seconds = 20'
    ])
    
    gin_config = AlphaVantageEconomicConfig()
    assert gin_config.base_url == "https://custom.alphavantage.api"
    assert gin_config.timeout_seconds == 45
    assert gin_config.rate_limit_delay_seconds == 20
    
    gin_client = AlphaVantageEconomicClient("test_key")
    assert gin_client.config.base_url == "https://custom.alphavantage.api"
    print("✅ Alpha Vantage client gin configuration works")
    return True

def test_tiingo_client_gin_refactor():
    """Test Tiingo client gin configuration refactoring"""
    import gin
    gin.clear_config()
    
    from economic_events.tiingo_client import TiingoEconomicConfig, TiingoEconomicEventsClient
    
    # Test default configuration
    config = TiingoEconomicConfig()
    assert config.base_url == "https://api.tiingo.com"
    assert config.timeout_seconds == 30
    assert config.news_limit == 1000
    assert config.crypto_news_limit == 500
    assert config.rate_limit_delay_seconds == 5
    
    client = TiingoEconomicEventsClient("test_key")
    assert client.config.base_url == "https://api.tiingo.com"
    print("✅ Tiingo client default configuration works")
    
    # Test gin configuration
    gin.parse_config([
        'economic_events.tiingo_client.TiingoEconomicConfig.base_url = "https://custom.tiingo.api"',
        'economic_events.tiingo_client.TiingoEconomicConfig.timeout_seconds = 45',
        'economic_events.tiingo_client.TiingoEconomicConfig.news_limit = 2000',
        'economic_events.tiingo_client.TiingoEconomicConfig.crypto_news_limit = 800',
        'economic_events.tiingo_client.TiingoEconomicConfig.rate_limit_delay_seconds = 10'
    ])
    
    gin_config = TiingoEconomicConfig()
    assert gin_config.base_url == "https://custom.tiingo.api"
    assert gin_config.timeout_seconds == 45
    assert gin_config.news_limit == 2000
    assert gin_config.crypto_news_limit == 800
    assert gin_config.rate_limit_delay_seconds == 10
    
    gin_client = TiingoEconomicEventsClient("test_key")
    assert gin_client.config.base_url == "https://custom.tiingo.api"
    print("✅ Tiingo client gin configuration works")
    return True

if __name__ == "__main__":
    print("🧪 Testing Economic Events Clients Gin Configuration Refactoring")
    print("=" * 75)
    
    try:
        test_fred_client_gin_refactor()
        test_polygon_client_gin_refactor()
        test_alpha_vantage_client_gin_refactor()
        test_tiingo_client_gin_refactor()
        
        print("\n🎉 All economic events clients gin configuration tests passed!")
        print("✅ Hardcoded values successfully moved to gin configuration!")
        print("\n📋 Successfully refactored:")
        print("  • FRED Economic Client - base_url, timeout_seconds, search_limit_default, observations_limit")
        print("  • Polygon Economic Events Client - base_url, timeout_seconds, api_limit, rate_limit_sleep_seconds")
        print("  • Alpha Vantage Economic Client - base_url, timeout_seconds, rate_limit_delay_seconds")
        print("  • Tiingo Economic Events Client - base_url, timeout_seconds, news_limit, crypto_news_limit, rate_limit_delay_seconds")
        print("\n🚀 Economic events clients refactoring is complete and working!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)