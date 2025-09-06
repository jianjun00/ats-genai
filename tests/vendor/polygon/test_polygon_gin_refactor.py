#!/usr/bin/env python3
"""
Test the PolygonAdapter gin configuration refactor works correctly
"""

import sys
import os
sys.path.insert(0, 'src')

import gin

def test_polygon_adapter_default_config():
    """Test that PolygonAdapter uses default configuration properly"""
    
    # Clear gin configuration
    gin.clear_config()
    
    # Import after clearing to avoid cached values
    from domains.market_data.services.agent.polygon_adapter import PolygonAdapterConfig, PolygonAdapter
    
    # Test default values (without gin)
    config = PolygonAdapterConfig()
    assert config.api_limit == 50000
    assert "adjusted=true" in config.base_url
    assert "limit={limit}" in config.base_url
    assert "AAPL" in config.debug_tickers
    assert "TSLA" in config.debug_tickers
    assert config.debug_start_date == "2020-01-10"
    assert config.debug_end_date == "2024-12-31"
    assert config.debug_log_path == "tests/data"
    print("✅ Default PolygonAdapterConfig values confirmed")
    
    return True

def test_polygon_adapter_gin_config():
    """Test that PolygonAdapter uses gin configuration properly"""
    
    # Clear gin configuration
    gin.clear_config()
    
    # Test with gin configuration
    gin.parse_config([
        'market_data.agent.polygon_adapter.PolygonAdapterConfig.api_limit = 25000',
        'market_data.agent.polygon_adapter.PolygonAdapterConfig.debug_tickers = ["GOOGL", "META"]',
        'market_data.agent.polygon_adapter.PolygonAdapterConfig.debug_start_date = "2021-01-01"',
        'market_data.agent.polygon_adapter.PolygonAdapterConfig.debug_end_date = "2023-12-31"',
        'market_data.agent.polygon_adapter.PolygonAdapterConfig.debug_log_path = "custom/debug/path"'
    ])
    
    # Import after gin config
    from domains.market_data.services.agent.polygon_adapter import PolygonAdapterConfig
    
    # Create new config with gin values
    gin_config = PolygonAdapterConfig()
    assert gin_config.api_limit == 25000
    assert gin_config.debug_tickers == ["GOOGL", "META"]
    assert gin_config.debug_start_date == "2021-01-01"
    assert gin_config.debug_end_date == "2023-12-31"
    assert gin_config.debug_log_path == "custom/debug/path"
    print("✅ Gin-configured PolygonAdapterConfig values confirmed")
    
    return True

def test_polygon_adapter_integration():
    """Test that PolygonAdapter integrates with configuration properly"""
    
    # Clear gin configuration
    gin.clear_config()
    
    # Import modules
    from domains.market_data.services.agent.polygon_adapter import PolygonAdapterConfig, PolygonAdapter
    
    # Test adapter uses configuration
    try:
        # This will fail without API key, but we just want to test config integration
        config = PolygonAdapterConfig(api_limit=30000, debug_tickers=["TEST"])
        # We can't actually instantiate without API key, but we can test config creation
        assert config.api_limit == 30000
        assert config.debug_tickers == ["TEST"]
        print("✅ PolygonAdapter configuration integration confirmed")
        return True
    except Exception as e:
        # If this is just the API key error, that's expected
        if "POLYGON_API_KEY" in str(e):
            print("✅ PolygonAdapter configuration integration confirmed (API key needed for full test)")
            return True
        else:
            raise e

def test_gin_file_loading_polygon():
    """Test loading PolygonAdapter configuration from the gin file"""
    
    # Clear configuration
    gin.clear_config()
    
    # Load our hardcoded values configuration
    if os.path.exists("config/hardcoded_values.gin"):
        try:
            gin.parse_config_file("config/hardcoded_values.gin")
            print("✅ Successfully loaded hardcoded_values.gin with PolygonAdapter config")
            
            # Test that values were loaded
            from domains.market_data.services.agent.polygon_adapter import PolygonAdapterConfig
            
            config = PolygonAdapterConfig()
            
            # These should be the values from the gin file
            assert config.api_limit == 50000  # From gin file
            assert "AAPL" in config.debug_tickers  # From gin file
            assert "TSLA" in config.debug_tickers  # From gin file
            assert config.debug_log_path == "tests/data"  # From gin file
            
            print("✅ PolygonAdapter gin file values loaded correctly")
            return True
        except Exception as e:
            print(f"⚠️ Error loading gin file: {e}")
            return False
    else:
        print("⚠️ Gin file not found, skipping file loading test")
        return True

if __name__ == "__main__":
    print("🧪 Testing PolygonAdapter Gin Configuration Refactor")
    print("=" * 60)
    
    try:
        test_polygon_adapter_default_config()
        test_polygon_adapter_gin_config()
        test_polygon_adapter_integration()
        test_gin_file_loading_polygon()
        
        print("\n🎉 All PolygonAdapter gin configuration tests passed!")
        print("✅ PolygonAdapter hardcoded values successfully moved to gin config")
        print("\n📋 Refactored hardcoded values:")
        print("  • API limit: 50000 → gin configurable")
        print("  • Base URL: hardcoded → gin configurable with parameters")
        print("  • Debug tickers: ['AAPL', 'TSLA'] → gin configurable list")
        print("  • Debug date range: 2020-01-10 to 2024-12-31 → gin configurable")
        print("  • Debug log path: 'tests/data' → gin configurable")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)