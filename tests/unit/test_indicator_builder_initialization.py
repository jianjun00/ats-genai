#!/usr/bin/env python3
"""
Unit Test: IndicatorBuilder Initialization Failure
Test that IndicatorBuilder can be imported and initialized properly.
"""

import pytest
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

class TestIndicatorBuilderInitialization:
    """Test IndicatorBuilder import and initialization."""
    
    def test_indicator_builder_import_success(self):
        """Test that IndicatorBuilder can be imported successfully."""
        try:
            from domains.trading.services.indicators.indicator_builder import IndicatorBuilder
            from domains.trading.services.indicators.indicator_config import IndicatorConfig
            
            # Verify classes are not None
            assert IndicatorBuilder is not None, "IndicatorBuilder should not be None"
            assert IndicatorConfig is not None, "IndicatorConfig should not be None"
            
            print("✅ IndicatorBuilder and IndicatorConfig imported successfully")
            
        except ImportError as e:
            pytest.fail(f"❌ Failed to import IndicatorBuilder: {e}")
    
    def test_indicator_builder_initialization(self):
        """Test that IndicatorBuilder can be initialized."""
        try:
            from domains.trading.services.indicators.indicator_builder import IndicatorBuilder
            from domains.trading.services.indicators.indicator_config import IndicatorConfig
            
            # Try to create an instance
            config = IndicatorConfig()
            builder = IndicatorBuilder(config)
            
            assert builder is not None, "IndicatorBuilder instance should not be None"
            assert hasattr(builder, 'build_indicator_intervals'), "IndicatorBuilder should have build_indicator_intervals method"
            
            print("✅ IndicatorBuilder initialized successfully")
            
        except Exception as e:
            pytest.fail(f"❌ Failed to initialize IndicatorBuilder: {e}")
    
    def test_universe_state_builder_imports_work(self):
        """Test that UniverseStateBuilder can import IndicatorBuilder classes without None fallback."""
        try:
            # Import the module - this should trigger the IndicatorBuilder imports
            from domains.trading.services.state import universe_state_builder
            
            # Check that the imports worked (classes should not be None)
            assert universe_state_builder.IndicatorBuilder is not None, "IndicatorBuilder should not be None after import fix"
            assert universe_state_builder.IndicatorConfig is not None, "IndicatorConfig should not be None after import fix"
            
            print("✅ UniverseStateBuilder successfully imports IndicatorBuilder and IndicatorConfig")
            
        except Exception as e:
            pytest.fail(f"❌ UniverseStateBuilder import issue: {e}")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])