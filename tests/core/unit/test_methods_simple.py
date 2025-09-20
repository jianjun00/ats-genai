#!/usr/bin/env python3
"""
Simple test to verify the missing methods are now implemented in UniverseStateManager.
"""

import sys
import os
sys.path.insert(0, 'src')

def test_methods_exist():
    """Test that all required methods exist in UniverseStateManager."""
    from domains.trading.services.state.universe_state_manager import UniverseStateManager
    
    # Create a basic instance (doesn't need to be fully functional for method existence check)
    manager = UniverseStateManager()
    
    missing_methods = []
    
    # Check get_lag_prices
    if not hasattr(manager, 'get_lag_prices'):
        missing_methods.append('get_lag_prices')
    
    # Check get_lead_prices  
    if not hasattr(manager, 'get_lead_prices'):
        missing_methods.append('get_lead_prices')
        
    # Check get_lagged_signals
    if not hasattr(manager, 'get_lagged_signals'):
        missing_methods.append('get_lagged_signals')
    
    if missing_methods:
        print(f"❌ Missing methods: {missing_methods}")
        return False
    else:
        print("✅ All required methods exist in UniverseStateManager")
        
        # Test that they're callable
        assert callable(manager.get_lag_prices), "get_lag_prices is not callable"
        assert callable(manager.get_lead_prices), "get_lead_prices is not callable" 
        assert callable(manager.get_lagged_signals), "get_lagged_signals is not callable"
        
        print("✅ All methods are callable")
        return True

if __name__ == "__main__":
    success = test_methods_exist()
    sys.exit(0 if success else 1)