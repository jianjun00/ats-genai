#!/usr/bin/env python3
"""
Test for identifying and fixing training data generation import issues.

This test will systematically identify missing imports and fix them 
to enable AAPL training data generation from 2025-06-01 to 2025-09-13.
"""

import pytest
import sys
import os
from unittest.mock import Mock, patch
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestTrainingDataImportFixes:
    """Test and fix import issues preventing training data generation."""

    def test_identify_missing_imports(self):
        """Identify all missing import issues in the training data system."""
        
        print("🔍 Testing import issues in training data generation system...")
        
        # Test 1: Check if we can import the runner
        import_errors = []
        
        try:
            from domains.trading.services.core.app.runner import Runner
            print("✅ Successfully imported Runner")
        except ImportError as e:
            import_errors.append(f"Runner import error: {e}")
            print(f"❌ Runner import failed: {e}")
        
        # Test 2: Check training data callback runner
        try:
            from domains.ml.services.training_data.runners.training_data_callback_runner import main
            print("✅ Successfully imported training_data_callback_runner main")
        except ImportError as e:
            import_errors.append(f"Training data callback runner import error: {e}")
            print(f"❌ Training data callback runner import failed: {e}")
        
        # Test 3: Check if we can import core market data components
        try:
            from core.market_data.unified_manager import UnifiedMarketDataManager
            print("✅ Successfully imported UnifiedMarketDataManager")
        except ImportError as e:
            import_errors.append(f"UnifiedMarketDataManager import error: {e}")
            print(f"❌ UnifiedMarketDataManager import failed: {e}")
        
        # Test 4: Check domain market data manager
        try:
            from domains.market_data.services.core.market_data_manager import MarketDataManager
            print("✅ Successfully imported MarketDataManager")
        except ImportError as e:
            import_errors.append(f"MarketDataManager import error: {e}")
            print(f"❌ MarketDataManager import failed: {e}")
        
        # Test 5: Check if universe state components work
        try:
            from domains.trading.services.state.universe_state_manager import UniverseStateManager
            print("✅ Successfully imported UniverseStateManager")
        except ImportError as e:
            import_errors.append(f"UniverseStateManager import error: {e}")
            print(f"❌ UniverseStateManager import failed: {e}")
        
        # Test 6: Check training data callback
        try:
            from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
            print("✅ Successfully imported IntervalBasedTrainingDataCallback")
        except ImportError as e:
            import_errors.append(f"IntervalBasedTrainingDataCallback import error: {e}")
            print(f"❌ IntervalBasedTrainingDataCallback import failed: {e}")
        
        # Report all errors found
        if import_errors:
            print(f"\n🚨 Found {len(import_errors)} import errors:")
            for i, error in enumerate(import_errors, 1):
                print(f"{i}. {error}")
        else:
            print("\n🎉 All imports successful!")
        
        return import_errors

    def test_runner_import_compatibility(self):
        """Test if the Runner class can be properly imported and instantiated."""
        
        print("\n🔧 Testing Runner class compatibility...")
        
        # Test basic import paths
        import_paths = [
            "services.core.app.runner",
            "core.market_data.unified_manager", 
            "domains.market_data.services.core.market_data_manager"
        ]
        
        for path in import_paths:
            try:
                __import__(path)
                print(f"✅ {path} imports successfully")
            except ImportError as e:
                print(f"❌ {path} import failed: {e}")
        
        # Test if we can create a mock runner  
        try:
            from domains.trading.services.core.app.runner import Runner
            print("✅ Runner can be imported without mocking needed")
            return True
        except ImportError as e:
            print(f"❌ Runner import failed: {e}")
            return False

    def test_training_data_callback_dependencies(self):
        """Test training data callback and its dependencies."""
        
        print("\n📊 Testing training data callback dependencies...")
        
        # Test the callback import with proper mocking
        try:
            from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
            print("✅ IntervalBasedTrainingDataCallback imports successfully")
            
            # Test if we can create a mock instance
            callback = IntervalBasedTrainingDataCallback(
                symbols=['AAPL'],
                config=None,
                output_dir='/tmp/test',
                storage_format='arrayrecord',
                start_date=datetime(2025, 6, 1).date(),
                end_date=datetime(2025, 9, 13).date(),
                start_day_offset=0,
                end_day_offset=0
            )
            print("✅ IntervalBasedTrainingDataCallback can be instantiated")
            return True
            
        except Exception as e:
            print(f"❌ Training data callback failed: {e}")
            return False

    def test_file_based_minute_manager_availability(self):
        """Test if we have working minute data management."""
        
        print("\n📁 Testing file-based minute data manager availability...")
        
        # Check if we can access minute bar data
        import os
        minute_data_path = "/mnt/d/ats-data/minute-bars/firstrate/A/AAPL"
        
        if os.path.exists(minute_data_path):
            print(f"✅ AAPL minute data directory exists: {minute_data_path}")
            
            # Check for AAPL parquet file
            aapl_file = os.path.join(minute_data_path, "AAPL_complete.parquet")
            if os.path.exists(aapl_file):
                print(f"✅ AAPL parquet file exists: {aapl_file}")
                
                # Try to read a small sample
                try:
                    import pandas as pd
                    df = pd.read_parquet(aapl_file, nrows=10)
                    print(f"✅ Can read AAPL data - sample shape: {df.shape}")
                    print(f"   Columns: {list(df.columns)}")
                    if not df.empty:
                        print(f"   Date range sample: {df.index[0]} to {df.index[-1] if len(df) > 1 else df.index[0]}")
                    return True
                except Exception as e:
                    print(f"❌ Cannot read AAPL parquet file: {e}")
            else:
                print(f"❌ AAPL parquet file not found: {aapl_file}")
        else:
            print(f"❌ AAPL minute data directory not found: {minute_data_path}")
        
        return False

if __name__ == "__main__":
    test_runner = TestTrainingDataImportFixes()
    
    print("🚀 Starting comprehensive import testing for AAPL training data generation...")
    print("=" * 80)
    
    # Run all tests
    import_errors = test_runner.test_identify_missing_imports()
    runner_compatible = test_runner.test_runner_import_compatibility() 
    callback_works = test_runner.test_training_data_callback_dependencies()
    data_available = test_runner.test_file_based_minute_manager_availability()
    
    print("\n" + "=" * 80)
    print("📋 COMPREHENSIVE TEST SUMMARY")
    print("=" * 80)
    print(f"Import errors found: {len(import_errors)}")
    print(f"Runner compatible: {'✅ YES' if runner_compatible else '❌ NO'}")
    print(f"Callback works: {'✅ YES' if callback_works else '❌ NO'}")
    print(f"AAPL data available: {'✅ YES' if data_available else '❌ NO'}")
    
    if import_errors:
        print(f"\n🔧 FIXES NEEDED:")
        for error in import_errors:
            print(f"- {error}")
    else:
        print("\n🎉 All components ready for AAPL training data generation!")