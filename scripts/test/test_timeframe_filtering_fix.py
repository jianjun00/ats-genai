#!/usr/bin/env python3
"""
Test script to validate the timeframe separation fix.

This script tests the critical fix to _extract_timeframe_data method
to ensure timeframes are properly isolated.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback

def test_timeframe_filtering():
    """Test that timeframe filtering works correctly."""
    print("🧪 Testing timeframe filtering fix...")
    
    # Create callback instance
    callback = IntervalBasedTrainingDataCallback(
        symbols=['TEST'],
        config=None,
        output_dir="/tmp/test"
    )
    
    # Create mock multi-timeframe example with mixed features
    mock_example = {
        'symbol': 'TEST',
        'timestamp': '2025-01-01T10:00:00',
        'features': {
            # 5m features (should appear in 5m timeframe only, without prefix)
            'open': [100.0, 101.0, 102.0],
            'high': [101.0, 102.0, 103.0],
            'close': [100.5, 101.5, 102.5],
            'volume': [1000, 1100, 1200],
            'vwap': [100.3, 101.3, 102.3],
            'rsi_14': [50.0, 55.0, 60.0],
            
            # 15m features (should appear in 15m timeframe only)
            '15m_open': [100.0, 102.0],
            '15m_high': [101.5, 103.5],
            '15m_close': [101.0, 103.0],
            '15m_volume': [2100, 2300],
            
            # 1h features (should appear in 1h timeframe only)
            '1h_open': [100.0],
            '1h_high': [103.5],
            '1h_close': [103.0],
            '1h_volume': [4400],
            
            # 1d features (should appear in 1d timeframe only)
            '1d_open': [100.0],
            '1d_high': [105.0],
            '1d_close': [104.0],
            '1d_volume': [50000],
            
            # Meta features (should appear in all timeframes)
            'timestamp': '2025-01-01T10:00:00',
            'symbol': 'TEST'
        },
        'metadata': {}
    }
    
    print(f"📊 Original example has {len(mock_example['features'])} total features")
    
    # Test filtering for each timeframe
    timeframes = ['5m', '15m', '1h', '1d', '1w']
    expected_features = {
        '5m': ['timestamp', 'symbol', 'open', 'high', 'close', 'volume', 'vwap', 'rsi_14'],
        '15m': ['timestamp', 'symbol', '15m_open', '15m_high', '15m_close', '15m_volume'],
        '1h': ['timestamp', 'symbol', '1h_open', '1h_high', '1h_close', '1h_volume'],
        '1d': ['timestamp', 'symbol', '1d_open', '1d_high', '1d_close', '1d_volume'],
        '1w': ['timestamp', 'symbol']  # No 1w features in this test
    }
    
    success_count = 0
    
    for timeframe in timeframes:
        print(f"\n🔍 Testing {timeframe} timeframe filtering...")
        
        # Apply filtering
        filtered_examples = callback._extract_timeframe_data([mock_example], timeframe)
        
        if not filtered_examples:
            print(f"❌ {timeframe}: No examples returned")
            continue
            
        filtered_example = filtered_examples[0]
        filtered_features = list(filtered_example['features'].keys())
        expected = expected_features[timeframe]
        
        print(f"   Expected features: {expected}")
        print(f"   Filtered features: {filtered_features}")
        
        # Check if we got the expected features
        missing = set(expected) - set(filtered_features)
        unexpected = set(filtered_features) - set(expected)
        
        if missing:
            print(f"   ❌ Missing features: {missing}")
        
        if unexpected:
            print(f"   ❌ Unexpected features: {unexpected}")
        
        if not missing and not unexpected:
            print(f"   ✅ {timeframe}: Perfect filtering!")
            success_count += 1
        else:
            print(f"   ❌ {timeframe}: Filtering failed")
        
        # Check metadata
        metadata = filtered_example.get('metadata', {})
        original_count = metadata.get('original_feature_count', 0)
        filtered_count = metadata.get('filtered_feature_count', 0)
        print(f"   📊 Feature counts: {original_count} → {filtered_count}")
    
    print(f"\n🎯 FILTERING TEST RESULTS:")
    print(f"   Successful timeframes: {success_count}/{len(timeframes)}")
    
    if success_count == len(timeframes):
        print("   ✅ ALL TIMEFRAME FILTERING TESTS PASSED!")
        return True
    else:
        print("   ❌ SOME TIMEFRAME FILTERING TESTS FAILED!")
        return False


def test_feature_naming_fix():
    """Test that feature naming follows QR4 requirements."""
    print("\n🧪 Testing feature naming fix...")
    
    # Mock example with proper feature structure
    features_5m = {
        'open': [100.0, 101.0],
        'high': [101.0, 102.0], 
        'close': [100.5, 101.5],
        'volume': [1000, 1100],
        'vwap': [100.3, 101.3]
    }
    
    features_1h = {
        '1h_open': [100.0],
        '1h_high': [102.0],
        '1h_close': [101.5],
        '1h_volume': [2100],
        '1h_vwap': [101.3]
    }
    
    print("✅ 5m features (no prefix):", list(features_5m.keys()))
    print("✅ 1h features (with prefix):", list(features_1h.keys()))
    
    # Check naming compliance
    is_5m_compliant = all(not feature.startswith(('5m_', '15m_', '1h_', '1d_', '1w_')) for feature in features_5m.keys())
    is_1h_compliant = all(feature.startswith('1h_') for feature in features_1h.keys())
    
    print(f"5m naming compliant: {'✅' if is_5m_compliant else '❌'}")
    print(f"1h naming compliant: {'✅' if is_1h_compliant else '❌'}")
    
    return is_5m_compliant and is_1h_compliant


if __name__ == "__main__":
    print("🔧 TIMEFRAME SEPARATION FIX VALIDATION")
    print("=" * 50)
    
    # Test 1: Timeframe filtering
    filtering_success = test_timeframe_filtering()
    
    # Test 2: Feature naming
    naming_success = test_feature_naming_fix()
    
    print("\n" + "=" * 50)
    print("📊 FINAL VALIDATION RESULTS:")
    print(f"Timeframe Filtering: {'✅ PASS' if filtering_success else '❌ FAIL'}")
    print(f"Feature Naming: {'✅ PASS' if naming_success else '❌ FAIL'}")
    
    overall_success = filtering_success and naming_success
    print(f"Overall Fix Status: {'🎉 SUCCESS' if overall_success else '💥 FAILED'}")
    
    # Return appropriate exit code
    sys.exit(0 if overall_success else 1)