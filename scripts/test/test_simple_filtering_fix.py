#!/usr/bin/env python3
"""
Simple test for timeframe filtering fix without dependencies.

This tests the core filtering logic isolated from the main callback class.
"""

def extract_timeframe_data_fixed(examples, timeframe):
    """
    Fixed version of _extract_timeframe_data method.

    CRITICAL FIX: This method now properly filters features to include ONLY
    the features for the specified timeframe, as required by QR4.
    """
    timeframe_examples = []

    for example in examples:
        all_features = example.get('features', {})

        # CRITICAL FIX: Filter features for specific timeframe only
        timeframe_features = {}

        if timeframe == '5m':
            # For 5m timeframe: include base OHLCV features without prefixes
            # Plus meta features like timestamp, symbol
            for feature_name, feature_values in all_features.items():
                # Include base features (no timeframe prefix) and meta features
                if not any(feature_name.startswith(f'{tf}_') for tf in ['5m', '15m', '1h', '1d', '1w']):
                    # This is a base feature (like 'open', 'close') or meta feature
                    if any(base in feature_name.lower() for base in ['open', 'high', 'low', 'close', 'volume', 'vwap', 'sma', 'ema', 'rsi', 'etop', 'ebot', 'pldot']):
                        timeframe_features[feature_name] = feature_values
        else:
            # For other timeframes: include ONLY features with matching prefix
            timeframe_prefix = f'{timeframe}_'
            for feature_name, feature_values in all_features.items():
                if feature_name.startswith(timeframe_prefix):
                    # Include this feature as it belongs to this timeframe
                    timeframe_features[feature_name] = feature_values

        # Always include meta features (timestamp, symbol)
        for meta_feature in ['timestamp', 'symbol']:
            if meta_feature in all_features:
                timeframe_features[meta_feature] = all_features[meta_feature]

        # Create filtered example with ONLY timeframe-specific features
        timeframe_example = {
            'symbol': example['symbol'],
            'timeframe': timeframe,
            'timestamp': example.get('timestamp'),
            'features': timeframe_features,  # NOW PROPERLY FILTERED
            'labels': example.get('labels', {}),      # Keep labels (could also filter if needed)
            'metadata': {
                **example.get('metadata', {}),
                'extracted_timeframe': timeframe,
                'original_feature_count': len(all_features),
                'filtered_feature_count': len(timeframe_features)
            }
        }
        timeframe_examples.append(timeframe_example)

    return timeframe_examples


def test_timeframe_filtering():
    """Test that timeframe filtering works correctly."""
    print("🧪 Testing timeframe filtering fix...")

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
            'rsi': [50.0, 55.0, 60.0],

            # 15m features (should appear in 15m timeframe only)
            '15m_open': [100.0, 102.0],
            '15m_high': [101.5, 103.5],
            '15m_close': [101.0, 103.0],
            '15m_volume': [2100, 2300],
            '15m_vwap': [101.0, 102.5],

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
        '5m': ['timestamp', 'symbol', 'open', 'high', 'close', 'volume', 'vwap', 'rsi'],
        '15m': ['timestamp', 'symbol', '15m_open', '15m_high', '15m_close', '15m_volume', '15m_vwap'],
        '1h': ['timestamp', 'symbol', '1h_open', '1h_high', '1h_close', '1h_volume'],
        '1d': ['timestamp', 'symbol', '1d_open', '1d_high', '1d_close', '1d_volume'],
        '1w': ['timestamp', 'symbol']  # No 1w features in this test
    }

    success_count = 0
    detailed_results = []

    for timeframe in timeframes:
        print(f"\n🔍 Testing {timeframe} timeframe filtering...")

        # Apply filtering
        filtered_examples = extract_timeframe_data_fixed([mock_example], timeframe)

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

        result = {
            'timeframe': timeframe,
            'expected_count': len(expected),
            'filtered_count': len(filtered_features),
            'missing': list(missing),
            'unexpected': list(unexpected),
            'perfect_match': not missing and not unexpected
        }

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
        filtering_ratio = (original_count - filtered_count) / original_count if original_count > 0 else 0

        print(f"   📊 Feature counts: {original_count} → {filtered_count} ({filtering_ratio:.1%} filtered)")

        result.update({
            'original_count': original_count,
            'filtering_ratio': filtering_ratio
        })

        detailed_results.append(result)

    print(f"\n🎯 FILTERING TEST RESULTS:")
    print(f"   Successful timeframes: {success_count}/{len(timeframes)}")

    # Summary table
    print("\n📋 DETAILED RESULTS:")
    print("Timeframe | Expected | Filtered | Ratio  | Status")
    print("-" * 50)
    for result in detailed_results:
        status = "✅ PASS" if result['perfect_match'] else "❌ FAIL"
        ratio = f"{result['filtering_ratio']:>5.1%}" if result.get('filtering_ratio') else " N/A "
        print(f"{result['timeframe']:>9} | {result['expected_count']:>8} | {result['filtered_count']:>8} | {ratio} | {status}")

    if success_count == len(timeframes):
        print("\n✅ ALL TIMEFRAME FILTERING TESTS PASSED!")
        print("🎉 The timeframe separation bug has been FIXED!")
        return True
    else:
        print(f"\n❌ {len(timeframes) - success_count} TIMEFRAME FILTERING TESTS FAILED!")
        return False


if __name__ == "__main__":
    print("🔧 TIMEFRAME SEPARATION FIX VALIDATION")
    print("=" * 50)

    success = test_timeframe_filtering()

    print("\n" + "=" * 50)
    print("📊 VALIDATION SUMMARY:")

    if success:
        print("🎉 SUCCESS: Timeframe separation fix is working correctly!")
        print("✅ Each timeframe now contains only its specific features")
        print("✅ 5m timeframe uses base feature names (no prefix)")
        print("✅ Other timeframes use prefixed feature names")
        print("✅ Meta features (timestamp, symbol) are included in all timeframes")
        print("\n🚀 Ready to regenerate training datasets with fixed logic!")
    else:
        print("💥 FAILED: Timeframe separation fix needs more work")
        print("❌ Some timeframes still have incorrect feature filtering")
        print("🔧 Review the filtering logic and fix remaining issues")

    exit(0 if success else 1)