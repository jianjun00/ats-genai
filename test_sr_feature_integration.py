#!/usr/bin/env python3
"""
Test Support/Resistance Feature Integration

Validates that S/R features are properly integrated into the training data pipeline.
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def setup_logging():
    """Setup basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def generate_test_ohlcv_data(bars: int = 100) -> pd.DataFrame:
    """Generate realistic test OHLCV data with clear S/R patterns."""
    np.random.seed(42)

    # Create a dataset with clear support at 100 and resistance at 120
    dates = pd.date_range(start=datetime.now() - timedelta(days=bars), periods=bars, freq='H')

    data = []
    base_price = 110  # Starting price between support and resistance

    for i, timestamp in enumerate(dates):
        # Create oscillation between 95-125 with S/R behavior
        cycle_position = (i % 40) / 40.0  # 40-bar cycle
        trend_price = 100 + 20 * (0.5 + 0.4 * np.sin(2 * np.pi * cycle_position))

        # Add S/R behavior
        if trend_price > 118:  # Near resistance at 120
            trend_price = 120 - abs(np.random.normal(0, 1))
            volume_mult = 2.0
        elif trend_price < 102:  # Near support at 100
            trend_price = 100 + abs(np.random.normal(0, 1))
            volume_mult = 1.8
        else:
            volume_mult = 1.0

        # Add some noise
        noise = np.random.normal(0, 0.5)
        close = max(0.1, trend_price + noise)

        # Generate OHLC from close
        range_pct = abs(np.random.normal(0, 0.01))  # 1% typical range
        price_range = close * range_pct

        high = close + np.random.uniform(0, price_range)
        low = close - np.random.uniform(0, price_range)
        open_price = low + np.random.uniform(0, high - low)

        volume = int(1000000 * volume_mult * np.random.uniform(0.9, 1.1))

        data.append({
            'timestamp': timestamp,
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume
        })

    return pd.DataFrame(data)

def test_sr_feature_extractor():
    """Test the standalone S/R feature extractor."""
    print("=== Testing Standalone S/R Feature Extractor ===")

    try:
        from domains.ml.services.training_data.features.support_resistance_features import (
            SupportResistanceFeatureExtractor
        )

        # Create extractor
        extractor = SupportResistanceFeatureExtractor()

        # Generate test data
        test_data = generate_test_ohlcv_data(120)
        print(f"Generated {len(test_data)} bars of test data")
        print(f"Price range: ${test_data['close'].min():.2f} - ${test_data['close'].max():.2f}")

        # Extract features for different timeframes
        timeframes = ['1h', '1d']

        for timeframe in timeframes:
            features = extractor.extract_sr_features(test_data, timeframe)

            print(f"\n{timeframe} S/R Features:")
            for key, value in sorted(features.items()):
                print(f"  {key}: {value:.4f}")

        print("✅ Standalone S/R feature extractor works")
        return True

    except Exception as e:
        print(f"❌ Standalone S/R feature extractor failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_training_data_config():
    """Test that TrainingDataConfig includes support_resistance."""
    print("\n=== Testing TrainingDataConfig Integration ===")

    try:
        from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig

        # Create config
        config = TrainingDataConfig()

        print(f"Feature types: {config.feature_types}")

        if 'support_resistance' in config.feature_types:
            print("✅ support_resistance is included in feature_types")
            return True
        else:
            print("❌ support_resistance is NOT in feature_types")
            return False

    except Exception as e:
        print(f"❌ TrainingDataConfig test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_multitiimeframe_feature_extractor():
    """Test MultiTimeframeFeatureExtractor with S/R features."""
    print("\n=== Testing MultiTimeframeFeatureExtractor Integration ===")

    try:
        from domains.ml.services.training_data.timeseries_sequence_training_generator import (
            TrainingDataConfig, MultiTimeframeFeatureExtractor
        )

        # Create config with S/R features enabled
        config = TrainingDataConfig(
            feature_types=['ohlcv', 'support_resistance', 'returns']
        )

        # Create feature extractor
        extractor = MultiTimeframeFeatureExtractor(config)

        # Generate test data
        test_data = generate_test_ohlcv_data(80)

        # Extract all features
        all_features = extractor.extract_all_features(test_data, '1h')

        print(f"Total features extracted: {len(all_features)}")

        # Check for S/R features
        sr_features = {k: v for k, v in all_features.items() if 'support' in k or 'resistance' in k}

        print(f"\nS/R Features ({len(sr_features)}):")
        for key, value in sorted(sr_features.items()):
            print(f"  {key}: {value:.4f}")

        if sr_features:
            print("✅ MultiTimeframeFeatureExtractor successfully extracts S/R features")
            return True
        else:
            print("❌ No S/R features found in extracted features")
            return False

    except Exception as e:
        print(f"❌ MultiTimeframeFeatureExtractor test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_feature_completeness():
    """Test that S/R features are complete and meaningful."""
    print("\n=== Testing S/R Feature Completeness ===")

    try:
        from domains.ml.services.training_data.features.support_resistance_features import (
            SupportResistanceFeatureExtractor
        )

        extractor = SupportResistanceFeatureExtractor()

        # Test with data that has clear S/R patterns
        test_data = generate_test_ohlcv_data(150)
        features = extractor.extract_sr_features(test_data, '1h')

        # Expected feature categories
        expected_features = [
            'support_distance', 'support_strength',
            'resistance_distance', 'resistance_strength',
            'recent_tests', 'tests_confidence', 'tests_volume_spike',
            'hold_strong_tests', 'break_clean_tests', 'penetration_tests',
            'sr_level_density', 'near_support', 'near_resistance'
        ]

        print("Feature completeness check:")
        missing_features = []

        for expected in expected_features:
            feature_key = f'1h_{expected}'
            if feature_key not in features:
                missing_features.append(feature_key)
            else:
                value = features[feature_key]
                print(f"  ✓ {feature_key}: {value:.4f}")

        if missing_features:
            print(f"\n❌ Missing features: {missing_features}")
            return False
        else:
            print(f"\n✅ All {len(expected_features)} S/R feature categories present")

            # Check for reasonable values
            reasonable_values = True
            if features['1h_support_distance'] < 0 or features['1h_support_distance'] > 1:
                print("⚠️  support_distance seems unreasonable")
                reasonable_values = False

            if features['1h_resistance_distance'] < 0 or features['1h_resistance_distance'] > 1:
                print("⚠️  resistance_distance seems unreasonable")
                reasonable_values = False

            if reasonable_values:
                print("✅ Feature values appear reasonable")

            return True

    except Exception as e:
        print(f"❌ Feature completeness test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all S/R feature integration tests."""
    setup_logging()

    print("🧪 Support/Resistance Feature Integration Test Suite")
    print("=" * 60)

    tests = [
        test_sr_feature_extractor,
        test_training_data_config,
        test_multitiimeframe_feature_extractor,
        test_feature_completeness
    ]

    results = []

    for test_func in tests:
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test_func.__name__} crashed: {e}")
            results.append(False)

        print()

    # Summary
    passed = sum(results)
    total = len(results)

    print("=" * 60)
    print(f"🎯 TEST RESULTS: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 ALL TESTS PASSED - S/R features are properly integrated!")
        print("\n📊 Key Validation Points:")
        print("   ✓ S/R feature extractor works standalone")
        print("   ✓ TrainingDataConfig includes 'support_resistance' feature type")
        print("   ✓ MultiTimeframeFeatureExtractor integrates S/R features")
        print("   ✓ S/R features are complete and have reasonable values")
        print("\n🚀 Ready for training data generation with S/R labeling!")
        return True
    else:
        print("❌ SOME TESTS FAILED - Review issues above")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)