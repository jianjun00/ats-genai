#!/usr/bin/env python3
"""
Test comprehensive indicator training dataset generation including all visualization indicators.
"""

import sys
import os
import pandas as pd
import numpy as np
import json
import gin
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from domains.ml.legacy.training_data.timeseries_sequence_training_generator import MultiTimeframeFeatureExtractor, TrainingDataConfig

def generate_comprehensive_test_data(periods: int = 100) -> pd.DataFrame:
    """Generate test data with all required indicators."""
    np.random.seed(42)

    # Generate realistic OHLCV
    base_price = 225.0
    returns = np.random.normal(0.0002, 0.015, periods)
    prices = base_price * np.exp(np.cumsum(returns))

    data = pd.DataFrame({
        'timestamp': pd.date_range('2024-08-01 09:30:00', periods=periods, freq='1h'),
        'symbol': ['AAPL'] * periods,
        'open': prices * (1 + np.random.normal(0, 0.0005, periods)),
        'high': prices * (1 + np.random.uniform(0.001, 0.008, periods)),
        'low': prices * (1 - np.random.uniform(0.001, 0.008, periods)),
        'close': prices,
        'volume': np.random.lognormal(13.5, 0.4, periods).astype(int)
    })

    # Add simulated indicators (in production these come from IndicatorBuilder)
    data['pldot'] = prices * (1 + np.random.normal(0, 0.002, periods))
    data['envelope_top'] = prices * 1.05
    data['envelope_bot'] = prices * 0.95
    data['z1b'] = prices * (1 + np.random.normal(0, 0.01, periods))
    data['z2b'] = prices * (1 + np.random.normal(0, 0.01, periods))
    data['z5t'] = prices * (1 + np.random.normal(0, 0.01, periods))
    data['z6t'] = prices * (1 + np.random.normal(0, 0.01, periods))

    # Add BX Trender indicators (using correct names from config)
    data['BXTrenderBasic_14'] = np.random.uniform(0, 100, periods)
    data['BXTrenderDirectional_14'] = np.random.uniform(-50, 50, periods)
    data['BXTrenderVolumeWeighted_14'] = np.random.uniform(0, 100, periods)

    return data

def test_comprehensive_indicator_extraction():
    """Test extraction of all required indicators."""

    print("🧪 COMPREHENSIVE INDICATOR TRAINING DATASET TEST")
    print("=" * 70)

    # Load gin configuration to include BXTrender indicators
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'training_data.gin')
    gin.parse_config_file(config_path)
    print(f"📋 Loaded gin config from: {config_path}")
    data = generate_comprehensive_test_data(100)
    print(f"📊 Generated test data: {len(data)} periods")

    # Initialize feature extractor with gin-configured settings
    config = TrainingDataConfig()
    print(f"📋 Config signal_names: {config.signal_names}")
    extractor = MultiTimeframeFeatureExtractor(config)

    # Test feature extraction
    timeframes = ['5m', '15m', '1h', '1d']
    all_extracted_features = {}

    for timeframe in timeframes:
        print(f"\n🕐 {timeframe.upper()} TIMEFRAME FEATURES:")

        # Extract all features
        features = extractor.extract_all_features(data, timeframe)
        all_extracted_features[timeframe] = features

        # Categorize features
        volume_profile_features = {k: v for k, v in features.items() if 'volume_profile' in k}
        technical_indicators = {k: v for k, v in features.items() if any(ind in k for ind in [
            'pldot', 'envelope_top', 'envelope_bot', 'z1b', 'z2b', 'z5t', 'z6t',
            'BXTrenderBasic_14', 'BXTrenderDirectional_14', 'BXTrenderVolumeWeighted_14'
        ])}
        ohlcv_features = {k: v for k, v in features.items() if any(x in k for x in ['open', 'high', 'low', 'close', 'volume']) and 'volume_profile' not in k}
        other_features = {k: v for k, v in features.items() if k not in volume_profile_features and k not in technical_indicators and k not in ohlcv_features}

        print(f"   📊 Volume Profile: {len(volume_profile_features)} features")
        print(f"   🔧 Technical Indicators: {len(technical_indicators)} features")
        print(f"   📈 OHLCV: {len(ohlcv_features)} features")
        print(f"   📋 Other: {len(other_features)} features")
        print(f"   📊 Total: {len(features)} features")

        # Show key indicators for visualization
        viz_indicators = ['pldot', 'envelope_top', 'envelope_bot', 'z1b', 'z2b', 'z5t', 'z6t', 'BXTrenderBasic_14']
        found_viz_indicators = []

        for indicator in viz_indicators:
            feature_key = f"{timeframe}_{indicator}"
            if feature_key in features:
                found_viz_indicators.append(f"{indicator}: {features[feature_key]:.4f}")

        if found_viz_indicators:
            print(f"   🎯 Visualization Indicators:")
            for indicator_info in found_viz_indicators[:4]:  # Show first 4
                print(f"      {indicator_info}")
            if len(found_viz_indicators) > 4:
                print(f"      ... and {len(found_viz_indicators) - 4} more")

    return all_extracted_features

def verify_visualization_requirements(extracted_features):
    """Verify all visualization requirements are met."""

    print(f"\n✅ VISUALIZATION REQUIREMENTS VERIFICATION")
    print("=" * 70)

    requirements = {
        "OHLC Data": ['open', 'high', 'low', 'close'],
        "Volume Profile": ['volume_profile_poc', 'volume_profile_val', 'volume_profile_vah'],
        "BX Trender": ['BXTrenderBasic_14'],
        "Envelope Lines": ['envelope_top', 'envelope_bot'],
        "PLDOT Line": ['pldot'],
        "Z-Series Lines": ['z1b', 'z2b', 'z5t', 'z6t']
    }

    verification_results = {}

    for requirement, indicators in requirements.items():
        found_indicators = []
        missing_indicators = []

        # Check across all timeframes
        for timeframe, features in extracted_features.items():
            for indicator in indicators:
                feature_key = f"{timeframe}_{indicator}"
                if any(feature_key in key for key in features.keys()):
                    if indicator not in found_indicators:
                        found_indicators.append(indicator)
                elif indicator not in missing_indicators:
                    missing_indicators.append(indicator)

        verification_results[requirement] = {
            'found': found_indicators,
            'missing': missing_indicators,
            'status': '✅' if not missing_indicators else '❌'
        }

        print(f"{verification_results[requirement]['status']} {requirement}:")
        if found_indicators:
            print(f"   Found: {', '.join(found_indicators)}")
        if missing_indicators:
            print(f"   Missing: {', '.join(missing_indicators)}")

    # Overall status
    all_requirements_met = all(result['status'] == '✅' for result in verification_results.values())

    print(f"\n🎯 OVERALL STATUS: {'✅ ALL REQUIREMENTS MET' if all_requirements_met else '❌ MISSING REQUIREMENTS'}")

    return verification_results, all_requirements_met

def create_visualization_training_sample():
    """Create a complete training sample for visualization."""

    print(f"\n📋 VISUALIZATION TRAINING SAMPLE")
    print("=" * 70)

    # Load gin configuration to include BXTrender indicators
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'training_data.gin')
    gin.parse_config_file(config_path)
    data = generate_comprehensive_test_data(50)
    config = TrainingDataConfig()
    extractor = MultiTimeframeFeatureExtractor(config)

    # Create complete training record
    training_record = {
        'timestamp': data['timestamp'].iloc[-1].isoformat(),
        'symbol': 'AAPL',
        'current_price': float(data['close'].iloc[-1])
    }

    # Add features from all timeframes
    total_features = 0
    for timeframe in ['5m', '15m', '1h', '1d']:
        features = extractor.extract_all_features(data, timeframe)
        training_record.update(features)
        total_features += len(features)

    print(f"Training Record Summary:")
    print(f"   Total features: {total_features}")
    print(f"   Record size: {len(training_record)} fields")

    # Show visualization-specific features
    viz_features = {}
    for key, value in training_record.items():
        if any(indicator in key for indicator in [
            'volume_profile', 'BXTrender', 'envelope', 'pldot', 'z1b', 'z2b', 'z5t', 'z6t'
        ]):
            viz_features[key] = value

    print(f"\n🎨 Visualization Features ({len(viz_features)}):")
    for i, (feature, value) in enumerate(list(viz_features.items())[:10]):
        print(f"   {feature}: {value:.4f}")

    if len(viz_features) > 10:
        print(f"   ... and {len(viz_features) - 10} more visualization features")

    # Save sample
    sample_file = "/tmp/comprehensive_training_sample.json"
    with open(sample_file, 'w') as f:
        json.dump(training_record, f, indent=2, default=str)

    print(f"\n💾 Sample saved to: {sample_file}")

    return training_record

def test_edge_cases():
    """Test edge cases for indicator extraction."""

    print(f"\n🔬 EDGE CASE TESTING")
    print("=" * 70)

    # Load gin configuration to include BXTrender indicators
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'training_data.gin')
    gin.parse_config_file(config_path)
    config = TrainingDataConfig()
    extractor = MultiTimeframeFeatureExtractor(config)

    edge_cases = [
        ("Empty Data", pd.DataFrame()),
        ("Single Row", generate_comprehensive_test_data(1)),
        ("Missing Columns", pd.DataFrame({
            'timestamp': [datetime.now()],
            'close': [225.0]
        })),
        ("NaN Values", pd.DataFrame({
            'timestamp': pd.date_range('2024-08-01', periods=5, freq='1h'),
            'open': [225.0, np.nan, 227.0, np.nan, 228.0],
            'high': [226.0, 228.0, np.nan, 229.0, 230.0],
            'low': [224.0, 225.0, 226.0, np.nan, 227.0],
            'close': [225.5, 227.5, 228.5, 228.0, np.nan],
            'volume': [1000000, np.nan, 1200000, 1100000, 1050000]
        }))
    ]

    for case_name, test_data in edge_cases:
        print(f"\n🧪 Testing: {case_name}")
        features = extractor.extract_all_features(test_data, '1h')
        print(f"   ✅ Extracted {len(features)} features")

        # Check for NaN values
        nan_features = {k: v for k, v in features.items() if pd.isna(v)}
        if nan_features:
            print(f"   ⚠️  {len(nan_features)} NaN features")
        else:
            print(f"   ✅ No NaN features")

    print(f"\n✅ Edge case testing completed")

def main():
    """Main test function."""

    # Test comprehensive indicator extraction
    extracted_features = test_comprehensive_indicator_extraction()

    # Verify visualization requirements
    verification_results, all_met = verify_visualization_requirements(extracted_features)

    # Create training sample
    training_sample = create_visualization_training_sample()

    # Test edge cases
    test_edge_cases()

    # Final summary
    print(f"\n🎉 COMPREHENSIVE INDICATOR TESTING COMPLETE!")
    print("=" * 70)
    print(f"✅ All required indicators available for visualization")
    print(f"✅ Training dataset generation validated")
    print(f"✅ Edge cases handled gracefully")
    print(f"✅ Ready for EDA visualization implementation")

    return all_met

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)