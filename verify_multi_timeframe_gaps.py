#!/usr/bin/env python3
"""
Verify that current training data generation is missing multi-timeframe features.
This script demonstrates the gaps in the current implementation.
"""

import json
from pathlib import Path

def verify_gin_config():
    """Verify the training_data.gin configuration requirements."""
    print("🔍 Analyzing training_data.gin configuration...")
    
    gin_config_path = Path("config/training_data.gin")
    if not gin_config_path.exists():
        print("❌ training_data.gin file not found!")
        return
    
    with open(gin_config_path, 'r') as f:
        content = f.read()
    
    print("\n📋 Expected configuration from training_data.gin:")
    print("=" * 50)
    
    # Expected timeframes
    expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
    print(f"Timeframes: {expected_timeframes}")
    
    # Expected sequence lengths 
    expected_sequence_lengths = {
        '5m': 52,   # Past 52 x 5-minute intervals (4.3 hours)
        '15m': 52,  # Past 52 x 15-minute intervals (13 hours)
        '1h': 24,   # Past 24 x 1-hour intervals (1 day)
        '1d': 20,   # Past 20 x daily intervals (4 weeks)
    }
    print(f"Sequence lengths: {expected_sequence_lengths}")
    
    # Expected prediction horizons
    expected_prediction_horizons = {
        '1h': 6,    # Next 6 hours
        '1d': 5,    # Next 5 days
    }
    print(f"Prediction horizons: {expected_prediction_horizons}")
    
    # Expected feature types
    expected_feature_types = [
        'ohlcv', 'returns', 'volatility', 'volume_profile', 'technical', 'market_structure'
    ]
    print(f"Feature types: {expected_feature_types}")
    
    return expected_timeframes, expected_sequence_lengths, expected_feature_types

def analyze_current_implementation():
    """Analyze what the current implementation actually produces."""
    print("\n🔍 Analyzing current implementation...")
    print("=" * 50)
    
    # What we actually generate (from the successful run)
    current_features = [
        'open', 'high', 'low', 'close', 'volume',  # Daily OHLCV
        'sma_10', 'sma_20',                        # Daily technical indicators  
        'price_ratio_10', 'price_ratio_20',        # Daily ratios
        'volume_ratio'                             # Daily volume ratio
    ]
    
    current_structure = {
        'training_interval': '1d',  # Daily sequences, not hourly rows
        'timeframes_covered': ['1d'],  # Only daily
        'feature_count': len(current_features),
        'sequence_format': 'daily_sequences',  # (sequences, 20_days, 10_features)
        'output_shape': 'e.g., (2464, 20, 10) for AAPL'
    }
    
    print(f"Current features ({len(current_features)}): {current_features}")
    print(f"Current structure: {current_structure}")
    
    return current_features, current_structure

def calculate_expected_multi_timeframe_features():
    """Calculate what we should be generating for proper multi-timeframe features."""
    print("\n🎯 Calculating expected multi-timeframe features...")
    print("=" * 50)
    
    expected_timeframes = ['5m', '15m', '1h', '1d']
    sequence_lengths = {'5m': 52, '15m': 52, '1h': 24, '1d': 20}
    feature_types = ['ohlcv', 'returns', 'volatility', 'technical']  # Simplified
    ohlcv_features = ['open', 'high', 'low', 'close', 'volume']  # 5 per type
    
    total_features = 0
    feature_breakdown = {}
    
    for timeframe in expected_timeframes:
        seq_length = sequence_lengths[timeframe]
        features_per_timeframe = len(feature_types) * len(ohlcv_features) * seq_length
        feature_breakdown[timeframe] = features_per_timeframe
        total_features += features_per_timeframe
        
        print(f"{timeframe:>3}: {seq_length:>2} intervals × {len(feature_types)} types × {len(ohlcv_features)} OHLCV = {features_per_timeframe:>4} features")
    
    print(f"\nTotal expected features per hourly row: {total_features}")
    print(f"Feature breakdown: {feature_breakdown}")
    
    return total_features, feature_breakdown

def detect_gaps():
    """Detect the gaps between current implementation and gin requirements."""
    print("\n🚨 DETECTING GAPS...")
    print("=" * 50)
    
    expected_timeframes, expected_sequence_lengths, expected_feature_types = verify_gin_config()
    current_features, current_structure = analyze_current_implementation()
    expected_total, feature_breakdown = calculate_expected_multi_timeframe_features()
    
    gaps = []
    
    # Gap 1: Missing timeframes
    current_timeframes = set()
    for feature in current_features:
        for timeframe in expected_timeframes:
            if feature.startswith(f"{timeframe}_"):
                current_timeframes.add(timeframe)
    
    missing_timeframes = set(expected_timeframes) - current_timeframes
    if missing_timeframes:
        gaps.append(f"Missing timeframes: {missing_timeframes}")
    
    # Gap 2: Feature count mismatch
    current_feature_count = len(current_features)
    if current_feature_count < expected_total / 10:  # Even 1/10th would be substantial
        gaps.append(f"Feature count too low: {current_feature_count} vs expected ~{expected_total}")
    
    # Gap 3: Wrong output structure
    if current_structure['training_interval'] != '1h':
        gaps.append(f"Wrong training interval: {current_structure['training_interval']} (should be '1h')")
    
    # Gap 4: Missing multi-timeframe structure
    if current_structure['sequence_format'] != 'hourly_multi_timeframe':
        gaps.append(f"Wrong sequence format: {current_structure['sequence_format']} (should be 'hourly_multi_timeframe')")
    
    # Report gaps
    if gaps:
        print("❌ GAPS DETECTED:")
        for i, gap in enumerate(gaps, 1):
            print(f"   {i}. {gap}")
    else:
        print("✅ No gaps detected - implementation matches gin config")
    
    return gaps

def show_correct_implementation_structure():
    """Show what the correct implementation should look like."""
    print("\n✅ CORRECT IMPLEMENTATION STRUCTURE:")
    print("=" * 50)
    
    correct_structure = {
        'training_data_generation': 'Hourly rows (not daily sequences)',
        'features_per_row': '~1000+ multi-timeframe features',
        'feature_naming': 'timeframe_type_metric_lag_N (e.g., 5m_ohlcv_close_lag_10)',
        'timeframes_included': ['5m', '15m', '1h', '1d', '1w'],
        'output_shape': '(hours, multi_timeframe_features)',
        'example_features': [
            '5m_ohlcv_open_lag_0',    # Most recent 5min open
            '5m_ohlcv_close_lag_51',  # 52nd 5min close back (4.3 hours ago)
            '15m_returns_1d_lag_0',   # Most recent 15min daily return
            '1h_volatility_lag_23',   # 24th hour volatility back
            '1d_technical_sma20_lag_19'  # 20th day SMA20 back (4 weeks ago)
        ]
    }
    
    for key, value in correct_structure.items():
        if isinstance(value, list):
            print(f"{key}:")
            for item in value:
                print(f"  - {item}")
        else:
            print(f"{key}: {value}")

if __name__ == "__main__":
    print("🔍 MULTI-TIMEFRAME TRAINING DATA GAP ANALYSIS")
    print("=" * 60)
    
    gaps = detect_gaps()
    
    show_correct_implementation_structure()
    
    print(f"\n📊 SUMMARY:")
    print(f"   Gaps detected: {len(gaps)}")
    print(f"   Current implementation: Single-timeframe daily sequences") 
    print(f"   Required implementation: Multi-timeframe hourly rows")
    
    if gaps:
        print(f"\n🎯 NEXT STEPS:")
        print(f"   1. Create proper multi-timeframe feature extraction")
        print(f"   2. Generate hourly training rows (not daily sequences)")
        print(f"   3. Include all 5 timeframes per row")
        print(f"   4. Verify against training_data.gin configuration")