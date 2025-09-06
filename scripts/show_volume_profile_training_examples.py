#!/usr/bin/env python3
"""
Show detailed examples of training datasets with Volume Profile features.
"""

import sys
import os
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ml.training_data.timeseries_sequence_training_generator import MultiTimeframeFeatureExtractor, TrainingDataConfig

def generate_realistic_aapl_data(periods: int = 100) -> pd.DataFrame:
    """Generate realistic AAPL-like market data."""
    np.random.seed(42)
    
    # AAPL-like parameters
    base_price = 225.0
    daily_vol = 0.015  # 1.5% daily volatility
    hourly_vol = daily_vol / np.sqrt(24)  # Scale to hourly
    
    # Generate realistic price movements
    returns = np.random.normal(0.0002, hourly_vol, periods)
    prices = base_price * np.exp(np.cumsum(returns))
    
    # Create realistic OHLCV data
    dates = pd.date_range('2024-08-01 09:30:00', periods=periods, freq='1h')
    
    data = pd.DataFrame({
        'timestamp': dates,
        'symbol': ['AAPL'] * periods,
        'open': prices * (1 + np.random.normal(0, 0.0005, periods)),
        'high': prices * (1 + np.random.uniform(0.001, 0.008, periods)),
        'low': prices * (1 - np.random.uniform(0.001, 0.008, periods)),
        'close': prices,
        'volume': np.random.lognormal(13.5, 0.4, periods).astype(int)
    })
    
    return data

def show_training_dataset_structure():
    """Show complete training dataset structure with Volume Profile features."""
    
    print("🎯 TRAINING DATASET WITH VOLUME PROFILE FEATURES")
    print("=" * 70)
    
    # Generate sample data
    data = generate_realistic_aapl_data(50)
    print(f"📊 Sample data generated: {len(data)} periods")
    print(f"   Symbol: AAPL")
    print(f"   Time range: {data['timestamp'].min()} to {data['timestamp'].max()}")
    print(f"   Price range: ${data['close'].min():.2f} - ${data['close'].max():.2f}")
    
    # Initialize feature extractor
    config = TrainingDataConfig()
    extractor = MultiTimeframeFeatureExtractor(config)
    
    # Extract features for each timeframe
    timeframes = ['5m', '15m', '1h', '1d']
    complete_record = {
        'timestamp': data['timestamp'].iloc[-1].isoformat(),
        'symbol': 'AAPL',
        'current_price': float(data['close'].iloc[-1])
    }
    
    print(f"\n📈 EXTRACTED FEATURES BY TIMEFRAME:")
    print("-" * 70)
    
    all_feature_count = 0
    
    for timeframe in timeframes:
        print(f"\n🕐 {timeframe.upper()} TIMEFRAME FEATURES:")
        
        # Extract all features for this timeframe
        features = extractor.extract_all_features(data, timeframe)
        
        # Separate Volume Profile features from others
        vp_features = {k: v for k, v in features.items() if 'volume_profile' in k}
        other_features = {k: v for k, v in features.items() if 'volume_profile' not in k}
        
        print(f"   📊 Volume Profile Features ({len(vp_features)}):")
        for feature_name, value in vp_features.items():
            print(f"      {feature_name}: {value:.4f}")
            complete_record[feature_name] = float(value)
        
        print(f"   📈 Other Features ({len(other_features)}):")
        for feature_name, value in list(other_features.items())[:3]:  # Show first 3
            print(f"      {feature_name}: {value:.4f}")
            complete_record[feature_name] = float(value)
        
        if len(other_features) > 3:
            print(f"      ... and {len(other_features) - 3} more features")
        
        # Add remaining features to record
        for feature_name, value in list(other_features.items())[3:]:
            complete_record[feature_name] = float(value)
            
        all_feature_count += len(features)
    
    print(f"\n📊 COMPLETE TRAINING RECORD SUMMARY:")
    print("-" * 70)
    print(f"   Total features: {all_feature_count}")
    print(f"   Volume Profile features: {len([k for k in complete_record.keys() if 'volume_profile' in k])}")
    print(f"   Record size: {len(complete_record)} fields")
    
    return complete_record

def show_volume_profile_interpretation(record: dict):
    """Show how to interpret Volume Profile features in training."""
    
    print(f"\n🔬 VOLUME PROFILE FEATURE INTERPRETATION:")
    print("=" * 70)
    
    current_price = record['current_price']
    
    for timeframe in ['5m', '15m', '1h', '1d']:
        tf_prefix = f"{timeframe}_volume_profile_"
        
        if f"{tf_prefix}poc" in record:
            poc = record[f"{tf_prefix}poc"]
            val = record[f"{tf_prefix}val"]
            vah = record[f"{tf_prefix}vah"]
            va_range = record[f"{tf_prefix}va_range"]
            price_vs_poc = record[f"{tf_prefix}price_vs_poc"]
            va_position = record[f"{tf_prefix}va_position"]
            
            print(f"\n📊 {timeframe.upper()} Volume Profile Analysis:")
            print(f"   🎯 Point of Control (POC): ${poc:.2f}")
            print(f"   📈 Value Area High (VAH): ${vah:.2f}")
            print(f"   📉 Value Area Low (VAL): ${val:.2f}")
            print(f"   📏 Value Area Range: ${va_range:.2f}")
            print(f"   💰 Current Price: ${current_price:.2f}")
            print(f"   🔄 Price vs POC: ${price_vs_poc:.2f} ({'above' if price_vs_poc > 0 else 'below'} POC)")
            print(f"   📍 VA Position: {va_position:.2f} (0.0=VAL, 1.0=VAH)")
            
            # Trading insights
            if abs(price_vs_poc) < 1.0:
                bias = "🟡 NEUTRAL (price near POC)"
            elif price_vs_poc > 2.0:
                bias = "🟢 BULLISH (price well above POC)"
            elif price_vs_poc < -2.0:
                bias = "🔴 BEARISH (price well below POC)"
            else:
                bias = "🟠 MIXED (price moderately from POC)"
            
            print(f"   📊 Market Bias: {bias}")
            
            if 0.3 <= va_position <= 0.7:
                zone = "🎯 IN VALUE AREA (institutional acceptance zone)"
            elif va_position < 0.3:
                zone = "📉 BELOW VALUE AREA (potential support area)"
            else:
                zone = "📈 ABOVE VALUE AREA (potential resistance area)"
            
            print(f"   🏛️ Institutional Zone: {zone}")

def show_ml_feature_matrix_example():
    """Show how Volume Profile features appear in ML training matrices."""
    
    print(f"\n🤖 MACHINE LEARNING FEATURE MATRIX EXAMPLE:")
    print("=" * 70)
    
    # Generate multiple records to show matrix format
    all_records = []
    
    for i in range(3):  # Generate 3 sample records
        np.random.seed(42 + i)
        data = generate_realistic_aapl_data(50)
        
        config = TrainingDataConfig()
        extractor = MultiTimeframeFeatureExtractor(config)
        
        record = {'timestamp': data['timestamp'].iloc[-1].isoformat()}
        
        # Extract features for all timeframes
        for timeframe in ['5m', '15m', '1h', '1d']:
            features = extractor.extract_all_features(data, timeframe)
            record.update(features)
        
        all_records.append(record)
    
    # Convert to DataFrame to show matrix format
    df = pd.DataFrame(all_records)
    
    # Show Volume Profile columns
    vp_columns = [col for col in df.columns if 'volume_profile' in col]
    
    print(f"📊 Volume Profile Feature Matrix ({len(all_records)} samples x {len(vp_columns)} VProfile features):")
    print("-" * 70)
    
    # Show subset of Volume Profile features in tabular format
    key_vp_features = [
        '5m_volume_profile_poc', '5m_volume_profile_price_vs_poc',
        '1h_volume_profile_poc', '1h_volume_profile_price_vs_poc',
        '1d_volume_profile_va_position'
    ]
    
    print("Sample | 5m_POC   | 5m_vs_POC | 1h_POC   | 1h_vs_POC | 1d_VA_Pos")
    print("-------|----------|-----------|----------|-----------|----------")
    
    for i, record in enumerate(all_records):
        row = f"   {i+1}   |"
        for feature in key_vp_features:
            if feature in record:
                row += f" {record[feature]:8.2f} |"
            else:
                row += "     N/A |"
        print(row)
    
    print(f"\n📈 Total Feature Dimensions:")
    print(f"   Volume Profile features: {len(vp_columns)}")
    print(f"   Total features per sample: {len(df.columns) - 1}")  # -1 for timestamp
    print(f"   Matrix shape: {len(all_records)} samples × {len(df.columns)-1} features")
    
    # Show feature distribution
    vp_by_timeframe = {}
    for col in vp_columns:
        tf = col.split('_')[0]  # Extract timeframe (5m, 15m, etc.)
        if tf not in vp_by_timeframe:
            vp_by_timeframe[tf] = 0
        vp_by_timeframe[tf] += 1
    
    print(f"\n📊 Volume Profile features per timeframe:")
    for tf, count in vp_by_timeframe.items():
        print(f"   {tf}: {count} features")

def main():
    """Main function to demonstrate Volume Profile training datasets."""
    
    # Show complete dataset structure
    sample_record = show_training_dataset_structure()
    
    # Show interpretation of Volume Profile features
    show_volume_profile_interpretation(sample_record)
    
    # Show ML matrix format
    show_ml_feature_matrix_example()
    
    # Save sample record for inspection
    output_file = "/tmp/volume_profile_training_sample.json"
    with open(output_file, 'w') as f:
        json.dump(sample_record, f, indent=2)
    
    print(f"\n💾 Sample training record saved to: {output_file}")
    
    print(f"\n🎉 VOLUME PROFILE TRAINING DATASET EXAMPLES COMPLETE!")
    print("=" * 70)
    print("✅ Volume Profile features provide institutional-grade market structure analysis")
    print("✅ 8 features per timeframe across 4 timeframes = 32 Volume Profile features")
    print("✅ Features capture POC, Value Areas, price relationships, and market bias")
    print("✅ Ready for sophisticated machine learning models and trading strategies")

if __name__ == "__main__":
    main()