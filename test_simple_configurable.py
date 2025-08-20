#!/usr/bin/env python3
"""
Simple test of the configurable training data framework.
Uses only basic transforms that don't require complex indicators.
"""

import gin
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def create_test_data(symbols=['AAPL', 'MSFT'], days=60):
    """Create test OHLCV data."""
    
    print(f"Creating test data for {symbols} over {days} days...")
    
    data_rows = []
    start_date = datetime.now().date() - timedelta(days=days)
    
    for symbol in symbols:
        # Create synthetic price data
        np.random.seed(hash(symbol) % 2**32)
        
        dates = [start_date + timedelta(days=i) for i in range(days)]
        
        # Start with a base price and add random walk
        base_price = 100.0
        prices = [base_price]
        
        for i in range(1, days):
            # Simple random walk with small trend
            change = np.random.normal(0.001, 0.02)  # Small positive trend with volatility
            new_price = prices[-1] * (1 + change)
            prices.append(max(new_price, 1.0))  # Prevent negative prices
        
        for i, (date, close_price) in enumerate(zip(dates, prices)):
            # Generate OHLC from close price
            daily_volatility = 0.015  # 1.5% daily volatility
            
            # Generate open close to previous close with small gap
            if i == 0:
                open_price = close_price
            else:
                gap = np.random.normal(0, 0.005)  # Small overnight gap
                open_price = prices[i-1] * (1 + gap)
            
            # Generate high and low
            high_low_range = close_price * daily_volatility * np.random.uniform(0.5, 2.0)
            high = max(open_price, close_price) + high_low_range * np.random.uniform(0.3, 0.7)
            low = min(open_price, close_price) - high_low_range * np.random.uniform(0.3, 0.7)
            
            # Generate volume
            volume = np.random.uniform(1000000, 5000000)
            
            data_rows.append({
                'date': date,
                'symbol': symbol,
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close_price, 2),
                'volume': int(volume)
            })
    
    df = pd.DataFrame(data_rows)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()
    
    print(f"Created test data: {len(df)} rows, {df['symbol'].nunique()} symbols")
    print(f"Date range: {df.index.min().date()} to {df.index.max().date()}")
    print(f"Sample data:\n{df.head()}")
    
    return df

def test_simple_configurable():
    """Test with simple gin configuration."""
    
    print("="*60)
    print("TESTING SIMPLE CONFIGURABLE FRAMEWORK")
    print("="*60)
    
    # Clear any existing gin config
    gin.clear_config()
    
    # Parse simple gin configuration
    gin.parse_config_file('config/configurable_training_simple.gin')
    
    # Import after gin is configured
    from modeling.configurable_train_data_generator import (
        ConfigurableTrainingDataGenerator, 
        create_configurable_training_data_config
    )
    
    # Create test data
    data = create_test_data(symbols=['AAPL', 'MSFT'], days=50)
    
    print("\nData shape:", data.shape)
    print("Columns:", list(data.columns))
    print("Index type:", type(data.index))
    
    # Create configuration
    config = create_configurable_training_data_config()
    
    print("\nConfiguration:")
    print(f"  Sequence length: {config.sequence_length}")
    print(f"  Prediction horizon: {config.prediction_horizon}")
    print(f"  Min valid ratio: {config.min_valid_ratio}")
    print(f"  Normalize features: {config.normalize_features}")
    
    # Create generator
    generator = ConfigurableTrainingDataGenerator(config)
    
    print("\nFeature configs:")
    for feature in config.feature_registry.get_enabled_features():
        print(f"  {feature.name}: {feature.feature_type} - {feature.parameters}")
    
    print("\nLabel configs:")
    for label in config.label_registry.get_enabled_labels():
        print(f"  {label.name}: {label.label_type} - {label.parameters}")
    
    # Generate training data
    print("\nGenerating training data...")
    try:
        result = generator.generate_training_data(data, symbols=['AAPL', 'MSFT'])
        
        print("\nResults:")
        print(f"  Features shape: {result['features'].shape}")
        print(f"  Labels shape: {result['labels'].shape}")
        print(f"  Feature names: {result['feature_names']}")
        print(f"  Label names: {result['label_names']}")
        
        # Show some statistics
        if hasattr(result['features'], 'numpy'):
            features_array = result['features'].numpy()
            labels_array = result['labels'].numpy()
        else:
            features_array = result['features']
            labels_array = result['labels']
        
        print(f"\nFeature statistics:")
        print(f"  Shape: {features_array.shape}")
        print(f"  Mean: {np.nanmean(features_array):.6f}")
        print(f"  Std: {np.nanstd(features_array):.6f}")
        print(f"  NaN count: {np.isnan(features_array).sum()}")
        
        print(f"\nLabel statistics:")
        print(f"  Shape: {labels_array.shape}")
        print(f"  Mean: {np.nanmean(labels_array):.6f}")
        print(f"  Std: {np.nanstd(labels_array):.6f}")
        print(f"  NaN count: {np.isnan(labels_array).sum()}")
        
        print("\n✅ SUCCESS: Training data generated successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run the test."""
    
    success = test_simple_configurable()
    
    if success:
        print("\n🎉 Simple configurable framework test PASSED!")
        return 0
    else:
        print("\n💥 Simple configurable framework test FAILED!")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)