#!/usr/bin/env python3
"""
Example: Configurable Training Data Generation

This example demonstrates how to use the configurable training data generation framework
with gin configuration files to create flexible feature and label sets for ML training.
"""

import gin
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from signals.feature_registry import FeatureRegistry, create_feature_config
from signals.label_registry import LabelRegistry, create_label_config
from signals.indicator_factory import IndicatorFactory
from modeling.configurable_train_data_generator import (
    ConfigurableTrainingDataGenerator, 
    ConfigurableTrainingDataConfig,
    create_configurable_training_data_config
)

def create_sample_data(symbols=['AAPL', 'MSFT'], days=100):
    """Create sample OHLCV data for testing."""
    
    print(f"Creating sample data for {symbols} over {days} days...")
    
    data_rows = []
    start_date = datetime.now().date() - timedelta(days=days)
    
    for symbol in symbols:
        # Create synthetic price data with some realistic patterns
        np.random.seed(hash(symbol) % 2**32)  # Consistent seed per symbol
        
        dates = [start_date + timedelta(days=i) for i in range(days)]
        base_price = np.random.uniform(50, 200)  # Random starting price
        
        # Generate price series with trend and noise
        trend = np.random.uniform(-0.001, 0.001)  # Daily trend
        volatility = np.random.uniform(0.01, 0.03)  # Daily volatility
        
        prices = [base_price]
        for i in range(1, days):
            # Price follows random walk with trend
            change = np.random.normal(trend, volatility)
            new_price = prices[-1] * (1 + change)
            prices.append(max(new_price, 0.01))  # Prevent negative prices
        
        for i, (date, price) in enumerate(zip(dates, prices)):
            # Generate OHLC from close price
            daily_range = price * np.random.uniform(0.005, 0.03)  # Daily range
            low = price - daily_range * np.random.uniform(0.3, 0.7)
            high = price + daily_range * np.random.uniform(0.3, 0.7)
            open_price = low + (high - low) * np.random.uniform(0.2, 0.8)
            
            # Ensure OHLC constraints
            low = min(low, price, open_price)
            high = max(high, price, open_price)
            
            # Generate volume
            volume = np.random.uniform(1000000, 10000000)
            
            data_rows.append({
                'date': date,
                'symbol': symbol,
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(price, 2),
                'volume': int(volume)
            })
    
    df = pd.DataFrame(data_rows)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()
    
    print(f"Created sample data: {len(df)} rows, {df['symbol'].nunique()} symbols")
    print(f"Date range: {df.index.min().date()} to {df.index.max().date()}")
    return df

def example_basic_configuration():
    """Example using basic gin configuration."""
    
    print("="*60)
    print("EXAMPLE 1: Basic Configuration")
    print("="*60)
    
    # Parse gin configuration
    gin.clear_config()
    gin.parse_config_file('config/configurable_training_basic.gin')
    
    # Create sample data
    data = create_sample_data(symbols=['AAPL', 'MSFT'], days=60)
    
    # Create training data generator
    config = create_configurable_training_data_config()
    generator = ConfigurableTrainingDataGenerator(config)
    
    print("\nConfiguration:")
    print(f"  Sequence length: {config.sequence_length}")
    print(f"  Prediction horizon: {config.prediction_horizon}")
    print(f"  Feature scaling: {config.feature_scaling_method}")
    print(f"  Normalize features: {config.normalize_features}")
    print(f"  Normalize labels: {config.normalize_labels}")
    
    # Generate training data
    print("\nGenerating training data...")
    result = generator.generate_training_data(data, symbols=['AAPL', 'MSFT'])
    
    print("\nResults:")
    print(f"  Features shape: {result['features'].shape}")
    print(f"  Labels shape: {result['labels'].shape}")
    print(f"  Feature names ({len(result['feature_names'])}): {result['feature_names']}")
    print(f"  Label names ({len(result['label_names'])}): {result['label_names']}")
    
    # Show some statistics
    if hasattr(result['features'], 'numpy'):
        features_array = result['features'].numpy()
        labels_array = result['labels'].numpy()
    else:
        features_array = result['features']
        labels_array = result['labels']
    
    print(f"\nFeature statistics:")
    print(f"  Mean: {np.nanmean(features_array):.6f}")
    print(f"  Std: {np.nanstd(features_array):.6f}")
    print(f"  Min: {np.nanmin(features_array):.6f}")
    print(f"  Max: {np.nanmax(features_array):.6f}")
    
    print(f"\nLabel statistics:")
    print(f"  Mean: {np.nanmean(labels_array):.6f}")
    print(f"  Std: {np.nanstd(labels_array):.6f}")
    print(f"  Min: {np.nanmin(labels_array):.6f}")
    print(f"  Max: {np.nanmax(labels_array):.6f}")
    
    return result

def example_advanced_configuration():
    """Example using advanced gin configuration."""
    
    print("="*60)
    print("EXAMPLE 2: Advanced Configuration")
    print("="*60)
    
    # Parse gin configuration
    gin.clear_config()
    gin.parse_config_file('config/configurable_training_advanced.gin')
    
    # Create sample data with more history for advanced features
    data = create_sample_data(symbols=['AAPL', 'GOOGL', 'TSLA'], days=120)
    
    # Create training data generator
    config = create_configurable_training_data_config()
    generator = ConfigurableTrainingDataGenerator(config)
    
    print("\nConfiguration:")
    print(f"  Sequence length: {config.sequence_length}")
    print(f"  Prediction horizon: {config.prediction_horizon}")
    print(f"  Window stride: {config.window_stride}")
    print(f"  Min valid ratio: {config.min_valid_ratio}")
    print(f"  Remove outliers: {config.remove_outliers}")
    print(f"  Outlier threshold: {config.outlier_threshold}")
    
    # Generate training data
    print("\nGenerating training data...")
    result = generator.generate_training_data(data, symbols=['AAPL', 'GOOGL', 'TSLA'])
    
    print("\nResults:")
    print(f"  Features shape: {result['features'].shape}")
    print(f"  Labels shape: {result['labels'].shape}")
    print(f"  Feature names ({len(result['feature_names'])}): {result['feature_names'][:10]}...")
    print(f"  Label names ({len(result['label_names'])}): {result['label_names']}")
    
    return result

def example_custom_configuration():
    """Example of creating custom configuration programmatically."""
    
    print("="*60)
    print("EXAMPLE 3: Custom Configuration (Programmatic)")
    print("="*60)
    
    gin.clear_config()
    
    # Create custom feature configurations
    feature_configs = [
        create_feature_config(
            name="sma_10",
            feature_type="indicator",
            parameters={'indicator_type': 'sma', 'period': 10}
        ),
        create_feature_config(
            name="sma_20",
            feature_type="indicator", 
            parameters={'indicator_type': 'sma', 'period': 20}
        ),
        create_feature_config(
            name="rsi",
            feature_type="indicator",
            parameters={'indicator_type': 'rsi', 'period': 14}
        ),
        create_feature_config(
            name="returns",
            feature_type="transform",
            parameters={'transform_type': 'pct_change', 'column': 'close', 'periods': 1},
            lag_periods=1
        ),
        create_feature_config(
            name="volatility",
            feature_type="transform",
            parameters={'transform_type': 'volatility', 'column': 'close', 'window': 10}
        )
    ]
    
    # Create custom label configurations
    label_configs = [
        create_label_config(
            name="next_return",
            label_type="return",
            parameters={'return_type': 'simple', 'column': 'close'},
            lead_periods=1
        ),
        create_label_config(
            name="direction",
            label_type="classification",
            parameters={'class_type': 'direction', 'column': 'close'},
            lead_periods=1
        )
    ]
    
    # Create registries
    feature_registry = FeatureRegistry(features=feature_configs)
    label_registry = LabelRegistry(labels=label_configs)
    
    # Create configuration
    config = ConfigurableTrainingDataConfig(
        sequence_length=20,
        prediction_horizon=3,
        feature_registry=feature_registry,
        label_registry=label_registry,
        normalize_features=True,
        normalize_labels=False,
        window_stride=1,
        min_valid_ratio=0.8,
        output_format='pytorch'
    )
    
    # Create sample data
    data = create_sample_data(symbols=['NVDA'], days=50)
    
    # Generate training data
    generator = ConfigurableTrainingDataGenerator(config)
    
    print("\nConfiguration:")
    print(f"  Features: {[f.name for f in feature_configs]}")
    print(f"  Labels: {[l.name for l in label_configs]}")
    print(f"  Sequence length: {config.sequence_length}")
    print(f"  Prediction horizon: {config.prediction_horizon}")
    
    print("\nGenerating training data...")
    result = generator.generate_training_data(data, symbols=['NVDA'])
    
    print("\nResults:")
    print(f"  Features shape: {result['features'].shape}")
    print(f"  Labels shape: {result['labels'].shape}")
    print(f"  Feature names: {result['feature_names']}")
    print(f"  Label names: {result['label_names']}")
    
    return result

def main():
    """Run all examples."""
    
    print("Configurable Training Data Generation Examples")
    print("=" * 60)
    
    try:
        # Example 1: Basic configuration
        result1 = example_basic_configuration()
        
        # Example 2: Advanced configuration
        result2 = example_advanced_configuration()
        
        # Example 3: Custom configuration
        result3 = example_custom_configuration()
        
        print("\n" + "="*60)
        print("ALL EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*60)
        
        print("\nSummary:")
        print(f"  Basic config: {result1['features'].shape[0]} sequences")
        print(f"  Advanced config: {result2['features'].shape[0]} sequences") 
        print(f"  Custom config: {result3['features'].shape[0]} sequences")
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)