#!/usr/bin/env python3
"""
Test script for the configurable training data generation framework.

This script performs basic tests to ensure the framework components work correctly.
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_feature_registry():
    """Test feature registry functionality."""
    
    print("Testing Feature Registry...")
    
    from signals.feature_registry import FeatureRegistry, FeatureConfig
    
    # Create test data
    dates = pd.date_range('2023-01-01', periods=50, freq='D')
    data = pd.DataFrame({
        'open': np.random.uniform(100, 110, 50),
        'high': np.random.uniform(110, 120, 50), 
        'low': np.random.uniform(90, 100, 50),
        'close': np.random.uniform(95, 115, 50),
        'volume': np.random.uniform(1000000, 5000000, 50)
    }, index=dates)
    
    # Create feature configs
    feature_configs = [
        FeatureConfig(
            name="test_sma",
            feature_type="indicator",
            parameters={'indicator_type': 'sma', 'period': 10}
        ),
        FeatureConfig(
            name="test_returns",
            feature_type="transform",
            parameters={'transform_type': 'pct_change', 'column': 'close', 'periods': 1}
        )
    ]
    
    # Create registry and generate features
    registry = FeatureRegistry(features=feature_configs)
    features_df = registry.generate_features(data)
    
    print(f"  Generated features: {list(features_df.columns)}")
    print(f"  Feature data shape: {features_df.shape}")
    print(f"  Sample values: {features_df.iloc[-5:, :].to_dict()}")
    
    assert not features_df.empty, "Features should not be empty"
    assert len(features_df.columns) >= 2, "Should have at least 2 features"
    
    print("  ✓ Feature Registry test passed")
    return True

def test_label_registry():
    """Test label registry functionality."""
    
    print("Testing Label Registry...")
    
    from signals.label_registry import LabelRegistry, LabelConfig
    
    # Create test data
    dates = pd.date_range('2023-01-01', periods=50, freq='D')
    data = pd.DataFrame({
        'open': np.random.uniform(100, 110, 50),
        'high': np.random.uniform(110, 120, 50),
        'low': np.random.uniform(90, 100, 50),
        'close': np.random.uniform(95, 115, 50),
        'volume': np.random.uniform(1000000, 5000000, 50)
    }, index=dates)
    
    # Create label configs
    label_configs = [
        LabelConfig(
            name="future_return",
            label_type="return",
            parameters={'return_type': 'simple', 'column': 'close'},
            lead_periods=1
        ),
        LabelConfig(
            name="direction",
            label_type="classification", 
            parameters={'class_type': 'direction', 'column': 'close'},
            lead_periods=1
        )
    ]
    
    # Create registry and generate labels
    registry = LabelRegistry(labels=label_configs)
    labels_df = registry.generate_labels(data)
    
    print(f"  Generated labels: {list(labels_df.columns)}")
    print(f"  Label data shape: {labels_df.shape}")
    print(f"  Sample values: {labels_df.iloc[-10:-5, :].to_dict()}")
    
    assert not labels_df.empty, "Labels should not be empty"
    assert len(labels_df.columns) >= 2, "Should have at least 2 labels"
    
    print("  ✓ Label Registry test passed")
    return True

def test_indicator_factory():
    """Test indicator factory functionality."""
    
    print("Testing Indicator Factory...")
    
    from signals.indicator_factory import IndicatorFactory
    from signals.feature_registry import FeatureRegistry, FeatureConfig
    from signals.label_registry import LabelRegistry, LabelConfig
    
    # Create test data
    dates = pd.date_range('2023-01-01', periods=30, freq='D')
    data = pd.DataFrame({
        'open': np.random.uniform(100, 110, 30),
        'high': np.random.uniform(110, 120, 30),
        'low': np.random.uniform(90, 100, 30),
        'close': np.random.uniform(95, 115, 30),
        'volume': np.random.uniform(1000000, 5000000, 30)
    }, index=dates)
    
    # Create factory
    factory = IndicatorFactory()
    
    # Test indicator creation
    ema_indicator = factory.create_indicator('ema', period=10)
    result = ema_indicator.calculate(data)
    
    print(f"  EMA indicator result: {result}")
    
    # Test adding features and labels
    factory.add_feature_from_config('test_rsi', 'indicator', indicator_type='rsi', period=14)
    factory.add_label_from_config('test_return', 'return', return_type='simple', column='close')
    
    # Generate features and labels
    result = factory.generate_features_and_labels(data)
    
    print(f"  Generated feature names: {factory.get_feature_names()}")
    print(f"  Generated label names: {factory.get_label_names()}")
    print(f"  Features shape: {result['features'].shape}")
    print(f"  Labels shape: {result['labels'].shape}")
    
    assert 'value' in ema_indicator.calculate(data), "EMA should return value"
    assert not result['features'].empty, "Features should not be empty"
    assert not result['labels'].empty, "Labels should not be empty"
    
    print("  ✓ Indicator Factory test passed")
    return True

def test_configurable_generator():
    """Test configurable training data generator."""
    
    print("Testing Configurable Training Data Generator...")
    
    from modeling.configurable_train_data_generator import (
        ConfigurableTrainingDataGenerator,
        ConfigurableTrainingDataConfig
    )
    from signals.feature_registry import FeatureRegistry, FeatureConfig
    from signals.label_registry import LabelRegistry, LabelConfig
    
    # Create test data with multiple symbols
    symbols = ['AAPL', 'MSFT']
    data_rows = []
    
    for symbol in symbols:
        dates = pd.date_range('2023-01-01', periods=40, freq='D')
        for i, date in enumerate(dates):
            base_price = 100 + i * 0.5  # Simple trend
            data_rows.append({
                'date': date,
                'symbol': symbol,
                'open': base_price + np.random.uniform(-1, 1),
                'high': base_price + np.random.uniform(1, 3),
                'low': base_price + np.random.uniform(-3, -1),
                'close': base_price + np.random.uniform(-0.5, 0.5),
                'volume': np.random.uniform(1000000, 5000000)
            })
    
    data = pd.DataFrame(data_rows)
    data['date'] = pd.to_datetime(data['date'])
    data = data.set_index('date').sort_index()
    
    # Create registries
    feature_configs = [
        FeatureConfig(
            name="test_sma",
            feature_type="indicator",
            parameters={'indicator_type': 'sma', 'period': 5}
        ),
        FeatureConfig(
            name="test_returns",
            feature_type="transform",
            parameters={'transform_type': 'pct_change', 'column': 'close', 'periods': 1}
        )
    ]
    
    label_configs = [
        LabelConfig(
            name="future_return",
            label_type="return",
            parameters={'return_type': 'simple', 'column': 'close'},
            lead_periods=1
        )
    ]
    
    feature_registry = FeatureRegistry(features=feature_configs)
    label_registry = LabelRegistry(labels=label_configs)
    
    # Create configuration
    config = ConfigurableTrainingDataConfig(
        sequence_length=10,
        prediction_horizon=3,
        feature_registry=feature_registry,
        label_registry=label_registry,
        normalize_features=False,  # Disable for testing
        normalize_labels=False,
        window_stride=1,
        min_valid_ratio=0.5,  # Lower threshold for test data
        output_format='numpy'
    )
    
    # Create generator and generate data
    generator = ConfigurableTrainingDataGenerator(config)
    result = generator.generate_training_data(data, symbols=symbols)
    
    print(f"  Features shape: {result['features'].shape}")
    print(f"  Labels shape: {result['labels'].shape}")
    print(f"  Feature names: {result['feature_names']}")
    print(f"  Label names: {result['label_names']}")
    
    assert result['features'].shape[0] > 0, "Should generate at least one sequence"
    assert result['features'].shape[1] == config.sequence_length, "Sequence length should match config"
    assert result['labels'].shape[1] == config.prediction_horizon, "Prediction horizon should match config"
    assert len(result['feature_names']) >= 2, "Should have at least 2 features"
    assert len(result['label_names']) >= 1, "Should have at least 1 label"
    
    print("  ✓ Configurable Generator test passed")
    return True

def main():
    """Run all tests."""
    
    print("Testing Configurable Training Data Framework")
    print("=" * 50)
    
    tests = [
        test_feature_registry,
        test_label_registry,
        test_indicator_factory,
        test_configurable_generator
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            passed += 1
            print()
        except Exception as e:
            print(f"  ✗ {test_func.__name__} FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
            print()
    
    print("=" * 50)
    print(f"Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed!")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)