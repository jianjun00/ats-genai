#!/usr/bin/env python3
"""
Comprehensive Gin Configuration Validation Tests.

Tests configuration validation, error handling, and gin integration.
"""

import pytest
import gin
import tempfile
import os
import sys
import pandas as pd
import numpy as np

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from domains.trading.services.indicators.feature_registry import create_feature_registry
from domains.trading.services.indicators.label_registry import create_label_registry
from domains.ml.services.training_data.generators.configurable_train_data_generator import (
    ConfigurableTrainingDataGenerator,
    ConfigurableTrainingDataConfig,
    create_configurable_training_data_config
)

@pytest.mark.unit
class TestGinConfigurationValidation:
    """Test gin configuration validation and error handling."""

    def setup_method(self):
        """Setup for each test method."""
        gin.clear_config()

    def teardown_method(self):
        """Cleanup after each test method."""
        gin.clear_config()

    def test_valid_basic_configuration(self):
        """Test a valid basic gin configuration."""
        config_content = """
# Basic valid configuration
import domains.trading.signals.feature_registry
import domains.trading.signals.label_registry
import modeling.configurable_train_data_generator

# Feature configuration
returns_feature/create_feature_config.name = "returns"
returns_feature/create_feature_config.feature_type = "transform"
returns_feature/create_feature_config.parameters = {
    'transform_type': 'pct_change',
    'column': 'close',
    'periods': 1
}
returns_feature/create_feature_config.lag_periods = 1

# Label configuration
future_return_label/create_label_config.name = "future_return"
future_return_label/create_label_config.label_type = "return"
future_return_label/create_label_config.parameters = {
    'return_type': 'simple',
    'column': 'close'
}
future_return_label/create_label_config.lead_periods = 1

# Registry configuration
create_feature_registry.feature_configs = [@returns_feature/create_feature_config()]
create_label_registry.label_configs = [@future_return_label/create_label_config()]

# Training data configuration
create_configurable_training_data_config.sequence_length = 10
create_configurable_training_data_config.prediction_horizon = 3
create_configurable_training_data_config.feature_registry = @create_feature_registry()
create_configurable_training_data_config.label_registry = @create_label_registry()
create_configurable_training_data_config.output_format = 'numpy'
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as f:
            f.write(config_content)
            f.flush()

            gin.parse_config_file(f.name)

            # Should be able to create config without errors
            config = create_configurable_training_data_config()
            assert config.sequence_length == 10
            assert config.prediction_horizon == 3
            assert config.feature_registry is not None
            assert config.label_registry is not None

            # Check feature registry
            feature_names = config.feature_registry.get_feature_names()
            assert 'returns_pct_change_lag1' in feature_names

            # Check label registry
            label_names = config.label_registry.get_label_names()
            assert 'future_return_simple_lead1' in label_names

    def test_invalid_feature_type_configuration(self):
        """Test gin configuration with invalid feature type."""
        config_content = """
import domains.trading.signals.feature_registry

# Invalid feature type
invalid_feature/create_feature_config.name = "invalid"
invalid_feature/create_feature_config.feature_type = "nonexistent_type"
invalid_feature/create_feature_config.parameters = {}

create_feature_registry.feature_configs = [@invalid_feature/create_feature_config()]
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as f:
            f.write(config_content)
            f.flush()

            gin.parse_config_file(f.name)

            # Should create registry but handle invalid type gracefully
            registry = create_feature_registry()
            assert len(registry.features) == 1
            assert registry.features[0].feature_type == "nonexistent_type"

            # When generating features, should handle error gracefully
            test_data = pd.DataFrame({
                'close': [100, 101, 102]
            })

            result = registry.generate_features(test_data)
            assert 'invalid' in result.columns  # Should create placeholder

    def test_missing_required_parameters(self):
        """Test gin configuration with missing required parameters."""
        config_content = """
import domains.trading.signals.feature_registry

# Missing required parameters
incomplete_feature/create_feature_config.name = "incomplete"
incomplete_feature/create_feature_config.feature_type = "indicator"
incomplete_feature/create_feature_config.parameters = {
    'indicator_type': 'sma'
    # Missing 'period' parameter
}

create_feature_registry.feature_configs = [@incomplete_feature/create_feature_config()]
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as f:
            f.write(config_content)
            f.flush()

            gin.parse_config_file(f.name)

            registry = create_feature_registry()

            # Should handle missing parameters gracefully
            test_data = pd.DataFrame({
                'close': [100, 101, 102, 104, 105]
            })

            result = registry.generate_features(test_data)
            # Feature should be generated with indicator name suffix
            assert 'incomplete_sma' in result.columns

    def test_invalid_parameter_types(self):
        """Test gin configuration with invalid parameter types."""
        config_content = """
import domains.trading.signals.feature_registry

# Invalid parameter types
bad_types_feature/create_feature_config.name = "bad_types"
bad_types_feature/create_feature_config.feature_type = "indicator"
bad_types_feature/create_feature_config.parameters = {
    'indicator_type': 'sma',
    'period': "not_a_number"  # Should be int
}
bad_types_feature/create_feature_config.lag_periods = "also_not_a_number"  # Should be int

create_feature_registry.feature_configs = [@bad_types_feature/create_feature_config()]
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as f:
            f.write(config_content)
            f.flush()

            gin.parse_config_file(f.name)

            registry = create_feature_registry()

            # Should handle type errors gracefully during feature generation
            test_data = pd.DataFrame({
                'close': [100, 101, 102, 104, 105]
            })

            result = registry.generate_features(test_data)
            assert 'bad_types' in result.columns

    def test_circular_dependency_detection(self):
        """Test detection of circular dependencies in gin configuration."""
        config_content = """
import domains.trading.signals.feature_registry

# Create circular dependency
circular_a/create_feature_config.name = "circular_a"
circular_a/create_feature_config.feature_type = "custom"
circular_a/create_feature_config.parameters = {
    'function_name': 'circular_b_func'
}

circular_b/create_feature_config.name = "circular_b"
circular_b/create_feature_config.feature_type = "custom"
circular_b/create_feature_config.parameters = {
    'function_name': 'circular_a_func'
}

create_feature_registry.feature_configs = [
    @circular_a/create_feature_config(),
    @circular_b/create_feature_config()
]
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as f:
            f.write(config_content)
            f.flush()

            gin.parse_config_file(f.name)

            registry = create_feature_registry()

            # Should handle circular dependencies without infinite loops
            test_data = pd.DataFrame({
                'close': [100, 101, 102]
            })

            # This should complete (not hang) even with circular dependencies
            result = registry.generate_features(test_data)
            assert len(result.columns) == 2

    def test_large_configuration_performance(self):
        """Test performance with large gin configuration."""
        # Generate large configuration programmatically
        config_lines = [
            "import domains.trading.signals.feature_registry",
            "import domains.trading.signals.label_registry",
            "import modeling.configurable_train_data_generator",
            ""
        ]

        feature_configs = []
        label_configs = []

        # Create many feature configurations
        for i in range(50):
            feature_name = f"feature_{i}"
            config_lines.extend([
                f"{feature_name}/create_feature_config.name = \"{feature_name}\"",
                f"{feature_name}/create_feature_config.feature_type = \"transform\"",
                f"{feature_name}/create_feature_config.parameters = {{",
                f"    'transform_type': 'pct_change',",
                f"    'column': 'close',",
                f"    'periods': {i % 10 + 1}",
                f"}}",
                f"{feature_name}/create_feature_config.lag_periods = {i % 5}",
                ""
            ])
            feature_configs.append(f"@{feature_name}/create_feature_config()")

        # Create many label configurations
        for i in range(20):
            label_name = f"label_{i}"
            config_lines.extend([
                f"{label_name}/create_label_config.name = \"{label_name}\"",
                f"{label_name}/create_label_config.label_type = \"return\"",
                f"{label_name}/create_label_config.parameters = {{",
                f"    'return_type': 'simple',",
                f"    'column': 'close'",
                f"}}",
                f"{label_name}/create_label_config.lead_periods = {i % 10 + 1}",
                ""
            ])
            label_configs.append(f"@{label_name}/create_label_config()")

        # Add registry configurations
        config_lines.extend([
            f"create_feature_registry.feature_configs = [{', '.join(feature_configs)}]",
            f"create_label_registry.label_configs = [{', '.join(label_configs)}]",
            "",
            "create_configurable_training_data_config.sequence_length = 20",
            "create_configurable_training_data_config.prediction_horizon = 5",
            "create_configurable_training_data_config.feature_registry = @create_feature_registry()",
            "create_configurable_training_data_config.label_registry = @create_label_registry()"
        ])

        config_content = "\n".join(config_lines)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as f:
            f.write(config_content)
            f.flush()

            import time

            # Test parsing performance
            start_time = time.time()
            gin.parse_config_file(f.name)
            parse_time = time.time() - start_time

            # Test configuration creation performance
            start_time = time.time()
            config = create_configurable_training_data_config()
            creation_time = time.time() - start_time

            print(f"Parse time: {parse_time:.3f}s, Creation time: {creation_time:.3f}s")

            # Should complete in reasonable time
            assert parse_time < 5.0  # Parse should be fast
            assert creation_time < 10.0  # Creation can be slower but reasonable

            # Verify configuration was created correctly
            assert len(config.feature_registry.features) == 50
            assert len(config.label_registry.labels) == 20

    def test_configuration_inheritance_and_overrides(self):
        """Test gin configuration inheritance and parameter overrides."""
        base_config_content = """
import domains.trading.signals.feature_registry
import domains.trading.signals.label_registry

# Base feature configuration
base_feature/create_feature_config.name = "base_returns"
base_feature/create_feature_config.feature_type = "transform"
base_feature/create_feature_config.parameters = {
    'transform_type': 'pct_change',
    'column': 'close',
    'periods': 1
}
base_feature/create_feature_config.lag_periods = 0

# Base label configuration
base_label/create_label_config.name = "base_future_return"
base_label/create_label_config.label_type = "return"
base_label/create_label_config.parameters = {
    'return_type': 'simple',
    'column': 'close'
}
base_label/create_label_config.lead_periods = 1
"""

        override_config_content = """
# Override configurations
base_feature/create_feature_config.lag_periods = 2  # Override lag periods
base_label/create_label_config.lead_periods = 5     # Override lead periods

# Add new feature
new_feature/create_feature_config.name = "volatility"
new_feature/create_feature_config.feature_type = "transform"
new_feature/create_feature_config.parameters = {
    'transform_type': 'volatility',
    'column': 'close',
    'window': 10
}

# Create registries
create_feature_registry.feature_configs = [
    @base_feature/create_feature_config(),
    @new_feature/create_feature_config()
]
create_label_registry.label_configs = [@base_label/create_label_config()]
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as base_file:
            base_file.write(base_config_content)
            base_file.flush()

            with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as override_file:
                override_file.write(override_config_content)
                override_file.flush()

                # Parse base configuration first
                gin.parse_config_file(base_file.name)

                # Parse override configuration (should override base settings)
                gin.parse_config_file(override_file.name)

                # Create registries
                feature_registry = create_feature_registry()
                label_registry = create_label_registry()

                # Check overrides took effect
                base_feature = next(f for f in feature_registry.features if f.name == "base_returns")
                assert base_feature.lag_periods == 2  # Should be overridden

                base_label = next(l for l in label_registry.labels if l.name == "base_future_return")
                assert base_label.lead_periods == 5  # Should be overridden

                # Check new feature was added
                assert any(f.name == "volatility" for f in feature_registry.features)

@pytest.mark.unit
class TestConfigurationErrorRecovery:
    """Test error recovery and fallback mechanisms."""

    def setup_method(self):
        """Setup for each test method."""
        gin.clear_config()

    def teardown_method(self):
        """Cleanup after each test method."""
        gin.clear_config()

    def test_malformed_gin_syntax_recovery(self):
        """Test recovery from malformed gin syntax."""
        malformed_config = """
import domains.trading.signals.feature_registry

# Missing closing brace
bad_feature/create_feature_config.parameters = {
    'transform_type': 'pct_change',
    'column': 'close'
    # Missing closing brace

good_feature/create_feature_config.name = "good"
good_feature/create_feature_config.feature_type = "transform"
good_feature/create_feature_config.parameters = {
    'transform_type': 'pct_change',
    'column': 'close'
}
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as f:
            f.write(malformed_config)
            f.flush()

            # Should raise gin parsing error
            with pytest.raises(Exception):  # Gin parsing error
                gin.parse_config_file(f.name)

    def test_partial_configuration_fallback(self):
        """Test fallback to default values with partial configuration."""
        partial_config = """
import modeling.configurable_train_data_generator

# Only specify some parameters, others should use defaults
create_configurable_training_data_config.sequence_length = 25
# prediction_horizon should use default
# normalize_features should use default
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as f:
            f.write(partial_config)
            f.flush()

            gin.parse_config_file(f.name)

            config = create_configurable_training_data_config()

            # Specified parameter should be set
            assert config.sequence_length == 25

            # Unspecified parameters should use defaults
            assert config.prediction_horizon == 5  # Default value
            assert config.normalize_features == True  # Default value

    def test_empty_configuration_handling(self):
        """Test handling of completely empty configuration."""
        empty_config = """
# Empty configuration file with just comments
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.gin', delete=False) as f:
            f.write(empty_config)
            f.flush()

            gin.parse_config_file(f.name)

            # Should be able to create default configuration
            from domains.trading.services.indicators.feature_registry import FeatureRegistry
            from domains.trading.services.indicators.label_registry import LabelRegistry

            # Create empty registries
            feature_registry = FeatureRegistry()
            label_registry = LabelRegistry()

            config = ConfigurableTrainingDataConfig(
                feature_registry=feature_registry,
                label_registry=label_registry
            )

            assert config.sequence_length == 60  # Default value
            assert config.prediction_horizon == 5  # Default value

@pytest.mark.gin_heavy
@pytest.mark.skip_in_batch
class TestConfigurationIntegration:
    """Test integration of gin configuration with the full pipeline."""

    def setup_method(self):
        """Setup for each test method."""
        gin.clear_config()

    def teardown_method(self):
        """Cleanup after each test method."""
        gin.clear_config()

    def test_end_to_end_configuration_pipeline(self):
        """Test complete end-to-end pipeline with gin configuration."""
        # Set working directory to project root so gin can find config files
        original_cwd = os.getcwd()
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        os.chdir(project_root)

        # Use the consolidated training configuration
        gin.parse_config_file('config/training_data.gin')

        # Create test data
        dates = pd.date_range('2023-01-01', periods=30, freq='D')
        data = pd.DataFrame({
            'symbol': ['AAPL'] * 30,
            'open': np.random.uniform(100, 110, 30),
            'high': np.random.uniform(110, 120, 30),
            'low': np.random.uniform(90, 100, 30),
            'close': np.random.uniform(95, 115, 30),
            'volume': np.random.uniform(1000000, 5000000, 30)
        }, index=dates)

        # Create configuration from gin
        config = create_configurable_training_data_config()

        # Create generator and run pipeline
        generator = ConfigurableTrainingDataGenerator(config)
        result = generator.generate_training_data(data, symbols=['AAPL'])

        # Verify end-to-end success
        assert result['features'].shape[0] > 0
        assert result['labels'].shape[0] > 0
        assert len(result['feature_names']) > 0
        assert len(result['label_names']) > 0
    def test_configuration_validation_with_real_data(self):
        """Test configuration validation using realistic market data."""
        # Set working directory to project root so gin can find config files
        original_cwd = os.getcwd()
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        os.chdir(project_root)

        # Create realistic market data scenario
        np.random.seed(42)
        dates = pd.date_range('2023-01-01', periods=100, freq='D')

        # Create data with realistic patterns
        returns = np.random.normal(0.001, 0.02, 100)
        prices = [100]
        for ret in returns:
            prices.append(prices[-1] * (1 + ret))

        data = pd.DataFrame({
            'symbol': ['AAPL'] * 100,
            'open': [p * np.random.uniform(0.995, 1.005) for p in prices[1:]],
            'high': [p * np.random.uniform(1.005, 1.025) for p in prices[1:]],
            'low': [p * np.random.uniform(0.975, 0.995) for p in prices[1:]],
            'close': prices[1:],
            'volume': np.random.lognormal(14, 0.5, 100)
        }, index=dates)

        # Test consolidated configuration
        config_files = [
            'config/training_data.gin',
            # Using single consolidated config file
        ]

        for config_file in config_files:
            gin.clear_config()

            gin.parse_config_file(config_file)
            config = create_configurable_training_data_config()
            generator = ConfigurableTrainingDataGenerator(config)

            result = generator.generate_training_data(data, symbols=['AAPL'])

            # Each configuration should produce valid results
            assert result['features'].shape[0] > 0, f"Failed for {config_file}"
            assert result['labels'].shape[0] > 0, f"Failed for {config_file}"

            print(f"✓ {config_file}: {result['features'].shape[0]} sequences generated")

        os.chdir(original_cwd)

    def test_configuration_consistency_across_runs(self):
        """Test that gin configuration produces consistent results."""
        # Set working directory to project root so gin can find config files
        original_cwd = os.getcwd()
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        os.chdir(project_root)

        gin.parse_config_file('config/training_data.gin')

        # Create deterministic test data
        np.random.seed(42)
        dates = pd.date_range('2023-01-01', periods=50, freq='D')
        data = pd.DataFrame({
            'symbol': ['AAPL'] * 50,
            'open': np.random.uniform(100, 110, 50),
            'high': np.random.uniform(110, 120, 50),
            'low': np.random.uniform(90, 100, 50),
            'close': np.random.uniform(95, 115, 50),
            'volume': np.random.uniform(1000000, 5000000, 50)
        }, index=dates)

        # Run pipeline multiple times
        results = []
        for i in range(3):
            config = create_configurable_training_data_config()
            generator = ConfigurableTrainingDataGenerator(config)
            result = generator.generate_training_data(data, symbols=['AAPL'])
            results.append(result)

        # Results should be consistent (same shapes, same feature/label names)
        for i in range(1, len(results)):
            assert results[i]['features'].shape == results[0]['features'].shape
            assert results[i]['labels'].shape == results[0]['labels'].shape
            assert results[i]['feature_names'] == results[0]['feature_names']
            assert results[i]['label_names'] == results[0]['label_names']
def run_comprehensive_tests():
    """Run all comprehensive configuration tests."""
    import pytest

    return pytest.main([__file__, '-v', '--tb=short', '-s'])

if __name__ == "__main__":
    run_comprehensive_tests()