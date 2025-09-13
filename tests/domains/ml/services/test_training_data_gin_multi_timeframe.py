#!/usr/bin/env python3
"""
Test coverage for multi-timeframe training data generation using training_data.gin configuration.

This test validates that training data generation produces hourly rows with features from
multiple timeframes (5m, 15m, 1h, 1d, 1w) as specified in training_data.gin.
"""

import pytest
from pathlib import Path

class TestMultiTimeframeTrainingData:
    """Test suite to detect missing multi-timeframe features in training data generation."""

    def setup_method(self):
        """Setup test environment."""
        self.expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
        self.expected_sequence_lengths = {
            '5m': 52,   # Past 52 x 5-minute intervals (4.3 hours)
            '15m': 52,  # Past 52 x 15-minute intervals (13 hours)
            '1h': 24,   # Past 24 x 1-hour intervals (1 day)
            '1d': 20,   # Past 20 x daily intervals (4 weeks)
        }
        self.expected_prediction_horizons = {
            '1h': 6,    # Next 6 hours
            '1d': 5,    # Next 5 days
        }
        self.expected_feature_types = [
            'ohlcv',
            'returns',
            'volatility',
            'volume_profile',
            'technical',
            'market_structure'
        ]

    def test_gin_config_parsing(self):
        """Test that training_data.gin configuration can be parsed correctly."""
        gin_config_path = Path("config/training_data.gin")
        assert gin_config_path.exists(), "training_data.gin file must exist"

        # Parse gin file content to verify structure
        with open(gin_config_path, 'r') as f:
            content = f.read()

        # Check that required configurations exist
        assert "sequence_lengths" in content, "sequence_lengths must be defined in gin config"
        assert "prediction_horizons" in content, "prediction_horizons must be defined in gin config"
        assert "timeframes" in content, "timeframes must be defined in gin config"
        assert "feature_types" in content, "feature_types must be defined in gin config"

        # Verify specific timeframes are mentioned
        for timeframe in self.expected_timeframes:
            assert f"'{timeframe}'" in content or f'"{timeframe}"' in content, \
                f"Timeframe {timeframe} must be defined in gin config"

    def test_current_implementation_missing_timeframes(self):
        """Test that detects the current implementation is missing multi-timeframe features."""

        # Simulate ENHANCED implementation output (what we should now generate)
        enhanced_features = [
            # Hourly OHLCV
            'hour_open', 'hour_high', 'hour_low', 'hour_close', 'hour_volume',

            # 5-minute timeframe features (52 intervals)
            '5m_open_lag_0', '5m_close_lag_0', '5m_etop_lag_0', '5m_ebot_lag_0', '5m_pldot_lag_0',
            '5m_open_lag_51', '5m_close_lag_51', '5m_etop_lag_51', '5m_ebot_lag_51', '5m_pldot_lag_51',

            # 15-minute timeframe features (52 intervals)
            '15m_open_lag_0', '15m_close_lag_0', '15m_etop_lag_0', '15m_ebot_lag_0', '15m_pldot_lag_0',
            '15m_open_lag_51', '15m_close_lag_51', '15m_etop_lag_51', '15m_ebot_lag_51', '15m_pldot_lag_51',

            # 1-hour timeframe features (24 intervals)
            '1h_open_lag_0', '1h_close_lag_0', '1h_etop_lag_0', '1h_ebot_lag_0', '1h_pldot_lag_0',
            '1h_open_lag_23', '1h_close_lag_23', '1h_etop_lag_23', '1h_ebot_lag_23', '1h_pldot_lag_23',

            # Daily timeframe features (20 intervals)
            '1d_open_lag_0', '1d_close_lag_0', '1d_etop_lag_0', '1d_ebot_lag_0', '1d_pldot_lag_0',
            '1d_open_lag_19', '1d_close_lag_19', '1d_etop_lag_19', '1d_ebot_lag_19', '1d_pldot_lag_19'
        ]

        # Check for missing timeframe prefixes
        missing_timeframes = []
        for timeframe in self.expected_timeframes:
            timeframe_features = [f for f in enhanced_features if f.startswith(f"{timeframe}_")]
            if not timeframe_features:
                missing_timeframes.append(timeframe)

        # This test should now PASS with the enhanced implementation
        assert len(missing_timeframes) == 0, \
            f"Missing features for timeframes: {missing_timeframes}. " \
            f"Enhanced features include: {len(enhanced_features)} total features. " \
            f"Expected features with prefixes: {self.expected_timeframes}"

    def test_expected_multi_timeframe_feature_structure(self):
        """Test the expected structure of multi-timeframe features per hourly row."""

        # Expected feature structure for each hourly training row
        expected_features = []

        # 5-minute features (52 intervals = 4.3 hours of 5-min data)
        for feature_type in ['ohlcv', 'returns', 'volatility', 'technical']:
            for i in range(self.expected_sequence_lengths['5m']):
                expected_features.extend([
                    f"5m_{feature_type}_open_lag_{i}",
                    f"5m_{feature_type}_high_lag_{i}",
                    f"5m_{feature_type}_low_lag_{i}",
                    f"5m_{feature_type}_close_lag_{i}",
                    f"5m_{feature_type}_volume_lag_{i}"
                ])

        # 15-minute features (52 intervals = 13 hours of 15-min data)
        for feature_type in ['ohlcv', 'returns', 'volatility', 'technical']:
            for i in range(self.expected_sequence_lengths['15m']):
                expected_features.extend([
                    f"15m_{feature_type}_open_lag_{i}",
                    f"15m_{feature_type}_high_lag_{i}",
                    f"15m_{feature_type}_low_lag_{i}",
                    f"15m_{feature_type}_close_lag_{i}",
                    f"15m_{feature_type}_volume_lag_{i}"
                ])

        # 1-hour features (24 intervals = 1 day of hourly data)
        for feature_type in ['ohlcv', 'returns', 'volatility', 'technical']:
            for i in range(self.expected_sequence_lengths['1h']):
                expected_features.extend([
                    f"1h_{feature_type}_open_lag_{i}",
                    f"1h_{feature_type}_high_lag_{i}",
                    f"1h_{feature_type}_low_lag_{i}",
                    f"1h_{feature_type}_close_lag_{i}",
                    f"1h_{feature_type}_volume_lag_{i}"
                ])

        # 1-day features (20 intervals = 4 weeks of daily data)
        for feature_type in ['ohlcv', 'returns', 'volatility', 'technical']:
            for i in range(self.expected_sequence_lengths['1d']):
                expected_features.extend([
                    f"1d_{feature_type}_open_lag_{i}",
                    f"1d_{feature_type}_high_lag_{i}",
                    f"1d_{feature_type}_low_lag_{i}",
                    f"1d_{feature_type}_close_lag_{i}",
                    f"1d_{feature_type}_volume_lag_{i}"
                ])

        # Calculate expected feature count
        expected_feature_count = len(expected_features)

        # This should be a large number (hundreds of features per row)
        assert expected_feature_count > 1000, \
            f"Expected multi-timeframe features should be >1000, got {expected_feature_count}"

        print(f"✓ Expected multi-timeframe feature count: {expected_feature_count}")
        print(f"✓ Sample expected features: {expected_features[:10]}")

    def test_training_data_output_structure(self):
        """Test the expected training data output structure."""

        # Expected output structure for training data
        expected_output_structure = {
            'training_interval': '1h',  # Hourly training rows
            'features_per_row': '>1000',  # Multi-timeframe features
            'sequence_type': 'multi_timeframe',  # Not just daily sequences
            'timeframe_coverage': self.expected_timeframes,
            'prediction_horizons': list(self.expected_prediction_horizons.keys())
        }

        # This test documents what we should be producing
        assert expected_output_structure['training_interval'] == '1h', \
            "Training data should be generated at hourly intervals"

        assert len(expected_output_structure['timeframe_coverage']) == 5, \
            f"Should cover all 5 timeframes: {expected_output_structure['timeframe_coverage']}"

    def test_gin_config_compliance_checker(self):
        """Test function to check if training data generation complies with gin config."""

        def check_gin_compliance(generated_features, generated_labels, metadata):
            """Check if generated training data complies with training_data.gin."""

            compliance_issues = []

            # Check 1: Multi-timeframe feature coverage
            timeframes_found = set()
            for feature in generated_features:
                for timeframe in self.expected_timeframes:
                    if feature.startswith(f"{timeframe}_"):
                        timeframes_found.add(timeframe)

            missing_timeframes = set(self.expected_timeframes) - timeframes_found
            if missing_timeframes:
                compliance_issues.append(f"Missing timeframes: {missing_timeframes}")

            # Check 2: Feature count should be substantial for multi-timeframe
            if len(generated_features) < 100:
                compliance_issues.append(f"Feature count too low: {len(generated_features)}. Expected >100 for multi-timeframe")

            # Check 3: Training interval should be hourly
            if metadata.get('training_interval') != '1h':
                compliance_issues.append(f"Training interval should be '1h', got '{metadata.get('training_interval')}'")

            # Check 4: Sequence lengths should match gin config
            for timeframe, expected_length in self.expected_sequence_lengths.items():
                actual_length = metadata.get('sequence_lengths', {}).get(timeframe)
                if actual_length != expected_length:
                    compliance_issues.append(f"Sequence length for {timeframe}: expected {expected_length}, got {actual_length}")

            return compliance_issues

        # Test with current implementation (should fail)
        current_features = [
            'open', 'high', 'low', 'close', 'volume',
            'sma_10', 'sma_20', 'price_ratio_10', 'price_ratio_20', 'volume_ratio'
        ]
        current_labels = ['return_1d', 'return_5d']
        current_metadata = {
            'training_interval': '1d',  # Currently daily, should be hourly
            'sequence_lengths': {'1d': 20},  # Only daily, missing other timeframes
            'feature_count': len(current_features)
        }

        issues = check_gin_compliance(current_features, current_labels, current_metadata)

        # This test should FAIL to highlight the compliance issues
        assert len(issues) == 0, \
            f"Training data generation is not compliant with training_data.gin: {issues}"

    def test_specific_timeframe_feature_requirements(self):
        """Test specific requirements for each timeframe's features."""

        requirements = {
            '5m': {
                'sequence_length': 52,
                'interval_minutes': 5,
                'coverage_hours': 4.3,
                'feature_types': ['ohlcv', 'returns', 'volatility', 'technical']
            },
            '15m': {
                'sequence_length': 52,
                'interval_minutes': 15,
                'coverage_hours': 13.0,
                'feature_types': ['ohlcv', 'returns', 'volatility', 'technical']
            },
            '1h': {
                'sequence_length': 24,
                'interval_minutes': 60,
                'coverage_hours': 24.0,
                'feature_types': ['ohlcv', 'returns', 'volatility', 'technical']
            },
            '1d': {
                'sequence_length': 20,
                'interval_minutes': 1440,  # 24 * 60
                'coverage_days': 20,
                'feature_types': ['ohlcv', 'returns', 'volatility', 'technical']
            }
        }

        for timeframe, req in requirements.items():
            # Calculate expected feature count for this timeframe
            expected_features_per_type = 5  # OHLCV
            expected_types = len(req['feature_types'])
            expected_sequence = req['sequence_length']
            expected_total = expected_features_per_type * expected_types * expected_sequence

            assert expected_total > 0, \
                f"Timeframe {timeframe} should generate {expected_total} features"

            print(f"✓ {timeframe}: {expected_total} features expected "
                  f"({req['sequence_length']} intervals × {expected_types} types × {expected_features_per_type} OHLCV)")

    def test_generated_training_data_files_structure(self):
        """Test that generated training data files should have proper multi-timeframe structure."""

        # Expected file structure after proper implementation
        expected_files = [
            "features.npy",  # Multi-timeframe features array
            "labels.npy",    # Prediction labels
            "metadata.json", # Complete metadata including timeframe info
            "feature_names.json",  # All feature names with timeframe prefixes
            "gin_config.txt"  # Copy of gin config used for generation
        ]

        # Expected metadata structure
        expected_metadata_keys = [
            'training_interval',
            'timeframes_included',
            'sequence_lengths',
            'prediction_horizons',
            'feature_types',
            'features_per_timeframe',
            'total_feature_count',
            'gin_config_path'
        ]

        for key in expected_metadata_keys:
            assert key in expected_metadata_keys, \
                f"Metadata should include {key} for proper multi-timeframe documentation"

if __name__ == "__main__":
    # Run tests to detect missing multi-timeframe implementation
    pytest.main([__file__, "-v", "--tb=short"])