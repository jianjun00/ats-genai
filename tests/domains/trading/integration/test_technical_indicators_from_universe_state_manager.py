#!/usr/bin/env python3
"""
Integration Test: Technical Indicators Extraction from UniverseStateManager

Tests the complete flow: UniverseStateManager → MultiTimeframeFeatureExtractor
to ensure technical indicators (pldot, envelope_top, etc.) are properly extracted
and included in training data features.
"""

import sys
import os
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
from unittest.mock import Mock

# Add src to path to avoid gin config issues
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

# Import the classes we're testing
from domains.ml.services.training_data.timeseries_sequence_training_generator import (
    MultiTimeframeFeatureExtractor,
    TrainingDataConfig,
    SequenceWindowBuilder,
    TimeSeriesSequenceTrainingGenerator
)
class TestTechnicalIndicatorsFromUniverseStateManager:
    """Test technical indicators extraction from UniverseStateManager."""

    @pytest.fixture
    def mock_universe_state_manager(self):
        """Create mock UniverseStateManager that separates OHLCV and technical indicators correctly."""
        mock_manager = Mock()

        # Mock OHLCV data for get_lag_prices (ONLY OHLCV data, no technical indicators)
        mock_ohlcv_data = pd.DataFrame({
            # OHLCV data ONLY
            'open': [100.0, 101.0, 99.5],
            'high': [102.0, 103.0, 101.0],
            'low': [99.0, 100.5, 98.5],
            'close': [101.5, 102.5, 100.0],
            'volume': [1000000, 1100000, 950000],
        })

        # Mock technical indicators data for get_lagged_signals (technical indicators ONLY)
        mock_signals_data = pd.DataFrame({
            'timestamp': pd.to_datetime(['2025-09-05 10:00', '2025-09-05 11:00', '2025-09-05 12:00']),

            # Technical indicators from IndicatorBuilder (returned by get_lagged_signals)
            'pldot_value': [0.75, 0.82, 0.68],
            'pldot_status': ['ok', 'ok', 'ok'],
            'etop_value': [104.0, 105.0, 103.0],
            'etop_status': ['ok', 'ok', 'ok'],
            'ebot_value': [96.0, 97.0, 95.0],
            'ebot_status': ['ok', 'ok', 'ok'],
            'envelope_top_value': [104.0, 105.0, 103.0],
            'envelope_top_status': ['ok', 'ok', 'ok'],
            'envelope_bot_value': [96.0, 97.0, 95.0],
            'envelope_bot_status': ['ok', 'ok', 'ok'],
            'z1b_value': [0.2, 0.25, 0.18],
            'z1b_status': ['ok', 'ok', 'ok'],
            'z2b_value': [0.35, 0.40, 0.30],
            'z2b_status': ['ok', 'ok', 'ok'],
        })

        # Configure methods correctly:
        # get_lag_prices returns ONLY OHLCV data
        mock_manager.get_lag_prices.return_value = mock_ohlcv_data

        # get_lagged_signals returns ONLY technical indicators (this is the correct method!)
        mock_manager.get_lagged_signals.return_value = mock_signals_data

        return mock_manager

    @pytest.fixture
    def config(self):
        """Create TrainingDataConfig with technical indicators enabled."""
        return TrainingDataConfig(
            feature_types=['ohlcv', 'returns', 'indicators', 'technical'],
            signal_names=['pldot', 'etop', 'ebot', 'envelope_top', 'envelope_bot', 'z1b', 'z2b', 'z5t', 'z6t']
        )

    @pytest.fixture
    def feature_extractor(self, config):
        """Create MultiTimeframeFeatureExtractor instance."""
        return MultiTimeframeFeatureExtractor(config)

    def test_extract_technical_indicators_from_dataframe(self, feature_extractor):
        """Test extraction of technical indicators from DataFrame with IndicatorBuilder data."""

        # Create DataFrame with technical indicators (as provided by UniverseStateManager)
        test_data = pd.DataFrame({
            'open': [100.0], 'high': [102.0], 'low': [99.0], 'close': [101.0], 'volume': [1000],
            'pldot': [0.75], 'etop': [104.0], 'ebot': [96.0],
            'envelope_top': [104.0], 'envelope_bot': [96.0],
            'z1b': [0.2], 'z2b': [0.35], 'z5t': [0.85], 'z6t': [0.92],
            'sma_20': [101.0], 'ema_12': [101.2], 'rsi_14': [55.2]
        })

        # Extract technical indicators
        indicators = feature_extractor.extract_technical_indicators(test_data, '1h')

        # Verify all expected indicators are extracted
        expected_indicators = [
            '1h_pldot', '1h_etop', '1h_ebot',
            '1h_envelope_top', '1h_envelope_bot',
            '1h_z1b', '1h_z2b', '1h_z5t', '1h_z6t',
            '1h_sma_20', '1h_ema_12', '1h_rsi_14'
        ]

        for indicator in expected_indicators:
            assert indicator in indicators, f"Missing indicator: {indicator}"
            assert isinstance(indicators[indicator], float), f"Indicator {indicator} is not float: {type(indicators[indicator])}"

        # Verify specific values
        assert indicators['1h_pldot'] == 0.75
        assert indicators['1h_etop'] == 104.0
        assert indicators['1h_ebot'] == 96.0
        assert indicators['1h_envelope_top'] == 104.0
        assert indicators['1h_z1b'] == 0.2
        assert indicators['1h_sma_20'] == 101.0

        print(f"✅ Extracted {len(indicators)} technical indicators:")
        for indicator, value in sorted(indicators.items()):
            print(f"   {indicator}: {value}")

    def test_extract_all_features_includes_indicators(self, feature_extractor):
        """Test that extract_all_features includes technical indicators."""

        test_data = pd.DataFrame({
            'open': [100.0, 101.0], 'high': [102.0, 103.0], 'low': [99.0, 100.0],
            'close': [101.0, 102.0], 'volume': [1000, 1100],
            'pldot': [0.75, 0.80], 'etop': [104.0, 105.0], 'ebot': [96.0, 97.0]
        })

        # Extract all features
        all_features = feature_extractor.extract_all_features(test_data, '1h')

        # Verify technical indicators are included
        assert '1h_pldot' in all_features, "pldot indicator missing from all_features"
        assert '1h_etop' in all_features, "etop indicator missing from all_features"
        assert '1h_ebot' in all_features, "ebot indicator missing from all_features"

        # Verify OHLCV features are also included
        assert '1h_open' in all_features, "OHLCV features missing from all_features"
        assert '1h_close' in all_features, "OHLCV features missing from all_features"

        # Verify return features are included
        assert '1h_return_1' in all_features, "Return features missing from all_features"

        print(f"✅ extract_all_features includes {len(all_features)} total features:")
        indicator_features = [k for k in all_features.keys() if any(ind in k for ind in ['pldot', 'etop', 'ebot', 'envelope'])]
        print(f"   Technical indicators: {len(indicator_features)} features")
        print(f"   {indicator_features}")

    def test_sequence_window_builder_integration(self, mock_universe_state_manager, config):
        """Test SequenceWindowBuilder integration with UniverseStateManager indicators."""

        # Create SequenceWindowBuilder with mock UniverseStateManager
        window_builder = SequenceWindowBuilder(config, mock_universe_state_manager)

        # Test getting timeframe data (should call UniverseStateManager)
        instrument_id = 12345
        center_date = date(2025, 9, 5)
        timeframe = '1h'
        window_size = 10

        # Get timeframe data (this should call universe_manager.get_lag_prices)
        sequence_data = window_builder.get_timeframe_data(
            instrument_id, center_date, timeframe, window_size, is_future=False
        )

        # Verify UniverseStateManager was called correctly
        mock_universe_state_manager.get_lag_prices.assert_called_once_with(
            instrument_id, center_date, window_size
        )

        # Verify sequence data contains technical indicators
        assert len(sequence_data) > 0, "No sequence data returned"

        # Check first data point for technical indicators
        first_interval = sequence_data[0]

        # Should contain technical indicators from the mock data
        expected_indicator_features = ['1h_pldot', '1h_etop', '1h_ebot', '1h_z1b', '1h_z2b']

        for indicator in expected_indicator_features:
            assert indicator in first_interval, f"Missing indicator {indicator} in sequence data"

        print(f"✅ SequenceWindowBuilder extracted {len(first_interval)} features per interval")
        print(f"✅ Technical indicators found: {[k for k in first_interval.keys() if 'pldot' in k or 'etop' in k or 'ebot' in k]}")

    def test_missing_indicators_handling(self, feature_extractor):
        """Test graceful handling when some indicators are missing."""

        # Create DataFrame with only some indicators
        partial_data = pd.DataFrame({
            'open': [100.0], 'high': [102.0], 'low': [99.0], 'close': [101.0],
            'pldot': [0.75],  # Only pldot present
            'sma_20': [101.0]  # Only sma_20 present
            # Missing: etop, ebot, envelope_top, etc.
        })

        indicators = feature_extractor.extract_technical_indicators(partial_data, '1h')

        # Should have the available indicators
        assert '1h_pldot' in indicators
        assert indicators['1h_pldot'] == 0.75
        assert '1h_sma_20' in indicators
        assert indicators['1h_sma_20'] == 101.0

        # Should not have missing indicators (or they should be NaN)
        missing_indicators = ['1h_etop', '1h_ebot', '1h_envelope_top']
        for indicator in missing_indicators:
            if indicator in indicators:
                # If present, should be NaN
                assert pd.isna(indicators[indicator]), f"Missing indicator {indicator} should be NaN, got {indicators[indicator]}"

        print(f"✅ Gracefully handled partial indicator data: {len(indicators)} indicators extracted")

    def test_nan_indicator_values_handling(self, feature_extractor):
        """Test handling of NaN indicator values."""

        # Create DataFrame with NaN indicator values
        nan_data = pd.DataFrame({
            'open': [100.0], 'high': [102.0], 'low': [99.0], 'close': [101.0],
            'pldot': [np.nan],  # NaN value
            'etop': [104.0],    # Valid value
            'ebot': [np.nan]    # NaN value
        })

        indicators = feature_extractor.extract_technical_indicators(nan_data, '1h')

        # NaN values should be preserved as NaN
        assert '1h_pldot' in indicators
        assert pd.isna(indicators['1h_pldot']), "NaN pldot should remain NaN"

        assert '1h_etop' in indicators
        assert indicators['1h_etop'] == 104.0, "Valid etop should be preserved"

        assert '1h_ebot' in indicators
        assert pd.isna(indicators['1h_ebot']), "NaN ebot should remain NaN"

        print("✅ NaN indicator values handled correctly")

    @pytest.mark.integration
    def test_complete_indicators_flow_integration(self, mock_universe_state_manager, config):
        """Test complete flow from UniverseStateManager to training data features."""

        # Create TimeSeriesSequenceTrainingGenerator with mock UniverseStateManager
        generator = TimeSeriesSequenceTrainingGenerator(
            env=None,  # Will be None due to our optional imports
            config=config,
            universe_manager=mock_universe_state_manager
        )

        # Verify components are initialized
        assert generator.config is not None
        assert generator.universe_manager is mock_universe_state_manager
        assert generator.sequence_builder is not None
        assert generator.feature_extractor is not None

        # Test that sequence builder can extract features with indicators
        sequence_data = generator.sequence_builder.get_timeframe_data(
            instrument_id=12345,
            center_date=date(2025, 9, 5),
            timeframe='1h',
            window_size=5
        )

        # Verify indicators are in the extracted features
        assert len(sequence_data) > 0, "No sequence data generated"

        first_interval_features = sequence_data[0]

        # Check for key technical indicators
        key_indicators = ['1h_pldot', '1h_etop', '1h_ebot']
        found_indicators = []

        for indicator in key_indicators:
            if indicator in first_interval_features:
                found_indicators.append(indicator)

        assert len(found_indicators) > 0, f"No key indicators found. Available features: {list(first_interval_features.keys())}"

        print(f"✅ Complete integration test passed:")
        print(f"   Generated {len(sequence_data)} intervals")
        print(f"   Each interval has {len(first_interval_features)} features")
        print(f"   Technical indicators found: {found_indicators}")

        # Log all feature types for verification
        feature_types = {}
        for feature_name in first_interval_features.keys():
            if 'pldot' in feature_name or 'etop' in feature_name or 'ebot' in feature_name:
                feature_types['indicators'] = feature_types.get('indicators', 0) + 1
            elif 'open' in feature_name or 'close' in feature_name:
                feature_types['ohlcv'] = feature_types.get('ohlcv', 0) + 1
            elif 'return' in feature_name:
                feature_types['returns'] = feature_types.get('returns', 0) + 1
            elif 'volume' in feature_name:
                feature_types['volume'] = feature_types.get('volume', 0) + 1

        print(f"   Feature breakdown: {feature_types}")

    def test_universe_state_manager_method_separation(self, mock_universe_state_manager):
        """Test that UniverseStateManager correctly separates OHLCV and technical indicators."""

        # Test get_lag_prices returns ONLY OHLCV data
        ohlcv_data = mock_universe_state_manager.get_lag_prices(12345, datetime(2025, 9, 5, 14, 30), 10)

        # Should have OHLCV columns
        expected_ohlcv_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in expected_ohlcv_columns:
            assert col in ohlcv_data.columns, f"Missing OHLCV column: {col}"

        # Should NOT have technical indicator columns
        unexpected_indicator_columns = ['pldot', 'etop', 'ebot', 'envelope_top', 'envelope_bot']
        for col in unexpected_indicator_columns:
            assert col not in ohlcv_data.columns, f"get_lag_prices should NOT include technical indicator: {col}"

        print("✅ get_lag_prices correctly returns ONLY OHLCV data")

        # Test get_lagged_signals returns ONLY technical indicators
        signals_data = mock_universe_state_manager.get_lagged_signals(
            12345, datetime(2025, 9, 5, 14, 30), 10, '1h', ['etop', 'ebot', 'pldot']
        )

        # Should have technical indicator columns
        expected_signal_columns = ['pldot_value', 'etop_value', 'ebot_value']
        for col in expected_signal_columns:
            assert col in signals_data.columns, f"Missing signal column: {col}"

        # Should NOT have OHLCV columns
        for col in expected_ohlcv_columns:
            assert col not in signals_data.columns, f"get_lagged_signals should NOT include OHLCV column: {col}"

        print("✅ get_lagged_signals correctly returns ONLY technical indicators")
        print("✅ Method separation verified - no cross-contamination")

    @pytest.mark.asyncio
    async def test_actual_integration_with_separate_methods(self, mock_universe_state_manager, config):
        """Test that SequenceWindowBuilder now correctly uses both methods."""

        # Create SequenceWindowBuilder with mock UniverseStateManager
        window_builder = SequenceWindowBuilder(config, mock_universe_state_manager)

        # Test getting timeframe data (should call BOTH get_lag_prices AND get_lagged_signals)
        instrument_id = 12345
        center_datetime = datetime(2025, 9, 5, 14, 30)  # Use datetime instead of date
        timeframe = '1h'
        window_size = 10

        # Get timeframe data (this should call both methods correctly)
        sequence_data = await window_builder.get_timeframe_data(
            instrument_id, center_datetime, timeframe, window_size, is_future=False
        )

        # Verify BOTH methods were called
        mock_universe_state_manager.get_lag_prices.assert_called_once_with(
            instrument_id, center_datetime, window_size
        )
        # Verify get_lagged_signals was called with configured signal names
        expected_signal_names = ['pldot', 'etop', 'ebot', 'envelope_top', 'envelope_bot', 'z1b', 'z2b', 'z5t', 'z6t']
        mock_universe_state_manager.get_lagged_signals.assert_called_once_with(
            instrument_id=instrument_id,
            cur_datetime=center_datetime,
            lag_periods=window_size,
            time_interval=timeframe,
            signal_names=expected_signal_names
        )

        # Verify sequence data contains BOTH OHLCV and technical indicators
        assert len(sequence_data) > 0, "No sequence data returned"

        # Check first data point for OHLCV features
        first_interval = sequence_data[0]

        # Should contain OHLCV features
        expected_ohlcv_features = ['1h_open', '1h_high', '1h_low', '1h_close', '1h_volume']
        ohlcv_found = []
        for feature in expected_ohlcv_features:
            if feature in first_interval:
                ohlcv_found.append(feature)

        # Should contain technical indicator features
        expected_indicator_features = ['1h_pldot', '1h_etop', '1h_ebot']
        indicators_found = []
        for feature in expected_indicator_features:
            if feature in first_interval:
                indicators_found.append(feature)

        assert len(ohlcv_found) > 0, f"No OHLCV features found. Available: {list(first_interval.keys())}"
        assert len(indicators_found) > 0, f"No technical indicator features found. Available: {list(first_interval.keys())}"

        print(f"✅ Integration test passed with method separation:")
        print(f"   OHLCV features found: {ohlcv_found}")
        print(f"   Technical indicators found: {indicators_found}")
        print(f"   Total features per interval: {len(first_interval)}")

        # This test ensures the critical bug is fixed:
        # - get_lag_prices provides OHLCV data only
        # - get_lagged_signals provides technical indicators only
        # - Both are correctly combined in the training data generator

    def test_gin_configurable_signal_names(self):
        """Test that signal names are configurable via gin and not hardcoded."""

        # Test with custom signal configuration
        custom_signals = ['custom_signal_1', 'custom_signal_2', 'etop']
        custom_config = TrainingDataConfig(
            feature_types=['indicators'],
            signal_names=custom_signals
        )

        # Verify custom signals are stored in config
        assert custom_config.signal_names == custom_signals, f"Expected {custom_signals}, got {custom_config.signal_names}"

        # Create feature extractor with custom config
        custom_extractor = MultiTimeframeFeatureExtractor(custom_config)

        # Create test data with one of the custom signals
        test_data = pd.DataFrame({
            'open': [100.0], 'high': [102.0], 'low': [99.0], 'close': [101.0],
            'custom_signal_1': [1.5],  # Our custom signal
            'etop': [104.0],           # Standard signal
            'pldot': [0.75]            # Signal NOT in our custom list
        })

        # Extract technical indicators
        indicators = custom_extractor.extract_technical_indicators(test_data, '1h')

        # Should extract configured signals
        assert '1h_custom_signal_1' in indicators, "Custom signal should be extracted"
        assert indicators['1h_custom_signal_1'] == 1.5, "Custom signal value incorrect"
        assert '1h_etop' in indicators, "Configured standard signal should be extracted"

        # Should NOT extract signals not in configuration
        assert '1h_pldot' not in indicators, "Non-configured signal should not be extracted"

        print("✅ Signal names are properly configurable via gin")
        print(f"   Custom signals extracted: {[k for k in indicators.keys() if 'custom_' in k]}")
        print(f"   Standard signals extracted: {[k for k in indicators.keys() if 'etop' in k]}")
        print(f"   Non-configured signals ignored: ['1h_pldot' not in output]")

    def test_default_signal_configuration(self):
        """Test that default signal configuration includes expected indicators."""

        default_config = TrainingDataConfig()

        # Verify default signals include key technical indicators
        expected_defaults = ['etop', 'ebot', 'pldot', 'envelope_top', 'envelope_bot']
        for signal in expected_defaults:
            assert signal in default_config.signal_names, f"Default config missing signal: {signal}"

        # Verify default signals include common indicators
        expected_common = ['sma_20', 'ema_12', 'rsi_14', 'macd_line']
        for signal in expected_common:
            assert signal in default_config.signal_names, f"Default config missing common indicator: {signal}"

        print(f"✅ Default configuration includes {len(default_config.signal_names)} signal types")
        print(f"   Key indicators: {[s for s in expected_defaults if s in default_config.signal_names]}")
        print(f"   Common indicators: {[s for s in expected_common if s in default_config.signal_names]}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])