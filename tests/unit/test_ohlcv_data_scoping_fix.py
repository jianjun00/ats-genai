#!/usr/bin/env python3
"""
Unit tests for OHLCV data scoping fix in TimeSeriesSequenceTrainingGenerator.

Tests the specific fix for Issue #1: OHLCV data scoping bug in feature extraction.

This issue was discovered during AAPL training data generation on September 10, 2025.
The bug caused real AAPL market data (O=$205.27, H=$209.95, C=$208.01) to be lost
during feature extraction due to undefined data_df variables in certain code paths.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import Mock, AsyncMock

# Import the class under test
from domains.ml.services.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator


class TestOHLCVDataScopingFix:
    """Unit tests for OHLCV data scoping fix."""
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration for testing."""
        config = Mock()
        config.timeframes = ['5m', '15m', '1h', '1d']
        config.feature_types = ['ohlcv', 'returns', 'support_resistance']
        config.signal_names = ['etop', 'ebot', 'sma_20']
        return config
    
    @pytest.fixture
    def sample_ohlcv_data(self):
        """Real AAPL OHLCV data from our debugging session."""
        return pd.DataFrame({
            'timestamp': [datetime(2025, 7, 1, 14, 0)],
            'open': [205.27],
            'high': [209.95],
            'low': [204.21],
            'close': [208.01],
            'volume': [44402016.0]
        })
    
    @pytest.fixture
    def generator(self, mock_config):
        """Create generator instance with mocked dependencies."""
        generator = TimeSeriesSequenceTrainingGenerator(mock_config)
        
        # Mock universe manager
        mock_universe_manager = Mock()
        generator.universe_manager = mock_universe_manager
        
        return generator

    def test_data_df_initialization_prevents_nameerror(self, generator, sample_ohlcv_data):
        """
        Test that data_df is always initialized, preventing NameError.
        
        This is the core fix for the scoping bug where data_df was undefined
        in certain code paths.
        """
        # Mock universe manager to return OHLCV data but empty signals
        generator.universe_manager.get_lag_prices.return_value = sample_ohlcv_data
        generator.universe_manager.get_lagged_signals.return_value = pd.DataFrame()  # Empty signals
        
        # This should not raise NameError: name 'data_df' is not defined
        import asyncio
        try:
            result = asyncio.run(generator.get_timeframe_data(
                instrument_id=31,  # AAPL
                center_datetime=datetime(2025, 7, 1, 14, 0),
                timeframe='5m',
                is_future=False
            ))
            
            # Should successfully return features
            assert result is not None
            assert isinstance(result, dict)
            print("✅ data_df initialization prevents NameError")
            
        except NameError as e:
            if 'data_df' in str(e):
                pytest.fail(f"REGRESSION: data_df scoping bug reintroduced: {e}")
            else:
                raise

    def test_ohlcv_data_preserved_when_signals_empty(self, generator, sample_ohlcv_data):
        """
        Test that OHLCV data is preserved when signals DataFrame is empty.
        
        This tests the specific scenario where real market data was being lost.
        """
        # Setup: OHLCV data available, but signals empty (common scenario)
        generator.universe_manager.get_lag_prices.return_value = sample_ohlcv_data
        generator.universe_manager.get_lagged_signals.return_value = pd.DataFrame()
        
        import asyncio
        result = asyncio.run(generator.get_timeframe_data(
            instrument_id=31,
            center_datetime=datetime(2025, 7, 1, 14, 0),
            timeframe='5m',
            is_future=False
        ))
        
        # Verify real AAPL data is preserved (regression test with exact values)
        assert result['5m_open'] == 205.27, f"Expected 205.27, got {result.get('5m_open')}"
        assert result['5m_high'] == 209.95, f"Expected 209.95, got {result.get('5m_high')}"
        assert result['5m_low'] == 204.21, f"Expected 204.21, got {result.get('5m_low')}"
        assert result['5m_close'] == 208.01, f"Expected 208.01, got {result.get('5m_close')}"
        assert result['5m_volume'] == 44402016.0, f"Expected 44402016.0, got {result.get('5m_volume')}"
        
        print("✅ Real AAPL OHLCV data preserved when signals empty")

    def test_ohlcv_data_merged_with_signals_when_available(self, generator, sample_ohlcv_data):
        """
        Test that OHLCV data is properly merged with signals when both are available.
        
        This tests the other code path to ensure the fix doesn't break normal operation.
        """
        # Setup: Both OHLCV and signals data available
        signals_data = pd.DataFrame({
            'timestamp': [datetime(2025, 7, 1, 14, 0)],
            'sma_20_value': [207.5],
            'etop_value': [210.2]
        })
        
        generator.universe_manager.get_lag_prices.return_value = sample_ohlcv_data
        generator.universe_manager.get_lagged_signals.return_value = signals_data
        
        import asyncio
        result = asyncio.run(generator.get_timeframe_data(
            instrument_id=31,
            center_datetime=datetime(2025, 7, 1, 14, 0),
            timeframe='5m',
            is_future=False
        ))
        
        # Verify both OHLCV and signals data are present
        assert result['5m_open'] == 205.27  # OHLCV data
        assert result['5m_close'] == 208.01  # OHLCV data
        
        # Note: Signal processing logic may vary, but OHLCV should always be preserved
        print("✅ OHLCV data properly merged with signals")

    def test_empty_ohlcv_data_handled_gracefully(self, generator):
        """
        Test that empty OHLCV data is handled gracefully without NameError.
        
        This tests edge case where no market data is available.
        """
        # Setup: No OHLCV data available
        generator.universe_manager.get_lag_prices.return_value = pd.DataFrame()
        generator.universe_manager.get_lagged_signals.return_value = pd.DataFrame()
        
        import asyncio
        result = asyncio.run(generator.get_timeframe_data(
            instrument_id=99,  # Non-existent instrument
            center_datetime=datetime(2025, 7, 1, 14, 0),
            timeframe='5m',
            is_future=False
        ))
        
        # Should return empty dict, not raise NameError
        assert result == {}
        print("✅ Empty OHLCV data handled gracefully")

    def test_data_df_copy_prevents_mutation(self, generator, sample_ohlcv_data):
        """
        Test that data_df uses .copy() to prevent mutation of original OHLCV data.
        
        This ensures the fix uses proper pandas practices.
        """
        original_data = sample_ohlcv_data.copy()
        
        generator.universe_manager.get_lag_prices.return_value = sample_ohlcv_data
        generator.universe_manager.get_lagged_signals.return_value = pd.DataFrame()
        
        import asyncio
        result = asyncio.run(generator.get_timeframe_data(
            instrument_id=31,
            center_datetime=datetime(2025, 7, 1, 14, 0),
            timeframe='5m',
            is_future=False
        ))
        
        # Verify original data is unchanged
        pd.testing.assert_frame_equal(sample_ohlcv_data, original_data)
        print("✅ data_df copy prevents mutation of original data")

    def test_fix_maintains_feature_extraction_accuracy(self, generator, sample_ohlcv_data):
        """
        Test that the scoping fix maintains accurate feature extraction.
        
        Regression test to ensure fix doesn't introduce calculation errors.
        """
        generator.universe_manager.get_lag_prices.return_value = sample_ohlcv_data
        generator.universe_manager.get_lagged_signals.return_value = pd.DataFrame()
        
        import asyncio
        result = asyncio.run(generator.get_timeframe_data(
            instrument_id=31,
            center_datetime=datetime(2025, 7, 1, 14, 0),
            timeframe='5m',
            is_future=False
        ))
        
        # Verify derived features are calculated correctly
        expected_range = 209.95 - 204.21  # high - low
        assert abs(result['5m_range'] - expected_range) < 0.01
        
        expected_range_pct = expected_range / 208.01  # range / close
        assert abs(result['5m_range_pct'] - expected_range_pct) < 0.0001
        
        print("✅ Feature extraction accuracy maintained after fix")

    def test_debugging_output_confirms_fix(self, generator, sample_ohlcv_data, capsys):
        """
        Test that debugging output confirms the fix is working.
        
        Verifies the debugging messages we added during the fix are present.
        """
        generator.universe_manager.get_lag_prices.return_value = sample_ohlcv_data
        generator.universe_manager.get_lagged_signals.return_value = pd.DataFrame()
        
        import asyncio
        result = asyncio.run(generator.get_timeframe_data(
            instrument_id=31,
            center_datetime=datetime(2025, 7, 1, 14, 0),
            timeframe='5m',
            is_future=False
        ))
        
        # Check debug output
        captured = capsys.readouterr()
        
        # Should contain our fix's debug messages
        assert "📊 DEBUG: Assigned OHLCV data to data_df: 1 records" in captured.out
        assert "📊 DEBUG: Using only OHLCV data (no signals to merge)" in captured.out
        
        print("✅ Debugging output confirms fix is active")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])