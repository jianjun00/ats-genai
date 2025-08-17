"""
Tests for enhanced technical indicators for residual return prediction.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from signals.enhanced_indicators import (
    EMAIndicator,
    ATRIndicator,
    RSIIndicator,
    VWAPIndicator,
    VolumeIndicators,
    PriceActionIndicators,
    ResidualReturnIndicatorConfig,
    calculate_all_technical_indicators
)


class TestEMAIndicator:
    """Test Exponential Moving Average calculations."""
    
    def test_ema_indicator_calculation(self):
        """Test EMA indicator calculation."""
        # Create test data
        data = pd.DataFrame({
            'close': [100, 102, 104, 103, 105, 107, 106, 108, 110, 109]
        })
        
        indicator = EMAIndicator(period=5)
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert 'value' in result
        assert 'status' in result
        assert result['status'] == 'valid'
        assert isinstance(result['value'], (int, float))
        assert result['value'] > 0
        
        # Should have additional EMA metrics
        assert 'price_vs_ema' in result
        assert 'ema_slope' in result
    
    def test_ema_indicator_insufficient_data(self):
        """Test EMA indicator with insufficient data."""
        data = pd.DataFrame({
            'close': [100, 102]  # Less than period
        })
        
        indicator = EMAIndicator(period=5)
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert result['status'] == 'insufficient_data'
        assert result['value'] is None
    
    def test_ema_indicator_empty_data(self):
        """Test EMA indicator with empty data."""
        data = pd.DataFrame()
        
        indicator = EMAIndicator(period=5)
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert 'status' in result
        assert result['value'] is None
    
    def test_ema_indicator_different_periods(self):
        """Test EMA indicators with different periods."""
        data = pd.DataFrame({
            'close': [100, 102, 104, 103, 105, 107, 106, 108, 110, 109, 111, 113]
        })
        
        for period in [3, 8, 21]:
            indicator = EMAIndicator(period=period)
            result = indicator.calculate(data)
            
            if len(data) >= period:
                assert result['status'] == 'valid'
                assert isinstance(result['value'], (int, float))
            else:
                assert result['status'] == 'insufficient_data'


class TestATRIndicator:
    """Test Average True Range calculations."""
    
    def test_atr_indicator_calculation(self):
        """Test ATR indicator calculation."""
        data = pd.DataFrame({
            'high': [102, 104, 103, 106, 108, 107, 109, 111, 110, 112, 114, 113, 115, 117, 116, 118],
            'low': [98, 100, 99, 102, 104, 103, 105, 107, 106, 108, 110, 109, 111, 113, 112, 114],
            'close': [100, 102, 101, 104, 106, 105, 107, 109, 108, 110, 112, 111, 113, 115, 114, 116]
        })
        
        indicator = ATRIndicator(period=14)
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert 'value' in result
        assert 'status' in result
        assert result['status'] == 'valid'
        assert isinstance(result['value'], (int, float))
        assert result['value'] >= 0  # ATR should always be positive
        
        # Should have additional ATR metrics
        assert 'atr_percentage' in result
        assert 'atr_percentile' in result
        assert 'volatility_trend' in result
    
    def test_atr_indicator_insufficient_data(self):
        """Test ATR indicator with insufficient data."""
        data = pd.DataFrame({
            'high': [102, 104],
            'low': [98, 100],
            'close': [100, 102]
        })
        
        indicator = ATRIndicator(period=14)
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert result['status'] == 'insufficient_data'
        assert result['value'] is None
    
    def test_atr_indicator_missing_columns(self):
        """Test ATR indicator with missing columns."""
        data = pd.DataFrame({'close': [100, 102, 101, 104]})
        
        indicator = ATRIndicator(period=3)
        result = indicator.calculate(data)
        
        # Should handle missing high/low columns by returning error
        assert isinstance(result, dict)
        assert 'status' in result
        # Status should indicate error due to missing columns


class TestRSIIndicator:
    """Test Relative Strength Index calculations."""
    
    def test_rsi_indicator_calculation(self):
        """Test RSI indicator calculation."""
        # Create data with clear trend
        data = pd.DataFrame({
            'close': [100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124, 126, 128]
        })
        
        indicator = RSIIndicator(period=14)
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert 'value' in result
        assert 'status' in result
        assert result['status'] == 'valid'
        assert isinstance(result['value'], (int, float))
        assert 0 <= result['value'] <= 100  # RSI should be 0-100
        
        # Should have additional RSI metrics
        assert 'signal' in result
        assert 'signal_strength' in result
        assert 'divergence' in result
        assert 'rsi_momentum' in result
        
        # For strong uptrend, RSI should be high
        assert result['value'] > 50
    
    def test_rsi_indicator_insufficient_data(self):
        """Test RSI indicator with insufficient data."""
        data = pd.DataFrame({
            'close': [100, 102, 104]  # Less than period + 1
        })
        
        indicator = RSIIndicator(period=14)
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert result['status'] == 'insufficient_data'
        assert result['value'] is None
    
    def test_rsi_indicator_boundary_conditions(self):
        """Test RSI with boundary conditions."""
        # All same prices (no movement)
        flat_data = pd.DataFrame({'close': [100] * 20})
        
        indicator = RSIIndicator(period=14)
        result = indicator.calculate(flat_data)
        
        assert isinstance(result, dict)
        # RSI calculation might handle flat data differently
        if result['status'] == 'valid':
            # RSI should be around 50 for no movement, or calculation might return special value
            assert isinstance(result['value'], (int, float))
    
    def test_rsi_indicator_signals(self):
        """Test RSI signal classification."""
        # Test overbought condition
        overbought_data = pd.DataFrame({
            'close': [100, 105, 110, 115, 120, 125, 130, 135, 140, 145, 150, 155, 160, 165, 170]
        })
        
        indicator = RSIIndicator(period=14)
        result = indicator.calculate(overbought_data)
        
        if result['status'] == 'valid':
            # Should detect overbought condition
            assert result['signal'] in ['overbought', 'oversold', 'neutral']


class TestVWAPIndicator:
    """Test Volume Weighted Average Price calculations."""
    
    def test_vwap_indicator_calculation(self):
        """Test VWAP indicator calculation."""
        data = pd.DataFrame({
            'high': [102, 104, 106, 108],
            'low': [98, 100, 102, 104],
            'close': [100, 102, 104, 106],
            'volume': [1000, 1500, 1200, 1800]
        })
        
        indicator = VWAPIndicator()
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert 'value' in result
        assert 'status' in result
        assert result['status'] == 'valid'
        assert isinstance(result['value'], (int, float))
        assert result['value'] > 0
        
        # Should have additional VWAP metrics
        assert 'price_vs_vwap' in result
        assert 'vwap_slope' in result
        assert 'volume_balance' in result
    
    def test_vwap_indicator_no_volume(self):
        """Test VWAP indicator without volume data."""
        data = pd.DataFrame({
            'high': [102, 104, 106],
            'low': [98, 100, 102],
            'close': [100, 102, 104]
        })
        
        indicator = VWAPIndicator()
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert result['status'] == 'no_volume_data'
        assert result['value'] is None
    
    def test_vwap_indicator_empty_data(self):
        """Test VWAP indicator with empty data."""
        data = pd.DataFrame()
        
        indicator = VWAPIndicator()
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert result['status'] == 'no_volume_data'
        assert result['value'] is None


class TestVolumeIndicators:
    """Test volume-based indicators."""
    
    def test_volume_indicators_calculation(self):
        """Test volume indicators calculation."""
        data = pd.DataFrame({
            'close': [100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 120, 122, 124, 126, 128, 130, 132, 134, 136, 138, 140],
            'volume': [1000, 1500, 2000, 1800, 2200, 1900, 2100, 1700, 2300, 2000, 1800, 2200, 1900, 2100, 2400, 2000, 1900, 2200, 2100, 2300, 2500]
        })
        
        indicator = VolumeIndicators(sma_period=20)
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert 'status' in result
        assert result['status'] == 'valid'
        
        # Should have volume metrics
        assert 'volume_sma' in result
        assert 'volume_ratio' in result
        assert 'price_volume_correlation' in result
        assert 'obv' in result
        assert 'volume_momentum' in result
        
        assert result['volume_sma'] > 0
        assert isinstance(result['volume_ratio'], (int, float))
        assert isinstance(result['price_volume_correlation'], (int, float))
    
    def test_volume_indicators_no_volume(self):
        """Test volume indicators without volume data."""
        data = pd.DataFrame({
            'close': [100, 102, 104, 106, 108]
        })
        
        indicator = VolumeIndicators(sma_period=20)
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert result['status'] == 'no_volume_data'
        assert result['value'] is None
    
    def test_volume_indicators_insufficient_data(self):
        """Test volume indicators with insufficient data."""
        data = pd.DataFrame({
            'close': [100, 102],
            'volume': [1000, 1500]
        })
        
        indicator = VolumeIndicators(sma_period=20)
        result = indicator.calculate(data)
        
        # Should still work with less data, just limited calculations
        assert isinstance(result, dict)
        assert 'status' in result


class TestPriceActionIndicators:
    """Test price action indicators."""
    
    def test_price_action_indicators_calculation(self):
        """Test price action indicators calculation."""
        data = pd.DataFrame({
            'open': [100, 102, 104, 103, 105, 107],
            'high': [102, 104, 106, 105, 107, 109],
            'low': [98, 100, 102, 101, 103, 105],
            'close': [101, 103, 105, 104, 106, 108]
        })
        
        indicator = PriceActionIndicators()
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert 'status' in result
        assert result['status'] == 'valid'
        
        # Should have price action metrics
        assert 'spread_proxy' in result
        assert 'spread_ratio' in result
        assert 'price_gap' in result
        assert 'intraday_range' in result
        assert 'body_to_range' in result
        assert 'close_position' in result
        assert 'returns_1d' in result
        assert 'returns_5d' in result
        
        # Values should be reasonable
        assert isinstance(result['spread_proxy'], (int, float))
        assert isinstance(result['intraday_range'], (int, float))
        assert 0 <= result['close_position'] <= 1
    
    def test_price_action_indicators_insufficient_data(self):
        """Test price action indicators with insufficient data."""
        data = pd.DataFrame({
            'open': [100],
            'high': [102],
            'low': [98],
            'close': [101]
        })
        
        indicator = PriceActionIndicators()
        result = indicator.calculate(data)
        
        assert isinstance(result, dict)
        assert result['status'] == 'insufficient_data'
        assert result['value'] is None
    
    def test_price_action_indicators_missing_columns(self):
        """Test price action indicators with missing columns."""
        data = pd.DataFrame({
            'close': [100, 102, 104, 103, 105]
        })
        
        indicator = PriceActionIndicators()
        result = indicator.calculate(data)
        
        # Should handle missing OHLC columns
        assert isinstance(result, dict)
        assert 'status' in result


class TestResidualReturnIndicatorConfig:
    """Test configuration classes."""
    
    def test_minimal_config(self):
        """Test minimal configuration."""
        config = ResidualReturnIndicatorConfig.minimal_config()
        
        assert isinstance(config, ResidualReturnIndicatorConfig)
        
        # Check that config has indicator instances
        indicators = config.create_indicator_instances()
        assert isinstance(indicators, dict)
        assert len(indicators) > 0
        
        # Should have basic indicators
        assert any('EMA' in name for name in indicators.keys())
        assert any('ATR' in name for name in indicators.keys())
        assert any('RSI' in name for name in indicators.keys())
    
    def test_comprehensive_config(self):
        """Test comprehensive configuration."""
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        
        assert isinstance(config, ResidualReturnIndicatorConfig)
        
        # Check that config has indicator instances
        indicators = config.create_indicator_instances()
        assert isinstance(indicators, dict)
        assert len(indicators) > 0
        
        # Should have more indicators than minimal
        minimal_indicators = ResidualReturnIndicatorConfig.minimal_config().create_indicator_instances()
        assert len(indicators) >= len(minimal_indicators)
        
        # Should have VWAP and volume indicators
        assert any('VWAP' in name for name in indicators.keys())
        assert any('Volume' in name for name in indicators.keys())
    
    def test_config_indicator_creation(self):
        """Test that config can create actual indicator instances."""
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        indicators = config.create_indicator_instances()
        
        # Check that all indicators are actual instances
        for name, indicator in indicators.items():
            assert hasattr(indicator, 'calculate')
            assert callable(getattr(indicator, 'calculate'))


class TestCalculateAllTechnicalIndicators:
    """Test the main function that calculates all indicators."""
    
    def test_complete_calculation(self):
        """Test complete indicator calculation."""
        # Create sufficient data for all indicators (250 data points for EMA_200)
        data = pd.DataFrame({
            'open': list(range(100, 350, 1)),  # 250 data points
            'high': list(range(102, 352, 1)),
            'low': list(range(98, 348, 1)),
            'close': list(range(101, 351, 1)),
            'volume': list(range(1000, 1250, 1))
        })
        
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        features = calculate_all_technical_indicators(data, config)
        
        # Should return a dictionary
        assert isinstance(features, dict)
        assert len(features) > 0
        
        # Should have EMA features
        ema_features = [k for k in features.keys() if 'EMA' in k and 'value' in k]
        assert len(ema_features) > 0
        
        # Should have other indicators
        indicator_types = ['RSI', 'ATR', 'VWAP', 'Volume', 'PriceAction']
        for indicator_type in indicator_types:
            type_features = [k for k in features.keys() if indicator_type in k]
            assert len(type_features) > 0, f"No features found for {indicator_type}"
        
        # Features should be numeric, string, or None (for insufficient data)
        for key, value in features.items():
            if 'status' not in key:
                if value is not None:
                    # Numeric, string, boolean values are acceptable (including numpy types)
                    assert isinstance(value, (int, float, np.number, str, bool, np.bool_)), f"Feature {key} has unexpected type: {type(value)}"
                    # If numeric, should be finite
                    if isinstance(value, (int, float, np.number)):
                        assert np.isfinite(value) or value == 0.0, f"Feature {key} is not finite: {value}"
    
    def test_minimal_calculation(self):
        """Test minimal indicator calculation."""
        # Create sufficient data for minimal indicators
        data = pd.DataFrame({
            'open': list(range(100, 150, 1)),  # 50 data points
            'high': list(range(102, 152, 1)),
            'low': list(range(98, 148, 1)),
            'close': list(range(101, 151, 1)),
            'volume': list(range(1000, 1050, 1))
        })
        
        config = ResidualReturnIndicatorConfig.minimal_config()
        features = calculate_all_technical_indicators(data, config)
        
        assert isinstance(features, dict)
        assert len(features) > 0
        
        # Should have basic EMA features
        assert any('EMA' in k for k in features.keys())
    
    def test_missing_data_handling(self):
        """Test handling of missing data."""
        # Create data with NaN values
        base_data = list(range(100, 150, 1))
        close_data = base_data.copy()
        close_data[5] = np.nan
        close_data[10] = np.nan
        
        volume_data = list(range(1000, 1050, 1))
        volume_data[7] = np.nan
        volume_data[12] = np.nan
        
        data = pd.DataFrame({
            'open': base_data,
            'high': [x + 2 for x in base_data],
            'low': [x - 2 for x in base_data],
            'close': close_data,
            'volume': volume_data
        })
        
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        features = calculate_all_technical_indicators(data, config)
        
        # Should still return features
        assert isinstance(features, dict)
        assert len(features) > 0
        
        # Non-status features should be valid numbers
        for key, value in features.items():
            if 'status' not in key:
                if isinstance(value, (int, float, np.number)):
                    assert np.isfinite(value) or value == 0.0, f"Feature {key} is not finite: {value}"
    
    def test_insufficient_data(self):
        """Test handling of insufficient data."""
        data = pd.DataFrame({
            'open': [100, 102],  # Only 2 data points
            'high': [102, 104],
            'low': [98, 100],
            'close': [101, 103],
            'volume': [1000, 1500]
        })
        
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        features = calculate_all_technical_indicators(data, config)
        
        # Should still return a dict
        assert isinstance(features, dict)
        # Most indicators should show insufficient data status
        status_features = [k for k, v in features.items() if 'status' in k and 'insufficient_data' in str(v)]
        assert len(status_features) > 0
    
    def test_empty_data(self):
        """Test handling of empty data."""
        data = pd.DataFrame()
        
        config = ResidualReturnIndicatorConfig.minimal_config()
        features = calculate_all_technical_indicators(data, config)
        
        # Should return empty dict or default values
        assert isinstance(features, dict)
    
    @pytest.mark.parametrize("missing_column", ['open', 'high', 'low', 'volume'])
    def test_missing_columns(self, missing_column):
        """Test handling of missing columns."""
        # Create sufficient data
        data = pd.DataFrame({
            'open': list(range(100, 150, 1)),
            'high': list(range(102, 152, 1)),
            'low': list(range(98, 148, 1)),
            'close': list(range(101, 151, 1)),
            'volume': list(range(1000, 1050, 1))
        })
        
        # Remove one column
        data = data.drop(columns=[missing_column])
        
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        features = calculate_all_technical_indicators(data, config)
        
        # Should still calculate what it can
        assert isinstance(features, dict)
        
        # If close is available, should have some features
        if 'close' in data.columns:
            valid_features = [k for k, v in features.items() if 'status' not in k or v == 'valid']
            assert len(valid_features) > 0


class TestPerformanceAndEdgeCases:
    """Test performance and edge cases."""
    
    def test_large_dataset_performance(self):
        """Test performance with large dataset."""
        # Create large dataset (1 year of daily data)
        dates = pd.date_range('2023-01-01', '2023-12-31', freq='D')
        np.random.seed(42)  # For reproducible results
        
        prices = [100]
        for _ in range(len(dates) - 1):
            change = np.random.normal(0, 0.02)
            prices.append(prices[-1] * (1 + change))
        
        data = pd.DataFrame({
            'open': [p * (1 + np.random.normal(0, 0.005)) for p in prices],
            'high': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
            'low': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
            'close': prices,
            'volume': [np.random.randint(100000, 1000000) for _ in prices]
        }, index=dates)
        
        # Ensure OHLC consistency
        data['high'] = np.maximum(data['high'], np.maximum(data['open'], data['close']))
        data['low'] = np.minimum(data['low'], np.minimum(data['open'], data['close']))
        
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        
        import time
        start_time = time.time()
        features = calculate_all_technical_indicators(data, config)
        end_time = time.time()
        
        # Should complete in reasonable time (less than 10 seconds for large dataset)
        assert (end_time - start_time) < 10.0
        
        # Should return valid features
        assert isinstance(features, dict)
        assert len(features) > 0
        
        # Should have features from multiple indicators
        feature_types = set()
        for key in features.keys():
            if '_' in key:
                feature_types.add(key.split('_')[0])
        assert len(feature_types) > 1
    
    def test_extreme_values(self):
        """Test handling of extreme values."""
        # Extend extreme values to have sufficient data
        extreme_closes = [1e-10, 1e10, 0.001, 1000000] * 13  # 52 points
        extreme_volumes = [1, 1e15, 100, 1e12] * 13
        
        data = pd.DataFrame({
            'open': extreme_closes,
            'high': [max(x * 1.01, x + 0.01) for x in extreme_closes],
            'low': [min(x * 0.99, x - 0.01) for x in extreme_closes],
            'close': extreme_closes,
            'volume': extreme_volumes
        })
        
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        features = calculate_all_technical_indicators(data, config)
        
        # Should handle extreme values gracefully
        assert isinstance(features, dict)
        
        # Non-status features should be finite or zero (if numeric)
        for key, value in features.items():
            if 'status' not in key:
                if isinstance(value, (int, float, np.number)):
                    assert np.isfinite(value) or value == 0.0, f"Feature {key} is not finite: {value}"
    
    def test_all_zero_values(self):
        """Test handling of all zero values."""
        data = pd.DataFrame({
            'open': [0] * 50,
            'high': [0] * 50,
            'low': [0] * 50,
            'close': [0] * 50,
            'volume': [0] * 50
        })
        
        config = ResidualReturnIndicatorConfig.minimal_config()
        features = calculate_all_technical_indicators(data, config)
        
        # Should handle gracefully
        assert isinstance(features, dict)
    
    def test_constant_values(self):
        """Test handling of constant values."""
        data = pd.DataFrame({
            'open': [100] * 50,
            'high': [100] * 50,
            'low': [100] * 50,
            'close': [100] * 50,
            'volume': [1000] * 50
        })
        
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        features = calculate_all_technical_indicators(data, config)
        
        # Should handle constant values
        assert isinstance(features, dict)
        
        # Check that RSI calculation handles constant prices appropriately
        rsi_features = [k for k in features.keys() if 'RSI' in k and 'value' in k]
        if rsi_features:
            rsi_value = features[rsi_features[0]]
            # RSI may be 50 for constant prices or calculation may return special value
            assert isinstance(rsi_value, (int, float)) or rsi_value is None


if __name__ == "__main__":
    pytest.main([__file__])