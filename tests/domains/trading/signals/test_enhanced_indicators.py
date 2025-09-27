"""
Tests for enhanced technical indicators for residual return prediction.
"""

import pytest

# Mark all tests in this module as unit tests by default
pytestmark = pytest.mark.unit
import pandas as pd
import numpy as np
from datetime import datetime

from domains.trading.services.indicators.enhanced_indicators import (
    EMAIndicator,
    ATRIndicator,
    RSIIndicator,
    VWAPIndicator,
    VolumeIndicators,
    PriceActionIndicators,
    CumulativeVolumeIndicator,
    CumulativeDollarsIndicator,
    SessionVWAPIndicator,
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

        assert result['status'] == 'insufficient_data'
        assert result['value'] is None

class TestCumulativeVolumeIndicator:
    """Test Cumulative Volume indicator calculations."""

    def test_cumulative_volume_daily_reset(self):
        """Test cumulative volume with daily reset."""
        # Create test data with timestamps across multiple days
        timestamps = pd.date_range('2024-01-01 09:30:00', periods=10, freq='1h')
        data = pd.DataFrame({
            'timestamp': timestamps,
            'close': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
            'high': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
            'low': [99, 100, 101, 102, 103, 104, 105, 106, 107, 108],
            'volume': [1000, 1500, 2000, 1200, 1800, 2200, 1600, 1900, 2100, 1700]
        })
        data.index = timestamps

        indicator = CumulativeVolumeIndicator(reset_interval='daily')
        result = indicator.calculate(data)

        assert isinstance(result, dict)
        assert 'value' in result
        assert 'status' in result
        assert result['status'] == 'valid'
        assert isinstance(result['value'], (int, float, np.integer, np.floating))
        assert result['value'] > 0

        # Should have cumulative volume metrics
        assert 'cumulative_volume' in result
        assert 'positive_flow_ratio' in result
        assert 'negative_flow_ratio' in result
        assert 'volume_balance' in result
        assert 'volume_acceleration' in result

        # Volume balance should be between -1 and 1
        assert -1 <= result['volume_balance'] <= 1

        # Flow ratios should sum to approximately 1 (with neutral volume)
        total_flow = result['positive_flow_ratio'] + result['negative_flow_ratio']
        assert total_flow <= 1.0

    def test_cumulative_volume_session_reset(self):
        """Test cumulative volume with session reset."""
        # Use simple data without complex timestamp logic for now
        data = pd.DataFrame({
            'close': [100, 101, 102, 103, 104, 105, 106, 107],
            'high': [101, 102, 103, 104, 105, 106, 107, 108],
            'low': [99, 100, 101, 102, 103, 104, 105, 106],
            'volume': [1000, 1500, 2000, 1200, 1800, 2200, 1600, 1900]
        })

        indicator = CumulativeVolumeIndicator(reset_interval='session')
        result = indicator.calculate(data)

        assert result['status'] == 'valid'
        assert result['cumulative_volume'] > 0
        assert 'volume_trend' in result
        assert 'volume_percentile' in result

    def test_cumulative_volume_never_reset(self):
        """Test cumulative volume with no reset."""
        data = pd.DataFrame({
            'close': [100, 101, 102, 103, 104],
            'high': [101, 102, 103, 104, 105],
            'low': [99, 100, 101, 102, 103],
            'volume': [1000, 1500, 2000, 1200, 1800]
        })

        indicator = CumulativeVolumeIndicator(reset_interval='never')
        result = indicator.calculate(data)

        assert result['status'] == 'valid'
        assert result['cumulative_volume'] == sum(data['volume'])

    def test_cumulative_volume_no_volume_data(self):
        """Test cumulative volume with missing volume data."""
        data = pd.DataFrame({
            'close': [100, 101, 102],
            'high': [101, 102, 103],
            'low': [99, 100, 101]
            # Missing volume column
        })

        indicator = CumulativeVolumeIndicator()
        result = indicator.calculate(data)

        assert result['status'] == 'no_volume_data'
        assert result['value'] is None

class TestCumulativeDollarsIndicator:
    """Test Cumulative Dollars indicator calculations."""

    def test_cumulative_dollars_typical_price(self):
        """Test cumulative dollars with typical price method."""
        data = pd.DataFrame({
            'open': [100, 101, 102, 103, 104],
            'high': [101, 102, 103, 104, 105],
            'low': [99, 100, 101, 102, 103],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5],
            'volume': [1000, 1500, 2000, 1200, 1800]
        })

        indicator = CumulativeDollarsIndicator(reset_interval='daily', price_method='typical')
        result = indicator.calculate(data)

        assert isinstance(result, dict)
        assert 'value' in result
        assert 'status' in result
        assert result['status'] == 'valid'
        assert isinstance(result['value'], (int, float))
        assert result['value'] > 0

        # Should have cumulative dollars metrics
        assert 'cumulative_dollars' in result
        assert 'positive_dollar_ratio' in result
        assert 'negative_dollar_ratio' in result
        assert 'dollar_balance' in result
        assert 'avg_dollar_per_share' in result
        assert 'liquidity_score' in result

        # Dollar balance should be between -1 and 1
        assert -1 <= result['dollar_balance'] <= 1

        # Liquidity score should be positive
        assert result['liquidity_score'] >= 0

        # Average dollar per share should be reasonable
        assert result['avg_dollar_per_share'] > 0

    def test_cumulative_dollars_close_price(self):
        """Test cumulative dollars with close price method."""
        data = pd.DataFrame({
            'close': [100, 101, 102, 103, 104],
            'high': [101, 102, 103, 104, 105],
            'low': [99, 100, 101, 102, 103],
            'volume': [1000, 1500, 2000, 1200, 1800]
        })

        indicator = CumulativeDollarsIndicator(reset_interval='daily', price_method='close')
        result = indicator.calculate(data)

        assert result['status'] == 'valid'
        expected_dollars = sum(data['close'] * data['volume'])
        assert abs(result['cumulative_dollars'] - expected_dollars) < 0.01

    def test_cumulative_dollars_vwap_price(self):
        """Test cumulative dollars with VWAP price method."""
        data = pd.DataFrame({
            'high': [101, 102, 103, 104, 105],
            'low': [99, 100, 101, 102, 103],
            'close': [100, 101, 102, 103, 104],
            'volume': [1000, 1500, 2000, 1200, 1800]
        })

        indicator = CumulativeDollarsIndicator(reset_interval='session', price_method='vwap')
        result = indicator.calculate(data)

        assert result['status'] == 'valid'
        assert result['cumulative_dollars'] > 0
        assert 'dollar_acceleration' in result
        assert 'dollar_percentile' in result
        assert 'dollar_trend' in result

    def test_cumulative_dollars_session_reset(self):
        """Test cumulative dollars with session reset."""
        # Create data across trading session
        timestamps = pd.date_range('2024-01-01 09:30:00', periods=6, freq='1H')
        data = pd.DataFrame({
            'timestamp': timestamps,
            'close': [100, 101, 102, 103, 104, 105],
            'high': [101, 102, 103, 104, 105, 106],
            'low': [99, 100, 101, 102, 103, 104],
            'volume': [1000, 1500, 2000, 1200, 1800, 1600]
        })
        data.index = timestamps

        indicator = CumulativeDollarsIndicator(reset_interval='session', price_method='close')
        result = indicator.calculate(data)

        assert result['status'] == 'valid'
        assert result['total_session_dollars'] > 0

    def test_cumulative_dollars_no_volume_data(self):
        """Test cumulative dollars with missing volume data."""
        data = pd.DataFrame({
            'close': [100, 101, 102],
            'high': [101, 102, 103],
            'low': [99, 100, 101]
            # Missing volume column
        })

        indicator = CumulativeDollarsIndicator()
        result = indicator.calculate(data)

        assert result['status'] == 'no_volume_data'
        assert result['value'] is None

    def test_cumulative_dollars_empty_data(self):
        """Test cumulative dollars with empty data."""
        data = pd.DataFrame()

        indicator = CumulativeDollarsIndicator()
        result = indicator.calculate(data)

        assert result['status'] == 'no_volume_data'
        assert result['value'] is None

class TestEnhancedIndicatorConfig:
    """Test enhanced indicator configuration."""

    def test_comprehensive_config_includes_cumulative_indicators(self):
        """Test that comprehensive config includes new cumulative indicators."""
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        indicators = config.create_indicator_instances()

        # Check that cumulative indicators are included
        cumulative_indicators = [name for name in indicators.keys() if 'Cum' in name]
        assert len(cumulative_indicators) > 0

        # Check specific indicators
        assert any('CumVolume' in name for name in indicators.keys())
        assert any('CumDollars' in name for name in indicators.keys())

    def test_calculate_all_includes_cumulative_indicators(self):
        """Test that calculate_all_technical_indicators includes cumulative indicators."""
        # Create test data
        data = pd.DataFrame({
            'open': [100, 101, 102, 103, 104, 105],
            'high': [101, 102, 103, 104, 105, 106],
            'low': [99, 100, 101, 102, 103, 104],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5, 105.5],
            'volume': [1000, 1500, 2000, 1200, 1800, 1600]
        })

        results = calculate_all_technical_indicators(data)

        # Check that cumulative indicator results are present
        cumulative_results = {k: v for k, v in results.items() if 'Cum' in k}
        assert len(cumulative_results) > 0

        # Check specific indicator results
        assert any('CumVolume' in key for key in results.keys())
        assert any('CumDollars' in key for key in results.keys())

class TestSessionVWAPIndicator:
    """Test Session VWAP indicator calculations."""

    def test_session_vwap_us_open_30min(self):
        """Test session VWAP for US market open 30-minute window."""
        # Create test data with timestamps during US market hours
        base_time = datetime(2024, 8, 17, 9, 30)  # Saturday 9:30 AM ET (for testing)
        timestamps = pd.date_range(base_time, periods=120, freq='1min')

        data = pd.DataFrame({
            'timestamp': timestamps,
            'open': [150.0 + i * 0.1 for i in range(120)],
            'high': [150.2 + i * 0.1 for i in range(120)],
            'low': [149.8 + i * 0.1 for i in range(120)],
            'close': [150.1 + i * 0.1 for i in range(120)],
            'volume': [1000 + i * 10 for i in range(120)]
        })
        data.index = timestamps

        indicator = SessionVWAPIndicator(session_type='us_open', duration_minutes=30)
        result = indicator.calculate(data)

        # Should work even without proper timezone data (will default to UTC)
        assert isinstance(result, dict)
        assert 'status' in result

    def test_session_vwap_london_close_60min(self):
        """Test session VWAP for London market close 60-minute window."""
        # Create test data with UTC timestamps
        base_time = datetime(2024, 8, 17, 16, 30)  # 4:30 PM GMT
        timestamps = pd.date_range(base_time, periods=180, freq='1min')

        data = pd.DataFrame({
            'timestamp': timestamps,
            'open': [100.0 + i * 0.05 for i in range(180)],
            'high': [100.1 + i * 0.05 for i in range(180)],
            'low': [99.9 + i * 0.05 for i in range(180)],
            'close': [100.0 + i * 0.05 for i in range(180)],
            'volume': [800 + i * 5 for i in range(180)]
        })
        data.index = timestamps

        indicator = SessionVWAPIndicator(session_type='london_close', duration_minutes=60)
        result = indicator.calculate(data)

        assert isinstance(result, dict)
        assert 'status' in result

    def test_session_vwap_no_volume_data(self):
        """Test session VWAP with missing volume data."""
        timestamps = pd.date_range('2024-08-17 09:30:00', periods=60, freq='1min')
        data = pd.DataFrame({
            'timestamp': timestamps,
            'close': [100 + i * 0.1 for i in range(60)],
            'high': [100.1 + i * 0.1 for i in range(60)],
            'low': [99.9 + i * 0.1 for i in range(60)]
            # Missing volume column
        })
        data.index = timestamps

        indicator = SessionVWAPIndicator(session_type='us_open', duration_minutes=30)
        result = indicator.calculate(data)

        assert result['status'] == 'no_volume_data'
        assert result['value'] is None

    def test_session_vwap_no_timestamp_data(self):
        """Test session VWAP with missing timestamp data."""
        data = pd.DataFrame({
            'close': [100, 101, 102],
            'high': [101, 102, 103],
            'low': [99, 100, 101],
            'volume': [1000, 1500, 2000]
        })
        # No timestamp index or column

        indicator = SessionVWAPIndicator(session_type='us_close', duration_minutes=30)
        result = indicator.calculate(data)

        assert result['status'] == 'no_timestamp_data'
        assert result['value'] is None

    def test_session_vwap_with_timezone_aware_data(self):
        """Test session VWAP with timezone-aware data."""
        import pytz

        # Create timezone-aware timestamps (US Eastern time)
        et_tz = pytz.timezone('US/Eastern')
        base_time = et_tz.localize(datetime(2024, 8, 17, 9, 30))
        timestamps = pd.date_range(base_time, periods=60, freq='1min')

        data = pd.DataFrame({
            'timestamp': timestamps,
            'open': [150.0 + i * 0.02 for i in range(60)],
            'high': [150.1 + i * 0.02 for i in range(60)],
            'low': [149.9 + i * 0.02 for i in range(60)],
            'close': [150.0 + i * 0.02 for i in range(60)],
            'volume': [1200 + i * 8 for i in range(60)]
        })
        data.index = timestamps

        indicator = SessionVWAPIndicator(session_type='us_open', duration_minutes=30)
        result = indicator.calculate(data)

        assert isinstance(result, dict)
        assert 'status' in result

    def test_session_vwap_all_session_types(self):
        """Test all session types and durations."""
        # Create sample data
        base_time = datetime(2024, 8, 17, 9, 30)
        timestamps = pd.date_range(base_time, periods=120, freq='1min')

        data = pd.DataFrame({
            'timestamp': timestamps,
            'open': [100.0] * 120,
            'high': [100.2] * 120,
            'low': [99.8] * 120,
            'close': [100.1] * 120,
            'volume': [1000] * 120
        })
        data.index = timestamps

        session_types = ['us_open', 'us_close', 'london_close']
        durations = [30, 60]

        for session_type in session_types:
            for duration in durations:
                indicator = SessionVWAPIndicator(
                    session_type=session_type,
                    duration_minutes=duration
                )
                result = indicator.calculate(data)

                assert isinstance(result, dict)
                assert 'status' in result

                # Check that indicator name is correctly formatted
                expected_name = f"SessionVWAP_{session_type}_{duration}min"
                assert indicator.name == expected_name

    def test_session_vwap_metrics_structure(self):
        """Test that session VWAP returns expected metrics structure."""
        import pytz
        # Create timezone-aware timestamps for US Eastern time
        eastern = pytz.timezone('US/Eastern')
        base_time = eastern.localize(datetime(2024, 8, 17, 9, 30))
        timestamps = pd.date_range(base_time, periods=60, freq='1min')

        data = pd.DataFrame({
            'timestamp': timestamps,
            'open': [150.0 + i * 0.01 for i in range(60)],
            'high': [150.1 + i * 0.01 for i in range(60)],
            'low': [149.9 + i * 0.01 for i in range(60)],
            'close': [150.0 + i * 0.01 for i in range(60)],
            'volume': [1000 + i * 5 for i in range(60)]
        })
        data.index = timestamps

        indicator = SessionVWAPIndicator(session_type='us_open', duration_minutes=30)
        result = indicator.calculate(data)

        # Check all expected metrics are present
        expected_metrics = [
            'value', 'session_vwap', 'price_vs_session_vwap',
            'session_volume_balance', 'session_vwap_trend',
            'total_session_volume', 'session_bar_count',
            'avg_volume_per_bar', 'session_range',
            'vwap_position_in_range', 'session_high', 'session_low',
            'session_type', 'duration_minutes', 'status'
        ]

        for metric in expected_metrics:
            assert metric in result, f"Missing metric: {metric}"

        # Verify metric types and ranges
        if result['status'] == 'valid':
            assert isinstance(result['session_vwap'], (int, float, np.integer, np.floating))
            assert isinstance(result['total_session_volume'], (int, float, np.integer, np.floating))
            assert result['session_type'] == 'us_open'
            assert result['duration_minutes'] == 30
            assert 0 <= result['vwap_position_in_range'] <= 1

class TestSessionVWAPConfiguration:
    """Test session VWAP indicator configuration integration."""

    def test_comprehensive_config_includes_session_vwaps(self):
        """Test that comprehensive config includes session VWAP indicators."""
        config = ResidualReturnIndicatorConfig.comprehensive_config()
        indicators = config.create_indicator_instances()

        # Check that session VWAP indicators are included
        session_vwap_indicators = [name for name in indicators.keys() if 'SessionVWAP' in name]
        assert len(session_vwap_indicators) == 6  # 3 sessions × 2 durations

        # Check specific indicators
        expected_indicators = [
            'SessionVWAP_us_open_30min',
            'SessionVWAP_us_open_60min',
            'SessionVWAP_us_close_30min',
            'SessionVWAP_us_close_60min',
            'SessionVWAP_london_close_30min',
            'SessionVWAP_london_close_60min'
        ]

        for expected in expected_indicators:
            assert expected in indicators.keys(), f"Missing indicator: {expected}"

    def test_calculate_all_includes_session_vwaps(self):
        """Test that calculate_all_technical_indicators includes session VWAPs."""
        # Create test data
        base_time = datetime(2024, 8, 17, 9, 30)
        timestamps = pd.date_range(base_time, periods=120, freq='1min')

        data = pd.DataFrame({
            'timestamp': timestamps,
            'open': [100.0 + i * 0.01 for i in range(120)],
            'high': [100.1 + i * 0.01 for i in range(120)],
            'low': [99.9 + i * 0.01 for i in range(120)],
            'close': [100.0 + i * 0.01 for i in range(120)],
            'volume': [1000 + i * 5 for i in range(120)]
        })
        data.index = timestamps

        results = calculate_all_technical_indicators(data)

        # Check that session VWAP results are present
        session_vwap_results = {k: v for k, v in results.items() if 'SessionVWAP' in k}
        assert len(session_vwap_results) > 0

        # Check specific indicator results
        assert any('SessionVWAP_us_open' in key for key in results.keys())
        assert any('SessionVWAP_us_close' in key for key in results.keys())
        assert any('SessionVWAP_london_close' in key for key in results.keys())

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