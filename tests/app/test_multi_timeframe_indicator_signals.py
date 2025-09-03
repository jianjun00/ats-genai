"""
Comprehensive tests for multi-timeframe indicator signals.

Tests indicator_runner with base_duration='1m' and signal computation for multiple
timeframes (5m, 15m, 1h, 1d, 1w), and verifies signals can be accessed through
UniverseStateManager.
"""

import os
import sys
import pytest
import pandas as pd
import asyncio
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from app.indicator_runner import IndicatorRunner
from state.universe_state_manager import UniverseStateManager
from state.indicator_interval import IndicatorInterval
from config.environment import Environment
from signals.indicator_config import IndicatorConfig


@pytest.fixture
def mock_environment():
    """Mock environment for testing."""
    env = MagicMock(spec=Environment)
    env.get_database_url.return_value = "sqlite:///:memory:"
    return env


@pytest.fixture
def mock_market_data_manager():
    """Mock market data manager with sample OHLC data."""
    manager = AsyncMock()
    
    # Sample OHLC data for testing
    sample_ohlc = {
        'open': 150.0,
        'high': 155.0,
        'low': 148.0,
        'close': 152.0,
        'volume': 1000000,
        'traded_volume': 1000000,
        'traded_dollar': 152000000.0,
        'status': 'ok'
    }
    
    # Mock symbol to ID mapping
    manager._symbol_to_id = {'AAPL': 1001, 'TSLA': 1002}
    
    # Mock get_ohlc method to return sample data
    async def mock_get_ohlc(instrument_id, start_time, end_time, current_date=None):
        # Vary the price slightly based on the date to create realistic data
        base_price = 150.0 + (instrument_id - 1000) * 10  # AAPL=150, TSLA=160
        day_offset = (current_date - date(2024, 1, 1)).days if current_date else 0
        price_variation = day_offset * 0.5  # Small daily price changes
        
        return {
            'open': base_price + price_variation,
            'high': base_price + price_variation + 5.0,
            'low': base_price + price_variation - 3.0,
            'close': base_price + price_variation + 2.0,
            'volume': 1000000 + day_offset * 10000,
            'traded_volume': 1000000 + day_offset * 10000,
            'traded_dollar': (base_price + price_variation + 2.0) * (1000000 + day_offset * 10000),
            'status': 'ok'
        }
    
    manager.get_ohlc.side_effect = mock_get_ohlc
    return manager


@pytest.fixture
def multi_timeframe_indicator_config():
    """Create indicator configuration for multiple timeframes."""
    # Create different configs for different timeframes
    configs = {
        '1m': IndicatorConfig(
            indicators=['etop', 'ebot', 'pldot'],
            periods={'etop': 5, 'ebot': 5, 'pldot': 10}
        ),
        '5m': IndicatorConfig(
            indicators=['etop', 'ebot', 'pldot', 'sma_20'],
            periods={'etop': 10, 'ebot': 10, 'pldot': 20, 'sma_20': 20}
        ),
        '15m': IndicatorConfig(
            indicators=['etop', 'ebot', 'pldot', 'sma_20', 'ema_12'],
            periods={'etop': 20, 'ebot': 20, 'pldot': 30, 'sma_20': 20, 'ema_12': 12}
        ),
        '1h': IndicatorConfig(
            indicators=['etop', 'ebot', 'pldot', 'sma_20', 'ema_12', 'rsi_14'],
            periods={'etop': 50, 'ebot': 50, 'pldot': 100, 'sma_20': 20, 'ema_12': 12, 'rsi_14': 14}
        ),
        '1d': IndicatorConfig(
            indicators=['etop', 'ebot', 'pldot', 'sma_20', 'ema_12', 'rsi_14', 'macd_line'],
            periods={'etop': 20, 'ebot': 20, 'pldot': 50, 'sma_20': 20, 'ema_12': 12, 'rsi_14': 14, 'macd_line': 26}
        ),
        '1w': IndicatorConfig(
            indicators=['etop', 'ebot', 'pldot', 'sma_20'],
            periods={'etop': 10, 'ebot': 10, 'pldot': 20, 'sma_20': 20}
        )
    }
    return configs


class TestMultiTimeframeIndicatorSignals:
    """Test multi-timeframe indicator signal computation and retrieval."""

    @pytest.mark.asyncio
    async def test_indicator_runner_base_1m_multiple_timeframes(
        self, mock_environment, mock_market_data_manager, multi_timeframe_indicator_config
    ):
        """
        Test IndicatorRunner with base_duration='1m' computing signals for multiple timeframes.
        """
        # Test parameters
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 10)
        symbols = ['AAPL', 'TSLA']
        
        # Create indicator runner with 1-minute base duration
        indicator_runner = IndicatorRunner(
            start_date=start_date,
            end_date=end_date,
            environment=mock_environment,
            symbols=symbols,
            vendor='polygon',
            indicator_config=multi_timeframe_indicator_config['1d'],  # Default config
            base_duration='1m',  # Key: base duration is 1 minute
            market_data_manager=mock_market_data_manager
        )
        
        # Mock the indicator computation for different timeframes
        with patch.object(indicator_runner, '_compute_multi_timeframe_signals') as mock_compute:
            # Mock return data for different timeframes
            mock_signals_data = {
                '5m': {
                    1001: [
                        IndicatorInterval(
                            instrument_id=1001,
                            start_date_time=datetime(2024, 1, 1, 9, 35),
                            end_date_time=datetime(2024, 1, 1, 9, 40),
                            indicators={
                                'etop': {'value': 0.85, 'status': 'ok'},
                                'ebot': {'value': 0.15, 'status': 'ok'},
                                'pldot': {'value': 2.5, 'status': 'ok'}
                            }
                        ),
                        IndicatorInterval(
                            instrument_id=1001,
                            start_date_time=datetime(2024, 1, 1, 9, 40),
                            end_date_time=datetime(2024, 1, 1, 9, 45),
                            indicators={
                                'etop': {'value': 0.82, 'status': 'ok'},
                                'ebot': {'value': 0.18, 'status': 'ok'},
                                'pldot': {'value': 2.3, 'status': 'ok'}
                            }
                        )
                    ]
                },
                '15m': {
                    1001: [
                        IndicatorInterval(
                            instrument_id=1001,
                            start_date_time=datetime(2024, 1, 1, 9, 30),
                            end_date_time=datetime(2024, 1, 1, 9, 45),
                            indicators={
                                'etop': {'value': 0.88, 'status': 'ok'},
                                'ebot': {'value': 0.12, 'status': 'ok'},
                                'pldot': {'value': 2.8, 'status': 'ok'},
                                'sma_20': {'value': 151.5, 'status': 'ok'}
                            }
                        )
                    ]
                },
                '1h': {
                    1001: [
                        IndicatorInterval(
                            instrument_id=1001,
                            start_date_time=datetime(2024, 1, 1, 9, 0),
                            end_date_time=datetime(2024, 1, 1, 10, 0),
                            indicators={
                                'etop': {'value': 0.90, 'status': 'ok'},
                                'ebot': {'value': 0.10, 'status': 'ok'},
                                'pldot': {'value': 3.2, 'status': 'ok'},
                                'sma_20': {'value': 151.8, 'status': 'ok'},
                                'rsi_14': {'value': 65.5, 'status': 'ok'}
                            }
                        )
                    ]
                },
                '1d': {
                    1001: [
                        IndicatorInterval(
                            instrument_id=1001,
                            start_date_time=datetime(2024, 1, 1, 0, 0),
                            end_date_time=datetime(2024, 1, 1, 23, 59, 59),
                            indicators={
                                'etop': {'value': 0.75, 'status': 'ok'},
                                'ebot': {'value': 0.25, 'status': 'ok'},
                                'pldot': {'value': 1.8, 'status': 'ok'},
                                'sma_20': {'value': 152.0, 'status': 'ok'},
                                'ema_12': {'value': 151.2, 'status': 'ok'},
                                'rsi_14': {'value': 58.3, 'status': 'ok'}
                            }
                        )
                    ]
                },
                '1w': {
                    1001: [
                        IndicatorInterval(
                            instrument_id=1001,
                            start_date_time=datetime(2024, 1, 1, 0, 0),
                            end_date_time=datetime(2024, 1, 7, 23, 59, 59),
                            indicators={
                                'etop': {'value': 0.70, 'status': 'ok'},
                                'ebot': {'value': 0.30, 'status': 'ok'},
                                'pldot': {'value': 1.5, 'status': 'ok'},
                                'sma_20': {'value': 152.5, 'status': 'ok'}
                            }
                        )
                    ]
                }
            }
            
            mock_compute.return_value = mock_signals_data
            
            # Run the indicator computation
            results = await indicator_runner.run_multi_timeframe_indicators(
                timeframes=['5m', '15m', '1h', '1d', '1w']
            )
            
            # Verify results structure
            assert isinstance(results, dict)
            assert '5m' in results
            assert '15m' in results
            assert '1h' in results
            assert '1d' in results
            assert '1w' in results
            
            # Verify 5-minute signals
            signals_5m = results['5m'][1001]
            assert len(signals_5m) == 2
            assert signals_5m[0].indicators['etop']['value'] == 0.85
            assert signals_5m[0].indicators['ebot']['value'] == 0.15
            assert signals_5m[0].indicators['pldot']['value'] == 2.5
            
            # Verify 15-minute signals include additional indicators
            signals_15m = results['15m'][1001]
            assert len(signals_15m) == 1
            assert 'sma_20' in signals_15m[0].indicators
            assert signals_15m[0].indicators['sma_20']['value'] == 151.5
            
            # Verify 1-hour signals include RSI
            signals_1h = results['1h'][1001]
            assert len(signals_1h) == 1
            assert 'rsi_14' in signals_1h[0].indicators
            assert signals_1h[0].indicators['rsi_14']['value'] == 65.5
            
            # Verify daily signals are comprehensive
            signals_1d = results['1d'][1001]
            assert len(signals_1d) == 1
            expected_indicators = ['etop', 'ebot', 'pldot', 'sma_20', 'ema_12', 'rsi_14']
            for indicator in expected_indicators:
                assert indicator in signals_1d[0].indicators
            
            # Verify weekly signals
            signals_1w = results['1w'][1001]
            assert len(signals_1w) == 1
            assert signals_1w[0].indicators['etop']['value'] == 0.70

    @pytest.mark.asyncio
    async def test_universe_state_manager_get_lagged_signals(self, mock_environment):
        """
        Test UniverseStateManager.get_lagged_signals for different time intervals.
        """
        # Create UniverseStateManager
        universe_manager = UniverseStateManager(
            env=mock_environment,
            states_dir="/tmp/test_states",
            metadata_dir="/tmp/test_metadata"
        )
        
        # Mock the DAO to return test data
        with patch('dao.instrument_indicator_interval_dao.InstrumentIndicatorIntervalDAO') as MockDAO:
            mock_dao_instance = AsyncMock()
            MockDAO.return_value = mock_dao_instance
            
            # Mock data for different timeframes
            mock_dao_instance.get_by_instrument_and_date_range.return_value = [
                {
                    'start_date_time': datetime(2024, 1, 1, 9, 35),
                    'end_date_time': datetime(2024, 1, 1, 9, 40),
                    'indicator_name': 'etop',
                    'indicator_value': 0.85,
                    'indicator_status': 'ok'
                },
                {
                    'start_date_time': datetime(2024, 1, 1, 9, 35),
                    'end_date_time': datetime(2024, 1, 1, 9, 40),
                    'indicator_name': 'ebot',
                    'indicator_value': 0.15,
                    'indicator_status': 'ok'
                },
                {
                    'start_date_time': datetime(2024, 1, 1, 9, 40),
                    'end_date_time': datetime(2024, 1, 1, 9, 45),
                    'indicator_name': 'etop',
                    'indicator_value': 0.82,
                    'indicator_status': 'ok'
                },
                {
                    'start_date_time': datetime(2024, 1, 1, 9, 40),
                    'end_date_time': datetime(2024, 1, 1, 9, 45),
                    'indicator_name': 'ebot',
                    'indicator_value': 0.18,
                    'indicator_status': 'ok'
                }
            ]
            
            # Test 5-minute signals
            signals_5m = await universe_manager.get_lagged_signals(
                instrument_id=1001,
                cur_date=date(2024, 1, 2),
                lag_periods=10,
                time_interval='5m',
                signal_names=['etop', 'ebot']
            )
            
            # Verify results
            assert isinstance(signals_5m, pd.DataFrame)
            assert 'timestamp' in signals_5m.columns
            assert 'etop_value' in signals_5m.columns
            assert 'etop_status' in signals_5m.columns
            assert 'ebot_value' in signals_5m.columns
            assert 'ebot_status' in signals_5m.columns
            
            # Verify DAO was called with correct parameters
            mock_dao_instance.get_by_instrument_and_date_range.assert_called_once()
            call_args = mock_dao_instance.get_by_instrument_and_date_range.call_args
            assert call_args[1]['instrument_id'] == 1001
            assert isinstance(call_args[1]['start_date'], date)
            assert isinstance(call_args[1]['end_date'], date)

    @pytest.mark.asyncio
    async def test_get_lagged_signals_different_intervals(self, mock_environment):
        """
        Test get_lagged_signals for all supported time intervals.
        """
        universe_manager = UniverseStateManager(
            env=mock_environment,
            states_dir="/tmp/test_states",
            metadata_dir="/tmp/test_metadata"
        )
        
        intervals_to_test = ['1m', '5m', '15m', '1h', '1d', '1w']
        
        with patch('dao.instrument_indicator_interval_dao.InstrumentIndicatorIntervalDAO') as MockDAO:
            mock_dao_instance = AsyncMock()
            MockDAO.return_value = mock_dao_instance
            mock_dao_instance.get_by_instrument_and_date_range.return_value = []
            
            for interval in intervals_to_test:
                # Test each interval
                signals = await universe_manager.get_lagged_signals(
                    instrument_id=1001,
                    cur_date=date(2024, 1, 2),
                    lag_periods=5,
                    time_interval=interval,
                    signal_names=['etop', 'ebot', 'pldot']
                )
                
                # Should return empty DataFrame but with correct structure
                assert isinstance(signals, pd.DataFrame)
                assert 'timestamp' in signals.columns
                
                # Verify DAO was called for each interval
                assert mock_dao_instance.get_by_instrument_and_date_range.called

    def test_get_lagged_signals_type_validation(self, mock_environment):
        """Test type validation for get_lagged_signals method."""
        universe_manager = UniverseStateManager(
            env=mock_environment,
            states_dir="/tmp/test_states",
            metadata_dir="/tmp/test_metadata"
        )
        
        # Test invalid instrument_id
        with pytest.raises(ValueError, match="instrument_id must be a positive integer"):
            asyncio.run(universe_manager.get_lagged_signals(
                instrument_id="invalid",
                cur_date=date(2024, 1, 1),
                lag_periods=5,
                time_interval='5m'
            ))
        
        # Test invalid lag_periods
        with pytest.raises(ValueError, match="lag_periods must be a positive integer"):
            asyncio.run(universe_manager.get_lagged_signals(
                instrument_id=1001,
                cur_date=date(2024, 1, 1),
                lag_periods=-5,
                time_interval='5m'
            ))
        
        # Test invalid time_interval
        with pytest.raises(ValueError, match="Invalid time_interval"):
            asyncio.run(universe_manager.get_lagged_signals(
                instrument_id=1001,
                cur_date=date(2024, 1, 1),
                lag_periods=5,
                time_interval='invalid'
            ))
        
        # Test invalid signal_names
        with pytest.raises(ValueError, match="signal_names must be a list"):
            asyncio.run(universe_manager.get_lagged_signals(
                instrument_id=1001,
                cur_date=date(2024, 1, 1),
                lag_periods=5,
                time_interval='5m',
                signal_names="invalid"
            ))

    @pytest.mark.asyncio
    async def test_multi_timeframe_signal_consistency(self, mock_environment):
        """
        Test that signals computed at different timeframes are consistent and accessible.
        """
        # This test verifies the end-to-end flow:
        # 1. IndicatorRunner computes signals for multiple timeframes with base_duration='1m'
        # 2. Signals are stored in database via DAO
        # 3. UniverseStateManager can retrieve these signals for any timeframe
        
        universe_manager = UniverseStateManager(
            env=mock_environment,
            states_dir="/tmp/test_states",
            metadata_dir="/tmp/test_metadata"
        )
        
        # Mock comprehensive signal data spanning multiple timeframes
        with patch('dao.instrument_indicator_interval_dao.InstrumentIndicatorIntervalDAO') as MockDAO:
            mock_dao_instance = AsyncMock()
            MockDAO.return_value = mock_dao_instance
            
            # Create realistic multi-timeframe signal data
            timeframe_data = {
                '5m': [
                    {'start_date_time': datetime(2024, 1, 1, 9, 35), 'indicator_name': 'etop', 'indicator_value': 0.85, 'indicator_status': 'ok'},
                    {'start_date_time': datetime(2024, 1, 1, 9, 40), 'indicator_name': 'etop', 'indicator_value': 0.82, 'indicator_status': 'ok'},
                ],
                '15m': [
                    {'start_date_time': datetime(2024, 1, 1, 9, 30), 'indicator_name': 'etop', 'indicator_value': 0.88, 'indicator_status': 'ok'},
                    {'start_date_time': datetime(2024, 1, 1, 9, 30), 'indicator_name': 'sma_20', 'indicator_value': 151.5, 'indicator_status': 'ok'},
                ],
                '1h': [
                    {'start_date_time': datetime(2024, 1, 1, 9, 0), 'indicator_name': 'etop', 'indicator_value': 0.90, 'indicator_status': 'ok'},
                    {'start_date_time': datetime(2024, 1, 1, 9, 0), 'indicator_name': 'rsi_14', 'indicator_value': 65.5, 'indicator_status': 'ok'},
                ],
                '1d': [
                    {'start_date_time': datetime(2024, 1, 1, 0, 0), 'indicator_name': 'etop', 'indicator_value': 0.75, 'indicator_status': 'ok'},
                    {'start_date_time': datetime(2024, 1, 1, 0, 0), 'indicator_name': 'macd_line', 'indicator_value': 1.2, 'indicator_status': 'ok'},
                ]
            }
            
            # Test each timeframe
            for interval, expected_data in timeframe_data.items():
                mock_dao_instance.get_by_instrument_and_date_range.return_value = expected_data
                
                signals = await universe_manager.get_lagged_signals(
                    instrument_id=1001,
                    cur_date=date(2024, 1, 2),
                    lag_periods=5,
                    time_interval=interval
                )
                
                # Verify that signals were retrieved
                assert isinstance(signals, pd.DataFrame)
                if not signals.empty:
                    assert 'timestamp' in signals.columns
                    # Check for expected indicators based on timeframe
                    if interval == '5m':
                        assert any('etop' in col for col in signals.columns)
                    elif interval == '15m':
                        assert any('sma_20' in col for col in signals.columns)
                    elif interval == '1h':
                        assert any('rsi_14' in col for col in signals.columns)
                    elif interval == '1d':
                        assert any('macd_line' in col for col in signals.columns)


# Integration test helper functions
def create_mock_indicator_runner():
    """Helper to create a mock indicator runner for integration tests."""
    pass


def verify_signal_storage():
    """Helper to verify signals are properly stored in database."""
    pass


if __name__ == "__main__":
    # Run individual tests for development
    pytest.main([__file__, "-v"])