"""
Unit tests for news event integration in training data callback.

These tests validate that the training data callback correctly integrates
with EconomicEventsDAO to generate training data only when news events are detected.
"""

import pytest
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List
from unittest.mock import MagicMock, AsyncMock, patch

# Import the callback class we're testing
from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from core.dao.economic_events_dao import EconomicEvent


class TestNewsEventIntegration:
    """Test suite for news event integration in training data callback."""
    
    @pytest.fixture
    def mock_config(self):
        """Create a mock TrainingDataConfig for testing."""
        config = MagicMock()
        config.timeframes = {'5m': 5, '15m': 15, '1h': 60, '1d': 1440}
        return config
    
    @pytest.fixture
    def mock_economic_events_dao(self):
        """Create a mock EconomicEventsDAO for testing."""
        dao = AsyncMock()
        return dao
    
    @pytest.fixture
    def callback_instance(self, mock_config, mock_economic_events_dao):
        """Create an IntervalBasedTrainingDataCallback instance with news event DAO."""
        with patch('ml.training_data.callbacks.training_data_callback.ray'):
            with patch('ml.training_data.callbacks.training_data_callback.get_news_event_filtering_config') as mock_gin_config:
                # Mock gin config to enable news event filtering for tests
                mock_gin_config.return_value = {
                    'enabled': True,
                    'time_window_hours': 24,
                    'min_importance_level': 3
                }
                callback = IntervalBasedTrainingDataCallback(
                    symbols=['AAPL', 'TSLA'],
                    config=mock_config,
                    output_dir='/tmp/test_training',
                    economic_events_dao=mock_economic_events_dao
                )
        return callback
    
    @pytest.mark.asyncio
    async def test_check_news_events_with_events_found(self, callback_instance):
        """Test _check_news_events_around_time when events are found."""
        # Mock economic events data
        mock_events = [
            {
                'event_name': 'Federal Reserve Interest Rate Decision',
                'date': date(2025, 1, 15),
                'release_time': datetime(2025, 1, 15, 14, 30),
                'importance_level': 5,
                'country': 'US'
            },
            {
                'event_name': 'GDP Growth Rate',
                'date': date(2025, 1, 15),
                'release_time': datetime(2025, 1, 15, 8, 30),
                'importance_level': 4,
                'country': 'US'
            }
        ]
        
        callback_instance.economic_events_dao.get_economic_events_with_types.return_value = mock_events
        
        current_time = datetime(2025, 1, 15, 15, 0)  # 3 PM on same day
        result = await callback_instance._check_news_events_around_time('AAPL', current_time)
        
        # Should find events
        assert result is True
        
        # Verify DAO was called with correct parameters
        callback_instance.economic_events_dao.get_economic_events_with_types.assert_called_once()
        call_args = callback_instance.economic_events_dao.get_economic_events_with_types.call_args
        assert call_args[1]['start_date'] <= date(2025, 1, 15) <= call_args[1]['end_date']
        assert call_args[1]['min_importance'] == 3
    
    @pytest.mark.asyncio
    async def test_check_news_events_with_no_events(self, callback_instance):
        """Test _check_news_events_around_time when no events are found."""
        # Mock empty events list
        callback_instance.economic_events_dao.get_economic_events_with_types.return_value = []
        
        current_time = datetime(2025, 1, 15, 15, 0)
        result = await callback_instance._check_news_events_around_time('AAPL', current_time)
        
        # Should not find events
        assert result is False
        
        # Verify DAO was called
        callback_instance.economic_events_dao.get_economic_events_with_types.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_check_news_events_no_dao(self, mock_config):
        """Test _check_news_events_around_time when no DAO is provided."""
        with patch('ml.training_data.callbacks.training_data_callback.ray'):
            with patch('ml.training_data.callbacks.training_data_callback.get_news_event_filtering_config') as mock_gin_config:
                mock_gin_config.return_value = {
                    'enabled': True,
                    'time_window_hours': 24,
                    'min_importance_level': 3
                }
                callback = IntervalBasedTrainingDataCallback(
                    symbols=['AAPL'],
                    config=mock_config,
                    output_dir='/tmp/test_training',
                    economic_events_dao=None  # No DAO provided
                )
        
        current_time = datetime(2025, 1, 15, 15, 0)
        result = await callback._check_news_events_around_time('AAPL', current_time)
        
        # Should return False when no DAO is provided
        assert result is False
    
    @pytest.mark.asyncio
    async def test_news_event_filtering_disabled_by_default(self, mock_config):
        """Test that news event filtering is disabled by default (gin config)."""
        with patch('ml.training_data.callbacks.training_data_callback.ray'):
            # Don't mock gin config - let it use defaults
            callback = IntervalBasedTrainingDataCallback(
                symbols=['AAPL'],
                config=mock_config,
                output_dir='/tmp/test_training',
                economic_events_dao=MagicMock()
            )
        
        # Should use default configuration with enabled=False
        assert callback.news_event_config['enabled'] is False
        assert callback.news_event_config['time_window_hours'] == 24
        assert callback.news_event_config['min_importance_level'] == 3
    
    @pytest.mark.asyncio
    async def test_check_news_events_time_window_filtering(self, callback_instance):
        """Test that events outside the time window are filtered out."""
        # Mock events - one inside window, one outside
        mock_events = [
            {
                'event_name': 'Recent Event',
                'date': date(2025, 1, 15),
                'release_time': datetime(2025, 1, 15, 14, 30),  # Within 24h window
                'importance_level': 4
            },
            {
                'event_name': 'Old Event',
                'date': date(2025, 1, 10),  # 5 days ago, outside 24h window
                'release_time': datetime(2025, 1, 10, 14, 30),
                'importance_level': 4
            }
        ]
        
        callback_instance.economic_events_dao.get_economic_events_with_types.return_value = mock_events
        
        current_time = datetime(2025, 1, 15, 15, 0)
        result = await callback_instance._check_news_events_around_time('AAPL', current_time)
        
        # Should find the recent event (within 24h window)
        assert result is True
    
    @pytest.mark.asyncio
    async def test_check_news_events_dao_error_handling(self, callback_instance):
        """Test error handling when DAO raises an exception."""
        # Mock DAO to raise an exception
        callback_instance.economic_events_dao.get_economic_events_with_types.side_effect = Exception("Database error")
        
        current_time = datetime(2025, 1, 15, 15, 0)
        result = await callback_instance._check_news_events_around_time('AAPL', current_time)
        
        # Should return False on error for safety
        assert result is False
    
    @pytest.mark.asyncio
    async def test_handleInterval_with_news_events(self, callback_instance):
        """Test handleInterval generates data when news events are found."""
        # Mock news event detection to return True
        callback_instance._check_news_events_around_time = AsyncMock(return_value=True)
        
        # Mock training data generation
        callback_instance.minute_data_manager = MagicMock()  # Set attribute to use new path
        callback_instance._generate_multi_timeframe_example = AsyncMock(return_value={
            'symbol': 'AAPL',
            'features': {'open': 100.0, 'close': 102.0},
            'timestamp': '2025-01-15T15:00:00'
        })
        callback_instance._save_interval_examples = AsyncMock()
        
        # Mock runner
        runner = MagicMock()
        current_time = datetime(2025, 1, 15, 15, 0)
        
        await callback_instance.handleInterval(runner, current_time)
        
        # Verify news event check was called for each symbol
        assert callback_instance._check_news_events_around_time.call_count == len(callback_instance.symbols)
        
        # Verify training data generation was called
        callback_instance._generate_multi_timeframe_example.assert_called()
        
        # Verify data was saved
        callback_instance._save_interval_examples.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_handleInterval_without_news_events(self, callback_instance):
        """Test handleInterval skips data generation when no news events are found."""
        # Mock news event detection to return False
        callback_instance._check_news_events_around_time = AsyncMock(return_value=False)
        
        # Mock training data generation (should not be called)
        callback_instance.minute_data_manager = MagicMock()
        callback_instance._generate_multi_timeframe_example = AsyncMock()
        callback_instance._save_interval_examples = AsyncMock()
        
        # Mock runner
        runner = MagicMock()
        current_time = datetime(2025, 1, 15, 15, 0)
        
        await callback_instance.handleInterval(runner, current_time)
        
        # Verify news event check was called
        assert callback_instance._check_news_events_around_time.call_count == len(callback_instance.symbols)
        
        # Verify training data generation was NOT called
        callback_instance._generate_multi_timeframe_example.assert_not_called()
        
        # Verify data was NOT saved
        callback_instance._save_interval_examples.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_handleInterval_mixed_symbols(self, callback_instance):
        """Test handleInterval with some symbols having news events and others not."""
        # Mock news event detection - True for AAPL, False for TSLA
        def mock_news_check(symbol, current_time):
            return symbol == 'AAPL'
        
        callback_instance._check_news_events_around_time = AsyncMock(side_effect=mock_news_check)
        
        # Mock training data generation
        callback_instance.minute_data_manager = MagicMock()
        callback_instance._generate_multi_timeframe_example = AsyncMock(return_value={
            'symbol': 'AAPL',
            'features': {'open': 100.0, 'close': 102.0},
            'timestamp': '2025-01-15T15:00:00'
        })
        callback_instance._save_interval_examples = AsyncMock()
        
        # Mock runner
        runner = MagicMock()
        current_time = datetime(2025, 1, 15, 15, 0)
        
        await callback_instance.handleInterval(runner, current_time)
        
        # Verify news event check was called for both symbols
        assert callback_instance._check_news_events_around_time.call_count == 2
        
        # Verify training data generation was called only once (for AAPL)
        callback_instance._generate_multi_timeframe_example.assert_called_once_with(
            symbol='AAPL',
            current_time=current_time
        )
        
        # Verify data was saved
        callback_instance._save_interval_examples.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_handleInterval_with_filtering_disabled(self, mock_config):
        """Test handleInterval generates data for all intervals when news filtering is disabled."""
        with patch('ml.training_data.callbacks.training_data_callback.ray'):
            # Mock gin config to disable news event filtering
            with patch('ml.training_data.callbacks.training_data_callback.get_news_event_filtering_config') as mock_gin_config:
                mock_gin_config.return_value = {
                    'enabled': False,  # Disabled
                    'time_window_hours': 24,
                    'min_importance_level': 3
                }
                callback = IntervalBasedTrainingDataCallback(
                    symbols=['AAPL'],
                    config=mock_config,
                    output_dir='/tmp/test_training',
                    economic_events_dao=MagicMock()
                )
        
        # Mock training data generation
        callback.minute_data_manager = MagicMock()
        callback._generate_multi_timeframe_example = AsyncMock(return_value={
            'symbol': 'AAPL',
            'features': {'open': 100.0, 'close': 102.0},
            'timestamp': '2025-01-15T15:00:00'
        })
        callback._save_interval_examples = AsyncMock()
        callback._check_news_events_around_time = AsyncMock()  # Should not be called
        
        # Mock runner
        runner = MagicMock()
        current_time = datetime(2025, 1, 15, 15, 0)
        
        await callback.handleInterval(runner, current_time)
        
        # Verify news event check was NOT called
        callback._check_news_events_around_time.assert_not_called()
        
        # Verify training data generation WAS called (filtering disabled)
        callback._generate_multi_timeframe_example.assert_called_once_with(
            symbol='AAPL',
            current_time=current_time
        )
        
        # Verify data was saved
        callback._save_interval_examples.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])