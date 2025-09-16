#!/usr/bin/env python3
"""
Real-time processor integration tests for Support/Resistance system
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from decimal import Decimal

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from events.processors.support_resistance_processor import SupportResistanceProcessor
from events.analysis.support_resistance_detector import (
    SRLevel, SRTest, SREvent, SRType, SRLevelType, SRTestOutcome, Timeframe
)

class TestSupportResistanceProcessor:
    """Test suite for SupportResistanceProcessor"""

    @pytest.fixture
    def mock_db_pool(self):
        """Create mock database pool"""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()

        # Mock connection context manager
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        # Mock database queries
        mock_conn.fetch.return_value = [
            {'symbol': 'TEST_AAPL'},
            {'symbol': 'TEST_MSFT'},
            {'symbol': 'TEST_GOOGL'}
        ]

        mock_conn.fetchrow.return_value = {'id': 123, 'price': Decimal('100.00')}
        mock_conn.execute.return_value = None

        return mock_pool

    @pytest.fixture
    def processor_config(self):
        """Test processor configuration"""
        return {
            'processing_interval_seconds': 10,  # Fast for testing
            'batch_size': 5,
            'max_concurrent_symbols': 3,
            'min_data_points': 20,
            'enable_cross_timeframe_validation': True,
            'alert_thresholds': {
                'strong_level_test': 0.7,
                'level_break': 0.6,
                'confluence_level': 0.8
            },
            'timeframe_priorities': [Timeframe.DAILY, Timeframe.INTRADAY_1H],
            'detector_config': {
                'pivot_lookback': 5,
                'cluster_epsilon': 0.02,
                'proximity_tolerance': 0.005,
                'break_threshold': 0.01
            }
        }

    @pytest.fixture
    async def mock_processor(self, processor_config, mock_db_pool):
        """Create mock processor for testing"""
        with patch('events.processors.support_resistance_processor.Environment') as mock_env:
            mock_env.return_value.database.create_pool_with_retry.return_value = mock_db_pool
            mock_env.return_value.get_table_name.return_value = 'dev_instrument'

            processor = SupportResistanceProcessor(processor_config)
            processor.db_pool = mock_db_pool
            processor.active_symbols = {'TEST_AAPL', 'TEST_MSFT', 'TEST_GOOGL'}
            processor._initialize_processing_state()

            yield processor

    @pytest.fixture
    def sample_market_data(self):
        """Generate sample market data for testing"""
        dates = pd.date_range(start='2024-01-01', end='2024-03-01', freq='D')

        # Create data with clear S/R levels
        data = []
        for i, date in enumerate(dates):
            # Price oscillates between 98-102 (support/resistance)
            base_price = 100 + 2 * np.sin(2 * np.pi * i / 20)
            variation = np.random.normal(0, 0.5)
            close = base_price + variation

            data.append({
                'timestamp': date,
                'open': close + np.random.normal(0, 0.2),
                'high': close + abs(np.random.normal(0, 0.8)),
                'low': close - abs(np.random.normal(0, 0.8)),
                'close': close,
                'volume': int(np.random.lognormal(15, 0.3))
            })

        return pd.DataFrame(data)

    @pytest.mark.asyncio
    async def test_processor_initialization(self, processor_config, mock_db_pool):
        """Test processor initialization"""
        with patch('events.processors.support_resistance_processor.Environment') as mock_env:
            mock_env.return_value.database.create_pool_with_retry.return_value = mock_db_pool
            mock_env.return_value.get_table_name.return_value = 'dev_instrument'

            processor = SupportResistanceProcessor(processor_config)
            await processor.initialize()

            # Should have initialized properly
            assert processor.db_pool is not None
            assert len(processor.active_symbols) > 0
            assert processor.stats['levels_detected'] == 0
            assert processor.stats['tests_identified'] == 0

    @pytest.mark.asyncio
    async def test_market_data_processing(self, mock_processor, sample_market_data):
        """Test processing of market data updates"""
        symbol = 'TEST_AAPL'
        timeframe = Timeframe.DAILY

        # Mock the detector to return levels and tests
        with patch.object(mock_processor.detector, 'detect_sr_levels') as mock_detect_levels, \
             patch.object(mock_processor.detector, 'detect_sr_tests') as mock_detect_tests:

            # Mock returns
            mock_levels = [
                SRLevel(
                    price=100.0, sr_type=SRType.SUPPORT, level_type=SRLevelType.PIVOT,
                    timeframe=Timeframe.DAILY, strength=0.8, first_established=datetime.now(),
                    last_tested=datetime.now(), test_count=1, hold_count=1, break_count=0,
                    confidence=0.9, volume_confirmation=True, metadata={}
                )
            ]

            mock_tests = [
                SRTest(
                    level_id='test_level_1', test_datetime=datetime.now(), test_price=100.25,
                    approach_direction='down', max_penetration=0.005, hold_duration=300,
                    volume_spike=2.0, outcome=SRTestOutcome.HOLD_STRONG, confidence=0.85
                )
            ]

            mock_detect_levels.return_value = mock_levels
            mock_detect_tests.return_value = mock_tests

            # Process market data
            await mock_processor.process_market_data_update(symbol, sample_market_data, timeframe)

            # Verify detection was called
            mock_detect_levels.assert_called_once_with(symbol, sample_market_data, timeframe)
            mock_detect_tests.assert_called_once_with(symbol, sample_market_data, mock_levels)

            # Check stats were updated
            assert mock_processor.stats['levels_detected'] > 0
            assert mock_processor.stats['tests_identified'] > 0
            assert mock_processor.stats['symbols_processed'] > 0

    @pytest.mark.asyncio
    async def test_insufficient_data_handling(self, mock_processor):
        """Test handling of insufficient data"""
        # Create small dataset (below min_data_points)
        small_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=5, freq='D'),
            'open': [100, 101, 102, 103, 104],
            'high': [101, 102, 103, 104, 105],
            'low': [99, 100, 101, 102, 103],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5],
            'volume': [1000, 1100, 1200, 1300, 1400]
        })

        initial_stats = mock_processor.stats.copy()

        # Should handle gracefully without processing
        await mock_processor.process_market_data_update('TEST_SMALL', small_data, Timeframe.DAILY)

        # Stats should not change significantly
        assert mock_processor.stats['levels_detected'] == initial_stats['levels_detected']
        assert mock_processor.stats['symbols_processed'] == initial_stats['symbols_processed']

    @pytest.mark.asyncio
    async def test_level_storage(self, mock_processor):
        """Test S/R level storage in database"""
        # Create test levels
        levels = [
            SRLevel(
                price=100.0, sr_type=SRType.SUPPORT, level_type=SRLevelType.PIVOT,
                timeframe=Timeframe.DAILY, strength=0.8, first_established=datetime.now(),
                last_tested=datetime.now(), test_count=2, hold_count=2, break_count=0,
                confidence=0.9, volume_confirmation=True, metadata={'source': 'test'}
            ),
            SRLevel(
                price=105.0, sr_type=SRType.RESISTANCE, level_type=SRLevelType.PSYCHOLOGICAL,
                timeframe=Timeframe.DAILY, strength=0.7, first_established=datetime.now(),
                last_tested=datetime.now(), test_count=1, hold_count=1, break_count=0,
                confidence=0.8, volume_confirmation=False, metadata={}
            )
        ]

        # Mock successful storage
        mock_processor.db_pool.acquire.return_value.__aenter__.return_value.fetchrow.return_value = {'id': 123}

        # Store levels
        stored_ids = await mock_processor._store_sr_levels('TEST_STORE', levels, Timeframe.DAILY)

        # Should return IDs for stored levels
        assert len(stored_ids) == len(levels)
        assert all(isinstance(id, int) for id in stored_ids)

    @pytest.mark.asyncio
    async def test_event_generation_and_emission(self, mock_processor):
        """Test S/R event generation and emission"""
        # Create test data
        test = SRTest(
            level_id='test_level_123', test_datetime=datetime.now(), test_price=100.50,
            approach_direction='up', max_penetration=0.015, hold_duration=600,
            volume_spike=3.5, outcome=SRTestOutcome.BREAK_CLEAN, confidence=0.95
        )

        # Mock database queries for event creation
        mock_conn = mock_processor.db_pool.acquire.return_value.__aenter__.return_value
        mock_conn.fetchrow.side_effect = [
            {'id': 456},  # test storage
            {  # level data for event creation
                'id': 123, 'price': Decimal('100.00'), 'sr_type': 'resistance',
                'level_type': 'pivot', 'timeframe': '1d', 'strength': Decimal('0.8'),
                'confidence': Decimal('0.9'), 'first_established': datetime.now(),
                'last_tested': datetime.now(), 'test_count': 3, 'hold_count': 2,
                'break_count': 1, 'volume_confirmation': True, 'metadata': {}
            }
        ]

        # Mock event emission
        with patch.object(mock_processor, '_emit_sr_event') as mock_emit:
            # Process the test
            await mock_processor._process_sr_tests('TEST_EVENT', [test], Timeframe.DAILY)

            # Should have emitted an event
            mock_emit.assert_called_once()

            # Check event properties
            emitted_event = mock_emit.call_args[0][0]
            assert isinstance(emitted_event, SREvent)
            assert emitted_event.symbol == 'TEST_EVENT'
            assert emitted_event.test.outcome == SRTestOutcome.BREAK_CLEAN

    @pytest.mark.asyncio
    async def test_event_significance_calculation(self, mock_processor):
        """Test event significance scoring"""
        # Test different outcomes
        test_cases = [
            (SRTestOutcome.HOLD_STRONG, 2.5, 0.9, True),   # High significance
            (SRTestOutcome.BREAK_CLEAN, 3.0, 0.95, True),  # High significance
            (SRTestOutcome.HOLD_WEAK, 1.2, 0.6, False),    # Lower significance
            (SRTestOutcome.PENETRATION, 1.0, 0.5, False)   # Lower significance
        ]

        for outcome, volume_spike, confidence, should_be_significant in test_cases:
            test = SRTest(
                level_id='significance_test', test_datetime=datetime.now(), test_price=100.0,
                approach_direction='up', max_penetration=0.01, hold_duration=300,
                volume_spike=volume_spike, outcome=outcome, confidence=confidence
            )

            significance = mock_processor._calculate_event_significance(test)

            if should_be_significant:
                assert significance > 0.7, f"Should be significant for {outcome.value}"
            else:
                assert significance < 0.7, f"Should be less significant for {outcome.value}"

    @pytest.mark.asyncio
    async def test_alert_logic(self, mock_processor):
        """Test alert triggering logic"""
        # Create test scenarios
        scenarios = [
            # (level_strength, outcome, volume_spike, should_alert)
            (0.9, SRTestOutcome.HOLD_STRONG, 3.0, True),   # Strong level + high volume
            (0.8, SRTestOutcome.BREAK_CLEAN, 1.5, True),   # Level break
            (0.5, SRTestOutcome.HOLD_WEAK, 1.0, False),    # Weak level
            (0.9, SRTestOutcome.PENETRATION, 1.2, False)   # No clean break
        ]

        for level_strength, outcome, volume_spike, should_alert in scenarios:
            # Create test event
            level = SRLevel(
                price=100.0, sr_type=SRType.SUPPORT, level_type=SRLevelType.CONFLUENCE,
                timeframe=Timeframe.DAILY, strength=level_strength, first_established=datetime.now(),
                last_tested=datetime.now(), test_count=1, hold_count=1, break_count=0,
                confidence=0.9, volume_confirmation=True, metadata={}
            )

            test = SRTest(
                level_id='alert_test', test_datetime=datetime.now(), test_price=100.0,
                approach_direction='down', max_penetration=0.01, hold_duration=300,
                volume_spike=volume_spike, outcome=outcome, confidence=0.8
            )

            event = SREvent(
                event_id=f'alert_test_{outcome.value}', symbol='TEST_ALERT',
                level=level, test=test, created_at=datetime.now(), updated_at=datetime.now()
            )

            # Test alert decision
            should_send_alert = mock_processor._should_alert(event)

            assert should_send_alert == should_alert, \
                f"Alert logic failed for strength={level_strength}, outcome={outcome.value}, volume={volume_spike}"

    @pytest.mark.asyncio
    async def test_batch_processing(self, mock_processor, sample_market_data):
        """Test batch processing of multiple symbols"""
        # Mock the market data retrieval
        with patch.object(mock_processor, '_get_market_data') as mock_get_data:
            mock_get_data.return_value = sample_market_data

            # Mock detection methods
            with patch.object(mock_processor.detector, 'detect_sr_levels') as mock_levels, \
                 patch.object(mock_processor.detector, 'detect_sr_tests') as mock_tests:

                mock_levels.return_value = []  # No levels for simplicity
                mock_tests.return_value = []   # No tests

                # Run batch processing
                await mock_processor.run_batch_processing()

                # Should have processed all active symbols
                expected_calls = len(mock_processor.active_symbols) * len(mock_processor.config['timeframe_priorities'])
                assert mock_get_data.call_count == expected_calls

    @pytest.mark.asyncio
    async def test_performance_tracking(self, mock_processor, sample_market_data):
        """Test performance statistics tracking"""
        initial_stats = mock_processor.get_processing_stats()

        # Mock detector to return some results
        with patch.object(mock_processor.detector, 'detect_sr_levels') as mock_levels, \
             patch.object(mock_processor.detector, 'detect_sr_tests') as mock_tests:

            mock_levels.return_value = [MagicMock()]  # One level
            mock_tests.return_value = [MagicMock()]   # One test

            # Process some data
            await mock_processor.process_market_data_update('TEST_PERF', sample_market_data, Timeframe.DAILY)

            # Check stats were updated
            final_stats = mock_processor.get_processing_stats()

            assert final_stats['levels_detected'] > initial_stats['levels_detected']
            assert final_stats['tests_identified'] > initial_stats['tests_identified']
            assert final_stats['symbols_processed'] > initial_stats['symbols_processed']
            assert final_stats['processing_time_ms'] > 0

    @pytest.mark.asyncio
    async def test_error_handling(self, mock_processor, sample_market_data):
        """Test error handling during processing"""
        # Mock detector to raise exception
        with patch.object(mock_processor.detector, 'detect_sr_levels') as mock_levels:
            mock_levels.side_effect = Exception("Test detection error")

            # Should handle error gracefully
            with pytest.raises(Exception):
                await mock_processor.process_market_data_update('TEST_ERROR', sample_market_data, Timeframe.DAILY)

            # Error count should increase
            assert mock_processor.stats['errors'] > 0

    @pytest.mark.asyncio
    async def test_cross_timeframe_validation(self, mock_processor):
        """Test cross-timeframe validation logic"""
        # This would test the integration of multiple timeframes
        # For now, just ensure the config is respected
        assert mock_processor.config['enable_cross_timeframe_validation'] == True
        assert len(mock_processor.config['timeframe_priorities']) > 1

    @pytest.mark.asyncio
    async def test_concurrent_processing_limits(self, mock_processor):
        """Test that concurrent processing respects limits"""
        max_concurrent = mock_processor.config['max_concurrent_symbols']

        # This is more of a configuration test
        assert max_concurrent > 0
        assert max_concurrent <= 100  # Reasonable limit

    def test_alert_message_formatting(self, mock_processor):
        """Test alert message formatting"""
        # Create test event for alert formatting
        level = SRLevel(
            price=123.45, sr_type=SRType.RESISTANCE, level_type=SRLevelType.PSYCHOLOGICAL,
            timeframe=Timeframe.DAILY, strength=0.85, first_established=datetime.now(),
            last_tested=datetime.now(), test_count=1, hold_count=1, break_count=0,
            confidence=0.9, volume_confirmation=True, metadata={}
        )

        test = SRTest(
            level_id='format_test', test_datetime=datetime.now(), test_price=123.50,
            approach_direction='up', max_penetration=0.01, hold_duration=300,
            volume_spike=2.8, outcome=SRTestOutcome.BREAK_CLEAN, confidence=0.9
        )

        event = SREvent(
            event_id='format_test_event', symbol='AAPL',
            level=level, test=test, created_at=datetime.now(), updated_at=datetime.now()
        )

        # Test message formatting
        message = mock_processor._format_alert_message(event)

        # Should contain key information
        assert 'AAPL' in message
        assert 'RESISTANCE' in message
        assert '$123.45' in message
        assert 'Break Clean' in message
        assert '0.85' in message  # Strength
        assert '2.8x' in message  # Volume spike

class TestProcessorIntegration:
    """Integration tests for the full S/R processing pipeline"""

    @pytest.mark.asyncio
    async def test_end_to_end_processing(self):
        """Test complete end-to-end S/R processing pipeline"""
        # This would be a comprehensive integration test
        # For now, we'll test that all components can be instantiated together

        config = {
            'processing_interval_seconds': 60,
            'batch_size': 10,
            'max_concurrent_symbols': 5,
            'min_data_points': 50,
            'detector_config': {
                'pivot_lookback': 20,
                'cluster_epsilon': 0.02
            }
        }

        # Should be able to create processor without errors
        processor = SupportResistanceProcessor(config)

        # Basic validation
        assert processor.config is not None
        assert processor.detector is not None
        assert processor.stats is not None

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])