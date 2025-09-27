#!/usr/bin/env python3
"""
Tests for Multi-Scale Sequence Data Structure

Comprehensive tests for the MultiScaleSequence class and related components,
ensuring proper multi-temporal data handling and sequence operations.
"""

import pytest
import numpy as np
import pandas as pd
import torch
from datetime import datetime, timedelta
from typing import List, Dict

from domains.trading.services.core.minute.multi_scale_sequence import (
    ScaleFeatures,
    EventSequence,
    MarketEvent,
    TimeScale,
    create_multi_scale_sequence
)
from domains.analytics.events.event_integration import EventType

class TestScaleFeatures:
    """Test ScaleFeatures data structure."""

    def test_scale_features_creation(self):
        """Test basic ScaleFeatures creation."""
        timestamps = pd.date_range('2024-01-01 09:30', periods=100, freq='1min')
        ohlcv = np.random.rand(100, 5)
        technical = np.random.rand(100, 3)

        features = ScaleFeatures(
            timestamps=timestamps,
            ohlcv=ohlcv,
            technical=technical
        )

        assert len(features.timestamps) == 100
        assert features.ohlcv.shape == (100, 5)
        assert features.technical.shape == (100, 3)

    def test_scale_features_validation(self):
        """Test ScaleFeatures validation."""
        timestamps = pd.date_range('2024-01-01 09:30', periods=100, freq='1min')
        ohlcv = np.random.rand(90, 5)  # Wrong length

        with pytest.raises(ValueError, match="Timestamps and OHLCV data length mismatch"):
            ScaleFeatures(timestamps=timestamps, ohlcv=ohlcv)

class TestMarketEvent:
    """Test MarketEvent data structure."""

    def test_market_event_creation(self):
        """Test MarketEvent creation."""
        event = MarketEvent(
            event_id="test_1",
            symbol="AAPL",
            timestamp=datetime(2024, 1, 1, 10, 30),
            event_type=EventType.NEWS,
            content="Positive earnings report",
            sentiment_score=0.8,
            importance_score=0.9
        )

        assert event.symbol == "AAPL"
        assert event.event_type == EventType.NEWS
        assert event.sentiment_score == 0.8

    def test_event_to_tensor(self):
        """Test event tensor conversion."""
        event = MarketEvent(
            event_id="test_1",
            symbol="AAPL",
            timestamp=datetime(2024, 1, 1, 10, 30),
            event_type=EventType.EARNINGS,
            content="Positive earnings",
            sentiment_score=0.5,
            importance_score=0.7,
            confidence_score=0.9
        )

        tensor = event.to_tensor()

        assert tensor.shape == (4,)  # sentiment, importance, confidence, event_type
        assert tensor[0].item() == 0.5  # sentiment
        assert tensor[1].item() == 0.7  # importance
        assert tensor[2].item() == 0.9  # confidence

        # Event type should be normalized
        expected_type = list(EventType).index(EventType.EARNINGS) / len(EventType)
        assert abs(tensor[3].item() - expected_type) < 1e-6

class TestEventSequence:
    """Test EventSequence functionality."""

    def create_test_events(self) -> List[MarketEvent]:
        """Create test events."""
        events = []
        base_time = datetime(2024, 1, 1, 9, 30)

        for i in range(5):
            event = MarketEvent(
                event_id=f"event_{i}",
                symbol="AAPL",
                timestamp=base_time + timedelta(hours=i),
                event_type=EventType.NEWS,
                content=f"Event {i}",
                sentiment_score=0.5 + 0.1 * i,
                importance_score=0.6 + 0.1 * i
            )
            events.append(event)

        return events

    def test_event_sequence_creation(self):
        """Test EventSequence creation."""
        events = self.create_test_events()
        time_range = (datetime(2024, 1, 1, 9, 0), datetime(2024, 1, 1, 15, 0))

        event_seq = EventSequence(
            events=events,
            time_range=time_range,
            symbol="AAPL"
        )

        assert len(event_seq.events) == 5
        assert event_seq.symbol == "AAPL"
        assert len(event_seq.event_index) > 0  # Should build temporal index

    def test_get_events_in_window(self):
        """Test event window retrieval."""
        events = self.create_test_events()
        time_range = (datetime(2024, 1, 1, 9, 0), datetime(2024, 1, 1, 15, 0))

        event_seq = EventSequence(events=events, time_range=time_range, symbol="AAPL")

        # Get events around noon
        center_time = datetime(2024, 1, 1, 12, 0)
        nearby_events = event_seq.get_events_in_window(center_time, window_hours=2.0)

        # Should find events within 2-hour window
        assert len(nearby_events) >= 1

        for event in nearby_events:
            time_diff = abs((event.timestamp - center_time).total_seconds() / 3600)
            assert time_diff <= 2.0

    def test_to_tensor_sequence(self):
        """Test tensor sequence conversion."""
        events = self.create_test_events()
        time_range = (datetime(2024, 1, 1, 9, 0), datetime(2024, 1, 1, 15, 0))

        event_seq = EventSequence(events=events, time_range=time_range, symbol="AAPL")

        # Create timestamps (Unix timestamps)
        timestamps = torch.tensor([
            [datetime(2024, 1, 1, 9, 30).timestamp()],  # First event time
            [datetime(2024, 1, 1, 10, 30).timestamp()]  # Second event time
        ])

        event_features, event_mask = event_seq.to_tensor_sequence(
            timestamps, max_events_per_step=3
        )

        assert event_features.shape == (2, 1, 3, 4)  # batch, seq, max_events, event_dim
        assert event_mask.shape == (2, 1, 3)

        # Should have some valid events
        assert event_mask.sum() > 0

class TestMultiScaleSequence:
    """Test MultiScaleSequence functionality."""

    def create_test_data(self) -> Dict:
        """Create test data for multiple scales."""
        base_time = datetime(2024, 1, 15, 9, 30)

        # Minute data (100 minutes)
        minute_timestamps = pd.date_range(base_time, periods=100, freq='1min')
        minute_df = pd.DataFrame({
            'open': np.random.uniform(150, 160, 100),
            'high': np.random.uniform(150, 160, 100),
            'low': np.random.uniform(150, 160, 100),
            'close': np.random.uniform(150, 160, 100),
            'volume': np.random.randint(1000, 10000, 100),
            'rsi': np.random.uniform(30, 70, 100),
            'macd': np.random.uniform(-1, 1, 100)
        }, index=minute_timestamps)

        # Hourly data (24 hours)
        hourly_timestamps = pd.date_range(base_time, periods=24, freq='1H')
        hourly_df = pd.DataFrame({
            'open': np.random.uniform(150, 160, 24),
            'high': np.random.uniform(150, 160, 24),
            'low': np.random.uniform(150, 160, 24),
            'close': np.random.uniform(150, 160, 24),
            'volume': np.random.randint(50000, 100000, 24),
            'rsi': np.random.uniform(30, 70, 24)
        }, index=hourly_timestamps)

        # Daily data (7 days)
        daily_timestamps = pd.date_range(base_time.date(), periods=7, freq='1D')
        daily_df = pd.DataFrame({
            'open': np.random.uniform(150, 160, 7),
            'high': np.random.uniform(150, 160, 7),
            'low': np.random.uniform(150, 160, 7),
            'close': np.random.uniform(150, 160, 7),
            'volume': np.random.randint(1000000, 2000000, 7)
        }, index=daily_timestamps)

        # Events
        events = [
            MarketEvent(
                event_id="1",
                symbol="AAPL",
                timestamp=base_time + timedelta(hours=2),
                event_type=EventType.NEWS,
                content="Positive news",
                sentiment_score=0.8,
                importance_score=0.9
            ),
            MarketEvent(
                event_id="2",
                symbol="AAPL",
                timestamp=base_time + timedelta(hours=4),
                event_type=EventType.EARNINGS,
                content="Earnings beat",
                sentiment_score=0.9,
                importance_score=1.0
            )
        ]

        return {
            'minute_data': minute_df,
            'hourly_data': hourly_df,
            'daily_data': daily_df,
            'events': events,
            'time_range': (base_time, base_time + timedelta(days=1))
        }

    def test_multi_scale_sequence_creation(self):
        """Test MultiScaleSequence creation."""
        test_data = self.create_test_data()

        sequence = create_multi_scale_sequence(
            symbol="AAPL",
            time_range=test_data['time_range'],
            minute_data=test_data['minute_data'],
            hourly_data=test_data['hourly_data'],
            daily_data=test_data['daily_data'],
            events=test_data['events']
        )

        assert sequence.symbol == "AAPL"
        assert TimeScale.MINUTE in sequence.scales
        assert TimeScale.HOURLY in sequence.scales
        assert TimeScale.DAILY in sequence.scales
        assert sequence.event_sequence is not None
        assert len(sequence.event_sequence.events) == 2

    def test_get_features(self):
        """Test feature retrieval."""
        test_data = self.create_test_data()
        sequence = create_multi_scale_sequence(
            symbol="AAPL",
            time_range=test_data['time_range'],
            minute_data=test_data['minute_data'],
            hourly_data=test_data['hourly_data']
        )

        # Test OHLCV features
        minute_ohlcv = sequence.get_features(TimeScale.MINUTE, 'ohlcv')
        assert minute_ohlcv is not None
        assert minute_ohlcv.shape[1] == 5  # OHLCV

        # Test technical features
        minute_technical = sequence.get_features(TimeScale.MINUTE, 'technical')
        assert minute_technical is not None
        assert minute_technical.shape[1] == 2  # RSI, MACD

        # Test all features
        all_features = sequence.get_features(TimeScale.MINUTE, 'all')
        assert all_features is not None
        assert all_features.shape[1] == 7  # OHLCV + technical

    def test_get_aligned_features(self):
        """Test aligned feature retrieval."""
        test_data = self.create_test_data()
        sequence = create_multi_scale_sequence(
            symbol="AAPL",
            time_range=test_data['time_range'],
            minute_data=test_data['minute_data'],
            hourly_data=test_data['hourly_data']
        )

        aligned_features = sequence.get_aligned_features(
            primary_scale=TimeScale.MINUTE,
            context_scales=[TimeScale.HOURLY],
            alignment_method='ffill'
        )

        assert 'minute' in aligned_features
        assert 'hourly' in aligned_features

        # Both should have same number of timesteps (aligned to minute)
        minute_shape = aligned_features['minute'].shape
        hourly_shape = aligned_features['hourly'].shape
        assert minute_shape[0] == hourly_shape[0]  # Same length

    def test_create_sequence_tensor(self):
        """Test sequence tensor creation."""
        test_data = self.create_test_data()
        sequence = create_multi_scale_sequence(
            symbol="AAPL",
            time_range=test_data['time_range'],
            minute_data=test_data['minute_data'],
            events=test_data['events']
        )

        tensors = sequence.create_sequence_tensor(
            scale=TimeScale.MINUTE,
            sequence_length=60,  # 1 hour sequences
            step_size=30,        # 30-minute steps
            include_events=True
        )

        assert 'sequences' in tensors
        assert 'timestamps' in tensors
        assert 'event_features' in tensors

        sequences = tensors['sequences']
        n_sequences = sequences.shape[0]
        assert sequences.shape == (n_sequences, 60, 7)  # seq_len, features

        event_features = tensors['event_features']
        assert event_features.shape == (n_sequences, 60, 2)  # sentiment, importance

    def test_get_context_features(self):
        """Test context feature retrieval."""
        test_data = self.create_test_data()
        sequence = create_multi_scale_sequence(
            symbol="AAPL",
            time_range=test_data['time_range'],
            minute_data=test_data['minute_data'],
            hourly_data=test_data['hourly_data'],
            daily_data=test_data['daily_data']
        )

        context = sequence.get_context_features(
            primary_scale=TimeScale.MINUTE,
            context_window=24  # 24 hours
        )

        # Should have context from higher scales
        assert 'hourly' in context
        assert 'daily' in context

        # Context should be appropriately sized
        hourly_context = context['hourly']
        daily_context = context['daily']

        assert hourly_context.shape[0] <= 24  # At most 24 hours
        assert daily_context.shape[0] <= 1   # At most 1 day for 24-hour window

    def test_summary(self):
        """Test sequence summary."""
        test_data = self.create_test_data()
        sequence = create_multi_scale_sequence(
            symbol="AAPL",
            time_range=test_data['time_range'],
            minute_data=test_data['minute_data'],
            hourly_data=test_data['hourly_data'],
            events=test_data['events']
        )

        summary = sequence.summary()

        assert summary['symbol'] == "AAPL"
        assert 'time_range' in summary
        assert 'scales' in summary
        assert 'events' in summary

        # Check scale information
        assert 'minute' in summary['scales']
        assert 'hourly' in summary['scales']

        minute_info = summary['scales']['minute']
        assert minute_info['n_timesteps'] == 100
        assert minute_info['n_ohlcv_features'] == 5
        assert minute_info['n_technical_features'] == 2

        # Check event information
        events_info = summary['events']
        assert events_info['n_events'] == 2
        assert 'news' in events_info['event_types']
        assert 'earnings' in events_info['event_types']

    def test_validate(self):
        """Test sequence validation."""
        test_data = self.create_test_data()
        sequence = create_multi_scale_sequence(
            symbol="AAPL",
            time_range=test_data['time_range'],
            minute_data=test_data['minute_data'],
            hourly_data=test_data['hourly_data']
        )

        validation_results = sequence.validate()

        assert 'errors' in validation_results
        assert 'warnings' in validation_results
        assert isinstance(validation_results['errors'], list)
        assert isinstance(validation_results['warnings'], list)

    def test_get_events_for_sequence(self):
        """Test event alignment with sequence."""
        test_data = self.create_test_data()
        sequence = create_multi_scale_sequence(
            symbol="AAPL",
            time_range=test_data['time_range'],
            minute_data=test_data['minute_data'],
            events=test_data['events']
        )

        aligned_events = sequence.get_events_for_sequence(
            scale=TimeScale.MINUTE,
            tolerance=timedelta(minutes=30)
        )

        # Should find events aligned with minute timestamps
        assert isinstance(aligned_events, list)

        for timestamp_idx, events in aligned_events:
            assert isinstance(timestamp_idx, int)
            assert isinstance(events, list)
            assert all(isinstance(event, MarketEvent) for event in events)

class TestCreateMultiScaleSequence:
    """Test convenience function for creating multi-scale sequences."""

    def test_create_from_dataframes(self):
        """Test creation from DataFrame inputs."""
        base_time = datetime(2024, 1, 1, 9, 30)

        # Create minute data
        minute_timestamps = pd.date_range(base_time, periods=60, freq='1min')
        minute_df = pd.DataFrame({
            'open': np.random.rand(60) * 100 + 100,
            'high': np.random.rand(60) * 100 + 100,
            'low': np.random.rand(60) * 100 + 100,
            'close': np.random.rand(60) * 100 + 100,
            'volume': np.random.randint(1000, 10000, 60)
        }, index=minute_timestamps)

        sequence = create_multi_scale_sequence(
            symbol="TEST",
            time_range=(base_time, base_time + timedelta(hours=1)),
            minute_data=minute_df
        )

        assert sequence.symbol == "TEST"
        assert TimeScale.MINUTE in sequence.scales

        minute_features = sequence.get_features(TimeScale.MINUTE, 'ohlcv')
        assert minute_features is not None
        assert minute_features.shape == (60, 5)  # 60 timesteps, 5 OHLCV features

    def test_create_with_empty_data(self):
        """Test creation with empty/None data."""
        base_time = datetime(2024, 1, 1, 9, 30)

        sequence = create_multi_scale_sequence(
            symbol="EMPTY",
            time_range=(base_time, base_time + timedelta(hours=1)),
            minute_data=None,
            hourly_data=None
        )

        assert sequence.symbol == "EMPTY"
        assert len(sequence.scales) == 0
        assert sequence.event_sequence is None

class TestPerformance:
    """Test performance characteristics."""

    def test_large_sequence_creation(self):
        """Test creation with large amounts of data."""
        base_time = datetime(2024, 1, 1, 9, 30)

        # Large minute dataset (1 week = 2520 minutes of market hours)
        n_minutes = 2520
        minute_timestamps = pd.date_range(base_time, periods=n_minutes, freq='1min')

        minute_df = pd.DataFrame({
            'open': np.random.rand(n_minutes) * 100 + 100,
            'high': np.random.rand(n_minutes) * 100 + 100,
            'low': np.random.rand(n_minutes) * 100 + 100,
            'close': np.random.rand(n_minutes) * 100 + 100,
            'volume': np.random.randint(1000, 10000, n_minutes),
            'rsi': np.random.rand(n_minutes) * 100,
            'macd': np.random.rand(n_minutes) * 2 - 1
        }, index=minute_timestamps)

        # Should handle large datasets efficiently
        import time
        start_time = time.time()

        sequence = create_multi_scale_sequence(
            symbol="LARGE",
            time_range=(base_time, base_time + timedelta(days=7)),
            minute_data=minute_df
        )

        creation_time = time.time() - start_time

        assert creation_time < 5.0  # Should create within 5 seconds
        assert sequence is not None
        assert len(sequence.scales) == 1

        # Test tensor creation performance
        start_time = time.time()

        tensors = sequence.create_sequence_tensor(
            scale=TimeScale.MINUTE,
            sequence_length=120,  # 2 hours
            step_size=60          # 1 hour steps
        )

        tensor_time = time.time() - start_time

        assert tensor_time < 2.0  # Should create tensors within 2 seconds
        assert 'sequences' in tensors

        sequences = tensors['sequences']
        expected_sequences = (n_minutes - 120) // 60 + 1
        assert sequences.shape[0] <= expected_sequences
        assert sequences.shape == (sequences.shape[0], 120, 7)  # sequence_length, features

if __name__ == "__main__":
    pytest.main([__file__, "-v"])