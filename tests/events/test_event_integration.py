#!/usr/bin/env python3
"""
Tests for Event Integration Layer

Comprehensive tests for event integration components including event encoding,
temporal attention, and gated integration mechanisms.
"""

import pytest
import torch
from datetime import datetime, timedelta
from typing import List

from src.events.event_integration import (
    EventIntegrationLayer,
    EventEncoder,
    TemporalEventAttention,
    EventGatingMechanism,
    TemporalPositionEncoder,
    RelativePositionBias,
    MarketEvent,
    EventSequence,
    EventType,
    create_event_integration_layer
)


class TestMarketEvent:
    """Test MarketEvent functionality."""

    def test_market_event_creation(self):
        """Test MarketEvent creation."""
        event = MarketEvent(
            event_id="test_1",
            symbol="AAPL",
            timestamp=datetime(2024, 1, 1, 10, 30),
            event_type=EventType.NEWS,
            content="Positive earnings report",
            sentiment_score=0.8,
            importance_score=0.9,
            confidence_score=0.95
        )

        assert event.symbol == "AAPL"
        assert event.event_type == EventType.NEWS
        assert event.sentiment_score == 0.8
        assert event.importance_score == 0.9
        assert event.confidence_score == 0.95

    def test_event_to_tensor(self):
        """Test event tensor conversion."""
        event = MarketEvent(
            event_id="test_1",
            symbol="AAPL",
            timestamp=datetime(2024, 1, 1, 10, 30),
            event_type=EventType.EARNINGS,
            content="Beat estimates",
            sentiment_score=0.7,
            importance_score=0.8,
            confidence_score=0.9
        )

        tensor = event.to_tensor()

        assert tensor.shape == (4,)
        assert tensor[0].item() == 0.7  # sentiment
        assert tensor[1].item() == 0.8  # importance
        assert tensor[2].item() == 0.9  # confidence

        # Check event type normalization
        expected_type = list(EventType).index(EventType.EARNINGS) / len(EventType)
        assert abs(tensor[3].item() - expected_type) < 1e-6

    def test_event_to_tensor_device(self):
        """Test event tensor with device specification."""
        event = MarketEvent(
            event_id="test_1",
            symbol="AAPL",
            timestamp=datetime.now(),
            event_type=EventType.NEWS,
            content="Test",
            sentiment_score=0.5,
            importance_score=0.6
        )

        device = torch.device('cpu')
        tensor = event.to_tensor(device=device)

        assert tensor.device == device
        assert tensor.dtype == torch.float32


class TestEventSequence:
    """Test EventSequence functionality."""

    def create_test_events(self) -> List[MarketEvent]:
        """Create test events."""
        events = []
        base_time = datetime(2024, 1, 1, 9, 30)

        event_types = [EventType.NEWS, EventType.EARNINGS, EventType.UPGRADE, EventType.NEWS]
        sentiments = [0.8, 0.9, 0.7, -0.2]
        importance = [0.9, 1.0, 0.8, 0.6]

        for i in range(4):
            event = MarketEvent(
                event_id=f"event_{i}",
                symbol="AAPL",
                timestamp=base_time + timedelta(hours=i * 2),
                event_type=event_types[i],
                content=f"Event {i}",
                sentiment_score=sentiments[i],
                importance_score=importance[i]
            )
            events.append(event)

        return events

    def test_event_sequence_creation(self):
        """Test EventSequence creation."""
        events = self.create_test_events()
        time_range = (datetime(2024, 1, 1, 9, 0), datetime(2024, 1, 1, 17, 0))

        event_seq = EventSequence(
            events=events,
            time_range=time_range,
            symbol="AAPL"
        )

        assert len(event_seq.events) == 4
        assert event_seq.symbol == "AAPL"
        assert event_seq.time_range == time_range

    def test_get_events_in_window(self):
        """Test event window retrieval."""
        events = self.create_test_events()
        time_range = (datetime(2024, 1, 1, 9, 0), datetime(2024, 1, 1, 17, 0))

        event_seq = EventSequence(events=events, time_range=time_range, symbol="AAPL")

        # Get events around 11:30 (should find event at 11:30)
        center_time = datetime(2024, 1, 1, 11, 30)
        nearby_events = event_seq.get_events_in_window(center_time, window_hours=1.0)

        assert len(nearby_events) >= 1

        # Check that returned events are within window
        for event in nearby_events:
            time_diff = abs((event.timestamp - center_time).total_seconds() / 3600)
            assert time_diff <= 1.0

    def test_to_tensor_sequence(self):
        """Test conversion to tensor sequence."""
        events = self.create_test_events()
        time_range = (datetime(2024, 1, 1, 9, 0), datetime(2024, 1, 1, 17, 0))

        event_seq = EventSequence(events=events, time_range=time_range, symbol="AAPL")

        # Create timestamps matching event times
        timestamps = torch.tensor([
            [datetime(2024, 1, 1, 9, 30).timestamp()],   # First event
            [datetime(2024, 1, 1, 11, 30).timestamp()],  # Second event
            [datetime(2024, 1, 1, 13, 30).timestamp()]   # Third event
        ])

        event_features, event_mask = event_seq.to_tensor_sequence(
            timestamps, max_events_per_step=2
        )

        assert event_features.shape == (3, 1, 2, 4)  # batch, seq, max_events, event_dim
        assert event_mask.shape == (3, 1, 2)

        # Should have valid events at the right times
        assert event_mask.sum() > 0


class TestEventEncoder:
    """Test EventEncoder functionality."""

    def test_event_encoder_creation(self):
        """Test EventEncoder creation."""
        encoder = EventEncoder(
            event_dim=4,
            hidden_dim=64,
            output_dim=32,
            num_event_types=7
        )

        assert encoder.event_dim == 4
        assert encoder.hidden_dim == 64
        assert encoder.output_dim == 32

    def test_event_encoder_forward(self):
        """Test EventEncoder forward pass."""
        encoder = EventEncoder(event_dim=4, output_dim=32)

        # Create batch of event features
        batch_size = 2
        seq_len = 10
        max_events = 3

        event_features = torch.randn(batch_size, seq_len, max_events, 4)

        # Ensure valid event type values (0-1 range)
        event_features[..., 3] = torch.rand(batch_size, seq_len, max_events)

        encoded = encoder(event_features)

        assert encoded.shape == (batch_size, seq_len, max_events, 32)
        assert not torch.isnan(encoded).any()

    def test_event_encoder_single_event(self):
        """Test encoding single event."""
        encoder = EventEncoder(event_dim=4, output_dim=16)

        # Single event features [sentiment, importance, confidence, event_type]
        event_features = torch.tensor([[0.8, 0.9, 0.95, 0.5]])  # Shape: (1, 4)

        encoded = encoder(event_features)

        assert encoded.shape == (1, 16)
        assert not torch.isnan(encoded).any()


class TestTemporalPositionEncoder:
    """Test TemporalPositionEncoder functionality."""

    def test_temporal_position_encoder_creation(self):
        """Test TemporalPositionEncoder creation."""
        encoder = TemporalPositionEncoder(d_model=64)

        assert encoder.d_model == 64
        assert encoder.pe.shape[1] == 64

    def test_temporal_encoding_forward(self):
        """Test temporal position encoding."""
        encoder = TemporalPositionEncoder(d_model=32)

        batch_size = 2
        seq_len = 50
        x = torch.randn(batch_size, seq_len, 32)

        encoded = encoder(x)

        assert encoded.shape == (batch_size, seq_len, 32)
        assert not torch.isnan(encoded).any()

        # Should not be identical to input (position added)
        assert not torch.equal(encoded, x)

    def test_temporal_encoding_with_timestamps(self):
        """Test encoding with explicit timestamps."""
        encoder = TemporalPositionEncoder(d_model=16)

        batch_size = 1
        seq_len = 10
        x = torch.randn(batch_size, seq_len, 16)

        # Create timestamps
        timestamps = torch.arange(0, seq_len).unsqueeze(0).float()

        encoded = encoder(x, timestamps)

        assert encoded.shape == (batch_size, seq_len, 16)
        assert not torch.isnan(encoded).any()


class TestRelativePositionBias:
    """Test RelativePositionBias functionality."""

    def test_relative_position_bias_creation(self):
        """Test RelativePositionBias creation."""
        bias = RelativePositionBias(num_heads=4, max_relative_position=128)

        assert bias.num_heads == 4
        assert bias.max_relative_position == 128
        assert bias.relative_position_bias_table.shape == (255, 4)  # 2 * 128 - 1

    def test_relative_position_bias_forward(self):
        """Test relative position bias computation."""
        bias = RelativePositionBias(num_heads=2, max_relative_position=16)

        seq_len = 10
        bias_matrix = bias(seq_len)

        assert bias_matrix.shape == (2, seq_len, seq_len)  # num_heads, seq_len, seq_len
        assert not torch.isnan(bias_matrix).any()


class TestTemporalEventAttention:
    """Test TemporalEventAttention functionality."""

    def test_temporal_event_attention_creation(self):
        """Test TemporalEventAttention creation."""
        attention = TemporalEventAttention(
            d_model=64,
            event_dim=32,
            num_heads=4,
            dropout=0.1
        )

        assert attention.d_model == 64
        assert attention.event_dim == 32
        assert attention.num_heads == 4

    def test_temporal_event_attention_forward(self):
        """Test temporal event attention forward pass."""
        attention = TemporalEventAttention(d_model=32, event_dim=16, num_heads=2)

        batch_size = 2
        seq_len = 10
        max_events = 3

        sequence_features = torch.randn(batch_size, seq_len, 32)
        event_features = torch.randn(batch_size, seq_len, max_events, 16)
        event_mask = torch.randint(0, 2, (batch_size, seq_len, max_events)).float()

        enhanced_features, attention_weights = attention(
            sequence_features, event_features, event_mask
        )

        assert enhanced_features.shape == (batch_size, seq_len, 32)
        assert attention_weights.shape == (batch_size, seq_len, max_events)
        assert not torch.isnan(enhanced_features).any()
        assert not torch.isnan(attention_weights).any()

        # Attention weights should sum to 1 where mask is 1
        for b in range(batch_size):
            for t in range(seq_len):
                mask = event_mask[b, t]
                if mask.sum() > 0:
                    weights = attention_weights[b, t]
                    masked_weights = weights * mask
                    # Should sum to approximately 1 (within tolerance)
                    assert abs(masked_weights.sum().item() - 1.0) < 0.1

    def test_temporal_event_attention_with_timestamps(self):
        """Test attention with timestamp information."""
        attention = TemporalEventAttention(d_model=16, event_dim=8, num_heads=2)

        batch_size = 1
        seq_len = 5
        max_events = 2

        sequence_features = torch.randn(batch_size, seq_len, 16)
        event_features = torch.randn(batch_size, seq_len, max_events, 8)
        event_mask = torch.ones(batch_size, seq_len, max_events)
        timestamps = torch.arange(0, seq_len).unsqueeze(0).float()

        enhanced_features, attention_weights = attention(
            sequence_features, event_features, event_mask, timestamps
        )

        assert enhanced_features.shape == (batch_size, seq_len, 16)
        assert attention_weights.shape == (batch_size, seq_len, max_events)


class TestEventGatingMechanism:
    """Test EventGatingMechanism functionality."""

    def test_event_gating_creation(self):
        """Test EventGatingMechanism creation."""
        gating = EventGatingMechanism(d_model=64, dropout=0.1)

        assert gating.d_model == 64

    def test_event_gating_forward(self):
        """Test event gating forward pass."""
        gating = EventGatingMechanism(d_model=32)

        batch_size = 2
        seq_len = 10

        sequence_features = torch.randn(batch_size, seq_len, 32)
        event_context = torch.randn(batch_size, seq_len, 32)

        enhanced_features = gating(sequence_features, event_context)

        assert enhanced_features.shape == (batch_size, seq_len, 32)
        assert not torch.isnan(enhanced_features).any()

        # Output should be different from input (gated combination)
        assert not torch.equal(enhanced_features, sequence_features)


class TestEventIntegrationLayer:
    """Test complete EventIntegrationLayer functionality."""

    def test_event_integration_layer_creation(self):
        """Test EventIntegrationLayer creation."""
        layer = EventIntegrationLayer(
            d_model=64,
            event_dim=4,
            encoded_event_dim=32,
            num_attention_heads=4,
            dropout=0.1,
            max_events_per_step=5
        )

        assert layer.d_model == 64
        assert layer.max_events_per_step == 5

    def test_event_integration_layer_forward(self):
        """Test EventIntegrationLayer forward pass."""
        layer = EventIntegrationLayer(
            d_model=32,
            event_dim=4,
            encoded_event_dim=16,
            num_attention_heads=2,
            max_events_per_step=3
        )

        batch_size = 2
        seq_len = 20
        max_events = 3

        sequence_features = torch.randn(batch_size, seq_len, 32)
        event_features = torch.randn(batch_size, seq_len, max_events, 4)
        event_mask = torch.randint(0, 2, (batch_size, seq_len, max_events)).float()

        # Ensure event features are in valid ranges
        event_features[..., :3] = torch.sigmoid(event_features[..., :3])  # 0-1 for sentiment, importance, confidence
        event_features[..., 3] = torch.rand_like(event_features[..., 3])  # 0-1 for event type

        results = layer(sequence_features, event_features, event_mask)

        assert 'enhanced_features' in results
        assert 'attention_weights' in results
        assert 'event_impact_scores' in results
        assert 'encoded_events' in results

        enhanced_features = results['enhanced_features']
        attention_weights = results['attention_weights']
        event_impact_scores = results['event_impact_scores']

        assert enhanced_features.shape == (batch_size, seq_len, 32)
        assert attention_weights.shape == (batch_size, seq_len, max_events)
        assert event_impact_scores.shape == (batch_size, seq_len)

        assert not torch.isnan(enhanced_features).any()
        assert not torch.isnan(attention_weights).any()
        assert not torch.isnan(event_impact_scores).any()

        # Enhanced features should be different from input
        assert not torch.equal(enhanced_features, sequence_features)

    def test_event_integration_with_timestamps(self):
        """Test event integration with timestamp information."""
        layer = EventIntegrationLayer(d_model=16, event_dim=4, encoded_event_dim=8)

        batch_size = 1
        seq_len = 10
        max_events = 2

        sequence_features = torch.randn(batch_size, seq_len, 16)
        event_features = torch.rand(batch_size, seq_len, max_events, 4)  # Use rand for valid ranges
        event_mask = torch.ones(batch_size, seq_len, max_events)
        timestamps = torch.arange(0, seq_len).unsqueeze(0).float()

        results = layer(sequence_features, event_features, event_mask, timestamps)

        assert 'enhanced_features' in results
        assert results['enhanced_features'].shape == (batch_size, seq_len, 16)

    def test_get_event_importance(self):
        """Test event importance computation."""
        layer = EventIntegrationLayer(d_model=16, max_events_per_step=3)

        batch_size = 2
        seq_len = 5
        max_events = 3

        attention_weights = torch.rand(batch_size, seq_len, max_events)
        event_mask = torch.randint(0, 2, (batch_size, seq_len, max_events)).float()

        importance_scores = layer.get_event_importance(attention_weights, event_mask)

        assert importance_scores.shape == (batch_size, seq_len)
        assert not torch.isnan(importance_scores).any()
        assert (importance_scores >= 0).all()  # Importance should be non-negative


class TestCreateEventIntegrationLayer:
    """Test convenience function for creating event integration layer."""

    def test_create_event_integration_layer(self):
        """Test creation function."""
        layer = create_event_integration_layer(
            d_model=64,
            max_events_per_step=5
        )

        assert isinstance(layer, EventIntegrationLayer)
        assert layer.d_model == 64
        assert layer.max_events_per_step == 5


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_realistic_news_integration(self):
        """Test realistic news event integration scenario."""
        layer = create_event_integration_layer(d_model=64, max_events_per_step=3)

        batch_size = 1
        seq_len = 60  # 1 hour of minute data

        # Create realistic sequence features (price movements)
        sequence_features = torch.randn(batch_size, seq_len, 64) * 0.1  # Small price movements

        # Create realistic news events
        event_features = torch.zeros(batch_size, seq_len, 3, 4)
        event_mask = torch.zeros(batch_size, seq_len, 3)

        # Add a positive news event at minute 20
        event_features[0, 20, 0] = torch.tensor([0.8, 0.9, 0.95, 0.14])  # Positive news
        event_mask[0, 20, 0] = 1.0

        # Add an earnings announcement at minute 40
        event_features[0, 40, 0] = torch.tensor([0.9, 1.0, 0.98, 0.28])  # Earnings (stronger)
        event_mask[0, 40, 0] = 1.0

        results = layer(sequence_features, event_features, event_mask)

        enhanced_features = results['enhanced_features']
        attention_weights = results['attention_weights']
        event_impact_scores = results['event_impact_scores']

        # Check that events had impact
        assert event_impact_scores[0, 20].item() > 0.1  # News should have impact
        assert event_impact_scores[0, 40].item() > 0.1  # Earnings should have impact

        # Earnings should have higher impact than news
        assert event_impact_scores[0, 40].item() > event_impact_scores[0, 20].item()

        # Enhanced features should be different at event times
        assert not torch.equal(enhanced_features[0, 20], sequence_features[0, 20])
        assert not torch.equal(enhanced_features[0, 40], sequence_features[0, 40])

    def test_no_events_scenario(self):
        """Test scenario with no events."""
        layer = create_event_integration_layer(d_model=32)

        batch_size = 2
        seq_len = 30

        sequence_features = torch.randn(batch_size, seq_len, 32)
        event_features = torch.zeros(batch_size, seq_len, 5, 4)
        event_mask = torch.zeros(batch_size, seq_len, 5)  # No events

        results = layer(sequence_features, event_features, event_mask)

        enhanced_features = results['enhanced_features']
        event_impact_scores = results['event_impact_scores']

        # With no events, impact should be minimal
        assert event_impact_scores.max().item() < 0.5

        # Enhanced features should still be processed (residual connections)
        assert not torch.equal(enhanced_features, sequence_features)

    def test_multiple_concurrent_events(self):
        """Test scenario with multiple events at same time."""
        layer = create_event_integration_layer(d_model=16, max_events_per_step=3)

        batch_size = 1
        seq_len = 10

        sequence_features = torch.randn(batch_size, seq_len, 16)
        event_features = torch.zeros(batch_size, seq_len, 3, 4)
        event_mask = torch.zeros(batch_size, seq_len, 3)

        # Add multiple events at same timestep
        timestep = 5
        event_features[0, timestep, 0] = torch.tensor([0.7, 0.8, 0.9, 0.14])  # News
        event_features[0, timestep, 1] = torch.tensor([0.9, 0.95, 0.98, 0.28])  # Earnings
        event_features[0, timestep, 2] = torch.tensor([0.6, 0.7, 0.8, 0.42])  # Upgrade

        event_mask[0, timestep, :] = 1.0  # All events active

        results = layer(sequence_features, event_features, event_mask)

        attention_weights = results['attention_weights']
        event_impact_scores = results['event_impact_scores']

        # Should attend to all events at that timestep
        timestep_attention = attention_weights[0, timestep, :]
        assert timestep_attention.sum().item() > 2.8  # Close to 3 (all events)

        # Should have high impact due to multiple events
        assert event_impact_scores[0, timestep].item() > 0.3


class TestGradientFlow:
    """Test gradient flow through the integration layer."""

    def test_gradient_flow(self):
        """Test that gradients flow properly through the layer."""
        layer = create_event_integration_layer(d_model=16)

        batch_size = 1
        seq_len = 5
        max_events = 2

        sequence_features = torch.randn(batch_size, seq_len, 16, requires_grad=True)
        event_features = torch.rand(batch_size, seq_len, max_events, 4, requires_grad=True)
        event_mask = torch.ones(batch_size, seq_len, max_events)

        results = layer(sequence_features, event_features, event_mask)

        # Compute a simple loss
        loss = results['enhanced_features'].sum()
        loss.backward()

        # Check that gradients exist
        assert sequence_features.grad is not None
        assert event_features.grad is not None

        # Gradients should be non-zero
        assert sequence_features.grad.abs().max() > 0
        assert event_features.grad.abs().max() > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])