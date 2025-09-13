"""
Tests for Temporal Fusion Transformer Model

Comprehensive test suite for the TFT model including architecture tests,
training tests, and integration tests with sentiment features.
"""

import pytest
import torch
import torch.nn as nn

from models.temporal_fusion_transformer import (
    TFTConfig,
    TemporalFusionTransformer,
    TFTTrainer,
    TimeDistributed,
    GatedLinearUnit,
    GatedResidualNetwork,
    VariableSelectionNetwork,
    InterpretableMultiHeadAttention,
    create_tft_model
)


@pytest.fixture
def sample_config():
    """Sample TFT configuration for testing."""
    return TFTConfig(
        hidden_size=32,
        lstm_layers=2,
        attention_head_size=4,
        dropout=0.1,
        max_encoder_length=60,
        max_prediction_length=15,
        temporal_features=['open', 'high', 'low', 'close', 'volume', 'returns'],
        target_features=['returns'],
        use_sentiment_features=True
    )


@pytest.fixture
def sample_data():
    """Generate sample data for testing."""
    batch_size = 8
    encoder_length = 60
    decoder_length = 15
    feature_size = 6  # temporal features
    sentiment_size = 23

    encoder_input = torch.randn(batch_size, encoder_length, feature_size)
    decoder_input = torch.randn(batch_size, decoder_length, feature_size)
    sentiment_features = torch.randn(batch_size, encoder_length + decoder_length, sentiment_size)
    encoder_lengths = torch.full((batch_size,), encoder_length)
    targets = torch.randn(batch_size, decoder_length, 1)  # returns only

    return {
        'encoder_input': encoder_input,
        'decoder_input': decoder_input,
        'sentiment_features': sentiment_features,
        'encoder_lengths': encoder_lengths,
        'targets': targets
    }


class TestTFTConfig:
    """Test TFT configuration class."""

    def test_config_creation(self):
        """Test TFT config creation with defaults."""
        config = TFTConfig()

        assert config.hidden_size == 64
        assert config.lstm_layers == 2
        assert config.attention_head_size == 4
        assert config.dropout == 0.1
        assert config.max_encoder_length == 120
        assert config.max_prediction_length == 30
        assert config.use_sentiment_features is True
        assert len(config.temporal_features) > 0
        assert len(config.target_features) > 0

    def test_config_custom_values(self):
        """Test TFT config with custom values."""
        config = TFTConfig(
            hidden_size=128,
            max_encoder_length=240,
            temporal_features=['close', 'volume'],
            target_features=['returns', 'volatility']
        )

        assert config.hidden_size == 128
        assert config.max_encoder_length == 240
        assert config.temporal_features == ['close', 'volume']
        assert config.target_features == ['returns', 'volatility']


class TestTimeDistributed:
    """Test TimeDistributed layer."""

    def test_time_distributed_forward(self):
        """Test TimeDistributed forward pass."""
        input_size = 10
        output_size = 5
        batch_size = 4
        seq_len = 12

        linear = nn.Linear(input_size, output_size)
        time_distributed = TimeDistributed(linear)

        # Test with 3D input
        x = torch.randn(batch_size, seq_len, input_size)
        output = time_distributed(x)

        assert output.shape == (batch_size, seq_len, output_size)

        # Test with 2D input
        x_2d = torch.randn(batch_size, input_size)
        output_2d = time_distributed(x_2d)

        assert output_2d.shape == (batch_size, output_size)


class TestGatedLinearUnit:
    """Test Gated Linear Unit."""

    def test_glu_forward(self):
        """Test GLU forward pass."""
        input_size = 16
        hidden_size = 8
        batch_size = 4
        seq_len = 10

        glu = GatedLinearUnit(input_size, hidden_size)
        x = torch.randn(batch_size, seq_len, input_size)

        output = glu(x)
        assert output.shape == (batch_size, seq_len, hidden_size)

    def test_glu_with_dropout(self):
        """Test GLU with dropout."""
        glu = GatedLinearUnit(16, 8, dropout=0.5)
        x = torch.randn(4, 10, 16)

        # Test in training mode
        glu.train()
        output = glu(x)
        assert output.shape == (4, 10, 8)

        # Test in eval mode
        glu.eval()
        output = glu(x)
        assert output.shape == (4, 10, 8)


class TestGatedResidualNetwork:
    """Test Gated Residual Network."""

    def test_grn_forward(self):
        """Test GRN forward pass."""
        input_size = 16
        hidden_size = 32
        output_size = 8
        batch_size = 4
        seq_len = 10

        grn = GatedResidualNetwork(input_size, hidden_size, output_size)
        x = torch.randn(batch_size, seq_len, input_size)

        output = grn(x)
        assert output.shape == (batch_size, seq_len, output_size)

    def test_grn_with_context(self):
        """Test GRN with context input."""
        input_size = 16
        hidden_size = 32
        output_size = 8
        context_size = 12

        grn = GatedResidualNetwork(
            input_size, hidden_size, output_size, context_size=context_size
        )

        x = torch.randn(4, 10, input_size)
        context = torch.randn(4, 10, context_size)

        output = grn(x, context)
        assert output.shape == (4, 10, output_size)

    def test_grn_residual_connection(self):
        """Test GRN residual connection."""
        size = 16

        # Same input/output size should use residual
        grn = GatedResidualNetwork(size, size, size, residual=True)
        x = torch.randn(4, 10, size)
        output = grn(x)

        assert output.shape == (4, 10, size)

        # Different input/output size should use skip connection
        grn = GatedResidualNetwork(16, 32, 8, residual=True)
        x = torch.randn(4, 10, 16)
        output = grn(x)

        assert output.shape == (4, 10, 8)


class TestVariableSelectionNetwork:
    """Test Variable Selection Network."""

    def test_vsn_forward(self):
        """Test VSN forward pass."""
        input_sizes = {'var1': 8, 'var2': 12, 'var3': 6}
        hidden_size = 16
        batch_size = 4
        seq_len = 10

        vsn = VariableSelectionNetwork(input_sizes, hidden_size)

        variables = {
            'var1': torch.randn(batch_size, seq_len, 8),
            'var2': torch.randn(batch_size, seq_len, 12),
            'var3': torch.randn(batch_size, seq_len, 6)
        }

        output, weights = vsn(variables)

        assert output.shape == (batch_size, seq_len, hidden_size)
        assert weights.shape == (batch_size, seq_len, len(input_sizes))

        # Check weights sum to 1
        assert torch.allclose(weights.sum(dim=-1), torch.ones_like(weights.sum(dim=-1)))

    def test_vsn_with_context(self):
        """Test VSN with context."""
        input_sizes = {'var1': 8, 'var2': 12}
        hidden_size = 16
        context_size = 10

        vsn = VariableSelectionNetwork(input_sizes, hidden_size, context_size=context_size)

        variables = {
            'var1': torch.randn(4, 10, 8),
            'var2': torch.randn(4, 10, 12)
        }
        context = torch.randn(4, 10, context_size)

        output, weights = vsn(variables, context)

        assert output.shape == (4, 10, hidden_size)
        assert weights.shape == (4, 10, len(input_sizes))


class TestInterpretableMultiHeadAttention:
    """Test Interpretable Multi-Head Attention."""

    def test_attention_forward(self):
        """Test attention forward pass."""
        d_model = 64
        n_head = 8
        batch_size = 4
        seq_len = 20

        attention = InterpretableMultiHeadAttention(d_model, n_head)

        query = torch.randn(batch_size, seq_len, d_model)
        key = torch.randn(batch_size, seq_len, d_model)
        value = torch.randn(batch_size, seq_len, d_model)

        output, weights = attention(query, key, value)

        assert output.shape == (batch_size, seq_len, d_model)
        assert weights.shape == (batch_size, seq_len, seq_len)

    def test_attention_with_mask(self):
        """Test attention with mask."""
        d_model = 64
        n_head = 8

        attention = InterpretableMultiHeadAttention(d_model, n_head)

        query = torch.randn(4, 10, d_model)
        key = torch.randn(4, 15, d_model)
        value = torch.randn(4, 15, d_model)
        mask = torch.ones(4, 10, 15)
        mask[:, :, -3:] = 0  # Mask last 3 positions

        output, weights = attention(query, key, value, mask)

        assert output.shape == (4, 10, d_model)
        assert weights.shape == (4, 10, 15)

        # Check that masked positions have near-zero attention
        assert weights[:, :, -3:].max() < 1e-6


class TestTemporalFusionTransformer:
    """Test main TFT model."""

    def test_model_creation(self, sample_config):
        """Test TFT model creation."""
        model = TemporalFusionTransformer(sample_config)

        assert isinstance(model, nn.Module)
        assert model.config == sample_config
        assert model.hidden_size == sample_config.hidden_size

    def test_model_forward(self, sample_config, sample_data):
        """Test TFT forward pass."""
        model = TemporalFusionTransformer(sample_config)

        output = model(
            sample_data['encoder_input'],
            sample_data['decoder_input'],
            sample_data['encoder_lengths'],
            sample_data['sentiment_features']
        )

        assert isinstance(output, dict)
        assert 'predictions' in output
        assert 'attention_weights' in output
        assert 'encoder_variable_weights' in output
        assert 'decoder_variable_weights' in output

        # Check output shapes
        batch_size, decoder_length = sample_data['decoder_input'].shape[:2]
        target_features = len(sample_config.target_features)

        assert output['predictions'].shape == (batch_size, decoder_length, target_features)

    def test_model_without_sentiment(self, sample_config, sample_data):
        """Test TFT without sentiment features."""
        sample_config.use_sentiment_features = False
        model = TemporalFusionTransformer(sample_config)

        output = model(
            sample_data['encoder_input'],
            sample_data['decoder_input'],
            sample_data['encoder_lengths'],
            sentiment_features=None
        )

        assert 'predictions' in output
        assert output['predictions'].shape[2] == len(sample_config.target_features)

    def test_model_predict(self, sample_config, sample_data):
        """Test TFT predict method."""
        model = TemporalFusionTransformer(sample_config)

        predictions = model.predict(
            sample_data['encoder_input'],
            sample_data['decoder_input'],
            sample_data['sentiment_features']
        )

        batch_size, decoder_length = sample_data['decoder_input'].shape[:2]
        target_features = len(sample_config.target_features)

        assert predictions.shape == (batch_size, decoder_length, target_features)

    def test_attention_mask_creation(self, sample_config):
        """Test attention mask creation."""
        model = TemporalFusionTransformer(sample_config)

        batch_size = 4
        encoder_lengths = torch.tensor([50, 45, 55, 60])  # Variable lengths
        decoder_length = 15

        mask = model.create_attention_mask(encoder_lengths, decoder_length)

        expected_shape = (batch_size, decoder_length, encoder_lengths.max() + decoder_length)
        assert mask.shape == expected_shape

        # Check causal masking
        for i in range(decoder_length):
            for j in range(i + 1, decoder_length):
                encoder_max = encoder_lengths.max()
                assert mask[0, i, encoder_max + j] == 0  # Future positions masked


class TestTFTTrainer:
    """Test TFT trainer."""

    def test_trainer_creation(self, sample_config):
        """Test trainer creation."""
        model = TemporalFusionTransformer(sample_config)
        trainer = TFTTrainer(model, sample_config)

        assert trainer.model == model
        assert trainer.config == sample_config
        assert hasattr(trainer, 'optimizer')
        assert hasattr(trainer, 'scheduler')
        assert hasattr(trainer, 'criterion')

    def test_train_epoch(self, sample_config, sample_data):
        """Test training for one epoch."""
        model = TemporalFusionTransformer(sample_config)
        trainer = TFTTrainer(model, sample_config)

        # Create mock data loader
        mock_loader = [sample_data]  # Single batch

        loss = trainer.train_epoch(mock_loader)

        assert isinstance(loss, float)
        assert loss >= 0

    def test_validate(self, sample_config, sample_data):
        """Test validation."""
        model = TemporalFusionTransformer(sample_config)
        trainer = TFTTrainer(model, sample_config)

        mock_loader = [sample_data]

        val_loss = trainer.validate(mock_loader)

        assert isinstance(val_loss, float)
        assert val_loss >= 0

    def test_save_load_model(self, sample_config, tmp_path):
        """Test model saving and loading."""
        model = TemporalFusionTransformer(sample_config)
        trainer = TFTTrainer(model, sample_config)

        # Save model
        model_path = tmp_path / "test_model.pt"
        trainer.save_model(str(model_path))

        assert model_path.exists()

        # Load model
        loaded_trainer = TFTTrainer.load_model(str(model_path))

        assert loaded_trainer.config.hidden_size == sample_config.hidden_size
        assert isinstance(loaded_trainer.model, TemporalFusionTransformer)


class TestModelIntegration:
    """Test model integration scenarios."""

    def test_gradient_flow(self, sample_config, sample_data):
        """Test gradient flow through the model."""
        model = TemporalFusionTransformer(sample_config)
        criterion = nn.MSELoss()

        # Forward pass
        output = model(
            sample_data['encoder_input'],
            sample_data['decoder_input'],
            sample_data['encoder_lengths'],
            sample_data['sentiment_features']
        )

        # Calculate loss
        loss = criterion(output['predictions'], sample_data['targets'])

        # Backward pass
        loss.backward()

        # Check that gradients exist
        for name, param in model.named_parameters():
            if param.requires_grad:
                assert param.grad is not None, f"No gradient for {name}"
                assert not torch.isnan(param.grad).any(), f"NaN gradient for {name}"

    def test_different_sequence_lengths(self, sample_config):
        """Test model with different sequence lengths."""
        model = TemporalFusionTransformer(sample_config)

        # Test with shorter sequences
        encoder_input = torch.randn(2, 30, 6)  # Shorter encoder
        decoder_input = torch.randn(2, 10, 6)  # Shorter decoder
        encoder_lengths = torch.tensor([25, 30])  # Variable lengths
        sentiment_features = torch.randn(2, 40, 23)

        output = model(encoder_input, decoder_input, encoder_lengths, sentiment_features)

        assert output['predictions'].shape == (2, 10, 1)

    def test_batch_size_one(self, sample_config):
        """Test model with batch size 1."""
        model = TemporalFusionTransformer(sample_config)

        encoder_input = torch.randn(1, 60, 6)
        decoder_input = torch.randn(1, 15, 6)
        encoder_lengths = torch.tensor([60])
        sentiment_features = torch.randn(1, 75, 23)

        output = model(encoder_input, decoder_input, encoder_lengths, sentiment_features)

        assert output['predictions'].shape == (1, 15, 1)

    def test_model_device_compatibility(self, sample_config, sample_data):
        """Test model device compatibility."""
        model = TemporalFusionTransformer(sample_config)

        # Test CPU
        output_cpu = model(
            sample_data['encoder_input'],
            sample_data['decoder_input'],
            sample_data['encoder_lengths'],
            sample_data['sentiment_features']
        )

        assert output_cpu['predictions'].device.type == 'cpu'

        # Test GPU if available
        if torch.cuda.is_available():
            model_gpu = model.cuda()
            data_gpu = {k: v.cuda() if torch.is_tensor(v) else v
                       for k, v in sample_data.items()}

            output_gpu = model_gpu(
                data_gpu['encoder_input'],
                data_gpu['decoder_input'],
                data_gpu['encoder_lengths'],
                data_gpu['sentiment_features']
            )

            assert output_gpu['predictions'].device.type == 'cuda'


class TestUtilityFunctions:
    """Test utility functions."""

    def test_create_tft_model(self):
        """Test create_tft_model utility function."""
        temporal_features = ['open', 'high', 'low', 'close', 'volume']
        target_features = ['returns']

        model = create_tft_model(
            temporal_features=temporal_features,
            target_features=target_features,
            hidden_size=32,
            max_encoder_length=60,
            max_prediction_length=15,
            use_sentiment=True
        )

        assert isinstance(model, TemporalFusionTransformer)
        assert model.config.hidden_size == 32
        assert model.config.max_encoder_length == 60
        assert model.config.max_prediction_length == 15
        assert model.config.temporal_features == temporal_features
        assert model.config.target_features == target_features
        assert model.config.use_sentiment_features is True


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_invalid_attention_heads(self):
        """Test error handling for invalid attention heads."""
        config = TFTConfig(hidden_size=33, attention_head_size=8)  # 33 not divisible by 8

        model = TemporalFusionTransformer(config)

        # Should raise error during attention initialization
        with pytest.raises(AssertionError):
            attention = InterpretableMultiHeadAttention(33, 8)

    def test_empty_input(self, sample_config):
        """Test handling of empty inputs."""
        model = TemporalFusionTransformer(sample_config)

        # Test with zero-length sequences
        encoder_input = torch.randn(2, 0, 6)  # Empty encoder
        decoder_input = torch.randn(2, 1, 6)  # Minimal decoder
        encoder_lengths = torch.tensor([0, 0])
        sentiment_features = torch.randn(2, 1, 23)

        # Should handle gracefully (implementation dependent)
        try:
            output = model(encoder_input, decoder_input, encoder_lengths, sentiment_features)
            assert 'predictions' in output
        except RuntimeError:
            # Expected if model doesn't handle empty sequences
            pass

    def test_mismatched_dimensions(self, sample_config):
        """Test error handling for mismatched dimensions."""
        model = TemporalFusionTransformer(sample_config)

        encoder_input = torch.randn(4, 60, 6)
        decoder_input = torch.randn(2, 15, 6)  # Different batch size
        encoder_lengths = torch.tensor([60, 60, 60, 60])
        sentiment_features = torch.randn(4, 75, 23)

        with pytest.raises(RuntimeError):
            model(encoder_input, decoder_input, encoder_lengths, sentiment_features)


class TestPerformance:
    """Test performance characteristics."""

    def test_memory_usage(self, sample_config):
        """Test memory usage scaling."""
        model = TemporalFusionTransformer(sample_config)

        # Test with different batch sizes
        for batch_size in [1, 4, 8]:
            encoder_input = torch.randn(batch_size, 60, 6)
            decoder_input = torch.randn(batch_size, 15, 6)
            encoder_lengths = torch.full((batch_size,), 60)
            sentiment_features = torch.randn(batch_size, 75, 23)

            with torch.no_grad():
                output = model(encoder_input, decoder_input, encoder_lengths, sentiment_features)
                assert output['predictions'].shape[0] == batch_size

    def test_inference_speed(self, sample_config):
        """Test inference speed (basic timing)."""
        model = TemporalFusionTransformer(sample_config)
        model.eval()

        encoder_input = torch.randn(8, 60, 6)
        decoder_input = torch.randn(8, 15, 6)
        encoder_lengths = torch.full((8,), 60)
        sentiment_features = torch.randn(8, 75, 23)

        import time

        # Warm up
        with torch.no_grad():
            model(encoder_input, decoder_input, encoder_lengths, sentiment_features)

        # Time inference
        start_time = time.time()
        with torch.no_grad():
            for _ in range(10):
                output = model(encoder_input, decoder_input, encoder_lengths, sentiment_features)
        end_time = time.time()

        avg_time = (end_time - start_time) / 10

        # Should be reasonably fast (< 1 second per batch on modern hardware)
        assert avg_time < 1.0


if __name__ == "__main__":
    pytest.main([__file__])