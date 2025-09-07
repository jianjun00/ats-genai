"""
Tests for Support/Resistance ML Model
"""

import pytest
import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch
import tempfile
import os

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from domains.ml.services.models.support_resistance_model import (
    SRModelConfig,
    SupportResistanceNet,
    SRLoss,
    SRDataset,
    SupportResistanceEnsemble
)
from domains.ml.services.training_data.support_resistance_generator import (
    TrainingExample,
    SupportResistanceLevel
)

class TestSRModelConfig:
    """Test suite for SRModelConfig"""

    def test_config_creation(self):
        """Test creating model configuration"""
        config = SRModelConfig(
            input_dim=50,
            hidden_dims=[128, 64],
            max_support_levels=3,
            max_resistance_levels=3,
            epochs=50
        )

        assert config.input_dim == 50
        assert config.hidden_dims == [128, 64]
        assert config.max_support_levels == 3
        assert config.max_resistance_levels == 3
        assert config.epochs == 50
        assert config.dropout_rate == 0.3  # Default value
        assert config.learning_rate == 0.001  # Default value

    def test_config_defaults(self):
        """Test default configuration values"""
        config = SRModelConfig()

        assert config.input_dim == 50
        assert config.hidden_dims == [256, 128, 64]  # Default from __post_init__
        assert config.dropout_rate == 0.3
        assert config.activation == 'relu'
        assert config.max_support_levels == 3
        assert config.max_resistance_levels == 3
        assert config.predict_confidence is True
        assert config.batch_size == 64
        assert config.learning_rate == 0.001
        assert config.epochs == 100

    def test_config_validation(self):
        """Test configuration validation"""
        # Valid config
        config = SRModelConfig(
            input_dim=25,
            hidden_dims=[64, 32],
            dropout_rate=0.5,
            learning_rate=0.01
        )

        assert config.input_dim > 0
        assert len(config.hidden_dims) > 0
        assert 0 <= config.dropout_rate <= 1
        assert config.learning_rate > 0
        assert config.epochs > 0

class TestSRDataset:
    """Test suite for SRDataset"""

    @pytest.fixture
    def sample_data(self):
        """Create sample data for dataset testing"""
        batch_size = 10
        feature_dim = 20
        max_levels = 3

        features = np.random.randn(batch_size, feature_dim)
        support_levels = np.random.uniform(90, 95, (batch_size, max_levels))
        resistance_levels = np.random.uniform(105, 110, (batch_size, max_levels))
        support_confidence = np.random.uniform(0, 1, (batch_size, max_levels))
        resistance_confidence = np.random.uniform(0, 1, (batch_size, max_levels))

        return (features, support_levels, resistance_levels,
                support_confidence, resistance_confidence)

    def test_dataset_creation(self, sample_data):
        """Test creating SRDataset"""
        features, support_levels, resistance_levels, support_conf, resistance_conf = sample_data

        dataset = SRDataset(features, support_levels, resistance_levels,
                           support_conf, resistance_conf)

        assert len(dataset) == len(features)
        assert isinstance(dataset.features, torch.Tensor)
        assert isinstance(dataset.support_levels, torch.Tensor)
        assert isinstance(dataset.resistance_levels, torch.Tensor)

    def test_dataset_getitem(self, sample_data):
        """Test getting items from dataset"""
        features, support_levels, resistance_levels, support_conf, resistance_conf = sample_data

        dataset = SRDataset(features, support_levels, resistance_levels,
                           support_conf, resistance_conf)

        item = dataset[0]

        assert isinstance(item, dict)
        assert 'features' in item
        assert 'support_levels' in item
        assert 'resistance_levels' in item
        assert 'support_confidence' in item
        assert 'resistance_confidence' in item

        # Check tensor shapes
        assert item['features'].shape == (features.shape[1],)
        assert item['support_levels'].shape == (support_levels.shape[1],)

class TestSupportResistanceNet:
    """Test suite for SupportResistanceNet"""

    @pytest.fixture
    def config(self):
        """Create test configuration"""
        return SRModelConfig(
            input_dim=20,
            hidden_dims=[64, 32],
            max_support_levels=2,
            max_resistance_levels=2,
            dropout_rate=0.1
        )

    def test_model_creation(self, config):
        """Test creating neural network model"""
        model = SupportResistanceNet(config)

        assert isinstance(model, nn.Module)
        assert hasattr(model, 'feature_extractor')
        assert hasattr(model, 'support_levels_head')
        assert hasattr(model, 'resistance_levels_head')
        assert hasattr(model, 'support_confidence_head')
        assert hasattr(model, 'resistance_confidence_head')

    def test_model_forward_pass(self, config):
        """Test forward pass through the model"""
        model = SupportResistanceNet(config)
        model.eval()

        batch_size = 5
        features = torch.randn(batch_size, config.input_dim)

        with torch.no_grad():
            output = model(features)

        assert isinstance(output, dict)
        assert 'support_levels' in output
        assert 'resistance_levels' in output
        assert 'support_confidence' in output
        assert 'resistance_confidence' in output
        assert 'support_attention' in output
        assert 'resistance_attention' in output

        # Check output shapes
        assert output['support_levels'].shape == (batch_size, config.max_support_levels)
        assert output['resistance_levels'].shape == (batch_size, config.max_resistance_levels)
        assert output['support_confidence'].shape == (batch_size, config.max_support_levels)
        assert output['resistance_confidence'].shape == (batch_size, config.max_resistance_levels)

    def test_activation_functions(self, config):
        """Test different activation functions"""
        activations = ['relu', 'leaky_relu', 'elu', 'swish', 'gelu']

        for activation in activations:
            config.activation = activation
            model = SupportResistanceNet(config)

            # Should create without error
            assert isinstance(model, nn.Module)

    def test_model_output_ranges(self, config):
        """Test that model outputs are in expected ranges"""
        model = SupportResistanceNet(config)
        model.eval()

        features = torch.randn(1, config.input_dim)

        with torch.no_grad():
            output = model(features)

        # Confidence scores should be between 0 and 1
        assert torch.all(output['support_confidence'] >= 0)
        assert torch.all(output['support_confidence'] <= 1)
        assert torch.all(output['resistance_confidence'] >= 0)
        assert torch.all(output['resistance_confidence'] <= 1)

        # Attention weights should sum to 1
        support_attention_sum = torch.sum(output['support_attention'], dim=1)
        resistance_attention_sum = torch.sum(output['resistance_attention'], dim=1)
        assert torch.allclose(support_attention_sum, torch.ones_like(support_attention_sum))
        assert torch.allclose(resistance_attention_sum, torch.ones_like(resistance_attention_sum))

class TestSRLoss:
    """Test suite for SRLoss"""

    @pytest.fixture
    def config(self):
        """Create test configuration"""
        return SRModelConfig(
            max_support_levels=2,
            max_resistance_levels=2,
            level_weight=1.0,
            confidence_weight=0.5,
            ranking_weight=0.3
        )

    @pytest.fixture
    def sample_predictions(self):
        """Create sample predictions"""
        batch_size = 3
        return {
            'support_levels': torch.randn(batch_size, 2),
            'resistance_levels': torch.randn(batch_size, 2),
            'support_confidence': torch.sigmoid(torch.randn(batch_size, 2)),
            'resistance_confidence': torch.sigmoid(torch.randn(batch_size, 2))
        }

    @pytest.fixture
    def sample_targets(self):
        """Create sample targets"""
        batch_size = 3
        return {
            'support_levels': torch.randn(batch_size, 2),
            'resistance_levels': torch.randn(batch_size, 2),
            'support_confidence': torch.rand(batch_size, 2),
            'resistance_confidence': torch.rand(batch_size, 2)
        }

    def test_loss_creation(self, config):
        """Test creating loss function"""
        loss_fn = SRLoss(config)

        assert isinstance(loss_fn, nn.Module)
        assert loss_fn.config == config

    def test_loss_computation(self, config, sample_predictions, sample_targets):
        """Test loss computation"""
        loss_fn = SRLoss(config)

        total_loss, loss_components = loss_fn(sample_predictions, sample_targets)

        assert isinstance(total_loss, torch.Tensor)
        assert total_loss.dim() == 0  # Scalar
        assert total_loss.item() >= 0  # Loss should be non-negative

        assert isinstance(loss_components, dict)
        assert 'support_levels' in loss_components
        assert 'resistance_levels' in loss_components
        assert 'support_confidence' in loss_components
        assert 'resistance_confidence' in loss_components

    def test_loss_components_weights(self, config, sample_predictions, sample_targets):
        """Test that loss weights are applied correctly"""
        loss_fn = SRLoss(config)

        total_loss, loss_components = loss_fn(sample_predictions, sample_targets)

        # All components should contribute to total loss
        for component_name, component_loss in loss_components.items():
            assert isinstance(component_loss, torch.Tensor)
            assert component_loss.item() >= 0

    def test_ranking_loss(self, config):
        """Test ranking loss component"""
        loss_fn = SRLoss(config)

        # Create simple test case
        levels = torch.tensor([[100.0, 101.0]])
        confidence = torch.tensor([[0.8, 0.3]])  # First level should be more confident

        ranking_loss = loss_fn._ranking_loss(levels, confidence)

        assert isinstance(ranking_loss, torch.Tensor)
        assert ranking_loss.item() >= 0

class TestSupportResistanceEnsemble:
    """Test suite for SupportResistanceEnsemble"""

    @pytest.fixture
    def config(self):
        """Create test configuration"""
        return SRModelConfig(
            input_dim=10,
            hidden_dims=[32, 16],
            max_support_levels=2,
            max_resistance_levels=2,
            epochs=5,  # Small for testing
            batch_size=8
        )

    @pytest.fixture
    def sample_training_examples(self):
        """Create sample training examples"""
        examples = []

        for i in range(20):  # Small dataset for testing
            example = TrainingExample(
                symbol='TEST',
                date=pd.Timestamp('2023-01-01') + pd.Timedelta(days=i),
                features={f'feature_{j}': np.random.randn() for j in range(10)},
                next_day_support_levels=[
                    SupportResistanceLevel(
                        level=95.0 + np.random.randn(),
                        level_type='support',
                        strength=np.random.uniform(0.3, 0.9),
                        tests_count=np.random.randint(2, 6),
                        volume_at_level=1000000,
                        time_held=30,
                        break_through=False
                    ),
                    SupportResistanceLevel(
                        level=93.0 + np.random.randn(),
                        level_type='support',
                        strength=np.random.uniform(0.2, 0.7),
                        tests_count=np.random.randint(2, 4),
                        volume_at_level=800000,
                        time_held=20,
                        break_through=False
                    )
                ],
                next_day_resistance_levels=[
                    SupportResistanceLevel(
                        level=105.0 + np.random.randn(),
                        level_type='resistance',
                        strength=np.random.uniform(0.3, 0.8),
                        tests_count=np.random.randint(2, 5),
                        volume_at_level=900000,
                        time_held=25,
                        break_through=False
                    ),
                    SupportResistanceLevel(
                        level=107.0 + np.random.randn(),
                        level_type='resistance',
                        strength=np.random.uniform(0.2, 0.6),
                        tests_count=np.random.randint(2, 4),
                        volume_at_level=700000,
                        time_held=15,
                        break_through=False
                    )
                ],
                next_day_high=106.0 + np.random.randn() * 0.5,
                next_day_low=94.0 + np.random.randn() * 0.5,
                next_day_close=100.0 + np.random.randn() * 2,
                next_day_volume=1200000
            )
            examples.append(example)

        return examples

    def test_ensemble_creation(self, config):
        """Test creating ensemble model"""
        ensemble = SupportResistanceEnsemble(config)

        assert isinstance(ensemble, SupportResistanceEnsemble)
        assert ensemble.config == config
        assert hasattr(ensemble, 'neural_net')
        assert hasattr(ensemble, 'support_rf')
        assert hasattr(ensemble, 'resistance_rf')

    def test_prepare_data(self, config, sample_training_examples):
        """Test data preparation for training"""
        ensemble = SupportResistanceEnsemble(config)

        data = ensemble.prepare_data(sample_training_examples)

        assert isinstance(data, dict)
        assert 'features' in data
        assert 'support_levels' in data
        assert 'resistance_levels' in data
        assert 'support_confidence' in data
        assert 'resistance_confidence' in data

        # Check shapes
        assert data['features'].shape[0] == len(sample_training_examples)
        assert data['features'].shape[1] == config.input_dim
        assert data['support_levels'].shape == (len(sample_training_examples), config.max_support_levels)
        assert data['resistance_levels'].shape == (len(sample_training_examples), config.max_resistance_levels)

    def test_train_ensemble(self, config, sample_training_examples):
        """Test training the ensemble model"""
        ensemble = SupportResistanceEnsemble(config)

        # Split data for training/validation
        train_examples = sample_training_examples[:15]
        val_examples = sample_training_examples[15:]

        # Train model (should not raise exceptions)
        ensemble.train(train_examples, val_examples)

        # Check that models were trained
        assert hasattr(ensemble, 'neural_net')
        assert hasattr(ensemble, 'support_rf')
        assert hasattr(ensemble, 'resistance_rf')

        # Check that scalers were fitted
        assert hasattr(ensemble, 'feature_scaler')
        assert ensemble.feature_scaler is not None

    def test_predict(self, config, sample_training_examples):
        """Test making predictions with the ensemble"""
        ensemble = SupportResistanceEnsemble(config)

        # Train first
        ensemble.train(sample_training_examples)

        # Prepare test features
        test_features = np.random.randn(3, config.input_dim)

        # Make predictions
        predictions = ensemble.predict(test_features)

        assert isinstance(predictions, dict)
        assert 'support_levels' in predictions
        assert 'resistance_levels' in predictions
        assert 'support_confidence' in predictions
        assert 'resistance_confidence' in predictions
        assert 'ensemble_support' in predictions
        assert 'ensemble_resistance' in predictions

        # Check shapes
        assert predictions['support_levels'].shape == (3, config.max_support_levels)
        assert predictions['resistance_levels'].shape == (3, config.max_resistance_levels)
        assert predictions['ensemble_support'].shape == (3,)
        assert predictions['ensemble_resistance'].shape == (3,)

    def test_evaluate(self, config, sample_training_examples):
        """Test model evaluation"""
        ensemble = SupportResistanceEnsemble(config)

        # Train first
        train_examples = sample_training_examples[:15]
        test_examples = sample_training_examples[15:]

        ensemble.train(train_examples)

        # Evaluate
        metrics = ensemble.evaluate(test_examples)

        assert isinstance(metrics, dict)
        assert 'support_mae' in metrics
        assert 'resistance_mae' in metrics
        assert 'overall_mae' in metrics

        # Metrics should be non-negative
        assert metrics['support_mae'] >= 0
        assert metrics['resistance_mae'] >= 0
        assert metrics['overall_mae'] >= 0

    def test_save_load_model(self, config, sample_training_examples):
        """Test saving and loading model"""
        ensemble = SupportResistanceEnsemble(config)

        # Train model
        ensemble.train(sample_training_examples[:10])

        # Save model
        with tempfile.TemporaryDirectory() as temp_dir:
            model_path = os.path.join(temp_dir, 'test_model.pkl')
            ensemble.save_model(model_path)

            # Create new ensemble and load
            new_ensemble = SupportResistanceEnsemble(config)
            new_ensemble.load_model(model_path)

            # Test that loaded model works
            test_features = np.random.randn(1, config.input_dim)
            predictions = new_ensemble.predict(test_features)

            assert isinstance(predictions, dict)
            assert 'support_levels' in predictions

@pytest.mark.integration
class TestSupportResistanceIntegration:
    """Integration tests for the complete model system"""

    def test_end_to_end_training_prediction(self):
        """Test complete training and prediction workflow"""
        # Create configuration
        config = SRModelConfig(
            input_dim=5,
            hidden_dims=[16, 8],
            max_support_levels=2,
            max_resistance_levels=2,
            epochs=3,  # Very small for testing
            batch_size=4
        )

        # Create simple training data
        training_examples = []
        for i in range(10):
            example = TrainingExample(
                symbol='TEST',
                date=pd.Timestamp('2023-01-01'),
                features={f'f_{j}': np.random.randn() for j in range(5)},
                next_day_support_levels=[
                    SupportResistanceLevel(95.0, 'support', 0.5, 3, 1000000, 30, False),
                    SupportResistanceLevel(93.0, 'support', 0.3, 2, 800000, 20, False)
                ],
                next_day_resistance_levels=[
                    SupportResistanceLevel(105.0, 'resistance', 0.6, 3, 900000, 25, False),
                    SupportResistanceLevel(107.0, 'resistance', 0.4, 2, 700000, 15, False)
                ],
                next_day_high=106.0,
                next_day_low=94.0,
                next_day_close=100.0,
                next_day_volume=1200000
            )
            training_examples.append(example)

        # Train model
        ensemble = SupportResistanceEnsemble(config)
        ensemble.train(training_examples)

        # Make predictions
        test_features = np.random.randn(2, 5)
        predictions = ensemble.predict(test_features)

        # Verify predictions structure
        assert isinstance(predictions, dict)
        assert predictions['support_levels'].shape == (2, 2)
        assert predictions['resistance_levels'].shape == (2, 2)

        # Evaluate
        metrics = ensemble.evaluate(training_examples[-3:])  # Use last few examples
        assert isinstance(metrics, dict)
        assert all(isinstance(v, (int, float)) for v in metrics.values())

    def test_model_consistency(self):
        """Test that model produces consistent results"""
        config = SRModelConfig(
            input_dim=3,
            hidden_dims=[8],
            max_support_levels=1,
            max_resistance_levels=1,
            epochs=2
        )

        # Create identical training data
        examples = [
            TrainingExample(
                symbol='TEST',
                date=pd.Timestamp('2023-01-01'),
                features={'f1': 1.0, 'f2': 2.0, 'f3': 3.0},
                next_day_support_levels=[
                    SupportResistanceLevel(95.0, 'support', 0.5, 3, 1000000, 30, False)
                ],
                next_day_resistance_levels=[
                    SupportResistanceLevel(105.0, 'resistance', 0.6, 3, 900000, 25, False)
                ],
                next_day_high=106.0,
                next_day_low=94.0,
                next_day_close=100.0,
                next_day_volume=1200000
            )
            for _ in range(5)
        ]

        # Train two models with same data
        ensemble1 = SupportResistanceEnsemble(config)
        ensemble2 = SupportResistanceEnsemble(config)

        # Set same random seed for reproducibility
        torch.manual_seed(42)
        np.random.seed(42)
        ensemble1.train(examples)

        torch.manual_seed(42)
        np.random.seed(42)
        ensemble2.train(examples)

        # Make predictions with same input
        test_input = np.array([[1.0, 2.0, 3.0]])

        pred1 = ensemble1.predict(test_input)
        pred2 = ensemble2.predict(test_input)

        # Should be very similar (allowing for some numerical differences)
        np.testing.assert_allclose(
            pred1['ensemble_support'],
            pred2['ensemble_support'],
            rtol=0.1  # 10% tolerance
        )

if __name__ == "__main__":
    pytest.main([__file__, "-v"])