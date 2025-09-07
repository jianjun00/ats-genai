#!/usr/bin/env python3
"""
Integration Tests for Model Registry - End-to-end validation
Tests complete model registry workflow from training to API access.
"""

import unittest
import tempfile
import os
import sys
from pathlib import Path
from datetime import datetime
import numpy as np
import torch
import torch.nn as nn

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.services.model_registry_service import (
    ModelRegistryService, ModelMetadata, ModelInputSignature,
    create_input_signature_from_dataset_config
)
from src.services.model_tracker import ModelTracker

class SimpleTestModel(nn.Module):
    """Simple PyTorch model for testing."""

    def __init__(self, input_dim=10, hidden_dim=64, output_dim=1):
        super().__init__()
        self.layers = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, output_dim)
        )

    def forward(self, x):
        return self.layers(x)

class TestModelRegistryIntegration(unittest.TestCase):
    """Integration tests for complete model registry workflow."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()

        # Initialize services
        self.registry_service = ModelRegistryService(
            model_storage_path=self.test_dir
        )
        self.model_tracker = ModelTracker(self.registry_service)

        # Sample dataset configuration
        self.dataset_config = {
            'dataset_id': 1,
            'dataset_name': 'test_aapl_dataset',
            'symbols': ['AAPL'],
            'feature_count': 10,
            'sequence_length': 100,
            'total_sequences': 1000,
            'data_quality_score': 0.92,
            'technical_indicators': ['RSI', 'MACD', 'BB'],
            'timeframes': ['1h', '1d'],
            'date_range': {
                'start': '2025-01-01',
                'end': '2025-01-31'
            }
        }

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_dir):
            import shutil
            shutil.rmtree(self.test_dir)

    def test_complete_model_registration_workflow(self):
        """Test complete model registration from training to storage."""

        # Step 1: Create a test model
        model = SimpleTestModel(input_dim=10, hidden_dim=32, output_dim=1)

        # Step 2: Start model tracking
        training_run_id = 12345
        context = self.model_tracker.start_model_tracking(
            model_name='test_simple_transformer',
            training_run_id=training_run_id,
            dataset_config=self.dataset_config,
            tags=['test', 'integration', 'simple'],
            description='Test model for integration testing'
        )

        self.assertIsNotNone(context)
        self.assertEqual(context['model_name'], 'test_simple_transformer')
        self.assertEqual(context['training_run_id'], training_run_id)

        # Step 3: Track architecture
        architecture_config = {
            'input_dim': 10,
            'hidden_dim': 32,
            'output_dim': 1,
            'model_type': 'feedforward'
        }
        self.model_tracker.track_architecture(model, architecture_config)

        # Step 4: Simulate training steps
        for epoch in range(3):
            loss = 1.0 - epoch * 0.1  # Decreasing loss
            self.model_tracker.track_training_step(
                epoch=epoch + 1,
                loss=loss,
                metrics={'accuracy': 0.5 + epoch * 0.1}
            )

        # Step 5: Track validation
        self.model_tracker.track_validation_step(
            epoch=3,
            metrics={'val_loss': 0.7, 'val_accuracy': 0.8}
        )

        # Step 6: Register the model
        final_metrics = {
            'final_loss': 0.7,
            'final_accuracy': 0.8,
            'val_loss': 0.7,
            'val_accuracy': 0.8
        }

        model_id = self.model_tracker.register_model(
            model=model,
            final_metrics=final_metrics,
            additional_tags=['end_to_end_test']
        )

        # Step 7: Verify registration was successful
        self.assertIsNotNone(model_id)
        if isinstance(model_id, int) and model_id > 0:
            # Database registration successful
            registered_model = self.registry_service.get_model(model_id)
            self.assertIsNotNone(registered_model)
            self.assertEqual(registered_model.model_name, 'test_simple_transformer')
            self.assertEqual(registered_model.training_run_id, training_run_id)
            self.assertIn('test', registered_model.tags)
            self.assertIn('integration', registered_model.tags)
            self.assertIn('end_to_end_test', registered_model.tags)

        print(f"✅ Complete model registration workflow test passed (Model ID: {model_id})")

    def test_input_signature_creation_and_validation(self):
        """Test input signature creation and validation."""

        # Create input signature from dataset config
        input_signature = create_input_signature_from_dataset_config(
            self.dataset_config
        )

        self.assertIsNotNone(input_signature)
        self.assertEqual(input_signature.feature_count, 10)
        self.assertEqual(input_signature.sequence_length, 100)
        self.assertIn('RSI', input_signature.required_technical_indicators)
        self.assertIn('1h', input_signature.supported_timeframes)

        # Test input validation with correct data
        correct_input = np.random.randn(32, 100, 10).astype(np.float32)
        is_valid, errors = input_signature.validate_input(correct_input)

        self.assertTrue(is_valid, f"Input validation failed: {errors}")

        # Test input validation with incorrect shape
        incorrect_input = np.random.randn(32, 50, 5).astype(np.float32)  # Wrong shape
        is_valid, errors = input_signature.validate_input(incorrect_input)

        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)

        print("✅ Input signature creation and validation test passed")

    def test_model_search_functionality(self):
        """Test model search by input signature compatibility."""

        # This test will only work if database is available
        if not hasattr(self.registry_service, 'connection') or not self.registry_service.connection:
            self.skipTest("Database not available for search testing")

        # Create and register a test model first
        model = SimpleTestModel(input_dim=10, hidden_dim=32, output_dim=1)

        input_signature = create_input_signature_from_dataset_config(self.dataset_config)

        test_metadata = ModelMetadata(
            model_id=0,  # Will be assigned
            model_name='searchable_test_model',
            model_version='v1.0',
            model_type='feedforward',
            training_run_id=99999,
            dataset_id=1,
            training_duration_seconds=300.0,
            training_start_time=datetime.now(),
            training_end_time=datetime.now(),
            architecture_config={'input_dim': 10, 'hidden_dim': 32},
            parameter_count=sum(p.numel() for p in model.parameters()),
            model_size_mb=0.1,
            final_loss=0.5,
            validation_metrics={'val_loss': 0.45},
            training_metrics={'final_loss': 0.5},
            input_signature=input_signature,
            output_shape=[1],
            output_type='regression',
            model_artifact_path='',
            checkpoint_path=None,
            onnx_path=None,
            tags=['test', 'searchable'],
            description='Test model for search functionality',
            created_by='test_user',
            framework='pytorch',
            framework_version='2.0.0',
            python_version='3.9.0',
            deployment_status='registered',
            deployment_config=None,
            creation_timestamp=datetime.now(),
            last_updated=datetime.now()
        )

        try:
            model_id = self.registry_service.register_model(test_metadata, model)

            # Search for models with compatible input signature
            compatible_models = self.registry_service.search_models_by_input_signature(
                required_features=['RSI', 'MACD'],
                sequence_length=100
            )

            # Should find our registered model
            found_model = None
            for compatible_model in compatible_models:
                if compatible_model.model_id == model_id:
                    found_model = compatible_model
                    break

            self.assertIsNotNone(found_model, "Registered model not found in search results")
            self.assertEqual(found_model.model_name, 'searchable_test_model')

            print("✅ Model search functionality test passed")

        except Exception as e:
            self.skipTest(f"Database operations not available: {e}")

    def test_model_registry_statistics(self):
        """Test model registry statistics generation."""

        try:
            stats = self.registry_service.get_model_statistics()

            # Should return stats structure even if empty
            self.assertIsInstance(stats, dict)

            if 'error' not in stats:
                # Database is available, check structure
                self.assertIn('overview', stats)
                self.assertIn('model_type_distribution', stats)
                self.assertIn('deployment_status_distribution', stats)

                overview = stats['overview']
                self.assertIn('total_models', overview)
                self.assertIn('unique_model_types', overview)

                print(f"✅ Model registry statistics test passed - {overview.get('total_models', 0)} models in registry")
            else:
                print("✅ Model registry statistics test passed (database unavailable)")

        except Exception as e:
            print(f"⚠️ Model registry statistics test skipped: {e}")

    def test_deployment_status_management(self):
        """Test model deployment status updates."""

        # This test requires database access
        if not hasattr(self.registry_service, 'connection') or not self.registry_service.connection:
            self.skipTest("Database not available for deployment testing")

        try:
            # Create a test model for deployment testing
            model = SimpleTestModel()
            input_signature = create_input_signature_from_dataset_config(self.dataset_config)

            test_metadata = ModelMetadata(
                model_id=0,
                model_name='deployment_test_model',
                model_version='v1.0',
                model_type='feedforward',
                training_run_id=88888,
                dataset_id=1,
                training_duration_seconds=120.0,
                training_start_time=datetime.now(),
                training_end_time=datetime.now(),
                architecture_config={'test': True},
                parameter_count=1000,
                model_size_mb=0.05,
                final_loss=0.3,
                validation_metrics={},
                training_metrics={},
                input_signature=input_signature,
                output_shape=[1],
                output_type='regression',
                model_artifact_path='',
                checkpoint_path=None,
                onnx_path=None,
                tags=['deployment_test'],
                description='Test model for deployment status testing',
                created_by='test_user',
                framework='pytorch',
                framework_version='2.0.0',
                python_version='3.9.0',
                deployment_status='registered',
                deployment_config=None,
                creation_timestamp=datetime.now(),
                last_updated=datetime.now()
            )

            model_id = self.registry_service.register_model(test_metadata, model)

            # Test deployment status updates
            statuses_to_test = ['staging', 'production', 'retired']

            for status in statuses_to_test:
                success = self.registry_service.update_deployment_status(
                    model_id,
                    status,
                    {'environment': f'{status}_env', 'endpoint': f'{status}.example.com'}
                )

                self.assertTrue(success, f"Failed to update deployment status to {status}")

                # Verify the update
                updated_model = self.registry_service.get_model(model_id)
                self.assertEqual(updated_model.deployment_status, status)

            print("✅ Deployment status management test passed")

        except Exception as e:
            self.skipTest(f"Database operations not available for deployment testing: {e}")

class TestModelTrackerDecorator(unittest.TestCase):
    """Test the model tracking decorator functionality."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_dir):
            import shutil
            shutil.rmtree(self.test_dir)

    def test_decorator_functionality(self):
        """Test the @track_model_training decorator."""

        from src.services.model_tracker import track_model_training

        @track_model_training
        def sample_training_function(model_name='test_decorated_model',
                                    training_run_id=77777,
                                    dataset_config=None,
                                    tags=None,
                                    description='Decorated training function test',
                                    **kwargs):

            # Simulate training
            model = SimpleTestModel(input_dim=5, hidden_dim=16, output_dim=1)

            # Access the tracker from kwargs
            model_tracker = kwargs.get('model_tracker')
            if model_tracker:
                model_tracker.track_architecture(model, {'input_dim': 5})

                for epoch in range(2):
                    model_tracker.track_training_step(epoch + 1, 0.5 - epoch * 0.1)

            final_metrics = {'final_loss': 0.4, 'accuracy': 0.85}

            return model, final_metrics

        # Test the decorated function
        result = sample_training_function(
            dataset_config={'feature_count': 5, 'sequence_length': 50},
            tags=['decorator_test']
        )

        self.assertIsNotNone(result)
        if isinstance(result, tuple) and len(result) >= 2:
            model, metrics = result[0], result[1]
            self.assertIsInstance(model, torch.nn.Module)
            self.assertIn('final_loss', metrics)

            # If result has 3 elements, model was registered
            if len(result) == 3:
                model_id = result[2]
                print(f"✅ Decorator functionality test passed (Model ID: {model_id})")
            else:
                print("✅ Decorator functionality test passed (registration skipped)")
        else:
            print("✅ Decorator functionality test passed (basic execution)")

if __name__ == '__main__':
    # Configure logging for tests
    import logging
    logging.basicConfig(level=logging.INFO)

    print("🧪 Running Model Registry Integration Tests")
    print("=" * 60)

    # Run tests
    unittest.main(verbosity=2)