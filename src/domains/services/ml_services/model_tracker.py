#!/usr/bin/env python3
"""
Model Tracker - Integration layer for automatic model registration during training
Provides seamless model tracking for all training jobs with minimal code changes.
"""

import os
import platform
import socket
import subprocess
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import torch
import numpy as np

from .model_registry_service import (
    ModelRegistryService, ModelMetadata, create_input_signature_from_dataset_config
)

logger = logging.getLogger(__name__)

class ModelTracker:
    """Automatic model tracking and registration for training jobs."""

    def __init__(self, registry_service: Optional[ModelRegistryService] = None):
        """Initialize model tracker."""
        self.registry = registry_service or ModelRegistryService()
        self.current_training_context = {}
        logger.info("✅ Model Tracker initialized")

    def start_model_tracking(self, model_name: str, training_run_id: int,
                           dataset_config: Optional[Dict[str, Any]] = None,
                           tags: Optional[List[str]] = None,
                           description: str = "") -> Dict[str, Any]:
        """Start tracking a new model training session."""

        context = {
            'model_name': model_name,
            'training_run_id': training_run_id,
            'dataset_config': dataset_config or {},
            'tags': tags or [],
            'description': description,
            'training_start_time': datetime.now(),
            'training_metrics': [],
            'validation_metrics': [],
            'architecture_info': {},
            'framework_info': self._get_framework_info()
        }

        self.current_training_context = context
        logger.info(f"🎯 Started tracking model: {model_name} (run {training_run_id})")

        return context

    def track_architecture(self, model: Any, architecture_config: Optional[Dict[str, Any]] = None):
        """Track model architecture information."""

        if not self.current_training_context:
            logger.warning("⚠️ No active tracking context - call start_model_tracking() first")
            return

        try:
            # Extract architecture information
            arch_info = architecture_config or {}

            # For PyTorch models
            if hasattr(model, 'parameters'):
                param_count = sum(p.numel() for p in model.parameters() if p.requires_grad)
                arch_info.update({
                    'parameter_count': param_count,
                    'trainable_parameters': param_count,
                    'model_class': model.__class__.__name__,
                    'framework': 'pytorch'
                })

                # Extract layer information if available
                if hasattr(model, 'modules'):
                    layers = []
                    for name, module in model.named_modules():
                        if len(list(module.children())) == 0:  # Leaf modules
                            layers.append({
                                'name': name,
                                'type': module.__class__.__name__,
                                'parameters': sum(p.numel() for p in module.parameters())
                            })
                    arch_info['layers'] = layers

            self.current_training_context['architecture_info'] = arch_info
            self.current_training_context['parameter_count'] = arch_info.get('parameter_count', 0)

            logger.info(f"📊 Architecture tracked: {arch_info.get('parameter_count', 0):,} parameters")

        except Exception as e:
            logger.warning(f"⚠️ Failed to track architecture: {e}")

    def track_training_step(self, epoch: int, loss: float,
                          metrics: Optional[Dict[str, float]] = None):
        """Track training step metrics."""

        if not self.current_training_context:
            return

        step_metrics = {
            'epoch': epoch,
            'loss': loss,
            'timestamp': datetime.now().isoformat(),
            **(metrics or {})
        }

        self.current_training_context['training_metrics'].append(step_metrics)

        # Keep only last 100 training steps for memory efficiency
        if len(self.current_training_context['training_metrics']) > 100:
            self.current_training_context['training_metrics'] = \
                self.current_training_context['training_metrics'][-100:]

    def track_validation_step(self, epoch: int, metrics: Dict[str, float]):
        """Track validation metrics."""

        if not self.current_training_context:
            return

        validation_metrics = {
            'epoch': epoch,
            'timestamp': datetime.now().isoformat(),
            **metrics
        }

        self.current_training_context['validation_metrics'].append(validation_metrics)

    def register_model(self, model: Any, final_metrics: Dict[str, float],
                      model_version: Optional[str] = None,
                      checkpoint_path: Optional[str] = None,
                      additional_tags: Optional[List[str]] = None) -> Optional[int]:
        """Register the trained model in the registry."""

        if not self.current_training_context:
            logger.error("❌ No active tracking context - cannot register model")
            return None

        try:
            context = self.current_training_context
            training_end_time = datetime.now()
            training_duration = (training_end_time - context['training_start_time']).total_seconds()

            # Generate model version if not provided
            if not model_version:
                model_version = f"v{training_end_time.strftime('%Y%m%d_%H%M%S')}"

            # Create input signature from dataset config
            input_signature = create_input_signature_from_dataset_config(
                context['dataset_config']
            )

            # Determine model type
            model_type = self._infer_model_type(model)

            # Aggregate training metrics
            training_metrics_summary = self._summarize_training_metrics(
                context['training_metrics']
            )

            # Aggregate validation metrics
            validation_metrics_summary = self._summarize_validation_metrics(
                context['validation_metrics']
            )

            # Combine tags
            all_tags = context['tags'] + (additional_tags or [])
            all_tags.append(f"run_{context['training_run_id']}")
            all_tags.append(f"framework_{context['framework_info']['framework']}")

            # Create model metadata
            model_metadata = ModelMetadata(
                model_id=0,  # Will be assigned by registry
                model_name=context['model_name'],
                model_version=model_version,
                model_type=model_type,
                training_run_id=context['training_run_id'],
                dataset_id=context['dataset_config'].get('dataset_id'),
                training_duration_seconds=training_duration,
                training_start_time=context['training_start_time'],
                training_end_time=training_end_time,
                architecture_config=context['architecture_info'],
                parameter_count=context.get('parameter_count', 0),
                model_size_mb=0.0,  # Will be calculated by registry
                final_loss=final_metrics.get('final_loss', final_metrics.get('loss', 0.0)),
                validation_metrics=validation_metrics_summary,
                training_metrics=training_metrics_summary,
                input_signature=input_signature,
                output_shape=self._infer_output_shape(model),
                output_type=self._infer_output_type(model),
                model_artifact_path="",  # Will be set by registry
                checkpoint_path=checkpoint_path,
                onnx_path=None,
                tags=all_tags,
                description=context['description'] or f"Model trained on run {context['training_run_id']}",
                created_by=self._get_user_info(),
                framework=context['framework_info']['framework'],
                framework_version=context['framework_info']['framework_version'],
                python_version=context['framework_info']['python_version'],
                deployment_status='registered',
                deployment_config=None,
                creation_timestamp=training_end_time,
                last_updated=training_end_time
            )

            # Register additional artifacts
            additional_artifacts = {}
            if checkpoint_path and os.path.exists(checkpoint_path):
                additional_artifacts['checkpoint'] = checkpoint_path

            # Register model
            model_id = self.registry.register_model(
                model_metadata,
                model,
                additional_artifacts
            )

            logger.info(f"🎉 Model registered successfully: {context['model_name']} v{model_version} (ID: {model_id})")

            # Clear tracking context
            self.current_training_context = {}

            return model_id

        except Exception as e:
            logger.error(f"❌ Failed to register model: {e}")
            return None

    def _get_framework_info(self) -> Dict[str, str]:
        """Get framework and environment information."""

        framework_info = {
            'framework': 'pytorch',
            'framework_version': torch.__version__ if torch else 'unknown',
            'python_version': platform.python_version(),
            'platform': platform.platform(),
            'hostname': socket.gethostname()
        }

        # Try to get CUDA info
        try:
            if torch.cuda.is_available():
                framework_info['cuda_version'] = torch.version.cuda
                framework_info['gpu_count'] = torch.cuda.device_count()
                framework_info['gpu_name'] = torch.cuda.get_device_name(0)
        except Exception:
            pass

        return framework_info

    def _infer_model_type(self, model: Any) -> str:
        """Infer model type from model instance."""

        if not model:
            return 'unknown'

        class_name = model.__class__.__name__.lower()

        if 'transformer' in class_name:
            return 'transformer'
        elif 'lstm' in class_name or 'rnn' in class_name or 'gru' in class_name:
            return 'rnn'
        elif 'conv' in class_name or 'cnn' in class_name:
            return 'cnn'
        elif 'linear' in class_name or 'mlp' in class_name:
            return 'feedforward'
        else:
            return 'neural_network'

    def _infer_output_shape(self, model: Any) -> List[int]:
        """Infer output shape from model."""

        # Default regression output shape
        return [1]

    def _infer_output_type(self, model: Any) -> str:
        """Infer output type from model."""

        # Default to regression
        return 'regression'

    def _summarize_training_metrics(self, training_metrics: List[Dict[str, Any]]) -> Dict[str, float]:
        """Summarize training metrics."""

        if not training_metrics:
            return {}

        # Calculate summary statistics
        losses = [m.get('loss', 0.0) for m in training_metrics if 'loss' in m]

        summary = {}

        if losses:
            summary.update({
                'final_loss': losses[-1],
                'best_loss': min(losses),
                'avg_loss': np.mean(losses),
                'loss_std': np.std(losses),
                'total_epochs': len(losses)
            })

        # Add other metrics if available
        all_metric_keys = set()
        for m in training_metrics:
            all_metric_keys.update(m.keys())

        for key in all_metric_keys:
            if key not in ['epoch', 'timestamp', 'loss']:
                values = [m.get(key) for m in training_metrics if key in m and isinstance(m[key], (int, float))]
                if values:
                    summary[f'{key}_final'] = values[-1]
                    summary[f'{key}_avg'] = np.mean(values)

        return summary

    def _summarize_validation_metrics(self, validation_metrics: List[Dict[str, Any]]) -> Dict[str, float]:
        """Summarize validation metrics."""

        if not validation_metrics:
            return {}

        # Get final validation metrics
        final_metrics = validation_metrics[-1] if validation_metrics else {}

        summary = {}
        for key, value in final_metrics.items():
            if key not in ['epoch', 'timestamp'] and isinstance(value, (int, float)):
                summary[f'final_{key}'] = value

        # Calculate best metrics across all validation steps
        all_metric_keys = set()
        for m in validation_metrics:
            all_metric_keys.update(m.keys())

        for key in all_metric_keys:
            if key not in ['epoch', 'timestamp']:
                values = [m.get(key) for m in validation_metrics if key in m and isinstance(m[key], (int, float))]
                if values:
                    # For loss metrics, best is minimum; for accuracy metrics, best is maximum
                    if 'loss' in key.lower() or 'error' in key.lower():
                        summary[f'best_{key}'] = min(values)
                    else:
                        summary[f'best_{key}'] = max(values)

        return summary

    def _get_user_info(self) -> str:
        """Get user information for model attribution."""

        try:
            # Try to get git user info
            git_user = subprocess.check_output(
                ['git', 'config', 'user.name'],
                universal_newlines=True,
                stderr=subprocess.DEVNULL
            ).strip()
            return git_user
        except Exception:
            pass

        # Fallback to system user
        return os.getenv('USER', os.getenv('USERNAME', 'unknown'))

def track_model_training(training_function):
    """Decorator for automatic model tracking in training functions."""

    def wrapper(*args, **kwargs):
        # Extract model name from function or kwargs
        model_name = kwargs.get('model_name', training_function.__name__)
        training_run_id = kwargs.get('training_run_id', 0)

        # Initialize tracker
        tracker = ModelTracker()

        try:
            # Start tracking
            context = tracker.start_model_tracking(
                model_name=model_name,
                training_run_id=training_run_id,
                dataset_config=kwargs.get('dataset_config', {}),
                tags=kwargs.get('tags', []),
                description=kwargs.get('description', f"Auto-tracked training of {model_name}")
            )

            # Add tracker to kwargs for use in training function
            kwargs['model_tracker'] = tracker

            # Execute training function
            result = training_function(*args, **kwargs)

            # If training function returns a model and metrics, register automatically
            if isinstance(result, tuple) and len(result) >= 2:
                model, metrics = result[0], result[1]

                if hasattr(model, 'parameters'):  # PyTorch model
                    tracker.track_architecture(model)
                    model_id = tracker.register_model(model, metrics)

                    # Add model_id to result if it was successful
                    if model_id and len(result) == 2:
                        result = (model, metrics, model_id)

            return result

        except Exception as e:
            logger.error(f"❌ Model tracking failed: {e}")
            # Continue with training even if tracking fails
            return training_function(*args, **kwargs)

    return wrapper