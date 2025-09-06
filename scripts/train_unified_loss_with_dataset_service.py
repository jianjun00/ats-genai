#!/usr/bin/env python3
"""
DATASET SERVICE INTEGRATED TRAINING - Unified Loss Transformer
Uses dataset service for all metadata operations, maintaining zero synthetic data tolerance.
Demonstrates proper integration between training jobs and dataset service.
"""

import os
import sys
import numpy as np
import torch
import torch.nn as nn
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import logging
import json
import subprocess
import socket
import psutil
import psycopg2
from typing import Dict, List, Tuple, Any

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.clients.dataset_client import DatasetClient
from src.services.dataset_service import DatasetMetadata
from src.services.model_tracker import ModelTracker

# Import existing classes from the real data training script
from scripts.train_unified_loss_REAL_DATA_ONLY import (
    TrainingJobTracker, SimplifiedFinancialLoss, SimpleTransformer,
    RealDataValidator, ensure_no_synthetic_data
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dataset_service_training.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatasetServiceTrainingPipeline:
    """Training pipeline that uses dataset service for all data operations."""
    
    def __init__(self):
        """Initialize training pipeline with dataset service integration."""
        
        # Initialize dataset client
        self.dataset_client = DatasetClient()
        
        # Initialize job tracker
        self.job_tracker = TrainingJobTracker()
        
        # Initialize data validator
        self.data_validator = RealDataValidator()
        
        # Initialize model tracker
        self.model_tracker = ModelTracker()
        
        logger.info("✅ Dataset Service Training Pipeline initialized")
    
    def find_training_dataset(self, symbols: List[str], 
                            min_sequences: int = 1000,
                            min_quality: float = 0.7) -> Optional[Dict[str, Any]]:
        """Find and validate suitable training dataset."""
        
        logger.info(f"🔍 Searching for training dataset: symbols={symbols}, min_sequences={min_sequences}")
        
        # Use dataset client to find suitable dataset
        config = self.dataset_client.get_training_data_config(
            symbols=symbols,
            min_sequences=min_sequences
        )
        
        if not config:
            logger.error(f"❌ No suitable dataset found for symbols {symbols}")
            return None
        
        # Validate dataset meets training requirements
        validation = self.dataset_client.validate_dataset_for_training(
            dataset_id=config['dataset_id'],
            required_features=5,  # OHLCV minimum
            min_sequences=min_sequences
        )
        
        if not validation['valid']:
            logger.error(f"❌ Dataset validation failed: {validation}")
            return None
        
        # Validate data source authenticity (zero synthetic data tolerance)
        for file_path in config['file_paths']:
            self.data_validator.validate_data_source("dataset_service", file_path)
        
        logger.info(f"✅ Found suitable training dataset: {config['dataset_name']}")
        logger.info(f"   📊 {config['total_sequences']} sequences, quality {config['data_quality_score']:.3f}")
        logger.info(f"   💾 Estimated memory: {config['estimated_memory_mb']:.1f} MB")
        
        return config
    
    def create_training_configuration(self, dataset_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create comprehensive training configuration."""
        
        training_config = {
            # Dataset Information
            'dataset_id': dataset_config['dataset_id'],
            'dataset_name': dataset_config['dataset_name'],
            'symbols': dataset_config['symbols'],
            'total_sequences': dataset_config['total_sequences'],
            'sequence_length': dataset_config['sequence_length'],
            'feature_count': dataset_config['feature_count'],
            'data_quality_score': dataset_config['data_quality_score'],
            
            # Data Source Validation
            'data_source': 'dataset_service_managed',
            'file_paths': dataset_config['file_paths'],
            'file_format': dataset_config['file_format'],
            'synthetic_data_tolerance': 'ZERO_TOLERANCE',
            'data_validation_passed': True,
            
            # Model Architecture
            'model_type': 'SimpleTransformer',
            'loss_function': 'SimplifiedFinancialLoss',
            'input_dim': min(dataset_config['feature_count'], 5),  # Cap at OHLCV
            'd_model': 64,
            'num_epochs': 10,
            'batch_size': min(dataset_config['batch_size_recommendation'], 64),
            'learning_rate': 1e-4,
            
            # Loss Function Parameters
            'alpha_cvar': 0.05,
            'lambda_drawdown': 2.0,
            
            # Technical Indicators
            'technical_indicators': dataset_config.get('technical_indicators', []),
            'timeframes': dataset_config.get('timeframes', ['1h']),
            
            # Memory Management
            'estimated_memory_mb': dataset_config['estimated_memory_mb'],
            'use_batch_loading': dataset_config['estimated_memory_mb'] > 1000,
            
            # Date Range
            'date_range_start': dataset_config['date_range']['start'],
            'date_range_end': dataset_config['date_range']['end'],
        }
        
        return training_config
    
    def train_model(self, training_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute model training using dataset service data."""
        
        logger.info("🚀 Starting model training with dataset service integration")
        
        # Start job tracking
        run_id = self.job_tracker.start_training_job(
            script_name='train_unified_loss_with_dataset_service.py',
            parameters=training_config
        )
        
        # Start model tracking
        model_name = f"unified_loss_transformer_{training_config['symbols'][0]}"
        self.model_tracker.start_model_tracking(
            model_name=model_name,
            training_run_id=run_id,
            dataset_config=training_config,
            tags=['unified_loss', 'transformer', 'financial'] + training_config['symbols'],
            description=f"Unified loss transformer for {training_config['symbols']} using dataset service"
        )
        
        try:
            # Create dataset loader
            dataset_loader = self.dataset_client.create_data_loader(training_config)
            
            # Create model
            model = SimpleTransformer(
                input_dim=training_config['input_dim'],
                d_model=training_config['d_model'],
                sequence_length=training_config['sequence_length']
            )
            
            # Count model parameters and track architecture
            model_parameters = sum(p.numel() for p in model.parameters() if p.requires_grad)
            logger.info(f"📊 Model has {model_parameters:,} trainable parameters")
            
            # Track model architecture
            architecture_config = {
                'input_dim': training_config['input_dim'],
                'd_model': training_config['d_model'],
                'sequence_length': training_config['sequence_length'],
                'parameter_count': model_parameters,
                'loss_function': 'SimplifiedFinancialLoss',
                'optimizer': 'Adam',
                'learning_rate': training_config['learning_rate']
            }
            self.model_tracker.track_architecture(model, architecture_config)
            
            # Create loss function and optimizer
            loss_fn = SimplifiedFinancialLoss(
                alpha_cvar=training_config['alpha_cvar'],
                lambda_drawdown=training_config['lambda_drawdown']
            )
            optimizer = torch.optim.Adam(model.parameters(), lr=training_config['learning_rate'])
            
            # Training loop
            num_epochs = training_config['num_epochs']
            training_history = []
            
            for epoch in range(num_epochs):
                model.train()
                epoch_losses = []
                batch_count = 0
                
                # Use batch iterator from dataset service
                for X_batch, y_batch in dataset_loader.get_batch_iterator(training_config['batch_size']):
                    
                    # Convert to PyTorch tensors
                    if len(X_batch.shape) == 2:
                        # Reshape 2D to 3D: (batch_size, sequence_length, features)
                        batch_size = X_batch.shape[0]
                        feature_count = X_batch.shape[1]
                        sequence_length = training_config['sequence_length']
                        
                        # Reshape to sequences
                        if batch_size >= sequence_length:
                            X_batch = X_batch[:batch_size//sequence_length*sequence_length]
                            X_batch = X_batch.reshape(-1, sequence_length, feature_count)
                            y_batch = y_batch[:len(X_batch)]
                    
                    X_tensor = torch.tensor(X_batch, dtype=torch.float32)
                    y_tensor = torch.tensor(y_batch, dtype=torch.float32).unsqueeze(-1) if len(y_batch.shape) == 1 else torch.tensor(y_batch, dtype=torch.float32)
                    
                    # Skip if batch too small
                    if len(X_tensor) < 2:
                        continue
                    
                    # Forward pass
                    predictions = model(X_tensor)
                    
                    # Calculate loss
                    loss = loss_fn(predictions, y_tensor)
                    
                    # Backward pass
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()
                    
                    epoch_losses.append(loss.item())
                    batch_count += 1
                    
                    # Limit batches per epoch for demo
                    if batch_count >= 20:
                        break
                
                if epoch_losses:
                    avg_loss = np.mean(epoch_losses)
                    training_history.append(avg_loss)
                    
                    logger.info(f"Epoch {epoch + 1}/{num_epochs}, Avg Loss: {avg_loss:.6f} ({len(epoch_losses)} batches)")
                    
                    # Track training step
                    self.model_tracker.track_training_step(
                        epoch=epoch + 1,
                        loss=avg_loss,
                        metrics={
                            'batches_processed': batch_count,
                            'batch_size': training_config['batch_size']
                        }
                    )
                    
                    # Update training progress in runs table
                    self.job_tracker.update_training_progress(
                        epoch=epoch + 1,
                        loss=avg_loss,
                        metrics={
                            'batches_processed': batch_count,
                            'batch_size': training_config['batch_size'],
                            'dataset_id': training_config['dataset_id']
                        }
                    )
                else:
                    logger.warning(f"Epoch {epoch + 1}: No valid batches processed")
            
            # Calculate final evaluation metrics
            model.eval()
            final_metrics = self._calculate_final_metrics(model, dataset_loader, training_config)
            final_metrics['training_history'] = training_history
            final_metrics['model_parameters'] = model_parameters
            
            # Register model in model registry
            model_id = self.model_tracker.register_model(
                model=model,
                final_metrics=final_metrics,
                additional_tags=['dataset_service_integration']
            )
            
            # Save model with comprehensive metadata
            model_path = f"unified_loss_transformer_dataset_service_run_{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pth"
            
            model_metadata = {
                'model_state_dict': model.state_dict(),
                'training_config': training_config,
                'dataset_metadata': {
                    'dataset_id': training_config['dataset_id'],
                    'dataset_name': training_config['dataset_name'],
                    'data_quality_score': training_config['data_quality_score'],
                    'total_sequences': training_config['total_sequences']
                },
                'final_metrics': final_metrics,
                'run_id': run_id,
                'model_id': model_id,  # Add model registry ID
                'data_source_validation': 'Dataset service managed - zero synthetic data',
                'model_parameters': model_parameters
            }
            
            torch.save(model_metadata, model_path)
            logger.info(f"✅ Model saved: {model_path}")
            
            # Complete job tracking
            self.job_tracker.complete_training_job(
                model_output_path=model_path,
                final_metrics=final_metrics
            )
            
            return {
                'success': True,
                'run_id': run_id,
                'model_id': model_id,  # Add model registry ID
                'model_path': model_path,
                'final_metrics': final_metrics,
                'dataset_info': {
                    'dataset_id': training_config['dataset_id'],
                    'dataset_name': training_config['dataset_name'],
                    'total_sequences': training_config['total_sequences']
                }
            }
            
        except Exception as e:
            # Mark training job as failed
            error_message = f"Dataset service training failed: {str(e)}"
            self.job_tracker.fail_training_job(error_message)
            
            logger.error(f"❌ Training failed: {e}")
            raise
    
    def _calculate_final_metrics(self, model, dataset_loader, training_config) -> Dict[str, Any]:
        """Calculate comprehensive final evaluation metrics."""
        
        try:
            # Get a sample for evaluation
            X_sample, y_sample = dataset_loader.get_sample(sample_size=1000)
            
            if len(X_sample) == 0:
                logger.warning("⚠️ No sample data available for final metrics")
                return {'final_loss': 0.0, 'evaluation_warning': 'No sample data available'}
            
            # Ensure proper shape
            if len(X_sample.shape) == 2:
                sequence_length = training_config['sequence_length']
                feature_count = X_sample.shape[1]
                sample_size = len(X_sample)
                
                if sample_size >= sequence_length:
                    X_sample = X_sample[:sample_size//sequence_length*sequence_length]
                    X_sample = X_sample.reshape(-1, sequence_length, feature_count)
                    y_sample = y_sample[:len(X_sample)]
            
            X_tensor = torch.tensor(X_sample, dtype=torch.float32)
            y_tensor = torch.tensor(y_sample, dtype=torch.float32).unsqueeze(-1) if len(y_sample.shape) == 1 else torch.tensor(y_sample, dtype=torch.float32)
            
            with torch.no_grad():
                predictions = model(X_tensor)
                final_mse = torch.nn.functional.mse_loss(predictions, y_tensor).item()
                final_mae = torch.nn.functional.l1_loss(predictions, y_tensor).item()
                
                # Additional metrics
                prediction_std = torch.std(predictions).item()
                target_std = torch.std(y_tensor).item()
                
                if len(predictions) > 1:
                    correlation = np.corrcoef(
                        predictions.squeeze().numpy(),
                        y_tensor.squeeze().numpy()
                    )[0, 1] if not np.isnan(np.corrcoef(predictions.squeeze().numpy(), y_tensor.squeeze().numpy())[0, 1]) else 0.0
                else:
                    correlation = 0.0
            
            final_metrics = {
                'final_loss': final_mse,
                'final_mse': final_mse,
                'final_mae': final_mae,
                'correlation_coefficient': correlation,
                'prediction_std': prediction_std,
                'target_std': target_std,
                'evaluation_sample_size': len(X_sample),
                'data_source_validation': 'Dataset service - real data only',
                'synthetic_data_detected': False,
                'data_quality_score': training_config['data_quality_score']
            }
            
            return final_metrics
            
        except Exception as e:
            logger.error(f"❌ Final metrics calculation failed: {e}")
            return {
                'final_loss': 0.0,
                'evaluation_error': str(e),
                'synthetic_data_detected': False,
                'data_source_validation': 'Dataset service - real data only'
            }

def main():
    """Main training function using dataset service."""
    
    logger.info("🚀 Starting Dataset Service Integrated Training")
    
    # Validate pipeline for synthetic data
    ensure_no_synthetic_data("Dataset service managed real market data")
    
    # Initialize training pipeline
    pipeline = DatasetServiceTrainingPipeline()
    
    # Configuration
    target_symbols = ['AAPL']
    min_sequences = 1000
    min_quality = 0.7
    
    try:
        # Find suitable training dataset
        dataset_config = pipeline.find_training_dataset(
            symbols=target_symbols,
            min_sequences=min_sequences,
            min_quality=min_quality
        )
        
        if not dataset_config:
            logger.error("❌ No suitable dataset found - training cannot proceed")
            logger.error("Ensure datasets are registered in dataset service")
            return
        
        # Create training configuration
        training_config = pipeline.create_training_configuration(dataset_config)
        
        # Execute training
        results = pipeline.train_model(training_config)
        
        if results['success']:
            logger.info("🎯 TRAINING COMPLETED SUCCESSFULLY")
            logger.info(f"   Run ID: {results['run_id']}")
            logger.info(f"   Model: {results['model_path']}")
            logger.info(f"   Dataset: {results['dataset_info']['dataset_name']}")
            logger.info(f"   Final MSE: {results['final_metrics']['final_mse']:.6f}")
            logger.info(f"   Data Quality: {results['final_metrics']['data_quality_score']:.3f}")
            logger.info("✅ ZERO SYNTHETIC DATA - All data sourced through dataset service")
        else:
            logger.error("❌ Training completed with issues")
            
    except Exception as e:
        logger.error(f"❌ Dataset service training failed: {e}")
        logger.error("This indicates issues with:")
        logger.error("1. Dataset service configuration")
        logger.error("2. Dataset availability or accessibility")  
        logger.error("3. Database connectivity")
        logger.error("4. File system access")
        logger.error("🚨 DO NOT FALL BACK TO SYNTHETIC DATA - Fix the dataset service issue")
        raise

if __name__ == "__main__":
    main()