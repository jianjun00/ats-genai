"""
TFT Training Pipeline

Complete training pipeline for Temporal Fusion Transformer models including
experiment tracking, model checkpointing, and performance evaluation.
"""

import logging
import json
import pickle
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.optim import Adam, AdamW
from torch.optim.lr_scheduler import ReduceLROnPlateau, CosineAnnealingLR
import asyncpg

from domains.models.temporal_fusion_transformer import (
    TemporalFusionTransformer,
    TFTConfig
)
from domains.models.data_loader import (
    TFTDataLoader,
    TFTDataConfig
)

logger = logging.getLogger(__name__)


@dataclass
class ExperimentConfig:
    """Configuration for TFT training experiment."""

    # Experiment metadata
    experiment_name: str = "tft_baseline"
    experiment_description: str = "Baseline TFT model training"
    random_seed: int = 42

    # Data configuration
    symbols: List[str] = None
    start_date: str = "2024-01-01"
    end_date: str = "2024-12-31"
    train_ratio: float = 0.7
    val_ratio: float = 0.2
    test_ratio: float = 0.1

    # Model configuration
    model_config: TFTConfig = None
    data_config: TFTDataConfig = None

    # Training configuration
    max_epochs: int = 100
    batch_size: int = 32
    learning_rate: float = 1e-3
    weight_decay: float = 1e-5
    patience: int = 15
    grad_clip_norm: float = 1.0

    # Optimizer and scheduler
    optimizer_type: str = "adam"  # "adam", "adamw"
    scheduler_type: str = "plateau"  # "plateau", "cosine"
    scheduler_params: Dict = None

    # Validation and checkpointing
    validation_frequency: int = 1  # Validate every N epochs
    checkpoint_frequency: int = 5  # Save checkpoint every N epochs
    save_best_only: bool = True

    # Evaluation metrics
    primary_metric: str = "val_loss"  # Primary metric for model selection
    monitor_metrics: List[str] = None

    # Output configuration
    output_dir: str = "experiments/tft"
    save_predictions: bool = True
    save_attention_weights: bool = False

    def __post_init__(self):
        if self.symbols is None:
            self.symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

        if self.model_config is None:
            self.model_config = TFTConfig()

        if self.data_config is None:
            self.data_config = TFTDataConfig()

        if self.scheduler_params is None:
            self.scheduler_params = {"factor": 0.5, "patience": 10, "min_lr": 1e-6}

        if self.monitor_metrics is None:
            self.monitor_metrics = ["train_loss", "val_loss", "val_mae", "val_mape"]


@dataclass
class TrainingMetrics:
    """Training metrics tracking."""

    epoch: int
    train_loss: float
    val_loss: float
    learning_rate: float

    # Additional metrics
    train_mae: Optional[float] = None
    val_mae: Optional[float] = None
    train_mape: Optional[float] = None
    val_mape: Optional[float] = None
    train_mse: Optional[float] = None
    val_mse: Optional[float] = None

    # Training time
    epoch_time: Optional[float] = None
    total_time: Optional[float] = None

    # Model info
    total_params: Optional[int] = None
    trainable_params: Optional[int] = None


class ExperimentTracker:
    """Experiment tracking and logging."""

    def __init__(self, experiment_config: ExperimentConfig):
        self.config = experiment_config
        self.experiment_dir = Path(experiment_config.output_dir) / experiment_config.experiment_name
        self.experiment_dir.mkdir(parents=True, exist_ok=True)

        # Initialize tracking
        self.metrics_history = []
        self.best_metric_value = float('inf') if 'loss' in experiment_config.primary_metric else 0.0
        self.best_epoch = 0

        # Save experiment config
        self._save_config()

        logger.info(f"Experiment tracker initialized: {self.experiment_dir}")

    def _save_config(self):
        """Save experiment configuration."""
        config_dict = asdict(self.config)
        config_dict['model_config'] = asdict(self.config.model_config)
        config_dict['data_config'] = asdict(self.config.data_config)

        config_path = self.experiment_dir / "config.json"
        with open(config_path, 'w') as f:
            json.dump(config_dict, f, indent=2, default=str)

    def log_metrics(self, metrics: TrainingMetrics):
        """Log training metrics."""
        self.metrics_history.append(metrics)

        # Check if this is the best model
        current_value = getattr(metrics, self.config.primary_metric.replace('val_', '').replace('train_', ''))
        if current_value is not None:
            is_better = (
                current_value < self.best_metric_value if 'loss' in self.config.primary_metric
                else current_value > self.best_metric_value
            )

            if is_better:
                self.best_metric_value = current_value
                self.best_epoch = metrics.epoch

        # Log to console
        logger.info(
            f"Epoch {metrics.epoch:3d} | "
            f"Train Loss: {metrics.train_loss:.6f} | "
            f"Val Loss: {metrics.val_loss:.6f} | "
            f"LR: {metrics.learning_rate:.2e} | "
            f"Time: {metrics.epoch_time:.1f}s"
        )

        # Save metrics to file
        self._save_metrics()

    def _save_metrics(self):
        """Save metrics history to file."""
        metrics_df = pd.DataFrame([asdict(m) for m in self.metrics_history])
        metrics_path = self.experiment_dir / "metrics.csv"
        metrics_df.to_csv(metrics_path, index=False)

    def save_checkpoint(self, model: nn.Module, optimizer, scheduler, epoch: int, is_best: bool = False):
        """Save model checkpoint."""
        checkpoint = {
            'epoch': epoch,
            'model_state_dict': model.state_dict(),
            'optimizer_state_dict': optimizer.state_dict(),
            'scheduler_state_dict': scheduler.state_dict() if scheduler else None,
            'config': self.config,
            'best_metric_value': self.best_metric_value,
            'best_epoch': self.best_epoch
        }

        # Save latest checkpoint
        checkpoint_path = self.experiment_dir / "latest_checkpoint.pt"
        torch.save(checkpoint, checkpoint_path)

        # Save best model
        if is_best:
            best_path = self.experiment_dir / "best_model.pt"
            torch.save(checkpoint, best_path)
            logger.info(f"New best model saved at epoch {epoch}")

        # Save periodic checkpoints
        if epoch % self.config.checkpoint_frequency == 0:
            periodic_path = self.experiment_dir / f"checkpoint_epoch_{epoch}.pt"
            torch.save(checkpoint, periodic_path)

    def save_predictions(self, predictions: Dict[str, np.ndarray], split: str = "test"):
        """Save model predictions."""
        predictions_dir = self.experiment_dir / "predictions"
        predictions_dir.mkdir(exist_ok=True)

        predictions_path = predictions_dir / f"{split}_predictions.pkl"
        with open(predictions_path, 'wb') as f:
            pickle.dump(predictions, f)

        logger.info(f"Predictions saved: {predictions_path}")

    def get_summary(self) -> Dict[str, Any]:
        """Get experiment summary."""
        if not self.metrics_history:
            return {}

        final_metrics = self.metrics_history[-1]

        summary = {
            'experiment_name': self.config.experiment_name,
            'total_epochs': len(self.metrics_history),
            'best_epoch': self.best_epoch,
            'best_metric_value': self.best_metric_value,
            'final_train_loss': final_metrics.train_loss,
            'final_val_loss': final_metrics.val_loss,
            'total_training_time': sum(m.epoch_time for m in self.metrics_history if m.epoch_time),
            'model_parameters': final_metrics.total_params,
            'trainable_parameters': final_metrics.trainable_params
        }

        return summary


class TFTTrainingPipeline:
    """Complete training pipeline for TFT models."""

    def __init__(self, pool: asyncpg.Pool, env_config, experiment_config: ExperimentConfig):
        self.pool = pool
        self.env = env_config
        self.config = experiment_config

        # Set random seeds
        self._set_random_seeds()

        # Initialize components
        self.data_loader = TFTDataLoader(pool, env_config)
        self.tracker = ExperimentTracker(experiment_config)

        # Model components (initialized during training)
        self.model = None
        self.trainer = None
        self.normalizer = None

        # Data loaders
        self.train_loader = None
        self.val_loader = None
        self.test_loader = None

        logger.info("TFT training pipeline initialized")

    def _set_random_seeds(self):
        """Set random seeds for reproducibility."""
        torch.manual_seed(self.config.random_seed)
        np.random.seed(self.config.random_seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed(self.config.random_seed)
            torch.cuda.manual_seed_all(self.config.random_seed)

    async def prepare_data(self):
        """Prepare training data."""
        logger.info("Preparing training data...")

        start_date = pd.to_datetime(self.config.start_date)
        end_date = pd.to_datetime(self.config.end_date)

        # Create data loaders
        self.train_loader, self.val_loader, self.test_loader, self.normalizer = (
            await self.data_loader.create_data_loaders(
                symbols=self.config.symbols,
                start_date=start_date,
                end_date=end_date,
                config=self.config.data_config,
                batch_size=self.config.batch_size
            )
        )

        logger.info(
            f"Data prepared - Train batches: {len(self.train_loader)}, "
            f"Val batches: {len(self.val_loader)}, Test batches: {len(self.test_loader)}"
        )

    def _initialize_model(self):
        """Initialize model and training components."""
        # Create model
        self.model = TemporalFusionTransformer(self.config.model_config)

        # Count parameters
        total_params = sum(p.numel() for p in self.model.parameters())
        trainable_params = sum(p.numel() for p in self.model.parameters() if p.requires_grad)

        logger.info(f"Model initialized - Total params: {total_params:,}, Trainable: {trainable_params:,}")

        # Move to device
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model = self.model.to(device)
        logger.info(f"Model moved to device: {device}")

        return total_params, trainable_params

    def _create_optimizer(self):
        """Create optimizer."""
        if self.config.optimizer_type.lower() == "adamw":
            optimizer = AdamW(
                self.model.parameters(),
                lr=self.config.learning_rate,
                weight_decay=self.config.weight_decay
            )
        else:
            optimizer = Adam(
                self.model.parameters(),
                lr=self.config.learning_rate,
                weight_decay=self.config.weight_decay
            )

        logger.info(f"Optimizer created: {type(optimizer).__name__}")
        return optimizer

    def _create_scheduler(self, optimizer):
        """Create learning rate scheduler."""
        if self.config.scheduler_type.lower() == "cosine":
            scheduler = CosineAnnealingLR(
                optimizer,
                T_max=self.config.max_epochs,
                **self.config.scheduler_params
            )
        else:
            scheduler = ReduceLROnPlateau(
                optimizer,
                mode='min' if 'loss' in self.config.primary_metric else 'max',
                **self.config.scheduler_params
            )

        logger.info(f"Scheduler created: {type(scheduler).__name__}")
        return scheduler

    def _calculate_metrics(self, predictions: torch.Tensor, targets: torch.Tensor) -> Dict[str, float]:
        """Calculate evaluation metrics."""
        predictions = predictions.detach().cpu().numpy()
        targets = targets.detach().cpu().numpy()

        # MSE
        mse = np.mean((predictions - targets) ** 2)

        # MAE
        mae = np.mean(np.abs(predictions - targets))

        # MAPE (Mean Absolute Percentage Error)
        non_zero_mask = targets != 0
        if np.any(non_zero_mask):
            mape = np.mean(np.abs((targets[non_zero_mask] - predictions[non_zero_mask]) / targets[non_zero_mask])) * 100
        else:
            mape = 0.0

        return {
            'mse': mse,
            'mae': mae,
            'mape': mape
        }

    def _train_epoch(self, optimizer, scheduler, epoch: int) -> Dict[str, float]:
        """Train for one epoch."""
        self.model.train()

        total_loss = 0
        total_metrics = {'mse': 0, 'mae': 0, 'mape': 0}
        num_batches = 0

        criterion = nn.MSELoss()
        device = next(self.model.parameters()).device

        for batch_idx, batch in enumerate(self.train_loader):
            optimizer.zero_grad()

            # Move data to device
            encoder_input = batch['encoder_input'].to(device)
            decoder_input = batch['decoder_input'].to(device)
            encoder_lengths = batch['encoder_lengths'].to(device)
            targets = batch['targets'].to(device)

            sentiment_features = None
            if 'sentiment_features' in batch:
                sentiment_features = batch['sentiment_features'].to(device)

            # Forward pass
            outputs = self.model(
                encoder_input, decoder_input, encoder_lengths, sentiment_features
            )

            # Calculate loss
            loss = criterion(outputs['predictions'], targets)

            # Backward pass
            loss.backward()

            # Gradient clipping
            if self.config.grad_clip_norm > 0:
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), self.config.grad_clip_norm)

            optimizer.step()

            # Update metrics
            total_loss += loss.item()
            batch_metrics = self._calculate_metrics(outputs['predictions'], targets)
            for key, value in batch_metrics.items():
                total_metrics[key] += value

            num_batches += 1

        # Update scheduler (for non-plateau schedulers)
        if not isinstance(scheduler, ReduceLROnPlateau):
            scheduler.step()

        # Average metrics
        avg_loss = total_loss / num_batches
        avg_metrics = {key: value / num_batches for key, value in total_metrics.items()}

        return {'loss': avg_loss, **avg_metrics}

    def _validate_epoch(self) -> Dict[str, float]:
        """Validate model performance."""
        self.model.eval()

        total_loss = 0
        total_metrics = {'mse': 0, 'mae': 0, 'mape': 0}
        num_batches = 0

        criterion = nn.MSELoss()
        device = next(self.model.parameters()).device

        with torch.no_grad():
            for batch in self.val_loader:
                # Move data to device
                encoder_input = batch['encoder_input'].to(device)
                decoder_input = batch['decoder_input'].to(device)
                encoder_lengths = batch['encoder_lengths'].to(device)
                targets = batch['targets'].to(device)

                sentiment_features = None
                if 'sentiment_features' in batch:
                    sentiment_features = batch['sentiment_features'].to(device)

                # Forward pass
                outputs = self.model(
                    encoder_input, decoder_input, encoder_lengths, sentiment_features
                )

                # Calculate loss
                loss = criterion(outputs['predictions'], targets)

                # Update metrics
                total_loss += loss.item()
                batch_metrics = self._calculate_metrics(outputs['predictions'], targets)
                for key, value in batch_metrics.items():
                    total_metrics[key] += value

                num_batches += 1

        # Average metrics
        avg_loss = total_loss / num_batches
        avg_metrics = {key: value / num_batches for key, value in total_metrics.items()}

        return {'loss': avg_loss, **avg_metrics}

    async def train(self):
        """Execute complete training pipeline."""
        logger.info("Starting TFT training pipeline")
        start_time = datetime.now()

        # Prepare data
        await self.prepare_data()

        # Initialize model
        total_params, trainable_params = self._initialize_model()

        # Create optimizer and scheduler
        optimizer = self._create_optimizer()
        scheduler = self._create_scheduler(optimizer)

        # Training loop
        best_val_loss = float('inf')
        patience_counter = 0

        for epoch in range(1, self.config.max_epochs + 1):
            epoch_start_time = datetime.now()

            # Training
            train_metrics = self._train_epoch(optimizer, scheduler, epoch)

            # Validation
            if epoch % self.config.validation_frequency == 0:
                val_metrics = self._validate_epoch()

                # Update scheduler (for plateau scheduler)
                if isinstance(scheduler, ReduceLROnPlateau):
                    scheduler.step(val_metrics['loss'])

                # Check for improvement
                current_val_loss = val_metrics['loss']
                if current_val_loss < best_val_loss:
                    best_val_loss = current_val_loss
                    patience_counter = 0
                    is_best = True
                else:
                    patience_counter += 1
                    is_best = False

                # Early stopping
                if patience_counter >= self.config.patience:
                    logger.info(f"Early stopping triggered after {epoch} epochs")
                    break
            else:
                val_metrics = {'loss': 0.0, 'mse': 0.0, 'mae': 0.0, 'mape': 0.0}
                is_best = False

            # Calculate epoch time
            epoch_time = (datetime.now() - epoch_start_time).total_seconds()
            total_time = (datetime.now() - start_time).total_seconds()

            # Create metrics object
            metrics = TrainingMetrics(
                epoch=epoch,
                train_loss=train_metrics['loss'],
                val_loss=val_metrics['loss'],
                learning_rate=optimizer.param_groups[0]['lr'],
                train_mae=train_metrics['mae'],
                val_mae=val_metrics['mae'],
                train_mape=train_metrics['mape'],
                val_mape=val_metrics['mape'],
                train_mse=train_metrics['mse'],
                val_mse=val_metrics['mse'],
                epoch_time=epoch_time,
                total_time=total_time,
                total_params=total_params,
                trainable_params=trainable_params
            )

            # Log metrics
            self.tracker.log_metrics(metrics)

            # Save checkpoint
            if epoch % self.config.checkpoint_frequency == 0 or is_best:
                self.tracker.save_checkpoint(self.model, optimizer, scheduler, epoch, is_best)

        # Final evaluation and save predictions
        await self._final_evaluation()

        # Print summary
        summary = self.tracker.get_summary()
        logger.info("Training completed!")
        logger.info(f"Best epoch: {summary.get('best_epoch', 'N/A')}")
        logger.info(f"Best {self.config.primary_metric}: {summary.get('best_metric_value', 'N/A'):.6f}")
        logger.info(f"Total training time: {summary.get('total_training_time', 0):.1f}s")

        return summary

    async def _final_evaluation(self):
        """Perform final evaluation on test set."""
        if self.test_loader is None or len(self.test_loader) == 0:
            logger.warning("No test data available for final evaluation")
            return

        logger.info("Performing final evaluation on test set")

        self.model.eval()
        device = next(self.model.parameters()).device

        all_predictions = []
        all_targets = []
        all_attention_weights = []

        with torch.no_grad():
            for batch in self.test_loader:
                # Move data to device
                encoder_input = batch['encoder_input'].to(device)
                decoder_input = batch['decoder_input'].to(device)
                encoder_lengths = batch['encoder_lengths'].to(device)
                targets = batch['targets'].to(device)

                sentiment_features = None
                if 'sentiment_features' in batch:
                    sentiment_features = batch['sentiment_features'].to(device)

                # Forward pass
                outputs = self.model(
                    encoder_input, decoder_input, encoder_lengths, sentiment_features
                )

                # Collect results
                all_predictions.append(outputs['predictions'].cpu().numpy())
                all_targets.append(targets.cpu().numpy())

                if self.config.save_attention_weights:
                    all_attention_weights.append(outputs['attention_weights'].cpu().numpy())

        # Concatenate all results
        predictions = np.concatenate(all_predictions, axis=0)
        targets = np.concatenate(all_targets, axis=0)

        # Calculate final metrics
        final_metrics = self._calculate_metrics(torch.tensor(predictions), torch.tensor(targets))
        logger.info(f"Final test metrics: {final_metrics}")

        # Save predictions
        if self.config.save_predictions:
            prediction_data = {
                'predictions': predictions,
                'targets': targets,
                'metrics': final_metrics
            }

            if self.config.save_attention_weights and all_attention_weights:
                prediction_data['attention_weights'] = np.concatenate(all_attention_weights, axis=0)

            self.tracker.save_predictions(prediction_data, "test")

    def load_checkpoint(self, checkpoint_path: str):
        """Load model from checkpoint."""
        checkpoint = torch.load(checkpoint_path, map_location='cpu')

        # Initialize model with config from checkpoint
        saved_config = checkpoint['config']
        self.model = TemporalFusionTransformer(saved_config.model_config)
        self.model.load_state_dict(checkpoint['model_state_dict'])

        logger.info(f"Model loaded from checkpoint: {checkpoint_path}")

        return checkpoint


def create_experiment_config(
    experiment_name: str,
    symbols: List[str],
    start_date: str = "2024-01-01",
    end_date: str = "2024-12-31",
    **kwargs
) -> ExperimentConfig:
    """Create experiment configuration with sensible defaults."""

    config = ExperimentConfig(
        experiment_name=experiment_name,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        **kwargs
    )

    return config


async def run_tft_experiment(
    pool: asyncpg.Pool,
    env_config,
    experiment_config: ExperimentConfig
) -> Dict[str, Any]:
    """Run a complete TFT training experiment."""

    pipeline = TFTTrainingPipeline(pool, env_config, experiment_config)
    summary = await pipeline.train()

    return summary


# Example usage
if __name__ == "__main__":
    pass

    async def main():
        # Example experiment configuration
        config = create_experiment_config(
            experiment_name="tft_baseline_experiment",
            symbols=["AAPL", "MSFT", "GOOGL"],
            start_date="2024-01-01",
            end_date="2024-06-30",
            max_epochs=50,
            batch_size=32,
            learning_rate=1e-3
        )

        # Run experiment (would need actual database connection)
        # summary = await run_tft_experiment(pool, env, config)
        # print(f"Experiment completed: {summary}")

    # asyncio.run(main())