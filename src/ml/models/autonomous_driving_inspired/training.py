"""
Training Pipeline for Autonomous Driving Inspired Financial Transformer

Implements:
- Multi-task loss functions for financial predictions
- Curriculum learning for progressive complexity
- Advanced training utilities and metrics
- Model checkpointing and evaluation
"""

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.tensorboard import SummaryWriter
from torch.optim.lr_scheduler import OneCycleLR, ReduceLROnPlateau
from typing import Dict, List, Tuple, Optional, Any, Union, Callable
import numpy as np
import pandas as pd
from pathlib import Path
import json
import logging
from datetime import datetime
from dataclasses import dataclass, asdict
import matplotlib.pyplot as plt
import seaborn as sns

from .transformer_model import AutonomousFinanceTransformer, TransformerConfig
from .data_preprocessing import AutonomousFinanceDataLoader

logger = logging.getLogger(__name__)


@dataclass
class TrainingConfig:
    """Configuration for training the autonomous finance transformer."""
    # Training hyperparameters
    learning_rate: float = 1e-4
    batch_size: int = 32
    num_epochs: int = 100
    warmup_epochs: int = 10
    max_lr: float = 1e-3
    min_lr: float = 1e-6
    weight_decay: float = 0.01
    gradient_clip_norm: float = 1.0

    # Multi-task loss weights
    task_weights: Dict[str, float] = None

    # Curriculum learning
    curriculum_enabled: bool = True
    curriculum_schedule: List[Dict[str, Any]] = None

    # Evaluation and checkpointing
    eval_every_n_epochs: int = 5
    save_top_k_models: int = 3
    early_stopping_patience: int = 15

    # Paths
    checkpoint_dir: str = "/tmp/autonomous_finance_checkpoints"
    tensorboard_log_dir: str = "/tmp/autonomous_finance_logs"

    def __post_init__(self):
        if self.task_weights is None:
            self.task_weights = {
                'price_movement': 1.0,    # Primary prediction task
                'volatility': 0.8,        # Important for risk management
                'volume_profile': 0.6,    # Market microstructure
                'regime_change': 0.4,     # Market regime detection
                'risk_assessment': 0.7    # Downside protection
            }

        if self.curriculum_schedule is None:
            self.curriculum_schedule = [
                {'epoch': 0, 'timeframes': ['1h'], 'prediction_horizon': 1},
                {'epoch': 10, 'timeframes': ['1h', '1d'], 'prediction_horizon': 3},
                {'epoch': 25, 'timeframes': ['15m', '1h', '1d'], 'prediction_horizon': 5},
                {'epoch': 50, 'timeframes': ['5m', '15m', '1h', '1d', '1w'], 'prediction_horizon': 10}
            ]


class MultiTaskLoss(nn.Module):
    """
    Multi-task loss function for financial transformer.

    Combines losses from different prediction tasks with adaptive weighting
    and uncertainty-based balancing inspired by autonomous driving systems.
    """

    def __init__(self, config: TrainingConfig):
        super().__init__()
        self.config = config
        self.task_weights = config.task_weights

        # Loss functions for different tasks
        self.regression_loss = nn.MSELoss(reduction='mean')
        self.classification_loss = nn.CrossEntropyLoss(reduction='mean')

        # Adaptive uncertainty-based weighting (learnable parameters)
        self.log_vars = nn.Parameter(torch.zeros(len(self.task_weights)))

        self.task_names = list(self.task_weights.keys())

    def forward(self, predictions: Dict[str, torch.Tensor],
                targets: Dict[str, torch.Tensor],
                return_individual_losses: bool = False) -> Union[torch.Tensor, Tuple[torch.Tensor, Dict[str, torch.Tensor]]]:
        """
        Compute multi-task loss.

        Args:
            predictions: Dict mapping task_name -> [batch, horizon, output_dim]
            targets: Dict mapping task_name -> [batch, horizon, output_dim]
            return_individual_losses: Whether to return individual task losses

        Returns:
            total_loss: Weighted sum of all task losses
            individual_losses: Optional dict of individual task losses
        """
        individual_losses = {}
        total_loss = 0.0

        for task_idx, task_name in enumerate(self.task_names):
            if task_name not in predictions or task_name not in targets:
                continue

            pred = predictions[task_name]  # [batch, horizon, output_dim]
            target = targets[task_name]

            # Compute task-specific loss
            if task_name == 'regime_change':
                # Classification loss - need to reshape for cross-entropy
                pred_flat = pred.view(-1, pred.shape[-1])  # [batch*horizon, num_classes]
                target_flat = target.view(-1).long()  # [batch*horizon]
                task_loss = self.classification_loss(pred_flat, target_flat)
            else:
                # Regression loss
                task_loss = self.regression_loss(pred, target)

            # Apply base task weight
            base_weight = self.task_weights[task_name]

            # Apply adaptive uncertainty-based weighting
            # Based on "Multi-Task Learning Using Uncertainty to Weigh Losses"
            precision = torch.exp(-self.log_vars[task_idx])
            uncertainty_weighted_loss = precision * task_loss + self.log_vars[task_idx]

            # Final weighted loss
            weighted_loss = base_weight * uncertainty_weighted_loss

            individual_losses[task_name] = {
                'raw_loss': task_loss.item(),
                'uncertainty_weighted': uncertainty_weighted_loss.item(),
                'final_weighted': weighted_loss.item(),
                'uncertainty_param': self.log_vars[task_idx].item()
            }

            total_loss += weighted_loss

        if return_individual_losses:
            return total_loss, individual_losses
        else:
            return total_loss


class CurriculumScheduler:
    """
    Curriculum learning scheduler for progressive training complexity.

    Inspired by autonomous driving training, gradually increases:
    1. Number of timeframes (sensor modalities)
    2. Prediction horizon length
    3. Task complexity
    """

    def __init__(self, config: TrainingConfig):
        self.config = config
        self.schedule = config.curriculum_schedule
        self.current_stage = 0

    def get_current_curriculum(self, epoch: int) -> Dict[str, Any]:
        """Get current curriculum settings based on epoch."""
        if not self.config.curriculum_enabled:
            # Return full complexity if curriculum learning disabled
            return {
                'timeframes': ['5m', '15m', '1h', '1d', '1w'],
                'prediction_horizon': 10,
                'active_tasks': list(self.config.task_weights.keys())
            }

        # Find current stage based on epoch
        current_settings = self.schedule[0]
        for stage in self.schedule:
            if epoch >= stage['epoch']:
                current_settings = stage
            else:
                break

        # Add default settings
        result = {
            'timeframes': current_settings.get('timeframes', ['1h']),
            'prediction_horizon': current_settings.get('prediction_horizon', 1),
            'active_tasks': current_settings.get('active_tasks', ['price_movement'])
        }

        return result


class FinancialMetrics:
    """
    Financial-specific metrics for evaluating model performance.

    Includes traditional ML metrics plus financial performance indicators.
    """

    @staticmethod
    def directional_accuracy(predictions: torch.Tensor, targets: torch.Tensor) -> float:
        """Compute directional accuracy (% of correct up/down predictions)."""
        pred_direction = torch.sign(predictions)
        target_direction = torch.sign(targets)
        correct = (pred_direction == target_direction).float()
        return correct.mean().item()

    @staticmethod
    def sharpe_ratio(predictions: torch.Tensor, targets: torch.Tensor, risk_free_rate: float = 0.02) -> float:
        """Compute Sharpe ratio based on prediction-based returns."""
        # Simulate trading based on predictions
        positions = torch.sign(predictions).squeeze()
        returns = (positions * targets.squeeze()).mean(dim=0)  # Average across batch

        if len(returns.shape) > 0:
            returns = returns.mean()  # Average across time horizon

        # Annualized Sharpe ratio (assuming hourly predictions)
        hourly_return = returns.item()
        annual_return = hourly_return * 24 * 365
        annual_vol = torch.std(positions * targets.squeeze()).item() * np.sqrt(24 * 365)

        if annual_vol == 0:
            return 0.0

        sharpe = (annual_return - risk_free_rate) / annual_vol
        return sharpe

    @staticmethod
    def maximum_drawdown(predictions: torch.Tensor, targets: torch.Tensor) -> float:
        """Compute maximum drawdown of prediction-based strategy."""
        positions = torch.sign(predictions).squeeze()
        returns = (positions * targets.squeeze())

        # Cumulative returns
        if len(returns.shape) > 1:
            cum_returns = torch.cumprod(1 + returns.mean(dim=0), dim=0)
        else:
            cum_returns = torch.cumprod(1 + returns, dim=0)

        # Running maximum
        running_max = torch.maximum.accumulate(cum_returns, dim=0)[0]

        # Drawdown
        drawdown = (cum_returns - running_max) / running_max
        max_drawdown = torch.min(drawdown).item()

        return max_drawdown

    @staticmethod
    def compute_all_metrics(predictions: Dict[str, torch.Tensor],
                           targets: Dict[str, torch.Tensor]) -> Dict[str, Dict[str, float]]:
        """Compute all financial metrics for each task."""
        metrics = {}

        for task_name in predictions.keys():
            if task_name not in targets:
                continue

            pred = predictions[task_name]
            target = targets[task_name]

            task_metrics = {}

            if task_name == 'regime_change':
                # Classification metrics
                pred_classes = torch.argmax(pred, dim=-1)
                target_classes = target.long().squeeze(-1) if target.dim() > pred_classes.dim() else target.long()

                accuracy = (pred_classes == target_classes).float().mean().item()
                task_metrics['accuracy'] = accuracy
            else:
                # Regression metrics
                mse = F.mse_loss(pred, target).item()
                mae = F.l1_loss(pred, target).item()

                task_metrics['mse'] = mse
                task_metrics['mae'] = mae
                task_metrics['directional_accuracy'] = FinancialMetrics.directional_accuracy(pred, target)
                task_metrics['sharpe_ratio'] = FinancialMetrics.sharpe_ratio(pred, target)
                task_metrics['max_drawdown'] = FinancialMetrics.maximum_drawdown(pred, target)

            metrics[task_name] = task_metrics

        return metrics


class AutonomousFinanceTrainer:
    """
    Main trainer for the autonomous driving inspired financial transformer.

    Handles:
    - Multi-task training with curriculum learning
    - Advanced optimization and scheduling
    - Comprehensive evaluation and metrics
    - Model checkpointing and visualization
    """

    def __init__(self, model: AutonomousFinanceTransformer,
                 train_loader: Any, val_loader: Any,
                 config: TrainingConfig):
        self.model = model
        self.train_loader = train_loader
        self.val_loader = val_loader
        self.config = config

        # Initialize training components
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.model.to(self.device)

        # Loss function and optimizer
        self.criterion = MultiTaskLoss(config)
        self.optimizer = optim.AdamW(
            self.model.parameters(),
            lr=config.learning_rate,
            weight_decay=config.weight_decay,
            betas=(0.9, 0.95)  # Following transformer best practices
        )

        # Learning rate scheduler
        total_steps = len(train_loader) * config.num_epochs
        self.scheduler = OneCycleLR(
            self.optimizer,
            max_lr=config.max_lr,
            total_steps=total_steps,
            pct_start=config.warmup_epochs / config.num_epochs,
            anneal_strategy='cos'
        )

        # Curriculum learning
        self.curriculum_scheduler = CurriculumScheduler(config)

        # Tracking
        self.training_history = {
            'train_losses': [],
            'val_losses': [],
            'metrics': [],
            'learning_rates': [],
            'curriculum_stages': []
        }

        # Setup directories
        self.checkpoint_dir = Path(config.checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # TensorBoard logging
        self.writer = SummaryWriter(config.tensorboard_log_dir)

        # Best model tracking
        self.best_models = []  # List of (loss, epoch, model_path) tuples

        logger.info(f"Initialized AutonomousFinanceTrainer on device: {self.device}")
        logger.info(f"Model has {sum(p.numel() for p in self.model.parameters()):,} parameters")

    def train_epoch(self, epoch: int) -> Dict[str, float]:
        """Train for one epoch."""
        self.model.train()
        epoch_losses = []
        epoch_metrics = []

        # Get current curriculum settings
        curriculum = self.curriculum_scheduler.get_current_curriculum(epoch)

        for batch_idx, batch in enumerate(self.train_loader):
            # Move data to device
            timeframe_sequences = {k: v.to(self.device) for k, v in batch['timeframe_sequences'].items()}
            position_data = {tf: {k: v.to(self.device) for k, v in pos_data.items()}
                           for tf, pos_data in batch['position_data'].items()}
            targets = batch['targets'].to(self.device) if 'targets' in batch else None

            # Forward pass
            self.optimizer.zero_grad()

            outputs = self.model(timeframe_sequences, position_data)
            predictions = outputs['predictions']

            # Create mock targets for multi-task learning
            batch_size = next(iter(predictions.values())).shape[0]
            mock_targets = self._create_mock_targets(predictions, batch_size)

            # Compute loss
            total_loss, individual_losses = self.criterion(
                predictions, mock_targets, return_individual_losses=True
            )

            # Backward pass
            total_loss.backward()

            # Gradient clipping
            torch.nn.utils.clip_grad_norm_(
                self.model.parameters(), self.config.gradient_clip_norm
            )

            self.optimizer.step()
            self.scheduler.step()

            # Logging
            epoch_losses.append(total_loss.item())

            # Compute metrics periodically
            if batch_idx % 50 == 0:
                metrics = FinancialMetrics.compute_all_metrics(predictions, mock_targets)
                epoch_metrics.append(metrics)

                logger.info(f"Epoch {epoch}, Batch {batch_idx}: Loss = {total_loss.item():.4f}")

        # Average epoch metrics
        avg_loss = np.mean(epoch_losses)
        avg_metrics = self._average_metrics(epoch_metrics)

        return {'loss': avg_loss, 'metrics': avg_metrics}

    def validate_epoch(self, epoch: int) -> Dict[str, float]:
        """Validate for one epoch."""
        self.model.eval()
        val_losses = []
        val_metrics = []

        with torch.no_grad():
            for batch in self.val_loader:
                # Move data to device
                timeframe_sequences = {k: v.to(self.device) for k, v in batch['timeframe_sequences'].items()}
                position_data = {tf: {k: v.to(self.device) for k, v in pos_data.items()}
                               for tf, pos_data in batch['position_data'].items()}

                # Forward pass
                outputs = self.model(timeframe_sequences, position_data)
                predictions = outputs['predictions']

                # Create mock targets
                batch_size = next(iter(predictions.values())).shape[0]
                mock_targets = self._create_mock_targets(predictions, batch_size)

                # Compute loss and metrics
                val_loss = self.criterion(predictions, mock_targets)
                metrics = FinancialMetrics.compute_all_metrics(predictions, mock_targets)

                val_losses.append(val_loss.item())
                val_metrics.append(metrics)

        avg_val_loss = np.mean(val_losses)
        avg_val_metrics = self._average_metrics(val_metrics)

        return {'loss': avg_val_loss, 'metrics': avg_val_metrics}

    def _create_mock_targets(self, predictions: Dict[str, torch.Tensor],
                           batch_size: int) -> Dict[str, torch.Tensor]:
        """Create mock targets for training (in real implementation, use real targets)."""
        mock_targets = {}

        for task_name, pred in predictions.items():
            if task_name == 'regime_change':
                # Random regime targets (0=Bull, 1=Bear, 2=Sideways, 3=Transition)
                mock_targets[task_name] = torch.randint(0, 4, pred.shape[:-1], device=pred.device)
            else:
                # Random financial targets with realistic scale
                mock_targets[task_name] = torch.randn_like(pred) * 0.02  # ±2% movements

        return mock_targets

    def _average_metrics(self, metrics_list: List[Dict]) -> Dict[str, Dict[str, float]]:
        """Average metrics across batches."""
        if not metrics_list:
            return {}

        avg_metrics = {}
        task_names = metrics_list[0].keys()

        for task_name in task_names:
            avg_metrics[task_name] = {}
            metric_names = metrics_list[0][task_name].keys()

            for metric_name in metric_names:
                values = [m[task_name][metric_name] for m in metrics_list if task_name in m and metric_name in m[task_name]]
                if values:
                    avg_metrics[task_name][metric_name] = np.mean(values)

        return avg_metrics

    def save_checkpoint(self, epoch: int, val_loss: float, is_best: bool = False):
        """Save model checkpoint."""
        checkpoint = {
            'epoch': epoch,
            'model_state_dict': self.model.state_dict(),
            'optimizer_state_dict': self.optimizer.state_dict(),
            'scheduler_state_dict': self.scheduler.state_dict(),
            'val_loss': val_loss,
            'config': asdict(self.config),
            'model_config': asdict(self.model.config),
            'training_history': self.training_history
        }

        # Save regular checkpoint
        checkpoint_path = self.checkpoint_dir / f"checkpoint_epoch_{epoch}.pt"
        torch.save(checkpoint, checkpoint_path)

        # Save best model
        if is_best:
            best_path = self.checkpoint_dir / "best_model.pt"
            torch.save(checkpoint, best_path)
            logger.info(f"Saved best model with validation loss: {val_loss:.4f}")

        # Maintain top-k models
        self.best_models.append((val_loss, epoch, checkpoint_path))
        self.best_models.sort(key=lambda x: x[0])  # Sort by loss

        # Remove excess checkpoints
        if len(self.best_models) > self.config.save_top_k_models:
            _, _, path_to_remove = self.best_models.pop()
            if path_to_remove.exists():
                path_to_remove.unlink()

    def train(self):
        """Main training loop."""
        logger.info("Starting training...")
        best_val_loss = float('inf')
        patience_counter = 0

        for epoch in range(self.config.num_epochs):
            # Training
            train_results = self.train_epoch(epoch)

            # Validation
            if epoch % self.config.eval_every_n_epochs == 0:
                val_results = self.validate_epoch(epoch)

                # Check for improvement
                val_loss = val_results['loss']
                is_best = val_loss < best_val_loss

                if is_best:
                    best_val_loss = val_loss
                    patience_counter = 0
                else:
                    patience_counter += 1

                # Save checkpoint
                self.save_checkpoint(epoch, val_loss, is_best)

                # Early stopping
                if patience_counter >= self.config.early_stopping_patience:
                    logger.info(f"Early stopping at epoch {epoch}")
                    break

                # Logging
                logger.info(f"Epoch {epoch}: Train Loss = {train_results['loss']:.4f}, "
                           f"Val Loss = {val_loss:.4f}")

                # TensorBoard logging
                self.writer.add_scalar('Loss/Train', train_results['loss'], epoch)
                self.writer.add_scalar('Loss/Validation', val_loss, epoch)
                self.writer.add_scalar('Learning_Rate', self.scheduler.get_last_lr()[0], epoch)

                # Log metrics
                for task_name, task_metrics in val_results['metrics'].items():
                    for metric_name, metric_value in task_metrics.items():
                        self.writer.add_scalar(f'{task_name}/{metric_name}', metric_value, epoch)

            # Update training history
            self.training_history['train_losses'].append(train_results['loss'])
            if epoch % self.config.eval_every_n_epochs == 0:
                self.training_history['val_losses'].append(val_results['loss'])
                self.training_history['metrics'].append(val_results['metrics'])
            self.training_history['learning_rates'].append(self.scheduler.get_last_lr()[0])

            # Curriculum info
            curriculum = self.curriculum_scheduler.get_current_curriculum(epoch)
            self.training_history['curriculum_stages'].append(curriculum)

        logger.info("Training completed!")
        self.writer.close()

        # Save final training history
        history_path = self.checkpoint_dir / "training_history.json"
        with open(history_path, 'w') as f:
            json.dump(self.training_history, f, indent=2, default=str)


if __name__ == "__main__":
    # Test training pipeline
    logging.basicConfig(level=logging.INFO)

    # Create model and data
    model_config = TransformerConfig(d_model=128, num_heads=4, num_layers=2)  # Smaller for testing
    model = AutonomousFinanceTransformer(model_config)

    # Mock data loaders (in real use, would use actual data)
    class MockDataLoader:
        def __init__(self, num_batches=10):
            self.num_batches = num_batches

        def __len__(self):
            return self.num_batches

        def __iter__(self):
            for _ in range(self.num_batches):
                batch_size = 4
                yield {
                    'timeframe_sequences': {
                        '5m': torch.randn(batch_size, 52, 6),
                        '1h': torch.randn(batch_size, 24, 6),
                        '1d': torch.randn(batch_size, 20, 6)
                    },
                    'position_data': {
                        '5m': {
                            'timeframe_ids': torch.zeros(batch_size, 52, dtype=torch.long),
                            'bar_indices': torch.arange(52).unsqueeze(0).repeat(batch_size, 1),
                            'temporal_offsets': torch.arange(52, 0, -1).unsqueeze(0).repeat(batch_size, 1),
                            'market_regimes': torch.zeros(batch_size, 52, dtype=torch.long)
                        },
                        '1h': {
                            'timeframe_ids': torch.ones(batch_size, 24, dtype=torch.long),
                            'bar_indices': torch.arange(24).unsqueeze(0).repeat(batch_size, 1),
                            'temporal_offsets': torch.arange(24, 0, -1).unsqueeze(0).repeat(batch_size, 1),
                            'market_regimes': torch.zeros(batch_size, 24, dtype=torch.long)
                        },
                        '1d': {
                            'timeframe_ids': torch.full((batch_size, 20), 2, dtype=torch.long),
                            'bar_indices': torch.arange(20).unsqueeze(0).repeat(batch_size, 1),
                            'temporal_offsets': torch.arange(20, 0, -1).unsqueeze(0).repeat(batch_size, 1),
                            'market_regimes': torch.zeros(batch_size, 20, dtype=torch.long)
                        }
                    }
                }

    train_loader = MockDataLoader(10)
    val_loader = MockDataLoader(5)

    # Training configuration
    training_config = TrainingConfig(
        num_epochs=5,  # Short test run
        eval_every_n_epochs=2,
        checkpoint_dir="/tmp/test_autonomous_finance_checkpoints",
        tensorboard_log_dir="/tmp/test_autonomous_finance_logs"
    )

    # Create trainer
    trainer = AutonomousFinanceTrainer(model, train_loader, val_loader, training_config)

    print("Testing AutonomousFinanceTrainer...")

    # Run short training
    trainer.train()

    print("\n✅ Training pipeline test completed successfully!")