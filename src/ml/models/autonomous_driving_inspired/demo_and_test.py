"""
Demo and Test Script for Autonomous Driving Inspired Financial Transformer

This script demonstrates:
1. Loading real training data from ArrayRecord files
2. Setting up the autonomous driving inspired transformer 
3. Training the model with multi-task objectives
4. Evaluating performance with financial metrics
5. Visualizing attention patterns and predictions

Usage:
    python demo_and_test.py --data_path /mnt/d/ats-data/training_data/83 --symbol AAPL
"""

import argparse
import logging
import torch
import torch.nn.functional as F
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, Any, List, Tuple
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Import our autonomous driving inspired components
from .data_preprocessing import (
    AutonomousFinanceDataLoader, MultiTimeframeProcessor, 
    TimeframeConfig, AutonomousFinanceDataset
)
from .transformer_model import (
    AutonomousFinanceTransformer, TransformerConfig
)
from .training import (
    AutonomousFinanceTrainer, TrainingConfig, MultiTaskLoss, FinancialMetrics
)
from .attention_mechanisms import AttentionConfig

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AutonomousFinanceDemo:
    """
    Comprehensive demo of the autonomous driving inspired financial transformer.
    """
    
    def __init__(self, data_path: Path, symbol: str = "AAPL", device: str = "auto"):
        self.data_path = Path(data_path)
        self.symbol = symbol
        self.device = torch.device(
            "cuda" if device == "auto" and torch.cuda.is_available() 
            else "cpu" if device == "auto" 
            else device
        )
        
        logger.info(f"Initializing AutonomousFinanceDemo for {symbol}")
        logger.info(f"Data path: {self.data_path}")
        logger.info(f"Device: {self.device}")
        
        # Initialize configurations
        self.setup_configs()
        
        # Initialize components (will be set up in setup methods)
        self.data_loader = None
        self.model = None
        self.trainer = None
    
    def setup_configs(self):
        """Setup model and training configurations."""
        # Transformer configuration - smaller for demo
        self.transformer_config = TransformerConfig(
            d_model=256,
            num_heads=8,
            num_layers=4,
            dropout=0.1,
            attention_temperature=1.0,
            temporal_memory_size=50,
            num_tasks=5,
            prediction_horizon=10
        )
        
        # Training configuration  
        self.training_config = TrainingConfig(
            learning_rate=1e-4,
            batch_size=4,  # Small batch for demo
            num_epochs=20,
            warmup_epochs=3,
            max_lr=5e-4,
            eval_every_n_epochs=2,
            early_stopping_patience=10,
            checkpoint_dir=f"/tmp/autonomous_finance_demo_{self.symbol}",
            tensorboard_log_dir=f"/tmp/autonomous_finance_logs_{self.symbol}",
            curriculum_enabled=True
        )
        
        logger.info(f"Model: {self.transformer_config.d_model}D, {self.transformer_config.num_layers} layers")
        logger.info(f"Training: {self.training_config.num_epochs} epochs, batch size {self.training_config.batch_size}")
    
    def setup_data(self) -> bool:
        """Setup data loaders with real ArrayRecord data."""
        try:
            logger.info("Setting up data loaders...")
            
            # Create data loader factory
            self.data_loader_factory = AutonomousFinanceDataLoader(
                data_dir=self.data_path,
                batch_size=self.training_config.batch_size,
                num_workers=0  # Avoid multiprocessing issues in demo
            )
            
            # Create train and validation loaders
            # Note: Using same data for both train/val in demo - in practice use separate datasets
            try:
                self.train_loader = self.data_loader_factory.create_train_loader(
                    symbol=self.symbol
                )
                self.val_loader = self.data_loader_factory.create_val_loader(
                    symbol=self.symbol
                )
                
                # Test loading a batch
                test_batch = next(iter(self.train_loader))
                logger.info("✅ Successfully loaded test batch")
                logger.info(f"   Timeframes available: {list(test_batch['timeframe_sequences'].keys())}")
                
                for tf_name, sequences in test_batch['timeframe_sequences'].items():
                    logger.info(f"   {tf_name}: {sequences.shape}")
                
                return True
                
            except Exception as e:
                logger.error(f"Failed to create data loaders: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to setup data: {e}")
            return False
    
    def setup_model(self):
        """Setup the autonomous driving inspired transformer model."""
        logger.info("Setting up AutonomousFinanceTransformer...")
        
        self.model = AutonomousFinanceTransformer(self.transformer_config)
        self.model.to(self.device)
        
        # Print model info
        model_info = self.model.get_model_info()
        logger.info(f"Model initialized:")
        logger.info(f"   Total parameters: {model_info['total_parameters']:,}")
        logger.info(f"   Model size: {model_info['model_size_mb']:.2f} MB")
        logger.info(f"   Components: {model_info['components']}")
    
    def test_forward_pass(self):
        """Test forward pass with real data."""
        logger.info("Testing forward pass...")
        
        try:
            # Get a batch of real data
            batch = next(iter(self.train_loader))
            
            # Move to device
            timeframe_sequences = {
                k: v.to(self.device) for k, v in batch['timeframe_sequences'].items()
            }
            position_data = {
                tf: {k: v.to(self.device) for k, v in pos_data.items()}
                for tf, pos_data in batch['position_data'].items()
            }
            
            # Forward pass
            self.model.eval()
            with torch.no_grad():
                outputs = self.model(
                    timeframe_sequences, 
                    position_data, 
                    return_attention_weights=True
                )
            
            # Analyze outputs
            predictions = outputs['predictions']
            attention_weights = outputs['attention_weights']
            
            logger.info("✅ Forward pass successful!")
            logger.info(f"Predictions generated for tasks:")
            for task_name, pred in predictions.items():
                logger.info(f"   {task_name}: {pred.shape}")
                
                # Show sample predictions
                sample_pred = pred[0].detach().cpu().numpy()  # First sample
                logger.info(f"      Sample values: {sample_pred[:3].flatten()}")
            
            logger.info(f"Attention weights available for {len(attention_weights)} layers")
            
            return outputs
            
        except Exception as e:
            logger.error(f"Forward pass failed: {e}")
            raise
    
    def train_model(self):
        """Train the model with real data."""
        logger.info("Starting model training...")
        
        # Setup trainer
        self.trainer = AutonomousFinanceTrainer(
            model=self.model,
            train_loader=self.train_loader,
            val_loader=self.val_loader,
            config=self.training_config
        )
        
        # Train the model
        try:
            self.trainer.train()
            logger.info("✅ Training completed successfully!")
            
            # Load best model
            best_model_path = Path(self.training_config.checkpoint_dir) / "best_model.pt"
            if best_model_path.exists():
                checkpoint = torch.load(best_model_path, map_location=self.device)
                self.model.load_state_dict(checkpoint['model_state_dict'])
                logger.info(f"Loaded best model from epoch {checkpoint['epoch']}")
            
        except Exception as e:
            logger.error(f"Training failed: {e}")
            raise
    
    def evaluate_model(self) -> Dict[str, Any]:
        """Comprehensive model evaluation."""
        logger.info("Evaluating model performance...")
        
        self.model.eval()
        all_predictions = []
        all_targets = []
        all_attention_weights = []
        
        with torch.no_grad():
            for batch_idx, batch in enumerate(self.val_loader):
                # Move to device
                timeframe_sequences = {
                    k: v.to(self.device) for k, v in batch['timeframe_sequences'].items()
                }
                position_data = {
                    tf: {k: v.to(self.device) for k, v in pos_data.items()}
                    for tf, pos_data in batch['position_data'].items()
                }
                
                # Forward pass
                outputs = self.model(
                    timeframe_sequences, 
                    position_data, 
                    return_attention_weights=True
                )
                
                predictions = outputs['predictions']
                attention_weights = outputs['attention_weights']
                
                # Create mock targets for evaluation
                mock_targets = self._create_mock_targets(predictions)
                
                all_predictions.append(predictions)
                all_targets.append(mock_targets)
                all_attention_weights.append(attention_weights)
                
                # Limit batches for demo
                if batch_idx >= 5:
                    break
        
        # Combine all predictions and targets
        combined_predictions = {}
        combined_targets = {}
        
        for task_name in all_predictions[0].keys():
            pred_list = [batch_pred[task_name] for batch_pred in all_predictions]
            target_list = [batch_target[task_name] for batch_target in all_targets]
            
            combined_predictions[task_name] = torch.cat(pred_list, dim=0)
            combined_targets[task_name] = torch.cat(target_list, dim=0)
        
        # Compute comprehensive metrics
        metrics = FinancialMetrics.compute_all_metrics(combined_predictions, combined_targets)
        
        logger.info("📊 Model Evaluation Results:")
        for task_name, task_metrics in metrics.items():
            logger.info(f"   {task_name.upper()}:")
            for metric_name, metric_value in task_metrics.items():
                logger.info(f"      {metric_name}: {metric_value:.4f}")
        
        evaluation_results = {
            'metrics': metrics,
            'predictions': combined_predictions,
            'targets': combined_targets,
            'attention_weights': all_attention_weights
        }
        
        return evaluation_results
    
    def visualize_attention(self, attention_weights: List[List[Dict]], save_path: str = "/tmp"):
        """Visualize attention patterns."""
        logger.info("Creating attention visualizations...")
        
        try:
            save_dir = Path(save_path)
            save_dir.mkdir(parents=True, exist_ok=True)
            
            # Use first batch, last layer attention weights
            if not attention_weights:
                logger.warning("No attention weights available for visualization")
                return
            
            last_layer_attention = attention_weights[0][-1]  # First batch, last layer
            
            # Visualize sensor cross-attention (timeframes)
            if 'sensor_cross_attention' in last_layer_attention and last_layer_attention['sensor_cross_attention']:
                fig, axes = plt.subplots(2, 3, figsize=(18, 12))
                fig.suptitle('Sensor Cross-Attention: Task Queries → Timeframes', fontsize=16)
                
                timeframes = list(last_layer_attention['sensor_cross_attention'].keys())
                
                for idx, tf_name in enumerate(timeframes[:6]):  # Show up to 6 timeframes
                    ax = axes[idx // 3, idx % 3]
                    
                    attention_matrix = last_layer_attention['sensor_cross_attention'][tf_name]
                    # Average over heads and take first sample: [batch, heads, tasks, seq_len] -> [tasks, seq_len]
                    avg_attention = attention_matrix[0].mean(dim=0).detach().cpu().numpy()
                    
                    sns.heatmap(avg_attention, ax=ax, cmap='Blues', cbar=True)
                    ax.set_title(f'{tf_name} Timeframe')
                    ax.set_xlabel('Sequence Position')
                    ax.set_ylabel('Task Query')
                    ax.set_yticklabels(['Price', 'Vol', 'Volume', 'Regime', 'Risk'])
                
                # Hide unused subplots
                for idx in range(len(timeframes), 6):
                    axes[idx // 3, idx % 3].set_visible(False)
                
                plt.tight_layout()
                plt.savefig(save_dir / 'sensor_cross_attention.png', dpi=300, bbox_inches='tight')
                plt.close()
                
                logger.info(f"✅ Saved sensor cross-attention visualization")
            
            # Visualize task self-attention
            if 'task_self_attention' in last_layer_attention and last_layer_attention['task_self_attention'] is not None:
                task_attention = last_layer_attention['task_self_attention']
                # Average over heads and batch: [batch, heads, tasks, tasks] -> [tasks, tasks] 
                avg_task_attention = task_attention[0].mean(dim=0).detach().cpu().numpy()
                
                plt.figure(figsize=(8, 6))
                sns.heatmap(
                    avg_task_attention, 
                    cmap='Reds', 
                    xticklabels=['Price', 'Vol', 'Volume', 'Regime', 'Risk'],
                    yticklabels=['Price', 'Vol', 'Volume', 'Regime', 'Risk'],
                    annot=True,
                    fmt='.3f'
                )
                plt.title('Task Self-Attention: Inter-Task Dependencies')
                plt.xlabel('Target Task')
                plt.ylabel('Source Task')
                plt.tight_layout()
                plt.savefig(save_dir / 'task_self_attention.png', dpi=300, bbox_inches='tight')
                plt.close()
                
                logger.info(f"✅ Saved task self-attention visualization")
            
        except Exception as e:
            logger.error(f"Failed to create attention visualizations: {e}")
    
    def visualize_predictions(self, predictions: Dict[str, torch.Tensor], 
                            targets: Dict[str, torch.Tensor], save_path: str = "/tmp"):
        """Visualize model predictions."""
        logger.info("Creating prediction visualizations...")
        
        try:
            save_dir = Path(save_path)
            save_dir.mkdir(parents=True, exist_ok=True)
            
            # Plot predictions vs targets for each task
            fig, axes = plt.subplots(2, 3, figsize=(18, 12))
            fig.suptitle('Model Predictions vs Targets (10-Hour Horizon)', fontsize=16)
            
            task_names = ['price_movement', 'volatility', 'volume_profile', 'regime_change', 'risk_assessment']
            
            for idx, task_name in enumerate(task_names):
                if task_name not in predictions:
                    continue
                
                ax = axes[idx // 3, idx % 3]
                
                pred = predictions[task_name].detach().cpu().numpy()
                target = targets[task_name].detach().cpu().numpy()
                
                if task_name == 'regime_change':
                    # Classification - show regime probabilities for first sample
                    sample_pred = pred[0]  # [horizon, num_classes]
                    regime_names = ['Bull', 'Bear', 'Sideways', 'Transition']
                    
                    x = np.arange(sample_pred.shape[0])
                    for class_idx, regime_name in enumerate(regime_names):
                        ax.plot(x, sample_pred[:, class_idx], label=regime_name, marker='o')
                    
                    ax.set_title(f'{task_name.replace("_", " ").title()}')
                    ax.set_xlabel('Hour Ahead')
                    ax.set_ylabel('Probability')
                    ax.legend()
                    ax.grid(True, alpha=0.3)
                else:
                    # Regression - show predictions vs targets for multiple samples
                    for sample_idx in range(min(3, pred.shape[0])):  # Show up to 3 samples
                        sample_pred = pred[sample_idx].flatten()
                        sample_target = target[sample_idx].flatten()
                        
                        x = np.arange(len(sample_pred))
                        ax.plot(x, sample_pred, 'o-', alpha=0.7, label=f'Pred {sample_idx+1}')
                        ax.plot(x, sample_target, 's--', alpha=0.7, label=f'Target {sample_idx+1}')
                    
                    ax.set_title(f'{task_name.replace("_", " ").title()}')
                    ax.set_xlabel('Hour Ahead')
                    ax.set_ylabel('Value')
                    ax.legend()
                    ax.grid(True, alpha=0.3)
            
            # Hide unused subplot
            axes[1, 2].set_visible(False)
            
            plt.tight_layout()
            plt.savefig(save_dir / 'predictions_vs_targets.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"✅ Saved prediction visualizations")
            
        except Exception as e:
            logger.error(f"Failed to create prediction visualizations: {e}")
    
    def _create_mock_targets(self, predictions: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        """Create realistic mock targets for demo purposes."""
        mock_targets = {}
        
        for task_name, pred in predictions.items():
            if task_name == 'regime_change':
                # Random regime targets
                mock_targets[task_name] = torch.randint(0, 4, pred.shape[:-1], device=pred.device)
            else:
                # Correlated with predictions but with noise
                noise = torch.randn_like(pred) * 0.1
                mock_targets[task_name] = pred.detach() + noise
        
        return mock_targets
    
    def run_complete_demo(self):
        """Run the complete demo pipeline."""
        logger.info("🚀 Starting Autonomous Driving Inspired Financial Transformer Demo")
        logger.info("=" * 70)
        
        try:
            # 1. Setup data
            logger.info("\n1️⃣ Setting up data loaders...")
            if not self.setup_data():
                logger.error("❌ Data setup failed!")
                return False
            
            # 2. Setup model
            logger.info("\n2️⃣ Setting up transformer model...")
            self.setup_model()
            
            # 3. Test forward pass
            logger.info("\n3️⃣ Testing forward pass...")
            test_outputs = self.test_forward_pass()
            
            # 4. Train model
            logger.info("\n4️⃣ Training model...")
            self.train_model()
            
            # 5. Evaluate model
            logger.info("\n5️⃣ Evaluating model...")
            evaluation_results = self.evaluate_model()
            
            # 6. Create visualizations
            logger.info("\n6️⃣ Creating visualizations...")
            vis_path = f"/tmp/autonomous_finance_demo_{self.symbol}"
            
            self.visualize_attention(
                evaluation_results['attention_weights'], 
                vis_path
            )
            
            self.visualize_predictions(
                evaluation_results['predictions'],
                evaluation_results['targets'],
                vis_path
            )
            
            logger.info("=" * 70)
            logger.info("🎉 Demo completed successfully!")
            logger.info(f"📁 Results saved to: {vis_path}")
            logger.info(f"📊 TensorBoard logs: {self.training_config.tensorboard_log_dir}")
            logger.info(f"💾 Model checkpoints: {self.training_config.checkpoint_dir}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Demo failed: {e}")
            raise


def main():
    parser = argparse.ArgumentParser(
        description="Demo of Autonomous Driving Inspired Financial Transformer"
    )
    parser.add_argument(
        "--data_path", 
        type=str, 
        default="/mnt/d/ats-data/training_data/83",
        help="Path to training data directory"
    )
    parser.add_argument(
        "--symbol", 
        type=str, 
        default="AAPL",
        help="Stock symbol to train on"
    )
    parser.add_argument(
        "--device",
        type=str,
        default="auto",
        help="Device to use (auto, cpu, cuda)"
    )
    parser.add_argument(
        "--quick_test",
        action="store_true",
        help="Run quick test without full training"
    )
    
    args = parser.parse_args()
    
    # Create demo instance
    demo = AutonomousFinanceDemo(
        data_path=Path(args.data_path),
        symbol=args.symbol,
        device=args.device
    )
    
    if args.quick_test:
        # Quick test - just forward pass
        logger.info("Running quick test (forward pass only)...")
        demo.setup_data()
        demo.setup_model()
        demo.test_forward_pass()
        logger.info("✅ Quick test completed!")
    else:
        # Full demo
        demo.run_complete_demo()


if __name__ == "__main__":
    main()