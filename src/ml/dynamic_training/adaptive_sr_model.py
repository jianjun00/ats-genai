"""
Adaptive Support/Resistance Model with Dynamic Daily Retraining

This module implements a production-ready ML model that:
1. Bootstraps on 2-4 years of historical data
2. Retrains/recalibrates daily during backtesting
3. Uses rolling window training to adapt to market regime changes
4. Implements incremental learning for efficiency
"""

import os
import pickle
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from pathlib import Path
import logging

from ml.models.support_resistance_model import SupportResistanceEnsemble, SRModelConfig
from ml.training_data.support_resistance_generator import (
    SupportResistanceTrainingGenerator, 
    TrainingExample
)

@dataclass
class AdaptiveModelConfig:
    """Configuration for adaptive model training"""
    # Bootstrap configuration
    bootstrap_years: int = 3  # Initial training period
    min_bootstrap_examples: int = 5000  # Minimum examples to start
    
    # Rolling window configuration
    rolling_window_days: int = 365  # 1 year rolling window
    min_retrain_examples: int = 100  # Minimum new examples to trigger retrain
    retrain_frequency_days: int = 1  # Retrain every N days
    
    # Incremental learning
    learning_rate_decay: float = 0.95  # Decay learning rate over time
    model_memory_weight: float = 0.8  # Weight for existing model vs new data
    
    # Performance monitoring
    performance_lookback_days: int = 30  # Days to monitor performance
    min_accuracy_threshold: float = 0.4  # Minimum accuracy before full retrain
    
    # Model architecture (lightweight for daily retraining)
    base_model_config: SRModelConfig = None
    
    def __post_init__(self):
        if self.base_model_config is None:
            self.base_model_config = SRModelConfig(
                input_dim=50,
                hidden_dims=[128, 64, 32],  # Smaller for faster training
                max_support_levels=3,
                max_resistance_levels=3,
                epochs=20,  # Fewer epochs for daily updates
                batch_size=32,
                learning_rate=0.001,
                patience=5
            )

class AdaptiveModelState:
    """Tracks the state of the adaptive model"""
    def __init__(self):
        self.last_retrain_date: Optional[date] = None
        self.total_training_examples: int = 0
        self.recent_performance: List[float] = []
        self.model_version: int = 0
        self.training_history: List[Dict] = []
        self.bootstrap_completed: bool = False

class AdaptiveSupportResistanceModel:
    """
    Adaptive S/R model that retrains daily during backtesting
    
    This simulates realistic production conditions where the model
    learns from new data daily and adapts to changing market conditions.
    """
    
    def __init__(self, config: AdaptiveModelConfig):
        self.config = config
        self.state = AdaptiveModelState()
        self.model: Optional[SupportResistanceEnsemble] = None
        self.training_generator = SupportResistanceTrainingGenerator()
        self.logger = logging.getLogger(__name__)
        
        # Cache for training data to avoid regenerating
        self.training_data_cache: Dict[str, List[TrainingExample]] = {}
        
    async def bootstrap_model(
        self, 
        symbols: List[str], 
        end_date: date,
        save_path: Optional[str] = None
    ) -> bool:
        """
        Bootstrap the initial model on 2-4 years of historical data
        
        Args:
            symbols: List of symbols to train on
            end_date: End date for bootstrap training (e.g., start of backtest)
            save_path: Optional path to save the bootstrapped model
            
        Returns:
            True if bootstrap successful, False otherwise
        """
        self.logger.info(f"Bootstrapping model with {self.config.bootstrap_years} years of data")
        
        # Calculate bootstrap period
        start_date = date(
            end_date.year - self.config.bootstrap_years, 
            end_date.month, 
            end_date.day
        )
        
        self.logger.info(f"Bootstrap period: {start_date} to {end_date}")
        
        # Generate initial training data
        bootstrap_examples = await self.training_generator.generate_training_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            min_examples_per_symbol=50  # Ensure substantial data per symbol
        )
        
        if len(bootstrap_examples) < self.config.min_bootstrap_examples:
            self.logger.error(
                f"Insufficient bootstrap examples: {len(bootstrap_examples)} < "
                f"{self.config.min_bootstrap_examples}"
            )
            return False
        
        self.logger.info(f"Generated {len(bootstrap_examples)} bootstrap examples")
        
        # Cache the bootstrap data
        cache_key = f"bootstrap_{start_date}_{end_date}"
        self.training_data_cache[cache_key] = bootstrap_examples
        
        # Train initial model
        self.model = SupportResistanceEnsemble(self.config.base_model_config)
        self.model.train(bootstrap_examples)
        
        # Update state
        self.state.bootstrap_completed = True
        self.state.last_retrain_date = end_date
        self.state.total_training_examples = len(bootstrap_examples)
        self.state.model_version = 1
        self.state.training_history.append({
            'date': end_date,
            'type': 'bootstrap',
            'examples': len(bootstrap_examples),
            'version': self.state.model_version
        })
        
        if save_path:
            self.save_model(save_path)
            
        self.logger.info(f"Bootstrap completed: model v{self.state.model_version}")
        return True
    
    async def daily_update(
        self, 
        current_date: date, 
        symbols: List[str],
        force_retrain: bool = False
    ) -> bool:
        """
        Perform daily model update/retraining
        
        Args:
            current_date: Current date in backtest
            symbols: Symbols to generate new data for
            force_retrain: Force full retrain regardless of other conditions
            
        Returns:
            True if model was updated, False otherwise
        """
        if not self.state.bootstrap_completed:
            self.logger.error("Cannot update model before bootstrap")
            return False
        
        # Check if we should retrain today
        days_since_retrain = (current_date - self.state.last_retrain_date).days
        should_retrain = (
            force_retrain or 
            days_since_retrain >= self.config.retrain_frequency_days or
            self._should_retrain_for_performance()
        )
        
        if not should_retrain:
            return False
        
        self.logger.info(f"Updating model for date: {current_date}")
        
        # Generate new training data for rolling window
        window_start = current_date - timedelta(days=self.config.rolling_window_days)
        
        new_examples = await self._get_training_data_for_period(
            symbols=symbols,
            start_date=window_start,
            end_date=current_date
        )
        
        if len(new_examples) < self.config.min_retrain_examples:
            self.logger.warning(
                f"Insufficient new examples for retraining: {len(new_examples)} < "
                f"{self.config.min_retrain_examples}"
            )
            return False
        
        # Determine update strategy
        if self._should_full_retrain():
            success = await self._full_retrain(new_examples, current_date)
        else:
            success = await self._incremental_update(new_examples, current_date)
        
        if success:
            self.state.last_retrain_date = current_date
            self.state.model_version += 1
            self.state.training_history.append({
                'date': current_date,
                'type': 'update',
                'examples': len(new_examples),
                'version': self.state.model_version
            })
            
        return success
    
    async def _get_training_data_for_period(
        self, 
        symbols: List[str], 
        start_date: date, 
        end_date: date
    ) -> List[TrainingExample]:
        """Get training data for a specific period, using cache when possible"""
        
        cache_key = f"{start_date}_{end_date}_{'_'.join(sorted(symbols))}"
        
        if cache_key in self.training_data_cache:
            self.logger.debug(f"Using cached training data for {cache_key}")
            return self.training_data_cache[cache_key]
        
        self.logger.debug(f"Generating new training data for {start_date} to {end_date}")
        
        examples = await self.training_generator.generate_training_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            min_examples_per_symbol=10  # Lower threshold for rolling updates
        )
        
        # Cache the data (but limit cache size)
        if len(self.training_data_cache) > 20:  # Limit cache size
            # Remove oldest entry
            oldest_key = min(self.training_data_cache.keys())
            del self.training_data_cache[oldest_key]
        
        self.training_data_cache[cache_key] = examples
        return examples
    
    async def _full_retrain(self, examples: List[TrainingExample], current_date: date) -> bool:
        """Perform full model retraining"""
        self.logger.info(f"Performing full retrain on {len(examples)} examples")
        
        try:
            # Create new model instance
            new_model = SupportResistanceEnsemble(self.config.base_model_config)
            new_model.train(examples)
            
            # Replace the old model
            self.model = new_model
            self.state.total_training_examples = len(examples)
            
            self.logger.info("Full retrain completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Full retrain failed: {e}")
            return False
    
    async def _incremental_update(
        self, 
        new_examples: List[TrainingExample], 
        current_date: date
    ) -> bool:
        """Perform incremental model update"""
        self.logger.info(f"Performing incremental update with {len(new_examples)} examples")
        
        try:
            # For now, implement as reduced-epoch retrain
            # TODO: Implement true incremental learning
            
            # Reduce learning rate for incremental updates
            original_lr = self.config.base_model_config.learning_rate
            self.config.base_model_config.learning_rate *= self.config.learning_rate_decay
            
            # Reduce epochs for faster updates
            original_epochs = self.config.base_model_config.epochs
            self.config.base_model_config.epochs = max(5, original_epochs // 4)
            
            # Retrain with reduced parameters
            self.model.train(new_examples)
            
            # Restore original parameters
            self.config.base_model_config.learning_rate = original_lr
            self.config.base_model_config.epochs = original_epochs
            
            self.state.total_training_examples += len(new_examples)
            
            self.logger.info("Incremental update completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Incremental update failed: {e}")
            return False
    
    def _should_retrain_for_performance(self) -> bool:
        """Check if model performance suggests retraining is needed"""
        if len(self.state.recent_performance) < 5:
            return False  # Not enough performance data
        
        recent_avg = np.mean(self.state.recent_performance[-5:])
        return recent_avg < self.config.min_accuracy_threshold
    
    def _should_full_retrain(self) -> bool:
        """Determine if full retrain is needed vs incremental update"""
        # Full retrain if performance is very poor
        if len(self.state.recent_performance) >= 3:
            recent_avg = np.mean(self.state.recent_performance[-3:])
            if recent_avg < self.config.min_accuracy_threshold * 0.8:
                return True
        
        # Full retrain every month
        if len(self.state.training_history) > 0:
            last_full_retrain = None
            for entry in reversed(self.state.training_history):
                if entry['type'] in ['bootstrap', 'full_retrain']:
                    last_full_retrain = entry['date']
                    break
            
            if last_full_retrain and isinstance(last_full_retrain, date):
                days_since_full = (date.today() - last_full_retrain).days
                if days_since_full > 30:  # Monthly full retrain
                    return True
        
        return False
    
    def predict(self, features: np.ndarray) -> Dict[str, np.ndarray]:
        """Make predictions using the current model"""
        if not self.model:
            raise ValueError("Model not trained. Call bootstrap_model first.")
        
        return self.model.predict(features)
    
    def evaluate_daily_performance(
        self, 
        test_examples: List[TrainingExample]
    ) -> Dict[str, float]:
        """Evaluate model performance and update tracking"""
        if not self.model:
            raise ValueError("Model not trained")
        
        metrics = self.model.evaluate(test_examples)
        
        # Track overall accuracy for performance monitoring
        overall_accuracy = 1.0 - metrics.get('overall_mae', 1.0)  # Convert MAE to accuracy proxy
        self.state.recent_performance.append(overall_accuracy)
        
        # Keep only recent performance data
        if len(self.state.recent_performance) > self.config.performance_lookback_days:
            self.state.recent_performance = self.state.recent_performance[-self.config.performance_lookback_days:]
        
        return metrics
    
    def save_model(self, file_path: str) -> None:
        """Save the complete adaptive model state"""
        model_data = {
            'model': self.model,
            'config': self.config,
            'state': self.state,
            'cache_keys': list(self.training_data_cache.keys())  # Don't save cache data
        }
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        self.logger.info(f"Adaptive model saved to {file_path}")
    
    def load_model(self, file_path: str) -> None:
        """Load the complete adaptive model state"""
        with open(file_path, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.config = model_data['config']
        self.state = model_data['state']
        # Note: training cache is not restored
        
        self.logger.info(f"Adaptive model loaded from {file_path}")
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about current model state"""
        return {
            'bootstrap_completed': self.state.bootstrap_completed,
            'model_version': self.state.model_version,
            'last_retrain_date': self.state.last_retrain_date,
            'total_training_examples': self.state.total_training_examples,
            'recent_performance': self.state.recent_performance[-5:] if self.state.recent_performance else [],
            'training_history_count': len(self.state.training_history),
            'cache_size': len(self.training_data_cache)
        }