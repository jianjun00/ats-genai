"""
Support/Resistance Prediction Models

Multi-output neural networks for predicting next-day support and resistance levels
with confidence scores and uncertainty quantification.
"""

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler, RobustScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
import logging
from typing import Dict, List
from dataclasses import dataclass
import gin

@dataclass
class SRModelConfig:
    """Configuration for support/resistance models"""
    # Model architecture
    input_dim: int = 50
    hidden_dims: List[int] = None
    dropout_rate: float = 0.3
    activation: str = 'relu'

    # Output configuration
    max_support_levels: int = 3
    max_resistance_levels: int = 3
    predict_confidence: bool = True

    # Training configuration
    batch_size: int = 64
    learning_rate: float = 0.001
    epochs: int = 100
    weight_decay: float = 1e-5
    patience: int = 10

    # Loss function weights
    level_weight: float = 1.0
    confidence_weight: float = 0.5
    ranking_weight: float = 0.3

    def __post_init__(self):
        if self.hidden_dims is None:
            self.hidden_dims = [256, 128, 64]

class SRDataset(Dataset):
    """Dataset for support/resistance prediction"""

    def __init__(self, features: np.ndarray, support_levels: np.ndarray,
                 resistance_levels: np.ndarray, support_confidence: np.ndarray,
                 resistance_confidence: np.ndarray):
        self.features = torch.FloatTensor(features)
        self.support_levels = torch.FloatTensor(support_levels)
        self.resistance_levels = torch.FloatTensor(resistance_levels)
        self.support_confidence = torch.FloatTensor(support_confidence)
        self.resistance_confidence = torch.FloatTensor(resistance_confidence)

    def __len__(self):
        return len(self.features)

    def __getitem__(self, idx):
        return {
            'features': self.features[idx],
            'support_levels': self.support_levels[idx],
            'resistance_levels': self.resistance_levels[idx],
            'support_confidence': self.support_confidence[idx],
            'resistance_confidence': self.resistance_confidence[idx]
        }

@gin.configurable
class SupportResistanceNet(nn.Module):
    """
    Multi-output neural network for predicting support and resistance levels.

    Predicts:
    - Support levels (up to 3) with confidence scores
    - Resistance levels (up to 3) with confidence scores
    - Level rankings (most important to least important)
    """

    def __init__(self, config: SRModelConfig):
        super(SupportResistanceNet, self).__init__()
        self.config = config

        # Shared feature extraction layers
        layers = []
        prev_dim = config.input_dim

        for hidden_dim in config.hidden_dims:
            layers.extend([
                nn.Linear(prev_dim, hidden_dim),
                nn.BatchNorm1d(hidden_dim),
                self._get_activation(config.activation),
                nn.Dropout(config.dropout_rate)
            ])
            prev_dim = hidden_dim

        self.feature_extractor = nn.Sequential(*layers)

        # Support prediction heads
        self.support_levels_head = nn.Linear(prev_dim, config.max_support_levels)
        self.support_confidence_head = nn.Linear(prev_dim, config.max_support_levels)

        # Resistance prediction heads
        self.resistance_levels_head = nn.Linear(prev_dim, config.max_resistance_levels)
        self.resistance_confidence_head = nn.Linear(prev_dim, config.max_resistance_levels)

        # Attention mechanism for level importance
        self.support_attention = nn.Linear(prev_dim, config.max_support_levels)
        self.resistance_attention = nn.Linear(prev_dim, config.max_resistance_levels)

        # Price range prediction for normalization
        self.price_range_head = nn.Linear(prev_dim, 2)  # [low, high] estimates

    def _get_activation(self, activation: str):
        """Get activation function by name"""
        activations = {
            'relu': nn.ReLU(),
            'leaky_relu': nn.LeakyReLU(0.1),
            'elu': nn.ELU(),
            'swish': nn.SiLU(),
            'gelu': nn.GELU()
        }
        return activations.get(activation, nn.ReLU())

    def forward(self, features):
        # Extract shared features
        x = self.feature_extractor(features)

        # Predict support levels and confidence
        support_levels_raw = self.support_levels_head(x)
        support_confidence_raw = self.support_confidence_head(x)
        support_attention_weights = F.softmax(self.support_attention(x), dim=1)

        # Predict resistance levels and confidence
        resistance_levels_raw = self.resistance_levels_head(x)
        resistance_confidence_raw = self.resistance_confidence_head(x)
        resistance_attention_weights = F.softmax(self.resistance_attention(x), dim=1)

        # Predict price range for normalization
        price_range = self.price_range_head(x)
        predicted_low = price_range[:, 0:1]
        predicted_high = price_range[:, 1:2]

        # Normalize levels to be within reasonable price range
        # Support levels should be below current price
        support_levels = predicted_low.expand(-1, self.config.max_support_levels) * (1 - F.sigmoid(support_levels_raw) * 0.1)

        # Resistance levels should be above current price
        resistance_levels = predicted_high.expand(-1, self.config.max_resistance_levels) * (1 + F.sigmoid(resistance_levels_raw) * 0.1)

        # Apply confidence scores (sigmoid for 0-1 range)
        support_confidence = F.sigmoid(support_confidence_raw)
        resistance_confidence = F.sigmoid(resistance_confidence_raw)

        return {
            'support_levels': support_levels,
            'support_confidence': support_confidence,
            'support_attention': support_attention_weights,
            'resistance_levels': resistance_levels,
            'resistance_confidence': resistance_confidence,
            'resistance_attention': resistance_attention_weights,
            'predicted_low': predicted_low,
            'predicted_high': predicted_high
        }

class SRLoss(nn.Module):
    """Custom loss function for support/resistance prediction"""

    def __init__(self, config: SRModelConfig):
        super(SRLoss, self).__init__()
        self.config = config

    def forward(self, predictions, targets):
        """
        Compute multi-component loss for support/resistance prediction

        Args:
            predictions: Model output dict
            targets: Target dict with levels and confidence
        """
        total_loss = 0.0
        loss_components = {}

        # Support level loss (Huber loss for robustness)
        support_level_loss = F.huber_loss(
            predictions['support_levels'],
            targets['support_levels'],
            reduction='mean'
        )
        loss_components['support_levels'] = support_level_loss
        total_loss += self.config.level_weight * support_level_loss

        # Resistance level loss
        resistance_level_loss = F.huber_loss(
            predictions['resistance_levels'],
            targets['resistance_levels'],
            reduction='mean'
        )
        loss_components['resistance_levels'] = resistance_level_loss
        total_loss += self.config.level_weight * resistance_level_loss

        # Confidence loss (Binary cross entropy)
        support_conf_loss = F.binary_cross_entropy(
            predictions['support_confidence'],
            targets['support_confidence'],
            reduction='mean'
        )
        loss_components['support_confidence'] = support_conf_loss
        total_loss += self.config.confidence_weight * support_conf_loss

        resistance_conf_loss = F.binary_cross_entropy(
            predictions['resistance_confidence'],
            targets['resistance_confidence'],
            reduction='mean'
        )
        loss_components['resistance_confidence'] = resistance_conf_loss
        total_loss += self.config.confidence_weight * resistance_conf_loss

        # Ranking loss (encourage proper ordering of levels)
        support_ranking_loss = self._ranking_loss(
            predictions['support_levels'], predictions['support_confidence']
        )
        loss_components['support_ranking'] = support_ranking_loss
        total_loss += self.config.ranking_weight * support_ranking_loss

        resistance_ranking_loss = self._ranking_loss(
            predictions['resistance_levels'], predictions['resistance_confidence']
        )
        loss_components['resistance_ranking'] = resistance_ranking_loss
        total_loss += self.config.ranking_weight * resistance_ranking_loss

        # Price range consistency loss
        range_loss = F.mse_loss(
            predictions['predicted_high'] - predictions['predicted_low'],
            targets.get('actual_range', torch.zeros_like(predictions['predicted_high'])),
            reduction='mean'
        )
        loss_components['range_consistency'] = range_loss
        total_loss += 0.1 * range_loss

        return total_loss, loss_components

    def _ranking_loss(self, levels, confidence):
        """Encourage levels with higher confidence to be more accurately predicted"""
        # Sort by confidence and apply ranking penalty
        batch_size = levels.size(0)
        ranking_loss = 0.0

        for i in range(batch_size):
            level_conf = confidence[i]
            sorted_indices = torch.argsort(level_conf, descending=True)

            # Higher confidence levels should have smaller prediction errors
            for j in range(len(sorted_indices) - 1):
                high_conf_idx = sorted_indices[j]
                low_conf_idx = sorted_indices[j + 1]

                conf_diff = level_conf[high_conf_idx] - level_conf[low_conf_idx]
                ranking_loss += F.relu(conf_diff * 0.1)  # Small penalty

        return ranking_loss / batch_size

@gin.configurable
class SupportResistanceEnsemble:
    """
    Ensemble model combining neural network with tree-based models
    for robust support/resistance prediction.
    """

    def __init__(self, config: SRModelConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Neural network model
        self.neural_net = SupportResistanceNet(config)
        self.criterion = SRLoss(config)
        self.optimizer = None
        self.scheduler = None

        # Tree-based models for ensemble
        self.support_rf = RandomForestRegressor(
            n_estimators=100, max_depth=10, random_state=42
        )
        self.resistance_rf = RandomForestRegressor(
            n_estimators=100, max_depth=10, random_state=42
        )

        # Scalers
        self.feature_scaler = RobustScaler()
        self.level_scaler = StandardScaler()

        # Training history
        self.training_history = {
            'train_loss': [],
            'val_loss': [],
            'train_mae': [],
            'val_mae': []
        }

    def prepare_data(self, training_examples) -> Dict[str, np.ndarray]:
        """Prepare training data from examples"""
        self.logger.info("Preparing training data...")

        features = []
        support_levels = []
        resistance_levels = []
        support_confidence = []
        resistance_confidence = []

        for example in training_examples:
            # Extract features
            feature_vector = [
                example.features.get(key, 0.0)
                for key in sorted(example.features.keys())
            ]
            features.append(feature_vector)

            # Extract support levels and confidence
            sup_levels = [0.0] * self.config.max_support_levels
            sup_conf = [0.0] * self.config.max_support_levels

            for i, level in enumerate(example.next_day_support_levels[:self.config.max_support_levels]):
                sup_levels[i] = level.level
                sup_conf[i] = level.strength

            support_levels.append(sup_levels)
            support_confidence.append(sup_conf)

            # Extract resistance levels and confidence
            res_levels = [0.0] * self.config.max_resistance_levels
            res_conf = [0.0] * self.config.max_resistance_levels

            for i, level in enumerate(example.next_day_resistance_levels[:self.config.max_resistance_levels]):
                res_levels[i] = level.level
                res_conf[i] = level.strength

            resistance_levels.append(res_levels)
            resistance_confidence.append(res_conf)

        # Convert to numpy arrays
        data = {
            'features': np.array(features),
            'support_levels': np.array(support_levels),
            'resistance_levels': np.array(resistance_levels),
            'support_confidence': np.array(support_confidence),
            'resistance_confidence': np.array(resistance_confidence)
        }

        self.logger.info(f"Prepared {len(features)} examples with {len(features[0])} features")
        return data

    def train(self, training_examples, validation_examples=None):
        """Train the ensemble model"""
        self.logger.info("Starting ensemble model training...")

        # Prepare data
        train_data = self.prepare_data(training_examples)
        val_data = None
        if validation_examples:
            val_data = self.prepare_data(validation_examples)

        # Scale features
        train_features_scaled = self.feature_scaler.fit_transform(train_data['features'])
        val_features_scaled = None
        if val_data is not None:
            val_features_scaled = self.feature_scaler.transform(val_data['features'])

        # Train neural network
        self._train_neural_network(train_data, val_data, train_features_scaled, val_features_scaled)

        # Train random forest models
        self._train_random_forests(train_features_scaled, train_data)

        self.logger.info("Ensemble training completed")

    def _train_neural_network(self, train_data, val_data, train_features_scaled, val_features_scaled):
        """Train the neural network component"""
        self.logger.info("Training neural network...")

        # Update config with actual input dimension
        self.config.input_dim = train_features_scaled.shape[1]
        self.neural_net = SupportResistanceNet(self.config)

        # Setup optimizer
        self.optimizer = torch.optim.AdamW(
            self.neural_net.parameters(),
            lr=self.config.learning_rate,
            weight_decay=self.config.weight_decay
        )

        self.scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            self.optimizer, mode='min', patience=self.config.patience//2, factor=0.5
        )

        # Create datasets
        train_dataset = SRDataset(
            train_features_scaled,
            train_data['support_levels'],
            train_data['resistance_levels'],
            train_data['support_confidence'],
            train_data['resistance_confidence']
        )

        train_loader = DataLoader(
            train_dataset, batch_size=self.config.batch_size, shuffle=True
        )

        val_loader = None
        if val_data is not None:
            val_dataset = SRDataset(
                val_features_scaled,
                val_data['support_levels'],
                val_data['resistance_levels'],
                val_data['support_confidence'],
                val_data['resistance_confidence']
            )
            val_loader = DataLoader(
                val_dataset, batch_size=self.config.batch_size, shuffle=False
            )

        # Training loop
        best_val_loss = float('inf')
        patience_counter = 0

        for epoch in range(self.config.epochs):
            # Training phase
            self.neural_net.train()
            train_loss = 0.0
            train_mae = 0.0

            for batch in train_loader:
                self.optimizer.zero_grad()

                predictions = self.neural_net(batch['features'])

                targets = {
                    'support_levels': batch['support_levels'],
                    'resistance_levels': batch['resistance_levels'],
                    'support_confidence': batch['support_confidence'],
                    'resistance_confidence': batch['resistance_confidence']
                }

                loss, loss_components = self.criterion(predictions, targets)
                loss.backward()

                # Gradient clipping
                torch.nn.utils.clip_grad_norm_(self.neural_net.parameters(), max_norm=1.0)

                self.optimizer.step()

                train_loss += loss.item()

                # Calculate MAE for monitoring
                with torch.no_grad():
                    support_mae = F.l1_loss(predictions['support_levels'], targets['support_levels'])
                    resistance_mae = F.l1_loss(predictions['resistance_levels'], targets['resistance_levels'])
                    train_mae += (support_mae + resistance_mae).item() / 2

            train_loss /= len(train_loader)
            train_mae /= len(train_loader)

            # Validation phase
            val_loss = 0.0
            val_mae = 0.0

            if val_loader is not None:
                self.neural_net.eval()
                with torch.no_grad():
                    for batch in val_loader:
                        predictions = self.neural_net(batch['features'])

                        targets = {
                            'support_levels': batch['support_levels'],
                            'resistance_levels': batch['resistance_levels'],
                            'support_confidence': batch['support_confidence'],
                            'resistance_confidence': batch['resistance_confidence']
                        }

                        loss, _ = self.criterion(predictions, targets)
                        val_loss += loss.item()

                        support_mae = F.l1_loss(predictions['support_levels'], targets['support_levels'])
                        resistance_mae = F.l1_loss(predictions['resistance_levels'], targets['resistance_levels'])
                        val_mae += (support_mae + resistance_mae).item() / 2

                val_loss /= len(val_loader)
                val_mae /= len(val_loader)

                # Learning rate scheduling
                self.scheduler.step(val_loss)

                # Early stopping
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    patience_counter = 0
                    # Save best model
                    torch.save(self.neural_net.state_dict(), 'best_sr_model.pt')
                else:
                    patience_counter += 1
                    if patience_counter >= self.config.patience:
                        self.logger.info(f"Early stopping at epoch {epoch}")
                        break

            # Log progress
            if epoch % 10 == 0:
                log_msg = f"Epoch {epoch}: train_loss={train_loss:.4f}, train_mae={train_mae:.4f}"
                if val_loader is not None:
                    log_msg += f", val_loss={val_loss:.4f}, val_mae={val_mae:.4f}"
                self.logger.info(log_msg)

            # Store history
            self.training_history['train_loss'].append(train_loss)
            self.training_history['train_mae'].append(train_mae)
            if val_loader is not None:
                self.training_history['val_loss'].append(val_loss)
                self.training_history['val_mae'].append(val_mae)

        # Load best model
        if val_loader is not None:
            self.neural_net.load_state_dict(torch.load('best_sr_model.pt'))

    def _train_random_forests(self, features, data):
        """Train random forest components"""
        self.logger.info("Training random forest models...")

        # Flatten multi-output targets for RF
        support_flat = data['support_levels'].reshape(-1, self.config.max_support_levels)
        resistance_flat = data['resistance_levels'].reshape(-1, self.config.max_resistance_levels)

        # Train support RF
        support_targets = []
        for i in range(len(support_flat)):
            # Use confidence-weighted average as single target
            levels = support_flat[i]
            conf = data['support_confidence'][i]
            weighted_avg = np.average(levels[levels > 0], weights=conf[levels > 0]) if np.any(levels > 0) else 0
            support_targets.append(weighted_avg)

        self.support_rf.fit(features, support_targets)

        # Train resistance RF
        resistance_targets = []
        for i in range(len(resistance_flat)):
            levels = resistance_flat[i]
            conf = data['resistance_confidence'][i]
            weighted_avg = np.average(levels[levels > 0], weights=conf[levels > 0]) if np.any(levels > 0) else 0
            resistance_targets.append(weighted_avg)

        self.resistance_rf.fit(features, resistance_targets)

    def predict(self, features: np.ndarray) -> Dict[str, np.ndarray]:
        """Make predictions using the ensemble"""
        features_scaled = self.feature_scaler.transform(features)

        # Neural network predictions
        self.neural_net.eval()
        with torch.no_grad():
            features_tensor = torch.FloatTensor(features_scaled)
            nn_predictions = self.neural_net(features_tensor)

            # Convert to numpy
            nn_support = nn_predictions['support_levels'].numpy()
            nn_resistance = nn_predictions['resistance_levels'].numpy()
            nn_support_conf = nn_predictions['support_confidence'].numpy()
            nn_resistance_conf = nn_predictions['resistance_confidence'].numpy()

        # Random forest predictions
        rf_support = self.support_rf.predict(features_scaled)
        rf_resistance = self.resistance_rf.predict(features_scaled)

        # Ensemble predictions (weighted average)
        ensemble_support = 0.7 * nn_support[:, 0] + 0.3 * rf_support
        ensemble_resistance = 0.7 * nn_resistance[:, 0] + 0.3 * rf_resistance

        return {
            'support_levels': nn_support,
            'resistance_levels': nn_resistance,
            'support_confidence': nn_support_conf,
            'resistance_confidence': nn_resistance_conf,
            'ensemble_support': ensemble_support,
            'ensemble_resistance': ensemble_resistance,
            'rf_support': rf_support,
            'rf_resistance': rf_resistance
        }

    def evaluate(self, test_examples) -> Dict[str, float]:
        """Evaluate model performance on test data"""
        test_data = self.prepare_data(test_examples)
        predictions = self.predict(test_data['features'])

        # Calculate metrics
        metrics = {}

        # Support level metrics
        support_mae = mean_absolute_error(
            test_data['support_levels'][:, 0],  # Primary support level
            predictions['ensemble_support']
        )
        metrics['support_mae'] = support_mae

        # Resistance level metrics
        resistance_mae = mean_absolute_error(
            test_data['resistance_levels'][:, 0],  # Primary resistance level
            predictions['ensemble_resistance']
        )
        metrics['resistance_mae'] = resistance_mae

        # Overall MAE
        metrics['overall_mae'] = (support_mae + resistance_mae) / 2

        # Confidence correlation
        support_conf_corr = np.corrcoef(
            test_data['support_confidence'][:, 0],
            predictions['support_confidence'][:, 0]
        )[0, 1]
        metrics['support_confidence_corr'] = support_conf_corr if not np.isnan(support_conf_corr) else 0.0

        return metrics

    def save_model(self, filepath: str):
        """Save the trained ensemble model"""
        import pickle

        model_data = {
            'config': self.config,
            'neural_net_state': self.neural_net.state_dict(),
            'support_rf': self.support_rf,
            'resistance_rf': self.resistance_rf,
            'feature_scaler': self.feature_scaler,
            'level_scaler': self.level_scaler,
            'training_history': self.training_history
        }

        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)

        self.logger.info(f"Model saved to {filepath}")

    def load_model(self, filepath: str):
        """Load a trained ensemble model"""
        import pickle

        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)

        self.config = model_data['config']
        self.neural_net = SupportResistanceNet(self.config)
        self.neural_net.load_state_dict(model_data['neural_net_state'])
        self.support_rf = model_data['support_rf']
        self.resistance_rf = model_data['resistance_rf']
        self.feature_scaler = model_data['feature_scaler']
        self.level_scaler = model_data['level_scaler']
        self.training_history = model_data['training_history']

        self.logger.info(f"Model loaded from {filepath}")


async def main():
    """Example usage of the support/resistance model"""

    # Use relative import
    from ..training_data.support_resistance_generator import SupportResistanceTrainingGenerator

    logging.basicConfig(level=logging.INFO)

    # Generate sample training data
    print("Generating sample training data...")
    SupportResistanceTrainingGenerator()

    # This would normally load from your unbiased training data
    # training_examples = generator.generate_training_data(...)

    # For demo, create minimal config
    config = SRModelConfig(
        input_dim=50,
        hidden_dims=[128, 64],
        max_support_levels=2,
        max_resistance_levels=2,
        epochs=10,
        batch_size=32
    )

    # Create and demonstrate model
    SupportResistanceEnsemble(config)

    print("Support/Resistance model ready for training!")
    print(f"Model configuration: {config}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())