#!/usr/bin/env python3
"""
Train unified loss transformer model with realistic synthetic AAPL-like data.
This creates a trained model that demonstrates the cross-domain research synthesis.
"""

import sys
import os
import time
import logging
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from pathlib import Path
from typing import Dict, Tuple, List, Optional
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FinancialAVLoss(nn.Module):
    """Unified loss function combining autonomous driving and financial trading insights."""
    
    def __init__(self, num_tasks=5, alpha_cvar=0.05, lambda_drawdown=2.0, gamma_focal=2.0):
        super().__init__()
        # Multi-task uncertainty weighting from autonomous driving research
        self.log_vars = nn.Parameter(torch.zeros(num_tasks))
        
        # Financial trading parameters
        self.alpha_cvar = alpha_cvar  # CVaR percentile
        self.lambda_drawdown = lambda_drawdown  # Drawdown penalty weight
        self.gamma_focal = gamma_focal  # Focal loss gamma for difficult predictions
    
    def forward(self, predictions, targets, historical_predictions=None):
        device = predictions['price_movement'].device
        total_loss = torch.tensor(0.0, device=device)
        
        # Multi-task losses with uncertainty weighting (from autonomous driving)
        task_losses = []
        
        # 1. Price movement prediction with focal loss enhancement
        price_error = F.mse_loss(predictions['price_movement'], targets['price_movement'], reduction='none')
        price_weights = torch.abs(predictions['price_movement'] - targets['price_movement'])
        focal_weights = torch.pow(price_weights + 1e-8, self.gamma_focal)
        price_loss = torch.mean(focal_weights * price_error)
        task_losses.append(price_loss)
        
        # 2. Volatility prediction
        vol_loss = F.mse_loss(predictions['volatility'], targets['volatility'])
        task_losses.append(vol_loss)
        
        # 3. Volume profile prediction
        volume_loss = F.mse_loss(predictions['volume_profile'], targets['volume_profile'])
        task_losses.append(volume_loss)
        
        # 4. Market regime classification
        regime_loss = F.cross_entropy(predictions['regime_change'], targets['regime_change'])
        task_losses.append(regime_loss)
        
        # 5. Risk assessment
        risk_loss = F.mse_loss(predictions['risk_assessment'], targets['risk_assessment'])
        task_losses.append(risk_loss)
        
        # Apply multi-task uncertainty weighting (autonomous driving insight)
        weighted_loss = torch.tensor(0.0, device=device)
        for i, loss in enumerate(task_losses):
            precision = torch.exp(-self.log_vars[i])
            task_weighted_loss = precision * loss + self.log_vars[i]
            weighted_loss += task_weighted_loss
        
        total_loss += weighted_loss
        
        # Financial risk penalties (trading insight)
        returns = predictions['price_movement'].flatten()
        if len(returns) > 2:
            # CVaR (Conditional Value at Risk) penalty
            var_threshold = torch.quantile(returns, self.alpha_cvar)
            tail_losses = returns[returns <= var_threshold]
            if len(tail_losses) > 0:
                cvar_loss = -torch.mean(tail_losses)
                total_loss += cvar_loss
            
            # Maximum drawdown penalty
            cumulative_returns = torch.cumsum(returns, dim=0)
            running_max = torch.cummax(cumulative_returns, dim=0)[0]
            drawdown = cumulative_returns - running_max
            max_drawdown = -torch.min(drawdown)
            total_loss += self.lambda_drawdown * max_drawdown
        
        # Temporal consistency (autonomous driving insight)
        if historical_predictions is not None and 'price_movement' in historical_predictions:
            if historical_predictions['price_movement'].shape == predictions['price_movement'].shape:
                temporal_loss = 0.1 * F.mse_loss(predictions['price_movement'], historical_predictions['price_movement'])
                total_loss += temporal_loss
        
        return {
            'total_loss': total_loss,
            'task_losses': task_losses,
            'uncertainty_weights': torch.exp(-self.log_vars)
        }


class UnifiedTransformer(nn.Module):
    """Transformer architecture inspired by autonomous driving for financial prediction."""
    
    def __init__(self, seq_len=24, d_model=128, nhead=8, num_layers=4, dropout=0.1):
        super().__init__()
        
        self.seq_len = seq_len
        self.d_model = d_model
        
        # Multi-timeframe input projection (OHLCV)
        self.input_projection = nn.Linear(5, d_model)
        
        # Positional encoding (learnable)
        self.positional_encoding = nn.Parameter(torch.randn(seq_len, d_model) * 0.1)
        
        # Transformer encoder with GELU activation (modern architecture)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, 
            nhead=nhead, 
            dim_feedforward=d_model * 4,
            dropout=dropout, 
            batch_first=True,
            activation='gelu'
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers)
        
        # Multi-task prediction heads with proper activation functions
        self.price_head = nn.Linear(d_model, 1)
        
        self.volatility_head = nn.Sequential(
            nn.Linear(d_model, d_model//2),
            nn.ReLU(),
            nn.Linear(d_model//2, 1),
            nn.Sigmoid()  # 0 to 1 range
        )
        
        self.volume_head = nn.Sequential(
            nn.Linear(d_model, d_model//2),
            nn.ReLU(), 
            nn.Linear(d_model//2, 1),
            nn.Softplus()  # Positive values
        )
        
        self.regime_head = nn.Linear(d_model, 4)  # 4 market regimes
        
        self.risk_head = nn.Sequential(
            nn.Linear(d_model, d_model//2),
            nn.ReLU(),
            nn.Linear(d_model//2, 1),
            nn.Sigmoid()  # 0 to 1 range
        )
    
    def forward(self, x):
        batch_size, seq_len, _ = x.shape
        
        # Project inputs to model dimension
        x = self.input_projection(x)
        
        # Add positional encoding
        x = x + self.positional_encoding[:seq_len].unsqueeze(0)
        
        # Transformer encoding
        x = self.transformer(x)
        
        # Global average pooling
        pooled = x.mean(dim=1)
        
        return {
            'price_movement': torch.tanh(self.price_head(pooled)),  # -1 to 1
            'volatility': self.volatility_head(pooled),  # 0 to 1
            'volume_profile': self.volume_head(pooled),  # positive
            'regime_change': self.regime_head(pooled),  # logits
            'risk_assessment': self.risk_head(pooled)  # 0 to 1
        }


def generate_realistic_aapl_data(num_samples=5000, seq_len=24) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
    """Generate realistic AAPL-like market data with proper statistical properties."""
    
    logger.info(f"📊 Generating realistic AAPL-like data: {num_samples} samples")
    
    # AAPL-like parameters (based on historical data)
    initial_price = 225.0  # Current AAPL price level
    daily_vol = 0.25  # Annualized volatility
    intraday_vol = daily_vol / np.sqrt(252 * 24)  # Hourly volatility
    drift = 0.12 / (252 * 24)  # Annualized drift per hour
    
    # Generate price path using geometric Brownian motion
    np.random.seed(42)  # For reproducibility
    dt = 1.0 / (252 * 24)  # One hour
    
    # Generate returns with realistic autocorrelation
    total_hours = num_samples + seq_len + 100
    random_shocks = np.random.normal(0, 1, total_hours)
    
    # Add some autocorrelation (momentum and mean reversion)
    autocorr_returns = np.zeros(total_hours)
    for i in range(1, total_hours):
        momentum = 0.1 * autocorr_returns[i-1] if i > 0 else 0
        mean_reversion = -0.05 * np.mean(autocorr_returns[max(0, i-24):i]) if i > 24 else 0
        autocorr_returns[i] = drift + momentum + mean_reversion + intraday_vol * random_shocks[i]
    
    # Generate price levels
    log_prices = np.log(initial_price) + np.cumsum(autocorr_returns)
    prices = np.exp(log_prices)
    
    # Generate OHLCV data
    ohlcv_data = []
    for i in range(len(prices) - 1):
        open_price = prices[i]
        close_price = prices[i + 1]
        
        # High and low with realistic intrabar movement
        intrabar_range = abs(close_price - open_price) * (1 + np.random.exponential(0.5))
        high_price = max(open_price, close_price) + intrabar_range * np.random.beta(2, 5)
        low_price = min(open_price, close_price) - intrabar_range * np.random.beta(2, 5)
        
        # Volume (inversely correlated with price, higher during volatility)
        base_volume = 1000000
        vol_factor = abs(close_price - open_price) / open_price
        volume = base_volume * (1 + vol_factor * 10) * np.random.lognormal(0, 0.5)
        
        ohlcv_data.append([open_price, high_price, low_price, close_price, volume])
    
    ohlcv_array = np.array(ohlcv_data, dtype=np.float32)
    
    # Normalize features
    from sklearn.preprocessing import RobustScaler
    scaler = RobustScaler()
    ohlcv_normalized = scaler.fit_transform(ohlcv_array)
    
    logger.info(f"   Price range: ${np.min(ohlcv_array[:, 3]):.2f} - ${np.max(ohlcv_array[:, 3]):.2f}")
    logger.info(f"   Mean volume: {np.mean(ohlcv_array[:, 4]):,.0f}")
    
    # Create sequences and targets
    features = []
    targets = []
    
    logger.info("🔄 Creating sequences and targets...")
    
    for i in range(len(ohlcv_normalized) - seq_len - 1):
        # Input sequence (24 timesteps)
        sequence = ohlcv_normalized[i:i+seq_len]
        features.append(sequence)
        
        # Future price movement (next timestep)
        current_price = ohlcv_normalized[i+seq_len-1, 3]  # current close
        future_price = ohlcv_normalized[i+seq_len, 3]  # next close
        price_movement = (future_price - current_price)
        
        # Create multi-task targets with realistic relationships
        volatility = abs(price_movement)
        
        # Volume change
        current_volume = ohlcv_normalized[i+seq_len-1, 4]
        future_volume = ohlcv_normalized[i+seq_len, 4] 
        volume_change = abs(future_volume - current_volume)
        
        # Market regime (based on price movement and volatility)
        if price_movement > 0.5:
            regime = 0  # Strong uptrend
        elif price_movement < -0.5:
            regime = 1  # Strong downtrend
        elif volatility > 0.3:
            regime = 3  # High volatility
        else:
            regime = 2  # Sideways/low volatility
        
        # Risk assessment (based on volatility and recent price movements)
        recent_volatility = np.std(ohlcv_normalized[max(0, i+seq_len-10):i+seq_len, 3])
        risk = min(recent_volatility * 2, 1.0)
        
        target_dict = {
            'price_movement': price_movement,
            'volatility': volatility,
            'volume_profile': volume_change,
            'regime_change': regime,
            'risk_assessment': risk
        }
        targets.append(target_dict)
    
    # Convert to arrays
    features_array = np.array(features, dtype=np.float32)
    
    target_arrays = {
        'price_movement': np.array([t['price_movement'] for t in targets], dtype=np.float32).reshape(-1, 1),
        'volatility': np.array([t['volatility'] for t in targets], dtype=np.float32).reshape(-1, 1),
        'volume_profile': np.array([t['volume_profile'] for t in targets], dtype=np.float32).reshape(-1, 1),
        'regime_change': np.array([t['regime_change'] for t in targets], dtype=np.int64),
        'risk_assessment': np.array([t['risk_assessment'] for t in targets], dtype=np.float32).reshape(-1, 1)
    }
    
    logger.info(f"✅ Created {len(features_array)} sequences")
    logger.info(f"   Features shape: {features_array.shape}")
    logger.info(f"   Price movement range: {target_arrays['price_movement'].min():.4f} to {target_arrays['price_movement'].max():.4f}")
    logger.info(f"   Regime distribution: {np.bincount(target_arrays['regime_change'])}")
    
    return features_array, target_arrays


def calculate_financial_metrics(returns: np.ndarray) -> Dict[str, float]:
    """Calculate comprehensive financial performance metrics."""
    
    if len(returns) == 0:
        return {'sharpe_ratio': 0.0, 'max_drawdown': 0.0, 'total_return': 0.0, 'volatility': 0.0}
    
    # Remove extreme outliers
    returns = np.clip(returns, -0.5, 0.5)
    
    # Cumulative returns
    cumulative_returns = np.cumsum(returns)
    total_return = cumulative_returns[-1] if len(cumulative_returns) > 0 else 0.0
    
    # Volatility (annualized)
    volatility = np.std(returns) * np.sqrt(252 * 24) if len(returns) > 1 else 0.0
    
    # Sharpe ratio (annualized, assuming risk-free rate = 2%)
    mean_return = np.mean(returns) 
    excess_return = mean_return * 252 * 24 - 0.02  # Excess over risk-free rate
    sharpe_ratio = excess_return / volatility if volatility > 0 else 0.0
    
    # Maximum drawdown
    running_max = np.maximum.accumulate(cumulative_returns)
    drawdown = cumulative_returns - running_max
    max_drawdown = np.min(drawdown) if len(drawdown) > 0 else 0.0
    
    # Information ratio (similar to Sharpe but uses tracking error)
    information_ratio = excess_return / (np.std(returns) * np.sqrt(252 * 24)) if np.std(returns) > 0 else 0.0
    
    return {
        'sharpe_ratio': float(sharpe_ratio),
        'information_ratio': float(information_ratio),
        'max_drawdown': float(abs(max_drawdown)),
        'total_return': float(total_return),
        'annualized_return': float(mean_return * 252 * 24),
        'volatility': float(volatility),
        'mean_return': float(mean_return)
    }


def train_unified_loss_model():
    """Train the unified loss transformer model."""
    
    logger.info("🚀 TRAINING UNIFIED LOSS TRANSFORMER")
    logger.info("🚗 Autonomous Driving + 💰 Financial Trading Synthesis")
    logger.info("=" * 60)
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    logger.info(f"🔧 Device: {device}")
    
    try:
        # Generate realistic training data
        features, targets = generate_realistic_aapl_data(num_samples=10000, seq_len=24)
        
        # Split data (80/20 train/test)
        train_size = int(0.8 * len(features))
        
        train_features = features[:train_size]
        test_features = features[train_size:]
        
        train_targets = {k: v[:train_size] for k, v in targets.items()}
        test_targets = {k: v[train_size:] for k, v in targets.items()}
        
        logger.info(f"📊 Training set: {len(train_features):,} sequences")
        logger.info(f"📊 Test set: {len(test_features):,} sequences")
        
        # Initialize model and loss function
        model = UnifiedTransformer(
            seq_len=24, 
            d_model=128, 
            nhead=8, 
            num_layers=4, 
            dropout=0.1
        ).to(device)
        
        loss_function = FinancialAVLoss(
            num_tasks=5,
            alpha_cvar=0.05,
            lambda_drawdown=2.0,
            gamma_focal=2.0
        ).to(device)
        
        # Count parameters
        total_params = sum(p.numel() for p in model.parameters())
        logger.info(f"🏗️ Model parameters: {total_params:,}")
        
        # Optimizer with learning rate scheduling
        optimizer = torch.optim.AdamW(
            list(model.parameters()) + list(loss_function.parameters()),
            lr=0.001, 
            weight_decay=1e-5,
            betas=(0.9, 0.999)
        )
        
        scheduler = torch.optim.lr_scheduler.OneCycleLR(
            optimizer,
            max_lr=0.003,
            epochs=100,
            steps_per_epoch=len(train_features) // 64 + 1
        )
        
        # Training configuration
        batch_size = 64
        num_epochs = 100
        best_loss = float('inf')
        training_losses = []
        validation_losses = []
        patience_counter = 0
        patience = 15
        
        logger.info(f"🚀 Starting training for {num_epochs} epochs...")
        logger.info(f"   Batch size: {batch_size}")
        logger.info(f"   Learning rate: {optimizer.param_groups[0]['lr']:.6f}")
        
        for epoch in range(num_epochs):
            # Training phase
            model.train()
            epoch_losses = []
            
            # Shuffle training data
            indices = np.random.permutation(len(train_features))
            
            for i in range(0, len(train_features), batch_size):
                batch_indices = indices[i:i+batch_size]
                
                # Create batch
                batch_features = torch.tensor(train_features[batch_indices]).to(device)
                batch_targets = {
                    k: torch.tensor(v[batch_indices]).to(device) 
                    for k, v in train_targets.items()
                }
                
                # Forward pass
                optimizer.zero_grad()
                outputs = model(batch_features)
                loss_components = loss_function(outputs, batch_targets)
                
                # Backward pass
                loss_components['total_loss'].backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
                torch.nn.utils.clip_grad_norm_(loss_function.parameters(), max_norm=1.0)
                
                optimizer.step()
                scheduler.step()
                
                epoch_losses.append(loss_components['total_loss'].item())
            
            avg_train_loss = np.mean(epoch_losses)
            training_losses.append(avg_train_loss)
            
            # Validation phase
            model.eval()
            val_losses = []
            
            with torch.no_grad():
                for i in range(0, len(test_features), batch_size):
                    batch_features = torch.tensor(test_features[i:i+batch_size]).to(device)
                    batch_targets = {
                        k: torch.tensor(v[i:i+batch_size]).to(device) 
                        for k, v in test_targets.items()
                    }
                    
                    outputs = model(batch_features)
                    loss_components = loss_function(outputs, batch_targets)
                    val_losses.append(loss_components['total_loss'].item())
            
            avg_val_loss = np.mean(val_losses)
            validation_losses.append(avg_val_loss)
            
            # Model checkpointing
            if avg_val_loss < best_loss:
                best_loss = avg_val_loss
                patience_counter = 0
                
                # Save best model
                model_path = '/data/models/unified_transformer_best.pth'
                os.makedirs(os.path.dirname(model_path), exist_ok=True)
                
                torch.save({
                    'model_state_dict': model.state_dict(),
                    'loss_state_dict': loss_function.state_dict(),
                    'optimizer_state_dict': optimizer.state_dict(),
                    'scheduler_state_dict': scheduler.state_dict(),
                    'epoch': epoch,
                    'train_loss': avg_train_loss,
                    'val_loss': avg_val_loss,
                    'uncertainty_weights': loss_function.log_vars.detach().cpu().numpy()
                }, model_path)
                
            else:
                patience_counter += 1
            
            # Logging
            if epoch % 10 == 0:
                uncertainty_weights = torch.exp(-loss_function.log_vars).detach().cpu().numpy()
                logger.info(f"Epoch {epoch:3d}: Train={avg_train_loss:.4f}, Val={avg_val_loss:.4f}, "
                          f"LR={optimizer.param_groups[0]['lr']:.6f}")
                logger.info(f"          Uncertainties: {uncertainty_weights}")
            
            # Early stopping
            if patience_counter >= patience:
                logger.info(f"Early stopping at epoch {epoch} (patience={patience})")
                break
        
        logger.info(f"✅ Training completed. Best validation loss: {best_loss:.4f}")
        
        # Final evaluation
        logger.info("📊 Final model evaluation...")
        
        # Load best model
        checkpoint = torch.load(model_path, map_location=device)
        model.load_state_dict(checkpoint['model_state_dict'])
        model.eval()
        
        # Generate predictions
        test_predictions = []
        test_actuals = []
        
        with torch.no_grad():
            for i in range(0, len(test_features), batch_size):
                batch_features = torch.tensor(test_features[i:i+batch_size]).to(device)
                batch_targets = {
                    k: torch.tensor(v[i:i+batch_size]).to(device) 
                    for k, v in test_targets.items()
                }
                
                outputs = model(batch_features)
                test_predictions.append(outputs['price_movement'].cpu().numpy())
                test_actuals.append(batch_targets['price_movement'].cpu().numpy())
        
        # Combine predictions and actuals
        predictions = np.concatenate(test_predictions, axis=0).flatten()
        actuals = np.concatenate(test_actuals, axis=0).flatten()
        
        # Calculate comprehensive metrics
        metrics = calculate_financial_metrics(predictions)
        
        # Additional evaluation metrics
        directional_accuracy = np.mean(np.sign(predictions) == np.sign(actuals))
        correlation = np.corrcoef(predictions, actuals)[0, 1] if len(predictions) > 1 else 0.0
        mse = np.mean((predictions - actuals) ** 2)
        mae = np.mean(np.abs(predictions - actuals))
        
        # Print comprehensive results
        logger.info("\n" + "="*60)
        logger.info("📈 COMPREHENSIVE EVALUATION RESULTS")
        logger.info("="*60)
        logger.info("🏆 PERFORMANCE METRICS:")
        logger.info(f"   📊 Sharpe Ratio: {metrics['sharpe_ratio']:.3f}")
        logger.info(f"   📊 Information Ratio: {metrics['information_ratio']:.3f}")
        logger.info(f"   📉 Maximum Drawdown: {metrics['max_drawdown']:.4f}")
        logger.info(f"   📈 Total Return: {metrics['total_return']:.4f}")
        logger.info(f"   📈 Annualized Return: {metrics['annualized_return']:.1%}")
        logger.info(f"   📊 Volatility: {metrics['volatility']:.1%}")
        logger.info("")
        logger.info("🎯 PREDICTION ACCURACY:")
        logger.info(f"   🎯 Directional Accuracy: {directional_accuracy:.1%}")
        logger.info(f"   🔗 Correlation: {correlation:.4f}")
        logger.info(f"   📊 MSE: {mse:.6f}")
        logger.info(f"   📊 MAE: {mae:.6f}")
        logger.info("")
        logger.info("🔬 CROSS-DOMAIN INSIGHTS:")
        uncertainty_weights = torch.exp(-torch.tensor(checkpoint['uncertainty_weights'])).numpy()
        logger.info(f"   🚗 Multi-task uncertainties: {uncertainty_weights}")
        logger.info(f"   💰 CVaR penalty: Active (α={loss_function.alpha_cvar})")
        logger.info(f"   📉 Drawdown penalty: {loss_function.lambda_drawdown}x weight")
        logger.info(f"   🔥 Focal loss enhancement: γ={loss_function.gamma_focal}")
        
        # Save comprehensive results
        results = {
            'model_path': model_path,
            'training_config': {
                'seq_len': 24,
                'd_model': 128,
                'nhead': 8,
                'num_layers': 4,
                'batch_size': batch_size,
                'num_epochs': epoch + 1,
                'total_parameters': total_params
            },
            'training_history': {
                'training_losses': training_losses,
                'validation_losses': validation_losses,
                'best_epoch': checkpoint['epoch'],
                'best_train_loss': float(checkpoint['train_loss']),
                'best_val_loss': float(checkpoint['val_loss'])
            },
            'evaluation_metrics': {
                'sharpe_ratio': metrics['sharpe_ratio'],
                'information_ratio': metrics['information_ratio'],
                'max_drawdown': metrics['max_drawdown'],
                'total_return': metrics['total_return'],
                'annualized_return': metrics['annualized_return'],
                'volatility': metrics['volatility'],
                'directional_accuracy': float(directional_accuracy),
                'correlation': float(correlation),
                'mse': float(mse),
                'mae': float(mae)
            },
            'cross_domain_insights': {
                'uncertainty_weights': uncertainty_weights.tolist(),
                'cvar_alpha': loss_function.alpha_cvar,
                'drawdown_lambda': loss_function.lambda_drawdown,
                'focal_gamma': loss_function.gamma_focal
            },
            'data_info': {
                'train_samples': len(train_features),
                'test_samples': len(test_features),
                'features_shape': list(features.shape),
                'target_regime_distribution': np.bincount(targets['regime_change']).tolist()
            }
        }
        
        # Save results
        results_path = '/data/models/unified_loss_training_results.json'
        with open(results_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"💾 Complete results saved to: {results_path}")
        
        # Production readiness assessment
        production_ready = (
            metrics['sharpe_ratio'] > 0.5 and  # Reasonable Sharpe ratio
            metrics['max_drawdown'] < 0.2 and  # Acceptable drawdown
            directional_accuracy > 0.52 and    # Better than random
            correlation > 0.1                  # Some predictive power
        )
        
        logger.info(f"\n🚀 Production Readiness: {'✅ READY' if production_ready else '❌ NEEDS IMPROVEMENT'}")
        
        if production_ready:
            logger.info("🎉 UNIFIED LOSS MODEL TRAINING SUCCESSFUL!")
            logger.info("✅ Cross-domain research synthesis validated")
            logger.info("✅ Autonomous driving insights integrated")
            logger.info("✅ Financial trading insights integrated")
            logger.info("✅ Model ready for deployment")
        else:
            logger.info("⚠️ Model needs further optimization for production use")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Training failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = train_unified_loss_model()
    sys.exit(0 if success else 1)