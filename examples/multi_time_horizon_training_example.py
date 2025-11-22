"""
Multi-Time Horizon Training Example

Shows how to use the MultiTimeHorizonDataLoader for training
state-of-the-art multi-scale attention models like Temporal Fusion Transformers.

This example shows:
1. Loading features from existing ArrayRecord files
2. Creating aligned multi-timeframe sequences  
3. Training a hierarchical attention model
4. Evaluating multi-horizon predictions

Based on 2024-2025 research:
- Temporal Fusion Transformers with interpretable multi-horizon attention
- Hierarchical Spatio-Temporal Attention (18.4% RMSE reduction)
- Multi-Scale Self-Supervised Learning with temporal alignment
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter

# Add project root to path
sys.path.append('/home/jianjun/ats-genai-model/src')

from domains.ml.data_loaders.multi_time_horizon_dataloader import (
    MultiTimeHorizonConfig,
    create_multi_horizon_dataloader
)
from domains.services.training_data.arrayrecord_feature_reader import FeatureService

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MultiScaleAttentionBlock(nn.Module):
    """
    Multi-scale attention block for hierarchical temporal modeling.
    
    Implements attention mechanisms that operate across different timeframes
    with scale-specific attention patterns.
    """
    
    def __init__(self, d_model: int, n_heads: int, dropout: float = 0.1):
        super().__init__()
        self.d_model = d_model
        self.n_heads = n_heads
        
        # Separate attention for different scales
        self.local_attention = nn.MultiheadAttention(
            embed_dim=d_model,
            num_heads=n_heads,
            dropout=dropout,
            batch_first=True
        )
        
        self.global_attention = nn.MultiheadAttention(
            embed_dim=d_model,
            num_heads=n_heads,
            dropout=dropout,
            batch_first=True
        )
        
        # Cross-scale attention for temporal alignment
        self.cross_attention = nn.MultiheadAttention(
            embed_dim=d_model,
            num_heads=n_heads,
            dropout=dropout,
            batch_first=True
        )
        
        # Feed forward networks
        self.ffn = nn.Sequential(
            nn.Linear(d_model, d_model * 4),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model * 4, d_model),
            nn.Dropout(dropout)
        )
        
        # Layer normalization
        self.norm1 = nn.LayerNorm(d_model)
        self.norm2 = nn.LayerNorm(d_model)
        self.norm3 = nn.LayerNorm(d_model)
        
    def forward(self, local_seq, global_seq, local_mask=None, global_mask=None):
        """
        Forward pass with multi-scale attention.
        
        Args:
            local_seq: High-frequency sequence (e.g., 5m data) [B, T_local, D]
            global_seq: Low-frequency sequence (e.g., 1h data) [B, T_global, D]
            local_mask: Attention mask for local sequence
            global_mask: Attention mask for global sequence
            
        Returns:
            Enhanced local and global representations
        """
        
        # Local self-attention
        local_attn, _ = self.local_attention(
            local_seq, local_seq, local_seq,
            key_padding_mask=local_mask
        )
        local_seq = self.norm1(local_seq + local_attn)
        
        # Global self-attention  
        global_attn, _ = self.global_attention(
            global_seq, global_seq, global_seq,
            key_padding_mask=global_mask
        )
        global_seq = self.norm2(global_seq + global_attn)
        
        # Cross-scale attention (local queries, global keys/values)
        cross_attn, cross_weights = self.cross_attention(
            local_seq, global_seq, global_seq,
            key_padding_mask=global_mask
        )
        local_enhanced = self.norm3(local_seq + cross_attn)
        
        # Feed forward
        local_out = local_enhanced + self.ffn(local_enhanced)
        global_out = global_seq + self.ffn(global_seq)
        
        return local_out, global_out, cross_weights


class HierarchicalTemporalFusionTransformer(nn.Module):
    """
    Hierarchical Temporal Fusion Transformer for multi-horizon forecasting.
    
    Architecture:
    1. Input embedding for each timeframe
    2. Multi-scale attention blocks for temporal modeling
    3. Temporal fusion with interpretable attention
    4. Multi-horizon prediction heads
    """
    
    def __init__(
        self,
        feature_dims: dict,  # {timeframe: feature_dim}
        d_model: int = 256,
        n_heads: int = 8,
        n_layers: int = 6,
        prediction_horizons: list = [1, 3, 6, 12, 24],
        dropout: float = 0.1
    ):
        super().__init__()
        self.feature_dims = feature_dims
        self.d_model = d_model
        self.prediction_horizons = prediction_horizons
        
        # Input embedding layers for each timeframe
        self.input_embeddings = nn.ModuleDict()
        for timeframe, feature_dim in feature_dims.items():
            self.input_embeddings[timeframe] = nn.Sequential(
                nn.Linear(feature_dim, d_model),
                nn.LayerNorm(d_model),
                nn.Dropout(dropout)
            )
        
        # Positional encoding
        self.positional_encoding = nn.Parameter(
            torch.randn(1000, d_model) * 0.1
        )
        
        # Multi-scale attention layers
        self.attention_layers = nn.ModuleList([
            MultiScaleAttentionBlock(d_model, n_heads, dropout)
            for _ in range(n_layers)
        ])
        
        # Temporal fusion layer
        self.temporal_fusion = nn.MultiheadAttention(
            embed_dim=d_model,
            num_heads=n_heads,
            dropout=dropout,
            batch_first=True
        )
        
        # Multi-horizon prediction heads
        self.prediction_heads = nn.ModuleList([
            nn.Sequential(
                nn.Linear(d_model, d_model // 2),
                nn.GELU(),
                nn.Dropout(dropout),
                nn.Linear(d_model // 2, 1)
            )
            for _ in prediction_horizons
        ])
        
        # Horizon attention weights
        self.horizon_attention = nn.Sequential(
            nn.Linear(d_model, len(prediction_horizons)),
            nn.Softmax(dim=-1)
        )
        
    def forward(self, features_by_scale, attention_masks):
        """
        Forward pass for multi-horizon prediction.
        
        Args:
            features_by_scale: Dict of {timeframe: tensor [B, T, F]}
            attention_masks: Dict of {timeframe: mask [B, T]}
            
        Returns:
            Multi-horizon predictions and attention weights
        """
        
        # Embed features for each timeframe
        embedded_sequences = {}
        for timeframe, features in features_by_scale.items():
            if timeframe in self.input_embeddings:
                embedded = self.input_embeddings[timeframe](features)
                
                # Add positional encoding
                seq_len = embedded.size(1)
                pos_enc = self.positional_encoding[:seq_len].unsqueeze(0)
                embedded = embedded + pos_enc
                
                embedded_sequences[timeframe] = embedded
        
        # Get local and global sequences
        timeframes = list(embedded_sequences.keys())
        if len(timeframes) >= 2:
            # Use highest frequency as local, lowest as global
            local_timeframe = timeframes[0]  # Assuming sorted by frequency
            global_timeframe = timeframes[-1]
            
            local_seq = embedded_sequences[local_timeframe]
            global_seq = embedded_sequences[global_timeframe]
            local_mask = attention_masks.get(local_timeframe)
            global_mask = attention_masks.get(global_timeframe)
        else:
            # Single timeframe case
            timeframe = timeframes[0]
            local_seq = global_seq = embedded_sequences[timeframe]
            local_mask = global_mask = attention_masks.get(timeframe)
        
        # Multi-scale attention layers
        cross_attention_weights = []
        for layer in self.attention_layers:
            local_seq, global_seq, cross_weights = layer(
                local_seq, global_seq, local_mask, global_mask
            )
            cross_attention_weights.append(cross_weights)
        
        # Temporal fusion
        fused_seq, fusion_weights = self.temporal_fusion(
            local_seq, global_seq, global_seq,
            key_padding_mask=global_mask
        )
        
        # Global pooling for prediction
        if local_mask is not None:
            # Masked average pooling
            mask_expanded = (~local_mask).unsqueeze(-1).float()
            masked_seq = fused_seq * mask_expanded
            seq_lengths = mask_expanded.sum(dim=1, keepdim=True).clamp(min=1)
            pooled = masked_seq.sum(dim=1) / seq_lengths.squeeze(-1)
        else:
            # Simple average pooling
            pooled = fused_seq.mean(dim=1)
        
        # Multi-horizon predictions
        horizon_preds = []
        for head in self.prediction_heads:
            pred = head(pooled)  # [B, 1]
            horizon_preds.append(pred)
        
        predictions = torch.cat(horizon_preds, dim=-1)  # [B, num_horizons]
        
        # Horizon attention weights
        horizon_weights = self.horizon_attention(pooled)  # [B, num_horizons]
        
        return {
            'predictions': predictions,
            'horizon_weights': horizon_weights,
            'fusion_weights': fusion_weights,
            'cross_attention_weights': cross_attention_weights[-1]  # Last layer
        }


def train_multi_horizon_model():
    """
    Train a multi-time horizon forecasting model using the custom data loader.
    """
    
    # Configuration
    config = MultiTimeHorizonConfig(
        timeframes=['5m', '15m', '1h', '1d'],
        base_timeframe='5m',
        sequence_length=144,  # 12 hours of 5-minute data
        prediction_horizons=[1, 3, 6, 12, 24],  # Steps ahead
        feature_groups=['ohlcv_basic', 'technical_momentum'],
        target_features=['close', 'returns'],
        alignment_method='forward_fill',
        max_missing_ratio=0.15
    )
    
    # Data configuration
    symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'NVDA']
    train_date_range = (
        datetime(2024, 1, 1),
        datetime(2024, 10, 31)
    )
    val_date_range = (
        datetime(2024, 11, 1),
        datetime(2024, 12, 31)
    )
    
    # Create data loaders
    logger.info("Creating training data loader...")
    train_loader = create_multi_horizon_dataloader(
        symbols=symbols,
        date_range=train_date_range,
        config=config,
        batch_size=32,
        shuffle=True,
        num_workers=4
    )
    
    logger.info("Creating validation data loader...")
    val_loader = create_multi_horizon_dataloader(
        symbols=symbols,
        date_range=val_date_range,
        config=config,
        batch_size=32,
        shuffle=False,
        num_workers=4
    )
    
    # Inspect a sample batch to determine feature dimensions
    sample_batch = next(iter(train_loader))
    features_sample, targets_sample, masks_sample = sample_batch
    
    feature_dims = {}
    for timeframe, feature_tensor in features_sample.items():
        feature_dims[timeframe] = feature_tensor.size(-1)
    
    logger.info(f"Feature dimensions: {feature_dims}")
    logger.info(f"Target shape: {targets_sample.shape}")
    
    # Model configuration
    model = HierarchicalTemporalFusionTransformer(
        feature_dims=feature_dims,
        d_model=256,
        n_heads=8,
        n_layers=6,
        prediction_horizons=config.prediction_horizons,
        dropout=0.1
    )
    
    # Training configuration
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = model.to(device)
    
    optimizer = optim.AdamW(model.parameters(), lr=1e-4, weight_decay=1e-5)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='min', factor=0.5, patience=10
    )
    
    # Loss function with horizon weighting
    criterion = nn.MSELoss(reduction='none')
    
    # Training setup
    num_epochs = 100
    best_val_loss = float('inf')
    patience = 20
    patience_counter = 0
    
    # Tensorboard logging
    writer = SummaryWriter('runs/multi_horizon_training')
    
    logger.info(f"Starting training on {device}...")
    logger.info(f"Model parameters: {sum(p.numel() for p in model.parameters()):,}")
    
    for epoch in range(num_epochs):
        # Training phase
        model.train()
        train_loss = 0.0
        train_batches = 0
        
        for batch_idx, (features, targets, masks) in enumerate(train_loader):
            # Move to device
            features = {k: v.to(device) for k, v in features.items()}
            targets = targets.to(device)
            masks = {k: v.to(device) for k, v in masks.items()}
            
            # Forward pass
            optimizer.zero_grad()
            output = model(features, masks)
            predictions = output['predictions']
            horizon_weights = output['horizon_weights']
            
            # Multi-horizon loss with attention weighting
            losses = criterion(predictions, targets)  # [B, num_horizons]
            weighted_losses = losses * horizon_weights
            loss = weighted_losses.mean()
            
            # Backward pass
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            
            train_loss += loss.item()
            train_batches += 1
            
            # Log progress
            if batch_idx % 100 == 0:
                logger.info(
                    f"Epoch {epoch}, Batch {batch_idx}, "
                    f"Loss: {loss.item():.6f}, "
                    f"LR: {optimizer.param_groups[0]['lr']:.2e}"
                )
        
        avg_train_loss = train_loss / train_batches
        
        # Validation phase
        model.eval()
        val_loss = 0.0
        val_batches = 0
        
        with torch.no_grad():
            for features, targets, masks in val_loader:
                # Move to device
                features = {k: v.to(device) for k, v in features.items()}
                targets = targets.to(device)
                masks = {k: v.to(device) for k, v in masks.items()}
                
                # Forward pass
                output = model(features, masks)
                predictions = output['predictions']
                horizon_weights = output['horizon_weights']
                
                # Loss calculation
                losses = criterion(predictions, targets)
                weighted_losses = losses * horizon_weights
                loss = weighted_losses.mean()
                
                val_loss += loss.item()
                val_batches += 1
        
        avg_val_loss = val_loss / val_batches
        
        # Learning rate scheduling
        scheduler.step(avg_val_loss)
        
        # Logging
        writer.add_scalar('Loss/Train', avg_train_loss, epoch)
        writer.add_scalar('Loss/Validation', avg_val_loss, epoch)
        writer.add_scalar('Learning_Rate', optimizer.param_groups[0]['lr'], epoch)
        
        logger.info(
            f"Epoch {epoch}: "
            f"Train Loss = {avg_train_loss:.6f}, "
            f"Val Loss = {avg_val_loss:.6f}"
        )
        
        # Early stopping
        if avg_val_loss < best_val_loss:
            best_val_loss = avg_val_loss
            patience_counter = 0
            
            # Save best model
            torch.save({
                'epoch': epoch,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'val_loss': avg_val_loss,
                'config': config
            }, 'best_multi_horizon_model.pt')
            
            logger.info(f"New best model saved with validation loss: {avg_val_loss:.6f}")
        else:
            patience_counter += 1
            
        if patience_counter >= patience:
            logger.info(f"Early stopping triggered after {patience} epochs without improvement")
            break
    
    writer.close()
    logger.info("Training completed!")


def evaluate_model_interpretability():
    """
    Evaluate the trained model's attention patterns and interpretability.
    """
    
    logger.info("Loading trained model for interpretability analysis...")
    
    # Load best model
    checkpoint = torch.load('best_multi_horizon_model.pt')
    config = checkpoint['config']
    
    # Recreate model architecture (simplified for evaluation)
    # In practice, you'd save the full model architecture
    logger.info("Model interpretability analysis would include:")
    logger.info("1. Cross-attention weights visualization across timeframes")
    logger.info("2. Horizon attention weights for different market conditions")
    logger.info("3. Feature importance analysis by timeframe")
    logger.info("4. Prediction confidence intervals")
    logger.info("5. Temporal pattern discovery in attention maps")


if __name__ == "__main__":
    """
    Run the complete multi-time horizon training example.
    """
    
    try:
        logger.info("Starting Multi-Time Horizon Training Example")
        logger.info("=" * 50)
        
        # Check if ArrayRecord features are available
        feature_service = FeatureService()
        if not hasattr(feature_service, 'base_path'):
            logger.warning(
                "FeatureService not properly initialized. "
                "Make sure ArrayRecord features have been generated."
            )
        
        # Run training
        train_multi_horizon_model()
        
        # Run interpretability analysis
        evaluate_model_interpretability()
        
        logger.info("Example completed successfully!")
        
    except Exception as e:
        logger.error(f"Training failed: {e}")
        raise
    
    logger.info("Multi-Time Horizon Training Example finished.")