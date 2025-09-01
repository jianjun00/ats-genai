"""
Temporal Fusion Transformer for Multi-Horizon Financial Forecasting

This module implements a state-of-the-art Temporal Fusion Transformer (TFT) model
adapted from the MathTypes ATS research system, specifically optimized for financial
time series forecasting with sentiment integration.

The TFT model provides:
- Multi-horizon forecasting with attention mechanisms
- Variable selection networks for feature importance
- Static and dynamic covariate processing
- Integration with sentiment analysis features
- Interpretable attention and variable selection outputs

Reference: "Temporal Fusion Transformers for Interpretable Multi-horizon Time Series Forecasting"
https://arxiv.org/pdf/1912.09363.pdf
"""

import math
import logging
from typing import Dict, List
from dataclasses import dataclass

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau

logger = logging.getLogger(__name__)


@dataclass
class TFTConfig:
    """Configuration for Temporal Fusion Transformer."""
    
    # Model architecture
    hidden_size: int = 64
    lstm_layers: int = 2
    attention_head_size: int = 4
    dropout: float = 0.1
    
    # Data configuration
    max_encoder_length: int = 120  # 2 hours of 1-minute data
    max_prediction_length: int = 30  # 30 minutes ahead
    
    # Feature configuration
    static_features: List[str] = None
    temporal_features: List[str] = None
    target_features: List[str] = None
    
    # Training configuration
    learning_rate: float = 1e-3
    batch_size: int = 64
    max_epochs: int = 100
    patience: int = 10
    
    # Sentiment integration
    use_sentiment_features: bool = True
    sentiment_weight: float = 0.3
    
    def __post_init__(self):
        if self.static_features is None:
            self.static_features = []
        if self.temporal_features is None:
            self.temporal_features = [
                'open', 'high', 'low', 'close', 'volume',
                'returns', 'volatility', 'rsi', 'macd'
            ]
        if self.target_features is None:
            self.target_features = ['returns']


class TimeDistributed(nn.Module):
    """Apply a module over the time dimension."""
    
    def __init__(self, module: nn.Module):
        super().__init__()
        self.module = module
    
    def forward(self, x):
        if len(x.size()) <= 2:
            return self.module(x)
        
        # Reshape to (batch * time, features)
        x_reshape = x.contiguous().view(-1, x.size(-1))
        y = self.module(x_reshape)
        
        # Reshape back to (batch, time, output_features)
        y = y.contiguous().view(x.size(0), x.size(1), -1)
        return y


class GatedLinearUnit(nn.Module):
    """Gated Linear Unit with optional dropout."""
    
    def __init__(self, input_size: int, hidden_size: int = None, dropout: float = None):
        super().__init__()
        
        self.hidden_size = hidden_size or input_size
        self.dropout = nn.Dropout(dropout) if dropout is not None else None
        self.fc = nn.Linear(input_size, self.hidden_size * 2)
        
        self._init_weights()
    
    def _init_weights(self):
        torch.nn.init.xavier_uniform_(self.fc.weight)
        torch.nn.init.zeros_(self.fc.bias)
    
    def forward(self, x):
        if self.dropout is not None:
            x = self.dropout(x)
        x = self.fc(x)
        return F.glu(x, dim=-1)


class GatedResidualNetwork(nn.Module):
    """Gated Residual Network for variable processing."""
    
    def __init__(
        self,
        input_size: int,
        hidden_size: int,
        output_size: int,
        dropout: float = 0.1,
        context_size: int = None,
        residual: bool = True
    ):
        super().__init__()
        
        self.input_size = input_size
        self.output_size = output_size
        self.context_size = context_size
        self.hidden_size = hidden_size
        self.residual = residual
        
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.elu = nn.ELU()
        self.fc2 = nn.Linear(hidden_size, hidden_size)
        self.dropout = nn.Dropout(dropout)
        self.gate = nn.Linear(hidden_size, output_size)
        self.fc3 = nn.Linear(hidden_size, output_size)
        
        if context_size is not None:
            self.context_fc = nn.Linear(context_size, hidden_size, bias=False)
        
        if input_size != output_size:
            self.skip_fc = nn.Linear(input_size, output_size)
        
        self.layernorm = nn.LayerNorm(output_size)
        
        self._init_weights()
    
    def _init_weights(self):
        for layer in [self.fc1, self.fc2, self.fc3, self.gate]:
            torch.nn.init.xavier_uniform_(layer.weight)
            torch.nn.init.zeros_(layer.bias)
        
        if hasattr(self, 'context_fc'):
            torch.nn.init.xavier_uniform_(self.context_fc.weight)
        
        if hasattr(self, 'skip_fc'):
            torch.nn.init.xavier_uniform_(self.skip_fc.weight)
            torch.nn.init.zeros_(self.skip_fc.bias)
    
    def forward(self, x, context=None):
        # Main path
        a = self.fc1(x)
        
        if context is not None:
            a = a + self.context_fc(context)
        
        a = self.elu(a)
        a = self.fc2(a)
        a = self.dropout(a)
        
        # Gating mechanism
        g = torch.sigmoid(self.gate(a))
        c = self.fc3(a)
        
        # Apply gate
        y = g * c
        
        # Residual connection
        if self.residual:
            if self.input_size != self.output_size:
                x = self.skip_fc(x)
            y = y + x
        
        # Layer normalization
        return self.layernorm(y)


class VariableSelectionNetwork(nn.Module):
    """Variable selection network for feature importance."""
    
    def __init__(
        self,
        input_sizes: Dict[str, int],
        hidden_size: int,
        dropout: float = 0.1,
        context_size: int = None
    ):
        super().__init__()
        
        self.input_sizes = input_sizes
        self.hidden_size = hidden_size
        self.variable_names = list(input_sizes.keys())
        
        # Single variable networks
        self.single_variable_grns = nn.ModuleDict()
        for name, size in input_sizes.items():
            self.single_variable_grns[name] = GatedResidualNetwork(
                size, hidden_size, hidden_size, dropout
            )
        
        # Flatten and process all variables
        total_size = len(input_sizes) * hidden_size
        self.flatten_grn = GatedResidualNetwork(
            total_size, hidden_size, len(input_sizes), dropout, context_size
        )
        
        self.softmax = nn.Softmax(dim=-1)
    
    def forward(self, variables: Dict[str, torch.Tensor], context: torch.Tensor = None):
        # Process each variable individually
        processed_vars = []
        for name in self.variable_names:
            var = variables[name]
            processed = self.single_variable_grns[name](var)
            processed_vars.append(processed)
        
        # Flatten all variables
        flattened = torch.cat(processed_vars, dim=-1)
        
        # Variable selection weights
        weights = self.flatten_grn(flattened, context)
        weights = self.softmax(weights)
        
        # Apply weights to processed variables
        weighted_vars = []
        for i, processed in enumerate(processed_vars):
            weight = weights[..., i:i+1]
            weighted = processed * weight
            weighted_vars.append(weighted)
        
        # Sum weighted variables
        output = torch.stack(weighted_vars, dim=-1).sum(dim=-1)
        
        return output, weights


class InterpretableMultiHeadAttention(nn.Module):
    """Interpretable multi-head attention mechanism."""
    
    def __init__(self, d_model: int, n_head: int, dropout: float = 0.1):
        super().__init__()
        
        assert d_model % n_head == 0
        
        self.d_model = d_model
        self.n_head = n_head
        self.d_k = d_model // n_head
        
        self.w_q = nn.Linear(d_model, d_model, bias=False)
        self.w_k = nn.Linear(d_model, d_model, bias=False)
        self.w_v = nn.Linear(d_model, d_model, bias=False)
        self.w_o = nn.Linear(d_model, d_model)
        
        self.dropout = nn.Dropout(dropout)
        
        self._init_weights()
    
    def _init_weights(self):
        for layer in [self.w_q, self.w_k, self.w_v, self.w_o]:
            torch.nn.init.xavier_uniform_(layer.weight)
            if layer.bias is not None:
                torch.nn.init.zeros_(layer.bias)
    
    def forward(self, query, key, value, mask=None):
        batch_size = query.size(0)
        seq_len = query.size(1)
        
        # Linear transformations
        Q = self.w_q(query).view(batch_size, seq_len, self.n_head, self.d_k).transpose(1, 2)
        K = self.w_k(key).view(batch_size, -1, self.n_head, self.d_k).transpose(1, 2)
        V = self.w_v(value).view(batch_size, -1, self.n_head, self.d_k).transpose(1, 2)
        
        # Attention computation
        scores = torch.matmul(Q, K.transpose(-2, -1)) / math.sqrt(self.d_k)
        
        if mask is not None:
            mask = mask.unsqueeze(1)  # Add head dimension
            scores.masked_fill_(mask == 0, -1e9)
        
        attention_weights = F.softmax(scores, dim=-1)
        attention_weights = self.dropout(attention_weights)
        
        # Apply attention to values
        context = torch.matmul(attention_weights, V)
        
        # Concatenate heads
        context = context.transpose(1, 2).contiguous().view(
            batch_size, seq_len, self.d_model
        )
        
        # Final linear transformation
        output = self.w_o(context)
        
        # Average attention weights across heads for interpretability
        avg_attention = attention_weights.mean(dim=1)
        
        return output, avg_attention


class TemporalFusionTransformer(nn.Module):
    """
    Temporal Fusion Transformer for multi-horizon financial forecasting.
    
    This implementation is adapted from the MathTypes ATS research system
    and optimized for financial time series with sentiment integration.
    """
    
    def __init__(self, config: TFTConfig):
        super().__init__()
        
        self.config = config
        self.hidden_size = config.hidden_size
        
        # Input processing
        self.temporal_input_size = len(config.temporal_features)
        if config.use_sentiment_features:
            self.temporal_input_size += 23  # Sentiment features from integrator
        
        self.temporal_projection = nn.Linear(self.temporal_input_size, config.hidden_size)
        
        # Variable selection networks
        temporal_sizes = {'temporal': config.hidden_size}
        
        self.encoder_variable_selection = VariableSelectionNetwork(
            temporal_sizes, config.hidden_size, config.dropout
        )
        
        self.decoder_variable_selection = VariableSelectionNetwork(
            temporal_sizes, config.hidden_size, config.dropout
        )
        
        # LSTM encoder/decoder
        self.lstm_encoder = nn.LSTM(
            config.hidden_size, config.hidden_size, config.lstm_layers,
            dropout=config.dropout if config.lstm_layers > 1 else 0,
            batch_first=True
        )
        
        self.lstm_decoder = nn.LSTM(
            config.hidden_size, config.hidden_size, config.lstm_layers,
            dropout=config.dropout if config.lstm_layers > 1 else 0,
            batch_first=True
        )
        
        # Gated skip connections
        self.post_lstm_gate_encoder = GatedLinearUnit(config.hidden_size, dropout=config.dropout)
        self.post_lstm_gate_decoder = GatedLinearUnit(config.hidden_size, dropout=config.dropout)
        
        # Multi-head attention
        self.multihead_attention = InterpretableMultiHeadAttention(
            config.hidden_size, config.attention_head_size, config.dropout
        )
        
        # Position-wise feed-forward
        self.position_wise_ff = GatedResidualNetwork(
            config.hidden_size, config.hidden_size, config.hidden_size, config.dropout
        )
        
        # Output layer
        self.output_projection = nn.Linear(config.hidden_size, len(config.target_features))
        
        # Layer normalization
        self.encoder_norm = nn.LayerNorm(config.hidden_size)
        self.decoder_norm = nn.LayerNorm(config.hidden_size)
        self.final_norm = nn.LayerNorm(config.hidden_size)
        
        self._init_weights()
    
    def _init_weights(self):
        """Initialize model weights."""
        for module in self.modules():
            if isinstance(module, nn.Linear):
                torch.nn.init.xavier_uniform_(module.weight)
                if module.bias is not None:
                    torch.nn.init.zeros_(module.bias)
            elif isinstance(module, nn.LSTM):
                for name, param in module.named_parameters():
                    if 'weight' in name:
                        torch.nn.init.xavier_uniform_(param)
                    elif 'bias' in name:
                        torch.nn.init.zeros_(param)
    
    def create_attention_mask(self, encoder_lengths: torch.Tensor, decoder_length: int):
        """Create causal attention mask."""
        batch_size = encoder_lengths.size(0)
        max_encoder_length = encoder_lengths.max().item()
        total_length = max_encoder_length + decoder_length
        
        mask = torch.ones(batch_size, decoder_length, total_length, device=encoder_lengths.device)
        
        # Mask out padding in encoder
        for i, enc_len in enumerate(encoder_lengths):
            if enc_len < max_encoder_length:
                mask[i, :, :max_encoder_length-enc_len] = 0
        
        # Causal mask for decoder
        for i in range(decoder_length):
            mask[:, i, max_encoder_length + i + 1:] = 0
        
        return mask
    
    def forward(
        self,
        encoder_input: torch.Tensor,
        decoder_input: torch.Tensor,
        encoder_lengths: torch.Tensor,
        sentiment_features: torch.Tensor = None
    ) -> Dict[str, torch.Tensor]:
        """
        Forward pass of the TFT model.
        
        Args:
            encoder_input: Historical data (batch, encoder_length, features)
            decoder_input: Future known features (batch, decoder_length, features)
            encoder_lengths: Actual lengths of encoder sequences
            sentiment_features: Optional sentiment features (batch, total_length, sentiment_features)
        
        Returns:
            Dictionary containing predictions and attention weights
        """
        encoder_input.size(0)
        encoder_length = encoder_input.size(1)
        decoder_length = decoder_input.size(1)
        
        # Concatenate encoder and decoder inputs
        total_input = torch.cat([encoder_input, decoder_input], dim=1)
        
        # Add sentiment features if available
        if sentiment_features is not None and self.config.use_sentiment_features:
            total_input = torch.cat([total_input, sentiment_features], dim=-1)
        
        # Project to hidden size
        embedded_input = self.temporal_projection(total_input)
        
        # Split back into encoder and decoder
        encoder_embedded = embedded_input[:, :encoder_length]
        decoder_embedded = embedded_input[:, encoder_length:]
        
        # Variable selection
        encoder_vars = {'temporal': encoder_embedded}
        decoder_vars = {'temporal': decoder_embedded}
        
        encoder_selected, encoder_weights = self.encoder_variable_selection(encoder_vars)
        decoder_selected, decoder_weights = self.decoder_variable_selection(decoder_vars)
        
        # LSTM processing
        encoder_output, (h_n, c_n) = self.lstm_encoder(encoder_selected)
        decoder_output, _ = self.lstm_decoder(decoder_selected, (h_n, c_n))
        
        # Apply gated skip connections
        encoder_gated = self.post_lstm_gate_encoder(encoder_output)
        encoder_output = self.encoder_norm(encoder_gated + encoder_selected)
        
        decoder_gated = self.post_lstm_gate_decoder(decoder_output)
        decoder_output = self.decoder_norm(decoder_gated + decoder_selected)
        
        # Combine encoder and decoder for attention
        combined_output = torch.cat([encoder_output, decoder_output], dim=1)
        
        # Multi-head attention (decoder attends to full sequence)
        attention_mask = self.create_attention_mask(encoder_lengths, decoder_length)
        
        attn_output, attention_weights = self.multihead_attention(
            decoder_output, combined_output, combined_output, attention_mask
        )
        
        # Position-wise feed-forward
        ff_output = self.position_wise_ff(attn_output)
        
        # Final residual connection and normalization
        final_output = self.final_norm(ff_output + decoder_output)
        
        # Generate predictions
        predictions = self.output_projection(final_output)
        
        return {
            'predictions': predictions,
            'attention_weights': attention_weights,
            'encoder_variable_weights': encoder_weights,
            'decoder_variable_weights': decoder_weights,
            'encoder_output': encoder_output,
            'decoder_output': final_output
        }
    
    def predict(
        self,
        historical_data: torch.Tensor,
        future_features: torch.Tensor,
        sentiment_features: torch.Tensor = None
    ) -> torch.Tensor:
        """
        Generate predictions for given inputs.
        
        Args:
            historical_data: Historical time series data
            future_features: Known future features
            sentiment_features: Optional sentiment features
        
        Returns:
            Predicted values
        """
        self.eval()
        with torch.no_grad():
            encoder_lengths = torch.full(
                (historical_data.size(0),), 
                historical_data.size(1), 
                device=historical_data.device
            )
            
            output = self.forward(
                historical_data, future_features, encoder_lengths, sentiment_features
            )
            
            return output['predictions']


class TFTTrainer:
    """Trainer class for the Temporal Fusion Transformer."""
    
    def __init__(self, model: TemporalFusionTransformer, config: TFTConfig):
        self.model = model
        self.config = config
        
        self.optimizer = Adam(model.parameters(), lr=config.learning_rate)
        self.scheduler = ReduceLROnPlateau(
            self.optimizer, mode='min', factor=0.5, patience=config.patience
        )
        
        self.criterion = nn.MSELoss()
        self.device = next(model.parameters()).device
        
        # Training history
        self.train_losses = []
        self.val_losses = []
        self.best_val_loss = float('inf')
        self.epochs_without_improvement = 0
    
    def train_epoch(self, train_loader):
        """Train for one epoch."""
        self.model.train()
        total_loss = 0
        num_batches = 0
        
        for batch in train_loader:
            self.optimizer.zero_grad()
            
            # Extract batch data
            encoder_input = batch['encoder_input'].to(self.device)
            decoder_input = batch['decoder_input'].to(self.device)
            encoder_lengths = batch['encoder_lengths'].to(self.device)
            targets = batch['targets'].to(self.device)
            sentiment_features = batch.get('sentiment_features')
            
            if sentiment_features is not None:
                sentiment_features = sentiment_features.to(self.device)
            
            # Forward pass
            outputs = self.model(
                encoder_input, decoder_input, encoder_lengths, sentiment_features
            )
            
            # Calculate loss
            loss = self.criterion(outputs['predictions'], targets)
            
            # Backward pass
            loss.backward()
            torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
            self.optimizer.step()
            
            total_loss += loss.item()
            num_batches += 1
        
        return total_loss / num_batches
    
    def validate(self, val_loader):
        """Validate the model."""
        self.model.eval()
        total_loss = 0
        num_batches = 0
        
        with torch.no_grad():
            for batch in val_loader:
                encoder_input = batch['encoder_input'].to(self.device)
                decoder_input = batch['decoder_input'].to(self.device)
                encoder_lengths = batch['encoder_lengths'].to(self.device)
                targets = batch['targets'].to(self.device)
                sentiment_features = batch.get('sentiment_features')
                
                if sentiment_features is not None:
                    sentiment_features = sentiment_features.to(self.device)
                
                outputs = self.model(
                    encoder_input, decoder_input, encoder_lengths, sentiment_features
                )
                
                loss = self.criterion(outputs['predictions'], targets)
                total_loss += loss.item()
                num_batches += 1
        
        return total_loss / num_batches
    
    def train(self, train_loader, val_loader):
        """Train the model."""
        logger.info(f"Starting TFT training for {self.config.max_epochs} epochs")
        
        for epoch in range(self.config.max_epochs):
            # Train
            train_loss = self.train_epoch(train_loader)
            self.train_losses.append(train_loss)
            
            # Validate
            val_loss = self.validate(val_loader)
            self.val_losses.append(val_loss)
            
            # Update learning rate
            self.scheduler.step(val_loss)
            
            # Early stopping check
            if val_loss < self.best_val_loss:
                self.best_val_loss = val_loss
                self.epochs_without_improvement = 0
                # Save best model
                torch.save(self.model.state_dict(), 'best_tft_model.pt')
            else:
                self.epochs_without_improvement += 1
            
            logger.info(
                f"Epoch {epoch+1}/{self.config.max_epochs}: "
                f"Train Loss: {train_loss:.6f}, Val Loss: {val_loss:.6f}, "
                f"LR: {self.optimizer.param_groups[0]['lr']:.2e}"
            )
            
            # Early stopping
            if self.epochs_without_improvement >= self.config.patience:
                logger.info(f"Early stopping after {epoch+1} epochs")
                break
        
        # Load best model
        self.model.load_state_dict(torch.load('best_tft_model.pt'))
        logger.info("Training completed. Best model loaded.")
    
    def save_model(self, path: str):
        """Save the trained model."""
        torch.save({
            'model_state_dict': self.model.state_dict(),
            'config': self.config,
            'train_losses': self.train_losses,
            'val_losses': self.val_losses,
            'best_val_loss': self.best_val_loss
        }, path)
    
    @classmethod
    def load_model(cls, path: str, device='cpu'):
        """Load a trained model."""
        checkpoint = torch.load(path, map_location=device)
        config = checkpoint['config']
        
        model = TemporalFusionTransformer(config).to(device)
        model.load_state_dict(checkpoint['model_state_dict'])
        
        trainer = cls(model, config)
        trainer.train_losses = checkpoint['train_losses']
        trainer.val_losses = checkpoint['val_losses']
        trainer.best_val_loss = checkpoint['best_val_loss']
        
        return trainer


def create_tft_model(
    temporal_features: List[str],
    target_features: List[str],
    hidden_size: int = 64,
    max_encoder_length: int = 120,
    max_prediction_length: int = 30,
    use_sentiment: bool = True
) -> TemporalFusionTransformer:
    """
    Create a TFT model with the specified configuration.
    
    Args:
        temporal_features: List of temporal feature names
        target_features: List of target feature names
        hidden_size: Hidden size of the model
        max_encoder_length: Maximum encoder sequence length
        max_prediction_length: Maximum prediction horizon
        use_sentiment: Whether to use sentiment features
    
    Returns:
        Configured TFT model
    """
    config = TFTConfig(
        hidden_size=hidden_size,
        max_encoder_length=max_encoder_length,
        max_prediction_length=max_prediction_length,
        temporal_features=temporal_features,
        target_features=target_features,
        use_sentiment_features=use_sentiment
    )
    
    return TemporalFusionTransformer(config)