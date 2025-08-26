# Multi-Modal Model Architecture Design

**Project**: News-Driven Multi-Modal Prediction System  
**Document Type**: Technical Architecture  
**Author**: ATS Platform Team  
**Date**: 2025-08-26  
**Version**: 1.0  

## 🎯 Executive Summary

This document defines the architecture for a multi-modal trading model that combines:
- **News sentiment and economic events** (textual/temporal features)
- **Market signals** (OHLC, volume, technical indicators) 
- **Factor exposures** (residual returns, cross-sectional factors)
- **Cross-timeframe patterns** (5min to daily alignment)

The design extends the existing ATS model infrastructure with news capabilities while maintaining compatibility with current feature systems.

## 🏗️ Overall Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Multi-Modal Prediction System                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │   News      │  │   Market    │  │   Factor    │  │Cross-Frame  │ │
│  │  Encoder    │  │  Encoder    │  │  Encoder    │  │  Encoder    │ │
│  │             │  │             │  │             │  │             │ │
│  │ • Sentiment │  │ • OHLC      │  │ • Residual  │  │ • 5min→1hr  │ │
│  │ • Events    │  │ • Volume    │  │   Returns   │  │ • Daily→5min│ │
│  │ • Topics    │  │ • Tech Ind. │  │ • Loadings  │  │ • Alignment │ │
│  │ • Timing    │  │ • Price Mom │  │ • Sectors   │  │ • Patterns  │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │
│         │                │                │                │        │
│         ▼                ▼                ▼                ▼        │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │              Multi-Modal Fusion Layer                           │ │
│  │                                                                 │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │ │
│  │  │Cross-Modal  │  │ Attention   │  │    Feature Alignment    │  │ │
│  │  │ Attention   │  │  Weights    │  │    & Synchronization    │  │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────────┘  │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                │                                     │
│                                ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                 Multi-Task Prediction Heads                     │ │
│  │                                                                 │ │
│  │ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │ │
│  │ │  Return     │ │ Volatility  │ │ Direction   │ │Risk-Adjusted│ │ │
│  │ │ Prediction  │ │ Prediction  │ │Classification│ │   Returns   │ │ │
│  │ │             │ │             │ │             │ │             │ │ │
│  │ │ • 1d, 5d,   │ │ • Realized  │ │ • Up/Down/  │ │ • Sharpe    │ │ │
│  │ │   10d, 20d  │ │   Vol       │ │   Sideways  │ │ • Max DD    │ │ │
│  │ │ • Residual  │ │ • VIX-adj   │ │ • Magnitude │ │ • Info Ratio│ │ │
│  │ │ • Total     │ │ • Regime    │ │ • Timing    │ │ • Tracking  │ │ │
│  │ └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

## 📊 Component-Level Architecture

### 1. News Encoder Architecture

**Extends**: Existing FinBERT sentiment analysis  
**Integration**: New feature type in `enhanced_feature_types.py`

```python
class NewsFeatureType(Enum):
    NEWS_SENTIMENT_INTERVALS = "news_sentiment_intervals"      # [time_steps, sentiment_dims]
    ECONOMIC_EVENT_SEQUENCES = "economic_event_sequences"      # [time_steps, event_features] 
    NEWS_TOPIC_EMBEDDINGS = "news_topic_embeddings"           # [time_steps, topic_dims]
    NEWS_VOLUME_INDICATORS = "news_volume_indicators"         # [time_steps, volume_features]

@dataclass
class NewsFeatureSpecification(FeatureSpecification):
    """News-specific feature specification"""
    sentiment_model: str = "ProsusAI/finbert"
    aggregation_window: int = 1440  # minutes (24 hours)
    event_categories: List[str] = None
    topic_extraction: bool = True
    cross_asset_impact: bool = True
```

**News Encoder Implementation**:
```python
class NewsEncoder(nn.Module):
    """Multi-head news feature encoder"""
    
    def __init__(self, config: NewsEncoderConfig):
        super().__init__()
        
        # Sentiment encoding
        self.sentiment_encoder = nn.LSTM(
            input_size=config.sentiment_dims,
            hidden_size=config.hidden_size,
            num_layers=2,
            batch_first=True,
            dropout=0.2
        )
        
        # Event sequence encoder
        self.event_encoder = nn.TransformerEncoder(
            nn.TransformerEncoderLayer(
                d_model=config.event_dims,
                nhead=8,
                dim_feedforward=config.hidden_size,
                dropout=0.1
            ),
            num_layers=3
        )
        
        # Topic/theme encoder
        self.topic_encoder = nn.Linear(config.topic_dims, config.hidden_size)
        
        # Cross-asset impact encoder
        self.impact_encoder = nn.MultiheadAttention(
            embed_dim=config.hidden_size,
            num_heads=4,
            dropout=0.1
        )
        
        # Feature fusion
        self.fusion_layer = nn.Sequential(
            nn.Linear(config.hidden_size * 3, config.output_dims),
            nn.LayerNorm(config.output_dims),
            nn.GELU(),
            nn.Dropout(0.2)
        )
    
    def forward(self, news_features):
        # Process sentiment sequences
        sentiment_out, _ = self.sentiment_encoder(news_features['sentiment'])
        
        # Process event sequences  
        event_out = self.event_encoder(news_features['events'])
        
        # Process topic embeddings
        topic_out = self.topic_encoder(news_features['topics'])
        
        # Cross-asset impact attention
        impact_out, _ = self.impact_encoder(
            sentiment_out, event_out, topic_out
        )
        
        # Fuse all news features
        combined = torch.cat([
            sentiment_out[:, -1, :],  # Last sentiment state
            event_out[:, -1, :],      # Last event state  
            impact_out[:, -1, :]      # Last impact state
        ], dim=-1)
        
        return self.fusion_layer(combined)
```

### 2. Market Encoder Architecture

**Extends**: Existing OHLC and technical indicator features  
**Reuses**: `FeatureType.OHLC_INTERVALS`, `FeatureType.PRICE_INDICATOR_INTERVALS`

```python
class MarketEncoder(nn.Module):
    """Market data encoder using existing feature types"""
    
    def __init__(self, config: MarketEncoderConfig, feature_registry: EnhancedFeatureRegistry):
        super().__init__()
        self.feature_registry = feature_registry
        
        # OHLC sequence encoder (reuse existing patterns)
        self.ohlc_encoder = nn.LSTM(
            input_size=4,  # OHLC
            hidden_size=config.hidden_size,
            num_layers=2,
            batch_first=True,
            dropout=0.2
        )
        
        # Technical indicator encoder
        self.technical_encoder = nn.Sequential(
            nn.Linear(config.technical_dims, config.hidden_size),
            nn.LayerNorm(config.hidden_size),
            nn.ReLU(),
            nn.Dropout(0.1)
        )
        
        # Volume pattern encoder
        self.volume_encoder = nn.Conv1d(
            in_channels=1,
            out_channels=config.hidden_size // 4,
            kernel_size=3,
            padding=1
        )
        
        # Feature combination
        self.combiner = nn.Sequential(
            nn.Linear(config.hidden_size * 2, config.output_dims),
            nn.LayerNorm(config.output_dims),
            nn.GELU()
        )
    
    def forward(self, market_features):
        # Encode OHLC sequences
        ohlc_out, _ = self.ohlc_encoder(market_features['ohlc'])
        
        # Encode technical indicators
        tech_out = self.technical_encoder(market_features['technical'])
        
        # Encode volume patterns
        volume_out = self.volume_encoder(
            market_features['volume'].unsqueeze(1)
        ).mean(dim=-1)  # Global average pooling
        
        # Combine market features
        combined = torch.cat([
            ohlc_out[:, -1, :],  # Last OHLC state
            tech_out,            # Technical indicators
        ], dim=-1)
        
        return self.combiner(combined)
```

### 3. Factor Encoder Architecture

**Integrates**: Existing factor models from `factor_models.py`  
**Extends**: Residual return calculation with news impact

```python
class FactorEncoder(nn.Module):
    """Factor model encoder integrating with existing factor calculations"""
    
    def __init__(self, config: FactorEncoderConfig):
        super().__init__()
        
        # Residual return encoder
        self.residual_encoder = nn.Linear(
            config.factor_dims,  # Market, size, value, momentum, quality, volatility
            config.hidden_size
        )
        
        # Factor loading encoder
        self.loading_encoder = nn.Sequential(
            nn.Linear(config.factor_dims, config.hidden_size),
            nn.BatchNorm1d(config.hidden_size),
            nn.ReLU(),
            nn.Dropout(0.1)
        )
        
        # Sector exposure encoder
        self.sector_encoder = nn.Embedding(
            num_embeddings=11,  # 11 GICS sectors
            embedding_dim=config.hidden_size // 4
        )
        
        # Factor interaction attention
        self.factor_attention = nn.MultiheadAttention(
            embed_dim=config.hidden_size,
            num_heads=6,  # One head per factor
            dropout=0.1
        )
        
        # Output projection
        self.output_proj = nn.Linear(config.hidden_size, config.output_dims)
    
    def forward(self, factor_features):
        # Encode residual returns and loadings
        residual_enc = self.residual_encoder(factor_features['residuals'])
        loading_enc = self.loading_encoder(factor_features['loadings'])
        
        # Encode sector exposure
        sector_enc = self.sector_encoder(factor_features['sector_ids'])
        
        # Factor interaction attention
        factor_stack = torch.stack([residual_enc, loading_enc], dim=1)
        attended, _ = self.factor_attention(
            factor_stack, factor_stack, factor_stack
        )
        
        # Combine with sector information
        combined = attended.mean(dim=1) + sector_enc
        
        return self.output_proj(combined)
```

### 4. Cross-Timeframe Encoder Architecture

**Extends**: Existing cross-timeframe alignment from `enhanced_feature_types.py`  
**Reuses**: `FeatureType.CROSS_TIMEFRAME_INDICATORS`

```python
class CrossTimeframeEncoder(nn.Module):
    """Cross-timeframe pattern encoder"""
    
    def __init__(self, config: CrossTimeframeConfig):
        super().__init__()
        
        # Multi-scale temporal convolutions
        self.temporal_convs = nn.ModuleDict({
            '5min': nn.Conv1d(config.input_dims, config.hidden_size, kernel_size=3, dilation=1),
            '15min': nn.Conv1d(config.input_dims, config.hidden_size, kernel_size=3, dilation=3),
            '1hour': nn.Conv1d(config.input_dims, config.hidden_size, kernel_size=3, dilation=12),
            'daily': nn.Conv1d(config.input_dims, config.hidden_size, kernel_size=3, dilation=288)
        })
        
        # Timeframe alignment attention
        self.alignment_attention = nn.MultiheadAttention(
            embed_dim=config.hidden_size,
            num_heads=4,
            dropout=0.1
        )
        
        # Temporal pattern recognition
        self.pattern_lstm = nn.LSTM(
            input_size=config.hidden_size,
            hidden_size=config.hidden_size,
            num_layers=2,
            batch_first=True,
            bidirectional=True
        )
        
        # Output projection
        self.output_proj = nn.Linear(config.hidden_size * 2, config.output_dims)
    
    def forward(self, cross_timeframe_features):
        # Multi-scale temporal convolutions
        timeframe_features = []
        for timeframe, conv in self.temporal_convs.items():
            if timeframe in cross_timeframe_features:
                # Apply temporal convolution
                conv_out = conv(cross_timeframe_features[timeframe].transpose(1, 2))
                conv_out = F.relu(conv_out).transpose(1, 2)
                timeframe_features.append(conv_out)
        
        # Align timeframes using attention
        if len(timeframe_features) > 1:
            aligned_stack = torch.stack(timeframe_features, dim=1)
            aligned, _ = self.alignment_attention(
                aligned_stack, aligned_stack, aligned_stack
            )
            aligned_features = aligned.mean(dim=1)
        else:
            aligned_features = timeframe_features[0]
        
        # Temporal pattern recognition
        pattern_out, _ = self.pattern_lstm(aligned_features)
        
        return self.output_proj(pattern_out[:, -1, :])
```

### 5. Multi-Modal Fusion Layer

**Core Innovation**: Cross-modal attention mechanism

```python
class MultiModalFusionLayer(nn.Module):
    """Advanced fusion of all modal encoders"""
    
    def __init__(self, config: FusionConfig):
        super().__init__()
        
        # Modal dimension alignment
        self.modal_projections = nn.ModuleDict({
            'news': nn.Linear(config.news_dims, config.fusion_dims),
            'market': nn.Linear(config.market_dims, config.fusion_dims),
            'factor': nn.Linear(config.factor_dims, config.fusion_dims),
            'cross_tf': nn.Linear(config.cross_tf_dims, config.fusion_dims)
        })
        
        # Cross-modal attention layers
        self.cross_attention_layers = nn.ModuleList([
            nn.MultiheadAttention(
                embed_dim=config.fusion_dims,
                num_heads=8,
                dropout=0.1
            ) for _ in range(config.num_attention_layers)
        ])
        
        # Modal importance weighting
        self.modal_weights = nn.Sequential(
            nn.Linear(config.fusion_dims * 4, config.fusion_dims),
            nn.ReLU(),
            nn.Linear(config.fusion_dims, 4),  # 4 modalities
            nn.Softmax(dim=-1)
        )
        
        # Feature synchronization
        self.sync_layer = nn.LayerNorm(config.fusion_dims)
        
        # Final fusion
        self.fusion_net = nn.Sequential(
            nn.Linear(config.fusion_dims * 4, config.fusion_dims * 2),
            nn.LayerNorm(config.fusion_dims * 2),
            nn.GELU(),
            nn.Dropout(0.2),
            nn.Linear(config.fusion_dims * 2, config.output_dims),
            nn.LayerNorm(config.output_dims)
        )
    
    def forward(self, modal_features):
        # Align modal dimensions
        aligned_modals = {}
        for modal_name, features in modal_features.items():
            if modal_name in self.modal_projections:
                aligned_modals[modal_name] = self.modal_projections[modal_name](features)
        
        # Stack modalities for cross-attention
        modal_stack = torch.stack(list(aligned_modals.values()), dim=1)
        
        # Apply cross-attention layers
        attended = modal_stack
        for attention_layer in self.cross_attention_layers:
            attended, _ = attention_layer(attended, attended, attended)
        
        # Calculate modal importance weights
        modal_concat = torch.cat(list(aligned_modals.values()), dim=-1)
        importance_weights = self.modal_weights(modal_concat)
        
        # Weight and combine modalities
        weighted_modals = []
        for i, (modal_name, features) in enumerate(aligned_modals.items()):
            weighted = features * importance_weights[:, i:i+1]
            weighted_modals.append(self.sync_layer(weighted))
        
        # Final fusion
        fused_features = torch.cat(weighted_modals, dim=-1)
        return self.fusion_net(fused_features)
```

### 6. Multi-Task Prediction Heads

**Extends**: Existing support/resistance multi-output pattern  
**Integrates**: Financial risk metrics

```python
class MultiTaskPredictionHeads(nn.Module):
    """Multiple prediction heads for different financial metrics"""
    
    def __init__(self, config: PredictionConfig):
        super().__init__()
        
        # Return prediction heads (multiple horizons)
        self.return_heads = nn.ModuleDict({
            f'{horizon}d': nn.Sequential(
                nn.Linear(config.input_dims, config.hidden_size),
                nn.ReLU(),
                nn.Dropout(0.1),
                nn.Linear(config.hidden_size, 1)  # Single return value
            ) for horizon in [1, 5, 10, 20]
        })
        
        # Volatility prediction heads
        self.volatility_heads = nn.ModuleDict({
            'realized_5d': nn.Linear(config.input_dims, 1),
            'realized_20d': nn.Linear(config.input_dims, 1),
            'vix_adjusted': nn.Linear(config.input_dims, 1),
            'regime_vol': nn.Linear(config.input_dims, 3)  # Low/Med/High
        })
        
        # Direction classification heads
        self.direction_heads = nn.ModuleDict({
            f'{horizon}d': nn.Sequential(
                nn.Linear(config.input_dims, config.hidden_size),
                nn.ReLU(),
                nn.Linear(config.hidden_size, 3)  # Up/Down/Sideways
            ) for horizon in [1, 5, 10, 20]
        })
        
        # Risk-adjusted return heads
        self.risk_heads = nn.ModuleDict({
            'sharpe_ratio': nn.Linear(config.input_dims, 1),
            'max_drawdown': nn.Linear(config.input_dims, 1),
            'information_ratio': nn.Linear(config.input_dims, 1),
            'tracking_error': nn.Linear(config.input_dims, 1)
        })
        
        # Confidence/uncertainty heads
        self.confidence_heads = nn.ModuleDict({
            'prediction_confidence': nn.Sequential(
                nn.Linear(config.input_dims, config.hidden_size),
                nn.ReLU(),
                nn.Linear(config.hidden_size, 1),
                nn.Sigmoid()  # 0-1 confidence
            ),
            'epistemic_uncertainty': nn.Linear(config.input_dims, 1),
            'aleatoric_uncertainty': nn.Linear(config.input_dims, 1)
        })
    
    def forward(self, fused_features):
        predictions = {}
        
        # Return predictions
        predictions['returns'] = {
            horizon: head(fused_features).squeeze(-1)
            for horizon, head in self.return_heads.items()
        }
        
        # Volatility predictions
        predictions['volatility'] = {
            vol_type: head(fused_features).squeeze(-1)
            for vol_type, head in self.volatility_heads.items()
        }
        
        # Direction predictions
        predictions['direction'] = {
            horizon: F.softmax(head(fused_features), dim=-1)
            for horizon, head in self.direction_heads.items()
        }
        
        # Risk-adjusted predictions
        predictions['risk_metrics'] = {
            metric: head(fused_features).squeeze(-1)
            for metric, head in self.risk_heads.items()
        }
        
        # Confidence predictions
        predictions['confidence'] = {
            conf_type: head(fused_features).squeeze(-1)
            for conf_type, head in self.confidence_heads.items()
        }
        
        return predictions
```

## 📈 Complete Multi-Modal Model

```python
@gin.configurable
class NewsAwareMultiModalPredictor(nn.Module):
    """Complete multi-modal trading prediction model"""
    
    def __init__(self, config: MultiModalConfig, feature_registry: EnhancedFeatureRegistry):
        super().__init__()
        
        self.config = config
        self.feature_registry = feature_registry
        
        # Modal encoders
        self.news_encoder = NewsEncoder(config.news_encoder)
        self.market_encoder = MarketEncoder(config.market_encoder, feature_registry)
        self.factor_encoder = FactorEncoder(config.factor_encoder)
        self.cross_tf_encoder = CrossTimeframeEncoder(config.cross_tf_encoder)
        
        # Fusion layer
        self.fusion_layer = MultiModalFusionLayer(config.fusion)
        
        # Prediction heads
        self.prediction_heads = MultiTaskPredictionHeads(config.prediction)
        
        # Loss function
        self.criterion = MultiModalLoss(config.loss)
    
    def forward(self, batch):
        # Extract modal features
        modal_features = {}
        
        # News features
        if 'news' in batch:
            modal_features['news'] = self.news_encoder(batch['news'])
        
        # Market features  
        if 'market' in batch:
            modal_features['market'] = self.market_encoder(batch['market'])
        
        # Factor features
        if 'factors' in batch:
            modal_features['factor'] = self.factor_encoder(batch['factors'])
        
        # Cross-timeframe features
        if 'cross_timeframe' in batch:
            modal_features['cross_tf'] = self.cross_tf_encoder(batch['cross_timeframe'])
        
        # Fuse all modalities
        fused_features = self.fusion_layer(modal_features)
        
        # Generate predictions
        predictions = self.prediction_heads(fused_features)
        
        return predictions
    
    def compute_loss(self, predictions, targets):
        """Compute multi-task loss"""
        return self.criterion(predictions, targets)
```

## 🔄 Integration with Existing Infrastructure

### Feature Registry Extensions

```python
# Add to enhanced_feature_types.py
class NewsFeatureSpecification(FeatureSpecification):
    """News-specific feature specification extending existing system"""
    
    news_sources: List[str] = field(default_factory=lambda: ['polygon', 'tiingo', 'alpha_vantage'])
    sentiment_model: str = "ProsusAI/finbert"
    event_categories: List[str] = field(default_factory=lambda: ['earnings', 'fed', 'macro'])
    aggregation_windows: List[int] = field(default_factory=lambda: [60, 360, 1440])  # 1hr, 6hr, 24hr
    
    def create_visualization_metadata(self) -> VisualizationMetadata:
        return VisualizationMetadata(
            type="news_sentiment_overlay",
            color="#FF6B35",
            opacity=0.7,
            y_axis="sentiment",
            layer=4
        )

# Register news features in enhanced_feature_registry.py
def _register_news_features(self):
    """Register news-specific features"""
    for timeframe in [TimeframeSpec.MINUTE_15, TimeframeSpec.HOUR_1, TimeframeSpec.DAILY]:
        for window in [8, 16, 24]:
            # Sentiment features
            sentiment_spec = NewsFeatureSpecification(
                name=f"news_sentiment_{timeframe.label}_{window}",
                feature_type=FeatureType.NEWS_SENTIMENT_INTERVALS,
                timeframe=timeframe,
                intervals=window,
                dimensions=(window, 4),  # [sentiment_score, confidence, volume, momentum]
                description=f"News sentiment aggregated over {window} {timeframe.label} intervals"
            )
            self.register_feature(sentiment_spec)
            
            # Economic events features  
            events_spec = NewsFeatureSpecification(
                name=f"economic_events_{timeframe.label}_{window}",
                feature_type=FeatureType.ECONOMIC_EVENT_SEQUENCES,
                timeframe=timeframe,
                intervals=window,
                dimensions=(window, 8),  # [impact_score, category_encoding, confidence, etc.]
                description=f"Economic events impact over {window} {timeframe.label} intervals"
            )
            self.register_feature(events_spec)
```

### Training Data Generator Integration

```python
# Extend enhanced_training_data_generator.py
class NewsAwareFeatureGenerator(MultiModalFeatureGenerator):
    """News-aware feature generator extending existing system"""
    
    async def generate_news_features(self, symbol: str, sample_date: date, 
                                   feature_specs: List[NewsFeatureSpecification]) -> Dict[str, np.ndarray]:
        """Generate news features compatible with existing training pipeline"""
        
        news_features = {}
        
        for spec in feature_specs:
            if spec.feature_type == FeatureType.NEWS_SENTIMENT_INTERVALS:
                # Get news sentiment data
                sentiment_data = await self._fetch_sentiment_intervals(
                    symbol, sample_date, spec.timeframe, spec.intervals
                )
                news_features[spec.name] = sentiment_data
                
            elif spec.feature_type == FeatureType.ECONOMIC_EVENT_SEQUENCES:
                # Get economic events data
                events_data = await self._fetch_economic_events(
                    symbol, sample_date, spec.timeframe, spec.intervals
                )
                news_features[spec.name] = events_data
        
        return news_features
    
    async def _fetch_sentiment_intervals(self, symbol: str, date: date, 
                                       timeframe: TimeframeSpec, intervals: int) -> np.ndarray:
        """Fetch sentiment data aligned to timeframe intervals"""
        # Integration with existing news tables
        async with self.db_pool.acquire() as conn:
            query = """
            WITH aligned_sentiment AS (
                SELECT 
                    date_trunc($3, published_date) as interval_start,
                    AVG(overall_sentiment_score) as avg_sentiment,
                    COUNT(*) as news_volume,
                    STDDEV(overall_sentiment_score) as sentiment_volatility,
                    AVG(overall_sentiment_score) - LAG(AVG(overall_sentiment_score)) 
                        OVER (ORDER BY date_trunc($3, published_date)) as sentiment_momentum
                FROM dev_news_alpha_vantage 
                WHERE $1 = ANY(tickers)
                AND published_date <= $2
                AND published_date > $2 - INTERVAL '%s minutes'
                GROUP BY date_trunc($3, published_date)
                ORDER BY interval_start DESC
                LIMIT $4
            )
            SELECT 
                COALESCE(avg_sentiment, 0) as sentiment,
                COALESCE(sentiment_volatility, 0) as confidence,
                COALESCE(news_volume, 0) as volume,
                COALESCE(sentiment_momentum, 0) as momentum
            FROM aligned_sentiment
            """ % (timeframe.multiplier * 5)  # Convert to minutes
            
            rows = await conn.fetch(query, symbol, date, timeframe.label, intervals)
            
            # Convert to numpy array
            if rows:
                return np.array([[
                    float(row['sentiment']),
                    float(row['confidence']), 
                    float(row['volume']),
                    float(row['momentum'])
                ] for row in rows])
            else:
                return np.zeros((intervals, 4))
```

## 🎯 Training and Deployment Integration

### Loss Function Design

```python
class MultiModalLoss(nn.Module):
    """Multi-task loss function for multi-modal model"""
    
    def __init__(self, config: LossConfig):
        super().__init__()
        self.config = config
        
        # Task-specific loss functions
        self.return_loss = nn.HuberLoss(delta=0.1)  # Robust to outliers
        self.direction_loss = nn.CrossEntropyLoss()
        self.volatility_loss = nn.MSELoss()
        self.confidence_loss = nn.BCELoss()
        
        # Loss weights (learnable)
        self.task_weights = nn.Parameter(torch.ones(4))
        
    def forward(self, predictions, targets):
        losses = {}
        total_loss = 0.0
        
        # Return prediction losses
        return_losses = []
        for horizon in [1, 5, 10, 20]:
            if f'{horizon}d' in predictions['returns'] and f'return_{horizon}d' in targets:
                loss = self.return_loss(
                    predictions['returns'][f'{horizon}d'],
                    targets[f'return_{horizon}d']
                )
                return_losses.append(loss)
                losses[f'return_{horizon}d'] = loss
        
        if return_losses:
            total_loss += self.task_weights[0] * torch.stack(return_losses).mean()
        
        # Direction prediction losses
        direction_losses = []
        for horizon in [1, 5, 10, 20]:
            if f'{horizon}d' in predictions['direction'] and f'direction_{horizon}d' in targets:
                loss = self.direction_loss(
                    predictions['direction'][f'{horizon}d'],
                    targets[f'direction_{horizon}d']
                )
                direction_losses.append(loss)
                losses[f'direction_{horizon}d'] = loss
        
        if direction_losses:
            total_loss += self.task_weights[1] * torch.stack(direction_losses).mean()
        
        # Volatility prediction losses
        vol_losses = []
        for vol_type in ['realized_5d', 'realized_20d']:
            if vol_type in predictions['volatility'] and f'vol_{vol_type}' in targets:
                loss = self.volatility_loss(
                    predictions['volatility'][vol_type],
                    targets[f'vol_{vol_type}']
                )
                vol_losses.append(loss)
                losses[f'vol_{vol_type}'] = loss
        
        if vol_losses:
            total_loss += self.task_weights[2] * torch.stack(vol_losses).mean()
        
        # Confidence/uncertainty losses
        if 'prediction_confidence' in predictions['confidence'] and 'confidence_target' in targets:
            conf_loss = self.confidence_loss(
                predictions['confidence']['prediction_confidence'],
                targets['confidence_target']
            )
            losses['confidence'] = conf_loss
            total_loss += self.task_weights[3] * conf_loss
        
        # Regularization on task weights
        weight_reg = torch.sum(torch.abs(self.task_weights - 1.0))
        losses['weight_regularization'] = weight_reg
        total_loss += 0.01 * weight_reg
        
        return total_loss, losses
```

### Kubernetes Deployment

```yaml
# k8s/multimodal-training-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: multimodal-model-training
  namespace: ats-dev
spec:
  template:
    spec:
      containers:
      - name: multimodal-trainer
        image: dragonflyer762/ats-genai:latest
        command: ["/bin/bash", "-c"]
        args:
        - |
          cd /app
          
          # Install additional ML dependencies
          pip install transformers torch torchvision pytorch-lightning wandb
          
          # Train multimodal model
          PYTHONPATH=/app/src python /app/src/ml/models/train_multimodal_model.py \
            --config config/multimodal_config.gin \
            --symbols AAPL,MSFT,GOOGL,AMZN,TSLA \
            --start_date 2023-01-01 \
            --end_date 2024-12-31 \
            --output_dir /app/models/multimodal \
            --use_news_features \
            --use_cross_timeframe \
            --prediction_horizons 1,5,10,20 \
            --validation_split 0.2 \
            --batch_size 64 \
            --epochs 100 \
            --early_stopping_patience 15
        
        resources:
          requests:
            memory: "8Gi"
            cpu: "2000m"
            nvidia.com/gpu: "1"
          limits:
            memory: "16Gi"
            cpu: "4000m"
            nvidia.com/gpu: "1"
        
        volumeMounts:
        - name: model-storage
          mountPath: /app/models
      
      volumes:
      - name: model-storage
        persistentVolumeClaim:
          claimName: multimodal-model-pvc
```

## 📊 Performance Monitoring

### Model Evaluation Metrics

```python
class MultiModalEvaluator:
    """Comprehensive evaluation for multi-modal models"""
    
    def evaluate_predictions(self, predictions, targets, metadata):
        """Evaluate all prediction tasks"""
        
        metrics = {}
        
        # Return prediction metrics
        for horizon in [1, 5, 10, 20]:
            pred_key = f'returns.{horizon}d'
            target_key = f'return_{horizon}d'
            
            if pred_key in predictions and target_key in targets:
                pred_returns = predictions[pred_key]
                actual_returns = targets[target_key]
                
                metrics[f'return_{horizon}d_mae'] = np.mean(np.abs(pred_returns - actual_returns))
                metrics[f'return_{horizon}d_mse'] = np.mean((pred_returns - actual_returns)**2)
                metrics[f'return_{horizon}d_corr'] = np.corrcoef(pred_returns, actual_returns)[0,1]
                
                # Financial metrics
                metrics[f'return_{horizon}d_hit_rate'] = np.mean(
                    np.sign(pred_returns) == np.sign(actual_returns)
                )
                
                # Information Ratio
                tracking_error = np.std(pred_returns - actual_returns)
                if tracking_error > 0:
                    metrics[f'return_{horizon}d_ir'] = np.mean(pred_returns - actual_returns) / tracking_error
        
        # Direction accuracy
        for horizon in [1, 5, 10, 20]:
            pred_key = f'direction.{horizon}d'
            target_key = f'direction_{horizon}d'
            
            if pred_key in predictions and target_key in targets:
                pred_dir = np.argmax(predictions[pred_key], axis=1)
                actual_dir = targets[target_key]
                
                metrics[f'direction_{horizon}d_accuracy'] = np.mean(pred_dir == actual_dir)
                
                # Class-specific metrics
                from sklearn.metrics import classification_report
                report = classification_report(actual_dir, pred_dir, output_dict=True)
                metrics[f'direction_{horizon}d_f1'] = report['macro avg']['f1-score']
        
        # Risk-adjusted metrics
        if 'risk_metrics.sharpe_ratio' in predictions and 'sharpe_target' in targets:
            pred_sharpe = predictions['risk_metrics.sharpe_ratio']
            actual_sharpe = targets['sharpe_target']
            
            metrics['sharpe_mae'] = np.mean(np.abs(pred_sharpe - actual_sharpe))
            metrics['sharpe_corr'] = np.corrcoef(pred_sharpe, actual_sharpe)[0,1]
        
        return metrics
```

This multi-modal architecture design:

1. **Extends existing infrastructure** - Builds on current feature types, training generators, and model patterns
2. **Integrates news seamlessly** - Adds news features as first-class citizens in the feature registry
3. **Maintains compatibility** - Works with existing OHLC, technical indicators, and factor models
4. **Provides rich predictions** - Multiple horizons, risk metrics, confidence scores
5. **Follows ATS patterns** - Uses Kubernetes, database integration, and existing deployment patterns

<function_calls>
<invoke name="TodoWrite">
<parameter name="todos">[{"content": "Research existing ATS data infrastructure patterns for news integration", "status": "completed", "id": "1"}, {"content": "Design news database schema for economic events and company news", "status": "completed", "id": "2"}, {"content": "Create PRD for News Population and Multi-Modal Prediction project", "status": "completed", "id": "3"}, {"content": "Design news data sources architecture (APIs, feeds, etc.)", "status": "completed", "id": "4"}, {"content": "Create DRD for technical implementation details", "status": "completed", "id": "5"}, {"content": "Implement news population infrastructure following K8s patterns", "status": "completed", "id": "6"}, {"content": "Design multi-modal model architecture combining news + market signals", "status": "completed", "id": "7"}, {"content": "Create integration tests for news population pipeline", "status": "pending", "id": "8"}]