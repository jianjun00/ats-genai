# DRD: News-Driven Multi-Modal Prediction System - Technical Design

**Project**: News Population and Multi-Modal Trading Signal Generation  
**Document Type**: Detailed Requirements Document (DRD)  
**Author**: ATS Platform Team  
**Date**: 2025-08-26  
**Version**: 1.0  

## 🏗️ System Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   News Sources  │    │  Economic Data  │    │  Social Feeds   │
│                 │    │                 │    │                 │
│ • Polygon API   │    │ • FRED API      │    │ • Twitter API   │  
│ • Tiingo API    │    │ • BLS API       │    │ • Reddit API    │
│ • Alpha Vantage │    │ • Census API    │    │ • StockTwits    │
│ • FMP API       │    │ • Fed API       │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Real-Time Ingestion Pipeline                │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │ News Collector  │  │ Event Detector  │  │ Sentiment       │ │
│  │                 │  │                 │  │ Analyzer        │ │
│  │ • Rate limiting │  │ • Event         │  │                 │ │
│  │ • Deduplication │  │   classification│  │ • FinBERT       │ │
│  │ • Content       │  │ • Impact        │  │ • VADER         │ │
│  │   extraction    │  │   scoring       │  │ • Ensemble      │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Enhanced Database Layer                     │
│                                                                 │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │
│ │news_polygon │ │news_tiingo  │ │ economic_   │ │multimodal_  │ │
│ │news_alpha_v │ │news_fmp     │ │ events      │ │training_    │ │
│ │             │ │             │ │             │ │samples      │ │
│ └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Multi-Modal Training Pipeline                  │
│                                                                 │
│ ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│ │ Feature         │  │ Dataset         │  │ Model Training  │ │
│ │ Engineering     │  │ Generation      │  │                 │ │
│ │                 │  │                 │  │ • Transformer   │ │
│ │ • News features │  │ • Time-series   │  │ • LSTM/GRU      │ │
│ │ • Market data   │  │   alignment     │  │ • Attention     │ │
│ │ • Economic      │  │ • Train/val     │  │ • Multi-task    │ │
│ │   indicators    │  │   splits        │  │   learning      │ │
│ └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Production Serving                        │
│                                                                 │
│ ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│ │ Real-time       │  │ Prediction API  │  │ Risk Management │ │
│ │ Inference       │  │                 │  │                 │ │
│ │                 │  │ • REST API      │  │ • Position      │ │ 
│ │ • Model serving │  │ • WebSocket     │  │   sizing        │ │
│ │ • Feature cache │  │ • GraphQL       │  │ • Stop loss     │ │
│ │ • A/B testing   │  │                 │  │ • Portfolio     │ │
│ │                 │  │                 │  │   balancing     │ │
│ └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## 📊 Enhanced Database Schema

### Economic Events Classification

```sql
-- Core economic events table
CREATE TABLE dev_economic_events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_subtype VARCHAR(50),
    event_category VARCHAR(30) NOT NULL, -- 'macro', 'earnings', 'corporate', 'fed', 'employment'
    severity INTEGER NOT NULL CHECK (severity BETWEEN 1 AND 10),
    confidence_score DECIMAL(5,3) NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
    
    -- Affected entities
    affected_symbols TEXT[] DEFAULT '{}',
    affected_sectors TEXT[] DEFAULT '{}',
    affected_regions TEXT[] DEFAULT '{}',
    
    -- Timing
    event_date TIMESTAMP WITH TIME ZONE NOT NULL,
    announcement_date TIMESTAMP WITH TIME ZONE,
    market_open_date TIMESTAMP WITH TIME ZONE, -- Next market open after event
    
    -- Impact analysis
    predicted_impact_score DECIMAL(7,4), -- -1 to 1, predicted market impact
    actual_impact_score DECIMAL(7,4), -- Measured post-event impact
    impact_duration_days INTEGER, -- How long effect lasted
    
    -- Event details
    title TEXT NOT NULL,
    description TEXT,
    source_url TEXT,
    data JSONB NOT NULL, -- Full structured event data
    
    -- Metadata
    data_vendor VARCHAR(30) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_economic_events_event_date ON dev_economic_events(event_date DESC);
CREATE INDEX idx_economic_events_type_category ON dev_economic_events(event_type, event_category);
CREATE INDEX idx_economic_events_symbols ON dev_economic_events USING GIN(affected_symbols);
CREATE INDEX idx_economic_events_sectors ON dev_economic_events USING GIN(affected_sectors);
CREATE INDEX idx_economic_events_severity ON dev_economic_events(severity DESC, event_date DESC);
```

### Multi-Modal Training Samples

```sql
-- Training samples with comprehensive feature set
CREATE TABLE dev_multimodal_training_samples (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    sample_date DATE NOT NULL,
    prediction_horizon INTEGER NOT NULL CHECK (prediction_horizon IN (1, 5, 10, 20)),
    
    -- News sentiment features (lookback: 1, 3, 7 days)
    news_sentiment_1d DECIMAL(7,4), -- Average sentiment last 1 day
    news_sentiment_3d DECIMAL(7,4), -- Average sentiment last 3 days  
    news_sentiment_7d DECIMAL(7,4), -- Average sentiment last 7 days
    news_volume_1d INTEGER DEFAULT 0, -- News article count
    news_volume_3d INTEGER DEFAULT 0,
    news_volume_7d INTEGER DEFAULT 0,
    news_momentum_3d DECIMAL(7,4), -- Sentiment change over 3 days
    news_momentum_7d DECIMAL(7,4), -- Sentiment change over 7 days
    
    -- Economic event features
    economic_event_impact_1d DECIMAL(7,4) DEFAULT 0, -- Economic events last 1 day
    economic_event_impact_3d DECIMAL(7,4) DEFAULT 0, -- Economic events last 3 days
    economic_event_impact_7d DECIMAL(7,4) DEFAULT 0, -- Economic events last 7 days
    earnings_impact_score DECIMAL(7,4), -- Earnings-specific impact
    macro_event_impact DECIMAL(7,4), -- Macro economic impact
    fed_event_impact DECIMAL(7,4), -- Federal Reserve impact
    
    -- Technical market features
    price_features JSONB NOT NULL, -- {
    --   "sma_10", "sma_20", "sma_50", "ema_12", "ema_26",
    --   "rsi_14", "macd", "bollinger_upper", "bollinger_lower", 
    --   "atr_14", "stochastic_k", "stochastic_d", "williams_r",
    --   "price_momentum_1d", "price_momentum_5d", "price_momentum_20d"
    -- }
    
    volume_features JSONB NOT NULL, -- {
    --   "volume_sma_10", "volume_sma_20", "relative_volume",
    --   "volume_momentum", "price_volume_trend", "accumulation_distribution",
    --   "on_balance_volume", "volume_weighted_average_price"
    -- }
    
    market_microstructure JSONB, -- {
    --   "bid_ask_spread", "order_imbalance", "trade_intensity",
    --   "market_impact", "liquidity_score"
    -- }
    
    -- Cross-asset features
    sector_correlation DECIMAL(7,4), -- Correlation with sector ETF
    market_correlation DECIMAL(7,4), -- Correlation with SPY
    vix_level DECIMAL(7,4), -- VIX at sample date
    yield_curve_10y2y DECIMAL(7,4), -- 10Y-2Y yield spread
    dxy_level DECIMAL(7,4), -- Dollar strength index
    
    -- Target variables (actual future performance)
    target_return_1d DECIMAL(8,5),
    target_return_5d DECIMAL(8,5), 
    target_return_10d DECIMAL(8,5),
    target_return_20d DECIMAL(8,5),
    target_volatility_5d DECIMAL(8,5), -- 5-day realized volatility
    target_volatility_20d DECIMAL(8,5), -- 20-day realized volatility
    target_max_drawdown DECIMAL(8,5), -- Maximum drawdown in horizon
    target_sharpe_ratio DECIMAL(8,5), -- Risk-adjusted return
    
    -- Classification targets
    target_direction_1d INTEGER, -- -1, 0, 1 (down, flat, up)
    target_direction_5d INTEGER,
    target_direction_10d INTEGER,
    target_direction_20d INTEGER,
    target_volatility_regime INTEGER, -- 1, 2, 3 (low, medium, high vol)
    
    -- Sample metadata
    sample_quality_score DECIMAL(5,3) DEFAULT 1.0, -- Data quality indicator
    sample_weight DECIMAL(7,4) DEFAULT 1.0, -- Training weight
    is_outlier BOOLEAN DEFAULT FALSE, -- Statistical outlier detection
    market_regime VARCHAR(20), -- 'bull', 'bear', 'sideways', 'crisis'
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (symbol, sample_date, prediction_horizon)
);

-- Indexes for efficient training data access
CREATE INDEX idx_multimodal_samples_symbol_date ON dev_multimodal_training_samples(symbol, sample_date DESC);
CREATE INDEX idx_multimodal_samples_horizon ON dev_multimodal_training_samples(prediction_horizon, sample_date DESC);
CREATE INDEX idx_multimodal_samples_quality ON dev_multimodal_training_samples(sample_quality_score DESC, is_outlier, sample_date DESC);
CREATE INDEX idx_multimodal_samples_regime ON dev_multimodal_training_samples(market_regime, sample_date DESC);
```

## 🔌 News Data Sources Architecture

### API Integration Specifications

```python
# src/market_data/news/enhanced_news_fetcher.py

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import aiohttp
import asyncio
from datetime import datetime, date

@dataclass
class NewsSourceConfig:
    """Configuration for each news source"""
    name: str
    base_url: str
    api_key_env: str
    rate_limit_per_minute: int
    max_concurrent: int
    supports_historical: bool
    supports_realtime: bool
    cost_per_request: float  # USD

class NewsSourceManager:
    """Manages multiple news API sources with failover and load balancing"""
    
    SOURCES = {
        'polygon': NewsSourceConfig(
            name='polygon',
            base_url='https://api.polygon.io/v2/reference/news',
            api_key_env='POLYGON_API_KEY',
            rate_limit_per_minute=500,
            max_concurrent=50,
            supports_historical=True,
            supports_realtime=True,
            cost_per_request=0.002
        ),
        'tiingo': NewsSourceConfig(
            name='tiingo', 
            base_url='https://api.tiingo.com/tiingo/news',
            api_key_env='TIINGO_API_KEY',
            rate_limit_per_minute=1000,
            max_concurrent=30,
            supports_historical=True,
            supports_realtime=True,
            cost_per_request=0.001
        ),
        'alpha_vantage': NewsSourceConfig(
            name='alpha_vantage',
            base_url='https://www.alphavantage.co/query',
            api_key_env='ALPHA_VANTAGE_API_KEY',
            rate_limit_per_minute=500,
            max_concurrent=25,
            supports_historical=True,
            supports_realtime=False,
            cost_per_request=0.003
        ),
        'fmp': NewsSourceConfig(
            name='fmp',
            base_url='https://financialmodelingprep.com/api/v3/stock_news',
            api_key_env='FMP_API_KEY', 
            rate_limit_per_minute=300,
            max_concurrent=20,
            supports_historical=True,
            supports_realtime=True,
            cost_per_request=0.004
        ),
        'benzinga': NewsSourceConfig(
            name='benzinga',
            base_url='https://api.benzinga.com/api/v2/news',
            api_key_env='BENZINGA_API_KEY',
            rate_limit_per_minute=1000, 
            max_concurrent=40,
            supports_historical=True,
            supports_realtime=True,
            cost_per_request=0.005
        )
    }
```

### Economic Data Integration

```python
# src/market_data/economic/economic_events_collector.py

class EconomicEventsCollector:
    """Collects and classifies economic events from multiple sources"""
    
    FRED_INDICATORS = {
        # Employment
        'UNRATE': {'category': 'employment', 'impact': 'high'},
        'NPPTTL': {'category': 'employment', 'impact': 'medium'},
        'ICSA': {'category': 'employment', 'impact': 'medium'},
        
        # Inflation
        'CPIAUCSL': {'category': 'inflation', 'impact': 'high'},
        'CPILFESL': {'category': 'inflation', 'impact': 'high'},
        'PCEPI': {'category': 'inflation', 'impact': 'high'},
        
        # GDP & Growth
        'GDP': {'category': 'growth', 'impact': 'high'},
        'GDPC1': {'category': 'growth', 'impact': 'high'},
        
        # Federal Reserve
        'FEDFUNDS': {'category': 'monetary', 'impact': 'high'},
        'DFF': {'category': 'monetary', 'impact': 'high'},
        
        # Market Indicators
        'VIXCLS': {'category': 'market', 'impact': 'medium'},
        'DEXUSEU': {'category': 'market', 'impact': 'medium'},
    }
    
    async def fetch_fred_releases(self, start_date: date, end_date: date) -> List[Dict]:
        """Fetch economic data releases from FRED API"""
        # Implementation for FRED API integration
        pass
    
    async def classify_economic_event(self, event_data: Dict) -> Dict:
        """Classify economic event impact and affected sectors"""
        # ML-based event classification
        pass
```

## 🧠 Multi-Modal Model Architecture

### Transformer-Based Architecture

```python
# src/models/multimodal/transformer_predictor.py

import torch
import torch.nn as nn
from transformers import AutoModel
from typing import Dict, Tuple

class MultiModalTransformerPredictor(nn.Module):
    """
    Multi-modal transformer combining news, time-series, and economic data
    """
    
    def __init__(self, config: Dict):
        super().__init__()
        
        # News encoder (FinBERT-based)
        self.news_encoder = AutoModel.from_pretrained("ProsusAI/finbert")
        self.news_projection = nn.Linear(768, config['hidden_dim'])
        
        # Time-series encoder
        self.price_encoder = nn.LSTM(
            input_size=config['price_features'],
            hidden_size=config['hidden_dim'],
            num_layers=config['lstm_layers'],
            batch_first=True,
            dropout=config['dropout']
        )
        
        # Economic events encoder
        self.economic_encoder = nn.Linear(
            config['economic_features'], 
            config['hidden_dim']
        )
        
        # Cross-modal attention
        self.cross_attention = nn.MultiheadAttention(
            embed_dim=config['hidden_dim'],
            num_heads=config['attention_heads'],
            dropout=config['dropout']
        )
        
        # Fusion layer
        self.fusion = nn.Sequential(
            nn.Linear(config['hidden_dim'] * 3, config['hidden_dim']),
            nn.LayerNorm(config['hidden_dim']),
            nn.ReLU(),
            nn.Dropout(config['dropout'])
        )
        
        # Prediction heads
        self.return_head = nn.Linear(config['hidden_dim'], config['prediction_horizons'])
        self.volatility_head = nn.Linear(config['hidden_dim'], config['prediction_horizons'])
        self.direction_head = nn.Linear(config['hidden_dim'], config['prediction_horizons'] * 3)
        
    def forward(self, batch: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        # Encode news sentiment
        news_features = self.news_encoder(**batch['news'])
        news_encoded = self.news_projection(news_features.last_hidden_state.mean(dim=1))
        
        # Encode time-series
        price_encoded, _ = self.price_encoder(batch['price_sequence'])
        price_encoded = price_encoded[:, -1, :]  # Take last hidden state
        
        # Encode economic events
        economic_encoded = self.economic_encoder(batch['economic_features'])
        
        # Cross-modal attention
        combined = torch.stack([news_encoded, price_encoded, economic_encoded], dim=1)
        attended, _ = self.cross_attention(combined, combined, combined)
        
        # Fusion
        fused = self.fusion(attended.flatten(1))
        
        # Multi-task predictions
        return {
            'returns': self.return_head(fused),
            'volatility': self.volatility_head(fused),
            'direction': self.direction_head(fused).view(-1, config['prediction_horizons'], 3)
        }
```

### Training Pipeline

```python
# src/training/multimodal_trainer.py

class MultiModalTrainer:
    """Training pipeline for multi-modal models"""
    
    def __init__(self, config: Dict, model: nn.Module):
        self.config = config
        self.model = model
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        
        # Multi-task loss function
        self.mse_loss = nn.MSELoss()
        self.ce_loss = nn.CrossEntropyLoss()
        
        # Optimizer with different learning rates for different components
        self.optimizer = torch.optim.AdamW([
            {'params': self.model.news_encoder.parameters(), 'lr': config['news_lr']},
            {'params': self.model.price_encoder.parameters(), 'lr': config['price_lr']},
            {'params': self.model.fusion.parameters(), 'lr': config['fusion_lr']},
        ], weight_decay=config['weight_decay'])
        
    async def train_epoch(self, dataloader) -> Dict[str, float]:
        """Train for one epoch"""
        self.model.train()
        total_loss = 0
        
        for batch in dataloader:
            # Move to device
            batch = {k: v.to(self.device) for k, v in batch.items()}
            
            # Forward pass
            predictions = self.model(batch)
            
            # Multi-task loss
            return_loss = self.mse_loss(predictions['returns'], batch['target_returns'])
            vol_loss = self.mse_loss(predictions['volatility'], batch['target_volatility'])
            dir_loss = self.ce_loss(
                predictions['direction'].view(-1, 3), 
                batch['target_directions'].view(-1)
            )
            
            # Weighted combination
            loss = (
                self.config['return_weight'] * return_loss +
                self.config['volatility_weight'] * vol_loss +
                self.config['direction_weight'] * dir_loss
            )
            
            # Backward pass
            self.optimizer.zero_grad()
            loss.backward()
            
            # Gradient clipping
            torch.nn.utils.clip_grad_norm_(self.model.parameters(), self.config['grad_clip'])
            
            self.optimizer.step()
            total_loss += loss.item()
            
        return {'train_loss': total_loss / len(dataloader)}
```

## ⚡ Real-Time Processing Pipeline

### Kubernetes Deployment Architecture

```yaml
# k8s/news-multimodal-system.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: news-multimodal-config
  namespace: ats-dev
data:
  config.yaml: |
    news_sources:
      - name: polygon
        enabled: true
        priority: 1
        max_concurrent: 50
      - name: tiingo  
        enabled: true
        priority: 2
        max_concurrent: 30
      - name: alpha_vantage
        enabled: true
        priority: 3
        max_concurrent: 25
    
    processing:
      batch_size: 1000
      processing_interval_seconds: 30
      sentiment_threshold: 0.7
      
    model_serving:
      model_path: /models/multimodal-v1.0
      batch_inference_size: 100
      max_latency_ms: 100
      
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: news-collector
  namespace: ats-dev
spec:
  replicas: 3
  selector:
    matchLabels:
      app: news-collector
  template:
    spec:
      containers:
      - name: news-collector
        image: dragonflyer762/ats-genai:latest
        command: ["/bin/bash", "-c"]
        args:
        - |
          cd /app
          python -m src.market_data.news.enhanced_news_collector \
            --config /config/config.yaml \
            --mode realtime
        env:
        - name: POLYGON_API_KEY
          valueFrom:
            secretKeyRef:
              name: api-credentials-dev
              key: POLYGON_API_KEY
        - name: TIINGO_API_KEY
          valueFrom:
            secretKeyRef:
              name: api-credentials-dev  
              key: TIINGO_API_KEY
        volumeMounts:
        - name: config
          mountPath: /config
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi" 
            cpu: "1000m"
      volumes:
      - name: config
        configMap:
          name: news-multimodal-config

---
apiVersion: apps/v1  
kind: Deployment
metadata:
  name: multimodal-predictor
  namespace: ats-dev
spec:
  replicas: 2
  selector:
    matchLabels:
      app: multimodal-predictor
  template:
    spec:
      containers:
      - name: predictor
        image: dragonflyer762/ats-genai:latest
        command: ["/bin/bash", "-c"]
        args:
        - |
          cd /app
          python -m src.models.multimodal.prediction_server \
            --port 8080 \
            --model-path /models/multimodal-v1.0
        ports:
        - containerPort: 8080
        resources:
          requests:
            memory: "4Gi"
            cpu: "1000m" 
            nvidia.com/gpu: "1"
          limits:
            memory: "8Gi"
            cpu: "2000m"
            nvidia.com/gpu: "1"

---
apiVersion: v1
kind: Service
metadata:
  name: multimodal-prediction-api
  namespace: ats-dev
spec:
  selector:
    app: multimodal-predictor
  ports:
  - port: 8080
    targetPort: 8080
    nodePort: 30080
  type: NodePort
```

## 🔄 Data Processing Pipelines

### Feature Engineering Pipeline

```python
# src/features/multimodal_features.py

class MultiModalFeatureGenerator:
    """Generate features for multi-modal training"""
    
    async def generate_news_features(self, symbol: str, date: date, 
                                   lookback_days: List[int] = [1, 3, 7]) -> Dict:
        """Generate news sentiment features"""
        features = {}
        
        for days in lookback_days:
            start_date = date - timedelta(days=days)
            
            # Get news articles for symbol in time window
            articles = await self.get_news_articles(symbol, start_date, date)
            
            if articles:
                sentiments = [article.sentiment_score for article in articles]
                features.update({
                    f'news_sentiment_{days}d': np.mean(sentiments),
                    f'news_volume_{days}d': len(articles),
                    f'news_momentum_{days}d': self._calculate_sentiment_momentum(articles)
                })
            else:
                features.update({
                    f'news_sentiment_{days}d': 0.0,
                    f'news_volume_{days}d': 0,
                    f'news_momentum_{days}d': 0.0
                })
                
        return features
    
    async def generate_technical_features(self, symbol: str, date: date) -> Dict:
        """Generate technical analysis features"""
        # Get 60 days of price data for indicator calculation
        prices = await self.get_price_history(symbol, date - timedelta(days=60), date)
        
        df = pd.DataFrame(prices)
        
        return {
            'sma_10': df['close'].rolling(10).mean().iloc[-1],
            'sma_20': df['close'].rolling(20).mean().iloc[-1], 
            'sma_50': df['close'].rolling(50).mean().iloc[-1],
            'ema_12': df['close'].ewm(span=12).mean().iloc[-1],
            'ema_26': df['close'].ewm(span=26).mean().iloc[-1],
            'rsi_14': self._calculate_rsi(df['close'], 14).iloc[-1],
            'macd': self._calculate_macd(df['close']).iloc[-1],
            'atr_14': self._calculate_atr(df, 14).iloc[-1],
            'bollinger_upper': self._calculate_bollinger(df['close'])[0].iloc[-1],
            'bollinger_lower': self._calculate_bollinger(df['close'])[1].iloc[-1],
        }
```

### Training Data Generation Job

```python
# src/jobs/generate_training_data.py

class TrainingDataGenerator:
    """Generate training samples for multi-modal models"""
    
    async def generate_historical_samples(self, start_date: date, end_date: date,
                                        symbols: List[str]) -> int:
        """Generate training samples for date range and symbols"""
        
        total_samples = 0
        batch_size = 1000
        current_batch = []
        
        for symbol in symbols:
            logger.info(f"Generating samples for {symbol}")
            
            current_date = start_date
            while current_date <= end_date:
                # Skip weekends
                if current_date.weekday() >= 5:
                    current_date += timedelta(days=1)
                    continue
                    
                for horizon in [1, 5, 10, 20]:
                    try:
                        sample = await self._generate_sample(symbol, current_date, horizon)
                        if sample:
                            current_batch.append(sample)
                            
                        if len(current_batch) >= batch_size:
                            await self._insert_batch(current_batch)
                            total_samples += len(current_batch)
                            current_batch = []
                            
                    except Exception as e:
                        logger.error(f"Failed to generate sample for {symbol} on {current_date}: {e}")
                
                current_date += timedelta(days=1)
        
        # Insert remaining samples
        if current_batch:
            await self._insert_batch(current_batch)
            total_samples += len(current_batch)
            
        return total_samples
    
    async def _generate_sample(self, symbol: str, date: date, horizon: int) -> Optional[Dict]:
        """Generate single training sample"""
        
        # Check if future data is available (for targets)
        future_date = date + timedelta(days=horizon)
        if future_date > datetime.now().date():
            return None
            
        # Generate features
        news_features = await self.feature_generator.generate_news_features(symbol, date)
        technical_features = await self.feature_generator.generate_technical_features(symbol, date)
        economic_features = await self.feature_generator.generate_economic_features(date)
        
        # Generate targets
        targets = await self._generate_targets(symbol, date, horizon)
        
        return {
            'symbol': symbol,
            'sample_date': date,
            'prediction_horizon': horizon,
            **news_features,
            **technical_features, 
            **economic_features,
            **targets
        }
```

## 📊 Performance Monitoring & Evaluation

### Model Performance Metrics

```python
# src/evaluation/multimodal_evaluator.py

class MultiModalEvaluator:
    """Evaluation framework for multi-modal models"""
    
    METRICS = {
        'regression': ['mse', 'mae', 'r2', 'directional_accuracy'],
        'classification': ['accuracy', 'precision', 'recall', 'f1', 'auc'],
        'financial': ['sharpe_ratio', 'max_drawdown', 'calmar_ratio', 'information_ratio']
    }
    
    async def evaluate_model(self, model, test_dataset) -> Dict:
        """Comprehensive model evaluation"""
        
        predictions = []
        actuals = []
        
        for batch in test_dataset:
            with torch.no_grad():
                pred = model(batch)
                predictions.append(pred)
                actuals.append({
                    'returns': batch['target_returns'],
                    'volatility': batch['target_volatility'],
                    'direction': batch['target_directions']
                })
        
        # Calculate metrics
        results = {}
        
        # Regression metrics (returns, volatility)
        for target in ['returns', 'volatility']:
            pred_values = torch.cat([p[target] for p in predictions]).numpy()
            actual_values = torch.cat([a[target] for a in actuals]).numpy()
            
            results[f'{target}_mse'] = np.mean((pred_values - actual_values) ** 2)
            results[f'{target}_mae'] = np.mean(np.abs(pred_values - actual_values))
            results[f'{target}_r2'] = 1 - (
                np.sum((pred_values - actual_values) ** 2) / 
                np.sum((actual_values - np.mean(actual_values)) ** 2)
            )
            
        # Classification metrics (direction)
        direction_pred = torch.cat([p['direction'].argmax(dim=-1) for p in predictions]).numpy()
        direction_actual = torch.cat([a['direction'] for a in actuals]).numpy()
        
        results['direction_accuracy'] = np.mean(direction_pred == direction_actual)
        
        # Financial metrics
        results.update(self._calculate_financial_metrics(predictions, actuals))
        
        return results
    
    def _calculate_financial_metrics(self, predictions, actuals) -> Dict:
        """Calculate financial performance metrics"""
        # Implementation of Sharpe ratio, max drawdown, etc.
        pass
```

## 🚀 Production Deployment

### API Server Implementation

```python
# src/api/multimodal_prediction_api.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
import torch
import asyncio

app = FastAPI(title="Multi-Modal Prediction API", version="1.0")

class PredictionRequest(BaseModel):
    symbols: List[str]
    prediction_horizons: List[int] = [1, 5, 10, 20]
    include_confidence: bool = True

class PredictionResponse(BaseModel):
    symbol: str
    predictions: Dict[int, Dict[str, float]]  # horizon -> predictions
    confidence: Optional[Dict[int, float]] = None
    generated_at: str

@app.post("/predict", response_model=List[PredictionResponse])
async def predict(request: PredictionRequest):
    """Generate multi-modal predictions for symbols"""
    
    try:
        results = []
        
        for symbol in request.symbols:
            # Get current features
            features = await feature_service.get_current_features(symbol)
            
            # Generate predictions
            predictions = await model_service.predict(
                features, 
                horizons=request.prediction_horizons
            )
            
            # Calculate confidence if requested
            confidence = None
            if request.include_confidence:
                confidence = await model_service.get_confidence(features)
            
            results.append(PredictionResponse(
                symbol=symbol,
                predictions=predictions,
                confidence=confidence,
                generated_at=datetime.now().isoformat()
            ))
            
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {e}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}
```

This technical design provides the complete architecture for implementing the news-driven multi-modal prediction system, building upon the existing ATS infrastructure while adding sophisticated ML capabilities for enhanced trading performance.

## 🎯 Implementation Priority

1. **Phase 1**: Enhanced news collection and economic events detection
2. **Phase 2**: Training data generation pipeline 
3. **Phase 3**: Multi-modal model development and training
4. **Phase 4**: Production API and integration with existing trading systems

The system is designed to be scalable, maintainable, and compatible with the existing Docker-first ATS platform architecture.