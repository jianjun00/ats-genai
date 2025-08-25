# 🤖 ML Platform

**Training, Models, Inference, and AI-Powered Portfolio Optimization**

The ML Platform provides end-to-end machine learning capabilities for the ATS system, including automated training data generation, model development, inference serving, and AI-powered portfolio optimization with Smart Money Zone detection.

---

## 🎯 Component Overview

### **Core Capabilities**
- **Automated Training Data Generation**: Feature engineering and labeling at scale
- **Model Lifecycle Management**: Training, validation, versioning, deployment
- **Real-Time Inference**: Low-latency prediction serving for live trading
- **Portfolio Optimization**: AI-driven market-neutral portfolio construction
- **Smart Money Zones**: Institutional flow analysis and detection
- **Model Monitoring**: Performance tracking and drift detection

### **Key Technologies**
- **Python ML Stack**: scikit-learn, XGBoost, LightGBM, PyTorch
- **Feature Engineering**: Custom indicators, technical analysis, alternative data
- **MLOps**: Model versioning, A/B testing, continuous training
- **Inference Serving**: FastAPI, async prediction endpoints
- **Experiment Tracking**: MLflow, model registry, experiment comparison
- **Kubernetes**: Containerized training jobs and model serving

---

## 📚 Documentation Structure

### **🏗️ Architecture & Design**
- **[SYSTEM_DESIGN.md](SYSTEM_DESIGN.md)** - ML architecture, model pipeline, training workflows
- Model lifecycle and deployment patterns
- Feature engineering and data science methodology
- AI-powered portfolio optimization algorithms

### **⚙️ Operations & Deployment**
- **[OPERATIONS.md](OPERATIONS.md)** - MLOps, model monitoring, performance tracking
- Model deployment and rollback procedures
- Training pipeline monitoring and alerting
- Model performance and drift detection

### **📋 Product & Planning**
- **[prd/](prd/)** - Product Requirements Documents
- **[drd/](drd/)** - Detailed Requirements Documents
- ML platform roadmap and feature specifications
- Model development and validation plans

---

## 🚀 Quick Start

### Model Training Pipeline
```bash
# Generate training data for enhanced model
run_dev job enhanced-training --symbol TSLA --days-back 120

# Train support/resistance prediction model
run_dev job model-training \
  --model-type support_resistance \
  --training-window 2y \
  --validation-split 0.2

# Deploy trained model to inference
kubectl apply -f k8s/ml-platform/model-inference-service.yaml
```

### Portfolio Optimization
```bash
# Run AI-powered portfolio optimization
run_dev job portfolio-optimization \
  --universe SP500 \
  --target-return 0.12 \
  --max-drawdown 0.08 \
  --rebalance-frequency weekly

# Generate Smart Money Zone signals
run_dev job smart-money-zones \
  --symbols AAPL,MSFT,GOOGL \
  --lookback-days 30
```

### Model Inference
```python
# Real-time model predictions
import requests

response = requests.post(
    "http://ml-inference-service:8080/predict/support_resistance",
    json={
        "symbol": "AAPL",
        "features": {
            "close": 150.25,
            "volume": 50000000,
            "rsi_14": 65.2,
            "bb_position": 0.7
        }
    }
)

predictions = response.json()
# {
#   "support_level": 148.50,
#   "resistance_level": 152.75,
#   "confidence": 0.87,
#   "model_version": "v1.2.3"
# }
```

---

## 🧠 ML Model Inventory

### **Production Models**
| Model | Purpose | Input Features | Output | Accuracy | Status |
|-------|---------|----------------|--------|----------|---------|
| **Support/Resistance Predictor** | Next-day level prediction | 50+ technical indicators | Price levels + confidence | 72.3% | ✅ Active |
| **Smart Money Detector** | Institutional flow analysis | Volume, price action, order flow | Buy/sell signals | 68.5% | ✅ Active |
| **Portfolio Optimizer** | Market-neutral allocation | Risk factors, correlations | Position weights | IR: 1.34 | ✅ Active |
| **Volatility Forecaster** | Risk management | Historical vol, option flows | Vol predictions | MAE: 0.024 | 🔄 Testing |

### **Model Performance Metrics**
```python
# Example model performance tracking
{
    "model_name": "support_resistance_v1.2.3",
    "metrics": {
        "accuracy": 0.723,
        "precision": 0.689,
        "recall": 0.756,
        "f1_score": 0.721,
        "mae": 0.024,
        "mse": 0.0012
    },
    "business_metrics": {
        "sharpe_ratio": 1.34,
        "max_drawdown": 0.081,
        "win_rate": 0.672,
        "profit_factor": 1.85
    },
    "last_updated": "2024-01-15T10:30:00Z",
    "training_data_size": 2500000,
    "feature_count": 52
}
```

---

## 🔬 Feature Engineering Framework

### **Technical Indicators**
- **Trend**: Moving averages (SMA, EMA, VWAP), MACD, ADX
- **Momentum**: RSI, Stochastic, Williams %R, Rate of Change
- **Volatility**: Bollinger Bands, ATR, Keltner Channels
- **Volume**: On-Balance Volume, Volume Profile, VWAP deviations
- **Pattern Recognition**: Support/resistance levels, chart patterns

### **Smart Money Features**
```python
class SmartMoneyFeatureEngine:
    def generate_features(self, symbol: str, lookback: int = 20) -> Dict:
        """Generate institutional flow detection features"""
        return {
            # Volume Analysis
            'volume_profile_poc': self.calculate_poc(symbol, lookback),
            'unusual_volume_ratio': self.detect_volume_anomalies(symbol),
            'block_trade_frequency': self.count_block_trades(symbol, lookback),
            
            # Price Action
            'accumulation_distribution': self.calculate_ad_line(symbol, lookback),
            'money_flow_index': self.calculate_mfi(symbol, lookback),
            'institutional_candle_patterns': self.detect_inst_patterns(symbol),
            
            # Order Flow (when available)
            'bid_ask_imbalance': self.calculate_imbalance(symbol),
            'large_order_flow': self.detect_large_orders(symbol),
            'dark_pool_activity': self.estimate_dark_pool(symbol)
        }
```

### **Alternative Data Integration**
- **Economic Indicators**: Fed data, treasury yields, economic releases
- **Sentiment Data**: News sentiment, social media sentiment scores
- **Fundamental Data**: Financial ratios, earnings estimates, analyst revisions
- **Cross-Asset Signals**: Sector rotation, commodity correlations, VIX levels

---

## 🏗️ Training Pipeline Architecture

### **Automated Training Workflow**
```
Data Ingestion → Feature Engineering → Label Generation → Model Training → Validation → Deployment
      ↓               ↓                    ↓               ↓              ↓           ↓
  [Multi-vendor]  [52 features]      [Support/Resist]  [XGBoost]    [Backtesting] [API Serving]
  [Time-series]   [Technical]        [Pattern Rec]     [PyTorch]    [A/B Testing] [Monitoring]
  [Corporate      [Smart Money]      [Price Targets]   [LightGBM]   [Validation]  [Rollback]
   Actions]       [Alternative]      [Confidence]      [Ensemble]   [Metrics]
```

### **Training Data Generation**
```python
# Enhanced training data generation
class TrainingDataGenerator:
    def generate_training_examples(
        self, 
        symbols: List[str], 
        start_date: datetime, 
        end_date: datetime
    ) -> List[TrainingExample]:
        """
        Generate comprehensive training examples with:
        - 50+ engineered features
        - Support/resistance level labels  
        - Confidence scores
        - Market context
        """
        examples = []
        
        for symbol in symbols:
            # Get market data
            data = self.get_market_data(symbol, start_date, end_date)
            
            # Generate features
            features = self.feature_engine.generate_features(data)
            
            # Generate labels using multiple methods
            labels = self.label_generator.generate_labels(
                data, methods=['pivot_points', 'volume_profile', 'fibonacci']
            )
            
            # Create training examples
            for i in range(len(data)):
                if self.is_valid_example(data.iloc[i], features[i], labels[i]):
                    examples.append(TrainingExample(
                        symbol=symbol,
                        date=data.iloc[i]['date'],
                        features=features[i],
                        labels=labels[i],
                        market_context=self.get_market_context(data.iloc[i])
                    ))
        
        return examples
```

---

## 🎯 Portfolio Optimization Engine

### **Market-Neutral Strategy**
```python
class MarketNeutralOptimizer:
    def optimize_portfolio(
        self,
        universe: List[str],
        target_return: float = 0.12,
        max_drawdown: float = 0.08,
        rebalance_freq: str = 'weekly'
    ) -> PortfolioAllocation:
        """
        AI-powered portfolio optimization with:
        - Market-neutral long/short construction
        - 19-factor risk model
        - Smart Money Zone integration
        - Dynamic rebalancing
        """
        
        # Get signals from ML models
        signals = self.get_model_signals(universe)
        
        # Apply Smart Money Zone filters
        smz_signals = self.apply_smart_money_filters(signals)
        
        # Risk factor analysis
        risk_factors = self.calculate_risk_factors(universe)
        
        # Optimization with constraints
        allocation = self.solve_optimization(
            signals=smz_signals,
            risk_factors=risk_factors,
            target_return=target_return,
            max_drawdown=max_drawdown
        )
        
        return allocation
```

### **Smart Money Zone Integration**
```python
class SmartMoneyZoneDetector:
    def detect_zones(self, symbol: str, lookback_days: int = 30) -> List[SmartMoneyZone]:
        """
        Detect institutional accumulation/distribution zones
        """
        zones = []
        
        # Volume Profile Analysis
        volume_profile = self.calculate_volume_profile(symbol, lookback_days)
        poc_levels = self.identify_poc_clusters(volume_profile)
        
        # Institutional Activity Detection
        block_trades = self.detect_block_trades(symbol, lookback_days)
        dark_pool_activity = self.estimate_dark_pool_flow(symbol, lookback_days)
        
        # Smart Money Zone Classification
        for level in poc_levels:
            zone_strength = self.calculate_zone_strength(
                level, block_trades, dark_pool_activity
            )
            
            if zone_strength > 0.7:  # High confidence threshold
                zones.append(SmartMoneyZone(
                    symbol=symbol,
                    price_level=level.price,
                    zone_type=level.type,  # accumulation/distribution
                    strength=zone_strength,
                    supporting_evidence=level.evidence
                ))
        
        return zones
```

---

## 📊 Model Performance Monitoring

### **Real-Time Model Metrics**
- **Prediction Accuracy**: Rolling accuracy over time windows
- **Business Impact**: Sharpe ratio, drawdown, win rate improvements
- **Model Drift**: Feature distribution changes, prediction drift
- **Latency**: Inference response time, throughput metrics
- **Resource Usage**: CPU, memory, GPU utilization

### **A/B Testing Framework**
```python
class ModelABTester:
    def run_ab_test(
        self,
        control_model: str,
        treatment_model: str, 
        test_duration: int = 30,
        traffic_split: float = 0.1
    ) -> ABTestResult:
        """
        A/B test new model versions against production
        """
        # Route traffic to models
        self.setup_traffic_split(control_model, treatment_model, traffic_split)
        
        # Collect metrics during test period
        metrics = self.collect_metrics(test_duration)
        
        # Statistical significance testing
        result = self.analyze_results(
            control_metrics=metrics[control_model],
            treatment_metrics=metrics[treatment_model]
        )
        
        return result
```

---

## 🔗 Related Components

- **[📊 Data Infrastructure](../data-infrastructure/)** - Provides training and inference data
- **[🔧 Backend Platform](../backend-platform/)** - Serves model predictions via APIs
- **[☁️ Online Infrastructure](../online-infrastructure/)** - Hosts training jobs and model serving

---

## 📊 Key Metrics & KPIs

- **Model Accuracy**: > 70% for support/resistance predictions
- **Inference Latency**: < 50ms for real-time predictions
- **Training Pipeline**: < 4 hours for full model retraining
- **Portfolio Performance**: Sharpe ratio > 1.2, max drawdown < 10%
- **Model Availability**: 99.9% uptime for inference services

---

## 👥 Team Ownership

- **Primary Team**: Data Science, ML Engineering
- **Secondary Teams**: Backend Engineering, DevOps
- **Key Contacts**: Head of Data Science, ML Platform Lead

---

*For ML-driven trading workflows, see the [📖 main documentation hub](../README.md)*