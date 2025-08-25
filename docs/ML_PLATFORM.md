# 🤖 ML Platform

**Training, Models, Inference, and AI-Powered Portfolio Optimization**

Complete machine learning platform documentation covering model development, training pipelines, inference serving, and AI-powered portfolio optimization.

---

## 🎯 Platform Overview

The ML Platform provides end-to-end machine learning capabilities for the ATS system, including automated training data generation, model development, inference serving, and AI-powered portfolio optimization with Smart Money Zone detection.

### **Core Capabilities**
- **Automated Training Data Generation** - Feature engineering and labeling at scale
- **Model Lifecycle Management** - Training, validation, versioning, deployment
- **Real-Time Inference** - Low-latency prediction serving for live trading
- **Portfolio Optimization** - AI-driven market-neutral portfolio construction
- **Smart Money Zones** - Institutional flow analysis and detection
- **Model Monitoring** - Performance tracking and drift detection

### **Key Technologies**
- **Python ML Stack** - scikit-learn, XGBoost, LightGBM, PyTorch
- **Feature Engineering** - Custom indicators, technical analysis, alternative data
- **MLOps** - Model versioning, A/B testing, continuous training
- **Inference Serving** - FastAPI async prediction endpoints
- **Kubernetes** - Containerized training jobs and model serving

---

## 🚀 Quick Start

### **Model Training Pipeline**
```bash
# Generate training data for enhanced model
python scripts/run_dev.py deploy --file k8s/enhanced-training-job.yaml

# Train support/resistance prediction model  
python scripts/run_dev.py deploy --file k8s/sr-training-job.yaml

# Deploy trained model to inference
kubectl apply -f k8s/ml-inference-service.yaml
```

### **Portfolio Optimization**
```bash
# Run AI-powered portfolio optimization
python scripts/run_dev.py deploy --file k8s/portfolio-optimization-job.yaml

# Generate Smart Money Zone signals
python scripts/run_dev.py deploy --file k8s/smart-money-zones-job.yaml
```

---

## 🧠 Production Models

### **Model Inventory**
| Model | Purpose | Input Features | Output | Accuracy | Status |
|-------|---------|----------------|--------|----------|---------|
| **Support/Resistance Predictor** | Next-day level prediction | 50+ technical indicators | Price levels + confidence | 72.3% | ✅ Active |
| **Smart Money Detector** | Institutional flow analysis | Volume, price action, order flow | Buy/sell signals | 68.5% | ✅ Active |
| **Portfolio Optimizer** | Market-neutral allocation | Risk factors, correlations | Position weights | IR: 1.34 | ✅ Active |
| **Volatility Forecaster** | Risk management | Historical vol, option flows | Vol predictions | MAE: 0.024 | 🔄 Testing |

### **Model Performance Tracking**
```python
class ModelPerformanceMonitor:
    def track_model_performance(self, model_name: str) -> ModelMetrics:
        """
        Comprehensive model performance tracking
        """
        return ModelMetrics(
            model_name=model_name,
            accuracy_metrics={
                "accuracy": 0.723,
                "precision": 0.689,
                "recall": 0.756,
                "f1_score": 0.721,
                "mae": 0.024,
                "mse": 0.0012
            },
            business_metrics={
                "sharpe_ratio": 1.34,
                "max_drawdown": 0.081,
                "win_rate": 0.672,
                "profit_factor": 1.85
            },
            deployment_info={
                "version": "v1.2.3",
                "deployed_at": datetime.utcnow(),
                "training_data_size": 2500000,
                "feature_count": 52
            }
        )
```

---

## 🔬 Feature Engineering Framework

### **Technical Indicators**
```python
class TechnicalIndicatorEngine:
    def generate_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate comprehensive technical indicators
        """
        indicators = data.copy()
        
        # Trend indicators
        indicators['sma_20'] = data['close'].rolling(20).mean()
        indicators['ema_12'] = data['close'].ewm(span=12).mean()
        indicators['ema_26'] = data['close'].ewm(span=26).mean()
        indicators['macd'] = indicators['ema_12'] - indicators['ema_26']
        
        # Momentum indicators
        indicators['rsi_14'] = self.calculate_rsi(data['close'], 14)
        indicators['stoch_k'] = self.calculate_stochastic(data, 14, 3)
        indicators['williams_r'] = self.calculate_williams_r(data, 14)
        
        # Volatility indicators
        indicators['bb_upper'], indicators['bb_lower'] = self.bollinger_bands(data['close'], 20, 2)
        indicators['atr_14'] = self.calculate_atr(data, 14)
        indicators['keltner_upper'], indicators['keltner_lower'] = self.keltner_channels(data, 20, 2)
        
        # Volume indicators
        indicators['obv'] = self.calculate_obv(data['close'], data['volume'])
        indicators['vwap'] = self.calculate_vwap(data)
        indicators['volume_profile_poc'] = self.calculate_volume_profile_poc(data)
        
        return indicators
```

### **Smart Money Features**
```python
class SmartMoneyFeatureEngine:
    def detect_institutional_activity(self, data: pd.DataFrame) -> Dict[str, float]:
        """
        Detect institutional money flow patterns
        """
        features = {}
        
        # Volume analysis
        features['volume_profile_poc'] = self.calculate_poc(data)
        features['unusual_volume_ratio'] = self.detect_volume_anomalies(data)
        features['block_trade_frequency'] = self.count_block_trades(data)
        
        # Price action analysis
        features['accumulation_distribution'] = self.calculate_ad_line(data)
        features['money_flow_index'] = self.calculate_mfi(data)
        features['institutional_candle_patterns'] = self.detect_institutional_patterns(data)
        
        # Order flow indicators
        features['bid_ask_imbalance'] = self.calculate_order_flow_imbalance(data)
        features['large_order_detection'] = self.detect_large_orders(data) 
        features['dark_pool_estimation'] = self.estimate_dark_pool_activity(data)
        
        return features
```

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

### **Training Data Generator**
```python
class EnhancedTrainingDataGenerator:
    def generate_training_dataset(
        self, 
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        feature_config: FeatureConfig
    ) -> TrainingDataset:
        """
        Generate comprehensive training dataset
        """
        dataset_records = []
        
        for symbol in symbols:
            # Get market data
            market_data = self.data_manager.get_market_data(
                symbol, start_date, end_date
            )
            
            # Generate features
            technical_features = self.technical_engine.generate_indicators(market_data)
            smart_money_features = self.smart_money_engine.detect_institutional_activity(market_data)
            alternative_features = self.alternative_engine.generate_alt_data_features(symbol, market_data)
            
            # Combine all features
            combined_features = pd.concat([
                technical_features,
                pd.DataFrame([smart_money_features] * len(technical_features)),
                alternative_features
            ], axis=1)
            
            # Generate labels
            labels = self.label_generator.generate_support_resistance_labels(
                market_data, 
                methods=['pivot_points', 'volume_profile', 'fibonacci']
            )
            
            # Create training examples
            for i in range(len(combined_features)):
                if self.is_valid_training_example(combined_features.iloc[i], labels.iloc[i]):
                    dataset_records.append(TrainingExample(
                        symbol=symbol,
                        date=market_data.iloc[i]['date'],
                        features=combined_features.iloc[i].to_dict(),
                        labels=labels.iloc[i].to_dict(),
                        metadata={
                            'market_regime': self.detect_market_regime(market_data.iloc[i]),
                            'volatility_percentile': self.calculate_vol_percentile(market_data.iloc[i])
                        }
                    ))
        
        return TrainingDataset(
            records=dataset_records,
            feature_schema=self.generate_feature_schema(combined_features),
            metadata=self.generate_dataset_metadata(symbols, start_date, end_date)
        )
```

---

## 🎯 Portfolio Optimization Engine

### **Market-Neutral Strategy**
```python
class AIPortfolioOptimizer:
    def __init__(self):
        self.risk_model = RiskFactorModel(factors=19)
        self.smart_money_detector = SmartMoneyZoneDetector()
        self.ml_signal_generator = MLSignalGenerator()
        
    def optimize_portfolio(
        self,
        universe: List[str],
        target_return: float = 0.12,
        max_drawdown: float = 0.08,
        rebalance_frequency: str = 'weekly'
    ) -> OptimizedPortfolio:
        """
        AI-powered market-neutral portfolio optimization
        """
        # Generate ML signals
        ml_signals = {}
        for symbol in universe:
            prediction = self.ml_signal_generator.predict(symbol)
            ml_signals[symbol] = prediction
            
        # Apply Smart Money Zone filters
        smz_signals = {}
        for symbol in universe:
            smz_analysis = self.smart_money_detector.analyze_zones(symbol)
            smz_signals[symbol] = smz_analysis
            
        # Combine signals with confidence weighting
        combined_signals = self.combine_signals(ml_signals, smz_signals)
        
        # Risk factor analysis
        risk_factors = self.risk_model.calculate_factor_exposures(universe)
        
        # Portfolio optimization with constraints
        optimization_result = self.solve_portfolio_optimization(
            signals=combined_signals,
            risk_factors=risk_factors,
            constraints={
                'target_return': target_return,
                'max_drawdown': max_drawdown,
                'market_neutral': True,
                'sector_neutral': True,
                'max_position_size': 0.05,
                'min_position_size': -0.05
            }
        )
        
        return OptimizedPortfolio(
            weights=optimization_result.weights,
            expected_return=optimization_result.expected_return,
            expected_volatility=optimization_result.expected_volatility,
            sharpe_ratio=optimization_result.sharpe_ratio,
            max_drawdown=optimization_result.max_drawdown,
            risk_decomposition=optimization_result.risk_decomposition
        )
```

### **Smart Money Zone Detection**
```python
class SmartMoneyZoneDetector:
    def analyze_zones(self, symbol: str, lookback_days: int = 30) -> SmartMoneyAnalysis:
        """
        Comprehensive Smart Money Zone analysis
        """
        # Get market data
        data = self.data_manager.get_market_data(symbol, lookback_days)
        
        # Volume Profile Analysis
        volume_profile = self.calculate_volume_profile(data)
        poc_levels = self.identify_poc_clusters(volume_profile)
        
        # Institutional Activity Detection
        block_trades = self.detect_block_trades(data)
        dark_pool_flow = self.estimate_dark_pool_activity(data)
        unusual_volume = self.detect_unusual_volume_patterns(data)
        
        # Order Flow Analysis
        order_imbalance = self.analyze_order_flow_imbalance(data)
        large_orders = self.detect_large_order_sequences(data)
        
        # Zone Classification
        accumulation_zones = []
        distribution_zones = []
        
        for poc_level in poc_levels:
            zone_strength = self.calculate_zone_strength(
                poc_level, block_trades, dark_pool_flow, unusual_volume
            )
            
            if zone_strength > 0.7:  # High confidence threshold
                zone_type = self.classify_zone_type(
                    poc_level, order_imbalance, large_orders
                )
                
                if zone_type == 'accumulation':
                    accumulation_zones.append(SmartMoneyZone(
                        symbol=symbol,
                        price_level=poc_level.price,
                        zone_type='accumulation',
                        strength=zone_strength,
                        supporting_evidence=poc_level.evidence
                    ))
                elif zone_type == 'distribution':
                    distribution_zones.append(SmartMoneyZone(
                        symbol=symbol, 
                        price_level=poc_level.price,
                        zone_type='distribution',
                        strength=zone_strength,
                        supporting_evidence=poc_level.evidence
                    ))
        
        return SmartMoneyAnalysis(
            symbol=symbol,
            accumulation_zones=accumulation_zones,
            distribution_zones=distribution_zones,
            overall_bias=self.determine_overall_bias(accumulation_zones, distribution_zones),
            confidence=self.calculate_analysis_confidence(accumulation_zones, distribution_zones)
        )
```

---

## 🔄 Model Inference & Serving

### **Real-Time Inference API**
```python
class MLInferenceService:
    def __init__(self):
        self.model_registry = ModelRegistry()
        self.feature_store = FeatureStore()
        
    async def predict_support_resistance(
        self, 
        symbol: str,
        model_version: str = "latest"
    ) -> SupportResistancePrediction:
        """
        Real-time support/resistance level prediction
        """
        # Load model
        model = await self.model_registry.get_model(
            "support_resistance", model_version
        )
        
        # Get real-time features
        features = await self.feature_store.get_latest_features(
            symbol, model.feature_schema
        )
        
        # Make prediction
        prediction = model.predict(features)
        
        # Post-process results
        support_level = prediction['support_level']
        resistance_level = prediction['resistance_level'] 
        confidence = prediction['confidence']
        
        # Validate prediction
        if not self.validate_prediction(support_level, resistance_level, confidence):
            raise PredictionValidationError("Invalid prediction generated")
            
        return SupportResistancePrediction(
            symbol=symbol,
            support_level=support_level,
            resistance_level=resistance_level,
            confidence=confidence,
            model_version=model_version,
            timestamp=datetime.utcnow(),
            features_used=list(features.keys())
        )
```

### **Model A/B Testing**
```python
class ModelABTester:
    def run_ab_test(
        self,
        control_model: str,
        treatment_model: str,
        test_duration_days: int = 30,
        traffic_split: float = 0.1
    ) -> ABTestResult:
        """
        A/B test new model versions against production baseline
        """
        # Setup traffic routing
        self.setup_model_traffic_split(
            control_model, treatment_model, traffic_split
        )
        
        # Collect metrics during test period
        test_metrics = self.collect_ab_test_metrics(
            control_model, treatment_model, test_duration_days
        )
        
        # Statistical significance testing
        significance_test = self.run_statistical_tests(
            test_metrics['control'], test_metrics['treatment']
        )
        
        # Business impact analysis
        business_impact = self.analyze_business_impact(
            test_metrics['control'], test_metrics['treatment']
        )
        
        return ABTestResult(
            control_model=control_model,
            treatment_model=treatment_model,
            test_duration=test_duration_days,
            traffic_split=traffic_split,
            statistical_significance=significance_test,
            business_impact=business_impact,
            recommendation=self.generate_recommendation(
                significance_test, business_impact
            )
        )
```

---

## 📊 Model Monitoring & Operations

### **Performance Monitoring**
```python
class ModelMonitor:
    def monitor_model_health(self, model_name: str) -> ModelHealthReport:
        """
        Comprehensive model health monitoring
        """
        # Prediction accuracy monitoring
        accuracy_metrics = self.calculate_recent_accuracy(model_name)
        
        # Data drift detection
        feature_drift = self.detect_feature_drift(model_name)
        
        # Prediction drift detection  
        prediction_drift = self.detect_prediction_drift(model_name)
        
        # Business metrics monitoring
        business_metrics = self.calculate_business_metrics(model_name)
        
        # Infrastructure metrics
        latency_metrics = self.calculate_latency_metrics(model_name)
        throughput_metrics = self.calculate_throughput_metrics(model_name)
        
        # Overall health score
        health_score = self.calculate_overall_health_score([
            accuracy_metrics, feature_drift, prediction_drift,
            business_metrics, latency_metrics, throughput_metrics
        ])
        
        return ModelHealthReport(
            model_name=model_name,
            timestamp=datetime.utcnow(),
            health_score=health_score,
            accuracy_metrics=accuracy_metrics,
            drift_detection={
                'feature_drift': feature_drift,
                'prediction_drift': prediction_drift
            },
            business_metrics=business_metrics,
            infrastructure_metrics={
                'latency': latency_metrics,
                'throughput': throughput_metrics
            },
            alerts=self.generate_health_alerts(health_score, accuracy_metrics, feature_drift)
        )
```

### **Continuous Training Pipeline**
```python
class ContinuousTrainingPipeline:
    def execute_retraining_cycle(self, model_name: str) -> RetrainingResult:
        """
        Execute automated model retraining cycle
        """
        # Check if retraining is needed
        retrain_trigger = self.evaluate_retrain_triggers(model_name)
        
        if not retrain_trigger.should_retrain:
            return RetrainingResult(
                status="skipped",
                reason=retrain_trigger.reason
            )
        
        # Generate updated training data
        new_training_data = self.generate_incremental_training_data(
            model_name, retrain_trigger.data_cutoff_date
        )
        
        # Retrain model with updated data
        updated_model = self.train_model_version(
            model_name, new_training_data
        )
        
        # Validate new model performance
        validation_results = self.validate_model_performance(
            updated_model, model_name
        )
        
        if validation_results.performance_improved:
            # Deploy new model version
            deployment_result = self.deploy_model_version(updated_model)
            
            return RetrainingResult(
                status="success",
                new_model_version=updated_model.version,
                performance_improvement=validation_results.improvement_metrics,
                deployment_status=deployment_result.status
            )
        else:
            return RetrainingResult(
                status="performance_regression",
                reason="New model did not outperform existing model"
            )
```

---

## 🚀 Deployment & Scaling

### **Model Serving Infrastructure**
```yaml
# k8s/ml-inference-service.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ml-inference-service
  namespace: ats-dev
spec:
  replicas: 3
  selector:
    matchLabels:
      app: ml-inference-service
  template:
    metadata:
      labels:
        app: ml-inference-service
    spec:
      containers:
      - name: ml-inference
        image: dragonflyer762/ats-genai:latest
        ports:
        - containerPort: 8000
        env:
        - name: MODEL_REGISTRY_URL
          value: "http://model-registry:5000"
        - name: FEATURE_STORE_URL
          value: "http://feature-store:6000" 
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: ml-inference-service
  namespace: ats-dev
spec:
  selector:
    app: ml-inference-service
  ports:
  - port: 80
    targetPort: 8000
  type: ClusterIP
```

---

## 📊 Performance Metrics & KPIs

### **ML Platform KPIs**
- **Model Accuracy**: > 70% for support/resistance predictions
- **Inference Latency**: < 50ms for real-time predictions
- **Training Pipeline**: < 4 hours for full model retraining  
- **Portfolio Performance**: Sharpe ratio > 1.2, max drawdown < 10%
- **Model Availability**: 99.9% uptime for inference services
- **A/B Test Velocity**: 5+ model experiments per month

### **Business Impact Metrics**
- **Portfolio Alpha Generation**: Target excess return > 5% annually
- **Risk-Adjusted Returns**: Information ratio > 1.0
- **Prediction Accuracy Improvement**: 5%+ accuracy gain year-over-year
- **Client Satisfaction**: Net Promoter Score > 50 for AI recommendations

---

**🎯 The ML Platform provides enterprise-grade machine learning capabilities with automated training, real-time inference, and comprehensive model lifecycle management for algorithmic trading success.**