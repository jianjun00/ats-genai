# Issue #15: Adaptive Model Selection Framework

## 📋 Summary
Implement an adaptive model selection system that automatically chooses between Linear, LSTM, Transformer, and Hybrid models based on market conditions, following 2024-2025 research showing that simple linear models sometimes outperform complex transformers.

## 🎯 Objectives
- [ ] Create adaptive model selection framework
- [ ] Implement market regime detection
- [ ] Add model performance monitoring and switching
- [ ] Support for Linear, LSTM, Transformer, and Hybrid models
- [ ] Real-time model selection based on market conditions

## 🔧 Technical Requirements

### Adaptive Model Selector
```python
class AdaptiveModelSelector:
    """Selects optimal model based on market conditions"""
    
    def __init__(self):
        self.models = {
            'linear': LinearTimeSeriesModel(),
            'lstm': LSTMModel(), 
            'transformer': TransformerModel(),
            'tft': TemporalFusionTransformer(),
            'hybrid': HybridLSTMTransformer()
        }
        
        self.regime_detector = MarketRegimeDetector()
        self.performance_monitor = ModelPerformanceMonitor()
        self.selection_strategy = ModelSelectionStrategy()
    
    async def select_model(self, market_data: pd.DataFrame, 
                          current_conditions: dict) -> str:
        # Detect current market regime
        regime = await self.regime_detector.detect_regime(market_data)
        
        # Get recent model performance 
        performance_scores = await self.performance_monitor.get_recent_scores()
        
        # Select optimal model for current conditions
        selected_model = self.selection_strategy.select(
            regime, performance_scores, current_conditions
        )
        
        return selected_model
```

### Market Regime Detection
```python
class MarketRegimeDetector:
    """Detects market regimes for model selection"""
    
    def __init__(self):
        self.regime_models = {
            'volatility': VolatilityRegimeModel(),
            'trend': TrendRegimeModel(),
            'correlation': CorrelationRegimeModel(),
            'liquidity': LiquidityRegimeModel()
        }
    
    async def detect_regime(self, market_data: pd.DataFrame) -> MarketRegime:
        regimes = {}
        
        # Detect different regime aspects
        for regime_type, model in self.regime_models.items():
            regimes[regime_type] = await model.detect(market_data)
        
        # Combine into overall market regime
        overall_regime = self._combine_regimes(regimes)
        
        return overall_regime
    
    def _combine_regimes(self, regimes: dict) -> MarketRegime:
        """Combine multiple regime signals"""
        return MarketRegime(
            volatility=regimes['volatility'],
            trend=regimes['trend'], 
            correlation=regimes['correlation'],
            liquidity=regimes['liquidity'],
            overall_classification=self._classify_overall_regime(regimes)
        )
```

### Model Performance Monitor
```python
class ModelPerformanceMonitor:
    """Monitors and tracks model performance across regimes"""
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.performance_history = defaultdict(list)
        self.regime_performance = defaultdict(dict)
    
    async def update_performance(self, model_name: str, 
                               predictions: np.ndarray,
                               actuals: np.ndarray,
                               regime: MarketRegime):
        # Calculate performance metrics
        metrics = self._calculate_metrics(predictions, actuals)
        
        # Update rolling performance
        self.performance_history[model_name].append(metrics)
        if len(self.performance_history[model_name]) > self.window_size:
            self.performance_history[model_name].pop(0)
        
        # Update regime-specific performance
        regime_key = regime.overall_classification
        if regime_key not in self.regime_performance[model_name]:
            self.regime_performance[model_name][regime_key] = []
        
        self.regime_performance[model_name][regime_key].append(metrics)
    
    async def get_best_model_for_regime(self, regime: MarketRegime) -> str:
        """Get best performing model for specific regime"""
        regime_key = regime.overall_classification
        
        best_model = None
        best_score = float('-inf')
        
        for model_name, regime_scores in self.regime_performance.items():
            if regime_key in regime_scores:
                avg_score = np.mean([s.sharpe_ratio for s in regime_scores[regime_key][-20:]])
                if avg_score > best_score:
                    best_score = avg_score
                    best_model = model_name
        
        return best_model or 'tft'  # Default to TFT
```

### Model Ensemble System
```python
class AdaptiveModelEnsemble:
    """Ensemble system with adaptive weighting"""
    
    def __init__(self, models: Dict[str, nn.Module]):
        self.models = models
        self.adaptive_weights = AdaptiveWeightingSystem()
        self.performance_tracker = EnsemblePerformanceTracker()
    
    async def predict(self, input_data: torch.Tensor, 
                     market_regime: MarketRegime) -> torch.Tensor:
        # Get predictions from all models
        predictions = {}
        for name, model in self.models.items():
            with torch.no_grad():
                pred = await self._safe_predict(model, input_data)
                predictions[name] = pred
        
        # Get adaptive weights based on regime and recent performance
        weights = await self.adaptive_weights.compute_weights(
            market_regime, self.performance_tracker.get_recent_scores()
        )
        
        # Weighted ensemble prediction
        ensemble_pred = self._weighted_combination(predictions, weights)
        
        return ensemble_pred
    
    async def _safe_predict(self, model: nn.Module, input_data: torch.Tensor):
        """Safe prediction with error handling"""
        try:
            return model(input_data)
        except Exception as e:
            logger.warning(f"Model prediction failed: {e}")
            # Return neutral prediction as fallback
            return torch.zeros_like(input_data[:, -1:, :])
```

## 📁 File Structure
```
src/models/adaptive/
├── model_selector.py              # Main adaptive selector
├── regime_detector.py             # Market regime detection
├── performance_monitor.py         # Model performance tracking
├── selection_strategy.py          # Selection algorithms
├── adaptive_ensemble.py           # Ensemble system
└── regime_models/                 # Specific regime detectors
    ├── volatility_regime.py
    ├── trend_regime.py
    ├── correlation_regime.py
    └── liquidity_regime.py

src/models/baselines/
├── linear_model.py               # Simple linear baseline
├── lstm_model.py                 # LSTM baseline
└── hybrid_models.py              # Hybrid combinations

tests/models/adaptive/
├── test_model_selector.py
├── test_regime_detector.py
├── test_performance_monitor.py
└── test_adaptive_ensemble.py
```

## 🧪 Acceptance Criteria
- [ ] Adaptive selector chooses appropriate models based on market regime
- [ ] Linear models used effectively in suitable market conditions
- [ ] Performance monitoring tracks model effectiveness over time
- [ ] Real-time regime detection works with live market data
- [ ] Ensemble system provides robust fallback mechanism
- [ ] Model switching decisions are explainable and auditable

## 🔗 Dependencies
- [ ] scikit-learn (for linear models)
- [ ] existing TFT and LSTM implementations
- [ ] market data infrastructure

## 📊 Performance Targets
- Model selection decision: <500ms
- Regime detection: <1s for 1000 data points
- Performance tracking overhead: <5% of prediction time
- Selection accuracy: >70% improvement over static model choice
- Ensemble prediction: <200ms per batch

## 🏷️ Labels
`enhancement`, `ml-models`, `adaptive`, `phase-3`

## 👥 Assignee
ML Team + Quant Team

## 🕒 Timeline
**Sprint 1** (Week 1-3)
- Design adaptive selection framework
- Implement market regime detection
- Create performance monitoring system

**Sprint 2** (Week 4-6)
- Model selection strategies
- Ensemble system implementation
- Integration testing

**Sprint 3** (Week 7-8)
- Performance optimization
- Real-time deployment
- Monitoring and alerting

---
**Priority:** Medium  
**Complexity:** High  
**Phase:** 3