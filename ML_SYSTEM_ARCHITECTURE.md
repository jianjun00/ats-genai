# ML System Architecture: Training, Prediction & Portfolio Optimization

## System Overview

This document describes the comprehensive ML system architecture for the ATS (Algorithmic Trading System) that implements:

1. **Training Data Generation** - Automated feature extraction and labeling
2. **Model Training Pipeline** - Initial training and continuous adaptation
3. **Backtest Prediction Workflow** - Real-time inference during simulation
4. **Portfolio Optimization** - Signal-to-portfolio conversion with risk management
5. **Continuous Training System** - Daily model updates and adaptation

---

## 🏗️ System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              ML TRAINING & PREDICTION SYSTEM                    │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw Market    │    │   Economic      │    │   Alternative   │
│   Data Sources  │    │   Data Sources  │    │   Data Sources  │
│                 │    │                 │    │                 │
│ • Daily Prices  │    │ • Fed Data      │    │ • News/Events   │
│ • Minute Data   │    │ • Economic      │    │ • Sentiment     │
│ • Volume        │    │   Indicators    │    │ • Social Media  │
│ • Splits/Divs   │    │ • Treasury      │    │ • Analyst Rec   │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           1. TRAINING DATA GENERATION                           │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │ SupportResistanceTrainingGenerator (src/ml/training_data/)              ││
│  │                                                                         ││
│  │ ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          ││
│  │ │ Feature         │  │ Technical       │  │ Market          │          ││
│  │ │ Engineering     │  │ Indicators      │  │ Microstructure  │          ││
│  │ │                 │  │                 │  │                 │          ││
│  │ │ • Price Ratios  │  │ • RSI, MACD     │  │ • Order Flow    │          ││
│  │ │ • Volatility    │  │ • Moving Avgs   │  │ • Bid/Ask       │          ││
│  │ │ • Volume Ratios │  │ • Bollinger     │  │ • Level 2 Data  │          ││
│  │ │ • Momentum      │  │ • Stochastic    │  │ • Time & Sales  │          ││
│  │ └─────────────────┘  └─────────────────┘  └─────────────────┘          ││
│  │                                │                                        ││
│  │                                ▼                                        ││
│  │ ┌─────────────────────────────────────────────────────────────────────┐ ││
│  │ │           LABEL GENERATION SYSTEM                                   │ ││
│  │ │                                                                     │ ││
│  │ │ • Identify intraday support/resistance levels                      │ ││
│  │ │ • Calculate level strength and test frequency                      │ ││
│  │ │ • Measure volume at levels and time held                          │ ││
│  │ │ • Generate binary/regression targets for next-day prediction       │ ││
│  │ │ • Create confidence scores for each level                          │ ││
│  │ └─────────────────────────────────────────────────────────────────────┘ ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                         │                                        │
│                                         ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │                        TRAINING EXAMPLES OUTPUT                             ││
│  │                                                                             ││
│  │ TrainingExample {                                                           ││
│  │   symbol: str                                                               ││
│  │   date: date                                                                ││
│  │   features: Dict[str, float]  # 50+ engineered features                    ││
│  │   next_day_support_levels: List[SupportResistanceLevel]                    ││
│  │   next_day_resistance_levels: List[SupportResistanceLevel]                 ││
│  │   next_day_high/low/close/volume: float                                    ││
│  │ }                                                                           ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           2. MODEL TRAINING PIPELINE                            │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │ AdaptiveSupportResistanceModel (src/ml/dynamic_training/)               ││
│  │                                                                         ││
│  │ BOOTSTRAP PHASE (Initial Training)                                     ││
│  │ ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          ││
│  │ │ Historical      │  │ Model           │  │ Ensemble        │          ││
│  │ │ Data Loading    │  │ Architecture    │  │ Training        │          ││
│  │ │                 │  │                 │  │                 │          ││
│  │ │ • 2-4 years     │  │ • Neural Net    │  │ • Random Forest │          ││
│  │ │ • 5000+ samples │  │ • Transformer   │  │ • XGBoost       │          ││
│  │ │ • Multi-symbol  │  │ • LSTM/GRU      │  │ • Linear Models │          ││
│  │ │ • Validation    │  │ • Attention     │  │ • Voting/Stack  │          ││
│  │ └─────────────────┘  └─────────────────┘  └─────────────────┘          ││
│  │                                │                                        ││
│  │                                ▼                                        ││
│  │ ┌─────────────────────────────────────────────────────────────────────┐ ││
│  │ │           INCREMENTAL LEARNING SYSTEM                               │ ││
│  │ │                                                                     │ ││
│  │ │ Daily Training Updates:                                             │ ││
│  │ │ • Rolling window training (365 days)                               │ ││
│  │ │ • Incremental model updates                                         │ ││
│  │ │ • Learning rate decay (0.95)                                       │ ││
│  │ │ • Model memory weighting (0.8)                                     │ ││
│  │ │ • Performance monitoring (30-day lookback)                         │ ││
│  │ └─────────────────────────────────────────────────────────────────────┘ ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                         │                                        │
│                                         ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │                           TRAINED MODEL OUTPUT                             ││
│  │                                                                             ││
│  │ SupportResistanceEnsemble {                                                 ││
│  │   models: List[BaseModel]                                                   ││
│  │   weights: List[float]                                                      ││
│  │   feature_importance: Dict[str, float]                                     ││
│  │   performance_metrics: Dict[str, float]                                    ││
│  │   last_updated: datetime                                                    ││
│  │   version: int                                                              ││
│  │ }                                                                           ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        3. BACKTEST PREDICTION WORKFLOW                          │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │ AdaptiveBacktester (src/ml/evaluation/)                                ││
│  │                                                                         ││
│  │ ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          ││
│  │ │ Daily Data      │  │ Feature         │  │ Model           │          ││
│  │ │ Processing      │  │ Preparation     │  │ Inference       │          ││
│  │ │                 │  │                 │  │                 │          ││
│  │ │ • Market Open   │  │ • Standardize   │  │ • Ensemble      │          ││
│  │ │ • EOD Features  │  │ • Normalize     │  │ • Prediction    │          ││
│  │ │ • Tech Analysis │  │ • Missing Data  │  │ • Confidence    │          ││
│  │ │ • Risk Factors  │  │ • Feature Sel   │  │ • Uncertainty   │          ││
│  │ └─────────────────┘  └─────────────────┘  └─────────────────┘          ││
│  │                                │                                        ││
│  │                                ▼                                        ││
│  │ ┌─────────────────────────────────────────────────────────────────────┐ ││
│  │ │                   PREDICTION GENERATION                             │ ││
│  │ │                                                                     │ ││
│  │ │ For each symbol in universe:                                        │ ││
│  │ │ • Generate next-day support/resistance levels                      │ ││
│  │ │ • Calculate confidence scores (0.0-1.0)                           │ ││
│  │ │ • Estimate probability of level tests                              │ ││
│  │ │ • Generate risk-adjusted signals                                   │ ││
│  │ │ • Store predictions for evaluation                                 │ ││
│  │ └─────────────────────────────────────────────────────────────────────┘ ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                         │                                        │
│                                         ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │                         PREDICTION RESULTS                                  ││
│  │                                                                             ││
│  │ PredictionResult {                                                          ││
│  │   symbol: str                                                               ││
│  │   prediction_date: date                                                     ││
│  │   support_levels: List[(level, confidence)]                                ││
│  │   resistance_levels: List[(level, confidence)]                             ││
│  │   expected_return: float                                                    ││
│  │   risk_score: float                                                         ││
│  │   signal_strength: float                                                    ││
│  │ }                                                                           ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      4. PORTFOLIO OPTIMIZATION PROCESS                          │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │ Portfolio Optimization Engine (src/portfolio/)                         ││
│  │                                                                         ││
│  │ ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          ││
│  │ │ Signal          │  │ Risk Model      │  │ Optimization    │          ││
│  │ │ Processing      │  │ Construction    │  │ Engine          │          ││
│  │ │                 │  │                 │  │                 │          ││
│  │ │ • Aggregate     │  │ • Factor Model  │  │ • Mean-Variance │          ││
│  │ │ • Filter        │  │ • Correlation   │  │ • Risk Parity   │          ││
│  │ │ • Weight        │  │ • Volatility    │  │ • Black-Litter  │          ││
│  │ │ • Rank          │  │ • Beta Exposure │  │ • Kelly Optimal │          ││
│  │ └─────────────────┘  └─────────────────┘  └─────────────────┘          ││
│  │                                │                                        ││
│  │                                ▼                                        ││
│  │ ┌─────────────────────────────────────────────────────────────────────┐ ││
│  │ │                    CONSTRAINT MANAGEMENT                            │ ││
│  │ │                                                                     │ ││
│  │ │ Position Constraints:                                               │ ││
│  │ │ • Max position weight: 5%                                          │ ││
│  │ │ • Max sector exposure: 20%                                         │ ││
│  │ │ • Max leverage: 2x gross                                           │ ││
│  │ │ • Transaction costs: 5 bps                                         │ ││
│  │ │                                                                     │ ││
│  │ │ Risk Constraints:                                                   │ ││
│  │ │ • Market beta: ±5%                                                 │ ││
│  │ │ • Sector beta: ±15%                                                │ ││
│  │ │ • Max volatility: 15%                                              │ ││
│  │ │ • Target Sharpe: 1.5                                               │ ││
│  │ │ • Dollar neutral: ±10%                                             │ ││
│  │ └─────────────────────────────────────────────────────────────────────┘ ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                         │                                        │
│                                         ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │                         OPTIMIZED PORTFOLIO                                 ││
│  │                                                                             ││
│  │ OptimizationResult {                                                        ││
│  │   weights: Dict[str, float]  # Symbol -> Position Weight                   ││
│  │   expected_return: float                                                    ││
│  │   expected_volatility: float                                                ││
│  │   sharpe_ratio: float                                                       ││
│  │   factor_exposures: Dict[str, float]                                       ││
│  │   gross_exposure: float                                                     ││
│  │   net_exposure: float                                                       ││
│  │   transaction_costs: float                                                  ││
│  │ }                                                                           ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        5. CONTINUOUS TRAINING SYSTEM                            │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │ Daily Model Update Cycle                                                ││
│  │                                                                         ││
│  │ ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          ││
│  │ │ End of Day      │  │ Performance     │  │ Model Update    │          ││
│  │ │ Processing      │  │ Evaluation      │  │ Decision        │          ││
│  │ │                 │  │                 │  │                 │          ││
│  │ │ • New Data      │  │ • Accuracy      │  │ • Full Retrain  │          ││
│  │ │ • Validation    │  │ • Drift         │  │ • Incremental   │          ││
│  │ │ • Feature Eng   │  │ • Calibration   │  │ • Parameter     │          ││
│  │ │ • Label Gen     │  │ • Metrics       │  │ • Skip Update   │          ││
│  │ └─────────────────┘  └─────────────────┘  └─────────────────┘          ││
│  │                                │                                        ││
│  │                                ▼                                        ││
│  │ ┌─────────────────────────────────────────────────────────────────────┐ ││
│  │ │                   ADAPTIVE TRAINING LOGIC                          │ ││
│  │ │                                                                     │ ││
│  │ │ Decision Tree:                                                      │ ││
│  │ │ • IF accuracy < 40% → Full retrain with expanded data             │ ││
│  │ │ • IF drift detected → Incremental update with new samples         │ ││
│  │ │ • IF new regime detected → Hybrid ensemble approach               │ ││
│  │ │ • IF stable performance → Parameter fine-tuning only              │ ││
│  │ │ • ELSE → Continue with current model                               │ ││
│  │ │                                                                     │ ││
│  │ │ Rolling Window Management:                                          │ ││
│  │ │ • Maintain 365-day training window                                 │ ││
│  │ │ • Add new day, drop oldest day                                     │ ││
│  │ │ • Adjust learning rates dynamically                                │ ││
│  │ │ • Monitor regime change indicators                                  │ ││
│  │ └─────────────────────────────────────────────────────────────────────┘ ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
│                                         │                                        │
│                                         ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │                         UPDATED MODEL STATE                                 ││
│  │                                                                             ││
│  │ AdaptiveModelState {                                                        ││
│  │   last_retrain_date: date                                                   ││
│  │   total_training_examples: int                                              ││
│  │   recent_performance: List[float]                                          ││
│  │   model_version: int                                                        ││
│  │   training_history: List[Dict]                                             ││
│  │   bootstrap_completed: bool                                                 ││
│  │ }                                                                           ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Detailed Component Documentation

### 1. Training Data Generation

**Purpose**: Generate labeled training examples from historical market data for support/resistance prediction.

**Key Components**:
- `SupportResistanceTrainingGenerator` (src/ml/training_data/support_resistance_generator.py)
- Feature engineering pipeline
- Label generation system

**Process**:
1. **Feature Engineering**:
   - Price ratios and momentum indicators
   - Volume analysis and flow metrics
   - Technical indicators (RSI, MACD, Bollinger Bands)
   - Market microstructure features
   - Cross-asset correlation features

2. **Label Generation**:
   - Identify intraday support/resistance levels using minute data
   - Calculate level strength based on:
     - Number of times level was tested
     - Volume at each test
     - Time level held before break
     - Break-through behavior
   - Generate binary and regression targets for next-day prediction

3. **Quality Control**:
   - Minimum level strength thresholds
   - Volume validation requirements
   - Time-based filtering
   - Outlier detection and removal

**Output**: `TrainingExample` objects containing 50+ features and labeled support/resistance levels.

### 2. Model Training Pipeline

**Purpose**: Train and maintain adaptive ML models that learn from new data daily.

**Key Components**:
- `AdaptiveSupportResistanceModel` (src/ml/dynamic_training/adaptive_sr_model.py)
- `SupportResistanceEnsemble` (src/ml/models/support_resistance_model.py)
- Incremental learning system

**Training Phases**:

#### Bootstrap Phase (Initial Training)
- **Data**: 2-4 years of historical data (5000+ examples)
- **Models**: Ensemble of Neural Networks, Random Forest, XGBoost
- **Validation**: Time-series cross-validation
- **Duration**: One-time setup, ~2-4 hours

#### Continuous Training Phase (Daily Updates)
- **Trigger**: Every trading day after market close
- **Data**: Rolling 365-day window + new day's data
- **Methods**: 
  - Incremental learning for compatible models
  - Full retraining when performance degrades
  - Ensemble weight adjustment
- **Performance Monitoring**: 30-day rolling accuracy metrics

**Model Architecture**:
```python
SupportResistanceEnsemble:
  - Neural Network (PyTorch): 128→64→32 architecture
  - Random Forest: 100 trees, adaptive depth
  - XGBoost: Gradient boosting with early stopping
  - Linear Models: Ridge/Lasso for baseline
  - Ensemble: Weighted voting based on recent performance
```

### 3. Backtest Prediction Workflow

**Purpose**: Generate daily predictions during backtesting simulation with realistic constraints.

**Key Components**:
- `AdaptiveBacktester` (src/ml/evaluation/adaptive_backtester.py)
- Daily prediction pipeline
- Performance tracking system

**Daily Prediction Process**:
1. **Market Open**: Process overnight news and pre-market data
2. **Feature Preparation**: 
   - Calculate technical indicators using available data
   - Normalize features using historical statistics
   - Handle missing data with forward-fill/interpolation
3. **Model Inference**: 
   - Generate ensemble predictions
   - Calculate confidence intervals
   - Apply uncertainty quantification
4. **Signal Generation**: 
   - Convert predictions to actionable signals
   - Apply minimum confidence thresholds
   - Risk-adjust signal strength

**Prediction Output**:
```python
PredictionResult:
  - support_levels: [(price, confidence), ...]
  - resistance_levels: [(price, confidence), ...]
  - expected_return: float
  - risk_score: float (0-1)
  - signal_strength: float (-1 to 1)
```

### 4. Portfolio Optimization Process

**Purpose**: Convert ML signals into risk-managed portfolio positions.

**Key Components**:
- Portfolio optimization engine (src/portfolio/optimization.py)
- Factor risk model (src/portfolio/factor_framework.py)
- Constraint management system

**Optimization Steps**:
1. **Signal Aggregation**: 
   - Combine multiple model predictions
   - Weight by confidence and recent performance
   - Apply signal decay for aging predictions

2. **Risk Model Construction**:
   - Factor loading estimation (market, sector, style)
   - Covariance matrix construction
   - Risk decomposition and attribution

3. **Optimization Engine**:
   - Objective: Maximize risk-adjusted return
   - Method: Mean-variance optimization with constraints
   - Solver: CVXPY or scipy.optimize

4. **Constraint Application**:
   - Position limits: 5% max per stock
   - Sector limits: 20% max per sector
   - Factor exposure limits: Market beta ±5%
   - Leverage limits: 2x gross exposure max
   - Transaction cost modeling: 5 bps per trade

**Portfolio Output**:
```python
OptimizationResult:
  - weights: Dict[symbol, weight]
  - expected_return: 12% annual
  - expected_volatility: 8% annual
  - sharpe_ratio: 1.5
  - gross_exposure: 180% (1.8x leverage)
  - net_exposure: ±5% (market neutral)
```

### 5. Continuous Training System

**Purpose**: Maintain model performance through adaptive learning and regime detection.

**Daily Update Cycle**:

#### End-of-Day Processing (5:00 PM ET)
1. **Data Collection**: 
   - Download new daily/minute market data
   - Validate data quality and completeness
   - Store in time-series database

2. **Feature Engineering**: 
   - Calculate new technical indicators
   - Update rolling statistics
   - Generate cross-sectional features

3. **Label Generation**: 
   - Identify support/resistance levels from intraday data
   - Calculate level characteristics and strength
   - Create training labels for new examples

#### Performance Evaluation (6:00 PM ET)
1. **Prediction Accuracy**: 
   - Evaluate previous day's predictions
   - Calculate support/resistance hit rates
   - Measure confidence calibration

2. **Model Drift Detection**: 
   - Monitor feature distributions
   - Track prediction residuals
   - Detect regime changes

3. **Portfolio Performance**: 
   - Calculate risk-adjusted returns
   - Analyze factor attribution
   - Monitor drawdown metrics

#### Model Update Decision (7:00 PM ET)
```python
def decide_training_approach(performance_metrics):
    if accuracy < 0.40:
        return "full_retrain"
    elif drift_detected():
        return "incremental_update"
    elif new_regime_detected():
        return "ensemble_reweight"
    elif stable_performance():
        return "parameter_tune"
    else:
        return "continue"
```

#### Training Execution (8:00 PM - 10:00 PM ET)
- **Full Retrain**: 2-3 hours, expanded dataset
- **Incremental Update**: 15-30 minutes, new data only
- **Ensemble Reweight**: 5-10 minutes, weight optimization
- **Parameter Tune**: 10-15 minutes, hyperparameter adjustment

---

## 🔄 Data Flow and Dependencies

### Data Dependencies
```
Raw Market Data → Feature Engineering → Training Examples → Model Training → Predictions → Portfolio Optimization → Trading Signals
```

### Timing Dependencies
```
Market Close (4:00 PM) → Data Processing (4:30 PM) → Feature Engineering (5:00 PM) → Model Training (6:00 PM) → Portfolio Construction (8:00 PM) → Trade Preparation (9:00 PM)
```

### System Dependencies
```
Database Layer → DAOs → ML Pipeline → Portfolio Engine → Backtest Framework → Performance Analytics
```

---

## 📈 Performance Monitoring

### Model Performance Metrics
- **Accuracy**: Support/resistance level hit rate (target: >65%)
- **Calibration**: Confidence vs actual accuracy correlation (target: >0.7)
- **Stability**: Performance consistency over time (target: <20% volatility)
- **Speed**: Training time constraints (target: <30 min daily)

### Portfolio Performance Metrics
- **Return**: Risk-adjusted returns (target: >12% annual)
- **Sharpe Ratio**: Risk-adjusted performance (target: >1.5)
- **Max Drawdown**: Worst-case losses (target: <8%)
- **Market Neutrality**: Beta exposure (target: <5%)

### System Performance Metrics
- **Latency**: End-to-end processing time (target: <2 hours)
- **Reliability**: System uptime (target: >99.9%)
- **Scalability**: Universe size capacity (target: >2000 stocks)
- **Data Quality**: Missing/corrupt data rate (target: <0.1%)

---

## 🛠️ Implementation Details

### Technology Stack
- **ML Framework**: PyTorch + scikit-learn + XGBoost
- **Optimization**: CVXPY + scipy.optimize
- **Database**: PostgreSQL + TimescaleDB
- **Scheduling**: Cron + Kubernetes Jobs
- **Monitoring**: Prometheus + Grafana
- **Compute**: Ray for parallel processing

### File Structure
```
src/ml/
├── training_data/
│   └── support_resistance_generator.py
├── models/
│   └── support_resistance_model.py
├── dynamic_training/
│   └── adaptive_sr_model.py
├── evaluation/
│   ├── adaptive_backtester.py
│   └── sr_backtester.py
└── portfolio/
    ├── optimization.py
    ├── factor_framework.py
    └── signal_generation.py
```

### Configuration Management
```python
# Model Configuration
@gin.configurable
class AdaptiveModelConfig:
    bootstrap_years: int = 3
    rolling_window_days: int = 365
    retrain_frequency_days: int = 1
    learning_rate_decay: float = 0.95
    performance_lookback_days: int = 30

# Portfolio Configuration  
@gin.configurable
class OptimizationConstraints:
    max_position_weight: float = 0.05
    max_leverage: float = 2.0
    transaction_cost_bps: float = 5.0
    target_sharpe_ratio: float = 1.5
```

This comprehensive system provides a production-ready ML pipeline that adapts to changing market conditions while maintaining rigorous risk management and performance monitoring.