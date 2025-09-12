"""
Advanced Analytics and ML Service Interface

Provides comprehensive financial analytics, machine learning, and quantitative analysis capabilities.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Union, AsyncIterator, Tuple, Callable
from dataclasses import dataclass
from enum import Enum
import numpy as np
import pandas as pd


class AnalyticsType(Enum):
    """Types of analytics."""
    TECHNICAL = "technical"
    FUNDAMENTAL = "fundamental"
    SENTIMENT = "sentiment"
    RISK = "risk"
    PERFORMANCE = "performance"
    CORRELATION = "correlation"
    VOLATILITY = "volatility"
    MOMENTUM = "momentum"


class MLModelType(Enum):
    """Machine learning model types."""
    PRICE_PREDICTION = "price_prediction"
    VOLATILITY_PREDICTION = "volatility_prediction"
    TREND_CLASSIFICATION = "trend_classification"
    ANOMALY_DETECTION = "anomaly_detection"
    SENTIMENT_ANALYSIS = "sentiment_analysis"
    RISK_ASSESSMENT = "risk_assessment"
    PORTFOLIO_OPTIMIZATION = "portfolio_optimization"
    PATTERN_RECOGNITION = "pattern_recognition"


class ModelStatus(Enum):
    """ML model status."""
    TRAINING = "training"
    TRAINED = "trained"
    VALIDATING = "validating"
    DEPLOYED = "deployed"
    FAILED = "failed"
    DEPRECATED = "deprecated"


class SignalType(Enum):
    """Trading signal types."""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    STRONG_BUY = "strong_buy"
    STRONG_SELL = "strong_sell"


@dataclass
class TechnicalIndicator:
    """Technical indicator result."""
    indicator_name: str
    symbol: str
    timestamp: datetime
    value: Decimal
    signal: Optional[SignalType]
    confidence: float
    parameters: Dict[str, Any]
    timeframe: str


@dataclass
class AnalyticsResult:
    """Analytics computation result."""
    analysis_id: str
    symbol: str
    analysis_type: AnalyticsType
    timestamp: datetime
    results: Dict[str, Any]
    confidence_score: float
    validity_period: timedelta
    metadata: Dict[str, Any]


@dataclass
class MLModelConfig:
    """ML model configuration."""
    model_id: str
    model_name: str
    model_type: MLModelType
    algorithm: str
    features: List[str]
    target_variable: str
    hyperparameters: Dict[str, Any]
    training_config: Dict[str, Any]
    validation_config: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass
class MLModelMetrics:
    """ML model performance metrics."""
    model_id: str
    evaluation_timestamp: datetime
    training_metrics: Dict[str, float]
    validation_metrics: Dict[str, float]
    test_metrics: Optional[Dict[str, float]]
    feature_importance: Dict[str, float]
    model_size_mb: float
    inference_time_ms: float


@dataclass
class Prediction:
    """ML model prediction result."""
    prediction_id: str
    model_id: str
    symbol: str
    timestamp: datetime
    prediction_timestamp: datetime  # When prediction is for
    prediction_value: Union[float, str, Dict[str, Any]]
    confidence: float
    prediction_interval: Optional[Tuple[float, float]]
    features_used: Dict[str, Any]
    explanation: Optional[Dict[str, Any]]


@dataclass
class FeatureSet:
    """Feature engineering result."""
    feature_set_id: str
    symbol: str
    timestamp: datetime
    features: Dict[str, float]
    feature_metadata: Dict[str, Dict[str, Any]]
    quality_score: float
    timeframe: str


@dataclass
class BacktestConfig:
    """Backtesting configuration."""
    backtest_id: str
    strategy_name: str
    symbol: str
    start_date: datetime
    end_date: datetime
    initial_capital: Decimal
    commission_rate: float
    slippage_rate: float
    risk_parameters: Dict[str, Any]
    benchmark_symbol: Optional[str]


@dataclass
class BacktestResult:
    """Backtesting result."""
    backtest_id: str
    strategy_name: str
    symbol: str
    performance_metrics: Dict[str, float]
    trades: List[Dict[str, Any]]
    equity_curve: List[Dict[str, Any]]
    drawdown_analysis: Dict[str, Any]
    risk_metrics: Dict[str, Any]
    benchmark_comparison: Optional[Dict[str, Any]]
    execution_time: float


@dataclass
class QuantitativeMetrics:
    """Quantitative analysis metrics."""
    symbol: str
    timestamp: datetime
    sharpe_ratio: Optional[float]
    sortino_ratio: Optional[float]
    calmar_ratio: Optional[float]
    max_drawdown: Optional[float]
    volatility: float
    skewness: Optional[float]
    kurtosis: Optional[float]
    var_95: Optional[float]
    var_99: Optional[float]
    expected_shortfall: Optional[float]
    beta: Optional[float]
    alpha: Optional[float]
    information_ratio: Optional[float]


@dataclass
class CorrelationAnalysis:
    """Correlation analysis result."""
    analysis_id: str
    symbols: List[str]
    timestamp: datetime
    correlation_matrix: Dict[str, Dict[str, float]]
    rolling_correlations: Dict[str, List[Dict[str, Any]]]
    correlation_clusters: List[List[str]]
    stability_metrics: Dict[str, float]


@dataclass
class SentimentAnalysis:
    """Sentiment analysis result."""
    analysis_id: str
    symbol: str
    timestamp: datetime
    overall_sentiment: float  # -1 to 1
    sentiment_sources: Dict[str, float]
    sentiment_trend: str  # "improving", "declining", "stable"
    confidence: float
    news_volume: int
    social_volume: int


@dataclass
class AnomalyDetection:
    """Anomaly detection result."""
    detection_id: str
    symbol: str
    timestamp: datetime
    anomaly_score: float
    is_anomaly: bool
    anomaly_type: str
    confidence: float
    context: Dict[str, Any]
    recommended_actions: List[str]


class AnalyticsMLServiceInterface(ABC):
    """
    Advanced Analytics and ML Service Interface
    
    Provides comprehensive financial analytics, machine learning, and quantitative
    analysis capabilities for trading systems including:
    - Technical and fundamental analysis
    - ML model training and inference
    - Feature engineering and selection
    - Backtesting and strategy validation
    - Quantitative risk metrics
    - Sentiment analysis
    - Anomaly detection
    """
    
    # Technical Analysis
    
    @abstractmethod
    async def calculate_technical_indicators(
        self,
        symbol: str,
        indicators: List[str],
        start_date: datetime,
        end_date: datetime,
        timeframe: str = "1D"
    ) -> List[TechnicalIndicator]:
        """
        Calculate technical indicators for symbol.
        
        Args:
            symbol: Symbol to analyze
            indicators: List of indicator names (SMA, EMA, RSI, MACD, etc.)
            start_date: Analysis start date
            end_date: Analysis end date
            timeframe: Data timeframe (1m, 5m, 1h, 1D, etc.)
            
        Returns:
            List of calculated technical indicators
        """
        pass
    
    @abstractmethod
    async def identify_chart_patterns(
        self,
        symbol: str,
        patterns: List[str],
        lookback_period: timedelta,
        confidence_threshold: float = 0.7
    ) -> List[AnalyticsResult]:
        """
        Identify chart patterns in price data.
        
        Args:
            symbol: Symbol to analyze
            patterns: Pattern types (head_and_shoulders, double_top, etc.)
            lookback_period: How far back to analyze
            confidence_threshold: Minimum confidence for pattern detection
            
        Returns:
            List of identified patterns
        """
        pass
    
    @abstractmethod
    async def calculate_support_resistance(
        self,
        symbol: str,
        lookback_period: timedelta,
        method: str = "pivot_points"
    ) -> Dict[str, List[Decimal]]:
        """
        Calculate support and resistance levels.
        
        Args:
            symbol: Symbol to analyze
            lookback_period: Historical period for calculation
            method: Calculation method (pivot_points, clustering, etc.)
            
        Returns:
            Support and resistance levels
        """
        pass
    
    # Machine Learning Models
    
    @abstractmethod
    async def create_ml_model(
        self,
        config: MLModelConfig,
        training_data: Dict[str, Any]
    ) -> str:
        """
        Create and train ML model.
        
        Args:
            config: Model configuration
            training_data: Training dataset
            
        Returns:
            Model ID
        """
        pass
    
    @abstractmethod
    async def train_model(
        self,
        model_id: str,
        training_data: Dict[str, Any],
        validation_split: float = 0.2
    ) -> MLModelMetrics:
        """
        Train ML model with provided data.
        
        Args:
            model_id: Model identifier
            training_data: Training dataset
            validation_split: Validation data percentage
            
        Returns:
            Training metrics and performance
        """
        pass
    
    @abstractmethod
    async def predict(
        self,
        model_id: str,
        input_data: Dict[str, Any],
        prediction_horizon: Optional[timedelta] = None
    ) -> Prediction:
        """
        Generate prediction using trained model.
        
        Args:
            model_id: Model identifier
            input_data: Input features for prediction
            prediction_horizon: How far into future to predict
            
        Returns:
            Model prediction result
        """
        pass
    
    @abstractmethod
    async def batch_predict(
        self,
        model_id: str,
        symbols: List[str],
        prediction_horizon: Optional[timedelta] = None
    ) -> List[Prediction]:
        """
        Generate batch predictions for multiple symbols.
        
        Args:
            model_id: Model identifier
            symbols: List of symbols to predict
            prediction_horizon: Prediction time horizon
            
        Returns:
            List of predictions
        """
        pass
    
    @abstractmethod
    async def evaluate_model(
        self,
        model_id: str,
        test_data: Dict[str, Any]
    ) -> MLModelMetrics:
        """
        Evaluate model performance on test data.
        
        Args:
            model_id: Model identifier
            test_data: Test dataset
            
        Returns:
            Model evaluation metrics
        """
        pass
    
    # Feature Engineering
    
    @abstractmethod
    async def engineer_features(
        self,
        symbol: str,
        feature_types: List[str],
        lookback_period: timedelta,
        target_timeframe: str = "1D"
    ) -> FeatureSet:
        """
        Engineer features for ML models.
        
        Args:
            symbol: Symbol to create features for
            feature_types: Types of features to create
            lookback_period: Historical period for feature calculation
            target_timeframe: Target data timeframe
            
        Returns:
            Engineered feature set
        """
        pass
    
    @abstractmethod
    async def select_features(
        self,
        feature_set_id: str,
        target_variable: str,
        selection_method: str = "mutual_info",
        max_features: Optional[int] = None
    ) -> List[str]:
        """
        Select most relevant features for modeling.
        
        Args:
            feature_set_id: Feature set identifier
            target_variable: Target variable name
            selection_method: Feature selection algorithm
            max_features: Maximum number of features to select
            
        Returns:
            List of selected feature names
        """
        pass
    
    @abstractmethod
    async def feature_importance_analysis(
        self,
        model_id: str,
        method: str = "permutation"
    ) -> Dict[str, float]:
        """
        Analyze feature importance for trained model.
        
        Args:
            model_id: Model identifier
            method: Importance calculation method
            
        Returns:
            Feature importance scores
        """
        pass
    
    # Backtesting & Strategy Validation
    
    @abstractmethod
    async def run_backtest(
        self,
        config: BacktestConfig,
        strategy_logic: Dict[str, Any]
    ) -> BacktestResult:
        """
        Run strategy backtesting.
        
        Args:
            config: Backtesting configuration
            strategy_logic: Strategy implementation details
            
        Returns:
            Backtesting results and metrics
        """
        pass
    
    @abstractmethod
    async def walk_forward_analysis(
        self,
        strategy_name: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        train_period: timedelta,
        test_period: timedelta,
        step_size: timedelta
    ) -> Dict[str, Any]:
        """
        Perform walk-forward analysis of strategy.
        
        Args:
            strategy_name: Strategy identifier
            symbol: Symbol to test
            start_date: Analysis start date
            end_date: Analysis end date
            train_period: Training window size
            test_period: Testing window size
            step_size: Step size for walk-forward
            
        Returns:
            Walk-forward analysis results
        """
        pass
    
    @abstractmethod
    async def monte_carlo_simulation(
        self,
        strategy_name: str,
        symbol: str,
        num_simulations: int,
        simulation_period: timedelta,
        confidence_levels: List[float] = [0.95, 0.99]
    ) -> Dict[str, Any]:
        """
        Run Monte Carlo simulation for strategy.
        
        Args:
            strategy_name: Strategy identifier
            symbol: Symbol to simulate
            num_simulations: Number of simulation runs
            simulation_period: Simulation time period
            confidence_levels: Confidence levels for analysis
            
        Returns:
            Monte Carlo simulation results
        """
        pass
    
    # Quantitative Analysis
    
    @abstractmethod
    async def calculate_quantitative_metrics(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        benchmark_symbol: Optional[str] = None
    ) -> QuantitativeMetrics:
        """
        Calculate quantitative performance metrics.
        
        Args:
            symbol: Symbol to analyze
            start_date: Analysis start date
            end_date: Analysis end date
            benchmark_symbol: Benchmark for relative metrics
            
        Returns:
            Quantitative metrics
        """
        pass
    
    @abstractmethod
    async def correlation_analysis(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        rolling_window: Optional[timedelta] = None
    ) -> CorrelationAnalysis:
        """
        Perform correlation analysis between symbols.
        
        Args:
            symbols: List of symbols to analyze
            start_date: Analysis start date
            end_date: Analysis end date
            rolling_window: Rolling correlation window
            
        Returns:
            Correlation analysis results
        """
        pass
    
    @abstractmethod
    async def regime_detection(
        self,
        symbol: str,
        lookback_period: timedelta,
        method: str = "hidden_markov"
    ) -> Dict[str, Any]:
        """
        Detect market regimes in price data.
        
        Args:
            symbol: Symbol to analyze
            lookback_period: Historical period for analysis
            method: Regime detection algorithm
            
        Returns:
            Detected regimes and transitions
        """
        pass
    
    # Sentiment Analysis
    
    @abstractmethod
    async def analyze_sentiment(
        self,
        symbol: str,
        sources: List[str],
        lookback_period: timedelta
    ) -> SentimentAnalysis:
        """
        Analyze market sentiment for symbol.
        
        Args:
            symbol: Symbol to analyze
            sources: Sentiment data sources (news, social, etc.)
            lookback_period: Analysis time window
            
        Returns:
            Sentiment analysis results
        """
        pass
    
    @abstractmethod
    async def sentiment_impact_analysis(
        self,
        symbol: str,
        sentiment_threshold: float,
        price_window: timedelta
    ) -> Dict[str, Any]:
        """
        Analyze impact of sentiment on price movements.
        
        Args:
            symbol: Symbol to analyze
            sentiment_threshold: Sentiment level threshold
            price_window: Price reaction time window
            
        Returns:
            Sentiment impact analysis
        """
        pass
    
    # Anomaly Detection
    
    @abstractmethod
    async def detect_anomalies(
        self,
        symbol: str,
        detection_methods: List[str],
        lookback_period: timedelta,
        sensitivity: float = 0.05
    ) -> List[AnomalyDetection]:
        """
        Detect anomalies in market data.
        
        Args:
            symbol: Symbol to analyze
            detection_methods: Anomaly detection algorithms
            lookback_period: Historical comparison period
            sensitivity: Detection sensitivity threshold
            
        Returns:
            List of detected anomalies
        """
        pass
    
    @abstractmethod
    async def real_time_anomaly_monitoring(
        self,
        symbols: List[str],
        callback: Callable[[AnomalyDetection], None]
    ) -> str:
        """
        Start real-time anomaly monitoring.
        
        Args:
            symbols: Symbols to monitor
            callback: Function to call when anomalies detected
            
        Returns:
            Monitoring session ID
        """
        pass
    
    # Model Management
    
    @abstractmethod
    async def get_model_status(self, model_id: str) -> Dict[str, Any]:
        """
        Get ML model status and metadata.
        
        Args:
            model_id: Model identifier
            
        Returns:
            Model status information
        """
        pass
    
    @abstractmethod
    async def list_models(
        self,
        model_type: Optional[MLModelType] = None,
        status: Optional[ModelStatus] = None
    ) -> List[Dict[str, Any]]:
        """
        List available ML models.
        
        Args:
            model_type: Filter by model type
            status: Filter by model status
            
        Returns:
            List of model information
        """
        pass
    
    @abstractmethod
    async def deploy_model(
        self,
        model_id: str,
        deployment_config: Dict[str, Any]
    ) -> bool:
        """
        Deploy model for production inference.
        
        Args:
            model_id: Model identifier
            deployment_config: Deployment configuration
            
        Returns:
            Deployment success status
        """
        pass
    
    @abstractmethod
    async def retire_model(self, model_id: str) -> bool:
        """
        Retire model from production use.
        
        Args:
            model_id: Model identifier
            
        Returns:
            Retirement success status
        """
        pass
    
    # Analytics Optimization
    
    @abstractmethod
    async def optimize_portfolio_allocation(
        self,
        symbols: List[str],
        objective: str,
        constraints: Dict[str, Any],
        lookback_period: timedelta
    ) -> Dict[str, float]:
        """
        Optimize portfolio allocation using quantitative methods.
        
        Args:
            symbols: Available symbols for portfolio
            objective: Optimization objective (sharpe, min_vol, max_return)
            constraints: Portfolio constraints
            lookback_period: Historical data period
            
        Returns:
            Optimal allocation weights
        """
        pass
    
    @abstractmethod
    async def risk_attribution_analysis(
        self,
        portfolio_allocation: Dict[str, float],
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """
        Perform risk attribution analysis.
        
        Args:
            portfolio_allocation: Portfolio weights
            start_date: Analysis start date
            end_date: Analysis end date
            
        Returns:
            Risk attribution results
        """
        pass
    
    # Real-time Analytics
    
    @abstractmethod
    async def start_real_time_analytics(
        self,
        symbols: List[str],
        analytics_types: List[AnalyticsType],
        update_frequency: timedelta,
        callback: Callable[[AnalyticsResult], None]
    ) -> str:
        """
        Start real-time analytics processing.
        
        Args:
            symbols: Symbols to analyze
            analytics_types: Types of analytics to compute
            update_frequency: How often to update analytics
            callback: Function to call with results
            
        Returns:
            Analytics session ID
        """
        pass
    
    @abstractmethod
    async def stop_real_time_analytics(self, session_id: str) -> bool:
        """
        Stop real-time analytics session.
        
        Args:
            session_id: Analytics session identifier
            
        Returns:
            Success status
        """
        pass
    
    # Data Export & Integration
    
    @abstractmethod
    async def export_analytics_results(
        self,
        analysis_ids: List[str],
        format: str = "json",
        include_metadata: bool = True
    ) -> bytes:
        """
        Export analytics results.
        
        Args:
            analysis_ids: List of analysis identifiers
            format: Export format (json, csv, parquet)
            include_metadata: Include analysis metadata
            
        Returns:
            Exported data as bytes
        """
        pass
    
    @abstractmethod
    async def get_feature_streaming_endpoint(
        self,
        feature_types: List[str],
        symbols: List[str]
    ) -> AsyncIterator[FeatureSet]:
        """
        Get streaming endpoint for real-time features.
        
        Args:
            feature_types: Types of features to stream
            symbols: Symbols to include
            
        Yields:
            Real-time feature sets
        """
        pass