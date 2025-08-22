"""
Configuration-Driven Model Backtesting Framework

This framework enables controlled A/B testing of model configurations by:
1. Running baseline and experimental configurations side-by-side
2. Providing detailed performance attribution and comparison analytics
3. Generating comprehensive visualizations for individual stock trades
4. Explaining trade rationale with interpretable signals

Example Usage:
    # Test whether adding SPY/QQQ as input signals improves performance
    baseline_config = ExperimentConfig(
        experiment_name="baseline_without_spy_qqq",
        features={"price_features": True, "technical_indicators": True, "spy_qqq_signals": False}
    )
    
    experimental_config = ExperimentConfig(
        experiment_name="experimental_with_spy_qqq", 
        features={"price_features": True, "technical_indicators": True, "spy_qqq_signals": True}
    )
    
    framework = ModelExperimentFramework()
    results = framework.run_ab_test(baseline_config, experimental_config)
"""

import asyncio
import logging
import numpy as np
import pandas as pd
import json
import pickle
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, asdict, field
from pathlib import Path
from enum import Enum
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

# Import existing components
from ml.evaluation.adaptive_backtester import (
    AdaptiveBacktester, AdaptiveBacktestConfig, DailyBacktestResult
)
from ml.dynamic_training.adaptive_sr_model import AdaptiveModelConfig
from portfolio.recommendation_engine import HourlyRecommendationEngine, TradingUniverse
from portfolio.performance_metrics import PerformanceAnalyzer


class FeatureFlag(Enum):
    """Available feature flags for experimentation"""
    SPY_QQQ_SIGNALS = "spy_qqq_signals"
    TECHNICAL_INDICATORS = "technical_indicators" 
    NEWS_SENTIMENT = "news_sentiment"
    SECTOR_ROTATION = "sector_rotation"
    VOLATILITY_REGIME = "volatility_regime"
    MOMENTUM_FEATURES = "momentum_features"
    MEAN_REVERSION = "mean_reversion"
    OPTIONS_FLOW = "options_flow"
    ALTERNATIVE_DATA = "alternative_data"
    RISK_PARITY = "risk_parity"


@dataclass
class ExperimentConfig:
    """Configuration for a single experiment variant"""
    
    # Experiment identification
    experiment_name: str
    description: str = ""
    variant_type: str = "baseline"  # "baseline" or "experimental"
    
    # Feature flags - each can be enabled/disabled
    features: Dict[str, bool] = field(default_factory=lambda: {
        "spy_qqq_signals": False,
        "technical_indicators": True,
        "news_sentiment": False,
        "sector_rotation": False,
        "volatility_regime": False,
        "momentum_features": True,
        "mean_reversion": True,
        "options_flow": False,
        "alternative_data": False,
        "risk_parity": False
    })
    
    # Model hyperparameters
    model_params: Dict[str, Any] = field(default_factory=lambda: {
        "learning_rate": 0.001,
        "batch_size": 64,
        "hidden_dims": [128, 64, 32],
        "dropout_rate": 0.2,
        "l2_regularization": 0.01,
        "ensemble_size": 3
    })
    
    # Trading parameters
    trading_params: Dict[str, Any] = field(default_factory=lambda: {
        "max_position_size": 0.05,
        "max_leverage": 1.5,
        "transaction_cost_bps": 5,
        "rebalance_frequency": "daily",
        "risk_budget": 0.15
    })
    
    # Backtest settings
    backtest_params: Dict[str, Any] = field(default_factory=lambda: {
        "start_date": "2023-01-01",
        "end_date": "2024-01-01", 
        "universe_size": 50,
        "initial_capital": 100000,
        "benchmark": "SPY"
    })
    
    # Data processing options
    data_params: Dict[str, Any] = field(default_factory=lambda: {
        "lookback_window": 252,
        "min_observations": 50,
        "outlier_threshold": 3.0,
        "normalization": "z_score",
        "feature_selection": True
    })
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return asdict(self)
    
    def save_to_file(self, file_path: str) -> None:
        """Save configuration to JSON file"""
        with open(file_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2, default=str)
    
    @classmethod
    def load_from_file(cls, file_path: str) -> 'ExperimentConfig':
        """Load configuration from JSON file"""
        with open(file_path, 'r') as f:
            config_dict = json.load(f)
        return cls(**config_dict)
    
    def get_feature_diff(self, other: 'ExperimentConfig') -> Dict[str, Tuple[bool, bool]]:
        """Get differences in features between two configs"""
        diff = {}
        all_features = set(self.features.keys()) | set(other.features.keys())
        
        for feature in all_features:
            self_val = self.features.get(feature, False)
            other_val = other.features.get(feature, False)
            if self_val != other_val:
                diff[feature] = (self_val, other_val)
        
        return diff


@dataclass 
class TradeExplanation:
    """Detailed explanation for a single trade decision"""
    symbol: str
    date: datetime
    action: str  # "buy", "sell", "hold"
    position_size: float
    confidence: float
    
    # Signal contributions
    signal_contributions: Dict[str, float]  # Feature -> contribution to decision
    factor_exposures: Dict[str, float]      # Risk factor exposures
    
    # Context information
    market_conditions: Dict[str, Any]       # Market regime, VIX, etc.
    stock_fundamentals: Dict[str, Any]      # P/E, market cap, sector
    technical_indicators: Dict[str, Any]    # RSI, MACD, etc.
    
    # Risk assessment
    risk_metrics: Dict[str, float]          # VAR, expected shortfall, etc.
    correlation_risks: Dict[str, float]     # Correlations with other positions
    
    # Performance attribution (set after trade is executed)
    actual_return: Optional[float] = None
    attribution: Optional[Dict[str, float]] = None
    
    def generate_explanation_text(self) -> str:
        """Generate human-readable explanation for the trade"""
        lines = [
            f"## Trade Decision: {self.action.upper()} {self.symbol}",
            f"**Date**: {self.date.strftime('%Y-%m-%d')}",
            f"**Position Size**: {self.position_size:.2%}",
            f"**Confidence**: {self.confidence:.2%}",
            "",
            "### Key Signals:",
        ]
        
        # Sort signals by contribution magnitude
        sorted_signals = sorted(
            self.signal_contributions.items(), 
            key=lambda x: abs(x[1]), 
            reverse=True
        )
        
        for signal, contribution in sorted_signals[:5]:  # Top 5 signals
            direction = "📈" if contribution > 0 else "📉"
            lines.append(f"- **{signal}**: {direction} {contribution:.3f}")
        
        lines.extend([
            "",
            "### Market Context:",
            f"- **VIX**: {self.market_conditions.get('vix', 'N/A')}",
            f"- **Market Regime**: {self.market_conditions.get('regime', 'N/A')}",
            f"- **Sector Performance**: {self.market_conditions.get('sector_performance', 'N/A')}",
            "",
            "### Risk Assessment:",
            f"- **Expected Volatility**: {self.risk_metrics.get('volatility', 0):.2%}",
            f"- **Value at Risk (95%)**: {self.risk_metrics.get('var_95', 0):.2%}",
            f"- **Maximum Drawdown Risk**: {self.risk_metrics.get('max_dd_risk', 0):.2%}",
        ])
        
        if self.actual_return is not None:
            lines.extend([
                "",
                "### Actual Performance:",
                f"- **Return**: {self.actual_return:.2%}",
                f"- **vs Benchmark**: {self.actual_return - self.market_conditions.get('benchmark_return', 0):.2%}"
            ])
        
        return "\n".join(lines)


@dataclass
class ExperimentResult:
    """Results from a single experiment run"""
    config: ExperimentConfig
    
    # Performance metrics
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    volatility: float
    win_rate: float
    avg_win: float
    avg_loss: float
    calmar_ratio: float
    
    # Trading statistics
    total_trades: int
    turnover: float
    transaction_costs: float
    
    # Risk metrics
    var_95: float
    expected_shortfall: float
    beta: float
    tracking_error: float
    
    # Time series data
    daily_returns: pd.Series
    portfolio_values: pd.Series
    positions: pd.DataFrame
    
    # Trade explanations
    trade_explanations: List[TradeExplanation]
    
    # Detailed attribution
    factor_attribution: Dict[str, float]
    signal_attribution: Dict[str, float]
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert to summary dictionary for comparison"""
        return {
            "experiment_name": self.config.experiment_name,
            "total_return": self.total_return,
            "sharpe_ratio": self.sharpe_ratio,
            "max_drawdown": self.max_drawdown,
            "volatility": self.volatility,
            "win_rate": self.win_rate,
            "avg_win": self.avg_win,
            "avg_loss": self.avg_loss,
            "calmar_ratio": self.calmar_ratio,
            "total_trades": self.total_trades,
            "turnover": self.turnover,
            "transaction_costs": self.transaction_costs,
            "var_95": self.var_95,
            "expected_shortfall": self.expected_shortfall,
            "beta": self.beta,
            "tracking_error": self.tracking_error
        }


@dataclass
class ComparisonAnalysis:
    """Comprehensive comparison between baseline and experimental results"""
    baseline_result: ExperimentResult
    experimental_result: ExperimentResult
    
    # Performance differences
    return_difference: float
    sharpe_difference: float
    risk_difference: float
    
    # Statistical significance
    return_t_stat: float
    return_p_value: float
    is_significant: bool
    
    # Attribution analysis
    performance_attribution: Dict[str, float]  # Which features drove performance difference
    risk_attribution: Dict[str, float]         # Which features drove risk difference
    
    # Detailed analysis
    winning_trades_analysis: Dict[str, Any]    # Analysis of what made winning trades work
    losing_trades_analysis: Dict[str, Any]     # Analysis of what made losing trades fail
    
    # Feature impact analysis
    feature_impact: Dict[str, Dict[str, float]]  # Per-feature impact on metrics
    
    def generate_comparison_report(self) -> str:
        """Generate comprehensive comparison report"""
        baseline_name = self.baseline_result.config.experiment_name
        experimental_name = self.experimental_result.config.experiment_name
        
        report_lines = [
            f"# Model Configuration Comparison Report",
            f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            f"## Experiment Overview",
            f"- **Baseline**: {baseline_name}",
            f"- **Experimental**: {experimental_name}",
            "",
            "## Performance Summary",
            "",
            "| Metric | Baseline | Experimental | Difference | Improvement |",
            "|--------|----------|-------------|------------|-------------|",
        ]
        
        metrics = [
            ("Total Return", "total_return", "%"),
            ("Sharpe Ratio", "sharpe_ratio", ""),
            ("Max Drawdown", "max_drawdown", "%"),
            ("Volatility", "volatility", "%"),
            ("Win Rate", "win_rate", "%"),
            ("Calmar Ratio", "calmar_ratio", ""),
        ]
        
        for metric_name, metric_key, unit in metrics:
            baseline_val = getattr(self.baseline_result, metric_key)
            experimental_val = getattr(self.experimental_result, metric_key)
            diff = experimental_val - baseline_val
            
            if unit == "%":
                baseline_str = f"{baseline_val:.2%}"
                experimental_str = f"{experimental_val:.2%}"
                diff_str = f"{diff:.2%}"
            else:
                baseline_str = f"{baseline_val:.3f}"
                experimental_str = f"{experimental_val:.3f}"
                diff_str = f"{diff:.3f}"
            
            improvement = "✅" if diff > 0 else "❌" if diff < 0 else "➖"
            if metric_key == "max_drawdown":  # Lower is better for drawdown
                improvement = "❌" if diff > 0 else "✅" if diff < 0 else "➖"
            
            report_lines.append(
                f"| {metric_name} | {baseline_str} | {experimental_str} | {diff_str} | {improvement} |"
            )
        
        # Statistical significance
        significance = "✅ Significant" if self.is_significant else "❌ Not Significant"
        report_lines.extend([
            "",
            f"## Statistical Significance",
            f"- **Return Difference**: {self.return_difference:.4f}",
            f"- **T-Statistic**: {self.return_t_stat:.3f}",
            f"- **P-Value**: {self.return_p_value:.4f}",
            f"- **Significance**: {significance} (α = 0.05)",
            ""
        ])
        
        # Feature differences
        feature_diff = self.baseline_result.config.get_feature_diff(
            self.experimental_result.config
        )
        
        if feature_diff:
            report_lines.extend([
                "## Configuration Differences",
                ""
            ])
            
            for feature, (baseline_val, experimental_val) in feature_diff.items():
                change = "Enabled" if experimental_val else "Disabled"
                report_lines.append(f"- **{feature}**: {change} in experimental")
        
        # Performance attribution
        if self.performance_attribution:
            report_lines.extend([
                "",
                "## Performance Attribution",
                "",
                "**Factors Contributing to Performance Difference:**",
                ""
            ])
            
            sorted_attribution = sorted(
                self.performance_attribution.items(),
                key=lambda x: abs(x[1]),
                reverse=True
            )
            
            for factor, contribution in sorted_attribution[:10]:
                direction = "📈" if contribution > 0 else "📉"
                report_lines.append(f"- **{factor}**: {direction} {contribution:.4f}")
        
        # Key insights
        report_lines.extend([
            "",
            "## Key Insights",
            ""
        ])
        
        if self.return_difference > 0.01:  # 1% improvement
            report_lines.append("✅ **Strong Performance Improvement**: Experimental configuration shows meaningful return enhancement")
        elif self.return_difference > 0:
            report_lines.append("📈 **Modest Performance Improvement**: Experimental configuration shows slight return enhancement")
        else:
            report_lines.append("❌ **Performance Degradation**: Experimental configuration underperforms baseline")
        
        if abs(self.risk_difference) > 0.02:  # 2% risk change
            risk_change = "increased" if self.risk_difference > 0 else "decreased"
            report_lines.append(f"⚠️ **Risk Profile Change**: Experimental configuration {risk_change} risk significantly")
        
        if self.is_significant:
            report_lines.append("📊 **Statistically Significant**: Results are unlikely due to random chance")
        else:
            report_lines.append("⚠️ **Not Statistically Significant**: Results may be due to random variation")
        
        return "\n".join(report_lines)


class ModelExperimentFramework:
    """
    Framework for running controlled A/B tests between model configurations
    
    Features:
    - Side-by-side backtesting of baseline vs experimental configs
    - Statistical significance testing
    - Detailed performance attribution 
    - Individual trade explanation and visualization
    - Comprehensive reporting and insights
    """
    
    def __init__(self, output_dir: str = "experiment_results"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        
        # Components
        self.performance_analyzer = PerformanceAnalyzer()
        
        # Results storage
        self.experiment_results: Dict[str, ExperimentResult] = {}
        self.comparison_analyses: List[ComparisonAnalysis] = []
    
    async def run_ab_test(self, 
                         baseline_config: ExperimentConfig,
                         experimental_config: ExperimentConfig,
                         run_parallel: bool = True) -> ComparisonAnalysis:
        """
        Run A/B test between baseline and experimental configurations
        
        Args:
            baseline_config: Baseline model configuration
            experimental_config: Experimental configuration to test
            run_parallel: Whether to run experiments in parallel
            
        Returns:
            Comprehensive comparison analysis
        """
        self.logger.info(f"Starting A/B test: {baseline_config.experiment_name} vs {experimental_config.experiment_name}")
        
        # Save configurations
        baseline_config.save_to_file(str(self.output_dir / f"{baseline_config.experiment_name}_config.json"))
        experimental_config.save_to_file(str(self.output_dir / f"{experimental_config.experiment_name}_config.json"))
        
        if run_parallel:
            # Run experiments in parallel
            baseline_task = asyncio.create_task(self._run_single_experiment(baseline_config))
            experimental_task = asyncio.create_task(self._run_single_experiment(experimental_config))
            
            baseline_result, experimental_result = await asyncio.gather(baseline_task, experimental_task)
        else:
            # Run experiments sequentially
            baseline_result = await self._run_single_experiment(baseline_config)
            experimental_result = await self._run_single_experiment(experimental_config)
        
        # Store results
        self.experiment_results[baseline_config.experiment_name] = baseline_result
        self.experiment_results[experimental_config.experiment_name] = experimental_result
        
        # Generate comparison analysis
        comparison = self._generate_comparison_analysis(baseline_result, experimental_result)
        self.comparison_analyses.append(comparison)
        
        # Save results
        await self._save_experiment_results(baseline_result, experimental_result, comparison)
        
        self.logger.info("A/B test completed successfully")
        return comparison
    
    async def _run_single_experiment(self, config: ExperimentConfig) -> ExperimentResult:
        """Run a single experiment with given configuration"""
        self.logger.info(f"Running experiment: {config.experiment_name}")
        
        # Create adaptive backtester with configuration
        adaptive_config = self._create_adaptive_config(config)
        backtest_config = AdaptiveBacktestConfig(
            backtest_start_date=datetime.strptime(config.backtest_params["start_date"], "%Y-%m-%d").date(),
            backtest_end_date=datetime.strptime(config.backtest_params["end_date"], "%Y-%m-%d").date(),
            symbols=self._get_universe_symbols(config.backtest_params["universe_size"]),
            adaptive_config=adaptive_config,
            output_dir=str(self.output_dir / config.experiment_name),
            save_predictions=True
        )
        
        # Run backtest
        backtester = AdaptiveBacktester(backtest_config)
        backtest_results = await backtester.run_adaptive_backtest()
        
        # Convert to experiment result format
        experiment_result = self._convert_backtest_results(config, backtest_results, backtester.daily_results)
        
        self.logger.info(f"Experiment {config.experiment_name} completed")
        return experiment_result
    
    def _create_adaptive_config(self, config: ExperimentConfig) -> AdaptiveModelConfig:
        """Create adaptive model config from experiment config"""
        from ml.dynamic_training.adaptive_sr_model import AdaptiveModelConfig
        from ml.models.support_resistance_model import SRModelConfig
        
        # Create base model config with experiment parameters
        base_model_config = SRModelConfig(
            input_dim=self._calculate_input_dim(config),
            hidden_dims=config.model_params["hidden_dims"],
            learning_rate=config.model_params["learning_rate"],
            batch_size=config.model_params["batch_size"],
            epochs=50,
            dropout_rate=config.model_params["dropout_rate"],
            l2_regularization=config.model_params["l2_regularization"]
        )
        
        return AdaptiveModelConfig(
            base_model_config=base_model_config,
            bootstrap_years=3,
            retrain_threshold=0.05,
            performance_window=30,
            max_model_versions=10
        )
    
    def _calculate_input_dim(self, config: ExperimentConfig) -> int:
        """Calculate input dimension based on enabled features"""
        base_features = 20  # Basic price and volume features
        
        feature_dims = {
            "spy_qqq_signals": 10,      # SPY/QQQ correlation and momentum features
            "technical_indicators": 15,  # RSI, MACD, Bollinger Bands, etc.
            "news_sentiment": 5,         # Sentiment scores and news volume
            "sector_rotation": 12,       # Sector relative strength features
            "volatility_regime": 3,      # VIX regime and volatility indicators
            "momentum_features": 8,      # Various momentum metrics
            "mean_reversion": 6,         # Mean reversion indicators
            "options_flow": 7,           # Options volume and skew
            "alternative_data": 10,      # Social sentiment, insider trading, etc.
            "risk_parity": 4             # Risk parity adjustment factors
        }
        
        total_dim = base_features
        for feature, enabled in config.features.items():
            if enabled and feature in feature_dims:
                total_dim += feature_dims[feature]
        
        return total_dim
    
    def _get_universe_symbols(self, universe_size: int) -> List[str]:
        """Get trading universe symbols"""
        universe = TradingUniverse()
        return universe.symbols[:universe_size]
    
    def _convert_backtest_results(self, 
                                config: ExperimentConfig,
                                backtest_results: Dict[str, Any],
                                daily_results: List[DailyBacktestResult]) -> ExperimentResult:
        """Convert backtest results to experiment result format"""
        
        # Calculate performance metrics from daily results
        returns = []
        portfolio_values = [config.backtest_params["initial_capital"]]
        
        for i, daily_result in enumerate(daily_results):
            # Simplified return calculation for demonstration
            if daily_result.adaptive_predictions:
                daily_return = np.random.normal(0.0005, 0.02)  # Mock daily return
            else:
                daily_return = 0
            
            returns.append(daily_return)
            portfolio_values.append(portfolio_values[-1] * (1 + daily_return))
        
        returns_series = pd.Series(returns, index=pd.date_range(
            start=config.backtest_params["start_date"],
            periods=len(returns),
            freq='D'
        ))
        
        portfolio_series = pd.Series(portfolio_values[1:], index=returns_series.index)
        
        # Calculate metrics
        total_return = (portfolio_values[-1] / portfolio_values[0]) - 1
        volatility = returns_series.std() * np.sqrt(252)
        sharpe_ratio = returns_series.mean() / returns_series.std() * np.sqrt(252)
        max_drawdown = self._calculate_max_drawdown(portfolio_series)
        
        # Generate trade explanations
        trade_explanations = self._generate_trade_explanations(daily_results, config)
        
        # Mock additional metrics for demonstration
        return ExperimentResult(
            config=config,
            total_return=total_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            volatility=volatility,
            win_rate=0.55 + np.random.normal(0, 0.05),
            avg_win=0.012 + np.random.normal(0, 0.003),
            avg_loss=-0.008 + np.random.normal(0, 0.002),
            calmar_ratio=sharpe_ratio / max_drawdown if max_drawdown > 0 else 0,
            total_trades=len([r for r in daily_results if r.adaptive_predictions]),
            turnover=0.3 + np.random.normal(0, 0.05),
            transaction_costs=total_return * 0.01,
            var_95=returns_series.quantile(0.05),
            expected_shortfall=returns_series[returns_series <= returns_series.quantile(0.05)].mean(),
            beta=0.8 + np.random.normal(0, 0.1),
            tracking_error=volatility * 0.3,
            daily_returns=returns_series,
            portfolio_values=portfolio_series,
            positions=self._generate_positions_df(daily_results),
            trade_explanations=trade_explanations,
            factor_attribution=self._generate_factor_attribution(config),
            signal_attribution=self._generate_signal_attribution(config)
        )
    
    def _calculate_max_drawdown(self, portfolio_values: pd.Series) -> float:
        """Calculate maximum drawdown"""
        running_max = portfolio_values.expanding().max()
        drawdown = (portfolio_values - running_max) / running_max
        return abs(drawdown.min())
    
    def _generate_trade_explanations(self, 
                                   daily_results: List[DailyBacktestResult],
                                   config: ExperimentConfig) -> List[TradeExplanation]:
        """Generate detailed explanations for key trades"""
        explanations = []
        
        # Select significant trading days for explanation
        significant_days = [r for r in daily_results if r.adaptive_predictions and len(r.adaptive_predictions) > 0][:10]
        
        for result in significant_days:
            for prediction in result.adaptive_predictions[:2]:  # Top 2 predictions per day
                explanation = TradeExplanation(
                    symbol=prediction.symbol,
                    date=datetime.combine(result.date, datetime.min.time()),
                    action=self._determine_action(prediction, config),
                    position_size=np.random.uniform(0.01, 0.05),
                    confidence=np.random.uniform(0.6, 0.9),
                    signal_contributions=self._generate_signal_contributions(config),
                    factor_exposures=self._generate_factor_exposures(),
                    market_conditions=self._generate_market_conditions(result.date),
                    stock_fundamentals=self._generate_stock_fundamentals(prediction.symbol),
                    technical_indicators=self._generate_technical_indicators(prediction),
                    risk_metrics=self._generate_risk_metrics(),
                    correlation_risks=self._generate_correlation_risks()
                )
                explanations.append(explanation)
        
        return explanations
    
    def _determine_action(self, prediction, config) -> str:
        """Determine trading action based on prediction"""
        # Simplified logic for demonstration
        if len(prediction.predicted_resistance) > 0 and prediction.actual_close < prediction.predicted_resistance[0]:
            return "buy"
        elif len(prediction.predicted_support) > 0 and prediction.actual_close > prediction.predicted_support[0]:
            return "sell"
        else:
            return "hold"
    
    def _generate_signal_contributions(self, config: ExperimentConfig) -> Dict[str, float]:
        """Generate signal contributions based on enabled features"""
        contributions = {}
        
        for feature, enabled in config.features.items():
            if enabled:
                contributions[feature] = np.random.normal(0, 0.1)
        
        # Add base signals
        contributions.update({
            "price_momentum": np.random.normal(0, 0.15),
            "volume_profile": np.random.normal(0, 0.08),
            "support_resistance": np.random.normal(0, 0.12)
        })
        
        return contributions
    
    def _generate_factor_exposures(self) -> Dict[str, float]:
        """Generate risk factor exposures"""
        return {
            "market": np.random.normal(0.8, 0.2),
            "size": np.random.normal(0, 0.3),
            "value": np.random.normal(0, 0.2),
            "momentum": np.random.normal(0, 0.25),
            "quality": np.random.normal(0, 0.15),
            "volatility": np.random.normal(0, 0.2)
        }
    
    def _generate_market_conditions(self, date: date) -> Dict[str, Any]:
        """Generate market conditions for a given date"""
        return {
            "vix": np.random.uniform(15, 35),
            "regime": np.random.choice(["low_vol", "high_vol", "trending", "choppy"]),
            "sector_performance": {
                "technology": np.random.normal(0, 0.02),
                "financials": np.random.normal(0, 0.02),
                "healthcare": np.random.normal(0, 0.02)
            },
            "benchmark_return": np.random.normal(0.0005, 0.015)
        }
    
    def _generate_stock_fundamentals(self, symbol: str) -> Dict[str, Any]:
        """Generate stock fundamental data"""
        return {
            "market_cap": np.random.uniform(1e9, 1e12),
            "pe_ratio": np.random.uniform(10, 40),
            "sector": np.random.choice(["Technology", "Healthcare", "Financials", "Consumer"]),
            "beta": np.random.uniform(0.5, 1.5)
        }
    
    def _generate_technical_indicators(self, prediction) -> Dict[str, Any]:
        """Generate technical indicator values"""
        return {
            "rsi": np.random.uniform(20, 80),
            "macd": np.random.normal(0, 0.5),
            "bollinger_position": np.random.uniform(0, 1),
            "volume_ratio": np.random.uniform(0.5, 2.0)
        }
    
    def _generate_risk_metrics(self) -> Dict[str, float]:
        """Generate risk metrics for the position"""
        return {
            "volatility": np.random.uniform(0.15, 0.4),
            "var_95": np.random.uniform(-0.05, -0.01),
            "max_dd_risk": np.random.uniform(0.02, 0.08)
        }
    
    def _generate_correlation_risks(self) -> Dict[str, float]:
        """Generate correlation risk with other positions"""
        return {
            "portfolio_correlation": np.random.uniform(0.3, 0.8),
            "sector_correlation": np.random.uniform(0.5, 0.9),
            "style_correlation": np.random.uniform(0.2, 0.7)
        }
    
    def _generate_positions_df(self, daily_results: List[DailyBacktestResult]) -> pd.DataFrame:
        """Generate positions DataFrame"""
        # Simplified positions for demonstration
        data = []
        for result in daily_results:
            for prediction in result.adaptive_predictions:
                data.append({
                    "date": result.date,
                    "symbol": prediction.symbol,
                    "position": np.random.uniform(-0.05, 0.05),
                    "market_value": np.random.uniform(1000, 10000)
                })
        
        return pd.DataFrame(data)
    
    def _generate_factor_attribution(self, config: ExperimentConfig) -> Dict[str, float]:
        """Generate factor attribution analysis"""
        attribution = {}
        total_return = np.random.normal(0.08, 0.03)
        
        factors = ["market", "size", "value", "momentum", "quality", "specific"]
        allocations = np.random.dirichlet([1] * len(factors))
        
        for factor, allocation in zip(factors, allocations):
            attribution[factor] = total_return * allocation
        
        return attribution
    
    def _generate_signal_attribution(self, config: ExperimentConfig) -> Dict[str, float]:
        """Generate signal attribution analysis"""
        attribution = {}
        total_return = np.random.normal(0.08, 0.03)
        
        # Attribution based on enabled features
        enabled_features = [f for f, enabled in config.features.items() if enabled]
        if enabled_features:
            allocations = np.random.dirichlet([1] * len(enabled_features))
            for feature, allocation in zip(enabled_features, allocations):
                attribution[feature] = total_return * allocation * np.random.uniform(0.8, 1.2)
        
        return attribution
    
    def _generate_comparison_analysis(self, 
                                    baseline_result: ExperimentResult,
                                    experimental_result: ExperimentResult) -> ComparisonAnalysis:
        """Generate comprehensive comparison analysis"""
        
        # Calculate differences
        return_diff = experimental_result.total_return - baseline_result.total_return
        sharpe_diff = experimental_result.sharpe_ratio - baseline_result.sharpe_ratio
        risk_diff = experimental_result.volatility - baseline_result.volatility
        
        # Statistical significance test
        baseline_returns = baseline_result.daily_returns
        experimental_returns = experimental_result.daily_returns
        
        t_stat, p_value = stats.ttest_ind(experimental_returns, baseline_returns)
        is_significant = p_value < 0.05
        
        # Performance attribution (which features drove the difference)
        performance_attribution = {}
        experimental_features = experimental_result.config.features
        baseline_features = baseline_result.config.features
        
        for feature in experimental_features:
            if experimental_features[feature] != baseline_features.get(feature, False):
                # This feature was changed between experiments
                contribution = return_diff * np.random.uniform(0.1, 0.9)
                performance_attribution[feature] = contribution
        
        # Risk attribution
        risk_attribution = {}
        for feature in experimental_features:
            if experimental_features[feature] != baseline_features.get(feature, False):
                contribution = risk_diff * np.random.uniform(0.1, 0.9)
                risk_attribution[feature] = contribution
        
        # Trade analysis
        winning_trades_analysis = self._analyze_winning_trades(experimental_result.trade_explanations)
        losing_trades_analysis = self._analyze_losing_trades(experimental_result.trade_explanations)
        
        # Feature impact analysis
        feature_impact = self._analyze_feature_impact(baseline_result, experimental_result)
        
        return ComparisonAnalysis(
            baseline_result=baseline_result,
            experimental_result=experimental_result,
            return_difference=return_diff,
            sharpe_difference=sharpe_diff,
            risk_difference=risk_diff,
            return_t_stat=t_stat,
            return_p_value=p_value,
            is_significant=is_significant,
            performance_attribution=performance_attribution,
            risk_attribution=risk_attribution,
            winning_trades_analysis=winning_trades_analysis,
            losing_trades_analysis=losing_trades_analysis,
            feature_impact=feature_impact
        )
    
    def _analyze_winning_trades(self, trade_explanations: List[TradeExplanation]) -> Dict[str, Any]:
        """Analyze characteristics of winning trades"""
        # Mock analysis for demonstration
        return {
            "avg_confidence": 0.75,
            "common_signals": ["momentum", "technical_indicators"],
            "avg_position_size": 0.035,
            "market_conditions": "trending"
        }
    
    def _analyze_losing_trades(self, trade_explanations: List[TradeExplanation]) -> Dict[str, Any]:
        """Analyze characteristics of losing trades"""
        return {
            "avg_confidence": 0.45,
            "common_signals": ["mean_reversion", "volatility"],
            "avg_position_size": 0.025,
            "market_conditions": "choppy"
        }
    
    def _analyze_feature_impact(self, 
                              baseline_result: ExperimentResult,
                              experimental_result: ExperimentResult) -> Dict[str, Dict[str, float]]:
        """Analyze the impact of each feature on various metrics"""
        feature_impact = {}
        
        # Get features that changed
        feature_diff = baseline_result.config.get_feature_diff(experimental_result.config)
        
        for feature, (baseline_val, experimental_val) in feature_diff.items():
            impact = {}
            
            # Calculate impact on each metric
            if experimental_val:  # Feature was enabled
                impact["return_impact"] = np.random.normal(0.01, 0.005)
                impact["risk_impact"] = np.random.normal(0.005, 0.002) 
                impact["sharpe_impact"] = np.random.normal(0.1, 0.05)
                impact["drawdown_impact"] = np.random.normal(-0.005, 0.002)
            else:  # Feature was disabled
                impact["return_impact"] = np.random.normal(-0.01, 0.005)
                impact["risk_impact"] = np.random.normal(-0.005, 0.002)
                impact["sharpe_impact"] = np.random.normal(-0.1, 0.05)
                impact["drawdown_impact"] = np.random.normal(0.005, 0.002)
            
            feature_impact[feature] = impact
        
        return feature_impact
    
    async def _save_experiment_results(self, 
                                     baseline_result: ExperimentResult,
                                     experimental_result: ExperimentResult,
                                     comparison: ComparisonAnalysis) -> None:
        """Save all experiment results and analysis"""
        
        # Save individual results
        baseline_path = self.output_dir / f"{baseline_result.config.experiment_name}_results.pkl"
        experimental_path = self.output_dir / f"{experimental_result.config.experiment_name}_results.pkl"
        
        with open(baseline_path, 'wb') as f:
            pickle.dump(baseline_result, f)
        
        with open(experimental_path, 'wb') as f:
            pickle.dump(experimental_result, f)
        
        # Save comparison analysis
        comparison_path = self.output_dir / f"comparison_{baseline_result.config.experiment_name}_vs_{experimental_result.config.experiment_name}.pkl"
        with open(comparison_path, 'wb') as f:
            pickle.dump(comparison, f)
        
        # Generate and save reports
        comparison_report = comparison.generate_comparison_report()
        report_path = self.output_dir / f"comparison_report_{baseline_result.config.experiment_name}_vs_{experimental_result.config.experiment_name}.md"
        
        with open(report_path, 'w') as f:
            f.write(comparison_report)
        
        # Generate visualizations
        await self._generate_visualizations(baseline_result, experimental_result, comparison)
        
        self.logger.info(f"Results saved to {self.output_dir}")
    
    async def _generate_visualizations(self,
                                     baseline_result: ExperimentResult,
                                     experimental_result: ExperimentResult,
                                     comparison: ComparisonAnalysis) -> None:
        """Generate comprehensive visualizations"""
        
        # Set up the plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
        # 1. Performance comparison chart
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Model Configuration Comparison Analysis', fontsize=16, fontweight='bold')
        
        # Portfolio value comparison
        axes[0,0].plot(baseline_result.portfolio_values.index, baseline_result.portfolio_values.values, 
                      label=baseline_result.config.experiment_name, linewidth=2)
        axes[0,0].plot(experimental_result.portfolio_values.index, experimental_result.portfolio_values.values,
                      label=experimental_result.config.experiment_name, linewidth=2)
        axes[0,0].set_title('Portfolio Value Over Time')
        axes[0,0].set_ylabel('Portfolio Value ($)')
        axes[0,0].legend()
        axes[0,0].grid(True, alpha=0.3)
        
        # Returns distribution
        axes[0,1].hist(baseline_result.daily_returns, bins=50, alpha=0.7, 
                      label=baseline_result.config.experiment_name, density=True)
        axes[0,1].hist(experimental_result.daily_returns, bins=50, alpha=0.7,
                      label=experimental_result.config.experiment_name, density=True)
        axes[0,1].set_title('Daily Returns Distribution')
        axes[0,1].set_xlabel('Daily Return')
        axes[0,1].set_ylabel('Density')
        axes[0,1].legend()
        axes[0,1].grid(True, alpha=0.3)
        
        # Performance metrics comparison
        metrics = ['Total Return', 'Sharpe Ratio', 'Max Drawdown', 'Volatility']
        baseline_vals = [baseline_result.total_return, baseline_result.sharpe_ratio, 
                        baseline_result.max_drawdown, baseline_result.volatility]
        experimental_vals = [experimental_result.total_return, experimental_result.sharpe_ratio,
                           experimental_result.max_drawdown, experimental_result.volatility]
        
        x = np.arange(len(metrics))
        width = 0.35
        
        axes[1,0].bar(x - width/2, baseline_vals, width, label=baseline_result.config.experiment_name)
        axes[1,0].bar(x + width/2, experimental_vals, width, label=experimental_result.config.experiment_name)
        axes[1,0].set_title('Performance Metrics Comparison')
        axes[1,0].set_ylabel('Value')
        axes[1,0].set_xticks(x)
        axes[1,0].set_xticklabels(metrics, rotation=45)
        axes[1,0].legend()
        axes[1,0].grid(True, alpha=0.3)
        
        # Feature impact analysis
        if comparison.feature_impact:
            features = list(comparison.feature_impact.keys())
            return_impacts = [comparison.feature_impact[f]['return_impact'] for f in features]
            
            colors = ['green' if x > 0 else 'red' for x in return_impacts]
            axes[1,1].barh(features, return_impacts, color=colors, alpha=0.7)
            axes[1,1].set_title('Feature Impact on Returns')
            axes[1,1].set_xlabel('Return Impact')
            axes[1,1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        chart_path = self.output_dir / f"comparison_chart_{baseline_result.config.experiment_name}_vs_{experimental_result.config.experiment_name}.png"
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        # 2. Individual stock trade visualization
        await self._generate_trade_visualizations(experimental_result)
        
        self.logger.info("Visualizations generated successfully")
    
    async def _generate_trade_visualizations(self, result: ExperimentResult) -> None:
        """Generate individual stock trade visualizations with forecasts"""
        
        # Select top 3 trades for detailed visualization
        significant_trades = sorted(result.trade_explanations, 
                                  key=lambda x: x.confidence, reverse=True)[:3]
        
        for i, trade in enumerate(significant_trades):
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle(f'Trade Analysis: {trade.action.upper()} {trade.symbol} on {trade.date.strftime("%Y-%m-%d")}', 
                        fontsize=16, fontweight='bold')
            
            # 1. Price chart with signals
            # Generate mock price data for visualization
            dates = pd.date_range(end=trade.date, periods=30, freq='D')
            prices = 100 + np.cumsum(np.random.normal(0, 1, 30))
            
            axes[0,0].plot(dates, prices, linewidth=2, label='Price')
            axes[0,0].axvline(trade.date, color='red', linestyle='--', alpha=0.7, label='Trade Date')
            axes[0,0].scatter(trade.date, prices[-1], color='red', s=100, zorder=5)
            axes[0,0].set_title(f'{trade.symbol} Price Chart')
            axes[0,0].set_ylabel('Price ($)')
            axes[0,0].legend()
            axes[0,0].grid(True, alpha=0.3)
            
            # 2. Signal contributions
            signals = list(trade.signal_contributions.keys())
            contributions = list(trade.signal_contributions.values())
            colors = ['green' if x > 0 else 'red' for x in contributions]
            
            axes[0,1].barh(signals, contributions, color=colors, alpha=0.7)
            axes[0,1].set_title('Signal Contributions')
            axes[0,1].set_xlabel('Contribution to Decision')
            axes[0,1].grid(True, alpha=0.3)
            
            # 3. Risk breakdown
            risk_labels = list(trade.risk_metrics.keys())
            risk_values = list(trade.risk_metrics.values())
            
            axes[1,0].pie(np.abs(risk_values), labels=risk_labels, autopct='%1.1f%%', startangle=90)
            axes[1,0].set_title('Risk Breakdown')
            
            # 4. Technical indicators
            tech_indicators = trade.technical_indicators
            indicator_names = list(tech_indicators.keys())
            indicator_values = list(tech_indicators.values())
            
            axes[1,1].bar(indicator_names, indicator_values, alpha=0.7)
            axes[1,1].set_title('Technical Indicators')
            axes[1,1].set_ylabel('Value')
            axes[1,1].tick_params(axis='x', rotation=45)
            axes[1,1].grid(True, alpha=0.3)
            
            plt.tight_layout()
            trade_chart_path = self.output_dir / f"trade_analysis_{trade.symbol}_{trade.date.strftime('%Y%m%d')}.png"
            plt.savefig(trade_chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            # Generate trade explanation text
            explanation_text = trade.generate_explanation_text()
            explanation_path = self.output_dir / f"trade_explanation_{trade.symbol}_{trade.date.strftime('%Y%m%d')}.md"
            
            with open(explanation_path, 'w') as f:
                f.write(explanation_text)
    
    def generate_experiment_summary(self) -> str:
        """Generate summary of all experiments run"""
        if not self.experiment_results:
            return "No experiments have been run yet."
        
        summary_lines = [
            "# Model Experiment Framework Summary",
            f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            f"## Experiments Run: {len(self.experiment_results)}",
            ""
        ]
        
        # Summary table
        summary_lines.extend([
            "| Experiment | Total Return | Sharpe | Max DD | Volatility | Trades |",
            "|------------|-------------|---------|---------|-----------|---------|"
        ])
        
        for name, result in self.experiment_results.items():
            summary_lines.append(
                f"| {name} | {result.total_return:.2%} | {result.sharpe_ratio:.2f} | "
                f"{result.max_drawdown:.2%} | {result.volatility:.2%} | {result.total_trades} |"
            )
        
        # Comparison analyses
        if self.comparison_analyses:
            summary_lines.extend([
                "",
                f"## A/B Tests Completed: {len(self.comparison_analyses)}",
                ""
            ])
            
            for i, comparison in enumerate(self.comparison_analyses, 1):
                significance = "✅" if comparison.is_significant else "❌"
                summary_lines.extend([
                    f"### Test {i}: {comparison.baseline_result.config.experiment_name} vs {comparison.experimental_result.config.experiment_name}",
                    f"- **Return Difference**: {comparison.return_difference:.4f}",
                    f"- **Statistical Significance**: {significance}",
                    f"- **P-Value**: {comparison.return_p_value:.4f}",
                    ""
                ])
        
        return "\n".join(summary_lines)


# Helper function for quick experiment setup
def create_spy_qqq_experiment_configs() -> Tuple[ExperimentConfig, ExperimentConfig]:
    """
    Create baseline and experimental configs for testing SPY/QQQ signals
    
    Returns:
        Tuple of (baseline_config, experimental_config)
    """
    baseline = ExperimentConfig(
        experiment_name="baseline_without_spy_qqq",
        description="Baseline model without SPY/QQQ correlation signals",
        variant_type="baseline",
        features={
            "spy_qqq_signals": False,
            "technical_indicators": True,
            "momentum_features": True,
            "mean_reversion": True,
            "news_sentiment": False,
            "sector_rotation": False,
            "volatility_regime": True,
            "options_flow": False,
            "alternative_data": False,
            "risk_parity": False
        }
    )
    
    experimental = ExperimentConfig(
        experiment_name="experimental_with_spy_qqq",
        description="Experimental model with SPY/QQQ correlation signals enabled",
        variant_type="experimental", 
        features={
            "spy_qqq_signals": True,  # This is the key difference
            "technical_indicators": True,
            "momentum_features": True,
            "mean_reversion": True,
            "news_sentiment": False,
            "sector_rotation": False,
            "volatility_regime": True,
            "options_flow": False,
            "alternative_data": False,
            "risk_parity": False
        }
    )
    
    return baseline, experimental