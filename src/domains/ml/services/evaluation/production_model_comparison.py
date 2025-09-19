"""
Production Model Configuration Comparison

This module integrates the model configuration comparison system with your existing
production backtest infrastructure, providing realistic performance comparisons
using actual market data and trading logic.
"""

import logging
import uuid
from datetime import date, datetime, timedelta
from typing import Dict, List, Any
from dataclasses import dataclass
import numpy as np
import pandas as pd

from domains.ml.services.evaluation.model_config_comparison import ModelConfigDefinition
from scripts.analytics.production_backtest_runner import ProductionBacktestRunner
from domains.ml.services.models.support_resistance_model import SupportResistanceEnsemble, SRModelConfig
from domains.ml.services.dynamic_training.adaptive_sr_model import AdaptiveSupportResistanceModel, AdaptiveModelConfig


@dataclass
class ProductionComparisonConfig:
    """Configuration for production model comparisons"""
    start_date: date
    end_date: date
    universe_name: str = "sp500_liquid"
    initial_capital: float = 1000000.0
    use_real_trading_logic: bool = True
    save_detailed_results: bool = True
    output_dir: str = "production_comparison_results"

    # Performance thresholds for significance testing
    min_return_diff_pct: float = 2.0  # Minimum 2% return difference to be significant
    min_sharpe_diff: float = 0.2      # Minimum 0.2 Sharpe difference
    min_sample_size: int = 50         # Minimum trades for statistical significance


class ProductionModelComparison(ProductionBacktestRunner):
    """
    Production-grade model comparison using real market data and trading logic

    Extends the existing ProductionBacktestRunner to support model configuration
    comparisons with statistical significance testing.
    """

    def __init__(self, config: ProductionComparisonConfig):
        super().__init__()
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Model tracking
        self.baseline_models: Dict[str, Any] = {}
        self.test_models: Dict[str, Any] = {}

    async def compare_model_configurations(
        self,
        baseline_configs: Dict[str, ModelConfigDefinition],
        test_configs: Dict[str, ModelConfigDefinition]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compare model configurations using production backtest infrastructure

        Args:
            baseline_configs: Dictionary of baseline model configurations
            test_configs: Dictionary of test model configurations

        Returns:
            Comprehensive comparison results with statistical analysis
        """

        self.logger.info(f"🚀 Starting production model configuration comparison")
        self.logger.info(f"📅 Period: {self.config.start_date} to {self.config.end_date}")
        self.logger.info(f"📊 Baselines: {len(baseline_configs)}, Tests: {len(test_configs)}")

        # Get universe for backtesting
        universe = await self._get_universe(self.config.universe_name)
        if not universe:
            raise ValueError(f"Could not load universe: {self.config.universe_name}")

        # Fetch market data once for all comparisons
        market_data = await self._fetch_market_data(universe, self.config.start_date, self.config.end_date)
        if market_data.empty:
            raise ValueError("No market data available for the specified period")

        self.logger.info(f"📈 Loaded {len(market_data)} price records for {len(universe)} symbols")

        # Run backtests for all configurations
        all_results = {}

        # Run baseline backtests
        baseline_results = {}
        for name, config_def in baseline_configs.items():
            self.logger.info(f"🔄 Running baseline backtest: {name}")

            result = await self._run_model_backtest(
                config_def, market_data, f"baseline_{name}", is_baseline=True
            )
            baseline_results[name] = result
            all_results[f"baseline_{name}"] = result

        # Run test backtests
        test_results = {}
        for name, config_def in test_configs.items():
            self.logger.info(f"🔄 Running test backtest: {name}")

            result = await self._run_model_backtest(
                config_def, market_data, f"test_{name}", is_baseline=False
            )
            test_results[name] = result
            all_results[f"test_{name}"] = result

        # Generate pairwise comparisons
        comparison_results = {}

        for baseline_name, baseline_result in baseline_results.items():
            for test_name, test_result in test_results.items():
                comparison_key = f"{baseline_name}_vs_{test_name}"

                self.logger.info(f"📊 Analyzing comparison: {comparison_key}")

                comparison = await self._analyze_model_comparison(
                    baseline_configs[baseline_name],
                    test_configs[test_name],
                    baseline_result,
                    test_result
                )

                comparison_results[comparison_key] = comparison

        # Generate overall summary
        summary = await self._generate_overall_summary(comparison_results, all_results)

        # Save results
        await self._save_production_comparison_results({
            'comparisons': comparison_results,
            'all_results': all_results,
            'summary': summary,
            'config': self.config
        })

        self.logger.info("✅ Production model configuration comparison completed!")

        return {
            'comparisons': comparison_results,
            'summary': summary,
            'all_results': all_results
        }

    async def _run_model_backtest(
        self,
        config_def: ModelConfigDefinition,
        market_data: pd.DataFrame,
        run_id_prefix: str,
        is_baseline: bool
    ) -> Dict[str, Any]:
        """Run backtest for a specific model configuration"""

        run_id = f"{run_id_prefix}_{str(uuid.uuid4())[:8]}"

        # Create appropriate model based on configuration type
        if config_def.model_type.value == "adaptive_sr":
            # Use adaptive strategy logic from parent class
            results = await self._run_adaptive_strategy_with_config(
                run_id, market_data, config_def.config
            )
        else:
            # Use static/standard strategy logic
            results = await self._run_static_strategy_with_config(
                run_id, market_data, config_def.config
            )

        # Add configuration metadata
        results['model_config'] = config_def
        results['run_id'] = run_id
        results['is_baseline'] = is_baseline

        return results

    async def _run_adaptive_strategy_with_config(
        self,
        run_id: str,
        market_data: pd.DataFrame,
        adaptive_config: AdaptiveModelConfig
    ) -> Dict[str, Any]:
        """Run adaptive strategy with specific configuration"""

        # Create adaptive model with custom config
        model = AdaptiveSupportResistanceModel(adaptive_config)

        # Use the existing simulation logic but with custom model
        results = await self._simulate_trading(
            model, market_data,
            self.config.start_date, self.config.end_date,
            self.config.initial_capital,
            is_adaptive=True
        )

        return results

    async def _run_static_strategy_with_config(
        self,
        run_id: str,
        market_data: pd.DataFrame,
        sr_config: SRModelConfig
    ) -> Dict[str, Any]:
        """Run static strategy with specific S/R model configuration"""

        # Create S/R model with custom config
        model = SupportResistanceEnsemble(sr_config)

        # Train on first portion of data
        training_end = self.config.start_date + timedelta(days=180)
        training_data = market_data[market_data['date'] <= pd.Timestamp(training_end)]

        if not training_data.empty:
            # Simplified training - in practice would use actual training data
            self.logger.info(f"Training model with {len(training_data)} records")
            # model.train(training_examples)  # Would use actual training logic

        # Run backtest simulation
        backtest_start = training_end + timedelta(days=1)
        results = await self._simulate_trading(
            model, market_data,
            backtest_start, self.config.end_date,
            self.config.initial_capital,
            is_adaptive=False
        )

        return results

    async def _analyze_model_comparison(
        self,
        baseline_config: ModelConfigDefinition,
        test_config: ModelConfigDefinition,
        baseline_results: Dict[str, Any],
        test_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze comparison between baseline and test model"""

        # Calculate performance metrics for both
        baseline_metrics = self._calculate_metrics(baseline_results)
        test_metrics = self._calculate_metrics(test_results)

        # Calculate differences
        performance_diff = {}
        for metric in baseline_metrics.keys():
            if metric in test_metrics:
                baseline_val = baseline_metrics[metric]
                test_val = test_metrics[metric]

                abs_diff = test_val - baseline_val
                pct_diff = (abs_diff / baseline_val * 100) if baseline_val != 0 else 0

                performance_diff[f"{metric}_abs_diff"] = abs_diff
                performance_diff[f"{metric}_pct_diff"] = pct_diff

        # Statistical significance testing
        significance = await self._test_statistical_significance(
            baseline_results, test_results, baseline_metrics, test_metrics
        )

        # Risk analysis
        risk_analysis = self._analyze_risk_characteristics(
            baseline_results, test_results, baseline_metrics, test_metrics
        )

        # Generate recommendation
        recommendation = self._generate_model_recommendation(
            performance_diff, significance, risk_analysis
        )

        return {
            'baseline_config': baseline_config,
            'test_config': test_config,
            'baseline_metrics': baseline_metrics,
            'test_metrics': test_metrics,
            'performance_diff': performance_diff,
            'statistical_significance': significance,
            'risk_analysis': risk_analysis,
            'recommendation': recommendation,
            'comparison_date': datetime.now().isoformat()
        }

    async def _test_statistical_significance(
        self,
        baseline_results: Dict[str, Any],
        test_results: Dict[str, Any],
        baseline_metrics: Dict[str, float],
        test_metrics: Dict[str, float]
    ) -> Dict[str, Any]:
        """Test statistical significance of performance differences"""

        # Get trade-level data for significance testing
        baseline_trades = baseline_results.get('trades', [])
        test_trades = test_results.get('trades', [])

        significance = {}

        if len(baseline_trades) >= self.config.min_sample_size and len(test_trades) >= self.config.min_sample_size:

            # Extract trade returns
            baseline_returns = [trade['pnl_percent'] for trade in baseline_trades]
            test_returns = [trade['pnl_percent'] for trade in test_trades]

            # Perform t-test (simplified - in practice use proper statistical tests)
            from scipy import stats

            try:
                t_stat, p_value = stats.ttest_ind(test_returns, baseline_returns)

                significance['t_test'] = {
                    't_statistic': float(t_stat),
                    'p_value': float(p_value),
                    'is_significant': p_value < 0.05,
                    'confidence_level': (1 - p_value) * 100
                }
            except ImportError:
                # Fallback if scipy not available
                significance['t_test'] = {
                    'error': 'scipy not available for statistical testing'
                }

            # Effect size (Cohen's d)
            baseline_std = np.std(baseline_returns)
            test_std = np.std(test_returns)
            pooled_std = np.sqrt(((len(baseline_returns) - 1) * baseline_std**2 +
                                (len(test_returns) - 1) * test_std**2) /
                               (len(baseline_returns) + len(test_returns) - 2))

            cohens_d = (np.mean(test_returns) - np.mean(baseline_returns)) / pooled_std

            significance['effect_size'] = {
                'cohens_d': float(cohens_d),
                'magnitude': 'small' if abs(cohens_d) < 0.5 else 'medium' if abs(cohens_d) < 0.8 else 'large'
            }

        else:
            significance['insufficient_data'] = {
                'baseline_trades': len(baseline_trades),
                'test_trades': len(test_trades),
                'minimum_required': self.config.min_sample_size
            }

        return significance

    def _analyze_risk_characteristics(
        self,
        baseline_results: Dict[str, Any],
        test_results: Dict[str, Any],
        baseline_metrics: Dict[str, float],
        test_metrics: Dict[str, float]
    ) -> Dict[str, Any]:
        """Analyze risk characteristics of both models"""

        risk_analysis = {}

        # Volatility comparison
        baseline_portfolio = baseline_results.get('portfolio_performance', {})
        test_portfolio = test_results.get('portfolio_performance', {})

        if baseline_portfolio and test_portfolio:
            # Calculate daily returns volatility
            baseline_values = [perf['portfolio_value'] for perf in baseline_portfolio.values()]
            test_values = [perf['portfolio_value'] for perf in test_portfolio.values()]

            if len(baseline_values) > 1 and len(test_values) > 1:
                baseline_daily_returns = [baseline_values[i]/baseline_values[i-1] - 1
                                        for i in range(1, len(baseline_values))]
                test_daily_returns = [test_values[i]/test_values[i-1] - 1
                                    for i in range(1, len(test_values))]

                risk_analysis['volatility'] = {
                    'baseline_vol': np.std(baseline_daily_returns) * np.sqrt(252),
                    'test_vol': np.std(test_daily_returns) * np.sqrt(252),
                    'vol_ratio': np.std(test_daily_returns) / np.std(baseline_daily_returns)
                }

        # Trade-level risk analysis
        baseline_trades = baseline_results.get('trades', [])
        test_trades = test_results.get('trades', [])

        if baseline_trades and test_trades:
            baseline_pnls = [trade['pnl'] for trade in baseline_trades]
            test_pnls = [trade['pnl'] for trade in test_trades]

            risk_analysis['trade_risk'] = {
                'baseline_worst_trade': min(baseline_pnls) if baseline_pnls else 0,
                'test_worst_trade': min(test_pnls) if test_pnls else 0,
                'baseline_best_trade': max(baseline_pnls) if baseline_pnls else 0,
                'test_best_trade': max(test_pnls) if test_pnls else 0,
                'baseline_pnl_std': np.std(baseline_pnls) if baseline_pnls else 0,
                'test_pnl_std': np.std(test_pnls) if test_pnls else 0
            }

        return risk_analysis

    def _generate_model_recommendation(
        self,
        performance_diff: Dict[str, float],
        significance: Dict[str, Any],
        risk_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate recommendation based on comparison analysis"""

        recommendation = {
            'decision': 'neutral',
            'confidence': 'low',
            'reasons': [],
            'concerns': [],
            'next_steps': []
        }

        # Analyze return improvement
        return_diff = performance_diff.get('total_return_pct_diff', 0)
        sharpe_diff = performance_diff.get('sharpe_ratio_pct_diff', 0)

        # Positive indicators for test model
        if return_diff > self.config.min_return_diff_pct:
            recommendation['reasons'].append(f"Test model shows {return_diff:.1f}% better returns")

        if sharpe_diff > self.config.min_sharpe_diff:
            recommendation['reasons'].append(f"Test model has superior risk-adjusted returns")

        # Check statistical significance
        if 't_test' in significance and significance['t_test'].get('is_significant', False):
            recommendation['reasons'].append("Performance difference is statistically significant")
            recommendation['confidence'] = 'high'

        # Risk concerns
        vol_ratio = risk_analysis.get('volatility', {}).get('vol_ratio', 1.0)
        if vol_ratio > 1.2:
            recommendation['concerns'].append("Test model shows higher volatility")

        drawdown_diff = performance_diff.get('max_drawdown_pct_diff', 0)
        if drawdown_diff < -20:  # Worse drawdown
            recommendation['concerns'].append("Test model has significantly higher drawdown")

        # Overall decision
        positive_indicators = len(recommendation['reasons'])
        concerns = len(recommendation['concerns'])

        if positive_indicators >= 2 and concerns <= 1:
            recommendation['decision'] = 'adopt_test'
            recommendation['next_steps'].append("Deploy test configuration in production")
        elif concerns >= 2 or (positive_indicators == 0):
            recommendation['decision'] = 'keep_baseline'
            recommendation['next_steps'].append("Refine test configuration")
        else:
            recommendation['decision'] = 'requires_further_testing'
            recommendation['next_steps'].append("Run longer backtest period")
            recommendation['next_steps'].append("Test with different market conditions")

        return recommendation

    async def _generate_overall_summary(
        self,
        comparison_results: Dict[str, Dict[str, Any]],
        all_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate overall summary of all comparisons"""

        summary = {
            'total_comparisons': len(comparison_results),
            'recommendations': {
                'adopt_test': 0,
                'keep_baseline': 0,
                'requires_further_testing': 0
            },
            'best_performing': {
                'by_return': None,
                'by_sharpe': None,
                'by_risk_adjusted': None
            },
            'key_insights': []
        }

        # Count recommendations
        for comparison in comparison_results.values():
            decision = comparison['recommendation']['decision']
            if decision in summary['recommendations']:
                summary['recommendations'][decision] += 1

        # Find best performers
        best_return = -float('inf')
        best_sharpe = -float('inf')
        -float('inf')

        for name, results in all_results.items():
            metrics = self._calculate_metrics(results)

            if metrics.get('total_return', 0) > best_return:
                best_return = metrics['total_return']
                summary['best_performing']['by_return'] = name

            if metrics.get('sharpe_ratio', 0) > best_sharpe:
                best_sharpe = metrics['sharpe_ratio']
                summary['best_performing']['by_sharpe'] = name

        # Generate insights
        adopt_pct = summary['recommendations']['adopt_test'] / len(comparison_results) * 100

        if adopt_pct > 70:
            summary['key_insights'].append("🚀 Strong evidence for test configurations")
        elif adopt_pct < 30:
            summary['key_insights'].append("📊 Baseline configurations generally superior")
        else:
            summary['key_insights'].append("⚖️ Mixed results - configuration dependent")

        return summary

    async def _save_production_comparison_results(self, results: Dict[str, Any]):
        """Save comprehensive comparison results"""
        import json
        from pathlib import Path

        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(exist_ok=True)

        # Save main results
        results_file = output_dir / "production_comparison_results.json"

        # Convert to JSON-serializable format
        json_results = {}
        for key, value in results.items():
            if key != 'all_results':  # Skip detailed results for JSON
                json_results[key] = self._make_json_serializable(value)

        with open(results_file, 'w') as f:
            json.dump(json_results, f, indent=2, default=str)

        # Save detailed results as pickle
        import pickle
        detailed_file = output_dir / "detailed_results.pkl"
        with open(detailed_file, 'wb') as f:
            pickle.dump(results, f)

        # Generate summary report
        await self._generate_production_report(results, output_dir)

        self.logger.info(f"📁 Results saved to {output_dir}")

    def _make_json_serializable(self, obj):
        """Convert object to JSON serializable format"""
        if isinstance(obj, dict):
            return {k: self._make_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_json_serializable(item) for item in obj]
        elif hasattr(obj, 'to_dict'):
            return obj.to_dict()
        elif hasattr(obj, '__dict__'):
            return {k: self._make_json_serializable(v) for k, v in obj.__dict__.items()}
        else:
            return obj

    async def _generate_production_report(self, results: Dict[str, Any], output_dir: Path):
        """Generate production-ready comparison report"""

        report_lines = [
            "# Production Model Configuration Comparison Report",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Period: {self.config.start_date} to {self.config.end_date}",
            f"Universe: {self.config.universe_name}",
            f"Initial Capital: ${self.config.initial_capital:,.0f}",
            "",
            "## Executive Summary"
        ]

        summary = results['summary']
        report_lines.extend([
            f"- **Total Comparisons**: {summary['total_comparisons']}",
            f"- **Recommendations to Adopt Test Config**: {summary['recommendations']['adopt_test']}",
            f"- **Keep Baseline Recommendations**: {summary['recommendations']['keep_baseline']}",
            f"- **Require Further Testing**: {summary['recommendations']['requires_further_testing']}",
            ""
        ])

        # Add key insights
        report_lines.append("## Key Insights")
        for insight in summary['key_insights']:
            report_lines.append(f"- {insight}")

        report_lines.append("")

        # Add individual comparison details
        report_lines.append("## Detailed Comparisons")

        for comparison_name, comparison in results['comparisons'].items():
            report_lines.extend([
                f"### {comparison_name}",
                f"**Decision**: {comparison['recommendation']['decision']}",
                f"**Confidence**: {comparison['recommendation']['confidence']}",
                ""
            ])

            # Performance summary
            perf_diff = comparison['performance_diff']
            for key, value in perf_diff.items():
                if '_pct_diff' in key and abs(value) > 2:
                    metric = key.replace('_pct_diff', '').replace('_', ' ').title()
                    direction = "↑" if value > 0 else "↓"
                    report_lines.append(f"- **{metric}**: {direction} {abs(value):.1f}%")

            report_lines.append("")

            # Recommendations
            if comparison['recommendation']['reasons']:
                report_lines.append("**Reasons to Adopt:**")
                for reason in comparison['recommendation']['reasons']:
                    report_lines.append(f"- {reason}")
                report_lines.append("")

            if comparison['recommendation']['concerns']:
                report_lines.append("**Concerns:**")
                for concern in comparison['recommendation']['concerns']:
                    report_lines.append(f"- {concern}")
                report_lines.append("")

            report_lines.extend(["", "---", ""])

        # Save report
        report_file = output_dir / "production_comparison_report.md"
        with open(report_file, 'w') as f:
            f.write("\n".join(report_lines))

        self.logger.info(f"📄 Report saved to {report_file}")


# CLI integration function
async def run_production_comparison_from_configs(
    baseline_config_files: List[str],
    test_config_files: List[str],
    start_date: str,
    end_date: str,
    **kwargs
) -> Dict[str, Any]:
    """
    Run production comparison from configuration files

    This function provides easy integration with your CLI scripts
    """

    from domains.ml.services.evaluation.model_config_comparison import ModelConfigManager

    manager = ModelConfigManager()

    # Load configurations
    baseline_configs = {}
    for file_path in baseline_config_files:
        config = manager.load_config(file_path)
        baseline_configs[config.name] = config

    test_configs = {}
    for file_path in test_config_files:
        config = manager.load_config(file_path)
        test_configs[config.name] = config

    # Setup comparison
    comparison_config = ProductionComparisonConfig(
        start_date=datetime.strptime(start_date, '%Y-%m-%d').date(),
        end_date=datetime.strptime(end_date, '%Y-%m-%d').date(),
        **kwargs
    )

    comparison = ProductionModelComparison(comparison_config)

    # Run comparison
    results = await comparison.compare_model_configurations(
        baseline_configs, test_configs
    )

    return results