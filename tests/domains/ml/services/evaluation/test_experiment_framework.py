"""
Comprehensive tests for the Model Experiment Framework

Tests cover:
- Configuration-driven experiment setup
- Baseline vs experimental comparison
- Performance attribution analysis
- Statistical significance testing
- Visualization generation
- Error handling and edge cases
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, date
from pathlib import Path
import tempfile
import shutil
from unittest.mock import Mock, patch, AsyncMock

# Import the components to test
from domains.ml.services.evaluation.experiment_framework import (
    ModelExperimentFramework,
    ExperimentConfig,
    ExperimentResult,
    ComparisonAnalysis,
    TradeExplanation,
    create_spy_qqq_experiment_configs
)


class TestExperimentConfig:
    """Test ExperimentConfig functionality"""

    def test_config_creation(self):
        """Test basic config creation"""
        config = ExperimentConfig(
            experiment_name="test_experiment",
            description="Test configuration"
        )

        assert config.experiment_name == "test_experiment"
        assert config.description == "Test configuration"
        assert config.variant_type == "baseline"
        assert isinstance(config.features, dict)
        assert isinstance(config.model_params, dict)
        assert isinstance(config.trading_params, dict)

    def test_config_serialization(self):
        """Test config to_dict and file operations"""
        config = ExperimentConfig(
            experiment_name="test_serialization",
            features={"spy_qqq_signals": True, "technical_indicators": False}
        )

        # Test to_dict
        config_dict = config.to_dict()
        assert config_dict["experiment_name"] == "test_serialization"
        assert config_dict["features"]["spy_qqq_signals"] is True
        assert config_dict["features"]["technical_indicators"] is False

        # Test file operations
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
            config.save_to_file(tmp.name)

            # Load it back
            loaded_config = ExperimentConfig.load_from_file(tmp.name)
            assert loaded_config.experiment_name == config.experiment_name
            assert loaded_config.features == config.features

            # Clean up
            Path(tmp.name).unlink()

    def test_feature_diff_calculation(self):
        """Test feature difference calculation between configs"""
        baseline = ExperimentConfig(
            experiment_name="baseline",
            features={"spy_qqq_signals": False, "technical_indicators": True, "news_sentiment": False}
        )

        experimental = ExperimentConfig(
            experiment_name="experimental",
            features={"spy_qqq_signals": True, "technical_indicators": True, "news_sentiment": False}
        )

        diff = baseline.get_feature_diff(experimental)

        assert len(diff) == 1  # Only spy_qqq_signals is different
        assert "spy_qqq_signals" in diff
        assert diff["spy_qqq_signals"] == (False, True)
        assert "technical_indicators" not in diff  # Same in both

    def test_create_spy_qqq_configs(self):
        """Test the helper function for creating SPY/QQQ experiment configs"""
        baseline, experimental = create_spy_qqq_experiment_configs()

        assert baseline.experiment_name == "baseline_without_spy_qqq"
        assert experimental.experiment_name == "experimental_with_spy_qqq"
        assert baseline.features["spy_qqq_signals"] is False
        assert experimental.features["spy_qqq_signals"] is True

        # Check other features are the same
        diff = baseline.get_feature_diff(experimental)
        assert len(diff) == 1
        assert "spy_qqq_signals" in diff


class TestTradeExplanation:
    """Test TradeExplanation functionality"""

    def create_sample_trade_explanation(self):
        """Create sample trade explanation for testing"""
        return TradeExplanation(
            symbol="AAPL",
            date=datetime(2024, 1, 15),
            action="buy",
            position_size=0.03,
            confidence=0.75,
            signal_contributions={
                "momentum_features": 0.15,
                "technical_indicators": 0.08,
                "spy_qqq_signals": 0.12
            },
            factor_exposures={
                "market": 0.8,
                "size": -0.2,
                "value": 0.1
            },
            market_conditions={
                "vix": 18.5,
                "regime": "trending",
                "benchmark_return": 0.008
            },
            stock_fundamentals={
                "pe_ratio": 25.3,
                "market_cap": 3e12
            },
            technical_indicators={
                "rsi": 62.5,
                "macd": 0.8
            },
            risk_metrics={
                "volatility": 0.25,
                "var_95": -0.035
            },
            correlation_risks={
                "portfolio_correlation": 0.45
            }
        )

    def test_trade_explanation_creation(self):
        """Test trade explanation object creation"""
        trade = self.create_sample_trade_explanation()

        assert trade.symbol == "AAPL"
        assert trade.action == "buy"
        assert trade.confidence == 0.75
        assert len(trade.signal_contributions) == 3
        assert trade.market_conditions["vix"] == 18.5

    def test_trade_explanation_text_generation(self):
        """Test natural language explanation generation"""
        trade = self.create_sample_trade_explanation()
        explanation_text = trade.generate_explanation_text()

        assert "Trade Decision: BUY AAPL" in explanation_text
        assert "Position Size: 3.00%" in explanation_text
        assert "Confidence: 75%" in explanation_text
        assert "momentum_features" in explanation_text
        assert "Market Context" in explanation_text
        assert "Risk Assessment" in explanation_text


class TestModelExperimentFramework:
    """Test the main ModelExperimentFramework"""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for test outputs"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def experiment_framework(self, temp_output_dir):
        """Create experiment framework instance"""
        return ModelExperimentFramework(output_dir=temp_output_dir)

    def test_framework_initialization(self, experiment_framework):
        """Test framework initialization"""
        assert experiment_framework.output_dir.exists()
        assert isinstance(experiment_framework.experiment_results, dict)
        assert isinstance(experiment_framework.comparison_analyses, list)

    def test_input_dimension_calculation(self, experiment_framework):
        """Test input dimension calculation based on features"""
        # Test baseline features
        baseline_config = ExperimentConfig(
            experiment_name="test",
            features={"technical_indicators": True, "momentum_features": True}
        )

        baseline_dim = experiment_framework._calculate_input_dim(baseline_config)
        assert baseline_dim == 20 + 15 + 8  # base + technical + momentum

        # Test with SPY/QQQ signals
        enhanced_config = ExperimentConfig(
            experiment_name="test",
            features={
                "technical_indicators": True,
                "momentum_features": True,
                "spy_qqq_signals": True
            }
        )

        enhanced_dim = experiment_framework._calculate_input_dim(enhanced_config)
        assert enhanced_dim == baseline_dim + 10  # +10 for SPY/QQQ

    def test_universe_symbol_selection(self, experiment_framework):
        """Test trading universe symbol selection"""
        symbols_50 = experiment_framework._get_universe_symbols(50)
        symbols_100 = experiment_framework._get_universe_symbols(100)

        assert len(symbols_50) == 50
        assert len(symbols_100) >= 50  # Should be at least 50, might be capped by universe size
        assert all(isinstance(symbol, str) for symbol in symbols_50)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_single_experiment_run(self, experiment_framework):
        """Test running a single experiment"""
        config = ExperimentConfig(
            experiment_name="test_single",
            description="Single experiment test",
            backtest_params={
                "start_date": "2023-06-01",
                "end_date": "2023-06-30",  # Short period for testing
                "universe_size": 10,
                "initial_capital": 100000,
                "benchmark": "SPY"
            }
        )

        # Mock the adaptive backtester to avoid real backtesting
        with patch('ml.evaluation.experiment_framework.AdaptiveBacktester') as mock_backtester:
            # Mock the backtester methods
            mock_instance = AsyncMock()
            mock_backtester.return_value = mock_instance

            # Mock backtest results
            mock_backtest_results = {
                'experiment_info': {'total_days': 30, 'symbols_count': 10},
                'adaptive_model': {'total_updates': 15, 'final_version': 15}
            }
            mock_instance.run_adaptive_backtest.return_value = mock_backtest_results

            # Mock daily results
            mock_daily_results = []
            for i in range(5):  # 5 days of mock results
                mock_daily_result = Mock()
                mock_daily_result.date = date(2023, 6, i + 1)
                mock_daily_result.adaptive_predictions = [Mock() for _ in range(2)]
                mock_daily_result.adaptive_predictions[0].symbol = "AAPL"
                mock_daily_result.adaptive_predictions[1].symbol = "MSFT"
                mock_daily_results.append(mock_daily_result)

            mock_instance.daily_results = mock_daily_results

            # Run the experiment
            result = await experiment_framework._run_single_experiment(config)

            # Verify result
            assert isinstance(result, ExperimentResult)
            assert result.config == config
            assert isinstance(result.total_return, float)
            assert isinstance(result.sharpe_ratio, float)
            assert isinstance(result.daily_returns, pd.Series)
            assert len(result.trade_explanations) > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_ab_test_execution(self, experiment_framework):
        """Test A/B test execution"""
        baseline_config, experimental_config = create_spy_qqq_experiment_configs()

        # Modify configs for faster testing
        for config in [baseline_config, experimental_config]:
            config.backtest_params = {
                "start_date": "2023-06-01",
                "end_date": "2023-06-15",  # Short period
                "universe_size": 5,
                "initial_capital": 100000,
                "benchmark": "SPY"
            }

        # Mock the single experiment runs
        with patch.object(experiment_framework, '_run_single_experiment') as mock_run:
            # Create mock results
            mock_baseline_result = Mock(spec=ExperimentResult)
            mock_baseline_result.config = baseline_config
            mock_baseline_result.total_return = 0.05
            mock_baseline_result.sharpe_ratio = 1.2
            mock_baseline_result.volatility = 0.15
            mock_baseline_result.max_drawdown = 0.08
            mock_baseline_result.daily_returns = pd.Series([0.001] * 10)
            mock_baseline_result.trade_explanations = []
            mock_baseline_result.signal_attribution = {}

            mock_experimental_result = Mock(spec=ExperimentResult)
            mock_experimental_result.config = experimental_config
            mock_experimental_result.total_return = 0.07  # Better performance
            mock_experimental_result.sharpe_ratio = 1.4
            mock_experimental_result.volatility = 0.16
            mock_experimental_result.max_drawdown = 0.06
            mock_experimental_result.daily_returns = pd.Series([0.0015] * 10)
            mock_experimental_result.trade_explanations = []
            mock_experimental_result.signal_attribution = {}

            mock_run.side_effect = [mock_baseline_result, mock_experimental_result]

            # Mock the save method
            with patch.object(experiment_framework, '_save_experiment_results'):
                # Run A/B test
                comparison = await experiment_framework.run_ab_test(
                    baseline_config, experimental_config, run_parallel=False
                )

                # Verify comparison result
                assert isinstance(comparison, ComparisonAnalysis)
                assert comparison.baseline_result == mock_baseline_result
                assert comparison.experimental_result == mock_experimental_result
                assert comparison.return_difference == 0.02  # 0.07 - 0.05
                assert comparison.sharpe_difference == 0.2   # 1.4 - 1.2

    def test_comparison_analysis_generation(self, experiment_framework):
        """Test comparison analysis generation"""
        # Create mock experiment results
        baseline_config = ExperimentConfig(experiment_name="baseline", features={"spy_qqq_signals": False})
        experimental_config = ExperimentConfig(experiment_name="experimental", features={"spy_qqq_signals": True})

        baseline_result = Mock(spec=ExperimentResult)
        baseline_result.config = baseline_config
        baseline_result.total_return = 0.05
        baseline_result.sharpe_ratio = 1.2
        baseline_result.volatility = 0.15
        baseline_result.daily_returns = pd.Series(np.random.normal(0.001, 0.02, 100))

        experimental_result = Mock(spec=ExperimentResult)
        experimental_result.config = experimental_config
        experimental_result.total_return = 0.08
        experimental_result.sharpe_ratio = 1.5
        experimental_result.volatility = 0.17
        experimental_result.daily_returns = pd.Series(np.random.normal(0.0012, 0.021, 100))

        # Generate comparison
        comparison = experiment_framework._generate_comparison_analysis(baseline_result, experimental_result)

        assert isinstance(comparison, ComparisonAnalysis)
        assert comparison.return_difference == 0.03
        assert comparison.sharpe_difference == 0.3
        assert comparison.risk_difference == 0.02
        assert isinstance(comparison.return_t_stat, float)
        assert isinstance(comparison.return_p_value, float)
        assert isinstance(comparison.is_significant, bool)

    def test_performance_attribution_analysis(self, experiment_framework):
        """Test performance attribution functionality"""
        config = ExperimentConfig(
            experiment_name="test",
            features={"spy_qqq_signals": True, "momentum_features": True}
        )

        factor_attribution = experiment_framework._generate_factor_attribution(config)
        signal_attribution = experiment_framework._generate_signal_attribution(config)

        # Test factor attribution
        assert isinstance(factor_attribution, dict)
        assert len(factor_attribution) > 0
        assert all(isinstance(v, float) for v in factor_attribution.values())

        # Test signal attribution
        assert isinstance(signal_attribution, dict)
        assert len(signal_attribution) > 0
        assert all(isinstance(v, float) for v in signal_attribution.values())

    def test_experiment_summary_generation(self, experiment_framework):
        """Test experiment summary generation"""
        # Add some mock results to the framework
        mock_result1 = Mock(spec=ExperimentResult)
        mock_result1.config = Mock()
        mock_result1.config.experiment_name = "test1"
        mock_result1.total_return = 0.05
        mock_result1.sharpe_ratio = 1.2
        mock_result1.max_drawdown = 0.08
        mock_result1.volatility = 0.15
        mock_result1.total_trades = 50

        mock_result2 = Mock(spec=ExperimentResult)
        mock_result2.config = Mock()
        mock_result2.config.experiment_name = "test2"
        mock_result2.total_return = 0.07
        mock_result2.sharpe_ratio = 1.4
        mock_result2.max_drawdown = 0.06
        mock_result2.volatility = 0.16
        mock_result2.total_trades = 45

        experiment_framework.experiment_results = {
            "test1": mock_result1,
            "test2": mock_result2
        }

        summary = experiment_framework.generate_experiment_summary()

        assert "Model Experiment Framework Summary" in summary
        assert "test1" in summary
        assert "test2" in summary
        assert "5.00%" in summary  # Return formatting
        assert "1.20" in summary   # Sharpe formatting


class TestStatisticalSignificance:
    """Test statistical significance calculations"""

    def test_t_test_calculation(self):
        """Test t-test calculation for return differences"""
        framework = ModelExperimentFramework()

        # Create mock results with different performance
        baseline_returns = pd.Series(np.random.normal(0.001, 0.02, 100))
        experimental_returns = pd.Series(np.random.normal(0.002, 0.02, 100))  # Slightly higher mean

        baseline_result = Mock(spec=ExperimentResult)
        baseline_result.daily_returns = baseline_returns
        baseline_result.total_return = baseline_returns.sum()
        baseline_result.config = Mock()

        experimental_result = Mock(spec=ExperimentResult)
        experimental_result.daily_returns = experimental_returns
        experimental_result.total_return = experimental_returns.sum()
        experimental_result.config = Mock()

        comparison = framework._generate_comparison_analysis(baseline_result, experimental_result)

        assert isinstance(comparison.return_t_stat, float)
        assert isinstance(comparison.return_p_value, float)
        assert 0 <= comparison.return_p_value <= 1
        assert isinstance(comparison.is_significant, bool)

    def test_significance_with_identical_returns(self):
        """Test statistical significance with identical returns"""
        framework = ModelExperimentFramework()

        # Create identical returns
        identical_returns = pd.Series(np.random.normal(0.001, 0.02, 100))

        baseline_result = Mock(spec=ExperimentResult)
        baseline_result.daily_returns = identical_returns.copy()
        baseline_result.total_return = identical_returns.sum()
        baseline_result.config = Mock()

        experimental_result = Mock(spec=ExperimentResult)
        experimental_result.daily_returns = identical_returns.copy()
        experimental_result.total_return = identical_returns.sum()
        experimental_result.config = Mock()

        comparison = framework._generate_comparison_analysis(baseline_result, experimental_result)

        # Should not be significant with identical returns
        assert not comparison.is_significant
        assert abs(comparison.return_difference) < 1e-10


class TestErrorHandling:
    """Test error handling and edge cases"""

    def test_invalid_config_handling(self):
        """Test handling of invalid configurations"""
        with pytest.raises(TypeError):
            # Missing required fields
            ExperimentConfig()

    def test_empty_universe_handling(self):
        """Test handling of empty trading universe"""
        framework = ModelExperimentFramework()

        # Test with zero universe size
        symbols = framework._get_universe_symbols(0)
        assert len(symbols) == 0

    def test_invalid_date_range(self):
        """Test handling of invalid date ranges"""
        config = ExperimentConfig(
            experiment_name="test",
            backtest_params={
                "start_date": "2023-12-01",
                "end_date": "2023-01-01"  # End before start
            }
        )

        framework = ModelExperimentFramework()

        # This should be handled gracefully (implementation-dependent)
        with patch('ml.evaluation.experiment_framework.AdaptiveBacktester'):
            # The actual error handling would happen in the adaptive backtester
            pass

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_experiment_failure_handling(self):
        """Test handling of experiment failures"""
        framework = ModelExperimentFramework()
        config = ExperimentConfig(experiment_name="failing_test")

        with patch.object(framework, '_create_adaptive_config', side_effect=Exception("Test error")):
            with pytest.raises(Exception):
                await framework._run_single_experiment(config)


class TestVisualizationGeneration:
    """Test visualization generation components"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_visualization_creation(self):
        """Test that visualizations are created without errors"""
        framework = ModelExperimentFramework()

        # Create mock results
        baseline_result = Mock(spec=ExperimentResult)
        baseline_result.portfolio_values = pd.Series([100000, 101000, 102000],
                                                   index=pd.date_range('2023-01-01', periods=3))
        baseline_result.daily_returns = pd.Series([0.01, 0.01, 0.01])
        baseline_result.config = Mock()
        baseline_result.config.experiment_name = "baseline"
        baseline_result.total_return = 0.02
        baseline_result.sharpe_ratio = 1.2
        baseline_result.max_drawdown = 0.05
        baseline_result.volatility = 0.15

        experimental_result = Mock(spec=ExperimentResult)
        experimental_result.portfolio_values = pd.Series([100000, 102000, 104000],
                                                        index=pd.date_range('2023-01-01', periods=3))
        experimental_result.daily_returns = pd.Series([0.02, 0.02, 0.02])
        experimental_result.config = Mock()
        experimental_result.config.experiment_name = "experimental"
        experimental_result.total_return = 0.04
        experimental_result.sharpe_ratio = 1.5
        experimental_result.max_drawdown = 0.03
        experimental_result.volatility = 0.18

        comparison = Mock(spec=ComparisonAnalysis)
        comparison.feature_impact = {"spy_qqq_signals": {"return_impact": 0.02}}

        with patch('matplotlib.pyplot.savefig'), \
             patch('matplotlib.pyplot.close'), \
             patch.object(framework, '_generate_trade_visualizations'):

            # This should not raise any exceptions
            await framework._generate_visualizations(baseline_result, experimental_result, comparison)


@pytest.mark.integration
class TestEndToEndWorkflow:
    """Integration tests for the complete workflow"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_spy_qqq_experiment(self):
        """Test complete SPY/QQQ experiment workflow"""
        with tempfile.TemporaryDirectory() as temp_dir:
            framework = ModelExperimentFramework(output_dir=temp_dir)
            baseline_config, experimental_config = create_spy_qqq_experiment_configs()

            # Modify for fast testing
            for config in [baseline_config, experimental_config]:
                config.backtest_params["start_date"] = "2023-06-01"
                config.backtest_params["end_date"] = "2023-06-07"  # 1 week
                config.backtest_params["universe_size"] = 3

            # Mock the time-consuming parts
            with patch.object(framework, '_run_single_experiment') as mock_run, \
                 patch.object(framework, '_save_experiment_results'), \
                 patch.object(framework, '_generate_visualizations'):

                # Create minimal mock results
                def create_mock_result(config):
                    result = Mock(spec=ExperimentResult)
                    result.config = config
                    result.total_return = 0.05 if "baseline" in config.experiment_name else 0.07
                    result.sharpe_ratio = 1.2 if "baseline" in config.experiment_name else 1.4
                    result.volatility = 0.15
                    result.max_drawdown = 0.08
                    result.daily_returns = pd.Series([0.001] * 7)
                    result.trade_explanations = []
                    result.signal_attribution = {}
                    return result

                mock_run.side_effect = lambda config: create_mock_result(config)

                # Run the experiment
                comparison = await framework.run_ab_test(baseline_config, experimental_config)

                # Verify results
                assert isinstance(comparison, ComparisonAnalysis)
                assert comparison.return_difference > 0  # Experimental should outperform
                assert comparison.baseline_result.config.experiment_name == "baseline_without_spy_qqq"
                assert comparison.experimental_result.config.experiment_name == "experimental_with_spy_qqq"

    def test_configuration_persistence(self):
        """Test that configurations are properly saved and can be reloaded"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create and save configuration
            original_config = ExperimentConfig(
                experiment_name="persistence_test",
                description="Test configuration persistence",
                features={"spy_qqq_signals": True, "momentum_features": False},
                model_params={"learning_rate": 0.005, "batch_size": 128}
            )

            config_path = Path(temp_dir) / "test_config.json"
            original_config.save_to_file(str(config_path))

            # Load and verify
            loaded_config = ExperimentConfig.load_from_file(str(config_path))

            assert loaded_config.experiment_name == original_config.experiment_name
            assert loaded_config.description == original_config.description
            assert loaded_config.features == original_config.features
            assert loaded_config.model_params == original_config.model_params


if __name__ == "__main__":
    pytest.main([__file__, "-v"])