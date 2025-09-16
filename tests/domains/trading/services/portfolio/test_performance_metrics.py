#!/usr/bin/env python3
"""
Comprehensive tests for Performance Metrics components.

Tests cover:
- Risk-adjusted performance metrics (Sharpe, Information, Calmar, Sortino)
- Drawdown analysis and risk measurement
- Factor attribution and market neutrality analysis
- Performance report generation
- Rolling metrics calculation
"""

import pytest
import pandas as pd
import numpy as np

from domains.trading.services.performance_metrics import (
    PerformanceMetrics,
    PerformanceAnalyzer
)

class TestPerformanceMetrics:
    """Test PerformanceMetrics data structure."""

    def test_metrics_creation(self):
        """Test basic metrics creation."""
        metrics = PerformanceMetrics(
            total_return=0.15,
            annualized_return=0.12,
            annualized_volatility=0.08,
            sharpe_ratio=1.5,
            information_ratio=1.2,
            calmar_ratio=2.0,
            sortino_ratio=2.1,
            max_drawdown=-0.05,
            max_drawdown_duration=15,
            current_drawdown=-0.02,
            market_beta=0.02,
            market_alpha=0.10,
            factor_exposures={'SPY': 0.01, 'TLT': -0.02},
            factor_attribution={'SPY': 0.001, 'TLT': -0.0005},
            win_rate=0.65,
            profit_factor=1.8,
            value_at_risk_95=-0.015,
            expected_shortfall_95=-0.022,
            skewness=0.2,
            kurtosis=3.5,
            best_month=0.08,
            worst_month=-0.03,
            positive_months=8,
            total_months=12,
            correlation_to_spy=0.05,
            correlation_to_bonds=-0.02,
            factor_neutrality_score=0.92,
            gross_pnl=0.15,
            net_pnl=0.14,
            transaction_costs=0.01
        )

        # Test basic attributes
        assert metrics.total_return == 0.15
        assert metrics.annualized_return == 0.12
        assert metrics.sharpe_ratio == 1.5
        assert metrics.max_drawdown == -0.05
        assert metrics.win_rate == 0.65

        # Test computed property
        efficiency = metrics.risk_return_efficiency
        assert isinstance(efficiency, float)
        assert efficiency > 0

    def test_metrics_validation(self):
        """Test metrics validation and bounds."""
        metrics = PerformanceMetrics(
            total_return=0.20,
            annualized_return=0.18,
            annualized_volatility=0.10,
            sharpe_ratio=1.8,
            information_ratio=1.5,
            calmar_ratio=3.6,
            sortino_ratio=2.5,
            max_drawdown=-0.03,
            max_drawdown_duration=10,
            current_drawdown=0.0,
            market_beta=0.01,
            market_alpha=0.17,
            factor_exposures={},
            factor_attribution={},
            win_rate=0.70,
            profit_factor=2.1,
            value_at_risk_95=-0.012,
            expected_shortfall_95=-0.018,
            skewness=0.1,
            kurtosis=3.2,
            best_month=0.06,
            worst_month=-0.02,
            positive_months=9,
            total_months=12,
            correlation_to_spy=0.03,
            correlation_to_bonds=-0.01,
            factor_neutrality_score=0.94,
            gross_pnl=0.20,
            net_pnl=0.19,
            transaction_costs=0.01
        )

        # Test reasonable bounds
        assert 0 <= metrics.win_rate <= 1
        assert metrics.profit_factor >= 1.0  # Profitable strategy
        assert metrics.max_drawdown <= 0  # Drawdown is negative
        assert abs(metrics.correlation_to_spy) < 0.1  # Market neutral
        assert 0 <= metrics.factor_neutrality_score <= 1

class TestPerformanceAnalyzer:
    """Test PerformanceAnalyzer functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.analyzer = PerformanceAnalyzer(risk_free_rate=0.02)
        self.test_returns = self._create_test_returns()
        self.factor_returns = self._create_factor_returns()
        self.benchmark_returns = self._create_benchmark_returns()

    def _create_test_returns(self) -> pd.Series:
        """Create realistic test return series."""
        np.random.seed(42)
        n_periods = 1000  # About 6 weeks of hourly data

        # Market-neutral strategy characteristics
        base_return = 0.12 / (252 * 24)  # 12% annual return
        base_volatility = 0.08 / np.sqrt(252 * 24)  # 8% annual volatility

        # Generate returns with some autocorrelation and regime changes
        returns = []
        current_regime = 1.0

        for i in range(n_periods):
            # Occasional regime shifts
            if i % 200 == 0 and i > 0:
                current_regime *= np.random.uniform(0.8, 1.2)

            # Mean reversion + momentum
            if i > 0:
                momentum = 0.05 * returns[-1]  # Small momentum
                mean_reversion = -0.1 * (returns[-1] - base_return)
            else:
                momentum = 0
                mean_reversion = 0

            shock = np.random.normal(0, base_volatility)
            ret = base_return * current_regime + momentum + mean_reversion + shock
            returns.append(ret)

        dates = pd.date_range('2024-01-01', periods=n_periods, freq='1H')
        return pd.Series(returns, index=dates, name='strategy_returns')

    def _create_factor_returns(self) -> pd.DataFrame:
        """Create factor return series."""
        np.random.seed(42)
        n_periods = len(self.test_returns)

        factor_data = {}

        # Market factors
        factor_data['SPY'] = np.random.normal(0.08/(252*24), 0.15/np.sqrt(252*24), n_periods)
        factor_data['QQQ'] = np.random.normal(0.10/(252*24), 0.18/np.sqrt(252*24), n_periods)

        # Interest rate factors
        factor_data['TLT'] = np.random.normal(0.03/(252*24), 0.12/np.sqrt(252*24), n_periods)
        factor_data['SHY'] = np.random.normal(0.02/(252*24), 0.05/np.sqrt(252*24), n_periods)

        # Commodity factors
        factor_data['GLD'] = np.random.normal(0.04/(252*24), 0.16/np.sqrt(252*24), n_periods)
        factor_data['USO'] = np.random.normal(0.05/(252*24), 0.30/np.sqrt(252*24), n_periods)

        # Volatility factor
        factor_data['VIX'] = np.random.normal(-0.02/(252*24), 0.60/np.sqrt(252*24), n_periods)

        return pd.DataFrame(factor_data, index=self.test_returns.index)

    def _create_benchmark_returns(self) -> pd.Series:
        """Create benchmark return series."""
        # Use SPY as benchmark
        return self.factor_returns['SPY']

    def test_analyzer_initialization(self):
        """Test analyzer initialization."""
        assert self.analyzer.benchmark_symbol == 'SPY'
        assert self.analyzer.risk_free_rate == 0.02
        assert hasattr(self.analyzer, 'factor_risk_model')

    def test_comprehensive_metrics_calculation(self):
        """Test comprehensive metrics calculation."""
        metrics = self.analyzer.calculate_comprehensive_metrics(
            self.test_returns,
            self.factor_returns,
            self.benchmark_returns
        )

        assert isinstance(metrics, PerformanceMetrics)

        # Test basic return metrics
        assert isinstance(metrics.total_return, float)
        assert isinstance(metrics.annualized_return, float)
        assert isinstance(metrics.annualized_volatility, float)
        assert metrics.annualized_volatility > 0

        # Test risk-adjusted metrics
        assert isinstance(metrics.sharpe_ratio, float)
        assert isinstance(metrics.information_ratio, float)
        assert isinstance(metrics.calmar_ratio, float)
        assert isinstance(metrics.sortino_ratio, float)

        # Test drawdown metrics
        assert metrics.max_drawdown <= 0
        assert isinstance(metrics.max_drawdown_duration, int)
        assert metrics.max_drawdown_duration >= 0

        # Test factor analysis
        assert isinstance(metrics.market_beta, float)
        assert isinstance(metrics.factor_exposures, dict)
        assert isinstance(metrics.factor_attribution, dict)

        # Test additional metrics
        assert 0 <= metrics.win_rate <= 1
        assert metrics.profit_factor >= 0
        assert metrics.value_at_risk_95 <= 0
        assert metrics.expected_shortfall_95 <= metrics.value_at_risk_95

    def test_frequency_inference(self):
        """Test frequency inference for different data types."""
        # Test hourly data
        hourly_freq = self.analyzer._infer_frequency(self.test_returns)
        assert hourly_freq == 252 * 24  # Hourly

        # Test daily data
        daily_returns = self.test_returns.resample('D').sum()
        daily_freq = self.analyzer._infer_frequency(daily_returns)
        assert daily_freq == 252  # Daily

        # Test monthly data
        monthly_returns = self.test_returns.resample('M').sum()
        monthly_freq = self.analyzer._infer_frequency(monthly_returns)
        assert monthly_freq == 12  # Monthly

    def test_sharpe_ratio_calculation(self):
        """Test Sharpe ratio calculation."""
        sharpe = self.analyzer._calculate_sharpe_ratio(self.test_returns, 252 * 24)

        assert isinstance(sharpe, float)
        assert np.isfinite(sharpe)

        # For a good market-neutral strategy, expect positive Sharpe
        assert sharpe > 0

        # Test edge case: zero volatility
        zero_vol_returns = pd.Series([0.001] * 100)
        sharpe_zero = self.analyzer._calculate_sharpe_ratio(zero_vol_returns, 252)
        assert sharpe_zero == 0

    def test_information_ratio_calculation(self):
        """Test Information ratio calculation."""
        info_ratio = self.analyzer._calculate_information_ratio(
            self.test_returns, self.benchmark_returns, 252 * 24
        )

        assert isinstance(info_ratio, float)
        assert np.isfinite(info_ratio)

        # Test with no benchmark
        info_ratio_none = self.analyzer._calculate_information_ratio(
            self.test_returns, None, 252 * 24
        )
        assert info_ratio_none == 0

    def test_max_drawdown_calculation(self):
        """Test maximum drawdown calculation."""
        max_dd = self.analyzer._calculate_max_drawdown(self.test_returns)

        assert isinstance(max_dd, float)
        assert max_dd <= 0  # Drawdown should be negative
        assert max_dd >= -1  # Shouldn't lose more than 100%

        # Test with portfolio values
        portfolio_values = (1 + self.test_returns).cumprod() * 100000
        max_dd_values = self.analyzer._calculate_max_drawdown(
            self.test_returns, portfolio_values
        )

        assert isinstance(max_dd_values, float)
        assert max_dd_values <= 0

    def test_sortino_ratio_calculation(self):
        """Test Sortino ratio calculation."""
        sortino = self.analyzer._calculate_sortino_ratio(self.test_returns, 252 * 24)

        assert isinstance(sortino, float)
        assert np.isfinite(sortino) or sortino == np.inf

        # Sortino should generally be higher than Sharpe for good strategies
        sharpe = self.analyzer._calculate_sharpe_ratio(self.test_returns, 252 * 24)
        if np.isfinite(sortino) and np.isfinite(sharpe):
            assert sortino >= sharpe

    def test_drawdown_analysis(self):
        """Test detailed drawdown analysis."""
        drawdown_metrics = self.analyzer._analyze_drawdowns(self.test_returns)

        assert isinstance(drawdown_metrics, dict)
        assert 'max_duration' in drawdown_metrics
        assert 'current_drawdown' in drawdown_metrics
        assert 'avg_duration' in drawdown_metrics
        assert 'num_drawdowns' in drawdown_metrics

        assert isinstance(drawdown_metrics['max_duration'], int)
        assert drawdown_metrics['max_duration'] >= 0
        assert isinstance(drawdown_metrics['num_drawdowns'], int)
        assert drawdown_metrics['num_drawdowns'] >= 0

    def test_factor_exposure_analysis(self):
        """Test factor exposure analysis."""
        factor_analysis = self.analyzer._analyze_factor_exposure(
            self.test_returns, self.factor_returns
        )

        assert isinstance(factor_analysis, dict)
        assert 'market_beta' in factor_analysis
        assert 'market_alpha' in factor_analysis
        assert 'factor_exposures' in factor_analysis
        assert 'factor_attribution' in factor_analysis

        # Test beta values are reasonable
        market_beta = factor_analysis['market_beta']
        assert isinstance(market_beta, float)
        assert -2 < market_beta < 2  # Reasonable beta range

        # Test factor exposures
        factor_exposures = factor_analysis['factor_exposures']
        assert isinstance(factor_exposures, dict)
        for factor, exposure in factor_exposures.items():
            assert isinstance(exposure, float)
            assert np.isfinite(exposure)

    def test_var_and_expected_shortfall(self):
        """Test VaR and Expected Shortfall calculation."""
        var_95 = self.analyzer._calculate_var(self.test_returns, 0.95)
        es_95 = self.analyzer._calculate_expected_shortfall(self.test_returns, 0.95)

        assert isinstance(var_95, float)
        assert isinstance(es_95, float)

        # VaR should be negative (loss)
        assert var_95 <= 0

        # Expected Shortfall should be worse than VaR
        assert es_95 <= var_95

        # Test different confidence levels
        var_99 = self.analyzer._calculate_var(self.test_returns, 0.99)
        assert var_99 <= var_95  # Higher confidence = worse VaR

    def test_monthly_performance_analysis(self):
        """Test monthly performance analysis."""
        monthly_metrics = self.analyzer._analyze_monthly_performance(self.test_returns)

        assert isinstance(monthly_metrics, dict)
        assert 'best_month' in monthly_metrics
        assert 'worst_month' in monthly_metrics
        assert 'positive_months' in monthly_metrics
        assert 'total_months' in monthly_metrics

        # Best month should be better than worst month
        assert monthly_metrics['best_month'] >= monthly_metrics['worst_month']

        # Positive months should be reasonable
        assert 0 <= monthly_metrics['positive_months'] <= monthly_metrics['total_months']

    def test_market_neutrality_analysis(self):
        """Test market neutrality analysis."""
        neutrality_metrics = self.analyzer._analyze_market_neutrality(
            self.test_returns, self.factor_returns, self.benchmark_returns
        )

        assert isinstance(neutrality_metrics, dict)
        assert 'spy_correlation' in neutrality_metrics
        assert 'bond_correlation' in neutrality_metrics
        assert 'neutrality_score' in neutrality_metrics

        # Correlations should be reasonable
        spy_corr = neutrality_metrics['spy_correlation']
        assert isinstance(spy_corr, float)
        assert -1 <= spy_corr <= 1

        # Neutrality score should be between 0 and 1
        neutrality_score = neutrality_metrics['neutrality_score']
        assert isinstance(neutrality_score, float)
        assert 0 <= neutrality_score <= 1

    def test_performance_report_generation(self):
        """Test performance report generation."""
        metrics = self.analyzer.calculate_comprehensive_metrics(
            self.test_returns,
            self.factor_returns,
            self.benchmark_returns
        )

        report = self.analyzer.generate_performance_report(metrics)

        assert isinstance(report, str)
        assert len(report) > 0

        # Check that key metrics are included in report
        assert 'Sharpe Ratio' in report
        assert 'Information Ratio' in report
        assert 'Maximum Drawdown' in report
        assert 'Market Beta' in report
        assert 'Win Rate' in report

    def test_rolling_metrics_calculation(self):
        """Test rolling metrics calculation."""
        rolling_metrics = self.analyzer.calculate_rolling_metrics(
            self.test_returns, window=60
        )

        assert isinstance(rolling_metrics, pd.DataFrame)
        assert len(rolling_metrics) == len(self.test_returns)

        # Check columns
        expected_columns = ['sharpe_ratio', 'volatility', 'max_drawdown']
        for col in expected_columns:
            assert col in rolling_metrics.columns

        # Check that values are reasonable
        assert rolling_metrics['volatility'].min() >= 0
        assert rolling_metrics['max_drawdown'].max() <= 0

        # Rolling metrics should have NaN values at the beginning
        assert rolling_metrics['sharpe_ratio'].iloc[:30].isna().any()

class TestPerformanceAnalyzerEdgeCases:
    """Test edge cases and error handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.analyzer = PerformanceAnalyzer()

    def test_insufficient_data(self):
        """Test handling of insufficient data."""
        short_returns = pd.Series([0.01, 0.02, -0.01],
                                 index=pd.date_range('2024-01-01', periods=3))

        with pytest.raises(ValueError):
            self.analyzer.calculate_comprehensive_metrics(short_returns)

    def test_zero_volatility_returns(self):
        """Test handling of zero volatility returns."""
        zero_vol_returns = pd.Series([0.001] * 100,
                                    index=pd.date_range('2024-01-01', periods=100, freq='D'))

        metrics = self.analyzer.calculate_comprehensive_metrics(zero_vol_returns)

        assert metrics.annualized_volatility == 0
        assert metrics.sharpe_ratio == 0
        assert metrics.max_drawdown == 0

    def test_all_negative_returns(self):
        """Test handling of consistently negative returns."""
        np.random.seed(42)
        negative_returns = pd.Series(
            np.random.normal(-0.01, 0.005, 100),
            index=pd.date_range('2024-01-01', periods=100, freq='D')
        )

        metrics = self.analyzer.calculate_comprehensive_metrics(negative_returns)

        assert metrics.total_return < 0
        assert metrics.win_rate < 0.5
        assert metrics.max_drawdown < -0.5  # Significant drawdown

    def test_missing_factor_data(self):
        """Test handling of missing factor data."""
        returns = pd.Series(
            np.random.normal(0.001, 0.01, 50),
            index=pd.date_range('2024-01-01', periods=50, freq='D')
        )

        # Test with None factor returns
        metrics = self.analyzer.calculate_comprehensive_metrics(returns, None, None)

        assert metrics.market_beta == 0
        assert metrics.information_ratio == 0
        assert len(metrics.factor_exposures) == 0

    def test_misaligned_data(self):
        """Test handling of misaligned return series."""
        returns1 = pd.Series(
            np.random.normal(0.001, 0.01, 50),
            index=pd.date_range('2024-01-01', periods=50, freq='D')
        )

        # Benchmark with different dates
        benchmark = pd.Series(
            np.random.normal(0.0008, 0.015, 50),
            index=pd.date_range('2024-01-15', periods=50, freq='D')  # Different start
        )

        metrics = self.analyzer.calculate_comprehensive_metrics(
            returns1, None, benchmark
        )

        # Should handle alignment internally
        assert isinstance(metrics.information_ratio, float)

class TestPerformanceMetricsIntegration:
    """Integration tests for performance measurement system."""

    def test_realistic_strategy_analysis(self):
        """Test analysis of realistic market-neutral strategy."""
        np.random.seed(42)

        # Create realistic market-neutral returns
        n_days = 252  # 1 year of daily data
        dates = pd.date_range('2023-01-01', periods=n_days, freq='D')

        # Simulate market-neutral strategy with:
        # - Low market correlation
        # - Positive alpha
        # - Moderate volatility
        # - Occasional drawdowns

        market_returns = np.random.normal(0.0008, 0.015, n_days)  # Market factor

        # Strategy returns = alpha + small market exposure + idiosyncratic
        alpha = 0.10 / 252  # 10% annual alpha
        market_beta = 0.05  # Low market exposure
        idiosyncratic_vol = 0.08 / np.sqrt(252)  # 8% annual vol

        strategy_returns = []
        for i in range(n_days):
            # Occasional regime changes
            if i in [63, 126, 189]:  # Quarterly regime shifts
                alpha *= np.random.uniform(0.8, 1.2)

            # Add some momentum and mean reversion
            momentum = 0.02 * strategy_returns[-1] if i > 0 else 0
            noise = np.random.normal(0, idiosyncratic_vol)

            ret = alpha + market_beta * market_returns[i] + momentum + noise
            strategy_returns.append(ret)

        returns_series = pd.Series(strategy_returns, index=dates)
        benchmark_series = pd.Series(market_returns, index=dates)

        # Create factor returns
        factor_returns = pd.DataFrame({
            'SPY': market_returns,
            'TLT': np.random.normal(0.0003, 0.008, n_days),
            'VIX': np.random.normal(-0.0001, 0.03, n_days)
        }, index=dates)

        # Analyze performance
        analyzer = PerformanceAnalyzer()
        metrics = analyzer.calculate_comprehensive_metrics(
            returns_series, factor_returns, benchmark_series
        )

        # Validate results for market-neutral strategy
        assert metrics.annualized_return > 0.05  # At least 5% annual return
        assert abs(metrics.correlation_to_spy) < 0.2  # Low market correlation
        assert metrics.sharpe_ratio > 1.0  # Good risk-adjusted return
        assert metrics.max_drawdown > -0.15  # Reasonable drawdown control
        assert abs(metrics.market_beta) < 0.15  # Low market beta

        # Generate and validate report
        report = analyzer.generate_performance_report(metrics)
        assert 'PORTFOLIO PERFORMANCE REPORT' in report
        assert 'RISK-ADJUSTED PERFORMANCE' in report
        assert 'MARKET EXPOSURE' in report

    def test_factor_attribution_accuracy(self):
        """Test accuracy of factor attribution analysis."""
        np.random.seed(42)

        # Create returns with known factor exposures
        n_periods = 500
        dates = pd.date_range('2023-01-01', periods=n_periods, freq='D')

        # Define true factor exposures
        true_exposures = {
            'SPY': 0.1,    # 10% market beta
            'TLT': -0.05,  # -5% bond beta
            'VIX': -0.15   # -15% volatility beta
        }

        # Generate factor returns
        factor_returns = pd.DataFrame({
            'SPY': np.random.normal(0.0008, 0.015, n_periods),
            'TLT': np.random.normal(0.0003, 0.008, n_periods),
            'VIX': np.random.normal(-0.0005, 0.025, n_periods)
        }, index=dates)

        # Generate strategy returns using factor model
        strategy_returns = np.zeros(n_periods)
        alpha = 0.08 / 252  # 8% annual alpha

        for i in range(n_periods):
            factor_contribution = sum(
                true_exposures[factor] * factor_returns.loc[dates[i], factor]
                for factor in true_exposures
            )

            idiosyncratic = np.random.normal(0, 0.005)  # Idiosyncratic risk
            strategy_returns[i] = alpha + factor_contribution + idiosyncratic

        returns_series = pd.Series(strategy_returns, index=dates)

        # Analyze factor exposures
        analyzer = PerformanceAnalyzer()
        metrics = analyzer.calculate_comprehensive_metrics(
            returns_series, factor_returns, factor_returns['SPY']
        )

        # Check that estimated exposures are close to true exposures
        for factor, true_exposure in true_exposures.items():
            if factor in metrics.factor_exposures:
                estimated_exposure = metrics.factor_exposures[factor]
                # Allow for estimation error
                assert abs(estimated_exposure - true_exposure) < 0.1

if __name__ == "__main__":
    pytest.main([__file__, "-v"])