#!/usr/bin/env python3
"""
Comprehensive tests for Factor Framework components.

Tests cover:
- Factor universe creation and categorization
- Factor exposure calculation
- Risk model implementation
- Neutrality constraints validation
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List

from domains.trading.services.factor_framework import (
    FactorType,
    RiskFactor,
    FactorUniverse,
    FactorExposureCalculator,
    FactorNeutralityConstraints,
    FactorRiskModel
)


class TestFactorUniverse:
    """Test factor universe creation and management."""

    def test_factor_universe_initialization(self):
        """Test factor universe initialization."""
        universe = FactorUniverse()

        assert len(universe.factors) > 0
        assert len(universe.factor_symbols) == len(universe.factors)
        assert len(universe.factor_weights) == len(universe.factors)

        # Check all factors have required attributes
        for factor in universe.factors:
            assert isinstance(factor, RiskFactor)
            assert factor.symbol is not None
            assert factor.name is not None
            assert isinstance(factor.factor_type, FactorType)
            assert 0 < factor.weight <= 1.0

    def test_factor_categorization(self):
        """Test factor categorization by type."""
        universe = FactorUniverse()

        # Test each factor type
        market_factors = universe.get_factors_by_type(FactorType.MARKET)
        interest_factors = universe.get_factors_by_type(FactorType.INTEREST_RATE)
        commodity_factors = universe.get_factors_by_type(FactorType.COMMODITY)

        assert len(market_factors) > 0
        assert len(interest_factors) > 0
        assert len(commodity_factors) > 0

        # Verify categorization
        for factor in market_factors:
            assert factor.factor_type == FactorType.MARKET

        # Test specific factors exist
        spy_factor = universe.get_factor_by_symbol('SPY')
        assert spy_factor is not None
        assert spy_factor.factor_type == FactorType.MARKET

        tlt_factor = universe.get_factor_by_symbol('TLT')
        assert tlt_factor is not None
        assert tlt_factor.factor_type == FactorType.INTEREST_RATE

    def test_factor_weights(self):
        """Test factor weight consistency."""
        universe = FactorUniverse()

        # All weights should be positive
        for symbol, weight in universe.factor_weights.items():
            assert weight > 0
            assert weight <= 1.0

        # Important factors should have higher weights
        assert universe.factor_weights.get('SPY', 0) >= 0.8  # Market factor
        assert universe.factor_weights.get('VIX', 0) >= 0.8  # Volatility factor


class TestFactorExposureCalculator:
    """Test factor exposure calculation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.universe = FactorUniverse()
        self.calculator = FactorExposureCalculator(self.universe)

        # Create test data
        self.test_returns = self._create_test_returns()
        self.factor_returns = self._create_factor_returns()

    def _create_test_returns(self) -> pd.Series:
        """Create test asset returns."""
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        returns = pd.Series(
            np.random.normal(0.001, 0.02, 100),
            index=dates,
            name='TEST_ASSET'
        )
        return returns

    def _create_factor_returns(self) -> pd.DataFrame:
        """Create test factor returns."""
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=100, freq='D')

        factor_data = {}
        for factor in self.universe.factors[:5]:  # Use first 5 factors
            if factor.symbol == 'SPY':
                # Market factor with higher volatility
                returns = np.random.normal(0.0008, 0.015, 100)
            elif factor.symbol == 'VIX':
                # Volatility factor - mean reverting
                returns = np.random.normal(-0.001, 0.05, 100)
            else:
                # Other factors
                returns = np.random.normal(0.0005, 0.012, 100)

            factor_data[factor.symbol] = returns

        return pd.DataFrame(factor_data, index=dates)

    def test_beta_calculation(self):
        """Test factor beta calculation."""
        betas = self.calculator.calculate_factor_betas(
            self.test_returns,
            self.factor_returns,
            period='medium'
        )

        assert isinstance(betas, dict)
        assert len(betas) > 0

        # All betas should be finite numbers
        for factor, beta in betas.items():
            assert np.isfinite(beta)
            assert isinstance(beta, (int, float))
            # Reasonable beta range
            assert -3 < beta < 3

    def test_portfolio_exposures(self):
        """Test portfolio exposure calculation."""
        # Create test portfolio weights
        portfolio_weights = {
            'ASSET_1': 0.3,
            'ASSET_2': -0.2,
            'ASSET_3': 0.1
        }

        # Create test asset betas
        asset_betas = {
            'ASSET_1': {'SPY': 1.2, 'TLT': -0.1, 'VIX': -0.3},
            'ASSET_2': {'SPY': 0.8, 'TLT': 0.2, 'VIX': 0.1},
            'ASSET_3': {'SPY': 1.5, 'TLT': -0.3, 'VIX': -0.2}
        }

        exposures = self.calculator.calculate_portfolio_exposures(
            portfolio_weights, asset_betas
        )

        assert isinstance(exposures, dict)

        # Check SPY exposure calculation
        expected_spy_exposure = (0.3 * 1.2) + (-0.2 * 0.8) + (0.1 * 1.5)
        assert abs(exposures.get('SPY', 0) - expected_spy_exposure) < 1e-6

    def test_risk_contribution(self):
        """Test factor risk contribution calculation."""
        # Simple test exposures
        exposures = {'SPY': 0.1, 'TLT': -0.05, 'VIX': 0.02}

        # Create simple covariance matrix
        factors = list(exposures.keys())
        cov_matrix = pd.DataFrame(
            np.eye(len(factors)) * 0.01,  # 1% variance
            index=factors,
            columns=factors
        )

        risk_contrib = self.calculator.calculate_factor_risk_contribution(
            exposures, cov_matrix
        )

        assert isinstance(risk_contrib, dict)
        assert len(risk_contrib) == len(exposures)

        # Risk contributions should sum to approximately 1
        total_contrib = sum(abs(contrib) for contrib in risk_contrib.values())
        assert 0.8 < total_contrib < 1.2  # Allow some numerical error


class TestFactorNeutralityConstraints:
    """Test factor neutrality constraints."""

    def setup_method(self):
        """Set up test fixtures."""
        self.universe = FactorUniverse()
        self.constraints = FactorNeutralityConstraints(self.universe)

    def test_constraint_initialization(self):
        """Test constraint initialization."""
        assert len(self.constraints.exposure_limits) > 0

        # Check that all factors have limits
        for factor in self.universe.factors:
            assert factor.symbol in self.constraints.exposure_limits

            min_limit, max_limit = self.constraints.exposure_limits[factor.symbol]
            assert min_limit < 0
            assert max_limit > 0
            assert min_limit < max_limit

    def test_constraint_levels(self):
        """Test constraint level appropriateness."""
        # Market factors should have tighter constraints
        spy_limits = self.constraints.exposure_limits.get('SPY')
        if spy_limits:
            assert abs(spy_limits[0]) <= 0.05  # Max 5% market beta
            assert spy_limits[1] <= 0.05

        # Sector factors should have looser constraints
        xlk_limits = self.constraints.exposure_limits.get('XLK')
        if xlk_limits:
            assert abs(xlk_limits[0]) <= 0.20  # Max 20% sector beta

    def test_neutrality_check(self):
        """Test neutrality status checking."""
        # Test compliant exposures
        compliant_exposures = {factor.symbol: 0.01 for factor in self.universe.factors}

        status = self.constraints.check_neutrality(compliant_exposures)
        assert all(status.values())  # All should be compliant

        # Test non-compliant exposures
        violating_exposures = compliant_exposures.copy()
        violating_exposures['SPY'] = 0.10  # Violate market constraint

        status = self.constraints.check_neutrality(violating_exposures)
        assert not status['SPY']  # Should be non-compliant

    def test_violation_severity(self):
        """Test violation severity calculation."""
        exposures = {factor.symbol: 0.0 for factor in self.universe.factors}
        exposures['SPY'] = 0.10  # Violate market constraint (assuming 5% limit)

        violations = self.constraints.get_violation_severity(exposures)

        assert violations['SPY'] > 0  # Should have violation
        assert all(v >= 0 for v in violations.values())  # All should be non-negative


class TestFactorRiskModel:
    """Test complete factor risk model."""

    def setup_method(self):
        """Set up test fixtures."""
        self.risk_model = FactorRiskModel()
        self.test_factor_returns = self._create_test_factor_returns()

    def _create_test_factor_returns(self) -> pd.DataFrame:
        """Create test factor returns."""
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=100, freq='D')

        # Create correlated factor returns
        n_factors = min(5, len(self.risk_model.factor_universe.factors))
        base_returns = np.random.multivariate_normal(
            mean=[0.0005] * n_factors,
            cov=np.eye(n_factors) * 0.0001 + 0.00005,  # Small correlation
            size=100
        )

        factor_symbols = [f.symbol for f in self.risk_model.factor_universe.factors[:n_factors]]

        return pd.DataFrame(base_returns, index=dates, columns=factor_symbols)

    def test_covariance_estimation(self):
        """Test factor covariance estimation."""
        # Test exponentially weighted method
        cov_ewm = self.risk_model.estimate_factor_covariance(
            self.test_factor_returns, method='ewm'
        )

        assert isinstance(cov_ewm, pd.DataFrame)
        assert cov_ewm.shape[0] == cov_ewm.shape[1]
        assert cov_ewm.shape[0] == len(self.test_factor_returns.columns)

        # Covariance matrix should be positive semi-definite
        eigenvals = np.linalg.eigvals(cov_ewm.values)
        assert all(eigenvals >= -1e-8)  # Allow small numerical errors

        # Test sample method
        cov_sample = self.risk_model.estimate_factor_covariance(
            self.test_factor_returns, method='sample'
        )

        assert isinstance(cov_sample, pd.DataFrame)
        assert cov_sample.shape == cov_ewm.shape

    def test_portfolio_risk_calculation(self):
        """Test portfolio risk calculation."""
        # Create test portfolio
        portfolio_weights = {
            'ASSET_1': 0.4,
            'ASSET_2': -0.3,
            'ASSET_3': 0.2
        }

        # Create test factor exposures
        asset_betas = {
            'ASSET_1': {col: 0.8 for col in self.test_factor_returns.columns},
            'ASSET_2': {col: -0.6 for col in self.test_factor_returns.columns},
            'ASSET_3': {col: 1.2 for col in self.test_factor_returns.columns}
        }

        factor_cov = self.risk_model.estimate_factor_covariance(self.test_factor_returns)

        portfolio_risk = self.risk_model.calculate_portfolio_risk(
            portfolio_weights, asset_betas, factor_cov
        )

        assert isinstance(portfolio_risk, float)
        assert portfolio_risk > 0
        assert portfolio_risk < 1.0  # Reasonable annual volatility

    def test_risk_report_generation(self):
        """Test risk report generation."""
        portfolio_weights = {'ASSET_1': 0.5, 'ASSET_2': -0.3}
        asset_betas = {
            'ASSET_1': {col: 0.8 for col in self.test_factor_returns.columns},
            'ASSET_2': {col: -0.6 for col in self.test_factor_returns.columns}
        }
        factor_cov = self.risk_model.estimate_factor_covariance(self.test_factor_returns)

        report = self.risk_model.generate_risk_report(
            portfolio_weights, asset_betas, factor_cov
        )

        assert isinstance(report, dict)

        # Check required fields
        required_fields = [
            'portfolio_exposures', 'neutrality_status', 'violation_severity',
            'risk_contributions', 'total_risk', 'is_market_neutral'
        ]

        for field in required_fields:
            assert field in report

        assert isinstance(report['is_market_neutral'], bool)
        assert isinstance(report['total_risk'], float)
        assert report['total_risk'] >= 0


class TestFactorFrameworkIntegration:
    """Integration tests for factor framework components."""

    def test_end_to_end_factor_analysis(self):
        """Test complete factor analysis workflow."""
        # Initialize components
        universe = FactorUniverse()
        risk_model = FactorRiskModel()

        # Create realistic test data
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=252, freq='D')  # 1 year

        # Create factor returns with realistic correlations
        factor_returns = self._create_realistic_factor_returns(universe, dates)

        # Create asset returns with factor exposures
        asset_returns, true_betas = self._create_assets_with_factor_exposure(
            factor_returns, ['STOCK_A', 'STOCK_B', 'STOCK_C']
        )

        # Calculate factor exposures
        calculator = FactorExposureCalculator(universe)
        estimated_betas = {}

        for asset in asset_returns.columns:
            asset_rets = asset_returns[asset]
            betas = calculator.calculate_factor_betas(
                asset_rets, factor_returns, period='long'
            )
            estimated_betas[asset] = betas

        # Check that estimated betas are reasonable
        for asset in asset_returns.columns:
            for factor in factor_returns.columns:
                if factor in true_betas[asset] and factor in estimated_betas[asset]:
                    true_beta = true_betas[asset][factor]
                    estimated_beta = estimated_betas[asset][factor]

                    # Allow for estimation error
                    assert abs(estimated_beta - true_beta) < 0.5

        # Test portfolio construction and risk analysis
        portfolio_weights = {'STOCK_A': 0.4, 'STOCK_B': -0.3, 'STOCK_C': 0.2}

        # Generate risk report
        factor_cov = risk_model.estimate_factor_covariance(factor_returns)
        risk_report = risk_model.generate_risk_report(
            portfolio_weights, estimated_betas, factor_cov
        )

        assert risk_report['status'] == 'success' or 'portfolio_exposures' in risk_report
        assert len(risk_report['portfolio_exposures']) > 0

    def _create_realistic_factor_returns(self, universe: FactorUniverse,
                                       dates: pd.DatetimeIndex) -> pd.DataFrame:
        """Create realistic factor returns."""
        np.random.seed(42)
        n_periods = len(dates)

        factor_data = {}

        for factor in universe.factors[:8]:  # Use subset for testing
            if factor.factor_type == FactorType.MARKET:
                # Market factors - moderate volatility, positive drift
                returns = np.random.normal(0.0008, 0.012, n_periods)
            elif factor.factor_type == FactorType.INTEREST_RATE:
                # Interest rate factors - lower volatility
                returns = np.random.normal(0.0002, 0.008, n_periods)
            elif factor.factor_type == FactorType.VOLATILITY:
                # Volatility - mean reverting, higher vol
                returns = np.random.normal(-0.0005, 0.040, n_periods)
            else:
                # Other factors
                returns = np.random.normal(0.0004, 0.015, n_periods)

            factor_data[factor.symbol] = returns

        return pd.DataFrame(factor_data, index=dates)

    def _create_assets_with_factor_exposure(self, factor_returns: pd.DataFrame,
                                          asset_names: List[str]) -> tuple:
        """Create asset returns with known factor exposures."""
        np.random.seed(42)

        # Define true factor betas for each asset
        true_betas = {}
        asset_returns_data = {}

        for asset in asset_names:
            # Generate random but realistic betas
            betas = {}
            for factor in factor_returns.columns:
                if 'SPY' in factor:
                    beta = np.random.normal(1.0, 0.3)  # Market beta around 1
                elif 'VIX' in factor:
                    beta = np.random.normal(-0.1, 0.2)  # Negative VIX beta
                else:
                    beta = np.random.normal(0.0, 0.3)  # Other factors

                betas[factor] = beta

            true_betas[asset] = betas

            # Generate asset returns based on factor model
            asset_rets = np.zeros(len(factor_returns))

            for factor, beta in betas.items():
                if factor in factor_returns.columns:
                    asset_rets += beta * factor_returns[factor].values

            # Add idiosyncratic risk
            idiosyncratic = np.random.normal(0, 0.01, len(factor_returns))
            asset_rets += idiosyncratic

            asset_returns_data[asset] = asset_rets

        asset_returns_df = pd.DataFrame(asset_returns_data, index=factor_returns.index)

        return asset_returns_df, true_betas


if __name__ == "__main__":
    pytest.main([__file__, "-v"])