"""
Tests for factor models and residual return calculation.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
import asyncpg

from modeling.factor_models import (
    MarketFactorCalculator,
    SectorFactorCalculator, 
    StyleFactorCalculator,
    ResidualReturnCalculator,
    FactorLoadings,
    FactorModelResult
)


@pytest.fixture
def mock_connection_pool():
    """Mock database connection pool."""
    pool = Mock(spec=asyncpg.Pool)
    conn = Mock(spec=asyncpg.Connection)
    pool.acquire.return_value.__aenter__.return_value = conn
    pool.acquire.return_value.__aexit__.return_value = None
    return pool, conn


@pytest.fixture
def mock_env():
    """Mock environment configuration."""
    env = Mock()
    env.get_table_name.side_effect = lambda x: f"test_{x}"
    return env


@pytest.fixture
def sample_price_data():
    """Sample price data for testing."""
    dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')
    return pd.DataFrame({
        'date': dates,
        'instrument_id': [1] * len(dates),
        'close': [100, 102, 101, 103, 105, 104, 106, 108, 107, 109],
        'volume': [1000, 1500, 1200, 1800, 2000, 1700, 1900, 2100, 1800, 2200]
    })


@pytest.fixture
def sample_market_data():
    """Sample market data for testing."""
    dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')
    return pd.DataFrame({
        'date': dates,
        'close': [4000, 4020, 4010, 4030, 4050, 4040, 4060, 4080, 4070, 4090]
    })


class TestMarketFactorCalculator:
    """Test market factor calculations."""
    
    @pytest.mark.asyncio
    async def test_calculate_market_factor_basic(self, mock_connection_pool, mock_env, sample_market_data):
        """Test basic market factor calculation."""
        pool, conn = mock_connection_pool
        
        # Mock database query return
        conn.fetch.return_value = [
            {'date': row['date'], 'close': row['close']} 
            for _, row in sample_market_data.iterrows()
        ]
        
        calculator = MarketFactorCalculator(pool, mock_env)
        
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 10)
        
        market_factor = await calculator.calculate_market_factor(start_date, end_date)
        
        assert isinstance(market_factor, pd.DataFrame)
        assert 'date' in market_factor.columns
        assert 'market_return' in market_factor.columns
        assert len(market_factor) == len(sample_market_data) - 1  # Returns are one less than prices
        
        # Check that returns are reasonable
        returns = market_factor['market_return']
        assert all(abs(returns) < 0.5)  # No more than 50% daily returns
    
    @pytest.mark.asyncio
    async def test_calculate_market_factor_custom_benchmark(self, mock_connection_pool, mock_env):
        """Test market factor calculation with custom benchmark."""
        pool, conn = mock_connection_pool
        
        conn.fetch.return_value = [
            {'date': datetime(2024, 1, 1), 'close': 100},
            {'date': datetime(2024, 1, 2), 'close': 102},
        ]
        
        calculator = MarketFactorCalculator(pool, mock_env)
        
        market_factor = await calculator.calculate_market_factor(
            datetime(2024, 1, 1), 
            datetime(2024, 1, 2),
            benchmark_symbol='QQQ'
        )
        
        # Should have called with custom benchmark
        conn.fetch.assert_called()
        call_args = conn.fetch.call_args[0][0]
        assert 'QQQ' in call_args
    
    @pytest.mark.asyncio
    async def test_calculate_market_factor_no_data(self, mock_connection_pool, mock_env):
        """Test market factor calculation with no data."""
        pool, conn = mock_connection_pool
        conn.fetch.return_value = []
        
        calculator = MarketFactorCalculator(pool, mock_env)
        
        market_factor = await calculator.calculate_market_factor(
            datetime(2024, 1, 1), 
            datetime(2024, 1, 2)
        )
        
        assert market_factor.empty
    
    @pytest.mark.asyncio
    async def test_calculate_market_beta(self, mock_connection_pool, mock_env):
        """Test market beta calculation."""
        calculator = MarketFactorCalculator(pool=None, env=None)
        
        # Create correlated data
        market_returns = pd.Series([0.01, -0.02, 0.015, -0.01, 0.02])
        stock_returns = pd.Series([0.02, -0.03, 0.025, -0.015, 0.03])  # Beta ≈ 1.5
        
        beta = calculator.calculate_market_beta(stock_returns, market_returns)
        
        assert isinstance(beta, float)
        assert 0.5 < beta < 2.5  # Reasonable beta range
    
    def test_calculate_market_beta_edge_cases(self):
        """Test market beta calculation edge cases."""
        calculator = MarketFactorCalculator(pool=None, env=None)
        
        # Zero variance in market returns
        market_returns = pd.Series([0.01, 0.01, 0.01, 0.01])
        stock_returns = pd.Series([0.02, -0.01, 0.015, -0.005])
        
        beta = calculator.calculate_market_beta(stock_returns, market_returns)
        assert beta == 0.0  # Should return 0 when market has no variance
        
        # Empty series
        empty_returns = pd.Series([], dtype=float)
        beta = calculator.calculate_market_beta(empty_returns, empty_returns)
        assert beta == 0.0


class TestSectorFactorCalculator:
    """Test sector factor calculations."""
    
    @pytest.mark.asyncio
    async def test_calculate_sector_factors(self, mock_connection_pool, mock_env):
        """Test sector factor calculation."""
        pool, conn = mock_connection_pool
        
        # Mock sector data
        conn.fetch.return_value = [
            {'sector': 'Technology', 'date': datetime(2024, 1, 1), 'avg_return': 0.02},
            {'sector': 'Healthcare', 'date': datetime(2024, 1, 1), 'avg_return': 0.01},
            {'sector': 'Technology', 'date': datetime(2024, 1, 2), 'avg_return': -0.01},
            {'sector': 'Healthcare', 'date': datetime(2024, 1, 2), 'avg_return': 0.005},
        ]
        
        calculator = SectorFactorCalculator(pool, mock_env)
        
        sector_factors = await calculator.calculate_sector_factors(
            datetime(2024, 1, 1),
            datetime(2024, 1, 2)
        )
        
        assert isinstance(sector_factors, pd.DataFrame)
        assert 'date' in sector_factors.columns
        assert 'Technology' in sector_factors.columns
        assert 'Healthcare' in sector_factors.columns
        
        # Check data integrity
        assert len(sector_factors) == 2  # Two dates
        assert not sector_factors.isnull().all().all()
    
    @pytest.mark.asyncio
    async def test_get_instrument_sector_loading(self, mock_connection_pool, mock_env):
        """Test instrument sector loading calculation."""
        pool, conn = mock_connection_pool
        
        conn.fetchrow.return_value = {'sector': 'Technology'}
        
        calculator = SectorFactorCalculator(pool, mock_env)
        
        loading = await calculator.get_instrument_sector_loading(123)
        
        assert isinstance(loading, dict)
        assert 'Technology' in loading
        assert loading['Technology'] == 1.0
        
        # Check for other sectors being 0
        total_loading = sum(loading.values())
        assert abs(total_loading - 1.0) < 1e-6  # Should sum to 1
    
    @pytest.mark.asyncio
    async def test_get_instrument_sector_loading_unknown(self, mock_connection_pool, mock_env):
        """Test instrument sector loading for unknown sector."""
        pool, conn = mock_connection_pool
        
        conn.fetchrow.return_value = {'sector': None}
        
        calculator = SectorFactorCalculator(pool, mock_env)
        
        loading = await calculator.get_instrument_sector_loading(123)
        
        # Should return empty loadings for unknown sector
        assert isinstance(loading, dict)
        assert all(v == 0.0 for v in loading.values())


class TestStyleFactorCalculator:
    """Test style factor calculations."""
    
    @pytest.mark.asyncio
    async def test_calculate_style_factors(self, mock_connection_pool, mock_env):
        """Test style factor calculation."""
        pool, conn = mock_connection_pool
        
        # Mock style factor data
        conn.fetch.return_value = [
            {
                'date': datetime(2024, 1, 1),
                'size_factor': 0.01,
                'value_factor': -0.005,
                'momentum_factor': 0.02,
                'quality_factor': 0.008,
                'volatility_factor': -0.01
            },
            {
                'date': datetime(2024, 1, 2),
                'size_factor': -0.008,
                'value_factor': 0.012,
                'momentum_factor': -0.015,
                'quality_factor': 0.003,
                'volatility_factor': 0.007
            }
        ]
        
        calculator = StyleFactorCalculator(pool, mock_env)
        
        style_factors = await calculator.calculate_style_factors(
            datetime(2024, 1, 1),
            datetime(2024, 1, 2)
        )
        
        assert isinstance(style_factors, pd.DataFrame)
        assert 'date' in style_factors.columns
        
        expected_factors = ['size_factor', 'value_factor', 'momentum_factor', 'quality_factor', 'volatility_factor']
        for factor in expected_factors:
            assert factor in style_factors.columns
        
        assert len(style_factors) == 2
    
    @pytest.mark.asyncio
    async def test_calculate_instrument_style_loadings(self, mock_connection_pool, mock_env):
        """Test instrument style loading calculation."""
        pool, conn = mock_connection_pool
        
        # Mock instrument data
        conn.fetchrow.return_value = {
            'market_cap': 50_000_000_000,  # Large cap
            'pe_ratio': 15.0,  # Value
            'beta': 1.2,
            'roa': 0.08,  # Quality
            'price_volatility': 0.25
        }
        
        calculator = StyleFactorCalculator(pool, mock_env)
        
        loadings = await calculator.calculate_instrument_style_loadings(123)
        
        assert isinstance(loadings, dict)
        
        expected_loadings = ['size_loading', 'value_loading', 'momentum_loading', 'quality_loading', 'volatility_loading']
        for loading in expected_loadings:
            assert loading in loadings
            assert isinstance(loadings[loading], (int, float))
            assert -3 <= loadings[loading] <= 3  # Reasonable z-score range
    
    @pytest.mark.asyncio
    async def test_calculate_instrument_style_loadings_missing_data(self, mock_connection_pool, mock_env):
        """Test style loadings with missing data."""
        pool, conn = mock_connection_pool
        
        conn.fetchrow.return_value = {
            'market_cap': None,
            'pe_ratio': None,
            'beta': 1.0,
            'roa': None,
            'price_volatility': None
        }
        
        calculator = StyleFactorCalculator(pool, mock_env)
        
        loadings = await calculator.calculate_instrument_style_loadings(123)
        
        # Should return zero loadings for missing data
        assert isinstance(loadings, dict)
        for loading in ['size_loading', 'value_loading', 'quality_loading', 'volatility_loading']:
            assert loadings[loading] == 0.0


class TestResidualReturnCalculator:
    """Test comprehensive residual return calculations."""
    
    @pytest.mark.asyncio
    async def test_calculate_residual_returns_market_model(self, mock_connection_pool, mock_env):
        """Test residual return calculation using market model."""
        pool, conn = mock_connection_pool
        
        # Mock price data
        price_data = [
            {'instrument_id': 1, 'date': datetime(2024, 1, 1), 'close': 100.0},
            {'instrument_id': 1, 'date': datetime(2024, 1, 2), 'close': 102.0},
            {'instrument_id': 1, 'date': datetime(2024, 1, 3), 'close': 101.0},
        ]
        
        # Mock market data
        market_data = [
            {'date': datetime(2024, 1, 1), 'close': 4000.0},
            {'date': datetime(2024, 1, 2), 'close': 4020.0},
            {'date': datetime(2024, 1, 3), 'close': 4010.0},
        ]
        
        # Configure mock to return different data based on query
        def mock_fetch(query, *args):
            if 'instruments' in query:
                return price_data
            elif 'SPY' in query or 'benchmark' in query.lower():
                return market_data
            else:
                return []
        
        conn.fetch.side_effect = mock_fetch
        
        calculator = ResidualReturnCalculator(pool, mock_env)
        
        residual_returns = await calculator.calculate_residual_returns(
            [1],
            datetime(2024, 1, 1),
            datetime(2024, 1, 3),
            'market_model'
        )
        
        assert isinstance(residual_returns, pd.DataFrame)
        assert 'instrument_id' in residual_returns.columns
        assert 'date' in residual_returns.columns
        assert 'residual_return' in residual_returns.columns
        assert 'market_return' in residual_returns.columns
        assert 'beta' in residual_returns.columns
        
        # Should have one less row than price data (returns calculated)
        assert len(residual_returns) <= len(price_data) - 1
    
    @pytest.mark.asyncio
    async def test_calculate_residual_returns_multi_factor(self, mock_connection_pool, mock_env):
        """Test residual return calculation using multi-factor model."""
        pool, conn = mock_connection_pool
        
        # Mock various data sources
        def mock_fetch(query, *args):
            if 'instruments' in query and 'close' in query:
                return [{'instrument_id': 1, 'date': datetime(2024, 1, 2), 'close': 102.0}]
            elif 'SPY' in query:
                return [{'date': datetime(2024, 1, 2), 'close': 4020.0}]
            elif 'sector' in query.lower():
                return [{'sector': 'Technology', 'date': datetime(2024, 1, 2), 'avg_return': 0.01}]
            else:
                return []
        
        def mock_fetchrow(query, *args):
            if 'sector' in query:
                return {'sector': 'Technology'}
            else:
                return {
                    'market_cap': 10_000_000_000,
                    'pe_ratio': 20.0,
                    'beta': 1.1,
                    'roa': 0.1,
                    'price_volatility': 0.3
                }
        
        conn.fetch.side_effect = mock_fetch
        conn.fetchrow.side_effect = mock_fetchrow
        
        calculator = ResidualReturnCalculator(pool, mock_env)
        
        residual_returns = await calculator.calculate_residual_returns(
            [1],
            datetime(2024, 1, 1),
            datetime(2024, 1, 3),
            'multi_factor'
        )
        
        assert isinstance(residual_returns, pd.DataFrame)
        
        # Should have factor loading columns
        factor_columns = [col for col in residual_returns.columns if 'loading' in col]
        assert len(factor_columns) > 0
    
    @pytest.mark.asyncio
    async def test_fit_factor_model(self, mock_connection_pool, mock_env):
        """Test factor model fitting."""
        calculator = ResidualReturnCalculator(None, None)
        
        # Create sample data
        stock_returns = pd.Series([0.02, -0.01, 0.015, -0.008, 0.01])
        market_returns = pd.Series([0.01, -0.005, 0.01, -0.004, 0.005])
        
        factor_data = pd.DataFrame({
            'market_factor': market_returns,
            'size_factor': [0.002, -0.001, 0.003, -0.002, 0.001],
            'value_factor': [-0.001, 0.002, -0.002, 0.003, -0.001]
        })
        
        loadings = {'size_loading': 0.5, 'value_loading': -0.3}
        
        result = calculator._fit_factor_model(stock_returns, factor_data, loadings)
        
        assert isinstance(result, FactorModelResult)
        assert isinstance(result.residual_returns, pd.Series)
        assert isinstance(result.factor_loadings, FactorLoadings)
        assert isinstance(result.r_squared, float)
        assert 0 <= result.r_squared <= 1
        
        # Residual returns should have same length as input
        assert len(result.residual_returns) == len(stock_returns)
    
    def test_fit_factor_model_insufficient_data(self):
        """Test factor model fitting with insufficient data."""
        calculator = ResidualReturnCalculator(None, None)
        
        stock_returns = pd.Series([0.01])  # Only one data point
        factor_data = pd.DataFrame({'market_factor': [0.005]})
        loadings = {}
        
        result = calculator._fit_factor_model(stock_returns, factor_data, loadings)
        
        # Should handle gracefully
        assert isinstance(result, FactorModelResult)
        assert result.r_squared == 0.0
    
    @pytest.mark.asyncio
    async def test_calculate_residual_returns_no_instruments(self, mock_connection_pool, mock_env):
        """Test residual return calculation with no instruments."""
        pool, conn = mock_connection_pool
        conn.fetch.return_value = []
        
        calculator = ResidualReturnCalculator(pool, mock_env)
        
        residual_returns = await calculator.calculate_residual_returns(
            [],
            datetime(2024, 1, 1),
            datetime(2024, 1, 3),
            'market_model'
        )
        
        assert residual_returns.empty
    
    @pytest.mark.asyncio
    async def test_calculate_residual_returns_invalid_model(self, mock_connection_pool, mock_env):
        """Test residual return calculation with invalid model type."""
        pool, conn = mock_connection_pool
        
        calculator = ResidualReturnCalculator(pool, mock_env)
        
        with pytest.raises(ValueError):
            await calculator.calculate_residual_returns(
                [1],
                datetime(2024, 1, 1),
                datetime(2024, 1, 3),
                'invalid_model'
            )


class TestFactorLoadingsAndResults:
    """Test factor loadings and result dataclasses."""
    
    def test_factor_loadings_creation(self):
        """Test FactorLoadings creation and properties."""
        loadings = FactorLoadings(
            market_loading=1.2,
            size_loading=0.5,
            value_loading=-0.3,
            momentum_loading=0.8,
            quality_loading=0.2,
            volatility_loading=-0.1
        )
        
        assert loadings.market_loading == 1.2
        assert loadings.size_loading == 0.5
        assert loadings.value_loading == -0.3
        
        # Test to_dict method
        loadings_dict = loadings.to_dict()
        assert isinstance(loadings_dict, dict)
        assert loadings_dict['market_loading'] == 1.2
        assert len(loadings_dict) == 6
    
    def test_factor_model_result_creation(self):
        """Test FactorModelResult creation."""
        residuals = pd.Series([0.001, -0.002, 0.003])
        loadings = FactorLoadings(market_loading=1.0)
        
        result = FactorModelResult(
            residual_returns=residuals,
            factor_loadings=loadings,
            r_squared=0.85,
            model_type='market_model'
        )
        
        assert len(result.residual_returns) == 3
        assert result.factor_loadings.market_loading == 1.0
        assert result.r_squared == 0.85
        assert result.model_type == 'market_model'


class TestIntegrationScenarios:
    """Test integration scenarios and realistic workflows."""
    
    @pytest.mark.asyncio
    async def test_full_residual_calculation_workflow(self, mock_connection_pool, mock_env):
        """Test complete residual return calculation workflow."""
        pool, conn = mock_connection_pool
        
        # Create realistic mock data
        dates = [datetime(2024, 1, i) for i in range(1, 11)]
        
        # Stock prices with some trend
        stock_prices = [100, 102, 101, 103, 105, 104, 106, 108, 107, 109]
        # Market prices with correlation
        market_prices = [4000, 4020, 4010, 4030, 4050, 4040, 4060, 4080, 4070, 4090]
        
        price_data = [
            {'instrument_id': 1, 'date': dates[i], 'close': stock_prices[i]}
            for i in range(len(dates))
        ]
        
        market_data = [
            {'date': dates[i], 'close': market_prices[i]}
            for i in range(len(dates))
        ]
        
        def mock_fetch(query, *args):
            if 'instruments' in query:
                return price_data
            else:
                return market_data
        
        conn.fetch.side_effect = mock_fetch
        
        calculator = ResidualReturnCalculator(pool, mock_env)
        
        residual_returns = await calculator.calculate_residual_returns(
            [1],
            dates[0],
            dates[-1],
            'market_model'
        )
        
        # Validate results
        assert not residual_returns.empty
        assert len(residual_returns) == len(dates) - 1  # Returns are one less than prices
        
        # Check that residual returns are reasonable
        residuals = residual_returns['residual_return']
        assert all(abs(residuals) < 0.1)  # No extreme residuals
        
        # Check that beta is reasonable
        betas = residual_returns['beta'].unique()
        assert len(betas) <= 2  # Should be consistent or slightly varying
        assert all(0.5 <= beta <= 2.0 for beta in betas if not np.isnan(beta))
    
    @pytest.mark.asyncio
    async def test_multiple_instruments_calculation(self, mock_connection_pool, mock_env):
        """Test residual return calculation for multiple instruments."""
        pool, conn = mock_connection_pool
        
        # Mock data for multiple instruments
        instruments = [1, 2, 3]
        dates = [datetime(2024, 1, i) for i in range(1, 6)]
        
        price_data = []
        for inst_id in instruments:
            base_price = 100 + inst_id * 10
            for i, date in enumerate(dates):
                price_data.append({
                    'instrument_id': inst_id,
                    'date': date,
                    'close': base_price + i * 2 + np.random.normal(0, 1)
                })
        
        market_data = [
            {'date': date, 'close': 4000 + i * 20}
            for i, date in enumerate(dates)
        ]
        
        def mock_fetch(query, *args):
            if 'instruments' in query:
                return price_data
            else:
                return market_data
        
        conn.fetch.side_effect = mock_fetch
        
        calculator = ResidualReturnCalculator(pool, mock_env)
        
        residual_returns = await calculator.calculate_residual_returns(
            instruments,
            dates[0],
            dates[-1],
            'market_model'
        )
        
        # Should have results for all instruments
        unique_instruments = residual_returns['instrument_id'].unique()
        assert len(unique_instruments) == len(instruments)
        
        # Each instrument should have the same number of return observations
        for inst_id in instruments:
            inst_data = residual_returns[residual_returns['instrument_id'] == inst_id]
            assert len(inst_data) == len(dates) - 1


@pytest.mark.asyncio
async def test_error_handling_scenarios():
    """Test various error handling scenarios."""
    
    # Test with database connection error
    with patch('asyncpg.Pool') as mock_pool:
        mock_pool.acquire.side_effect = Exception("Database connection failed")
        
        calculator = ResidualReturnCalculator(mock_pool, Mock())
        
        # Should handle database errors gracefully
        with pytest.raises(Exception):
            await calculator.calculate_residual_returns(
                [1], datetime.now(), datetime.now(), 'market_model'
            )


if __name__ == "__main__":
    pytest.main([__file__])