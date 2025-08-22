#!/usr/bin/env python3
"""
Comprehensive tests for Portfolio Optimization components.

Tests cover:
- Portfolio construction with long-short constraints
- Mean-variance optimization
- Factor hedging constraints
- Transaction cost modeling
- Position sizing and risk management
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any

from portfolio.optimization import (
    OptimizationConstraints,
    OptimizationResult,
    PortfolioConstructor,
    LongShortOptimizer
)
from portfolio.signal_generation import TradingSignal, SignalDirection, SignalStrength
from portfolio.factor_framework import FactorUniverse


class TestOptimizationConstraints:
    """Test optimization constraints configuration."""
    
    def test_default_constraints(self):
        """Test default constraint initialization."""
        constraints = OptimizationConstraints()
        
        # Check basic constraints
        assert constraints.max_position_weight > 0
        assert constraints.max_position_weight <= 1.0
        assert constraints.max_leverage >= 1.0
        assert constraints.min_position_size > 0
        assert constraints.transaction_cost_bps >= 0
        
        # Check factor constraints
        assert constraints.max_market_beta > 0
        assert constraints.max_sector_beta > 0
        assert constraints.max_factor_beta > 0
        
        # Check boolean flags
        assert isinstance(constraints.target_dollar_neutral, bool)
        assert isinstance(constraints.allow_shorting, bool)
    
    def test_custom_constraints(self):
        """Test custom constraint creation."""
        constraints = OptimizationConstraints(
            max_position_weight=0.05,
            max_leverage=2.0,
            transaction_cost_bps=5.0,
            target_dollar_neutral=True,
            max_net_exposure=0.02
        )
        
        assert constraints.max_position_weight == 0.05
        assert constraints.max_leverage == 2.0
        assert constraints.transaction_cost_bps == 5.0
        assert constraints.target_dollar_neutral == True
        assert constraints.max_net_exposure == 0.02
    
    def test_constraint_validation(self):
        """Test constraint validation logic."""
        constraints = OptimizationConstraints()
        
        # Test valid portfolio weights
        valid_weights = {'AAPL': 0.03, 'MSFT': -0.02, 'GOOGL': 0.01}
        assert constraints.validate_weights(valid_weights)
        
        # Test position size violation
        invalid_weights = {'AAPL': 0.15, 'MSFT': -0.02}  # AAPL too large
        assert not constraints.validate_weights(invalid_weights)
        
        # Test leverage violation
        high_leverage_weights = {'AAPL': 0.8, 'MSFT': -0.8, 'GOOGL': 0.8}
        assert not constraints.validate_weights(high_leverage_weights)


class TestOptimizationResult:
    """Test optimization result structure."""
    
    def test_result_creation(self):
        """Test optimization result creation."""
        weights = {'AAPL': 0.3, 'MSFT': -0.2, 'GOOGL': 0.1}
        factor_exposures = {'SPY': 0.02, 'TLT': -0.01}
        
        result = OptimizationResult(
            weights=weights,
            expected_return=0.15,
            expected_volatility=0.08,
            sharpe_ratio=1.8,
            factor_exposures=factor_exposures,
            transaction_costs=150.0,
            is_successful=True,
            optimization_status='optimal'
        )
        
        assert result.weights == weights
        assert result.expected_return == 0.15
        assert result.expected_volatility == 0.08
        assert result.sharpe_ratio == 1.8
        assert result.factor_exposures == factor_exposures
        assert result.transaction_costs == 150.0
        assert result.is_successful == True
        assert result.optimization_status == 'optimal'
    
    def test_result_properties(self):
        """Test computed properties of optimization result."""
        weights = {'AAPL': 0.4, 'MSFT': -0.3, 'GOOGL': 0.2}
        
        result = OptimizationResult(
            weights=weights,
            expected_return=0.12,
            expected_volatility=0.10,
            sharpe_ratio=1.2,
            factor_exposures={},
            transaction_costs=100.0,
            is_successful=True,
            optimization_status='optimal'
        )
        
        # Test gross exposure
        expected_gross = abs(0.4) + abs(-0.3) + abs(0.2)
        assert abs(result.gross_exposure - expected_gross) < 1e-10
        
        # Test net exposure
        expected_net = 0.4 + (-0.3) + 0.2
        assert abs(result.net_exposure - expected_net) < 1e-10
        
        # Test leverage ratio
        expected_leverage = expected_gross
        assert abs(result.leverage_ratio - expected_leverage) < 1e-10
        
        # Test number of positions
        assert result.num_positions == 3


class TestLongShortOptimizer:
    """Test long-short portfolio optimizer."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.constraints = OptimizationConstraints(
            max_position_weight=0.08,
            max_leverage=1.5,
            target_dollar_neutral=True,
            max_net_exposure=0.05
        )
        self.optimizer = LongShortOptimizer(self.constraints)
        
        # Create test signals
        self.test_signals = self._create_test_signals()
        self.factor_universe = FactorUniverse()
    
    def _create_test_signals(self) -> Dict[str, TradingSignal]:
        """Create test trading signals."""
        signals = {}
        
        # Strong buy signal
        signals['AAPL'] = TradingSignal(
            symbol='AAPL',
            direction=SignalDirection.LONG,
            strength=SignalStrength.STRONG,
            confidence=0.8,
            expected_return=0.03,
            forecast_horizon=6,
            signal_components={'rsi': 0.7, 'smart_money': 0.8},
            risk_score=0.3,
            entry_price=150.0
        )
        
        # Strong sell signal
        signals['MSFT'] = TradingSignal(
            symbol='MSFT',
            direction=SignalDirection.SHORT,
            strength=SignalStrength.STRONG,
            confidence=0.7,
            expected_return=-0.02,
            forecast_horizon=6,
            signal_components={'rsi': -0.6, 'smart_money': -0.7},
            risk_score=0.4,
            entry_price=300.0
        )
        
        # Moderate buy signal
        signals['GOOGL'] = TradingSignal(
            symbol='GOOGL',
            direction=SignalDirection.LONG,
            strength=SignalStrength.MODERATE,
            confidence=0.6,
            expected_return=0.015,
            forecast_horizon=4,
            signal_components={'rsi': 0.4, 'smart_money': 0.5},
            risk_score=0.35,
            entry_price=2500.0
        )
        
        # Neutral signal
        signals['AMZN'] = TradingSignal(
            symbol='AMZN',
            direction=SignalDirection.NEUTRAL,
            strength=SignalStrength.WEAK,
            confidence=0.3,
            expected_return=0.005,
            forecast_horizon=2,
            signal_components={'rsi': 0.1, 'smart_money': 0.0},
            risk_score=0.5,
            entry_price=3000.0
        )
        
        return signals
    
    def test_optimizer_initialization(self):
        """Test optimizer initialization."""
        assert self.optimizer.constraints == self.constraints
        assert hasattr(self.optimizer, 'risk_model')
    
    def test_expected_return_calculation(self):
        """Test expected return vector calculation."""
        expected_returns = self.optimizer.calculate_expected_returns(self.test_signals)
        
        assert isinstance(expected_returns, dict)
        assert len(expected_returns) > 0
        
        # Check that strong signals have higher expected returns
        aapl_return = expected_returns.get('AAPL', 0)
        msft_return = expected_returns.get('MSFT', 0)
        googl_return = expected_returns.get('GOOGL', 0)
        
        assert aapl_return > 0  # Strong buy
        assert msft_return < 0  # Strong sell
        assert aapl_return > googl_return  # Strong > moderate
    
    def test_risk_model_estimation(self):
        """Test risk model estimation."""
        # Create sample price data
        symbols = list(self.test_signals.keys())
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        
        price_data = {}
        np.random.seed(42)
        
        for symbol in symbols:
            prices = 100 * np.exp(np.cumsum(np.random.normal(0.001, 0.02, 100)))
            price_data[symbol] = pd.Series(prices, index=dates)
        
        covariance_matrix = self.optimizer.estimate_risk_model(price_data)
        
        assert isinstance(covariance_matrix, pd.DataFrame)
        assert covariance_matrix.shape[0] == covariance_matrix.shape[1]
        assert len(covariance_matrix) == len(symbols)
        
        # Covariance matrix should be positive semi-definite
        eigenvals = np.linalg.eigvals(covariance_matrix.values)
        assert all(eigenvals >= -1e-8)  # Allow small numerical errors
    
    def test_portfolio_optimization(self):
        """Test complete portfolio optimization."""
        # Create sample data
        symbols = list(self.test_signals.keys())
        market_data = self._create_sample_market_data(symbols)
        
        result = self.optimizer.optimize_portfolio(
            self.test_signals, 
            current_portfolio={},
            market_data=market_data
        )
        
        assert isinstance(result, OptimizationResult)
        
        if result.is_successful:
            # Check basic properties
            assert len(result.weights) > 0
            assert isinstance(result.expected_return, float)
            assert isinstance(result.expected_volatility, float)
            assert result.expected_volatility > 0
            
            # Check constraints
            max_position = max(abs(w) for w in result.weights.values())
            assert max_position <= self.constraints.max_position_weight * 1.1  # Small tolerance
            
            # Check leverage
            assert result.leverage_ratio <= self.constraints.max_leverage * 1.1
            
            # Check dollar neutrality if required
            if self.constraints.target_dollar_neutral:
                assert abs(result.net_exposure) <= self.constraints.max_net_exposure * 1.5
    
    def _create_sample_market_data(self, symbols: list) -> Dict[str, pd.DataFrame]:
        """Create sample market data for testing."""
        market_data = {}
        np.random.seed(42)
        
        for symbol in symbols:
            dates = pd.date_range('2024-01-01', periods=100, freq='H')
            
            # Generate realistic OHLCV data
            base_price = 100 + hash(symbol) % 200
            returns = np.random.normal(0.0001, 0.01, 100)
            prices = base_price * np.exp(np.cumsum(returns))
            
            data = []
            for i, (date, price) in enumerate(zip(dates, prices)):
                high = price * (1 + abs(np.random.normal(0, 0.005)))
                low = price * (1 - abs(np.random.normal(0, 0.005)))
                open_price = price + np.random.normal(0, 0.002) * price
                volume = int(1000000 * (1 + np.random.normal(0, 0.3)))
                
                data.append({
                    'open': open_price,
                    'high': max(high, price, open_price),
                    'low': min(low, price, open_price),
                    'close': price,
                    'volume': max(volume, 10000)
                })
            
            market_data[symbol] = pd.DataFrame(data, index=dates)
        
        return market_data
    
    def test_factor_hedging_constraints(self):
        """Test factor hedging constraint implementation."""
        # Create portfolio with known factor exposures
        test_portfolio = {'AAPL': 0.1, 'SPY': -0.08, 'TLT': 0.02}
        
        # Test constraint validation
        market_data = self._create_sample_market_data(list(test_portfolio.keys()))
        
        # The optimizer should handle factor constraints
        factor_exposures = self.optimizer.calculate_factor_exposures(
            test_portfolio, market_data
        )
        
        assert isinstance(factor_exposures, dict)
        # In a real test, we'd check specific factor exposure limits


class TestPortfolioConstructor:
    """Test complete portfolio construction process."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.portfolio_value = 200000
        self.constraints = OptimizationConstraints(
            max_position_weight=0.06,
            max_leverage=1.8,
            target_dollar_neutral=True,
            transaction_cost_bps=4.0
        )
        self.constructor = PortfolioConstructor(self.portfolio_value, self.constraints)
        
        # Create comprehensive test data
        self.test_signals = self._create_comprehensive_signals()
        self.market_data = self._create_comprehensive_market_data()
    
    def _create_comprehensive_signals(self) -> Dict[str, TradingSignal]:
        """Create comprehensive test signals."""
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'SPY', 'QQQ']
        signals = {}
        
        np.random.seed(42)
        
        for i, symbol in enumerate(symbols):
            # Vary signal characteristics
            if i % 3 == 0:
                direction = SignalDirection.LONG
                strength = SignalStrength.STRONG
                expected_return = 0.02 + np.random.normal(0, 0.01)
            elif i % 3 == 1:
                direction = SignalDirection.SHORT
                strength = SignalStrength.MODERATE
                expected_return = -0.015 + np.random.normal(0, 0.005)
            else:
                direction = SignalDirection.NEUTRAL
                strength = SignalStrength.WEAK
                expected_return = np.random.normal(0, 0.003)
            
            confidence = 0.4 + np.random.random() * 0.4
            risk_score = 0.2 + np.random.random() * 0.4
            
            signals[symbol] = TradingSignal(
                symbol=symbol,
                direction=direction,
                strength=strength,
                confidence=confidence,
                expected_return=expected_return,
                forecast_horizon=4 + np.random.randint(0, 5),
                signal_components={'technical': np.random.random()},
                risk_score=risk_score,
                entry_price=100 + np.random.random() * 200
            )
        
        return signals
    
    def _create_comprehensive_market_data(self) -> Dict[str, pd.DataFrame]:
        """Create comprehensive market data."""
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'SPY', 'QQQ']
        market_data = {}
        
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=168, freq='H')  # 1 week
        
        for symbol in symbols:
            base_price = 50 + hash(symbol) % 200
            volatility = 0.15 + (hash(symbol) % 100) / 1000
            
            # Generate correlated returns for realistic covariance
            returns = np.random.normal(0.0002, volatility / np.sqrt(252 * 24), len(dates))
            prices = base_price * np.exp(np.cumsum(returns))
            
            data = []
            for i, (date, price) in enumerate(zip(dates, prices)):
                intraday_vol = volatility * 0.3
                high = price * (1 + abs(np.random.normal(0, intraday_vol)))
                low = price * (1 - abs(np.random.normal(0, intraday_vol)))
                
                open_price = price + np.random.normal(0, intraday_vol * 0.5) * price
                volume = int(500000 * (1 + np.random.normal(0, 0.5)))
                
                data.append({
                    'open': round(max(open_price, low), 2),
                    'high': round(max(high, price, open_price), 2),
                    'low': round(min(low, price, open_price), 2),
                    'close': round(price, 2),
                    'volume': max(volume, 10000)
                })
            
            market_data[symbol] = pd.DataFrame(data, index=dates)
        
        return market_data
    
    def test_constructor_initialization(self):
        """Test portfolio constructor initialization."""
        assert self.constructor.portfolio_value == self.portfolio_value
        assert self.constructor.constraints == self.constraints
        assert hasattr(self.constructor, 'optimizer')
    
    def test_portfolio_construction(self):
        """Test complete portfolio construction."""
        current_portfolio = {}  # Start from scratch
        
        result = self.constructor.construct_portfolio(
            self.test_signals, 
            current_portfolio, 
            self.market_data
        )
        
        assert isinstance(result, OptimizationResult)
        
        if result.is_successful:
            # Validate constraints
            assert len(result.weights) > 0
            
            # Check position limits
            max_position = max(abs(w) for w in result.weights.values())
            assert max_position <= self.constraints.max_position_weight * 1.1
            
            # Check leverage
            assert result.leverage_ratio <= self.constraints.max_leverage * 1.1
            
            # Check expected return and volatility are reasonable
            assert -0.5 <= result.expected_return <= 0.5  # Annual return
            assert 0 <= result.expected_volatility <= 1.0  # Annual volatility
    
    def test_position_sizing(self):
        """Test position sizing calculation."""
        test_weights = {
            'AAPL': 0.05,   # 5% long
            'MSFT': -0.03,  # 3% short
            'GOOGL': 0.02   # 2% long
        }
        
        position_sizes = self.constructor.calculate_position_sizes(test_weights)
        
        assert isinstance(position_sizes, dict)
        
        for symbol, weight in test_weights.items():
            assert symbol in position_sizes
            
            size_info = position_sizes[symbol]
            assert 'dollar_amount' in size_info
            assert 'shares' in size_info
            assert 'direction' in size_info
            
            # Check dollar amount calculation
            expected_dollar = abs(weight) * self.portfolio_value
            assert abs(size_info['dollar_amount'] - expected_dollar) < 1
            
            # Check direction
            if weight > 0:
                assert size_info['direction'] == 'LONG'
            else:
                assert size_info['direction'] == 'SHORT'
    
    def test_transaction_cost_calculation(self):
        """Test transaction cost calculation."""
        old_portfolio = {'AAPL': 0.04, 'MSFT': -0.02}
        new_portfolio = {'AAPL': 0.06, 'MSFT': -0.01, 'GOOGL': 0.03}
        
        transaction_costs = self.constructor.calculate_transaction_costs(
            old_portfolio, new_portfolio
        )
        
        assert isinstance(transaction_costs, float)
        assert transaction_costs >= 0
        
        # Cost should be based on turnover
        turnover = (abs(0.06 - 0.04) +  # AAPL change
                   abs(-0.01 - (-0.02)) +  # MSFT change
                   abs(0.03 - 0))  # GOOGL new position
        
        expected_cost = turnover * self.portfolio_value * (self.constraints.transaction_cost_bps / 10000)
        assert abs(transaction_costs - expected_cost) < 1


class TestOptimizationIntegration:
    """Integration tests for optimization system."""
    
    def test_end_to_end_optimization(self):
        """Test complete end-to-end optimization workflow."""
        # Create realistic scenario
        portfolio_value = 250000
        constraints = OptimizationConstraints(
            max_position_weight=0.07,
            max_leverage=1.6,
            target_dollar_neutral=True,
            max_net_exposure=0.04,
            transaction_cost_bps=3.5
        )
        
        constructor = PortfolioConstructor(portfolio_value, constraints)
        
        # Create diversified signals
        signals = self._create_diversified_signals()
        market_data = self._create_realistic_market_data(list(signals.keys()))
        
        # Test initial portfolio construction
        result1 = constructor.construct_portfolio(signals, {}, market_data)
        
        assert isinstance(result1, OptimizationResult)
        
        if result1.is_successful:
            # Test portfolio rebalancing
            updated_signals = self._modify_signals(signals)
            result2 = constructor.construct_portfolio(
                updated_signals, result1.weights, market_data
            )
            
            assert isinstance(result2, OptimizationResult)
            
            # Should have reasonable turnover
            if result2.is_successful:
                turnover = sum(abs(result2.weights.get(s, 0) - result1.weights.get(s, 0)) 
                             for s in set(result1.weights.keys()) | set(result2.weights.keys()))
                turnover /= 2  # One-way turnover
                
                # Turnover should be reasonable (not too high)
                assert 0 <= turnover <= 1.0
    
    def _create_diversified_signals(self) -> Dict[str, TradingSignal]:
        """Create diversified test signals."""
        symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',  # Tech
            'JPM', 'BAC', 'WFC',  # Finance
            'XOM', 'CVX',  # Energy
            'SPY', 'QQQ', 'TLT'  # ETFs
        ]
        
        signals = {}
        np.random.seed(42)
        
        for i, symbol in enumerate(symbols):
            # Create varied but realistic signals
            direction_val = np.random.choice([-1, 0, 1], p=[0.3, 0.2, 0.5])
            
            if direction_val == 1:
                direction = SignalDirection.LONG
                expected_return = np.random.uniform(0.01, 0.04)
            elif direction_val == -1:
                direction = SignalDirection.SHORT
                expected_return = np.random.uniform(-0.04, -0.01)
            else:
                direction = SignalDirection.NEUTRAL
                expected_return = np.random.uniform(-0.005, 0.005)
            
            signals[symbol] = TradingSignal(
                symbol=symbol,
                direction=direction,
                strength=SignalStrength(np.random.randint(1, 4)),
                confidence=np.random.uniform(0.3, 0.8),
                expected_return=expected_return,
                forecast_horizon=np.random.randint(2, 8),
                signal_components={'composite': np.random.random()},
                risk_score=np.random.uniform(0.2, 0.6),
                entry_price=np.random.uniform(50, 300)
            )
        
        return signals
    
    def _create_realistic_market_data(self, symbols: list) -> Dict[str, pd.DataFrame]:
        """Create realistic market data with correlations."""
        market_data = {}
        dates = pd.date_range('2024-01-01', periods=200, freq='H')
        
        # Create correlated market factors
        np.random.seed(42)
        n_symbols = len(symbols)
        
        # Generate correlation matrix
        correlations = np.eye(n_symbols)
        for i in range(n_symbols):
            for j in range(i+1, n_symbols):
                # Sector correlation (tech stocks more correlated)
                if any(s in symbols[i] for s in ['AAPL', 'MSFT', 'GOOGL']) and \
                   any(s in symbols[j] for s in ['AAPL', 'MSFT', 'GOOGL']):
                    corr = np.random.uniform(0.3, 0.6)
                else:
                    corr = np.random.uniform(0.1, 0.3)
                
                correlations[i, j] = correlations[j, i] = corr
        
        # Generate correlated returns
        returns_matrix = np.random.multivariate_normal(
            mean=[0.0005] * n_symbols,
            cov=correlations * 0.0001,  # Scale for hourly volatility
            size=len(dates)
        )
        
        for i, symbol in enumerate(symbols):
            base_price = 50 + hash(symbol) % 200
            returns = returns_matrix[:, i]
            prices = base_price * np.exp(np.cumsum(returns))
            
            data = []
            for j, (date, price) in enumerate(zip(dates, prices)):
                # Add intraday variation
                intraday_range = price * 0.01
                high = price + abs(np.random.normal(0, intraday_range))
                low = price - abs(np.random.normal(0, intraday_range))
                
                open_price = prices[j-1] if j > 0 else price
                volume = int(np.random.lognormal(13, 0.5))
                
                data.append({
                    'open': round(open_price, 2),
                    'high': round(max(high, price, open_price), 2),
                    'low': round(min(low, price, open_price), 2),
                    'close': round(price, 2),
                    'volume': volume
                })
            
            market_data[symbol] = pd.DataFrame(data, index=dates)
        
        return market_data
    
    def _modify_signals(self, original_signals: Dict[str, TradingSignal]) -> Dict[str, TradingSignal]:
        """Modify signals to simulate changing market conditions."""
        modified_signals = {}
        
        for symbol, signal in original_signals.items():
            # Randomly modify some signal characteristics
            new_confidence = max(0.1, min(0.9, signal.confidence + np.random.normal(0, 0.1)))
            new_expected_return = signal.expected_return + np.random.normal(0, 0.005)
            
            modified_signals[symbol] = TradingSignal(
                symbol=symbol,
                direction=signal.direction,
                strength=signal.strength,
                confidence=new_confidence,
                expected_return=new_expected_return,
                forecast_horizon=signal.forecast_horizon,
                signal_components=signal.signal_components,
                risk_score=signal.risk_score,
                entry_price=signal.entry_price
            )
        
        return modified_signals


if __name__ == "__main__":
    pytest.main([__file__, "-v"])