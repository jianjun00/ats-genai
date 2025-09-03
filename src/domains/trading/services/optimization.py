"""
Portfolio Optimization System for Market-Neutral Strategies

Implements mean-variance optimization with factor hedging constraints
to construct optimal long-short portfolios with risk management.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import warnings
from scipy.optimize import minimize

from .factor_framework import FactorRiskModel
from .signal_generation import TradingSignal, SignalDirection


@dataclass
class OptimizationConstraints:
    """Defines constraints for portfolio optimization."""
    max_position_weight: float = 0.05  # 5% max per position
    max_sector_exposure: float = 0.20  # 20% max per sector
    max_leverage: float = 2.0  # 2x gross leverage (long + short)
    min_position_size: float = 0.001  # 0.1% minimum position
    transaction_cost_bps: float = 5.0  # 5 bps transaction cost
    
    # Factor exposure limits (beta constraints)
    max_market_beta: float = 0.05  # ±5% market beta
    max_sector_beta: float = 0.15  # ±15% sector beta
    max_factor_beta: float = 0.10  # ±10% other factor beta
    
    # Risk constraints
    max_portfolio_volatility: float = 0.15  # 15% annual volatility
    target_sharpe_ratio: float = 1.5  # Target Sharpe ratio
    
    # Long-short constraints
    target_dollar_neutral: bool = True  # Target dollar neutrality
    max_net_exposure: float = 0.10  # ±10% net exposure
    min_gross_exposure: float = 0.80  # Minimum 80% gross exposure


@dataclass
class OptimizationResult:
    """Results from portfolio optimization."""
    weights: Dict[str, float]
    expected_return: float
    expected_volatility: float
    sharpe_ratio: float
    factor_exposures: Dict[str, float]
    gross_exposure: float
    net_exposure: float
    long_exposure: float
    short_exposure: float
    transaction_costs: float
    status: str
    optimization_info: Dict[str, Any]
    
    @property
    def is_successful(self) -> bool:
        """Check if optimization was successful."""
        return self.status == 'success'
    
    @property
    def leverage_ratio(self) -> float:
        """Calculate leverage ratio."""
        return self.gross_exposure


class TransactionCostModel:
    """Models transaction costs for portfolio optimization."""
    
    def __init__(self, cost_bps: float = 5.0):
        self.cost_bps = cost_bps / 10000  # Convert bps to decimal
        
    def calculate_costs(self, current_weights: Dict[str, float],
                       target_weights: Dict[str, float],
                       portfolio_value: float) -> float:
        """Calculate transaction costs for portfolio rebalancing."""
        total_cost = 0.0
        
        # Get all assets
        all_assets = set(current_weights.keys()) | set(target_weights.keys())
        
        for asset in all_assets:
            current_weight = current_weights.get(asset, 0.0)
            target_weight = target_weights.get(asset, 0.0)
            
            # Calculate turnover (absolute change in weights)
            turnover = abs(target_weight - current_weight)
            
            # Cost = turnover * portfolio_value * cost_rate
            cost = turnover * portfolio_value * self.cost_bps
            total_cost += cost
        
        return total_cost
    
    def calculate_turnover(self, current_weights: Dict[str, float],
                          target_weights: Dict[str, float]) -> float:
        """Calculate portfolio turnover rate."""
        all_assets = set(current_weights.keys()) | set(target_weights.keys())
        
        total_turnover = 0.0
        for asset in all_assets:
            current = current_weights.get(asset, 0.0)
            target = target_weights.get(asset, 0.0)
            total_turnover += abs(target - current)
        
        return total_turnover / 2  # Divide by 2 for one-way turnover


class LongShortOptimizer:
    """Optimizes long-short portfolios with factor hedging."""
    
    def __init__(self, factor_risk_model: FactorRiskModel,
                 constraints: OptimizationConstraints = None,
                 portfolio_value: float = 200000):
        self.factor_risk_model = factor_risk_model
        self.constraints = constraints or OptimizationConstraints()
        self.portfolio_value = portfolio_value
        self.transaction_cost_model = TransactionCostModel(self.constraints.transaction_cost_bps)
        
    def optimize_portfolio(self, signals: Dict[str, TradingSignal],
                          current_weights: Optional[Dict[str, float]] = None,
                          factor_returns: Optional[pd.DataFrame] = None,
                          asset_returns: Optional[pd.DataFrame] = None) -> OptimizationResult:
        """
        Optimize portfolio weights given signals and constraints.
        
        Args:
            signals: Trading signals for each asset
            current_weights: Current portfolio weights (for transaction costs)
            factor_returns: Historical factor returns for risk modeling
            asset_returns: Historical asset returns for covariance estimation
            
        Returns:
            Optimization result with optimal weights and metrics
        """
        if not signals:
            return self._create_empty_result("No signals provided")
        
        # Extract expected returns and risk estimates from signals
        asset_list = list(signals.keys())
        expected_returns = np.array([signals[asset].expected_return for asset in asset_list])
        risk_scores = np.array([signals[asset].risk_score for asset in asset_list])
        
        # Build covariance matrix
        if asset_returns is not None and len(asset_returns) > 50:
            cov_matrix = self._build_covariance_matrix(asset_returns, asset_list)
        else:
            # Use simple risk model if no historical data
            cov_matrix = self._build_simple_covariance_matrix(risk_scores, asset_list)
        
        # Build factor exposure matrix
        factor_exposures = self._build_factor_exposure_matrix(asset_list, factor_returns)
        
        # Set up optimization problem
        len(asset_list)
        
        # Objective function: maximize Sharpe ratio with transaction costs
        def objective(weights):
            portfolio_return = np.dot(weights, expected_returns)
            portfolio_variance = np.dot(weights, np.dot(cov_matrix, weights))
            portfolio_vol = np.sqrt(portfolio_variance)
            
            # Transaction costs
            weights_dict = {asset: weight for asset, weight in zip(asset_list, weights)}
            transaction_costs = 0
            if current_weights:
                transaction_costs = self.transaction_cost_model.calculate_costs(
                    current_weights, weights_dict, self.portfolio_value
                )
                # Convert to return impact
                transaction_costs /= self.portfolio_value
            
            # Sharpe ratio (negative for minimization)
            if portfolio_vol > 0:
                sharpe = (portfolio_return - transaction_costs) / portfolio_vol
                return -sharpe
            else:
                return 1000  # High penalty for zero volatility
        
        # Constraints
        constraints = []
        
        # Dollar neutral constraint (optional)
        if self.constraints.target_dollar_neutral:
            constraints.append({
                'type': 'eq',
                'fun': lambda w: np.sum(w)  # Sum of weights = 0
            })
        else:
            # Net exposure constraint
            constraints.append({
                'type': 'ineq',
                'fun': lambda w: self.constraints.max_net_exposure - abs(np.sum(w))
            })
        
        # Gross exposure constraint
        constraints.append({
            'type': 'ineq',
            'fun': lambda w: np.sum(np.abs(w)) - self.constraints.min_gross_exposure
        })
        constraints.append({
            'type': 'ineq',
            'fun': lambda w: self.constraints.max_leverage - np.sum(np.abs(w))
        })
        
        # Factor exposure constraints
        if factor_exposures is not None:
            for i, factor in enumerate(self.factor_risk_model.factor_universe.factor_symbols):
                self.factor_risk_model.factor_universe.factor_weights.get(factor, 1.0)
                
                # Get appropriate limit based on factor type
                if factor in ['SPY', 'QQQ', 'IWM']:
                    max_beta = self.constraints.max_market_beta
                elif factor.startswith('XL'):  # Sector ETFs
                    max_beta = self.constraints.max_sector_beta
                else:
                    max_beta = self.constraints.max_factor_beta
                
                # Exposure = weights @ factor_exposures[:, i]
                constraints.append({
                    'type': 'ineq',
                    'fun': lambda w, idx=i, limit=max_beta: limit - abs(np.dot(w, factor_exposures[:, idx]))
                })
        
        # Individual position size constraints
        bounds = []
        for i, asset in enumerate(asset_list):
            signal = signals[asset]
            
            if signal.direction == SignalDirection.LONG:
                # Long position bounds
                lower = max(0, self.constraints.min_position_size * signal.confidence)
                upper = min(self.constraints.max_position_weight, 
                           signal.confidence * self.constraints.max_position_weight)
                bounds.append((lower, upper))
            elif signal.direction == SignalDirection.SHORT:
                # Short position bounds
                lower = -min(self.constraints.max_position_weight,
                            signal.confidence * self.constraints.max_position_weight)
                upper = max(0, -self.constraints.min_position_size * signal.confidence)
                bounds.append((lower, upper))
            else:
                # Neutral - small bounds around zero
                bounds.append((-0.01, 0.01))
        
        # Initial guess: signal-based weights
        initial_weights = self._generate_initial_weights(signals, asset_list)
        
        # Solve optimization
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                
                result = minimize(
                    objective,
                    initial_weights,
                    method='SLSQP',
                    bounds=bounds,
                    constraints=constraints,
                    options={'maxiter': 1000, 'ftol': 1e-9}
                )
            
            if result.success:
                optimal_weights = {asset: weight for asset, weight in zip(asset_list, result.x)}
                return self._create_optimization_result(
                    optimal_weights, expected_returns, cov_matrix, 
                    factor_exposures, result, current_weights
                )
            else:
                return self._create_empty_result(f"Optimization failed: {result.message}")
                
        except Exception as e:
            return self._create_empty_result(f"Optimization error: {str(e)}")
    
    def _build_covariance_matrix(self, asset_returns: pd.DataFrame, 
                                asset_list: List[str]) -> np.ndarray:
        """Build covariance matrix from historical returns."""
        # Filter to available assets
        available_assets = [asset for asset in asset_list if asset in asset_returns.columns]
        
        if len(available_assets) < len(asset_list):
            # Fallback to simple model if missing data
            return self._build_simple_covariance_matrix(
                np.array([0.5] * len(asset_list)), asset_list
            )
        
        returns_data = asset_returns[available_assets].dropna()
        
        if len(returns_data) < 50:
            # Insufficient data
            return self._build_simple_covariance_matrix(
                np.array([0.5] * len(asset_list)), asset_list
            )
        
        # Use exponentially weighted covariance
        cov_matrix = returns_data.ewm(alpha=0.06).cov().iloc[-len(available_assets):]
        
        # Annualize (assuming daily returns)
        cov_matrix *= 252
        
        return cov_matrix.values
    
    def _build_simple_covariance_matrix(self, risk_scores: np.ndarray,
                                       asset_list: List[str]) -> np.ndarray:
        """Build simple diagonal covariance matrix from risk scores."""
        n_assets = len(asset_list)
        
        # Convert risk scores to volatilities (10-40% annual vol)
        volatilities = 0.10 + risk_scores * 0.30
        
        # Create diagonal matrix with some correlation
        cov_matrix = np.eye(n_assets)
        
        # Add correlation structure
        for i in range(n_assets):
            for j in range(i+1, n_assets):
                # Small positive correlation (market effect)
                correlation = 0.1 + np.random.normal(0, 0.05)
                correlation = np.clip(correlation, -0.3, 0.5)
                cov_matrix[i, j] = cov_matrix[j, i] = correlation
        
        # Scale by volatilities
        vol_matrix = np.outer(volatilities, volatilities)
        cov_matrix *= vol_matrix
        
        return cov_matrix
    
    def _build_factor_exposure_matrix(self, asset_list: List[str],
                                     factor_returns: Optional[pd.DataFrame]) -> Optional[np.ndarray]:
        """Build factor exposure matrix (beta matrix)."""
        if factor_returns is None:
            # Create simple factor exposures based on asset characteristics
            n_assets = len(asset_list)
            n_factors = len(self.factor_risk_model.factor_universe.factor_symbols)
            
            # Random factor exposures with some structure
            np.random.seed(42)  # For reproducibility
            exposures = np.random.normal(0, 0.5, (n_assets, n_factors))
            
            # Add structure based on asset names
            for i, asset in enumerate(asset_list):
                # Market exposure
                if asset in self.factor_risk_model.factor_universe.factor_symbols:
                    # Factor assets have unit exposure to themselves
                    factor_idx = self.factor_risk_model.factor_universe.factor_symbols.index(asset)
                    exposures[i, factor_idx] = 1.0
                else:
                    # Regular assets have moderate market exposure
                    spy_idx = 0  # Assuming SPY is first factor
                    exposures[i, spy_idx] = 0.7 + np.random.normal(0, 0.2)
            
            return exposures
        
        # Calculate actual factor exposures from historical data
        # This would require regression analysis - simplified for now
        return None
    
    def _generate_initial_weights(self, signals: Dict[str, TradingSignal],
                                 asset_list: List[str]) -> np.ndarray:
        """Generate initial weights based on signals."""
        weights = np.zeros(len(asset_list))
        
        # Calculate signal scores
        signal_scores = []
        for asset in asset_list:
            signal = signals[asset]
            score = signal.confidence * signal.strength.value
            if signal.direction == SignalDirection.SHORT:
                score *= -1
            elif signal.direction == SignalDirection.NEUTRAL:
                score = 0
            signal_scores.append(score)
        
        signal_scores = np.array(signal_scores)
        
        # Normalize to target leverage
        if np.sum(np.abs(signal_scores)) > 0:
            weights = signal_scores / np.sum(np.abs(signal_scores)) * 1.0  # Target 100% gross exposure
        
        # Ensure dollar neutrality
        if self.constraints.target_dollar_neutral:
            weights = weights - np.mean(weights)
        
        return weights
    
    def _create_optimization_result(self, optimal_weights: Dict[str, float],
                                   expected_returns: np.ndarray,
                                   cov_matrix: np.ndarray,
                                   factor_exposures: Optional[np.ndarray],
                                   optimization_result,
                                   current_weights: Optional[Dict[str, float]]) -> OptimizationResult:
        """Create optimization result object."""
        weights_array = np.array(list(optimal_weights.values()))
        
        # Calculate portfolio metrics
        portfolio_return = np.dot(weights_array, expected_returns)
        portfolio_variance = np.dot(weights_array, np.dot(cov_matrix, weights_array))
        portfolio_vol = np.sqrt(portfolio_variance)
        
        # Factor exposures
        factor_exp = {}
        if factor_exposures is not None:
            for i, factor in enumerate(self.factor_risk_model.factor_universe.factor_symbols):
                factor_exp[factor] = np.dot(weights_array, factor_exposures[:, i])
        
        # Long/short exposures
        long_exposure = sum(max(0, w) for w in weights_array)
        short_exposure = sum(min(0, w) for w in weights_array)
        gross_exposure = long_exposure - short_exposure  # short_exposure is negative
        net_exposure = long_exposure + short_exposure
        
        # Transaction costs
        transaction_costs = 0
        if current_weights:
            transaction_costs = self.transaction_cost_model.calculate_costs(
                current_weights, optimal_weights, self.portfolio_value
            )
        
        # Sharpe ratio
        sharpe_ratio = portfolio_return / portfolio_vol if portfolio_vol > 0 else 0
        
        return OptimizationResult(
            weights=optimal_weights,
            expected_return=portfolio_return,
            expected_volatility=portfolio_vol,
            sharpe_ratio=sharpe_ratio,
            factor_exposures=factor_exp,
            gross_exposure=gross_exposure,
            net_exposure=net_exposure,
            long_exposure=long_exposure,
            short_exposure=short_exposure,
            transaction_costs=transaction_costs,
            status='success',
            optimization_info={
                'optimization_success': optimization_result.success,
                'optimization_message': optimization_result.message,
                'n_iterations': optimization_result.nit,
                'objective_value': optimization_result.fun
            }
        )
    
    def _create_empty_result(self, message: str) -> OptimizationResult:
        """Create empty result for failed optimization."""
        return OptimizationResult(
            weights={},
            expected_return=0.0,
            expected_volatility=0.0,
            sharpe_ratio=0.0,
            factor_exposures={},
            gross_exposure=0.0,
            net_exposure=0.0,
            long_exposure=0.0,
            short_exposure=0.0,
            transaction_costs=0.0,
            status='failed',
            optimization_info={'error_message': message}
        )


class PortfolioConstructor:
    """High-level portfolio construction system."""
    
    def __init__(self, portfolio_value: float = 200000,
                 constraints: OptimizationConstraints = None):
        self.portfolio_value = portfolio_value
        self.constraints = constraints or OptimizationConstraints()
        self.factor_risk_model = FactorRiskModel()
        self.optimizer = LongShortOptimizer(
            self.factor_risk_model, self.constraints, portfolio_value
        )
        
    def construct_portfolio(self, signals: Dict[str, TradingSignal],
                           current_portfolio: Optional[Dict[str, float]] = None,
                           market_data: Optional[Dict[str, pd.DataFrame]] = None) -> OptimizationResult:
        """
        Construct optimal portfolio from trading signals.
        
        Args:
            signals: Trading signals for universe assets
            current_portfolio: Current portfolio weights (for transaction costs)
            market_data: Historical market data for risk modeling
            
        Returns:
            Optimization result with recommended portfolio
        """
        # Filter signals for quality
        qualified_signals = self._filter_signals(signals)
        
        if not qualified_signals:
            return self.optimizer._create_empty_result("No qualified signals")
        
        # Prepare historical data for risk modeling
        asset_returns = None
        factor_returns = None
        
        if market_data:
            asset_returns = self._prepare_return_data(market_data, list(qualified_signals.keys()))
            factor_returns = self._prepare_factor_returns(market_data)
        
        # Run optimization
        result = self.optimizer.optimize_portfolio(
            qualified_signals, current_portfolio, factor_returns, asset_returns
        )
        
        # Post-process results
        if result.is_successful:
            result = self._post_process_weights(result)
        
        return result
    
    def _filter_signals(self, signals: Dict[str, TradingSignal]) -> Dict[str, TradingSignal]:
        """Filter signals based on quality criteria."""
        filtered = {}
        
        for symbol, signal in signals.items():
            # Quality filters
            if (signal.confidence >= 0.3 and  # Minimum confidence
                signal.direction != signal.direction.NEUTRAL and  # Non-neutral
                abs(signal.expected_return) >= 0.001):  # Minimum expected return
                filtered[symbol] = signal
        
        return filtered
    
    def _prepare_return_data(self, market_data: Dict[str, pd.DataFrame],
                            asset_list: List[str]) -> Optional[pd.DataFrame]:
        """Prepare asset return data for covariance estimation."""
        returns_list = []
        
        for asset in asset_list:
            if asset in market_data and len(market_data[asset]) > 1:
                prices = market_data[asset]['close']
                returns = prices.pct_change().dropna()
                returns.name = asset
                returns_list.append(returns)
        
        if returns_list:
            return pd.concat(returns_list, axis=1)
        return None
    
    def _prepare_factor_returns(self, market_data: Dict[str, pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Prepare factor return data."""
        factor_symbols = self.factor_risk_model.factor_universe.factor_symbols
        return self._prepare_return_data(market_data, factor_symbols)
    
    def _post_process_weights(self, result: OptimizationResult) -> OptimizationResult:
        """Post-process optimization results."""
        # Round small weights to zero
        processed_weights = {}
        min_weight = self.constraints.min_position_size
        
        for asset, weight in result.weights.items():
            if abs(weight) >= min_weight:
                processed_weights[asset] = weight
        
        # Update result with processed weights
        result.weights = processed_weights
        
        # Recalculate exposures
        if processed_weights:
            weights_array = np.array(list(processed_weights.values()))
            result.long_exposure = sum(max(0, w) for w in weights_array)
            result.short_exposure = sum(min(0, w) for w in weights_array)
            result.gross_exposure = result.long_exposure - result.short_exposure
            result.net_exposure = result.long_exposure + result.short_exposure
        
        return result
    
    def calculate_position_sizes(self, weights: Dict[str, float]) -> Dict[str, Dict[str, float]]:
        """Calculate actual position sizes in dollars and shares."""
        position_sizes = {}
        
        for asset, weight in weights.items():
            dollar_amount = weight * self.portfolio_value
            position_sizes[asset] = {
                'weight': weight,
                'dollar_amount': dollar_amount,
                'direction': 'long' if weight > 0 else 'short',
                'abs_dollar_amount': abs(dollar_amount)
            }
        
        return position_sizes