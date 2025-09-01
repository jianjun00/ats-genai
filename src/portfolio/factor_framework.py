"""
Factor Framework for Market-Neutral Portfolio Construction

This module defines the core risk factors and exposure calculation system
for building a market-neutral portfolio that hedges against major market risks.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class FactorType(Enum):
    """Types of risk factors."""
    MARKET = "market"
    INTEREST_RATE = "interest_rate"
    COMMODITY = "commodity"
    CURRENCY = "currency"
    VOLATILITY = "volatility"
    SECTOR = "sector"
    SIZE = "size"
    MOMENTUM = "momentum"


@dataclass
class RiskFactor:
    """Represents a single risk factor."""
    symbol: str
    name: str
    factor_type: FactorType
    weight: float = 1.0  # Importance weight in risk model
    description: str = ""
    
    def __post_init__(self):
        if not self.description:
            self.description = f"{self.name} ({self.symbol})"


class FactorUniverse:
    """Defines the universe of risk factors for portfolio hedging."""
    
    def __init__(self):
        self.factors = self._initialize_factors()
        self.factor_symbols = [f.symbol for f in self.factors]
        self.factor_weights = {f.symbol: f.weight for f in self.factors}
    
    def _initialize_factors(self) -> List[RiskFactor]:
        """Initialize the comprehensive factor universe."""
        factors = [
            # Market Factors
            RiskFactor("SPY", "S&P 500", FactorType.MARKET, 1.0, 
                      "Broad US equity market exposure"),
            RiskFactor("QQQ", "NASDAQ 100", FactorType.MARKET, 0.8,
                      "Large-cap technology-heavy market exposure"),
            RiskFactor("IWM", "Russell 2000", FactorType.SIZE, 0.6,
                      "Small-cap equity exposure"),
            RiskFactor("EFA", "EAFE", FactorType.MARKET, 0.4,
                      "International developed market exposure"),
            
            # Interest Rate Factors
            RiskFactor("TLT", "20+ Year Treasury", FactorType.INTEREST_RATE, 0.8,
                      "Long-term interest rate sensitivity"),
            RiskFactor("SHY", "1-3 Year Treasury", FactorType.INTEREST_RATE, 0.4,
                      "Short-term interest rate sensitivity"),
            RiskFactor("^TNX", "10-Year Note Yield", FactorType.INTEREST_RATE, 1.0,
                      "Benchmark interest rate level"),
            
            # Commodity Factors
            RiskFactor("USO", "Oil Fund", FactorType.COMMODITY, 0.7,
                      "Crude oil price exposure"),
            RiskFactor("GLD", "Gold", FactorType.COMMODITY, 0.6,
                      "Precious metals and inflation hedge"),
            RiskFactor("DBA", "Agriculture", FactorType.COMMODITY, 0.3,
                      "Agricultural commodity exposure"),
            
            # Currency Factor
            RiskFactor("DXY", "Dollar Index", FactorType.CURRENCY, 0.8,
                      "US Dollar strength"),
            
            # Volatility Factor
            RiskFactor("VIX", "Volatility Index", FactorType.VOLATILITY, 1.0,
                      "Market fear and volatility"),
            
            # Sector Factors
            RiskFactor("XLK", "Technology", FactorType.SECTOR, 0.9,
                      "Technology sector exposure"),
            RiskFactor("XLF", "Financials", FactorType.SECTOR, 0.8,
                      "Financial sector exposure"),
            RiskFactor("XLE", "Energy", FactorType.SECTOR, 0.7,
                      "Energy sector exposure"),
            RiskFactor("XLI", "Industrials", FactorType.SECTOR, 0.6,
                      "Industrial sector exposure"),
            RiskFactor("XLV", "Healthcare", FactorType.SECTOR, 0.6,
                      "Healthcare sector exposure"),
            RiskFactor("XLC", "Communication", FactorType.SECTOR, 0.5,
                      "Communication services sector"),
            
            # Style Factors
            RiskFactor("MTUM", "Momentum", FactorType.MOMENTUM, 0.6,
                      "Price momentum factor"),
        ]
        
        return factors
    
    def get_factors_by_type(self, factor_type: FactorType) -> List[RiskFactor]:
        """Get all factors of a specific type."""
        return [f for f in self.factors if f.factor_type == factor_type]
    
    def get_factor_by_symbol(self, symbol: str) -> Optional[RiskFactor]:
        """Get factor by symbol."""
        for factor in self.factors:
            if factor.symbol == symbol:
                return factor
        return None


class FactorExposureCalculator:
    """Calculates portfolio exposure to risk factors."""
    
    def __init__(self, factor_universe: FactorUniverse):
        self.factor_universe = factor_universe
        self.lookback_periods = {
            'short': 21,    # 1 month
            'medium': 63,   # 3 months  
            'long': 252     # 1 year
        }
    
    def calculate_factor_betas(self, returns: pd.Series, 
                              factor_returns: pd.DataFrame,
                              period: str = 'medium') -> Dict[str, float]:
        """
        Calculate factor betas using regression analysis.
        
        Args:
            returns: Asset return series
            factor_returns: DataFrame with factor return series
            period: Lookback period ('short', 'medium', 'long')
            
        Returns:
            Dictionary of factor betas
        """
        if period not in self.lookback_periods:
            raise ValueError(f"Period must be one of {list(self.lookback_periods.keys())}")
        
        lookback = self.lookback_periods[period]
        
        # Align data and get recent period
        aligned_data = pd.concat([returns, factor_returns], axis=1).dropna()
        recent_data = aligned_data.tail(lookback)
        
        if len(recent_data) < max(21, lookback // 4):
            raise ValueError(f"Insufficient data for {period} period calculation")
        
        asset_returns = recent_data.iloc[:, 0]
        factor_data = recent_data.iloc[:, 1:]
        
        betas = {}
        
        for factor_symbol in factor_data.columns:
            factor_rets = factor_data[factor_symbol]
            
            # Calculate beta using covariance method
            covariance = np.cov(asset_returns, factor_rets)[0, 1]
            factor_variance = np.var(factor_rets)
            
            if factor_variance > 0:
                beta = covariance / factor_variance
            else:
                beta = 0.0
            
            betas[factor_symbol] = beta
        
        return betas
    
    def calculate_portfolio_exposures(self, portfolio_weights: Dict[str, float],
                                    asset_betas: Dict[str, Dict[str, float]]) -> Dict[str, float]:
        """
        Calculate portfolio-level factor exposures.
        
        Args:
            portfolio_weights: Dictionary of asset weights
            asset_betas: Dictionary of asset -> factor betas
            
        Returns:
            Portfolio factor exposures
        """
        portfolio_exposures = {}
        
        # Initialize exposures
        for factor_symbol in self.factor_universe.factor_symbols:
            portfolio_exposures[factor_symbol] = 0.0
        
        # Calculate weighted average exposures
        for asset, weight in portfolio_weights.items():
            if asset in asset_betas:
                for factor_symbol, beta in asset_betas[asset].items():
                    if factor_symbol in portfolio_exposures:
                        portfolio_exposures[factor_symbol] += weight * beta
        
        return portfolio_exposures
    
    def calculate_factor_risk_contribution(self, portfolio_exposures: Dict[str, float],
                                         factor_covariance: pd.DataFrame) -> Dict[str, float]:
        """Calculate each factor's contribution to portfolio risk."""
        factor_symbols = list(portfolio_exposures.keys())
        exposures = np.array([portfolio_exposures[f] for f in factor_symbols])
        
        # Ensure covariance matrix alignment
        cov_matrix = factor_covariance.reindex(index=factor_symbols, columns=factor_symbols).fillna(0)
        
        # Portfolio variance = w' * Cov * w
        portfolio_variance = exposures.T @ cov_matrix.values @ exposures
        
        # Marginal risk contribution = Cov * w
        marginal_contrib = cov_matrix.values @ exposures
        
        # Risk contribution = exposure * marginal contribution
        risk_contributions = {}
        for i, factor in enumerate(factor_symbols):
            contrib = exposures[i] * marginal_contrib[i]
            risk_contributions[factor] = contrib / portfolio_variance if portfolio_variance > 0 else 0
        
        return risk_contributions


class FactorNeutralityConstraints:
    """Defines constraints for maintaining factor neutrality."""
    
    def __init__(self, factor_universe: FactorUniverse):
        self.factor_universe = factor_universe
        self.exposure_limits = self._initialize_exposure_limits()
    
    def _initialize_exposure_limits(self) -> Dict[str, Tuple[float, float]]:
        """Initialize factor exposure limits (min, max)."""
        limits = {}
        
        for factor in self.factor_universe.factors:
            if factor.factor_type == FactorType.MARKET:
                # Market factors: very tight constraints
                limits[factor.symbol] = (-0.05, 0.05)  # ±5% beta
            elif factor.factor_type == FactorType.INTEREST_RATE:
                # Interest rate: moderate constraints
                limits[factor.symbol] = (-0.10, 0.10)  # ±10% beta
            elif factor.factor_type == FactorType.SECTOR:
                # Sector: looser constraints
                limits[factor.symbol] = (-0.15, 0.15)  # ±15% beta
            elif factor.factor_type == FactorType.VOLATILITY:
                # Volatility: very tight constraints
                limits[factor.symbol] = (-0.03, 0.03)  # ±3% beta
            else:
                # Other factors: moderate constraints
                limits[factor.symbol] = (-0.08, 0.08)  # ±8% beta
        
        return limits
    
    def check_neutrality(self, portfolio_exposures: Dict[str, float]) -> Dict[str, bool]:
        """Check if portfolio exposures are within neutrality limits."""
        neutrality_status = {}
        
        for factor_symbol, exposure in portfolio_exposures.items():
            if factor_symbol in self.exposure_limits:
                min_limit, max_limit = self.exposure_limits[factor_symbol]
                neutrality_status[factor_symbol] = min_limit <= exposure <= max_limit
            else:
                neutrality_status[factor_symbol] = True
        
        return neutrality_status
    
    def get_violation_severity(self, portfolio_exposures: Dict[str, float]) -> Dict[str, float]:
        """Calculate severity of constraint violations."""
        violations = {}
        
        for factor_symbol, exposure in portfolio_exposures.items():
            if factor_symbol in self.exposure_limits:
                min_limit, max_limit = self.exposure_limits[factor_symbol]
                
                if exposure < min_limit:
                    violations[factor_symbol] = (min_limit - exposure) / abs(min_limit)
                elif exposure > max_limit:
                    violations[factor_symbol] = (exposure - max_limit) / abs(max_limit)
                else:
                    violations[factor_symbol] = 0.0
            else:
                violations[factor_symbol] = 0.0
        
        return violations


class FactorRiskModel:
    """Complete factor risk model for portfolio construction."""
    
    def __init__(self):
        self.factor_universe = FactorUniverse()
        self.exposure_calculator = FactorExposureCalculator(self.factor_universe)
        self.neutrality_constraints = FactorNeutralityConstraints(self.factor_universe)
        
        # Risk model parameters
        self.decay_factor = 0.94  # Exponential decay for covariance estimation
        self.min_observations = 63  # Minimum observations for stable estimates
    
    def estimate_factor_covariance(self, factor_returns: pd.DataFrame,
                                  method: str = 'ewm') -> pd.DataFrame:
        """
        Estimate factor covariance matrix.
        
        Args:
            factor_returns: DataFrame of factor returns
            method: 'ewm' (exponentially weighted) or 'sample'
            
        Returns:
            Factor covariance matrix
        """
        if len(factor_returns) < self.min_observations:
            raise ValueError(f"Need at least {self.min_observations} observations")
        
        if method == 'ewm':
            # Exponentially weighted covariance
            cov_matrix = factor_returns.ewm(alpha=1-self.decay_factor).cov().iloc[-len(factor_returns.columns):]
        else:
            # Sample covariance
            cov_matrix = factor_returns.cov()
        
        return cov_matrix
    
    def calculate_portfolio_risk(self, portfolio_weights: Dict[str, float],
                               asset_betas: Dict[str, Dict[str, float]],
                               factor_covariance: pd.DataFrame,
                               idiosyncratic_risk: Optional[Dict[str, float]] = None) -> float:
        """
        Calculate total portfolio risk using factor model.
        
        Args:
            portfolio_weights: Asset weights
            asset_betas: Asset factor betas
            factor_covariance: Factor covariance matrix
            idiosyncratic_risk: Asset-specific risks
            
        Returns:
            Portfolio volatility (annualized)
        """
        # Calculate factor exposures
        portfolio_exposures = self.exposure_calculator.calculate_portfolio_exposures(
            portfolio_weights, asset_betas
        )
        
        # Factor risk component
        factor_symbols = list(portfolio_exposures.keys())
        exposures = np.array([portfolio_exposures[f] for f in factor_symbols])
        
        # Align covariance matrix
        cov_matrix = factor_covariance.reindex(index=factor_symbols, columns=factor_symbols).fillna(0)
        
        factor_risk = exposures.T @ cov_matrix.values @ exposures
        
        # Idiosyncratic risk component
        idio_risk = 0.0
        if idiosyncratic_risk:
            for asset, weight in portfolio_weights.items():
                if asset in idiosyncratic_risk:
                    idio_risk += (weight ** 2) * (idiosyncratic_risk[asset] ** 2)
        
        # Total portfolio variance
        total_risk = factor_risk + idio_risk
        
        # Convert to annualized volatility
        return np.sqrt(total_risk * 252)
    
    def generate_risk_report(self, portfolio_weights: Dict[str, float],
                           asset_betas: Dict[str, Dict[str, float]],
                           factor_covariance: pd.DataFrame) -> Dict[str, any]:
        """Generate comprehensive risk report."""
        # Calculate exposures
        portfolio_exposures = self.exposure_calculator.calculate_portfolio_exposures(
            portfolio_weights, asset_betas
        )
        
        # Check neutrality
        neutrality_status = self.neutrality_constraints.check_neutrality(portfolio_exposures)
        violations = self.neutrality_constraints.get_violation_severity(portfolio_exposures)
        
        # Calculate risk contributions
        risk_contributions = self.exposure_calculator.calculate_factor_risk_contribution(
            portfolio_exposures, factor_covariance
        )
        
        # Calculate total risk
        total_risk = self.calculate_portfolio_risk(
            portfolio_weights, asset_betas, factor_covariance
        )
        
        return {
            'portfolio_exposures': portfolio_exposures,
            'neutrality_status': neutrality_status,
            'violation_severity': violations,
            'risk_contributions': risk_contributions,
            'total_risk': total_risk,
            'is_market_neutral': all(neutrality_status.values()),
            'max_violation': max(violations.values()) if violations else 0.0,
            'factor_universe': [f.symbol for f in self.factor_universe.factors]
        }