"""
Advanced Performance Metrics for Market-Neutral Portfolio Strategies

Implements comprehensive performance measurement including Sharpe, Information Ratio,
Calmar Ratio, Sortino Ratio, Maximum Drawdown, and factor attribution analysis.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import warnings
from scipy import stats

from .factor_framework import FactorRiskModel


@dataclass
class PerformanceMetrics:
    """Container for comprehensive performance metrics."""
    
    # Basic Return Metrics
    total_return: float
    annualized_return: float
    annualized_volatility: float
    
    # Risk-Adjusted Metrics
    sharpe_ratio: float
    information_ratio: float
    calmar_ratio: float
    sortino_ratio: float
    
    # Drawdown Metrics
    max_drawdown: float
    max_drawdown_duration: int  # Days
    current_drawdown: float
    
    # Factor Analysis
    market_beta: float
    market_alpha: float
    factor_exposures: Dict[str, float]
    factor_attribution: Dict[str, float]
    
    # Additional Metrics
    win_rate: float
    profit_factor: float
    value_at_risk_95: float
    expected_shortfall_95: float
    skewness: float
    kurtosis: float
    
    # Period-specific
    best_month: float
    worst_month: float
    positive_months: int
    total_months: int
    
    # Market Neutrality
    correlation_to_spy: float
    correlation_to_bonds: float
    factor_neutrality_score: float
    
    # Performance Attribution
    gross_pnl: float
    net_pnl: float
    transaction_costs: float
    
    @property
    def risk_return_efficiency(self) -> float:
        """Overall risk-return efficiency score."""
        return (self.sharpe_ratio * 0.4 + 
                self.information_ratio * 0.3 + 
                self.calmar_ratio * 0.3)


class PerformanceAnalyzer:
    """Comprehensive performance analysis for portfolio strategies."""
    
    def __init__(self, factor_risk_model: Optional[FactorRiskModel] = None,
                 benchmark_symbol: str = 'SPY',
                 risk_free_rate: float = 0.02):
        self.factor_risk_model = factor_risk_model or FactorRiskModel()
        self.benchmark_symbol = benchmark_symbol
        self.risk_free_rate = risk_free_rate  # Annual risk-free rate
        
    def calculate_comprehensive_metrics(self, 
                                      returns: pd.Series,
                                      factor_returns: Optional[pd.DataFrame] = None,
                                      benchmark_returns: Optional[pd.Series] = None,
                                      portfolio_values: Optional[pd.Series] = None) -> PerformanceMetrics:
        """
        Calculate comprehensive performance metrics.
        
        Args:
            returns: Portfolio return series
            factor_returns: Factor return series for attribution
            benchmark_returns: Benchmark return series
            portfolio_values: Portfolio value series for drawdown analysis
            
        Returns:
            Comprehensive performance metrics
        """
        if len(returns) < 30:
            raise ValueError("Need at least 30 observations for meaningful analysis")
        
        returns = returns.dropna()
        
        # Basic return metrics
        total_return = (1 + returns).prod() - 1
        periods_per_year = self._infer_frequency(returns)
        annualized_return = (1 + total_return) ** (periods_per_year / len(returns)) - 1
        annualized_volatility = returns.std() * np.sqrt(periods_per_year)
        
        # Risk-adjusted metrics
        excess_returns = returns - self.risk_free_rate / periods_per_year
        sharpe_ratio = self._calculate_sharpe_ratio(returns, periods_per_year)
        
        # Information ratio (vs benchmark)
        information_ratio = self._calculate_information_ratio(
            returns, benchmark_returns, periods_per_year
        )
        
        # Calmar ratio
        max_drawdown = self._calculate_max_drawdown(returns, portfolio_values)
        calmar_ratio = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0
        
        # Sortino ratio
        sortino_ratio = self._calculate_sortino_ratio(returns, periods_per_year)
        
        # Drawdown analysis
        drawdown_metrics = self._analyze_drawdowns(returns, portfolio_values)
        
        # Factor analysis
        factor_analysis = self._analyze_factor_exposure(returns, factor_returns)
        
        # Additional risk metrics
        var_95 = self._calculate_var(returns, 0.95)
        es_95 = self._calculate_expected_shortfall(returns, 0.95)
        
        # Distribution metrics
        skewness = stats.skew(returns)
        kurtosis = stats.kurtosis(returns)
        
        # Monthly analysis
        monthly_metrics = self._analyze_monthly_performance(returns)
        
        # Market neutrality analysis
        neutrality_metrics = self._analyze_market_neutrality(
            returns, factor_returns, benchmark_returns
        )
        
        # Win rate and profit factor
        winning_periods = (returns > 0).sum()
        win_rate = winning_periods / len(returns)
        
        positive_returns = returns[returns > 0].sum()
        negative_returns = abs(returns[returns < 0].sum())
        profit_factor = positive_returns / negative_returns if negative_returns > 0 else np.inf
        
        return PerformanceMetrics(
            # Basic metrics
            total_return=total_return,
            annualized_return=annualized_return,
            annualized_volatility=annualized_volatility,
            
            # Risk-adjusted metrics
            sharpe_ratio=sharpe_ratio,
            information_ratio=information_ratio,
            calmar_ratio=calmar_ratio,
            sortino_ratio=sortino_ratio,
            
            # Drawdown metrics
            max_drawdown=max_drawdown,
            max_drawdown_duration=drawdown_metrics['max_duration'],
            current_drawdown=drawdown_metrics['current_drawdown'],
            
            # Factor analysis
            market_beta=factor_analysis['market_beta'],
            market_alpha=factor_analysis['market_alpha'],
            factor_exposures=factor_analysis['factor_exposures'],
            factor_attribution=factor_analysis['factor_attribution'],
            
            # Additional metrics
            win_rate=win_rate,
            profit_factor=profit_factor,
            value_at_risk_95=var_95,
            expected_shortfall_95=es_95,
            skewness=skewness,
            kurtosis=kurtosis,
            
            # Monthly metrics
            best_month=monthly_metrics['best_month'],
            worst_month=monthly_metrics['worst_month'],
            positive_months=monthly_metrics['positive_months'],
            total_months=monthly_metrics['total_months'],
            
            # Market neutrality
            correlation_to_spy=neutrality_metrics['spy_correlation'],
            correlation_to_bonds=neutrality_metrics['bond_correlation'],
            factor_neutrality_score=neutrality_metrics['neutrality_score'],
            
            # Performance attribution (placeholder - would need transaction data)
            gross_pnl=total_return,
            net_pnl=total_return,
            transaction_costs=0.0
        )
    
    def _infer_frequency(self, returns: pd.Series) -> int:
        """Infer the frequency of returns (periods per year)."""
        if hasattr(returns.index, 'freq'):
            if returns.index.freq == 'D':
                return 252  # Daily
            elif returns.index.freq == 'H':
                return 252 * 24  # Hourly
            elif returns.index.freq == 'M':
                return 12  # Monthly
        
        # Fallback: estimate from time differences
        if len(returns) < 2:
            return 252
        
        time_diff = returns.index[1] - returns.index[0]
        if time_diff <= pd.Timedelta(hours=2):
            return 252 * 24  # Hourly
        elif time_diff <= pd.Timedelta(days=2):
            return 252  # Daily
        elif time_diff <= pd.Timedelta(days=35):
            return 12  # Monthly
        else:
            return 4  # Quarterly
    
    def _calculate_sharpe_ratio(self, returns: pd.Series, periods_per_year: int) -> float:
        """Calculate Sharpe ratio."""
        excess_returns = returns - self.risk_free_rate / periods_per_year
        if returns.std() == 0:
            return 0
        return excess_returns.mean() / returns.std() * np.sqrt(periods_per_year)
    
    def _calculate_information_ratio(self, returns: pd.Series, 
                                   benchmark_returns: Optional[pd.Series],
                                   periods_per_year: int) -> float:
        """Calculate Information ratio (active return / tracking error)."""
        if benchmark_returns is None:
            return 0
        
        # Align returns
        aligned_returns, aligned_bench = returns.align(benchmark_returns, join='inner')
        
        if len(aligned_returns) < 10:
            return 0
        
        active_returns = aligned_returns - aligned_bench
        tracking_error = active_returns.std() * np.sqrt(periods_per_year)
        
        if tracking_error == 0:
            return 0
        
        active_return = active_returns.mean() * periods_per_year
        return active_return / tracking_error
    
    def _calculate_max_drawdown(self, returns: pd.Series, 
                               portfolio_values: Optional[pd.Series] = None) -> float:
        """Calculate maximum drawdown."""
        if portfolio_values is not None:
            cumulative_values = portfolio_values
        else:
            cumulative_values = (1 + returns).cumprod()
        
        running_max = cumulative_values.expanding().max()
        drawdowns = (cumulative_values - running_max) / running_max
        
        return drawdowns.min()
    
    def _calculate_sortino_ratio(self, returns: pd.Series, periods_per_year: int) -> float:
        """Calculate Sortino ratio (return / downside deviation)."""
        excess_returns = returns - self.risk_free_rate / periods_per_year
        downside_returns = excess_returns[excess_returns < 0]
        
        if len(downside_returns) == 0:
            return np.inf
        
        downside_deviation = downside_returns.std() * np.sqrt(periods_per_year)
        
        if downside_deviation == 0:
            return 0
        
        return excess_returns.mean() * periods_per_year / downside_deviation
    
    def _analyze_drawdowns(self, returns: pd.Series, 
                          portfolio_values: Optional[pd.Series] = None) -> Dict[str, Any]:
        """Analyze drawdown characteristics."""
        if portfolio_values is not None:
            cumulative_values = portfolio_values
        else:
            cumulative_values = (1 + returns).cumprod()
        
        running_max = cumulative_values.expanding().max()
        drawdowns = (cumulative_values - running_max) / running_max
        
        # Find drawdown periods
        in_drawdown = drawdowns < 0
        drawdown_periods = []
        
        if in_drawdown.any():
            start_idx = None
            for i, is_dd in enumerate(in_drawdown):
                if is_dd and start_idx is None:
                    start_idx = i
                elif not is_dd and start_idx is not None:
                    drawdown_periods.append(i - start_idx)
                    start_idx = None
            
            # Handle case where drawdown continues to end
            if start_idx is not None:
                drawdown_periods.append(len(in_drawdown) - start_idx)
        
        max_duration = max(drawdown_periods) if drawdown_periods else 0
        current_drawdown = drawdowns.iloc[-1]
        
        return {
            'max_duration': max_duration,
            'current_drawdown': current_drawdown,
            'avg_duration': np.mean(drawdown_periods) if drawdown_periods else 0,
            'num_drawdowns': len(drawdown_periods)
        }
    
    def _analyze_factor_exposure(self, returns: pd.Series,
                                factor_returns: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """Analyze factor exposures and attribution."""
        if factor_returns is None or len(returns) < 30:
            return {
                'market_beta': 0,
                'market_alpha': 0,
                'factor_exposures': {},
                'factor_attribution': {}
            }
        
        try:
            # Align data
            aligned_data = pd.concat([returns, factor_returns], axis=1).dropna()
            
            if len(aligned_data) < 20:
                return {
                    'market_beta': 0,
                    'market_alpha': 0,
                    'factor_exposures': {},
                    'factor_attribution': {}
                }
            
            portfolio_returns = aligned_data.iloc[:, 0]
            factor_data = aligned_data.iloc[:, 1:]
            
            # Market beta (assuming first factor is market)
            if len(factor_data.columns) > 0:
                market_factor = factor_data.iloc[:, 0]
                market_beta = np.cov(portfolio_returns, market_factor)[0, 1] / np.var(market_factor)
                market_alpha = portfolio_returns.mean() - market_beta * market_factor.mean()
            else:
                market_beta = 0
                market_alpha = portfolio_returns.mean()
            
            # Factor exposures using regression
            factor_exposures = {}
            factor_attribution = {}
            
            for factor_name in factor_data.columns:
                factor_rets = factor_data[factor_name]
                
                # Calculate beta
                covariance = np.cov(portfolio_returns, factor_rets)[0, 1]
                factor_variance = np.var(factor_rets)
                
                if factor_variance > 0:
                    beta = covariance / factor_variance
                    factor_exposures[factor_name] = beta
                    
                    # Attribution = beta * factor_return
                    factor_attribution[factor_name] = beta * factor_rets.mean()
                else:
                    factor_exposures[factor_name] = 0
                    factor_attribution[factor_name] = 0
            
            return {
                'market_beta': market_beta,
                'market_alpha': market_alpha,
                'factor_exposures': factor_exposures,
                'factor_attribution': factor_attribution
            }
            
        except Exception:
            return {
                'market_beta': 0,
                'market_alpha': 0,
                'factor_exposures': {},
                'factor_attribution': {}
            }
    
    def _calculate_var(self, returns: pd.Series, confidence: float) -> float:
        """Calculate Value at Risk."""
        return np.percentile(returns, (1 - confidence) * 100)
    
    def _calculate_expected_shortfall(self, returns: pd.Series, confidence: float) -> float:
        """Calculate Expected Shortfall (Conditional VaR)."""
        var = self._calculate_var(returns, confidence)
        return returns[returns <= var].mean()
    
    def _analyze_monthly_performance(self, returns: pd.Series) -> Dict[str, Any]:
        """Analyze monthly performance statistics."""
        try:
            # Resample to monthly if not already
            monthly_returns = returns.resample('M').apply(lambda x: (1 + x).prod() - 1)
            
            if len(monthly_returns) == 0:
                return {
                    'best_month': 0,
                    'worst_month': 0,
                    'positive_months': 0,
                    'total_months': 0
                }
            
            positive_months = (monthly_returns > 0).sum()
            
            return {
                'best_month': monthly_returns.max(),
                'worst_month': monthly_returns.min(),
                'positive_months': positive_months,
                'total_months': len(monthly_returns)
            }
            
        except Exception:
            return {
                'best_month': 0,
                'worst_month': 0,
                'positive_months': 0,
                'total_months': 0
            }
    
    def _analyze_market_neutrality(self, returns: pd.Series,
                                  factor_returns: Optional[pd.DataFrame],
                                  benchmark_returns: Optional[pd.Series]) -> Dict[str, float]:
        """Analyze market neutrality characteristics."""
        spy_correlation = 0
        bond_correlation = 0
        neutrality_score = 1.0
        
        try:
            if benchmark_returns is not None:
                aligned_returns, aligned_bench = returns.align(benchmark_returns, join='inner')
                if len(aligned_returns) > 10:
                    spy_correlation = aligned_returns.corr(aligned_bench)
            
            if factor_returns is not None:
                # Look for bond proxy (TLT, SHY, etc.)
                bond_proxies = ['TLT', 'SHY', '^TNX']
                for proxy in bond_proxies:
                    if proxy in factor_returns.columns:
                        aligned_returns, aligned_bonds = returns.align(
                            factor_returns[proxy], join='inner'
                        )
                        if len(aligned_returns) > 10:
                            bond_correlation = aligned_returns.corr(aligned_bonds)
                        break
            
            # Neutrality score: lower correlations = higher neutrality
            neutrality_score = 1 - (abs(spy_correlation) * 0.6 + abs(bond_correlation) * 0.4)
            neutrality_score = max(0, min(1, neutrality_score))
            
        except Exception:
            pass
        
        return {
            'spy_correlation': spy_correlation,
            'bond_correlation': bond_correlation,
            'neutrality_score': neutrality_score
        }
    
    def generate_performance_report(self, metrics: PerformanceMetrics) -> str:
        """Generate formatted performance report."""
        report = f"""
PORTFOLIO PERFORMANCE REPORT
{'='*50}

RETURNS & RISK METRICS:
  Total Return:           {metrics.total_return:>8.2%}
  Annualized Return:      {metrics.annualized_return:>8.2%}
  Annualized Volatility:  {metrics.annualized_volatility:>8.2%}

RISK-ADJUSTED PERFORMANCE:
  Sharpe Ratio:           {metrics.sharpe_ratio:>8.2f}
  Information Ratio:      {metrics.information_ratio:>8.2f}
  Calmar Ratio:           {metrics.calmar_ratio:>8.2f}
  Sortino Ratio:          {metrics.sortino_ratio:>8.2f}

DRAWDOWN ANALYSIS:
  Maximum Drawdown:       {metrics.max_drawdown:>8.2%}
  Current Drawdown:       {metrics.current_drawdown:>8.2%}
  Max DD Duration:        {metrics.max_drawdown_duration:>8d} periods

MARKET EXPOSURE:
  Market Beta:            {metrics.market_beta:>8.3f}
  Market Alpha:           {metrics.market_alpha:>8.4f}
  SPY Correlation:        {metrics.correlation_to_spy:>8.3f}
  Factor Neutrality:      {metrics.factor_neutrality_score:>8.2%}

ADDITIONAL METRICS:
  Win Rate:               {metrics.win_rate:>8.2%}
  Profit Factor:          {metrics.profit_factor:>8.2f}
  VaR (95%):              {metrics.value_at_risk_95:>8.2%}
  Expected Shortfall:     {metrics.expected_shortfall_95:>8.2%}
  
DISTRIBUTION:
  Skewness:               {metrics.skewness:>8.3f}
  Kurtosis:               {metrics.kurtosis:>8.3f}

MONTHLY PERFORMANCE:
  Best Month:             {metrics.best_month:>8.2%}
  Worst Month:            {metrics.worst_month:>8.2%}
  Positive Months:        {metrics.positive_months}/{metrics.total_months}

EFFICIENCY SCORE:         {metrics.risk_return_efficiency:>8.2f}
"""
        
        if metrics.factor_exposures:
            report += "\nFACTOR EXPOSURES:\n"
            for factor, exposure in metrics.factor_exposures.items():
                report += f"  {factor:<15}     {exposure:>8.3f}\n"
        
        return report
    
    def calculate_rolling_metrics(self, returns: pd.Series, 
                                 window: int = 60) -> pd.DataFrame:
        """Calculate rolling performance metrics."""
        rolling_metrics = pd.DataFrame(index=returns.index)
        
        # Rolling Sharpe ratio
        periods_per_year = self._infer_frequency(returns)
        excess_returns = returns - self.risk_free_rate / periods_per_year
        
        rolling_metrics['sharpe_ratio'] = (
            excess_returns.rolling(window).mean() / 
            returns.rolling(window).std() * np.sqrt(periods_per_year)
        )
        
        # Rolling volatility
        rolling_metrics['volatility'] = (
            returns.rolling(window).std() * np.sqrt(periods_per_year)
        )
        
        # Rolling max drawdown
        cumulative_returns = (1 + returns).cumprod()
        rolling_max = cumulative_returns.rolling(window, min_periods=1).max()
        rolling_dd = (cumulative_returns - rolling_max) / rolling_max
        rolling_metrics['max_drawdown'] = rolling_dd.rolling(window).min()
        
        return rolling_metrics