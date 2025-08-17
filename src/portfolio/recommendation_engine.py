"""
Hourly Portfolio Recommendation Engine

Main system that generates hourly portfolio recommendations with risk-adjusted returns.
Integrates signal generation, factor hedging, portfolio optimization, and performance measurement.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import logging
from pathlib import Path
import json

from .factor_framework import FactorRiskModel, FactorUniverse
from .signal_generation import (
    PortfolioSignalManager, TradingSignal, SignalDirection, 
    CompositeSignalGenerator
)
from .optimization import (
    PortfolioConstructor, OptimizationConstraints, OptimizationResult
)
from .performance_metrics import PerformanceAnalyzer, PerformanceMetrics


@dataclass
class RecommendationOutput:
    """Complete recommendation output for hourly delivery."""
    timestamp: datetime
    portfolio_weights: Dict[str, float]
    position_sizes: Dict[str, Dict[str, float]]
    expected_return: float
    expected_volatility: float
    sharpe_ratio: float
    factor_exposures: Dict[str, float]
    signals_summary: Dict[str, Any]
    performance_metrics: Optional[PerformanceMetrics]
    risk_warnings: List[str]
    execution_notes: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str, indent=2)


class TradingUniverse:
    """Defines the trading universe for portfolio construction."""
    
    def __init__(self, symbols: Optional[List[str]] = None):
        if symbols is None:
            # Default universe with good liquidity and factor coverage
            self.symbols = [
                # Large Cap Stocks
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM',
                'JNJ', 'V', 'PG', 'UNH', 'HD', 'MA', 'DIS', 'PYPL', 'ADBE',
                'NFLX', 'CRM', 'CMCSA', 'PEP', 'INTC', 'VZ', 'T', 'ABT',
                
                # Mid Cap Stocks
                'ZM', 'ROKU', 'SQ', 'SHOP', 'TWLO', 'OKTA', 'NET', 'DDOG',
                'SNOW', 'CRWD', 'ZS', 'TEAM', 'WDAY', 'NOW', 'SPLK',
                
                # Factor ETFs (for hedging)
                'SPY', 'QQQ', 'IWM', 'EFA', 'TLT', 'SHY', 'GLD', 'USO',
                'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLC', 'VIX',
                
                # Additional hedging instruments
                'DXY'  # Dollar index proxy
            ]
        else:
            self.symbols = symbols
        
        # Categorize symbols
        self.stocks = [s for s in self.symbols if not self._is_etf(s)]
        self.etfs = [s for s in self.symbols if self._is_etf(s)]
        self.factor_instruments = [
            'SPY', 'QQQ', 'IWM', 'EFA', 'TLT', 'SHY', 'GLD', 'USO',
            'XLK', 'XLF', 'XLE', 'XLV', 'XLI', 'XLC', 'VIX', 'DXY'
        ]
    
    def _is_etf(self, symbol: str) -> bool:
        """Check if symbol is an ETF."""
        etf_prefixes = ['SPY', 'QQQ', 'IWM', 'EFA', 'TLT', 'SHY', 'GLD', 'USO']
        etf_prefixes += ['XL', 'VIX', 'DXY']
        return any(symbol.startswith(prefix) for prefix in etf_prefixes)


class DataManager:
    """Manages market data fetching and preparation."""
    
    def __init__(self, universe: TradingUniverse):
        self.universe = universe
        self.data_cache = {}
        self.last_update = None
        
    def fetch_market_data(self, lookback_hours: int = 168) -> Dict[str, pd.DataFrame]:
        """
        Fetch market data for all universe symbols.
        
        Args:
            lookback_hours: Hours of historical data to fetch
            
        Returns:
            Dictionary of symbol -> DataFrame with OHLCV data
        """
        # In a real implementation, this would connect to data providers
        # For now, create realistic simulated data
        market_data = {}
        
        for symbol in self.universe.symbols:
            data = self._generate_realistic_data(symbol, lookback_hours)
            market_data[symbol] = data
        
        self.data_cache = market_data
        self.last_update = datetime.now()
        
        return market_data
    
    def _generate_realistic_data(self, symbol: str, hours: int) -> pd.DataFrame:
        """Generate realistic market data for backtesting."""
        np.random.seed(hash(symbol) % 2**32)  # Deterministic per symbol
        
        # Create hourly timestamps
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        timestamps = pd.date_range(start_time, end_time, freq='1H')
        
        # Base parameters by symbol type
        if symbol in ['SPY', 'QQQ', 'IWM']:
            base_price = 400 + hash(symbol) % 200
            volatility = 0.15
            drift = 0.08
        elif symbol.startswith('XL'):  # Sector ETFs
            base_price = 50 + hash(symbol) % 100
            volatility = 0.20
            drift = 0.06
        elif symbol in ['TLT', 'SHY']:
            base_price = 80 + hash(symbol) % 40
            volatility = 0.12
            drift = 0.03
        elif symbol in ['GLD', 'USO']:
            base_price = 80 + hash(symbol) % 80
            volatility = 0.25
            drift = 0.04
        elif symbol == 'VIX':
            base_price = 20
            volatility = 0.80
            drift = -0.10  # VIX mean-reverts
        else:  # Individual stocks
            base_price = 50 + hash(symbol) % 300
            volatility = 0.25 + (hash(symbol) % 100) / 1000  # 0.25-0.35
            drift = 0.10
        
        # Generate price series using GBM
        dt = 1 / (252 * 24)  # Hourly time step
        n_steps = len(timestamps)
        
        # Price evolution
        shocks = np.random.normal(0, 1, n_steps)
        price_changes = (drift - 0.5 * volatility**2) * dt + volatility * np.sqrt(dt) * shocks
        
        prices = [base_price]
        for change in price_changes:
            prices.append(prices[-1] * np.exp(change))
        
        prices = np.array(prices[1:])  # Remove initial price
        
        # Generate OHLCV
        data = []
        for i, (timestamp, price) in enumerate(zip(timestamps, prices)):
            # Intraday volatility
            intraday_vol = volatility * 0.3
            high = price * (1 + abs(np.random.normal(0, intraday_vol)))
            low = price * (1 - abs(np.random.normal(0, intraday_vol)))
            
            # Ensure price is between high and low
            high = max(high, price)
            low = min(low, price)
            
            # Open price (close of previous + gap)
            if i == 0:
                open_price = price
            else:
                gap = np.random.normal(0, volatility * 0.1)
                open_price = prices[i-1] * (1 + gap)
                open_price = max(min(open_price, high), low)
            
            # Volume (higher during market hours)
            hour = timestamp.hour
            if 9 <= hour <= 16:  # Market hours
                base_volume = 1000000
                volume_multiplier = 1.5
            else:
                base_volume = 300000
                volume_multiplier = 0.8
            
            volume = int(base_volume * volume_multiplier * (1 + np.random.normal(0, 0.3)))
            volume = max(volume, 10000)
            
            data.append({
                'timestamp': timestamp,
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(price, 2),
                'volume': volume
            })
        
        df = pd.DataFrame(data)
        df.index = df['timestamp']
        return df[['open', 'high', 'low', 'close', 'volume']]


class HourlyRecommendationEngine:
    """Main engine for generating hourly portfolio recommendations."""
    
    def __init__(self, portfolio_value: float = 200000,
                 universe: Optional[TradingUniverse] = None,
                 constraints: Optional[OptimizationConstraints] = None):
        
        self.portfolio_value = portfolio_value
        self.universe = universe or TradingUniverse()
        self.constraints = constraints or OptimizationConstraints()
        
        # Initialize components
        self.factor_risk_model = FactorRiskModel()
        self.signal_manager = PortfolioSignalManager(self.universe.symbols)
        self.portfolio_constructor = PortfolioConstructor(
            portfolio_value, self.constraints
        )
        self.performance_analyzer = PerformanceAnalyzer(self.factor_risk_model)
        self.data_manager = DataManager(self.universe)
        
        # State tracking
        self.current_portfolio = {}
        self.portfolio_history = []
        self.performance_history = []
        
        # Logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def generate_hourly_recommendation(self, 
                                     current_portfolio: Optional[Dict[str, float]] = None) -> RecommendationOutput:
        """
        Generate comprehensive hourly portfolio recommendation.
        
        Args:
            current_portfolio: Current portfolio weights (if any)
            
        Returns:
            Complete recommendation output
        """
        self.logger.info("Starting hourly recommendation generation")
        timestamp = datetime.now()
        
        try:
            # Step 1: Fetch market data
            self.logger.info("Fetching market data")
            market_data = self.data_manager.fetch_market_data(lookback_hours=168)
            
            # Step 2: Generate signals
            self.logger.info("Generating trading signals")
            signals = self.signal_manager.generate_portfolio_signals(market_data)
            signals_summary = self.signal_manager.get_signal_summary(signals)
            
            # Step 3: Optimize portfolio
            self.logger.info("Optimizing portfolio")
            if current_portfolio is None:
                current_portfolio = self.current_portfolio
                
            optimization_result = self.portfolio_constructor.construct_portfolio(
                signals, current_portfolio, market_data
            )
            
            # Step 4: Calculate position sizes
            position_sizes = {}
            if optimization_result.is_successful:
                position_sizes = self.portfolio_constructor.calculate_position_sizes(
                    optimization_result.weights
                )
            
            # Step 5: Performance analysis (if we have history)
            performance_metrics = None
            if len(self.portfolio_history) > 30:
                performance_metrics = self._calculate_historical_performance()
            
            # Step 6: Risk analysis and warnings
            risk_warnings = self._generate_risk_warnings(optimization_result, signals)
            execution_notes = self._generate_execution_notes(optimization_result, current_portfolio)
            
            # Step 7: Update state
            if optimization_result.is_successful:
                self.current_portfolio = optimization_result.weights.copy()
                self.portfolio_history.append({
                    'timestamp': timestamp,
                    'weights': optimization_result.weights.copy(),
                    'expected_return': optimization_result.expected_return,
                    'expected_volatility': optimization_result.expected_volatility
                })
            
            # Create recommendation output
            recommendation = RecommendationOutput(
                timestamp=timestamp,
                portfolio_weights=optimization_result.weights,
                position_sizes=position_sizes,
                expected_return=optimization_result.expected_return,
                expected_volatility=optimization_result.expected_volatility,
                sharpe_ratio=optimization_result.sharpe_ratio,
                factor_exposures=optimization_result.factor_exposures,
                signals_summary=signals_summary,
                performance_metrics=performance_metrics,
                risk_warnings=risk_warnings,
                execution_notes=execution_notes
            )
            
            self.logger.info("Recommendation generation completed successfully")
            return recommendation
            
        except Exception as e:
            self.logger.error(f"Error generating recommendation: {str(e)}")
            return self._create_error_recommendation(timestamp, str(e))
    
    def _calculate_historical_performance(self) -> Optional[PerformanceMetrics]:
        """Calculate performance metrics from portfolio history."""
        if len(self.portfolio_history) < 30:
            return None
        
        try:
            # Extract returns from portfolio history
            returns = []
            for i in range(1, len(self.portfolio_history)):
                prev_weights = self.portfolio_history[i-1]['weights']
                curr_weights = self.portfolio_history[i]['weights']
                
                # Simplified return calculation (would use actual price changes in real implementation)
                expected_return = self.portfolio_history[i]['expected_return']
                returns.append(expected_return / (252 * 24))  # Convert to hourly return
            
            returns_series = pd.Series(returns)
            returns_series.index = pd.date_range(
                end=datetime.now(), 
                periods=len(returns), 
                freq='1H'
            )
            
            # Calculate comprehensive metrics
            return self.performance_analyzer.calculate_comprehensive_metrics(returns_series)
            
        except Exception as e:
            self.logger.error(f"Error calculating historical performance: {str(e)}")
            return None
    
    def _generate_risk_warnings(self, optimization_result: OptimizationResult,
                               signals: Dict[str, TradingSignal]) -> List[str]:
        """Generate risk warnings based on portfolio and signals."""
        warnings = []
        
        if not optimization_result.is_successful:
            warnings.append("❌ Portfolio optimization failed - using current positions")
        
        # Check factor exposures
        for factor, exposure in optimization_result.factor_exposures.items():
            if abs(exposure) > 0.15:
                warnings.append(f"⚠️ High {factor} exposure: {exposure:.1%}")
        
        # Check leverage
        if optimization_result.leverage_ratio > 1.8:
            warnings.append(f"⚠️ High leverage: {optimization_result.leverage_ratio:.1f}x")
        
        # Check concentration
        max_position = max(abs(w) for w in optimization_result.weights.values()) if optimization_result.weights else 0
        if max_position > 0.08:
            warnings.append(f"⚠️ High concentration: {max_position:.1%} in single position")
        
        # Check signal quality
        if signals:
            avg_confidence = np.mean([s.confidence for s in signals.values()])
            if avg_confidence < 0.4:
                warnings.append(f"⚠️ Low signal quality: {avg_confidence:.1%} average confidence")
        
        return warnings
    
    def _generate_execution_notes(self, optimization_result: OptimizationResult,
                                 current_portfolio: Dict[str, float]) -> List[str]:
        """Generate execution notes for portfolio implementation."""
        notes = []
        
        if not optimization_result.weights:
            notes.append("🔄 No position changes recommended")
            return notes
        
        # Calculate turnover
        if current_portfolio:
            turnover = 0
            for symbol in set(optimization_result.weights.keys()) | set(current_portfolio.keys()):
                old_weight = current_portfolio.get(symbol, 0)
                new_weight = optimization_result.weights.get(symbol, 0)
                turnover += abs(new_weight - old_weight)
            
            turnover /= 2  # One-way turnover
            
            if turnover > 0.5:
                notes.append(f"📈 High turnover: {turnover:.1%} - consider gradual implementation")
            elif turnover > 0.2:
                notes.append(f"🔄 Moderate turnover: {turnover:.1%}")
            else:
                notes.append(f"✅ Low turnover: {turnover:.1%}")
        
        # Execution timing
        current_hour = datetime.now().hour
        if 9 <= current_hour <= 16:
            notes.append("🕒 Market hours - good liquidity for execution")
        else:
            notes.append("🌙 After hours - consider waiting for market open")
        
        # Transaction cost estimate
        estimated_cost = optimization_result.transaction_costs
        if estimated_cost > 0:
            notes.append(f"💰 Estimated transaction costs: ${estimated_cost:.0f}")
        
        return notes
    
    def _create_error_recommendation(self, timestamp: datetime, error_msg: str) -> RecommendationOutput:
        """Create error recommendation when generation fails."""
        return RecommendationOutput(
            timestamp=timestamp,
            portfolio_weights=self.current_portfolio.copy(),
            position_sizes={},
            expected_return=0.0,
            expected_volatility=0.0,
            sharpe_ratio=0.0,
            factor_exposures={},
            signals_summary={},
            performance_metrics=None,
            risk_warnings=[f"❌ Error: {error_msg}"],
            execution_notes=["🚫 No changes recommended due to error"]
        )
    
    def run_continuous_recommendations(self, hours: int = 24, 
                                     output_path: Optional[str] = None) -> List[RecommendationOutput]:
        """
        Run continuous hourly recommendations for specified duration.
        
        Args:
            hours: Number of hours to run
            output_path: Optional path to save recommendations
            
        Returns:
            List of all recommendations generated
        """
        recommendations = []
        
        for hour in range(hours):
            self.logger.info(f"Generating recommendation {hour + 1}/{hours}")
            
            recommendation = self.generate_hourly_recommendation()
            recommendations.append(recommendation)
            
            # Save to file if specified
            if output_path:
                output_file = Path(output_path) / f"recommendation_{recommendation.timestamp.strftime('%Y%m%d_%H%M%S')}.json"
                output_file.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_file, 'w') as f:
                    f.write(recommendation.to_json())
            
            # Log key metrics
            self.logger.info(f"Expected Return: {recommendation.expected_return:.2%}")
            self.logger.info(f"Expected Volatility: {recommendation.expected_volatility:.2%}")
            self.logger.info(f"Sharpe Ratio: {recommendation.sharpe_ratio:.2f}")
            self.logger.info(f"Gross Exposure: {sum(abs(w) for w in recommendation.portfolio_weights.values()):.1%}")
            
            # Simulate waiting for next hour (in real implementation)
            if hour < hours - 1:
                self.logger.info("Waiting for next hour...")
        
        return recommendations
    
    def generate_performance_report(self) -> str:
        """Generate comprehensive performance report."""
        if len(self.portfolio_history) < 30:
            return "Insufficient history for performance report (need at least 30 observations)"
        
        performance_metrics = self._calculate_historical_performance()
        if performance_metrics is None:
            return "Error calculating performance metrics"
        
        return self.performance_analyzer.generate_performance_report(performance_metrics)