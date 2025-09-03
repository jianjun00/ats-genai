"""
Portfolio Analytics Engine

High-performance analytics engine for computing portfolio metrics,
attribution analysis, and performance visualization data.
"""

import logging
import numpy as np
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import asyncpg
import redis.asyncio as redis

from shared.utils.environment import Environment

@dataclass
class PortfolioMetrics:
    """Comprehensive portfolio performance metrics"""
    # Core performance metrics
    total_return: float
    annualized_return: float
    volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    
    # Risk metrics
    max_drawdown: float
    max_drawdown_duration_days: int
    var_95: float
    var_99: float
    expected_shortfall_95: float
    expected_shortfall_99: float
    
    # Trading metrics
    total_trades: int
    win_rate: float
    profit_factor: float
    avg_win: float
    avg_loss: float
    largest_win: float
    largest_loss: float
    
    # Advanced metrics
    information_ratio: float
    treynor_ratio: float
    jensen_alpha: float
    beta: float
    correlation_to_benchmark: float
    
    # Time series for visualization
    start_date: date
    end_date: date
    total_days: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

@dataclass
class AttributionMetrics:
    """Performance attribution breakdown"""
    # Stock-level attribution
    stock_attribution: Dict[str, float]  # symbol -> return contribution
    stock_weights: Dict[str, float]      # symbol -> average weight
    stock_returns: Dict[str, float]      # symbol -> individual return
    
    # Sector attribution
    sector_attribution: Dict[str, float]  # sector -> return contribution
    sector_weights: Dict[str, float]      # sector -> average weight
    sector_returns: Dict[str, float]      # sector -> sector return
    
    # Signal attribution
    signal_attribution: Dict[str, float]  # signal_type -> return contribution
    signal_win_rates: Dict[str, float]    # signal_type -> win rate
    signal_trade_counts: Dict[str, int]   # signal_type -> number of trades
    
    # Time-based attribution
    monthly_attribution: Dict[str, float]  # month -> return contribution
    quarterly_attribution: Dict[str, float]  # quarter -> return contribution
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

@dataclass
class ModelPerformanceMetrics:
    """Model prediction performance tracking"""
    # Prediction accuracy
    support_accuracy: float
    resistance_accuracy: float
    overall_accuracy: float
    
    # Confidence analysis
    confidence_correlation: float
    confidence_calibration: Dict[str, float]  # confidence_bucket -> actual_accuracy
    
    # Prediction errors
    support_mae: float
    resistance_mae: float
    overall_mae: float
    
    # Model evolution
    model_versions: List[int]
    accuracy_by_version: Dict[int, float]
    retrain_dates: List[date]
    
    # Feature importance (if available)
    feature_importance: Dict[str, float]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = asdict(self)
        # Convert dates to strings for JSON
        result['retrain_dates'] = [d.isoformat() for d in self.retrain_dates]
        return result

@dataclass
class DrillDownAnalysis:
    """Detailed analysis for specific time periods or stocks"""
    analysis_type: str  # "period", "stock", "trade"
    analysis_target: str  # date range, symbol, or trade_id
    
    # Basic metrics for the drill-down period
    metrics: PortfolioMetrics
    
    # Detailed breakdowns
    daily_returns: pd.Series
    position_details: Dict[str, Any]
    trade_details: List[Dict[str, Any]]
    
    # Market context
    market_regime: str  # "trending", "range_bound", "volatile"
    major_events: List[Dict[str, Any]]  # news, earnings, etc.
    
    # Model performance during period
    model_performance: ModelPerformanceMetrics
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'analysis_type': self.analysis_type,
            'analysis_target': self.analysis_target,
            'metrics': self.metrics.to_dict(),
            'daily_returns': self.daily_returns.to_dict() if self.daily_returns is not None else {},
            'position_details': self.position_details,
            'trade_details': self.trade_details,
            'market_regime': self.market_regime,
            'major_events': self.major_events,
            'model_performance': self.model_performance.to_dict()
        }

class PortfolioAnalyticsEngine:
    """High-performance analytics engine for portfolio analysis"""
    
    def __init__(self, env: Environment = None):
        self.env = env or Environment()
        self.logger = logging.getLogger(__name__)
        self.db_pool = None
        self.redis_client = None
        
    async def initialize(self):
        """Initialize database connections - REQUIRES REAL DATABASE"""
        # Initialize PostgreSQL connection pool
        db_url = self.env.get_database_url()
        if not db_url:
            raise ValueError("No database URL configured. Portfolio analytics requires real database connection.")
            
        self.db_pool = await asyncpg.create_pool(
            db_url,
            min_size=5,
            max_size=20,
            command_timeout=60
        )
        
        # Initialize Redis for caching
        self.redis_client = redis.Redis.from_url(
            "redis://localhost:6379",  # TODO: Make configurable
            decode_responses=True
        )
        
        self.logger.info("Portfolio analytics engine initialized with real database")
    
    async def close(self):
        """Clean up connections"""
        if self.db_pool:
            await self.db_pool.close()
        if self.redis_client:
            await self.redis_client.close()
    
    async def compute_portfolio_metrics(
        self,
        backtest_run_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        benchmark_run_id: Optional[str] = None
    ) -> PortfolioMetrics:
        """Compute comprehensive portfolio metrics with caching - USES REAL DATA ONLY"""
        
        # Create cache key
        cache_key = f"portfolio_metrics:{backtest_run_id}:{start_date}:{end_date}:{benchmark_run_id}"
        
        # Try to get from cache first
        cached_result = await self._get_from_cache(cache_key)
        if cached_result:
            return PortfolioMetrics(**cached_result)
        
        # Fetch portfolio performance data
        performance_data = await self._fetch_portfolio_performance_data(
            backtest_run_id, start_date, end_date
        )
        
        if performance_data.empty:
            raise ValueError(f"No performance data found for backtest {backtest_run_id}")
        
        # Fetch benchmark data if requested
        benchmark_data = None
        if benchmark_run_id:
            benchmark_data = await self._fetch_portfolio_performance_data(
                benchmark_run_id, start_date, end_date
            )
        
        # Compute metrics
        metrics = self._compute_portfolio_metrics(performance_data, benchmark_data)
        
        # Cache results for 5 minutes
        await self._cache_result(cache_key, metrics.to_dict(), ttl=300)
        
        return metrics
    
    async def compute_attribution_analysis(
        self,
        backtest_run_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> AttributionMetrics:
        """Compute performance attribution at various levels"""
        
        cache_key = f"attribution:{backtest_run_id}:{start_date}:{end_date}"
        cached_result = await self._get_from_cache(cache_key)
        if cached_result:
            return AttributionMetrics(**cached_result)
        
        # Fetch trade and position data
        trades_data = await self._fetch_trade_data(backtest_run_id, start_date, end_date)
        positions_data = await self._fetch_position_data(backtest_run_id, start_date, end_date)
        
        # Compute attribution
        attribution = self._compute_attribution_metrics(trades_data, positions_data)
        
        # Cache results
        await self._cache_result(cache_key, attribution.to_dict(), ttl=300)
        
        return attribution
    
    async def compute_model_performance(
        self,
        backtest_run_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> ModelPerformanceMetrics:
        """Analyze model prediction performance over time"""
        
        cache_key = f"model_performance:{backtest_run_id}:{start_date}:{end_date}"
        cached_result = await self._get_from_cache(cache_key)
        if cached_result:
            # Handle date conversion for cached results
            if 'retrain_dates' in cached_result:
                cached_result['retrain_dates'] = [
                    datetime.fromisoformat(d).date() for d in cached_result['retrain_dates']
                ]
            return ModelPerformanceMetrics(**cached_result)
        
        # Fetch model performance data
        model_data = await self._fetch_model_performance_data(backtest_run_id, start_date, end_date)
        forecast_data = await self._fetch_forecast_data(backtest_run_id, start_date, end_date)
        
        # Compute model-specific metrics
        metrics = self._compute_model_metrics(model_data, forecast_data)
        
        # Cache results
        await self._cache_result(cache_key, metrics.to_dict(), ttl=300)
        
        return metrics
    
    async def drill_down_analysis(
        self,
        backtest_run_id: str,
        analysis_type: str,
        analysis_target: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> DrillDownAnalysis:
        """Perform detailed drill-down analysis"""
        
        if analysis_type == "period":
            return await self._drill_down_period(backtest_run_id, analysis_target, start_date, end_date)
        elif analysis_type == "stock":
            return await self._drill_down_stock(backtest_run_id, analysis_target, start_date, end_date)
        elif analysis_type == "trade":
            return await self._drill_down_trade(backtest_run_id, analysis_target)
        else:
            raise ValueError(f"Unknown analysis type: {analysis_type}")
    
    # Private methods for data fetching
    async def _fetch_portfolio_performance_data(
        self,
        backtest_run_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """Fetch portfolio performance time series data"""
        
        query = """
        SELECT 
            timestamp::date as date,
            portfolio_value,
            daily_return,
            cumulative_return,
            drawdown,
            volatility_30d,
            sharpe_ratio_30d,
            positions_count,
            cash_position
        FROM portfolio_performance 
        WHERE backtest_run_id = $1
        """
        
        params = [backtest_run_id]
        
        if start_date:
            query += " AND timestamp::date >= $2"
            params.append(start_date)
            
        if end_date:
            if start_date:
                query += " AND timestamp::date <= $3"
            else:
                query += " AND timestamp::date <= $2"
            params.append(end_date)
        
        query += " ORDER BY timestamp"
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            
        if not rows:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame([dict(row) for row in rows])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        return df
    
    async def _fetch_trade_data(
        self,
        backtest_run_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """Fetch trade execution data"""
        
        query = """
        SELECT 
            trade_id,
            timestamp,
            symbol,
            trade_type,
            quantity,
            price,
            commission,
            signal_type,
            model_confidence,
            entry_rationale,
            exit_rationale
        FROM trade_execution 
        WHERE backtest_run_id = $1
        """
        
        params = [backtest_run_id]
        
        if start_date:
            query += " AND timestamp::date >= $2"
            params.append(start_date)
            
        if end_date:
            if start_date:
                query += " AND timestamp::date <= $3"
            else:
                query += " AND timestamp::date <= $2"
            params.append(end_date)
        
        query += " ORDER BY timestamp"
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        if not rows:
            return pd.DataFrame()
        
        return pd.DataFrame([dict(row) for row in rows])
    
    async def _fetch_position_data(
        self,
        backtest_run_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """Fetch position performance data"""
        
        query = """
        SELECT 
            timestamp,
            symbol,
            position_size,
            market_value,
            unrealized_pnl,
            realized_pnl,
            entry_price,
            current_price,
            support_levels,
            resistance_levels,
            confidence_scores,
            model_version
        FROM position_performance 
        WHERE backtest_run_id = $1
        """
        
        params = [backtest_run_id]
        
        if start_date:
            query += " AND timestamp::date >= $2"
            params.append(start_date)
            
        if end_date:
            if start_date:
                query += " AND timestamp::date <= $3"
            else:
                query += " AND timestamp::date <= $2"
            params.append(end_date)
        
        query += " ORDER BY timestamp, symbol"
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        if not rows:
            return pd.DataFrame()
        
        return pd.DataFrame([dict(row) for row in rows])
    
    async def _fetch_model_performance_data(
        self,
        backtest_run_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """Fetch model performance metrics data"""
        
        query = """
        SELECT 
            date,
            model_version,
            support_accuracy,
            resistance_accuracy,
            prediction_mae,
            confidence_correlation,
            predictions_count,
            retraining_occurred,
            processing_time_seconds
        FROM model_performance 
        WHERE backtest_run_id = $1
        """
        
        params = [backtest_run_id]
        
        if start_date:
            query += " AND date >= $2"
            params.append(start_date)
            
        if end_date:
            if start_date:
                query += " AND date <= $3"
            else:
                query += " AND date <= $2"
            params.append(end_date)
        
        query += " ORDER BY date"
        
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
        
        if not rows:
            return pd.DataFrame()
        
        return pd.DataFrame([dict(row) for row in rows])
    
    async def _fetch_forecast_data(
        self,
        backtest_run_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """Fetch forecast/prediction data"""
        
        # This would fetch from position_performance table's support/resistance levels
        # For now, return empty DataFrame as placeholder
        return pd.DataFrame()
    
    # Private methods for metric computation
    def _compute_portfolio_metrics(
        self,
        performance_data: pd.DataFrame,
        benchmark_data: Optional[pd.DataFrame] = None
    ) -> PortfolioMetrics:
        """Core portfolio metrics computation"""
        
        if performance_data.empty:
            raise ValueError("No performance data available")
        
        # Basic data preparation
        returns = performance_data['daily_return'].dropna()
        equity_curve = performance_data['portfolio_value']
        
        # Handle missing or invalid data
        if len(returns) == 0:
            raise ValueError("No valid daily returns found")
        
        # Basic performance metrics
        total_return = (equity_curve.iloc[-1] / equity_curve.iloc[0]) - 1
        trading_days = len(returns)
        annualized_return = (1 + total_return) ** (252 / trading_days) - 1 if trading_days > 0 else 0
        
        # Risk metrics
        volatility = returns.std() * np.sqrt(252) if len(returns) > 1 else 0
        downside_returns = returns[returns < 0]
        downside_volatility = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 1 else 0
        
        # Risk-adjusted returns
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0
        sortino_ratio = annualized_return / downside_volatility if downside_volatility > 0 else 0
        
        # Drawdown analysis
        if 'drawdown' in performance_data.columns:
            drawdown_series = performance_data['drawdown']
        else:
            running_max = equity_curve.expanding().max()
            drawdown_series = (equity_curve - running_max) / running_max
        
        max_drawdown = abs(drawdown_series.min()) if not drawdown_series.empty else 0
        
        # Calculate drawdown duration
        in_drawdown = drawdown_series < -0.01  # 1% threshold
        max_drawdown_duration = 0
        if in_drawdown.any():
            # Find longest consecutive period in drawdown
            drawdown_periods = []
            current_period = 0
            for is_dd in in_drawdown:
                if is_dd:
                    current_period += 1
                else:
                    if current_period > 0:
                        drawdown_periods.append(current_period)
                        current_period = 0
            if current_period > 0:
                drawdown_periods.append(current_period)
            max_drawdown_duration = max(drawdown_periods) if drawdown_periods else 0
        
        # VaR and Expected Shortfall
        var_95 = np.percentile(returns, 5) if len(returns) > 0 else 0
        var_99 = np.percentile(returns, 1) if len(returns) > 0 else 0
        expected_shortfall_95 = returns[returns <= var_95].mean() if (returns <= var_95).any() else 0
        expected_shortfall_99 = returns[returns <= var_99].mean() if (returns <= var_99).any() else 0
        
        # Trading metrics (these would need trade-level data)
        total_trades = 0
        win_rate = 0.0
        profit_factor = 0.0
        avg_win = 0.0
        avg_loss = 0.0
        largest_win = 0.0
        largest_loss = 0.0
        
        # Additional risk-adjusted metrics
        calmar_ratio = annualized_return / max_drawdown if max_drawdown > 0 else 0
        
        # Benchmark-relative metrics
        information_ratio = 0.0
        treynor_ratio = 0.0
        jensen_alpha = 0.0
        beta = 0.0
        correlation_to_benchmark = 0.0
        
        if benchmark_data is not None and not benchmark_data.empty:
            benchmark_returns = benchmark_data['daily_return'].dropna()
            if len(benchmark_returns) > 0 and len(returns) == len(benchmark_returns):
                # Align the series
                aligned_returns = returns.align(benchmark_returns, join='inner')
                portfolio_ret, benchmark_ret = aligned_returns
                
                excess_returns = portfolio_ret - benchmark_ret
                tracking_error = excess_returns.std() * np.sqrt(252) if len(excess_returns) > 1 else 0
                information_ratio = excess_returns.mean() * np.sqrt(252) / tracking_error if tracking_error > 0 else 0
                
                # Beta and correlation
                if len(portfolio_ret) > 1 and len(benchmark_ret) > 1:
                    correlation_to_benchmark = portfolio_ret.corr(benchmark_ret)
                    beta = portfolio_ret.cov(benchmark_ret) / benchmark_ret.var() if benchmark_ret.var() > 0 else 0
                    
                    # Risk-free rate assumption (could be parameterized)
                    risk_free_rate = 0.02 / 252  # 2% annual
                    benchmark_excess = benchmark_ret.mean() - risk_free_rate
                    treynor_ratio = (portfolio_ret.mean() - risk_free_rate) / beta if beta > 0 else 0
                    jensen_alpha = portfolio_ret.mean() - (risk_free_rate + beta * benchmark_excess)
        
        return PortfolioMetrics(
            total_return=float(total_return),
            annualized_return=float(annualized_return),
            volatility=float(volatility),
            sharpe_ratio=float(sharpe_ratio),
            sortino_ratio=float(sortino_ratio),
            calmar_ratio=float(calmar_ratio),
            max_drawdown=float(max_drawdown),
            max_drawdown_duration_days=int(max_drawdown_duration),
            var_95=float(var_95),
            var_99=float(var_99),
            expected_shortfall_95=float(expected_shortfall_95),
            expected_shortfall_99=float(expected_shortfall_99),
            total_trades=total_trades,
            win_rate=win_rate,
            profit_factor=profit_factor,
            avg_win=avg_win,
            avg_loss=avg_loss,
            largest_win=largest_win,
            largest_loss=largest_loss,
            information_ratio=float(information_ratio),
            treynor_ratio=float(treynor_ratio),
            jensen_alpha=float(jensen_alpha),
            beta=float(beta),
            correlation_to_benchmark=float(correlation_to_benchmark),
            start_date=performance_data.index[0].date(),
            end_date=performance_data.index[-1].date(),
            total_days=len(performance_data)
        )
    
    def _compute_attribution_metrics(
        self,
        trades_data: pd.DataFrame,
        positions_data: pd.DataFrame
    ) -> AttributionMetrics:
        """Compute performance attribution analysis"""
        
        # Initialize empty attribution
        stock_attribution = {}
        stock_weights = {}
        stock_returns = {}
        sector_attribution = {}
        sector_weights = {}
        sector_returns = {}
        signal_attribution = {}
        signal_win_rates = {}
        signal_trade_counts = {}
        monthly_attribution = {}
        quarterly_attribution = {}
        
        # Stock-level attribution from positions
        if not positions_data.empty:
            # Calculate stock-level metrics
            for symbol in positions_data['symbol'].unique():
                symbol_data = positions_data[positions_data['symbol'] == symbol]
                
                # Calculate return contribution
                total_pnl = symbol_data['realized_pnl'].sum() + symbol_data['unrealized_pnl'].iloc[-1]
                stock_attribution[symbol] = float(total_pnl)
                
                # Calculate average weight
                avg_weight = symbol_data['market_value'].mean()
                stock_weights[symbol] = float(avg_weight) if pd.notna(avg_weight) else 0.0
                
                # Calculate individual return
                if len(symbol_data) > 1:
                    start_value = symbol_data['market_value'].iloc[0]
                    end_value = symbol_data['market_value'].iloc[-1]
                    stock_returns[symbol] = float((end_value / start_value - 1)) if start_value > 0 else 0.0
                else:
                    stock_returns[symbol] = 0.0
        
        # Signal-level attribution from trades
        if not trades_data.empty:
            for signal_type in trades_data['signal_type'].dropna().unique():
                signal_trades = trades_data[trades_data['signal_type'] == signal_type]
                
                # Calculate P&L for this signal type
                # This is simplified - would need proper trade matching for accurate P&L
                signal_pnl = len(signal_trades) * 0.01  # Placeholder
                signal_attribution[signal_type] = signal_pnl
                
                # Calculate win rate (simplified)
                signal_win_rates[signal_type] = 0.6  # Placeholder
                signal_trade_counts[signal_type] = len(signal_trades)
        
        # Time-based attribution (simplified)
        if not positions_data.empty and 'timestamp' in positions_data.columns:
            positions_data['month'] = pd.to_datetime(positions_data['timestamp']).dt.to_period('M')
            monthly_pnl = positions_data.groupby('month')['realized_pnl'].sum()
            monthly_attribution = {str(month): float(pnl) for month, pnl in monthly_pnl.items()}
        
        return AttributionMetrics(
            stock_attribution=stock_attribution,
            stock_weights=stock_weights,
            stock_returns=stock_returns,
            sector_attribution=sector_attribution,
            sector_weights=sector_weights,
            sector_returns=sector_returns,
            signal_attribution=signal_attribution,
            signal_win_rates=signal_win_rates,
            signal_trade_counts=signal_trade_counts,
            monthly_attribution=monthly_attribution,
            quarterly_attribution=quarterly_attribution
        )
    
    def _compute_model_metrics(
        self,
        model_data: pd.DataFrame,
        forecast_data: pd.DataFrame
    ) -> ModelPerformanceMetrics:
        """Compute model prediction performance metrics"""
        
        if model_data.empty:
            # Return default metrics if no data
            return ModelPerformanceMetrics(
                support_accuracy=0.0,
                resistance_accuracy=0.0,
                overall_accuracy=0.0,
                confidence_correlation=0.0,
                confidence_calibration={},
                support_mae=0.0,
                resistance_mae=0.0,
                overall_mae=0.0,
                model_versions=[],
                accuracy_by_version={},
                retrain_dates=[],
                feature_importance={}
            )
        
        # Calculate accuracy metrics
        support_accuracy = model_data['support_accuracy'].mean() if 'support_accuracy' in model_data.columns else 0.0
        resistance_accuracy = model_data['resistance_accuracy'].mean() if 'resistance_accuracy' in model_data.columns else 0.0
        overall_accuracy = (support_accuracy + resistance_accuracy) / 2
        
        # Confidence correlation
        confidence_correlation = model_data['confidence_correlation'].mean() if 'confidence_correlation' in model_data.columns else 0.0
        
        # MAE metrics
        support_mae = model_data['prediction_mae'].mean() if 'prediction_mae' in model_data.columns else 0.0
        resistance_mae = support_mae  # Simplified - would need separate tracking
        overall_mae = support_mae
        
        # Model versions and evolution
        model_versions = sorted(model_data['model_version'].unique()) if 'model_version' in model_data.columns else []
        accuracy_by_version = {}
        retrain_dates = []
        
        if 'model_version' in model_data.columns:
            for version in model_versions:
                version_data = model_data[model_data['model_version'] == version]
                avg_accuracy = version_data['support_accuracy'].mean()
                accuracy_by_version[int(version)] = float(avg_accuracy) if pd.notna(avg_accuracy) else 0.0
        
        if 'retraining_occurred' in model_data.columns:
            retrain_mask = model_data['retraining_occurred']
            retrain_dates = model_data[retrain_mask]['date'].tolist()
        
        # Confidence calibration (simplified)
        confidence_calibration = {
            "0.0-0.2": 0.15,
            "0.2-0.4": 0.35,
            "0.4-0.6": 0.55,
            "0.6-0.8": 0.75,
            "0.8-1.0": 0.85
        }  # Placeholder values
        
        # Feature importance (placeholder)
        feature_importance = {
            "rsi_14": 0.15,
            "ma_crossover": 0.12,
            "volume_ratio": 0.10,
            "volatility": 0.08,
            "support_distance": 0.20,
            "resistance_distance": 0.18,
            "market_regime": 0.17
        }
        
        return ModelPerformanceMetrics(
            support_accuracy=float(support_accuracy),
            resistance_accuracy=float(resistance_accuracy),
            overall_accuracy=float(overall_accuracy),
            confidence_correlation=float(confidence_correlation),
            confidence_calibration=confidence_calibration,
            support_mae=float(support_mae),
            resistance_mae=float(resistance_mae),
            overall_mae=float(overall_mae),
            model_versions=model_versions,
            accuracy_by_version=accuracy_by_version,
            retrain_dates=retrain_dates,
            feature_importance=feature_importance
        )
    
    # Drill-down analysis methods
    async def _drill_down_period(
        self,
        backtest_run_id: str,
        period_spec: str,  # e.g., "2023-01-01:2023-01-31"
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> DrillDownAnalysis:
        """Perform period-based drill-down analysis"""
        
        # Parse period specification
        if ':' in period_spec:
            period_start, period_end = period_spec.split(':')
            period_start = datetime.fromisoformat(period_start).date()
            period_end = datetime.fromisoformat(period_end).date()
        else:
            # Single date - analyze that day
            period_start = period_end = datetime.fromisoformat(period_spec).date()
        
        # Compute metrics for the period
        metrics = await self.compute_portfolio_metrics(backtest_run_id, period_start, period_end)
        
        # Get detailed data for the period
        performance_data = await self._fetch_portfolio_performance_data(
            backtest_run_id, period_start, period_end
        )
        daily_returns = performance_data['daily_return'] if not performance_data.empty else pd.Series()
        
        # Get position and trade details
        positions = await self._fetch_position_data(backtest_run_id, period_start, period_end)
        trades = await self._fetch_trade_data(backtest_run_id, period_start, period_end)
        
        position_details = {}
        if not positions.empty:
            position_details = {
                'symbols': positions['symbol'].unique().tolist(),
                'avg_positions': len(positions['symbol'].unique()),
                'total_market_value': float(positions['market_value'].sum()),
                'top_positions': positions.groupby('symbol')['market_value'].mean().nlargest(5).to_dict()
            }
        
        trade_details = []
        if not trades.empty:
            trade_details = trades.to_dict('records')
        
        # Market regime analysis (simplified)
        market_regime = "range_bound"  # Placeholder
        if not daily_returns.empty:
            volatility = daily_returns.std()
            if volatility > 0.03:
                market_regime = "volatile"
            elif abs(daily_returns.mean()) > 0.002:
                market_regime = "trending"
        
        # Model performance for period
        model_performance = await self.compute_model_performance(
            backtest_run_id, period_start, period_end
        )
        
        return DrillDownAnalysis(
            analysis_type="period",
            analysis_target=period_spec,
            metrics=metrics,
            daily_returns=daily_returns,
            position_details=position_details,
            trade_details=trade_details,
            market_regime=market_regime,
            major_events=[],  # Placeholder
            model_performance=model_performance
        )
    
    async def _drill_down_stock(
        self,
        backtest_run_id: str,
        symbol: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> DrillDownAnalysis:
        """Perform stock-specific drill-down analysis"""
        
        # This would be similar to period drill-down but filtered by symbol
        # Implementation would fetch symbol-specific data and compute metrics
        
        # Placeholder implementation
        return DrillDownAnalysis(
            analysis_type="stock",
            analysis_target=symbol,
            metrics=PortfolioMetrics(
                total_return=0.0, annualized_return=0.0, volatility=0.0,
                sharpe_ratio=0.0, sortino_ratio=0.0, calmar_ratio=0.0,
                max_drawdown=0.0, max_drawdown_duration_days=0,
                var_95=0.0, var_99=0.0, expected_shortfall_95=0.0, expected_shortfall_99=0.0,
                total_trades=0, win_rate=0.0, profit_factor=0.0,
                avg_win=0.0, avg_loss=0.0, largest_win=0.0, largest_loss=0.0,
                information_ratio=0.0, treynor_ratio=0.0, jensen_alpha=0.0,
                beta=0.0, correlation_to_benchmark=0.0,
                start_date=date.today(), end_date=date.today(), total_days=0
            ),
            daily_returns=pd.Series(),
            position_details={},
            trade_details=[],
            market_regime="unknown",
            major_events=[],
            model_performance=ModelPerformanceMetrics(
                support_accuracy=0.0, resistance_accuracy=0.0, overall_accuracy=0.0,
                confidence_correlation=0.0, confidence_calibration={},
                support_mae=0.0, resistance_mae=0.0, overall_mae=0.0,
                model_versions=[], accuracy_by_version={}, retrain_dates=[], feature_importance={}
            )
        )
    
    async def _drill_down_trade(self, backtest_run_id: str, trade_id: str) -> DrillDownAnalysis:
        """Perform trade-specific drill-down analysis"""
        
        # This would fetch specific trade details and surrounding context
        # Placeholder implementation
        
        return DrillDownAnalysis(
            analysis_type="trade",
            analysis_target=trade_id,
            metrics=PortfolioMetrics(
                total_return=0.0, annualized_return=0.0, volatility=0.0,
                sharpe_ratio=0.0, sortino_ratio=0.0, calmar_ratio=0.0,
                max_drawdown=0.0, max_drawdown_duration_days=0,
                var_95=0.0, var_99=0.0, expected_shortfall_95=0.0, expected_shortfall_99=0.0,
                total_trades=1, win_rate=0.0, profit_factor=0.0,
                avg_win=0.0, avg_loss=0.0, largest_win=0.0, largest_loss=0.0,
                information_ratio=0.0, treynor_ratio=0.0, jensen_alpha=0.0,
                beta=0.0, correlation_to_benchmark=0.0,
                start_date=date.today(), end_date=date.today(), total_days=1
            ),
            daily_returns=pd.Series(),
            position_details={},
            trade_details=[],
            market_regime="unknown",
            major_events=[],
            model_performance=ModelPerformanceMetrics(
                support_accuracy=0.0, resistance_accuracy=0.0, overall_accuracy=0.0,
                confidence_correlation=0.0, confidence_calibration={},
                support_mae=0.0, resistance_mae=0.0, overall_mae=0.0,
                model_versions=[], accuracy_by_version={}, retrain_dates=[], feature_importance={}
            )
        )
    
    # Cache management
    async def _get_from_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Get data from Redis cache"""
        try:
            if self.redis_client:
                cached_data = await self.redis_client.get(key)
                if cached_data:
                    import json
                    return json.loads(cached_data)
        except Exception as e:
            self.logger.warning(f"Cache get failed for key {key}: {e}")
        return None
    
    async def _cache_result(self, key: str, data: Dict[str, Any], ttl: int = 300):
        """Cache data in Redis"""
        try:
            if self.redis_client:
                import json
                await self.redis_client.setex(key, ttl, json.dumps(data, default=str))
        except Exception as e:
            self.logger.warning(f"Cache set failed for key {key}: {e}")
    
    async def invalidate_cache(self, pattern: str = None):
        """Invalidate cache entries matching pattern"""
        try:
            if self.redis_client:
                if pattern:
                    keys = await self.redis_client.keys(pattern)
                    if keys:
                        await self.redis_client.delete(*keys)
                else:
                    await self.redis_client.flushdb()
        except Exception as e:
            self.logger.warning(f"Cache invalidation failed: {e}")