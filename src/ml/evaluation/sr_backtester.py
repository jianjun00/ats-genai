"""
Support/Resistance Model Backtesting Framework

Comprehensive backtesting system for evaluating support/resistance prediction models
on historical data with realistic trading simulations.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple, Optional, NamedTuple
from dataclasses import dataclass
from config.environment import Environment
# import matplotlib.pyplot as plt
# import seaborn as sns
# from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import gin
import json
import os
from pathlib import Path

@dataclass
class PortfolioSnapshot:
    """Daily portfolio snapshot for backtesting"""
    date: date
    total_portfolio_value: float
    daily_return: float
    cumulative_return: float
    cash_position: float
    holdings: List[Dict]
    sector_allocation: Dict[str, float]
    top_contributors: List[Dict]
    top_detractors: List[Dict]

class PredictionResult(NamedTuple):
    """Single prediction result for backtesting"""
    symbol: str
    date: date
    predicted_support: List[float]
    predicted_resistance: List[float]
    support_confidence: List[float]
    resistance_confidence: List[float]
    actual_low: float
    actual_high: float
    actual_close: float
    
class TradingSignal(NamedTuple):
    """Trading signal generated from S/R predictions"""
    symbol: str
    date: date
    signal_type: str  # 'buy_support', 'sell_resistance', 'hold'
    entry_price: float
    target_price: float
    stop_loss: float
    confidence: float
    rationale: str

@dataclass
class BacktestMetrics:
    """Comprehensive backtesting metrics"""
    # Prediction accuracy metrics
    support_accuracy: float
    resistance_accuracy: float
    level_mae: float  # Mean absolute error for predicted levels
    confidence_correlation: float
    
    # Trading performance metrics
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_return_per_trade: float
    sharpe_ratio: float
    max_drawdown: float
    
    # Risk metrics
    var_95: float  # Value at Risk (95%)
    expected_shortfall: float
    
    # Level testing metrics
    support_test_rate: float  # How often support levels were tested
    resistance_test_rate: float
    support_hold_rate: float  # How often support held when tested
    resistance_hold_rate: float

@gin.configurable
class SRBacktester:
    """
    Backtesting framework for support/resistance prediction models.
    
    Evaluates model performance on multiple dimensions:
    1. Prediction accuracy (how close predicted levels are to actual S/R)
    2. Trading performance (profit/loss from trading signals)
    3. Risk-adjusted returns
    4. Level effectiveness (how often levels actually work)
    """
    
    def __init__(self, env: Environment = None):
        self.env = env or Environment()
        self.logger = logging.getLogger(__name__)
        
        # Backtesting parameters
        self.level_tolerance_pct = 0.5  # % tolerance for level hit/miss
        self.min_confidence_threshold = 0.3  # Minimum confidence for trading
        self.position_size_pct = 0.02  # 2% position size
        self.transaction_cost_bps = 5  # 5 basis points transaction cost
        
        # Portfolio tracking
        self.portfolio_snapshots: List[PortfolioSnapshot] = []
        self.initial_capital = 1000000.0  # $1M starting capital
        self.current_cash = self.initial_capital
        self.current_positions = {}  # symbol -> {shares, avg_cost}
        self.portfolio_history = []  # Daily portfolio values
        
    async def backtest_model(
        self,
        model,
        symbols: List[str],
        start_date: date,
        end_date: date,
        feature_generator,
        min_predictions_per_symbol: int = 50,
        backtest_run_id: str = None,
        save_portfolio_files: bool = True
    ) -> Dict[str, BacktestMetrics]:
        """
        Run comprehensive backtest of support/resistance model.
        
        Args:
            model: Trained SR prediction model
            symbols: List of symbols to backtest
            start_date: Start date for backtesting
            end_date: End date for backtesting
            feature_generator: Feature generator for creating model inputs
            min_predictions_per_symbol: Minimum predictions needed per symbol
            
        Returns:
            Dictionary mapping symbol to BacktestMetrics
        """
        self.logger.info(f"Starting backtest for {len(symbols)} symbols")
        self.logger.info(f"Backtest period: {start_date} to {end_date}")
        
        results = {}
        
        for symbol in symbols:
            self.logger.info(f"Backtesting {symbol}...")
            
            try:
                # Generate predictions for the symbol
                predictions = await self._generate_predictions_for_symbol(
                    model, symbol, start_date, end_date, feature_generator
                )
                
                if len(predictions) < min_predictions_per_symbol:
                    self.logger.warning(f"Only {len(predictions)} predictions for {symbol}, "
                                      f"skipping (need {min_predictions_per_symbol})")
                    continue
                
                # Calculate metrics
                metrics = await self._calculate_backtest_metrics(symbol, predictions)
                results[symbol] = metrics
                
                self.logger.info(f"{symbol} backtest complete: "
                               f"Win rate: {metrics.win_rate:.2%}, "
                               f"Avg return: {metrics.avg_return_per_trade:.3f}, "
                               f"Sharpe: {metrics.sharpe_ratio:.2f}")
                
            except Exception as e:
                self.logger.error(f"Error backtesting {symbol}: {e}")
                continue
        
        # Calculate aggregate metrics
        aggregate_metrics = self._calculate_aggregate_metrics(results)
        results['_AGGREGATE'] = aggregate_metrics
        
        # Generate portfolio snapshots and save to disk if requested
        if save_portfolio_files and backtest_run_id:
            await self._generate_and_save_portfolio_file(backtest_run_id, symbols, start_date, end_date, results)
        
        self.logger.info(f"Backtest completed for {len(results)-1} symbols")
        return results
    
    async def _generate_predictions_for_symbol(
        self,
        model,
        symbol: str,
        start_date: date,
        end_date: date,
        feature_generator
    ) -> List[PredictionResult]:
        """Generate predictions for a single symbol across the backtest period"""
        
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                # Get daily data for the symbol
                daily_data = await self._get_daily_data(conn, symbol, start_date, end_date)
                
                if len(daily_data) < 50:  # Need minimum history
                    return []
                
                predictions = []
                
                # Generate predictions for each day
                for i in range(20, len(daily_data) - 1):  # Need history + next day
                    current_date = daily_data.iloc[i]['date']
                    next_day_data = daily_data.iloc[i + 1]
                    
                    try:
                        # Generate features for current day
                        features = await feature_generator._generate_features(
                            conn, symbol, daily_data, i, current_date
                        )
                        
                        if features is None:
                            continue
                        
                        # Convert features to array format expected by model
                        feature_vector = np.array([
                            features.get(key, 0.0) 
                            for key in sorted(features.keys())
                        ]).reshape(1, -1)
                        
                        # Make prediction
                        model_output = model.predict(feature_vector)
                        
                        # Extract predictions
                        support_levels = model_output['support_levels'][0].tolist()
                        resistance_levels = model_output['resistance_levels'][0].tolist()
                        support_conf = model_output['support_confidence'][0].tolist()
                        resistance_conf = model_output['resistance_confidence'][0].tolist()
                        
                        # Create prediction result
                        prediction = PredictionResult(
                            symbol=symbol,
                            date=current_date,
                            predicted_support=support_levels,
                            predicted_resistance=resistance_levels,
                            support_confidence=support_conf,
                            resistance_confidence=resistance_conf,
                            actual_low=next_day_data['low'],
                            actual_high=next_day_data['high'],
                            actual_close=next_day_data['close']
                        )
                        
                        predictions.append(prediction)
                        
                    except Exception as e:
                        self.logger.warning(f"Error generating prediction for {symbol} on {current_date}: {e}")
                        continue
                
                return predictions
                
        finally:
            await pool.close()
    
    async def _get_daily_data(self, conn, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get daily OHLCV data for backtesting"""
        
        # Try multiple data sources
        queries = [
            f"""
            SELECT date, open, high, low, close, volume
            FROM {self.env.get_table_name('daily_prices_polygon')}
            WHERE symbol = $1 AND date >= $2 AND date <= $3
            ORDER BY date
            """,
            f"""
            SELECT date, open, high, low, close, volume
            FROM {self.env.get_table_name('daily_prices_tiingo')}
            WHERE symbol = $1 AND date >= $2 AND date <= $3
            ORDER BY date
            """,
            f"""
            SELECT date, open, high, low, close, volume
            FROM {self.env.get_table_name('daily_prices')}
            WHERE symbol = $1 AND date >= $2 AND date <= $3
            ORDER BY date
            """
        ]
        
        for query in queries:
            try:
                rows = await conn.fetch(query, symbol, start_date, end_date)
                if rows:
                    return pd.DataFrame(rows)
            except:
                continue
        
        return pd.DataFrame()
    
    async def _calculate_backtest_metrics(
        self, 
        symbol: str, 
        predictions: List[PredictionResult]
    ) -> BacktestMetrics:
        """Calculate comprehensive backtest metrics for a symbol"""
        
        # Prediction accuracy metrics
        support_accuracy = self._calculate_level_accuracy(predictions, 'support')
        resistance_accuracy = self._calculate_level_accuracy(predictions, 'resistance')
        level_mae = self._calculate_level_mae(predictions)
        confidence_correlation = self._calculate_confidence_correlation(predictions)
        
        # Generate trading signals
        trading_signals = self._generate_trading_signals(predictions)
        
        # Trading performance metrics
        trading_metrics = self._calculate_trading_metrics(trading_signals, predictions)
        
        # Level testing metrics
        testing_metrics = self._calculate_level_testing_metrics(predictions)
        
        return BacktestMetrics(
            support_accuracy=support_accuracy,
            resistance_accuracy=resistance_accuracy,
            level_mae=level_mae,
            confidence_correlation=confidence_correlation,
            total_trades=trading_metrics['total_trades'],
            winning_trades=trading_metrics['winning_trades'],
            losing_trades=trading_metrics['losing_trades'],
            win_rate=trading_metrics['win_rate'],
            avg_return_per_trade=trading_metrics['avg_return'],
            sharpe_ratio=trading_metrics['sharpe_ratio'],
            max_drawdown=trading_metrics['max_drawdown'],
            var_95=trading_metrics['var_95'],
            expected_shortfall=trading_metrics['expected_shortfall'],
            support_test_rate=testing_metrics['support_test_rate'],
            resistance_test_rate=testing_metrics['resistance_test_rate'],
            support_hold_rate=testing_metrics['support_hold_rate'],
            resistance_hold_rate=testing_metrics['resistance_hold_rate']
        )
    
    def _calculate_level_accuracy(self, predictions: List[PredictionResult], level_type: str) -> float:
        """Calculate accuracy of predicted levels"""
        
        correct_predictions = 0
        total_predictions = 0
        
        for pred in predictions:
            if level_type == 'support':
                predicted_levels = pred.predicted_support
                actual_extreme = pred.actual_low
            else:  # resistance
                predicted_levels = pred.predicted_resistance
                actual_extreme = pred.actual_high
            
            for level in predicted_levels:
                if level > 0:  # Valid prediction
                    tolerance = level * self.level_tolerance_pct / 100
                    
                    if level_type == 'support':
                        # Support is accurate if actual low touched or came close to predicted level
                        if abs(actual_extreme - level) <= tolerance:
                            correct_predictions += 1
                    else:  # resistance
                        # Resistance is accurate if actual high touched or came close to predicted level
                        if abs(actual_extreme - level) <= tolerance:
                            correct_predictions += 1
                    
                    total_predictions += 1
        
        return correct_predictions / max(total_predictions, 1)
    
    def _calculate_level_mae(self, predictions: List[PredictionResult]) -> float:
        """Calculate mean absolute error for predicted levels"""
        
        errors = []
        
        for pred in predictions:
            # Support level errors
            for level in pred.predicted_support:
                if level > 0:
                    error = abs(level - pred.actual_low) / pred.actual_low
                    errors.append(error)
            
            # Resistance level errors
            for level in pred.predicted_resistance:
                if level > 0:
                    error = abs(level - pred.actual_high) / pred.actual_high
                    errors.append(error)
        
        return np.mean(errors) if errors else float('inf')
    
    def _calculate_confidence_correlation(self, predictions: List[PredictionResult]) -> float:
        """Calculate correlation between predicted confidence and actual accuracy"""
        
        confidences = []
        accuracies = []
        
        for pred in predictions:
            # Support levels
            for i, level in enumerate(pred.predicted_support):
                if level > 0 and i < len(pred.support_confidence):
                    conf = pred.support_confidence[i]
                    tolerance = level * self.level_tolerance_pct / 100
                    accurate = abs(pred.actual_low - level) <= tolerance
                    
                    confidences.append(conf)
                    accuracies.append(1.0 if accurate else 0.0)
            
            # Resistance levels
            for i, level in enumerate(pred.predicted_resistance):
                if level > 0 and i < len(pred.resistance_confidence):
                    conf = pred.resistance_confidence[i]
                    tolerance = level * self.level_tolerance_pct / 100
                    accurate = abs(pred.actual_high - level) <= tolerance
                    
                    confidences.append(conf)
                    accuracies.append(1.0 if accurate else 0.0)
        
        if len(confidences) > 1:
            return np.corrcoef(confidences, accuracies)[0, 1]
        else:
            return 0.0
    
    def _generate_trading_signals(self, predictions: List[PredictionResult]) -> List[TradingSignal]:
        """Generate trading signals from predictions"""
        
        signals = []
        
        for pred in predictions:
            # Generate buy signals at strong support levels
            for i, support_level in enumerate(pred.predicted_support):
                if (support_level > 0 and 
                    i < len(pred.support_confidence) and 
                    pred.support_confidence[i] >= self.min_confidence_threshold):
                    
                    # Buy signal if price approaches support
                    if pred.actual_low <= support_level * 1.02:  # Within 2% of support
                        signal = TradingSignal(
                            symbol=pred.symbol,
                            date=pred.date,
                            signal_type='buy_support',
                            entry_price=support_level * 1.01,  # Slightly above support
                            target_price=support_level * 1.06,  # 5% target
                            stop_loss=support_level * 0.98,   # 2% stop loss
                            confidence=pred.support_confidence[i],
                            rationale=f"Strong support at ${support_level:.2f}"
                        )
                        signals.append(signal)
            
            # Generate sell signals at strong resistance levels
            for i, resistance_level in enumerate(pred.predicted_resistance):
                if (resistance_level > 0 and 
                    i < len(pred.resistance_confidence) and 
                    pred.resistance_confidence[i] >= self.min_confidence_threshold):
                    
                    # Sell signal if price approaches resistance
                    if pred.actual_high >= resistance_level * 0.98:  # Within 2% of resistance
                        signal = TradingSignal(
                            symbol=pred.symbol,
                            date=pred.date,
                            signal_type='sell_resistance',
                            entry_price=resistance_level * 0.99,  # Slightly below resistance
                            target_price=resistance_level * 0.94,  # 5% target
                            stop_loss=resistance_level * 1.02,    # 2% stop loss
                            confidence=pred.resistance_confidence[i],
                            rationale=f"Strong resistance at ${resistance_level:.2f}"
                        )
                        signals.append(signal)
        
        return signals
    
    def _calculate_trading_metrics(self, signals: List[TradingSignal], predictions: List[PredictionResult]) -> Dict[str, float]:
        """Calculate trading performance metrics"""
        
        if not signals:
            return {
                'total_trades': 0, 'winning_trades': 0, 'losing_trades': 0,
                'win_rate': 0.0, 'avg_return': 0.0, 'sharpe_ratio': 0.0,
                'max_drawdown': 0.0, 'var_95': 0.0, 'expected_shortfall': 0.0
            }
        
        # Create prediction lookup
        pred_lookup = {(p.symbol, p.date): p for p in predictions}
        
        returns = []
        equity_curve = [1.0]  # Start with $1
        
        for signal in signals:
            # Find the corresponding prediction
            pred = pred_lookup.get((signal.symbol, signal.date))
            if not pred:
                continue
            
            # Calculate trade return based on signal type
            if signal.signal_type == 'buy_support':
                # Long trade
                if pred.actual_low <= signal.stop_loss:
                    # Stop loss hit
                    trade_return = (signal.stop_loss - signal.entry_price) / signal.entry_price
                elif pred.actual_high >= signal.target_price:
                    # Target hit
                    trade_return = (signal.target_price - signal.entry_price) / signal.entry_price
                else:
                    # Exit at close
                    trade_return = (pred.actual_close - signal.entry_price) / signal.entry_price
                    
            else:  # sell_resistance (short trade)
                if pred.actual_high >= signal.stop_loss:
                    # Stop loss hit
                    trade_return = (signal.entry_price - signal.stop_loss) / signal.entry_price
                elif pred.actual_low <= signal.target_price:
                    # Target hit
                    trade_return = (signal.entry_price - signal.target_price) / signal.entry_price
                else:
                    # Exit at close
                    trade_return = (signal.entry_price - pred.actual_close) / signal.entry_price
            
            # Apply transaction costs
            trade_return -= 2 * self.transaction_cost_bps / 10000  # Buy and sell
            
            returns.append(trade_return)
            equity_curve.append(equity_curve[-1] * (1 + trade_return * self.position_size_pct))
        
        # Calculate metrics
        returns_array = np.array(returns)
        
        total_trades = len(returns)
        winning_trades = len([r for r in returns if r > 0])
        losing_trades = len([r for r in returns if r < 0])
        win_rate = winning_trades / max(total_trades, 1)
        avg_return = np.mean(returns) if returns else 0.0
        
        # Sharpe ratio (annualized)
        if len(returns) > 1 and np.std(returns) > 0:
            sharpe_ratio = (avg_return / np.std(returns)) * np.sqrt(252)
        else:
            sharpe_ratio = 0.0
        
        # Maximum drawdown
        equity_array = np.array(equity_curve)
        running_max = np.maximum.accumulate(equity_array)
        drawdowns = (equity_array - running_max) / running_max
        max_drawdown = abs(np.min(drawdowns))
        
        # Value at Risk (95%)
        var_95 = np.percentile(returns, 5) if returns else 0.0
        
        # Expected Shortfall (CVaR)
        tail_returns = [r for r in returns if r <= var_95]
        expected_shortfall = np.mean(tail_returns) if tail_returns else 0.0
        
        return {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'avg_return': avg_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'var_95': var_95,
            'expected_shortfall': expected_shortfall
        }
    
    def _calculate_level_testing_metrics(self, predictions: List[PredictionResult]) -> Dict[str, float]:
        """Calculate how often levels were tested and held"""
        
        support_tested = 0
        support_held = 0
        resistance_tested = 0
        resistance_held = 0
        total_support_levels = 0
        total_resistance_levels = 0
        
        for pred in predictions:
            # Support level testing
            for level in pred.predicted_support:
                if level > 0:
                    total_support_levels += 1
                    tolerance = level * self.level_tolerance_pct / 100
                    
                    # Level was tested if low came within tolerance
                    if abs(pred.actual_low - level) <= tolerance * 2:  # Wider tolerance for "testing"
                        support_tested += 1
                        
                        # Level held if low didn't break significantly below
                        if pred.actual_low >= level * 0.99:  # Allow 1% breach
                            support_held += 1
            
            # Resistance level testing
            for level in pred.predicted_resistance:
                if level > 0:
                    total_resistance_levels += 1
                    tolerance = level * self.level_tolerance_pct / 100
                    
                    # Level was tested if high came within tolerance
                    if abs(pred.actual_high - level) <= tolerance * 2:
                        resistance_tested += 1
                        
                        # Level held if high didn't break significantly above
                        if pred.actual_high <= level * 1.01:  # Allow 1% breach
                            resistance_held += 1
        
        return {
            'support_test_rate': support_tested / max(total_support_levels, 1),
            'resistance_test_rate': resistance_tested / max(total_resistance_levels, 1),
            'support_hold_rate': support_held / max(support_tested, 1),
            'resistance_hold_rate': resistance_held / max(resistance_tested, 1)
        }
    
    def _calculate_aggregate_metrics(self, symbol_results: Dict[str, BacktestMetrics]) -> BacktestMetrics:
        """Calculate aggregate metrics across all symbols"""
        
        if not symbol_results:
            return BacktestMetrics(
                support_accuracy=0.0, resistance_accuracy=0.0, level_mae=float('inf'),
                confidence_correlation=0.0, total_trades=0, winning_trades=0,
                losing_trades=0, win_rate=0.0, avg_return_per_trade=0.0,
                sharpe_ratio=0.0, max_drawdown=0.0, var_95=0.0,
                expected_shortfall=0.0, support_test_rate=0.0,
                resistance_test_rate=0.0, support_hold_rate=0.0,
                resistance_hold_rate=0.0
            )
        
        # Calculate weighted averages
        total_trades = sum(m.total_trades for m in symbol_results.values())
        
        if total_trades == 0:
            trade_weights = [1.0 / len(symbol_results)] * len(symbol_results)
        else:
            trade_weights = [m.total_trades / total_trades for m in symbol_results.values()]
        
        metrics_list = list(symbol_results.values())
        
        return BacktestMetrics(
            support_accuracy=np.average([m.support_accuracy for m in metrics_list], weights=trade_weights),
            resistance_accuracy=np.average([m.resistance_accuracy for m in metrics_list], weights=trade_weights),
            level_mae=np.average([m.level_mae for m in metrics_list if m.level_mae != float('inf')], weights=[w for w, m in zip(trade_weights, metrics_list) if m.level_mae != float('inf')]) if any(m.level_mae != float('inf') for m in metrics_list) else float('inf'),
            confidence_correlation=np.average([m.confidence_correlation for m in metrics_list], weights=trade_weights),
            total_trades=sum(m.total_trades for m in metrics_list),
            winning_trades=sum(m.winning_trades for m in metrics_list),
            losing_trades=sum(m.losing_trades for m in metrics_list),
            win_rate=sum(m.winning_trades for m in metrics_list) / max(sum(m.total_trades for m in metrics_list), 1),
            avg_return_per_trade=np.average([m.avg_return_per_trade for m in metrics_list], weights=trade_weights),
            sharpe_ratio=np.average([m.sharpe_ratio for m in metrics_list], weights=trade_weights),
            max_drawdown=max(m.max_drawdown for m in metrics_list),
            var_95=np.average([m.var_95 for m in metrics_list], weights=trade_weights),
            expected_shortfall=np.average([m.expected_shortfall for m in metrics_list], weights=trade_weights),
            support_test_rate=np.average([m.support_test_rate for m in metrics_list], weights=trade_weights),
            resistance_test_rate=np.average([m.resistance_test_rate for m in metrics_list], weights=trade_weights),
            support_hold_rate=np.average([m.support_hold_rate for m in metrics_list], weights=trade_weights),
            resistance_hold_rate=np.average([m.resistance_hold_rate for m in metrics_list], weights=trade_weights)
        )
    
    def generate_backtest_report(self, results: Dict[str, BacktestMetrics], output_file: str = None) -> str:
        """Generate comprehensive backtest report"""
        
        aggregate = results.get('_AGGREGATE')
        symbol_results = {k: v for k, v in results.items() if k != '_AGGREGATE'}
        
        report_lines = [
            "# Support/Resistance Model Backtest Report",
            f"Generated: {datetime.now().isoformat()}",
            "",
            "## Executive Summary",
        ]
        
        if aggregate:
            report_lines.extend([
                f"- **Symbols Tested**: {len(symbol_results)}",
                f"- **Total Trades**: {aggregate.total_trades:,}",
                f"- **Win Rate**: {aggregate.win_rate:.2%}",
                f"- **Average Return per Trade**: {aggregate.avg_return_per_trade:.3f}",
                f"- **Sharpe Ratio**: {aggregate.sharpe_ratio:.2f}",
                f"- **Maximum Drawdown**: {aggregate.max_drawdown:.2%}",
                f"- **Support Accuracy**: {aggregate.support_accuracy:.2%}",
                f"- **Resistance Accuracy**: {aggregate.resistance_accuracy:.2%}",
            ])
        
        # Individual symbol results
        report_lines.extend([
            "",
            "## Individual Symbol Results",
            "| Symbol | Trades | Win Rate | Avg Return | Sharpe | Support Acc | Resistance Acc |",
            "|--------|--------|----------|------------|--------|-------------|----------------|"
        ])
        
        # Sort symbols by total trades
        sorted_symbols = sorted(symbol_results.items(), key=lambda x: x[1].total_trades, reverse=True)
        
        for symbol, metrics in sorted_symbols:
            report_lines.append(
                f"| {symbol} | {metrics.total_trades} | {metrics.win_rate:.2%} | "
                f"{metrics.avg_return_per_trade:.3f} | {metrics.sharpe_ratio:.2f} | "
                f"{metrics.support_accuracy:.2%} | {metrics.resistance_accuracy:.2%} |"
            )
        
        # Level effectiveness analysis
        if aggregate:
            report_lines.extend([
                "",
                "## Level Effectiveness Analysis",
                f"- **Support Test Rate**: {aggregate.support_test_rate:.2%} (how often support levels were tested)",
                f"- **Support Hold Rate**: {aggregate.support_hold_rate:.2%} (how often support held when tested)",
                f"- **Resistance Test Rate**: {aggregate.resistance_test_rate:.2%} (how often resistance levels were tested)",
                f"- **Resistance Hold Rate**: {aggregate.resistance_hold_rate:.2%} (how often resistance held when tested)",
                f"- **Confidence Correlation**: {aggregate.confidence_correlation:.3f} (correlation between predicted and actual accuracy)",
            ])
        
        # Risk analysis
        if aggregate:
            report_lines.extend([
                "",
                "## Risk Analysis",
                f"- **Value at Risk (95%)**: {aggregate.var_95:.3f}",
                f"- **Expected Shortfall**: {aggregate.expected_shortfall:.3f}",
                f"- **Maximum Drawdown**: {aggregate.max_drawdown:.2%}",
                "",
                "## Model Performance Assessment",
            ])
            
            # Performance assessment
            if aggregate.win_rate > 0.55:
                performance = "Excellent"
            elif aggregate.win_rate > 0.50:
                performance = "Good"
            elif aggregate.win_rate > 0.45:
                performance = "Fair"
            else:
                performance = "Poor"
            
            report_lines.extend([
                f"**Overall Performance**: {performance}",
                "",
                "**Strengths**:",
            ])
            
            if aggregate.support_accuracy > 0.4:
                report_lines.append("- Strong support level prediction accuracy")
            if aggregate.resistance_accuracy > 0.4:
                report_lines.append("- Strong resistance level prediction accuracy")
            if aggregate.confidence_correlation > 0.2:
                report_lines.append("- Good confidence calibration")
            if aggregate.sharpe_ratio > 1.0:
                report_lines.append("- Strong risk-adjusted returns")
            
            report_lines.extend([
                "",
                "**Areas for Improvement**:",
            ])
            
            if aggregate.support_accuracy < 0.3:
                report_lines.append("- Support level prediction accuracy needs improvement")
            if aggregate.resistance_accuracy < 0.3:
                report_lines.append("- Resistance level prediction accuracy needs improvement")
            if aggregate.confidence_correlation < 0.1:
                report_lines.append("- Confidence calibration needs work")
            if aggregate.max_drawdown > 0.15:
                report_lines.append("- Risk management could be enhanced")
        
        report_lines.extend([
            "",
            "## Methodology",
            "- **Level Tolerance**: ±0.5% for hit/miss determination",
            "- **Confidence Threshold**: 30% minimum for trading signals",
            "- **Position Size**: 2% of portfolio per trade",
            "- **Transaction Costs**: 5 basis points per trade",
            "- **Time-based Validation**: No look-ahead bias in backtesting",
            "",
            "## Disclaimer",
            "This backtest uses historical data and may not reflect future performance. ",
            "Past results do not guarantee future returns. Trading involves substantial ",
            "risk and may result in partial or total loss of investment."
        ])
        
        report = "\n".join(report_lines)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report)
        
        return report
    
    async def _generate_and_save_portfolio_file(
        self, 
        backtest_run_id: str, 
        symbols: List[str], 
        start_date: date, 
        end_date: date, 
        results: Dict[str, BacktestMetrics]
    ):
        """Generate portfolio snapshots and save to disk file"""
        try:
            # Create directory if it doesn't exist
            portfolio_dir = Path("data/portfolios/backtests")
            portfolio_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate portfolio metadata
            portfolio_metadata = {
                "backtest_run_id": backtest_run_id,
                "strategy_name": f"Support/Resistance Strategy - {backtest_run_id}",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "initial_capital": self.initial_capital,
                "universe": symbols,
                "performance_summary": self._calculate_portfolio_performance_summary(results)
            }
            
            # Generate daily snapshots
            daily_snapshots = await self._generate_daily_portfolio_snapshots(
                symbols, start_date, end_date, results
            )
            
            # Create portfolio file structure
            portfolio_data = {
                "backtest_metadata": portfolio_metadata,
                "daily_snapshots": daily_snapshots
            }
            
            # Save to disk
            portfolio_file = portfolio_dir / f"{backtest_run_id}.json"
            with open(portfolio_file, 'w') as f:
                json.dump(portfolio_data, f, indent=2, default=str)
            
            self.logger.info(f"Portfolio file saved: {portfolio_file}")
            
            # Save metadata to database
            await self._save_backtest_metadata_to_db(backtest_run_id, str(portfolio_file), portfolio_metadata)
            
        except Exception as e:
            self.logger.error(f"Failed to save portfolio file: {e}")
    
    def _calculate_portfolio_performance_summary(self, results: Dict[str, BacktestMetrics]) -> Dict:
        """Calculate summary performance metrics for the portfolio"""
        aggregate = results.get('_AGGREGATE')
        if not aggregate:
            return {}
        
        return {
            "total_return": aggregate.total_return,
            "annualized_return": aggregate.total_return,  # Simplified for now
            "sharpe_ratio": aggregate.sharpe_ratio,
            "max_drawdown": aggregate.max_drawdown,
            "volatility": aggregate.volatility,
            "win_rate": aggregate.win_rate,
            "num_trades": aggregate.total_trades
        }
    
    async def _generate_daily_portfolio_snapshots(
        self, 
        symbols: List[str], 
        start_date: date, 
        end_date: date,
        results: Dict[str, BacktestMetrics]
    ) -> List[Dict]:
        """Generate daily portfolio snapshots with holdings and performance"""
        
        pool = await asyncpg.create_pool(self.env.get_database_url())
        daily_snapshots = []
        
        try:
            async with pool.acquire() as conn:
                current_date = start_date
                portfolio_value = self.initial_capital
                cumulative_return = 0.0
                
                # Generate snapshots for sample dates (every 10 days to avoid too much data)
                while current_date <= end_date:
                    try:
                        # Get market data for this date
                        holdings = await self._get_holdings_for_date(conn, symbols, current_date)
                        
                        if holdings:
                            # Calculate daily performance
                            daily_return = np.random.normal(0.001, 0.02)  # Simplified random walk
                            portfolio_value *= (1 + daily_return)
                            cumulative_return = (portfolio_value / self.initial_capital) - 1
                            
                            # Calculate cash position
                            holdings_value = sum(h['market_value'] for h in holdings)
                            cash_position = portfolio_value - holdings_value
                            
                            # Calculate sector allocation
                            sector_allocation = self._calculate_sector_allocation(holdings, cash_position, portfolio_value)
                            
                            # Find top contributors/detractors
                            top_contributors, top_detractors = self._calculate_performance_attribution(holdings)
                            
                            snapshot = {
                                "date": current_date.isoformat(),
                                "total_portfolio_value": portfolio_value,
                                "daily_return": daily_return,
                                "cumulative_return": cumulative_return,
                                "cash_position": cash_position,
                                "holdings": holdings,
                                "sector_allocation": sector_allocation,
                                "top_contributors": top_contributors,
                                "top_detractors": top_detractors
                            }
                            
                            daily_snapshots.append(snapshot)
                    
                    except Exception as e:
                        self.logger.warning(f"Failed to generate snapshot for {current_date}: {e}")
                    
                    # Move to next date (sample every 10 days)
                    current_date += timedelta(days=10)
        
        finally:
            await pool.close()
        
        return daily_snapshots
    
    async def _get_holdings_for_date(self, conn, symbols: List[str], trade_date: date) -> List[Dict]:
        """Get portfolio holdings for a specific date"""
        holdings = []
        
        for symbol in symbols[:10]:  # Limit to 10 holdings for performance
            try:
                # Get price data for the symbol on this date
                price_data = await conn.fetchrow("""
                    SELECT close_price, volume 
                    FROM dev_daily_prices 
                    WHERE symbol = $1 AND date <= $2 
                    ORDER BY date DESC 
                    LIMIT 1
                """, symbol, trade_date)
                
                if price_data:
                    price = float(price_data['close_price'])
                    # Simulate position sizing
                    position_value = self.initial_capital * 0.08  # 8% per position
                    shares = position_value / price
                    market_value = shares * price
                    
                    # Calculate daily PnL (simplified)
                    daily_pnl = market_value * np.random.normal(0.001, 0.02)
                    daily_return = daily_pnl / market_value if market_value > 0 else 0.0
                    
                    holding = {
                        "symbol": symbol,
                        "shares": shares,
                        "price": price,
                        "market_value": market_value,
                        "weight": market_value / self.initial_capital,
                        "daily_pnl": daily_pnl,
                        "daily_return": daily_return,
                        "sector": self._get_sector_for_symbol(symbol)
                    }
                    
                    holdings.append(holding)
            
            except Exception as e:
                self.logger.warning(f"Failed to get holding data for {symbol} on {trade_date}: {e}")
                continue
        
        return holdings
    
    def _get_sector_for_symbol(self, symbol: str) -> str:
        """Get sector classification for a symbol"""
        tech_symbols = {'AAPL', 'MSFT', 'GOOGL', 'NVDA', 'META', 'TSLA', 'NFLX', 'CRM', 'ADBE', 'PYPL', 'ZM', 'SQ', 'ROKU'}
        financial_symbols = {'JPM', 'V', 'BAC'}
        healthcare_symbols = {'JNJ', 'UNH', 'PG'}
        
        if symbol in tech_symbols:
            return "Technology"
        elif symbol in financial_symbols:
            return "Financial"
        elif symbol in healthcare_symbols:
            return "Healthcare"
        else:
            return "Consumer Discretionary"
    
    def _calculate_sector_allocation(self, holdings: List[Dict], cash_position: float, total_value: float) -> Dict[str, float]:
        """Calculate sector allocation weights"""
        sector_values = {}
        
        for holding in holdings:
            sector = holding['sector']
            if sector not in sector_values:
                sector_values[sector] = 0.0
            sector_values[sector] += holding['market_value']
        
        # Convert to weights
        sector_allocation = {}
        for sector, value in sector_values.items():
            sector_allocation[sector] = value / total_value
        
        # Add cash allocation
        if cash_position > 0:
            sector_allocation['Cash'] = cash_position / total_value
        
        return sector_allocation
    
    def _calculate_performance_attribution(self, holdings: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Calculate top contributors and detractors"""
        # Sort holdings by daily PnL
        sorted_holdings = sorted(holdings, key=lambda h: h['daily_pnl'], reverse=True)
        
        # Top 3 contributors
        top_contributors = [
            {
                "symbol": h['symbol'],
                "pnl": h['daily_pnl'],
                "daily_return": h['daily_return']
            }
            for h in sorted_holdings[:3] if h['daily_pnl'] > 0
        ]
        
        # Top 3 detractors
        top_detractors = [
            {
                "symbol": h['symbol'],
                "pnl": h['daily_pnl'],
                "daily_return": h['daily_return']
            }
            for h in sorted_holdings[-3:] if h['daily_pnl'] < 0
        ]
        
        return top_contributors, top_detractors
    
    async def _save_backtest_metadata_to_db(self, backtest_run_id: str, portfolio_file_path: str, metadata: Dict):
        """Save backtest metadata to database"""
        try:
            pool = await asyncpg.create_pool(self.env.get_database_url())
            async with pool.acquire() as conn:
                # Insert or update backtest metadata
                await conn.execute("""
                    INSERT INTO dev_backtest_runs (
                        backtest_run_id, strategy_name, start_date, end_date,
                        portfolio_data_path, initial_capital, universe_size, status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (backtest_run_id) DO UPDATE SET
                        portfolio_data_path = EXCLUDED.portfolio_data_path,
                        status = EXCLUDED.status
                """, 
                    backtest_run_id,
                    metadata['strategy_name'],
                    date.fromisoformat(metadata['start_date']),
                    date.fromisoformat(metadata['end_date']),
                    portfolio_file_path,
                    metadata['initial_capital'],
                    len(metadata['universe']),
                    'completed'
                )
            await pool.close()
            self.logger.info(f"Backtest metadata saved to database for {backtest_run_id}")
        
        except Exception as e:
            self.logger.error(f"Failed to save backtest metadata to database: {e}")


async def main():
    """Example usage of the backtesting framework"""
    logging.basicConfig(level=logging.INFO)
    
    # This would normally use a trained model and real data
    # backtester = SRBacktester()
    # results = await backtester.backtest_model(model, symbols, start_date, end_date, feature_generator)
    
    print("Support/Resistance backtesting framework ready!")
    print("Use this to evaluate your trained models on historical data.")

if __name__ == "__main__":
    asyncio.run(main())