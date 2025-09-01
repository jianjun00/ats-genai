"""
Portfolio Evaluator with Runner Framework Integration.
Evaluates model predictions against actual portfolio performance using the Runner framework.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging
import asyncio
import asyncpg

from state.universe_state_manager import UniverseStateManager
from modeling.factor_models import ResidualReturnCalculator
from ml.training_data.generators.training_data_generator import ResidualReturnTrainingDataGenerator
from modeling.interpretability_framework import ResidualReturnInterpreter

logger = logging.getLogger(__name__)


@dataclass
class PredictionRecord:
    """Single prediction record for evaluation."""
    instrument_id: int
    prediction_date: datetime
    prediction_horizon: int
    predicted_residual_return: float
    predicted_confidence: float
    actual_residual_return: Optional[float] = None
    position_size: float = 0.0
    entry_price: float = 0.0
    exit_price: Optional[float] = None
    realized_pnl: Optional[float] = None


@dataclass
class PortfolioMetrics:
    """Portfolio performance metrics."""
    total_return: float
    annualized_return: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    information_ratio: float
    prediction_accuracy: float
    prediction_mse: float


@dataclass
class EvaluationConfig:
    """Configuration for portfolio evaluation."""
    evaluation_start_date: datetime
    evaluation_end_date: datetime
    prediction_horizons: List[int] = None
    position_sizing_method: str = 'equal_weight'  # 'equal_weight', 'volatility_adjusted', 'confidence_weighted'
    max_positions: int = 50
    min_confidence_threshold: float = 0.6
    rebalance_frequency: str = 'daily'  # 'daily', 'weekly', 'monthly'
    transaction_cost_bps: float = 10.0  # Transaction costs in basis points
    benchmark_symbol: str = 'SPY'
    
    def __post_init__(self):
        if self.prediction_horizons is None:
            self.prediction_horizons = [1, 2, 3, 4, 5]


class PortfolioEvaluator:
    """Evaluate residual return predictions through portfolio simulation."""
    
    def __init__(self,
                 connection_pool: asyncpg.Pool,
                 env,
                 universe_state_manager: UniverseStateManager,
                 config: Optional[EvaluationConfig] = None):
        self.pool = connection_pool
        self.env = env
        self.universe_state_manager = universe_state_manager
        self.config = config or EvaluationConfig(
            evaluation_start_date=datetime(2024, 1, 1),
            evaluation_end_date=datetime(2024, 12, 31)
        )
        
        # Initialize components
        self.residual_calculator = ResidualReturnCalculator(connection_pool, env)
        self.data_generator = ResidualReturnTrainingDataGenerator(
            connection_pool, env, universe_state_manager
        )
        self.interpreter = ResidualReturnInterpreter()
        
        # Portfolio state
        self.prediction_records: List[PredictionRecord] = []
        self.portfolio_history: List[Dict[str, Any]] = []
        self.current_positions: Dict[int, PredictionRecord] = {}
        
        # Performance tracking
        self.daily_returns: List[float] = []
        self.portfolio_values: List[float] = []
        self.benchmark_returns: List[float] = []
    
    async def evaluate_model_predictions(self,
                                       prediction_df: pd.DataFrame,
                                       model_name: str = "residual_return_model") -> PortfolioMetrics:
        """
        Evaluate model predictions through portfolio simulation.
        
        Args:
            prediction_df: DataFrame with predictions (instrument_id, date, predicted_return, confidence)
            model_name: Name of the model being evaluated
            
        Returns:
            PortfolioMetrics with comprehensive evaluation results
        """
        logger.info(f"Evaluating {model_name} predictions from {self.config.evaluation_start_date} to {self.config.evaluation_end_date}")
        
        # Initialize portfolio state
        self._initialize_portfolio()
        
        # Process predictions day by day
        current_date = self.config.evaluation_start_date
        
        while current_date <= self.config.evaluation_end_date:
            # Skip weekends
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue
            
            try:
                # Process predictions for current date
                await self._process_daily_predictions(prediction_df, current_date)
                
                # Update portfolio positions and calculate returns
                await self._update_portfolio(current_date)
                
                # Record portfolio state
                self._record_portfolio_state(current_date)
                
            except Exception as e:
                logger.warning(f"Failed to process date {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        # Calculate final metrics
        metrics = self._calculate_portfolio_metrics()
        
        logger.info(f"Evaluation complete. Total return: {metrics.total_return:.2%}, Sharpe: {metrics.sharpe_ratio:.2f}")
        
        return metrics
    
    async def backtest_strategy(self,
                              strategy_function,
                              feature_generator_function,
                              start_date: datetime,
                              end_date: datetime) -> PortfolioMetrics:
        """
        Backtest a complete residual return strategy.
        
        Args:
            strategy_function: Function that generates predictions from features
            feature_generator_function: Function that generates features for prediction
            start_date: Backtest start date
            end_date: Backtest end date
            
        Returns:
            PortfolioMetrics from backtest
        """
        logger.info(f"Running strategy backtest from {start_date} to {end_date}")
        
        # Initialize
        self._initialize_portfolio()
        current_date = start_date
        
        while current_date <= end_date:
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue
            
            try:
                # Generate features for current date
                features_df = await feature_generator_function(current_date)
                
                if not features_df.empty:
                    # Generate predictions
                    predictions_df = strategy_function(features_df)
                    
                    # Process predictions
                    await self._process_daily_predictions(predictions_df, current_date)
                
                # Update portfolio
                await self._update_portfolio(current_date)
                self._record_portfolio_state(current_date)
                
            except Exception as e:
                logger.warning(f"Failed to process backtest date {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        return self._calculate_portfolio_metrics()
    
    def _initialize_portfolio(self):
        """Initialize portfolio state."""
        self.prediction_records = []
        self.portfolio_history = []
        self.current_positions = {}
        self.daily_returns = []
        self.portfolio_values = [100000.0]  # Start with $100k
        self.benchmark_returns = []
    
    async def _process_daily_predictions(self,
                                       prediction_df: pd.DataFrame,
                                       current_date: datetime):
        """Process predictions for a specific date."""
        # Filter predictions for current date
        daily_predictions = prediction_df[
            pd.to_datetime(prediction_df['date']).dt.date == current_date.date()
        ].copy()
        
        if daily_predictions.empty:
            return
        
        # Filter by confidence threshold
        high_confidence_predictions = daily_predictions[
            daily_predictions['confidence'] >= self.config.min_confidence_threshold
        ]
        
        # Select top predictions based on position limits
        if len(high_confidence_predictions) > self.config.max_positions:
            # Sort by confidence * predicted_return (risk-adjusted signal)
            high_confidence_predictions['signal_strength'] = (
                high_confidence_predictions['confidence'] * 
                abs(high_confidence_predictions['predicted_return'])
            )
            high_confidence_predictions = high_confidence_predictions.nlargest(
                self.config.max_positions, 'signal_strength'
            )
        
        # Create prediction records
        for _, row in high_confidence_predictions.iterrows():
            prediction_record = await self._create_prediction_record(row, current_date)
            if prediction_record:
                self.prediction_records.append(prediction_record)
    
    async def _create_prediction_record(self,
                                      prediction_row: pd.Series,
                                      current_date: datetime) -> Optional[PredictionRecord]:
        """Create a prediction record from a prediction row."""
        try:
            instrument_id = int(prediction_row['instrument_id'])
            
            # Get current price for entry
            current_prices = self.universe_state_manager.get_lag_prices(
                instrument_id, current_date, 1
            )
            
            if current_prices.empty:
                return None
            
            entry_price = current_prices['close'].iloc[-1] if 'close' in current_prices.columns else current_prices['high'].iloc[-1]
            
            # Calculate position size
            position_size = self._calculate_position_size(
                prediction_row, entry_price, current_date
            )
            
            record = PredictionRecord(
                instrument_id=instrument_id,
                prediction_date=current_date,
                prediction_horizon=prediction_row.get('horizon', 1),
                predicted_residual_return=prediction_row['predicted_return'],
                predicted_confidence=prediction_row['confidence'],
                position_size=position_size,
                entry_price=entry_price
            )
            
            return record
            
        except Exception as e:
            logger.warning(f"Failed to create prediction record: {e}")
            return None
    
    def _calculate_position_size(self,
                               prediction_row: pd.Series,
                               entry_price: float,
                               current_date: datetime) -> float:
        """Calculate position size based on sizing method."""
        current_portfolio_value = self.portfolio_values[-1]
        
        if self.config.position_sizing_method == 'equal_weight':
            # Equal weight across all positions
            target_weight = 1.0 / self.config.max_positions
            dollar_amount = current_portfolio_value * target_weight
            return dollar_amount / entry_price
            
        elif self.config.position_sizing_method == 'volatility_adjusted':
            # Adjust position size by volatility
            base_weight = 1.0 / self.config.max_positions
            # This would require volatility calculation
            volatility_adj = 1.0  # Simplified
            dollar_amount = current_portfolio_value * base_weight * volatility_adj
            return dollar_amount / entry_price
            
        elif self.config.position_sizing_method == 'confidence_weighted':
            # Weight by prediction confidence
            confidence = prediction_row['confidence']
            base_weight = 1.0 / self.config.max_positions
            confidence_weight = confidence / 0.8  # Normalize to 80% confidence
            dollar_amount = current_portfolio_value * base_weight * confidence_weight
            return dollar_amount / entry_price
        
        else:
            # Default to equal weight
            target_weight = 1.0 / self.config.max_positions
            dollar_amount = current_portfolio_value * target_weight
            return dollar_amount / entry_price
    
    async def _update_portfolio(self, current_date: datetime):
        """Update portfolio positions and calculate returns."""
        # Close expired positions
        await self._close_expired_positions(current_date)
        
        # Open new positions
        await self._open_new_positions(current_date)
        
        # Calculate daily portfolio value and return
        portfolio_value = await self._calculate_portfolio_value(current_date)
        
        if self.portfolio_values:
            daily_return = (portfolio_value / self.portfolio_values[-1]) - 1
            self.daily_returns.append(daily_return)
        
        self.portfolio_values.append(portfolio_value)
        
        # Get benchmark return
        benchmark_return = await self._get_benchmark_return(current_date)
        self.benchmark_returns.append(benchmark_return)
    
    async def _close_expired_positions(self, current_date: datetime):
        """Close positions that have reached their prediction horizon."""
        expired_positions = []
        
        for instrument_id, position in self.current_positions.items():
            days_held = (current_date - position.prediction_date).days
            
            if days_held >= position.prediction_horizon:
                # Get exit price
                exit_prices = self.universe_state_manager.get_lag_prices(
                    instrument_id, current_date, 1
                )
                
                if not exit_prices.empty:
                    exit_price = exit_prices['close'].iloc[-1] if 'close' in exit_prices.columns else exit_prices['high'].iloc[-1]
                    
                    # Calculate realized P&L
                    position.exit_price = exit_price
                    position.realized_pnl = (exit_price - position.entry_price) * position.position_size
                    
                    # Get actual residual return for evaluation
                    actual_residual = await self._get_actual_residual_return(
                        instrument_id, position.prediction_date, current_date
                    )
                    position.actual_residual_return = actual_residual
                    
                    expired_positions.append(instrument_id)
        
        # Remove expired positions
        for instrument_id in expired_positions:
            del self.current_positions[instrument_id]
    
    async def _open_new_positions(self, current_date: datetime):
        """Open new positions from recent predictions."""
        # Find new predictions for current date
        new_predictions = [
            record for record in self.prediction_records
            if (record.prediction_date == current_date and 
                record.instrument_id not in self.current_positions)
        ]
        
        # Add to current positions
        for prediction in new_predictions:
            self.current_positions[prediction.instrument_id] = prediction
    
    async def _calculate_portfolio_value(self, current_date: datetime) -> float:
        """Calculate current portfolio value."""
        total_value = 0.0
        
        # Cash component (simplified - assume rest is cash)
        cash_value = self.portfolio_values[-1] if self.portfolio_values else 100000.0
        
        # Position values
        for instrument_id, position in self.current_positions.items():
            current_prices = self.universe_state_manager.get_lag_prices(
                instrument_id, current_date, 1
            )
            
            if not current_prices.empty:
                current_price = current_prices['close'].iloc[-1] if 'close' in current_prices.columns else current_prices['high'].iloc[-1]
                position_value = current_price * position.position_size
                total_value += position_value
                
                # Subtract from cash
                cash_value -= position.entry_price * position.position_size
        
        return cash_value + total_value
    
    async def _get_actual_residual_return(self,
                                        instrument_id: int,
                                        start_date: datetime,
                                        end_date: datetime) -> Optional[float]:
        """Get actual residual return for comparison."""
        try:
            residual_returns = await self.residual_calculator.calculate_residual_returns(
                [instrument_id], start_date, end_date, 'multi_factor'
            )
            
            if not residual_returns.empty:
                instrument_returns = residual_returns[
                    residual_returns['instrument_id'] == instrument_id
                ]
                
                if not instrument_returns.empty:
                    return instrument_returns['residual_return'].iloc[-1]
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to get actual residual return: {e}")
            return None
    
    async def _get_benchmark_return(self, current_date: datetime) -> float:
        """Get benchmark return for current date."""
        # This would typically use SPY or another benchmark
        # For now, return a simple market return estimate
        return np.random.normal(0.0005, 0.01)  # Simplified benchmark
    
    def _record_portfolio_state(self, current_date: datetime):
        """Record current portfolio state for analysis."""
        state = {
            'date': current_date,
            'portfolio_value': self.portfolio_values[-1],
            'num_positions': len(self.current_positions),
            'daily_return': self.daily_returns[-1] if self.daily_returns else 0.0,
            'benchmark_return': self.benchmark_returns[-1] if self.benchmark_returns else 0.0
        }
        
        self.portfolio_history.append(state)
    
    def _calculate_portfolio_metrics(self) -> PortfolioMetrics:
        """Calculate comprehensive portfolio performance metrics."""
        if not self.daily_returns:
            return PortfolioMetrics(
                total_return=0.0, annualized_return=0.0, volatility=0.0,
                sharpe_ratio=0.0, max_drawdown=0.0, win_rate=0.0,
                avg_win=0.0, avg_loss=0.0, profit_factor=0.0,
                information_ratio=0.0, prediction_accuracy=0.0, prediction_mse=0.0
            )
        
        returns = np.array(self.daily_returns)
        portfolio_values = np.array(self.portfolio_values)
        
        # Basic return metrics
        total_return = (portfolio_values[-1] / portfolio_values[0]) - 1
        annualized_return = (1 + total_return) ** (252 / len(returns)) - 1
        volatility = returns.std() * np.sqrt(252)
        
        # Risk-adjusted metrics
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0
        
        # Maximum drawdown
        peak = np.maximum.accumulate(portfolio_values)
        drawdown = (portfolio_values - peak) / peak
        max_drawdown = drawdown.min()
        
        # Win/loss statistics
        positive_returns = returns[returns > 0]
        negative_returns = returns[returns < 0]
        
        win_rate = len(positive_returns) / len(returns) if len(returns) > 0 else 0
        avg_win = positive_returns.mean() if len(positive_returns) > 0 else 0
        avg_loss = negative_returns.mean() if len(negative_returns) > 0 else 0
        
        profit_factor = (positive_returns.sum() / abs(negative_returns.sum()) 
                        if len(negative_returns) > 0 and negative_returns.sum() != 0 else 0)
        
        # Information ratio vs benchmark
        if self.benchmark_returns:
            excess_returns = returns - np.array(self.benchmark_returns[:len(returns)])
            information_ratio = (excess_returns.mean() / excess_returns.std() * np.sqrt(252) 
                               if excess_returns.std() > 0 else 0)
        else:
            information_ratio = 0
        
        # Prediction accuracy metrics
        prediction_accuracy, prediction_mse = self._calculate_prediction_metrics()
        
        return PortfolioMetrics(
            total_return=total_return,
            annualized_return=annualized_return,
            volatility=volatility,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
            information_ratio=information_ratio,
            prediction_accuracy=prediction_accuracy,
            prediction_mse=prediction_mse
        )
    
    def _calculate_prediction_metrics(self) -> Tuple[float, float]:
        """Calculate prediction accuracy and MSE."""
        predictions_with_actuals = [
            record for record in self.prediction_records
            if record.actual_residual_return is not None
        ]
        
        if not predictions_with_actuals:
            return 0.0, 0.0
        
        predicted = np.array([r.predicted_residual_return for r in predictions_with_actuals])
        actual = np.array([r.actual_residual_return for r in predictions_with_actuals])
        
        # Direction accuracy
        predicted_direction = np.sign(predicted)
        actual_direction = np.sign(actual)
        accuracy = np.mean(predicted_direction == actual_direction)
        
        # Mean squared error
        mse = np.mean((predicted - actual) ** 2)
        
        return accuracy, mse
    
    def generate_evaluation_report(self, metrics: PortfolioMetrics) -> Dict[str, Any]:
        """Generate comprehensive evaluation report."""
        report = {
            'evaluation_period': {
                'start_date': self.config.evaluation_start_date.isoformat(),
                'end_date': self.config.evaluation_end_date.isoformat(),
                'total_days': len(self.daily_returns)
            },
            'performance_metrics': {
                'total_return': f"{metrics.total_return:.2%}",
                'annualized_return': f"{metrics.annualized_return:.2%}",
                'volatility': f"{metrics.volatility:.2%}",
                'sharpe_ratio': f"{metrics.sharpe_ratio:.2f}",
                'max_drawdown': f"{metrics.max_drawdown:.2%}",
                'information_ratio': f"{metrics.information_ratio:.2f}"
            },
            'trading_statistics': {
                'win_rate': f"{metrics.win_rate:.2%}",
                'average_win': f"{metrics.avg_win:.2%}",
                'average_loss': f"{metrics.avg_loss:.2%}",
                'profit_factor': f"{metrics.profit_factor:.2f}",
                'total_trades': len(self.prediction_records)
            },
            'prediction_quality': {
                'direction_accuracy': f"{metrics.prediction_accuracy:.2%}",
                'prediction_mse': f"{metrics.prediction_mse:.4f}",
                'avg_confidence': f"{np.mean([r.predicted_confidence for r in self.prediction_records]):.2f}"
            },
            'configuration': {
                'max_positions': self.config.max_positions,
                'min_confidence_threshold': self.config.min_confidence_threshold,
                'position_sizing_method': self.config.position_sizing_method,
                'transaction_cost_bps': self.config.transaction_cost_bps
            }
        }
        
        return report


# Convenience function for portfolio evaluation
async def evaluate_residual_return_strategy(
    connection_pool: asyncpg.Pool,
    env,
    universe_state_manager: UniverseStateManager,
    prediction_df: pd.DataFrame,
    config: Optional[EvaluationConfig] = None
) -> Tuple[PortfolioMetrics, Dict[str, Any]]:
    """
    Convenience function to evaluate a residual return strategy.
    
    Returns:
        Tuple of (PortfolioMetrics, evaluation_report)
    """
    evaluator = PortfolioEvaluator(connection_pool, env, universe_state_manager, config)
    
    metrics = await evaluator.evaluate_model_predictions(prediction_df)
    report = evaluator.generate_evaluation_report(metrics)
    
    return metrics, report