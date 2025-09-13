"""
Tests for portfolio evaluator with Runner framework integration.
"""

import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock
import asyncpg

from domains.ml.services.portfolio_evaluator import (
    PredictionRecord,
    PortfolioMetrics,
    EvaluationConfig,
    PortfolioEvaluator,
    evaluate_residual_return_strategy
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
def mock_universe_state_manager():
    """Mock universe state manager."""
    manager = Mock()

    # Default price data
    default_prices = pd.DataFrame({
        'high': [102, 104, 106, 108, 110],
        'low': [98, 100, 102, 104, 106],
        'close': [100, 102, 104, 106, 108]
    })

    manager.get_lag_prices.return_value = default_prices

    return manager


@pytest.fixture
def sample_evaluation_config():
    """Sample evaluation configuration."""
    return EvaluationConfig(
        evaluation_start_date=datetime(2024, 1, 1),
        evaluation_end_date=datetime(2024, 1, 31),
        prediction_horizons=[1, 2, 3],
        position_sizing_method='equal_weight',
        max_positions=10,
        min_confidence_threshold=0.6,
        transaction_cost_bps=10.0
    )


@pytest.fixture
def sample_predictions_df():
    """Sample predictions DataFrame."""
    dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')

    predictions = []
    for i, date in enumerate(dates):
        predictions.append({
            'instrument_id': 1,
            'date': date,
            'horizon': 1,
            'predicted_return': 0.01 + i * 0.002,
            'confidence': 0.7 + i * 0.02
        })
        predictions.append({
            'instrument_id': 2,
            'date': date,
            'horizon': 1,
            'predicted_return': -0.005 + i * 0.001,
            'confidence': 0.6 + i * 0.03
        })

    return pd.DataFrame(predictions)


class TestPredictionRecord:
    """Test PredictionRecord dataclass."""

    def test_prediction_record_creation(self):
        """Test PredictionRecord creation."""
        record = PredictionRecord(
            instrument_id=123,
            prediction_date=datetime(2024, 1, 15),
            prediction_horizon=2,
            predicted_residual_return=0.025,
            predicted_confidence=0.85,
            position_size=100.0,
            entry_price=105.50
        )

        assert record.instrument_id == 123
        assert record.prediction_date == datetime(2024, 1, 15)
        assert record.prediction_horizon == 2
        assert record.predicted_residual_return == 0.025
        assert record.predicted_confidence == 0.85
        assert record.position_size == 100.0
        assert record.entry_price == 105.50
        assert record.actual_residual_return is None
        assert record.exit_price is None
        assert record.realized_pnl is None


class TestPortfolioMetrics:
    """Test PortfolioMetrics dataclass."""

    def test_portfolio_metrics_creation(self):
        """Test PortfolioMetrics creation."""
        metrics = PortfolioMetrics(
            total_return=0.15,
            annualized_return=0.12,
            volatility=0.18,
            sharpe_ratio=0.67,
            max_drawdown=-0.08,
            win_rate=0.65,
            avg_win=0.025,
            avg_loss=-0.015,
            profit_factor=1.8,
            information_ratio=0.45,
            prediction_accuracy=0.72,
            prediction_mse=0.0004
        )

        assert metrics.total_return == 0.15
        assert metrics.sharpe_ratio == 0.67
        assert metrics.win_rate == 0.65
        assert metrics.prediction_accuracy == 0.72


class TestEvaluationConfig:
    """Test EvaluationConfig dataclass."""

    def test_evaluation_config_defaults(self):
        """Test EvaluationConfig with defaults."""
        config = EvaluationConfig(
            evaluation_start_date=datetime(2024, 1, 1),
            evaluation_end_date=datetime(2024, 1, 31)
        )

        assert config.prediction_horizons == [1, 2, 3, 4, 5]
        assert config.position_sizing_method == 'equal_weight'
        assert config.max_positions == 50
        assert config.min_confidence_threshold == 0.6
        assert config.transaction_cost_bps == 10.0
        assert config.benchmark_symbol == 'SPY'

    def test_evaluation_config_custom(self):
        """Test EvaluationConfig with custom values."""
        config = EvaluationConfig(
            evaluation_start_date=datetime(2024, 1, 1),
            evaluation_end_date=datetime(2024, 1, 31),
            prediction_horizons=[1, 3, 5],
            max_positions=20,
            min_confidence_threshold=0.7
        )

        assert config.prediction_horizons == [1, 3, 5]
        assert config.max_positions == 20
        assert config.min_confidence_threshold == 0.7


class TestPortfolioEvaluator:
    """Test PortfolioEvaluator functionality."""

    def test_evaluator_initialization(self, mock_connection_pool, mock_env, mock_universe_state_manager, sample_evaluation_config):
        """Test evaluator initialization."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager, sample_evaluation_config
        )

        assert evaluator.pool == pool
        assert evaluator.env == mock_env
        assert evaluator.universe_state_manager == mock_universe_state_manager
        assert evaluator.config == sample_evaluation_config

        # Check initialized components
        assert hasattr(evaluator, 'residual_calculator')
        assert hasattr(evaluator, 'data_generator')
        assert hasattr(evaluator, 'interpreter')

        # Check portfolio state
        assert evaluator.prediction_records == []
        assert evaluator.portfolio_history == []
        assert evaluator.current_positions == {}

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_evaluate_model_predictions_basic(self, mock_connection_pool, mock_env, mock_universe_state_manager, sample_predictions_df):
        """Test basic model prediction evaluation."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        # Mock portfolio update methods
        with patch.object(evaluator, '_process_daily_predictions') as mock_process, \
             patch.object(evaluator, '_update_portfolio') as mock_update, \
             patch.object(evaluator, '_record_portfolio_state') as mock_record:

            mock_process.return_value = None
            mock_update.return_value = None
            mock_record.return_value = None

            # Set up some portfolio values for metrics calculation
            evaluator.portfolio_values = [100000, 101000, 102000, 101500, 103000]
            evaluator.daily_returns = [0.01, 0.0099, -0.0049, 0.0148]
            evaluator.benchmark_returns = [0.005, 0.008, -0.002, 0.012]

            metrics = await evaluator.evaluate_model_predictions(sample_predictions_df)

            assert isinstance(metrics, PortfolioMetrics)
            assert isinstance(metrics.total_return, float)
            assert isinstance(metrics.sharpe_ratio, float)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_process_daily_predictions(self, mock_connection_pool, mock_env, mock_universe_state_manager, sample_predictions_df):
        """Test daily prediction processing."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        # Set portfolio value for position sizing
        evaluator.portfolio_values = [100000]

        with patch.object(evaluator, '_create_prediction_record') as mock_create:
            mock_create.return_value = PredictionRecord(
                instrument_id=1,
                prediction_date=datetime(2024, 1, 1),
                prediction_horizon=1,
                predicted_residual_return=0.01,
                predicted_confidence=0.7,
                position_size=1000.0,
                entry_price=100.0
            )

            await evaluator._process_daily_predictions(
                sample_predictions_df, datetime(2024, 1, 1)
            )

            assert len(evaluator.prediction_records) > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_create_prediction_record(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test prediction record creation."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        # Set portfolio value
        evaluator.portfolio_values = [100000]

        prediction_row = pd.Series({
            'instrument_id': 123,
            'horizon': 2,
            'predicted_return': 0.02,
            'confidence': 0.8
        })

        record = await evaluator._create_prediction_record(
            prediction_row, datetime(2024, 1, 15)
        )

        assert record is not None
        assert isinstance(record, PredictionRecord)
        assert record.instrument_id == 123
        assert record.prediction_horizon == 2
        assert record.predicted_residual_return == 0.02
        assert record.predicted_confidence == 0.8
        assert record.position_size > 0
        assert record.entry_price > 0

    def test_calculate_position_size_equal_weight(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test equal weight position sizing."""
        config = EvaluationConfig(
            evaluation_start_date=datetime(2024, 1, 1),
            evaluation_end_date=datetime(2024, 1, 31),
            position_sizing_method='equal_weight',
            max_positions=10
        )

        evaluator = PortfolioEvaluator(
            None, None, None, config
        )

        evaluator.portfolio_values = [100000]  # $100k portfolio

        prediction_row = pd.Series({'confidence': 0.8})
        entry_price = 100.0

        position_size = evaluator._calculate_position_size(
            prediction_row, entry_price, datetime(2024, 1, 15)
        )

        expected_shares = (100000 / 10) / 100.0  # $10k position / $100 price
        assert abs(position_size - expected_shares) < 1.0

    def test_calculate_position_size_confidence_weighted(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test confidence-weighted position sizing."""
        config = EvaluationConfig(
            evaluation_start_date=datetime(2024, 1, 1),
            evaluation_end_date=datetime(2024, 1, 31),
            position_sizing_method='confidence_weighted',
            max_positions=10
        )

        evaluator = PortfolioEvaluator(
            None, None, None, config
        )

        evaluator.portfolio_values = [100000]

        # High confidence prediction
        high_conf_row = pd.Series({'confidence': 0.9})
        high_conf_size = evaluator._calculate_position_size(
            high_conf_row, 100.0, datetime(2024, 1, 15)
        )

        # Low confidence prediction
        low_conf_row = pd.Series({'confidence': 0.6})
        low_conf_size = evaluator._calculate_position_size(
            low_conf_row, 100.0, datetime(2024, 1, 15)
        )

        # High confidence should get larger position
        assert high_conf_size > low_conf_size

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_update_portfolio(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test portfolio update process."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        with patch.object(evaluator, '_close_expired_positions') as mock_close, \
             patch.object(evaluator, '_open_new_positions') as mock_open, \
             patch.object(evaluator, '_calculate_portfolio_value') as mock_calc, \
             patch.object(evaluator, '_get_benchmark_return') as mock_bench:

            mock_close.return_value = None
            mock_open.return_value = None
            mock_calc.return_value = 101000.0
            mock_bench.return_value = 0.005

            evaluator.portfolio_values = [100000]

            await evaluator._update_portfolio(datetime(2024, 1, 15))

            assert len(evaluator.portfolio_values) == 2
            assert evaluator.portfolio_values[-1] == 101000.0
            assert len(evaluator.daily_returns) == 1
            assert len(evaluator.benchmark_returns) == 1

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_close_expired_positions(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test closing expired positions."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        # Add expired position
        expired_position = PredictionRecord(
            instrument_id=123,
            prediction_date=datetime(2024, 1, 10),  # 5 days ago
            prediction_horizon=2,
            predicted_residual_return=0.01,
            predicted_confidence=0.7,
            position_size=100.0,
            entry_price=100.0
        )

        evaluator.current_positions[123] = expired_position

        with patch.object(evaluator, '_get_actual_residual_return') as mock_residual:
            mock_residual.return_value = 0.015

            await evaluator._close_expired_positions(datetime(2024, 1, 15))

            # Position should be closed
            assert 123 not in evaluator.current_positions
            # P&L should be calculated
            assert expired_position.exit_price is not None
            assert expired_position.realized_pnl is not None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_open_new_positions(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test opening new positions."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        # Add new prediction
        new_prediction = PredictionRecord(
            instrument_id=456,
            prediction_date=datetime(2024, 1, 15),
            prediction_horizon=1,
            predicted_residual_return=0.02,
            predicted_confidence=0.8,
            position_size=150.0,
            entry_price=105.0
        )

        evaluator.prediction_records = [new_prediction]

        await evaluator._open_new_positions(datetime(2024, 1, 15))

        # Position should be added
        assert 456 in evaluator.current_positions
        assert evaluator.current_positions[456] == new_prediction

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_calculate_portfolio_value(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test portfolio value calculation."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        evaluator.portfolio_values = [100000]

        # Add current position
        position = PredictionRecord(
            instrument_id=123,
            prediction_date=datetime(2024, 1, 10),
            prediction_horizon=2,
            predicted_residual_return=0.01,
            predicted_confidence=0.7,
            position_size=100.0,
            entry_price=100.0
        )

        evaluator.current_positions[123] = position

        # Mock current price higher than entry
        current_prices = pd.DataFrame({'close': [105.0]})
        mock_universe_state_manager.get_lag_prices.return_value = current_prices

        portfolio_value = await evaluator._calculate_portfolio_value(datetime(2024, 1, 15))

        # Should be initial cash + position value
        expected_value = 100000 - (100.0 * 100.0) + (105.0 * 100.0)  # Gain of $500
        assert abs(portfolio_value - expected_value) < 100  # Allow small tolerance

    def test_calculate_portfolio_metrics(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test portfolio metrics calculation."""
        evaluator = PortfolioEvaluator(
            None, None, None
        )

        # Set up sample returns
        evaluator.daily_returns = [0.01, -0.005, 0.015, -0.008, 0.012]
        evaluator.portfolio_values = [100000, 101000, 100495, 101007, 100199, 101404]
        evaluator.benchmark_returns = [0.005, -0.002, 0.01, -0.004, 0.008]

        # Add some prediction records for accuracy calculation
        evaluator.prediction_records = [
            PredictionRecord(123, datetime(2024, 1, 1), 1, 0.01, 0.8, actual_residual_return=0.015),
            PredictionRecord(124, datetime(2024, 1, 2), 1, -0.005, 0.7, actual_residual_return=-0.003),
            PredictionRecord(125, datetime(2024, 1, 3), 1, 0.008, 0.9, actual_residual_return=-0.002),
        ]

        metrics = evaluator._calculate_portfolio_metrics()

        assert isinstance(metrics, PortfolioMetrics)
        assert metrics.total_return != 0
        assert metrics.volatility > 0
        assert 0 <= metrics.prediction_accuracy <= 1
        assert metrics.prediction_mse >= 0

    def test_calculate_prediction_metrics(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test prediction accuracy metrics calculation."""
        evaluator = PortfolioEvaluator(
            None, None, None
        )

        # Predictions with actual outcomes
        evaluator.prediction_records = [
            PredictionRecord(1, datetime(2024, 1, 1), 1, 0.01, 0.8, actual_residual_return=0.015),   # Correct direction
            PredictionRecord(2, datetime(2024, 1, 2), 1, -0.005, 0.7, actual_residual_return=-0.003), # Correct direction
            PredictionRecord(3, datetime(2024, 1, 3), 1, 0.008, 0.9, actual_residual_return=-0.002),  # Wrong direction
        ]

        accuracy, mse = evaluator._calculate_prediction_metrics()

        assert 0 <= accuracy <= 1
        assert accuracy == 2/3  # 2 out of 3 correct directions
        assert mse > 0

    def test_generate_evaluation_report(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test evaluation report generation."""
        config = EvaluationConfig(
            evaluation_start_date=datetime(2024, 1, 1),
            evaluation_end_date=datetime(2024, 1, 31),
            max_positions=20,
            min_confidence_threshold=0.7
        )

        evaluator = PortfolioEvaluator(
            None, None, None, config
        )

        # Sample metrics
        metrics = PortfolioMetrics(
            total_return=0.15,
            annualized_return=0.12,
            volatility=0.18,
            sharpe_ratio=0.67,
            max_drawdown=-0.08,
            win_rate=0.65,
            avg_win=0.025,
            avg_loss=-0.015,
            profit_factor=1.8,
            information_ratio=0.45,
            prediction_accuracy=0.72,
            prediction_mse=0.0004
        )

        evaluator.daily_returns = [0.01] * 20
        evaluator.prediction_records = [Mock(predicted_confidence=0.8)] * 50

        report = evaluator.generate_evaluation_report(metrics)

        assert isinstance(report, dict)
        assert 'evaluation_period' in report
        assert 'performance_metrics' in report
        assert 'trading_statistics' in report
        assert 'prediction_quality' in report
        assert 'configuration' in report

        # Check formatting
        assert '15.00%' in report['performance_metrics']['total_return']
        assert '0.67' in report['performance_metrics']['sharpe_ratio']


class TestConvenienceFunction:
    """Test convenience function."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_evaluate_residual_return_strategy(self, mock_connection_pool, mock_env, mock_universe_state_manager, sample_predictions_df):
        """Test convenience function."""
        pool, conn = mock_connection_pool

        with patch('modeling.portfolio_evaluator.PortfolioEvaluator') as mock_class:
            mock_evaluator = Mock()

            mock_metrics = PortfolioMetrics(
                total_return=0.12, annualized_return=0.10, volatility=0.15,
                sharpe_ratio=0.67, max_drawdown=-0.05, win_rate=0.68,
                avg_win=0.02, avg_loss=-0.012, profit_factor=1.9,
                information_ratio=0.5, prediction_accuracy=0.75, prediction_mse=0.0003
            )

            mock_evaluator.evaluate_model_predictions = AsyncMock(return_value=mock_metrics)
            mock_evaluator.generate_evaluation_report.return_value = {'test': 'report'}
            mock_class.return_value = mock_evaluator

            metrics, report = await evaluate_residual_return_strategy(
                pool, mock_env, mock_universe_state_manager, sample_predictions_df
            )

            assert isinstance(metrics, PortfolioMetrics)
            assert isinstance(report, dict)
            assert metrics.total_return == 0.12


class TestErrorHandlingAndEdgeCases:
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_evaluate_with_no_predictions(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test evaluation with no predictions."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        empty_predictions = pd.DataFrame()

        metrics = await evaluator.evaluate_model_predictions(empty_predictions)

        # Should return default metrics
        assert isinstance(metrics, PortfolioMetrics)
        assert metrics.total_return == 0.0
        assert metrics.prediction_accuracy == 0.0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_create_prediction_record_no_price_data(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test prediction record creation with no price data."""
        pool, conn = mock_connection_pool

        mock_universe_state_manager.get_lag_prices.return_value = pd.DataFrame()

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        evaluator.portfolio_values = [100000]

        prediction_row = pd.Series({
            'instrument_id': 123,
            'predicted_return': 0.01,
            'confidence': 0.8
        })

        record = await evaluator._create_prediction_record(
            prediction_row, datetime(2024, 1, 15)
        )

        assert record is None

    def test_calculate_portfolio_metrics_empty_returns(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test metrics calculation with empty returns."""
        evaluator = PortfolioEvaluator(
            None, None, None
        )

        # Empty returns
        evaluator.daily_returns = []
        evaluator.portfolio_values = []
        evaluator.prediction_records = []

        metrics = evaluator._calculate_portfolio_metrics()

        assert isinstance(metrics, PortfolioMetrics)
        assert metrics.total_return == 0.0
        assert metrics.volatility == 0.0
        assert metrics.sharpe_ratio == 0.0

    def test_calculate_position_size_zero_portfolio_value(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test position sizing with zero portfolio value."""
        evaluator = PortfolioEvaluator(
            None, None, None
        )

        evaluator.portfolio_values = [0]  # Zero portfolio value

        prediction_row = pd.Series({'confidence': 0.8})

        position_size = evaluator._calculate_position_size(
            prediction_row, 100.0, datetime(2024, 1, 15)
        )

        assert position_size == 0.0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_actual_residual_return_error(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test handling of errors in residual return calculation."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        # Mock residual calculator to raise error
        with patch.object(evaluator.residual_calculator, 'calculate_residual_returns') as mock_calc:
            mock_calc.side_effect = Exception("Database error")

            residual = await evaluator._get_actual_residual_return(
                123, datetime(2024, 1, 1), datetime(2024, 1, 2)
            )

            assert residual is None

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_backtest_strategy_basic(self, mock_connection_pool, mock_env, mock_universe_state_manager):
        """Test basic strategy backtesting."""
        pool, conn = mock_connection_pool

        evaluator = PortfolioEvaluator(
            pool, mock_env, mock_universe_state_manager
        )

        # Mock strategy and feature functions
        def mock_strategy_function(features_df):
            return pd.DataFrame({
                'instrument_id': [1, 2],
                'predicted_return': [0.01, -0.005],
                'confidence': [0.8, 0.7]
            })

        async def mock_feature_generator(current_date):
            return pd.DataFrame({
                'instrument_id': [1, 2],
                'feature1': [0.5, 0.3],
                'feature2': [1.2, 0.8]
            })

        with patch.object(evaluator, '_process_daily_predictions') as mock_process, \
             patch.object(evaluator, '_update_portfolio') as mock_update, \
             patch.object(evaluator, '_record_portfolio_state') as mock_record:

            mock_process.return_value = None
            mock_update.return_value = None
            mock_record.return_value = None

            evaluator.portfolio_values = [100000, 101000]
            evaluator.daily_returns = [0.01]

            metrics = await evaluator.backtest_strategy(
                mock_strategy_function,
                mock_feature_generator,
                datetime(2024, 1, 1),
                datetime(2024, 1, 2)
            )

            assert isinstance(metrics, PortfolioMetrics)


if __name__ == "__main__":
    pytest.main([__file__])