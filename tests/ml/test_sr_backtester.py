"""
Tests for Support/Resistance Backtesting Framework
"""

import pytest
import asyncio
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import asdict

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from domains.ml.services.evaluation.sr_backtester import (
    SRBacktester,
    PredictionResult,
    TradingSignal,
    BacktestMetrics
)
from shared.utils.environment import Environment

class TestPredictionResult:
    """Test suite for PredictionResult data structure"""

    def test_prediction_result_creation(self):
        """Test creating PredictionResult objects"""
        result = PredictionResult(
            symbol='AAPL',
            date=date(2023, 6, 15),
            predicted_support=[180.0, 175.0],
            predicted_resistance=[190.0, 195.0],
            support_confidence=[0.7, 0.5],
            resistance_confidence=[0.6, 0.4],
            actual_low=178.5,
            actual_high=188.2,
            actual_close=185.0
        )

        assert result.symbol == 'AAPL'
        assert result.date == date(2023, 6, 15)
        assert len(result.predicted_support) == 2
        assert len(result.predicted_resistance) == 2
        assert len(result.support_confidence) == 2
        assert len(result.resistance_confidence) == 2
        assert result.actual_high > result.actual_low

    def test_prediction_result_validation(self):
        """Test validation of PredictionResult values"""
        result = PredictionResult(
            symbol='TEST',
            date=date(2023, 1, 1),
            predicted_support=[95.0],
            predicted_resistance=[105.0],
            support_confidence=[0.8],
            resistance_confidence=[0.6],
            actual_low=94.5,
            actual_high=106.2,
            actual_close=100.0
        )

        assert len(result.predicted_support) == len(result.support_confidence)
        assert len(result.predicted_resistance) == len(result.resistance_confidence)
        assert all(0 <= conf <= 1 for conf in result.support_confidence)
        assert all(0 <= conf <= 1 for conf in result.resistance_confidence)

class TestTradingSignal:
    """Test suite for TradingSignal data structure"""

    def test_trading_signal_creation(self):
        """Test creating TradingSignal objects"""
        signal = TradingSignal(
            symbol='MSFT',
            date=date(2023, 6, 20),
            signal_type='buy_support',
            entry_price=350.0,
            target_price=365.0,
            stop_loss=340.0,
            confidence=0.75,
            rationale='Strong support at $350'
        )

        assert signal.symbol == 'MSFT'
        assert signal.signal_type in ['buy_support', 'sell_resistance', 'hold']
        assert signal.entry_price > 0
        assert signal.target_price > 0
        assert signal.stop_loss > 0
        assert 0 <= signal.confidence <= 1

    def test_signal_risk_reward(self):
        """Test risk/reward calculation for signals"""
        # Buy signal
        buy_signal = TradingSignal(
            symbol='TEST',
            date=date(2023, 1, 1),
            signal_type='buy_support',
            entry_price=100.0,
            target_price=105.0,
            stop_loss=98.0,
            confidence=0.6,
            rationale='Test'
        )

        # Calculate risk/reward
        risk = buy_signal.entry_price - buy_signal.stop_loss
        reward = buy_signal.target_price - buy_signal.entry_price
        risk_reward_ratio = reward / risk if risk > 0 else 0

        assert risk > 0
        assert reward > 0
        assert risk_reward_ratio > 0

class TestBacktestMetrics:
    """Test suite for BacktestMetrics data structure"""

    def test_backtest_metrics_creation(self):
        """Test creating BacktestMetrics objects"""
        metrics = BacktestMetrics(
            support_accuracy=0.65,
            resistance_accuracy=0.58,
            level_mae=0.025,
            confidence_correlation=0.42,
            total_trades=150,
            winning_trades=85,
            losing_trades=65,
            win_rate=0.567,
            avg_return_per_trade=0.015,
            sharpe_ratio=1.25,
            max_drawdown=0.085,
            var_95=-0.032,
            expected_shortfall=-0.045,
            support_test_rate=0.78,
            resistance_test_rate=0.72,
            support_hold_rate=0.65,
            resistance_hold_rate=0.58
        )

        assert 0 <= metrics.support_accuracy <= 1
        assert 0 <= metrics.resistance_accuracy <= 1
        assert metrics.level_mae >= 0
        assert metrics.total_trades == metrics.winning_trades + metrics.losing_trades
        assert 0 <= metrics.win_rate <= 1
        assert metrics.max_drawdown >= 0

    def test_metrics_validation(self):
        """Test metrics validation"""
        metrics = BacktestMetrics(
            support_accuracy=0.5,
            resistance_accuracy=0.6,
            level_mae=0.02,
            confidence_correlation=0.3,
            total_trades=100,
            winning_trades=55,
            losing_trades=45,
            win_rate=0.55,
            avg_return_per_trade=0.01,
            sharpe_ratio=1.0,
            max_drawdown=0.1,
            var_95=-0.02,
            expected_shortfall=-0.03,
            support_test_rate=0.7,
            resistance_test_rate=0.65,
            support_hold_rate=0.6,
            resistance_hold_rate=0.55
        )

        # Basic validation
        assert metrics.total_trades == metrics.winning_trades + metrics.losing_trades
        assert abs(metrics.win_rate - metrics.winning_trades / metrics.total_trades) < 1e-6

class TestSRBacktester:
    """Test suite for SRBacktester"""

    @pytest.fixture
    def backtester(self):
        """Create backtester instance for testing"""
        env = MagicMock()
        env.get_database_url.return_value = "test://db"
        env.get_table_name.return_value = "test_table"
        return SRBacktester(env=env)

    @pytest.fixture
    def sample_predictions(self):
        """Create sample prediction results"""
        predictions = []
        base_date = date(2023, 1, 1)

        for i in range(10):
            pred = PredictionResult(
                symbol='TEST',
                date=base_date + timedelta(days=i),
                predicted_support=[95.0 + i * 0.1, 93.0 + i * 0.1],
                predicted_resistance=[105.0 + i * 0.1, 107.0 + i * 0.1],
                support_confidence=[0.7 - i * 0.02, 0.5 - i * 0.01],
                resistance_confidence=[0.6 + i * 0.01, 0.4 + i * 0.01],
                actual_low=94.0 + i * 0.15 + np.random.normal(0, 0.5),
                actual_high=106.0 + i * 0.15 + np.random.normal(0, 0.5),
                actual_close=100.0 + i * 0.1 + np.random.normal(0, 1.0)
            )
            predictions.append(pred)

        return predictions

    def test_backtester_initialization(self, backtester):
        """Test backtester initialization"""
        assert backtester.env is not None
        assert backtester.level_tolerance_pct == 0.5
        assert backtester.min_confidence_threshold == 0.3
        assert backtester.position_size_pct == 0.02
        assert backtester.transaction_cost_bps == 5

    def test_calculate_level_accuracy_support(self, backtester, sample_predictions):
        """Test support level accuracy calculation"""
        accuracy = backtester._calculate_level_accuracy(sample_predictions, 'support')

        assert 0 <= accuracy <= 1
        assert isinstance(accuracy, float)

    def test_calculate_level_accuracy_resistance(self, backtester, sample_predictions):
        """Test resistance level accuracy calculation"""
        accuracy = backtester._calculate_level_accuracy(sample_predictions, 'resistance')

        assert 0 <= accuracy <= 1
        assert isinstance(accuracy, float)

    def test_calculate_level_mae(self, backtester, sample_predictions):
        """Test mean absolute error calculation"""
        mae = backtester._calculate_level_mae(sample_predictions)

        assert mae >= 0
        assert isinstance(mae, (float, int))

    def test_calculate_confidence_correlation(self, backtester, sample_predictions):
        """Test confidence correlation calculation"""
        correlation = backtester._calculate_confidence_correlation(sample_predictions)

        assert -1 <= correlation <= 1
        assert isinstance(correlation, (float, int))

    def test_generate_trading_signals(self, backtester, sample_predictions):
        """Test trading signal generation"""
        signals = backtester._generate_trading_signals(sample_predictions)

        assert isinstance(signals, list)

        for signal in signals:
            assert isinstance(signal, TradingSignal)
            assert signal.symbol == 'TEST'
            assert signal.signal_type in ['buy_support', 'sell_resistance']
            assert signal.entry_price > 0
            assert signal.target_price > 0
            assert signal.stop_loss > 0
            assert 0 <= signal.confidence <= 1

    def test_generate_buy_signals(self, backtester):
        """Test generation of buy signals at support"""
        # Create prediction with strong support that gets tested
        pred = PredictionResult(
            symbol='TEST',
            date=date(2023, 1, 1),
            predicted_support=[100.0],
            predicted_resistance=[110.0],
            support_confidence=[0.8],  # High confidence
            resistance_confidence=[0.5],
            actual_low=99.5,  # Price tested support
            actual_high=105.0,
            actual_close=102.0
        )

        signals = backtester._generate_trading_signals([pred])

        # Should generate at least one buy signal
        buy_signals = [s for s in signals if s.signal_type == 'buy_support']
        assert len(buy_signals) >= 1

        if buy_signals:
            signal = buy_signals[0]
            assert signal.entry_price <= signal.target_price
            assert signal.stop_loss < signal.entry_price

    def test_generate_sell_signals(self, backtester):
        """Test generation of sell signals at resistance"""
        # Create prediction with strong resistance that gets tested
        pred = PredictionResult(
            symbol='TEST',
            date=date(2023, 1, 1),
            predicted_support=[90.0],
            predicted_resistance=[110.0],
            support_confidence=[0.5],
            resistance_confidence=[0.9],  # High confidence
            actual_low=95.0,
            actual_high=109.8,  # Price tested resistance
            actual_close=105.0
        )

        signals = backtester._generate_trading_signals([pred])

        # Should generate at least one sell signal
        sell_signals = [s for s in signals if s.signal_type == 'sell_resistance']
        assert len(sell_signals) >= 1

        if sell_signals:
            signal = sell_signals[0]
            assert signal.entry_price >= signal.target_price
            assert signal.stop_loss > signal.entry_price

    def test_calculate_trading_metrics_empty(self, backtester):
        """Test trading metrics with empty signals"""
        metrics = backtester._calculate_trading_metrics([], [])

        assert metrics['total_trades'] == 0
        assert metrics['winning_trades'] == 0
        assert metrics['losing_trades'] == 0
        assert metrics['win_rate'] == 0.0
        assert metrics['avg_return'] == 0.0

    def test_calculate_trading_metrics_with_signals(self, backtester, sample_predictions):
        """Test trading metrics calculation with signals"""
        signals = backtester._generate_trading_signals(sample_predictions)

        if signals:  # Only test if signals were generated
            metrics = backtester._calculate_trading_metrics(signals, sample_predictions)

            assert isinstance(metrics, dict)
            assert 'total_trades' in metrics
            assert 'winning_trades' in metrics
            assert 'losing_trades' in metrics
            assert 'win_rate' in metrics
            assert 'avg_return' in metrics
            assert 'sharpe_ratio' in metrics
            assert 'max_drawdown' in metrics

            assert metrics['total_trades'] >= 0
            assert metrics['winning_trades'] >= 0
            assert metrics['losing_trades'] >= 0
            assert 0 <= metrics['win_rate'] <= 1
            assert metrics['max_drawdown'] >= 0

    def test_calculate_level_testing_metrics(self, backtester, sample_predictions):
        """Test level testing metrics calculation"""
        metrics = backtester._calculate_level_testing_metrics(sample_predictions)

        assert isinstance(metrics, dict)
        assert 'support_test_rate' in metrics
        assert 'resistance_test_rate' in metrics
        assert 'support_hold_rate' in metrics
        assert 'resistance_hold_rate' in metrics

        assert 0 <= metrics['support_test_rate'] <= 1
        assert 0 <= metrics['resistance_test_rate'] <= 1
        assert 0 <= metrics['support_hold_rate'] <= 1
        assert 0 <= metrics['resistance_hold_rate'] <= 1

    def test_calculate_aggregate_metrics_empty(self, backtester):
        """Test aggregate metrics with empty results"""
        aggregate = backtester._calculate_aggregate_metrics({})

        assert isinstance(aggregate, BacktestMetrics)
        assert aggregate.total_trades == 0
        assert aggregate.win_rate == 0.0

    def test_calculate_aggregate_metrics_with_data(self, backtester):
        """Test aggregate metrics calculation"""
        # Create sample symbol results
        symbol_results = {
            'AAPL': BacktestMetrics(
                support_accuracy=0.6, resistance_accuracy=0.7, level_mae=0.02,
                confidence_correlation=0.4, total_trades=50, winning_trades=30,
                losing_trades=20, win_rate=0.6, avg_return_per_trade=0.01,
                sharpe_ratio=1.2, max_drawdown=0.08, var_95=-0.02,
                expected_shortfall=-0.03, support_test_rate=0.7,
                resistance_test_rate=0.65, support_hold_rate=0.6,
                resistance_hold_rate=0.55
            ),
            'MSFT': BacktestMetrics(
                support_accuracy=0.55, resistance_accuracy=0.65, level_mae=0.025,
                confidence_correlation=0.35, total_trades=40, winning_trades=22,
                losing_trades=18, win_rate=0.55, avg_return_per_trade=0.008,
                sharpe_ratio=1.0, max_drawdown=0.1, var_95=-0.025,
                expected_shortfall=-0.035, support_test_rate=0.65,
                resistance_test_rate=0.6, support_hold_rate=0.55,
                resistance_hold_rate=0.5
            )
        }

        aggregate = backtester._calculate_aggregate_metrics(symbol_results)

        assert isinstance(aggregate, BacktestMetrics)
        assert aggregate.total_trades == 90  # 50 + 40
        assert aggregate.winning_trades == 52  # 30 + 22
        assert aggregate.losing_trades == 38  # 20 + 18
        assert abs(aggregate.win_rate - 52/90) < 1e-6

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_daily_data_mock(self, backtester):
        """Test getting daily data with mocked database"""
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = [
            {
                'date': date(2023, 1, i),
                'open': 100.0 + i * 0.1,
                'high': 102.0 + i * 0.1,
                'low': 98.0 + i * 0.1,
                'close': 100.5 + i * 0.1,
                'volume': 1000000 + i * 1000
            }
            for i in range(1, 11)
        ]

        df = await backtester._get_daily_data(
            mock_conn, 'TEST', date(2023, 1, 1), date(2023, 1, 10)
        )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        assert 'date' in df.columns
        assert 'open' in df.columns
        assert 'high' in df.columns
        assert 'low' in df.columns
        assert 'close' in df.columns
        assert 'volume' in df.columns

    def test_generate_backtest_report(self, backtester):
        """Test backtest report generation"""
        # Create sample results
        results = {
            'AAPL': BacktestMetrics(
                support_accuracy=0.65, resistance_accuracy=0.7, level_mae=0.02,
                confidence_correlation=0.45, total_trades=100, winning_trades=65,
                losing_trades=35, win_rate=0.65, avg_return_per_trade=0.012,
                sharpe_ratio=1.3, max_drawdown=0.08, var_95=-0.02,
                expected_shortfall=-0.03, support_test_rate=0.75,
                resistance_test_rate=0.7, support_hold_rate=0.65,
                resistance_hold_rate=0.6
            ),
            '_AGGREGATE': BacktestMetrics(
                support_accuracy=0.65, resistance_accuracy=0.7, level_mae=0.02,
                confidence_correlation=0.45, total_trades=100, winning_trades=65,
                losing_trades=35, win_rate=0.65, avg_return_per_trade=0.012,
                sharpe_ratio=1.3, max_drawdown=0.08, var_95=-0.02,
                expected_shortfall=-0.03, support_test_rate=0.75,
                resistance_test_rate=0.7, support_hold_rate=0.65,
                resistance_hold_rate=0.6
            )
        }

        report = backtester.generate_backtest_report(results)

        assert isinstance(report, str)
        assert len(report) > 0
        assert 'Support/Resistance Model Backtest Report' in report
        assert 'Executive Summary' in report
        assert 'Individual Symbol Results' in report
        assert 'AAPL' in report
        assert '65.0%' in report  # Win rate

@pytest.mark.integration
class TestSRBacktesterIntegration:
    """Integration tests for the backtesting system"""

    @pytest.fixture
    def mock_env(self):
        """Create mock environment for integration tests"""
        env = MagicMock()
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name = lambda name: f"test_{name}"
        return env

    @pytest.fixture
    def mock_model(self):
        """Create mock model for testing"""
        model = MagicMock()

        def mock_predict(features):
            batch_size = features.shape[0]
            return {
                'support_levels': np.random.uniform(90, 95, (batch_size, 2)),
                'resistance_levels': np.random.uniform(105, 110, (batch_size, 2)),
                'support_confidence': np.random.uniform(0.3, 0.8, (batch_size, 2)),
                'resistance_confidence': np.random.uniform(0.3, 0.8, (batch_size, 2))
            }

        model.predict = mock_predict
        return model

    @pytest.fixture
    def mock_feature_generator(self):
        """Create mock feature generator"""
        generator = MagicMock()

        async def mock_generate_features(conn, symbol, daily_data, idx, current_date):
            return {f'feature_{i}': np.random.randn() for i in range(10)}

        generator._generate_features = mock_generate_features
        return generator

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_backtest_model_mock(self, mock_env, mock_model, mock_feature_generator):
        """Test backtesting with mocked components"""
        backtester = SRBacktester(env=mock_env)

        # Mock database operations
        with patch('asyncpg.create_pool') as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn

            # Mock daily data
            mock_conn.fetch.return_value = [
                {
                    'date': date(2023, 1, i),
                    'open': 100.0 + i * 0.1,
                    'high': 102.0 + i * 0.1,
                    'low': 98.0 + i * 0.1,
                    'close': 100.5 + i * 0.1,
                    'volume': 1000000
                }
                for i in range(1, 31)
            ]

            # Test backtesting
            results = await backtester.backtest_model(
                model=mock_model,
                symbols=['TEST'],
                start_date=date(2023, 1, 10),
                end_date=date(2023, 1, 20),
                feature_generator=mock_feature_generator,
                min_predictions_per_symbol=5
            )

            # Verify results structure
            assert isinstance(results, dict)
            if results:  # May be empty due to mocked data
                for symbol, metrics in results.items():
                    if symbol != '_AGGREGATE':
                        assert isinstance(metrics, BacktestMetrics)

    def test_prediction_accuracy_calculation(self):
        """Test prediction accuracy calculation with known data"""
        backtester = SRBacktester()

        # Create predictions where we know the accuracy
        predictions = [
            PredictionResult(
                symbol='TEST', date=date(2023, 1, 1),
                predicted_support=[95.0], predicted_resistance=[105.0],
                support_confidence=[0.7], resistance_confidence=[0.6],
                actual_low=95.1, actual_high=104.9, actual_close=100.0  # Both levels hit
            ),
            PredictionResult(
                symbol='TEST', date=date(2023, 1, 2),
                predicted_support=[95.0], predicted_resistance=[105.0],
                support_confidence=[0.6], resistance_confidence=[0.5],
                actual_low=97.0, actual_high=103.0, actual_close=100.0  # Neither level hit
            )
        ]

        support_accuracy = backtester._calculate_level_accuracy(predictions, 'support')
        resistance_accuracy = backtester._calculate_level_accuracy(predictions, 'resistance')

        # With tolerance of 0.5%, both should have 50% accuracy (1 out of 2 hit)
        assert 0.4 <= support_accuracy <= 0.6  # Allow some tolerance
        assert 0.4 <= resistance_accuracy <= 0.6

    def test_trading_signal_profitability(self):
        """Test trading signal profitability calculation"""
        backtester = SRBacktester()

        # Create profitable buy signal
        buy_signal = TradingSignal(
            symbol='TEST', date=date(2023, 1, 1),
            signal_type='buy_support', entry_price=100.0,
            target_price=105.0, stop_loss=98.0,
            confidence=0.8, rationale='Test'
        )

        # Create prediction where target is hit
        prediction = PredictionResult(
            symbol='TEST', date=date(2023, 1, 1),
            predicted_support=[100.0], predicted_resistance=[110.0],
            support_confidence=[0.8], resistance_confidence=[0.6],
            actual_low=99.0, actual_high=106.0, actual_close=105.5
        )

        metrics = backtester._calculate_trading_metrics([buy_signal], [prediction])

        # Should be profitable
        assert metrics['total_trades'] == 1
        assert metrics['avg_return'] > 0  # Should be positive return

if __name__ == "__main__":
    pytest.main([__file__, "-v"])