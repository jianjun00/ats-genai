#!/usr/bin/env python3
"""
Comprehensive tests for Signal Generation components.

Tests cover:
- Technical signal generation (RSI, MACD, Bollinger Bands)
- Smart Money signal integration
- Volume signal analysis
- Composite signal combination
- Signal quality and validation
"""

import pytest
import pandas as pd
import numpy as np
from typing import Dict

from domains.trading.services.signal_generation import (
    SignalDirection,
    SignalStrength,
    TradingSignal,
    IndicatorWeights,
    TechnicalSignalGenerator,
    SmartMoneySignalGenerator,
    VolumeSignalGenerator,
    CompositeSignalGenerator,
    PortfolioSignalManager
)

class TestTradingSignal:
    """Test TradingSignal data structure."""

    def test_signal_creation(self):
        """Test basic signal creation."""
        signal = TradingSignal(
            symbol='TEST',
            direction=SignalDirection.LONG,
            strength=SignalStrength.STRONG,
            confidence=0.8,
            expected_return=0.02,
            forecast_horizon=6,
            signal_components={'rsi': 0.7, 'macd': 0.6},
            risk_score=0.3,
            entry_price=100.0
        )

        assert signal.symbol == 'TEST'
        assert signal.direction == SignalDirection.LONG
        assert signal.strength == SignalStrength.STRONG
        assert signal.confidence == 0.8
        assert signal.expected_return == 0.02
        assert signal.forecast_horizon == 6
        assert signal.risk_score == 0.3
        assert signal.entry_price == 100.0
        assert signal.timestamp is not None

    def test_signal_properties(self):
        """Test signal computed properties."""
        signal = TradingSignal(
            symbol='TEST',
            direction=SignalDirection.LONG,
            strength=SignalStrength.STRONG,
            confidence=0.8,
            expected_return=0.02,
            forecast_horizon=6,
            signal_components={},
            risk_score=0.3,
            entry_price=100.0
        )

        # Test risk-adjusted return
        expected_risk_adj = 0.02 / (1 + 0.3)
        assert abs(signal.risk_adjusted_return - expected_risk_adj) < 1e-10

        # Test signal score
        expected_score = SignalStrength.STRONG.value * 0.8
        assert signal.signal_score == expected_score

    def test_signal_directions(self):
        """Test all signal directions."""
        directions = [SignalDirection.LONG, SignalDirection.SHORT, SignalDirection.NEUTRAL]

        for direction in directions:
            signal = TradingSignal(
                symbol='TEST',
                direction=direction,
                strength=SignalStrength.MODERATE,
                confidence=0.5,
                expected_return=0.01 if direction == SignalDirection.LONG else -0.01,
                forecast_horizon=3,
                signal_components={},
                risk_score=0.5,
                entry_price=100.0
            )

            assert signal.direction == direction

class TestIndicatorWeights:
    """Test indicator weighting system."""

    def test_weights_initialization(self):
        """Test weight initialization and normalization."""
        weights = IndicatorWeights()

        # Check all weights are positive
        for indicator, weight in weights.weights.items():
            assert weight > 0
            assert weight <= 1.0

        # Check weights sum to 1 (normalized)
        total_weight = sum(weights.weights.values())
        assert abs(total_weight - 1.0) < 1e-10

    def test_weight_retrieval(self):
        """Test weight retrieval for specific indicators."""
        weights = IndicatorWeights()

        # Test existing indicators
        assert weights.get_weight('smart_money_zones') > 0
        assert weights.get_weight('rsi') > 0
        assert weights.get_weight('session_vwap') > 0

        # Test non-existing indicator
        assert weights.get_weight('non_existent') == 0.0

    def test_weight_hierarchy(self):
        """Test that important indicators have higher weights."""
        weights = IndicatorWeights()

        # Smart money analysis should have high weight
        smz_weight = weights.get_weight('smart_money_zones')
        rsi_weight = weights.get_weight('rsi')

        assert smz_weight >= rsi_weight  # SMZ should be weighted higher than RSI

class TestTechnicalSignalGenerator:
    """Test technical indicator signal generation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = TechnicalSignalGenerator()

    def test_rsi_analysis(self):
        """Test RSI signal analysis."""
        # Test oversold condition
        direction, strength, confidence = self.generator.analyze_rsi(25.0)
        assert direction == SignalDirection.LONG
        assert strength > 1
        assert confidence > 0.5

        # Test overbought condition
        direction, strength, confidence = self.generator.analyze_rsi(75.0)
        assert direction == SignalDirection.SHORT
        assert strength > 1
        assert confidence > 0.5

        # Test neutral condition
        direction, strength, confidence = self.generator.analyze_rsi(50.0)
        assert direction == SignalDirection.NEUTRAL
        assert confidence < 0.2

    def test_macd_analysis(self):
        """Test MACD signal analysis."""
        # Test bullish crossover
        direction, strength, confidence = self.generator.analyze_macd(
            macd_line=0.5, macd_signal=0.3, macd_histogram=0.2
        )
        assert direction == SignalDirection.LONG
        assert strength > 1
        assert confidence > 0.3

        # Test bearish crossover
        direction, strength, confidence = self.generator.analyze_macd(
            macd_line=-0.5, macd_signal=-0.3, macd_histogram=-0.2
        )
        assert direction == SignalDirection.SHORT
        assert strength > 1
        assert confidence > 0.3

    def test_bollinger_bands_analysis(self):
        """Test Bollinger Bands signal analysis."""
        bb_upper = 105.0
        bb_lower = 95.0
        bb_middle = 100.0

        # Test price below lower band
        direction, strength, confidence = self.generator.analyze_bollinger_bands(
            price=94.0, bb_upper=bb_upper, bb_lower=bb_lower, bb_middle=bb_middle
        )
        assert direction == SignalDirection.LONG
        assert strength > 3
        assert confidence > 0.6

        # Test price above upper band
        direction, strength, confidence = self.generator.analyze_bollinger_bands(
            price=106.0, bb_upper=bb_upper, bb_lower=bb_lower, bb_middle=bb_middle
        )
        assert direction == SignalDirection.SHORT
        assert strength > 3
        assert confidence > 0.6

class TestSmartMoneySignalGenerator:
    """Test Smart Money signal generation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = SmartMoneySignalGenerator()
        self.test_data = self._create_test_data()

    def _create_test_data(self) -> pd.DataFrame:
        """Create test price data."""
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=50, freq='H')

        # Create trending price data
        base_price = 100.0
        prices = []

        for i in range(50):
            # Add some trend and noise
            trend = i * 0.1
            noise = np.random.normal(0, 0.5)
            price = base_price + trend + noise

            # Create OHLCV
            open_price = price + np.random.normal(0, 0.2)
            high = max(open_price, price) + abs(np.random.normal(0, 0.3))
            low = min(open_price, price) - abs(np.random.normal(0, 0.3))
            close = price
            volume = int(1000 + np.random.normal(0, 200))

            prices.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })

        df = pd.DataFrame(prices, index=dates)
        return df

    def test_smart_money_zone_analysis(self):
        """Test Smart Money Zone analysis."""
        result = self.generator.analyze_smart_money_zones(self.test_data)

        assert isinstance(result, dict)
        assert 'direction' in result
        assert 'strength' in result
        assert 'confidence' in result

        # Check result structure
        assert hasattr(result['direction'], 'value') or result['direction'] in [1, -1, 0]
        assert isinstance(result['strength'], (int, float))
        assert isinstance(result['confidence'], (int, float))
        assert 0 <= result['confidence'] <= 1

    def test_market_structure_analysis(self):
        """Test market structure analysis."""
        result = self.generator.analyze_market_structure(self.test_data)

        assert isinstance(result, dict)
        assert 'direction' in result
        assert 'strength' in result
        assert 'confidence' in result

        # Verify confidence bounds
        assert 0 <= result['confidence'] <= 1

class TestVolumeSignalGenerator:
    """Test volume signal generation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = VolumeSignalGenerator()

    def test_volume_signal_analysis(self):
        """Test volume signal analysis."""
        # Create test data with volume patterns
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=30, freq='H')

        # Normal volume first, then spike
        volumes = [1000] * 25 + [3000] * 5  # Volume spike at end
        prices = list(range(100, 130))  # Rising prices

        data = pd.DataFrame({
            'close': prices,
            'volume': volumes
        }, index=dates)

        result = self.generator.analyze_volume_signals(data)

        assert isinstance(result, dict)
        assert 'direction' in result
        assert 'strength' in result
        assert 'confidence' in result

    def test_volume_confirmation(self):
        """Test volume confirmation logic."""
        # Create data with rising prices and volume
        dates = pd.date_range('2024-01-01', periods=20, freq='H')

        data = pd.DataFrame({
            'close': [100, 101, 102, 103, 104] + [100] * 15,  # Rising then flat
            'volume': [2000] * 5 + [1000] * 15  # High volume then normal
        }, index=dates)

        result = self.generator.analyze_volume_signals(data)

        # Should detect bullish signal with volume confirmation
        assert result['direction'] in [SignalDirection.LONG, 1, 'LONG']

class TestCompositeSignalGenerator:
    """Test composite signal generation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = CompositeSignalGenerator()
        self.test_data = self._create_comprehensive_test_data()

    def _create_comprehensive_test_data(self) -> pd.DataFrame:
        """Create comprehensive test data for signal generation."""
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=100, freq='H')

        base_price = 100.0
        data = []

        for i in range(100):
            # Create price trend with some patterns
            if i < 30:
                trend = 0.1  # Uptrend
            elif i < 60:
                trend = -0.05  # Downtrend
            else:
                trend = 0.08  # Recovery

            price = base_price + i * trend + np.random.normal(0, 0.5)

            # Create realistic OHLCV
            open_price = price + np.random.normal(0, 0.2)
            high = max(open_price, price) + abs(np.random.normal(0, 0.4))
            low = min(open_price, price) - abs(np.random.normal(0, 0.4))
            close = price

            # Volume with some patterns
            if 30 <= i < 35:  # Volume spike during trend change
                volume = int(2000 + np.random.normal(0, 300))
            else:
                volume = int(1000 + np.random.normal(0, 200))

            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })

        return pd.DataFrame(data, index=dates)

    def test_comprehensive_signal_generation(self):
        """Test comprehensive signal generation."""
        signal = self.generator.generate_comprehensive_signal('TEST', self.test_data)

        assert isinstance(signal, TradingSignal)
        assert signal.symbol == 'TEST'
        assert hasattr(signal.direction, 'value') or signal.direction in [-1, 0, 1]
        assert hasattr(signal.strength, 'value') or isinstance(signal.strength, int)
        assert 0 <= signal.confidence <= 1
        assert signal.forecast_horizon > 0
        assert isinstance(signal.signal_components, dict)
        assert 0 <= signal.risk_score <= 1
        assert signal.entry_price > 0

    def test_signal_component_integration(self):
        """Test that multiple signal components are integrated."""
        signal = self.generator.generate_comprehensive_signal('TEST', self.test_data)

        # Should have multiple signal components
        assert len(signal.signal_components) > 0

        # Check for expected component types
        expected_components = ['rsi', 'smart_money_zones', 'market_structure', 'volume']
        found_components = set(signal.signal_components.keys())

        # At least some expected components should be present
        assert len(found_components.intersection(expected_components)) > 0

    def test_signal_consistency(self):
        """Test signal consistency and bounds."""
        signal = self.generator.generate_comprehensive_signal('TEST', self.test_data)

        # Confidence should be reasonable
        assert 0.05 <= signal.confidence <= 0.95

        # Expected return should be reasonable
        assert -0.1 <= signal.expected_return <= 0.1  # Within ±10%

        # Risk score should be reasonable
        assert 0.05 <= signal.risk_score <= 0.95

        # Forecast horizon should be reasonable
        assert 1 <= signal.forecast_horizon <= 24  # 1-24 hours

class TestPortfolioSignalManager:
    """Test portfolio-level signal management."""

    def setup_method(self):
        """Set up test fixtures."""
        self.universe = ['AAPL', 'MSFT', 'GOOGL', 'SPY', 'TLT']
        self.manager = PortfolioSignalManager(self.universe)
        self.market_data = self._create_market_data()

    def _create_market_data(self) -> Dict[str, pd.DataFrame]:
        """Create market data for multiple symbols."""
        market_data = {}

        for symbol in self.universe:
            np.random.seed(hash(symbol) % 2**32)  # Different seed per symbol
            dates = pd.date_range('2024-01-01', periods=50, freq='H')

            base_price = 100 + hash(symbol) % 200
            data = []

            for i in range(50):
                price = base_price + i * 0.1 + np.random.normal(0, 1)

                data.append({
                    'open': round(price + np.random.normal(0, 0.2), 2),
                    'high': round(price + abs(np.random.normal(0, 0.5)), 2),
                    'low': round(price - abs(np.random.normal(0, 0.5)), 2),
                    'close': round(price, 2),
                    'volume': max(100, int(1000 + np.random.normal(0, 300)))
                })

            market_data[symbol] = pd.DataFrame(data, index=dates)

        return market_data

    def test_portfolio_signal_generation(self):
        """Test portfolio-wide signal generation."""
        signals = self.manager.generate_portfolio_signals(self.market_data)

        assert isinstance(signals, dict)
        assert len(signals) > 0

        # Check that signals are generated for available symbols
        for symbol in self.universe:
            if symbol in self.market_data and len(self.market_data[symbol]) >= 20:
                assert symbol in signals
                assert isinstance(signals[symbol], TradingSignal)

    def test_signal_summary(self):
        """Test signal summary generation."""
        signals = self.manager.generate_portfolio_signals(self.market_data)
        summary = self.manager.get_signal_summary(signals)

        assert isinstance(summary, dict)

        expected_fields = [
            'total_signals', 'long_signals', 'short_signals', 'neutral_signals',
            'avg_confidence', 'avg_expected_return', 'high_confidence_signals'
        ]

        for field in expected_fields:
            assert field in summary

        # Check field types and bounds
        assert summary['total_signals'] >= 0
        assert summary['long_signals'] >= 0
        assert summary['short_signals'] >= 0
        assert summary['neutral_signals'] >= 0
        assert 0 <= summary['avg_confidence'] <= 1
        assert summary['high_confidence_signals'] >= 0

        # Check that signal counts add up
        total_by_direction = (summary['long_signals'] +
                            summary['short_signals'] +
                            summary['neutral_signals'])
        assert total_by_direction == summary['total_signals']

    def test_signal_history_tracking(self):
        """Test signal history tracking."""
        # Generate signals multiple times
        for _ in range(3):
            self.manager.generate_portfolio_signals(self.market_data)

        # Check that history is tracked
        for symbol in self.universe:
            if symbol in self.manager.signal_history:
                history = self.manager.signal_history[symbol]
                assert len(history) <= 3  # Should have stored signals

                for signal in history:
                    assert isinstance(signal, TradingSignal)

class TestSignalGenerationIntegration:
    """Integration tests for signal generation system."""

    def test_end_to_end_signal_pipeline(self):
        """Test complete signal generation pipeline."""
        # Create realistic test scenario
        universe = ['AAPL', 'MSFT', 'SPY']
        manager = PortfolioSignalManager(universe)

        # Create market data with different patterns
        market_data = {}

        # AAPL: Bullish trend
        aapl_data = self._create_trending_data(100, 0.2, 50, 'bullish')
        market_data['AAPL'] = aapl_data

        # MSFT: Bearish trend
        msft_data = self._create_trending_data(150, -0.15, 50, 'bearish')
        market_data['MSFT'] = msft_data

        # SPY: Sideways
        spy_data = self._create_trending_data(400, 0.02, 50, 'sideways')
        market_data['SPY'] = spy_data

        # Generate signals
        signals = manager.generate_portfolio_signals(market_data)

        # Verify signal quality
        assert len(signals) == 3

        for symbol, signal in signals.items():
            assert isinstance(signal, TradingSignal)
            assert signal.symbol == symbol
            assert 0 <= signal.confidence <= 1
            assert signal.forecast_horizon > 0

        # Generate summary
        summary = manager.get_signal_summary(signals)

        # Should have reasonable distribution of signals
        assert summary['total_signals'] == 3
        assert summary['avg_confidence'] > 0

    def _create_trending_data(self, base_price: float, trend: float,
                            periods: int, pattern: str) -> pd.DataFrame:
        """Create trending price data."""
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=periods, freq='H')

        data = []
        for i in range(periods):
            if pattern == 'bullish':
                price = base_price + i * trend + np.random.normal(0, 0.3)
            elif pattern == 'bearish':
                price = base_price + i * trend + np.random.normal(0, 0.3)
            else:  # sideways
                price = base_price + np.sin(i * 0.2) * 2 + np.random.normal(0, 0.5)

            # Create OHLCV
            open_price = price + np.random.normal(0, 0.2)
            high = max(open_price, price) + abs(np.random.normal(0, 0.4))
            low = min(open_price, price) - abs(np.random.normal(0, 0.4))
            close = price
            volume = max(100, int(1000 + np.random.normal(0, 200)))

            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': volume
            })

        return pd.DataFrame(data, index=dates)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])