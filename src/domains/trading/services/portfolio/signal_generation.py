"""
Signal Generation System for Market-Neutral Portfolio

Combines multiple technical indicators, Smart Money Zones, and institutional
flow analysis to generate expected return forecasts with confidence scores.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from domains.trading.services.indicators.enhanced_indicators import (
    calculate_all_technical_indicators
)
from domains.trading.services.smart_money_zones import (
    MarketStructureDetector,
    SmartMoneyZoneDetector,
    SMZEntryConfirmation
)


class SignalDirection(Enum):
    """Signal direction for long/short positions."""
    LONG = 1
    SHORT = -1
    NEUTRAL = 0


class SignalStrength(Enum):
    """Signal strength categories."""
    VERY_WEAK = 1
    WEAK = 2
    MODERATE = 3
    STRONG = 4
    VERY_STRONG = 5


@dataclass
class TradingSignal:
    """Represents a trading signal with all relevant information."""
    symbol: str
    direction: SignalDirection
    strength: SignalStrength
    confidence: float  # 0-1
    expected_return: float  # Expected return over forecast horizon
    forecast_horizon: int  # Hours
    signal_components: Dict[str, float]  # Individual indicator contributions
    risk_score: float  # 0-1, higher = riskier
    entry_price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    @property
    def risk_adjusted_return(self) -> float:
        """Calculate risk-adjusted expected return."""
        if self.risk_score > 0:
            return self.expected_return / (1 + self.risk_score)
        return self.expected_return

    @property
    def signal_score(self) -> float:
        """Overall signal score combining strength and confidence."""
        return self.strength.value * self.confidence


class IndicatorWeights:
    """Defines weights for different indicator types in signal generation."""

    def __init__(self):
        self.weights = {
            # Technical Indicators
            'rsi': 0.15,
            'macd': 0.12,
            'bollinger_bands': 0.10,
            'moving_averages': 0.08,

            # Smart Money Analysis
            'smart_money_zones': 0.20,
            'market_structure': 0.15,
            'session_vwap': 0.15,

            # Volume Analysis
            'cumulative_volume': 0.08,
            'cumulative_dollars': 0.07,
            'volume_profile': 0.06,

            # Momentum Indicators
            'momentum': 0.10,
            'volatility': 0.08,

            # Mean Reversion
            'mean_reversion': 0.12
        }

        # Normalize weights
        total_weight = sum(self.weights.values())
        self.weights = {k: v/total_weight for k, v in self.weights.items()}

    def get_weight(self, indicator: str) -> float:
        """Get weight for specific indicator."""
        return self.weights.get(indicator, 0.0)


class TechnicalSignalGenerator:
    """Generates signals from technical indicators."""

    def __init__(self):
        self.rsi_oversold = 30
        self.rsi_overbought = 70
        self.bb_std_dev = 2.0

    def analyze_rsi(self, rsi_value: float) -> Tuple[SignalDirection, float, float]:
        """
        Analyze RSI for signal generation.

        Returns:
            (direction, strength, confidence)
        """
        if rsi_value < self.rsi_oversold:
            # Oversold - potential long signal
            strength = min(5, 1 + (self.rsi_oversold - rsi_value) / 5)
            confidence = min(0.9, 0.5 + (self.rsi_oversold - rsi_value) / 30)
            return SignalDirection.LONG, strength, confidence
        elif rsi_value > self.rsi_overbought:
            # Overbought - potential short signal
            strength = min(5, 1 + (rsi_value - self.rsi_overbought) / 5)
            confidence = min(0.9, 0.5 + (rsi_value - self.rsi_overbought) / 30)
            return SignalDirection.SHORT, strength, confidence
        else:
            # Neutral zone
            return SignalDirection.NEUTRAL, 1, 0.1

    def analyze_macd(self, macd_line: float, macd_signal: float,
                    macd_histogram: float) -> Tuple[SignalDirection, float, float]:
        """Analyze MACD for signal generation."""
        # MACD crossover signal
        if macd_line > macd_signal and macd_histogram > 0:
            # Bullish crossover
            strength = min(5, 2 + abs(macd_histogram) * 10)
            confidence = min(0.8, 0.4 + abs(macd_line - macd_signal) * 5)
            return SignalDirection.LONG, strength, confidence
        elif macd_line < macd_signal and macd_histogram < 0:
            # Bearish crossover
            strength = min(5, 2 + abs(macd_histogram) * 10)
            confidence = min(0.8, 0.4 + abs(macd_line - macd_signal) * 5)
            return SignalDirection.SHORT, strength, confidence
        else:
            return SignalDirection.NEUTRAL, 1, 0.1

    def analyze_bollinger_bands(self, price: float, bb_upper: float,
                               bb_lower: float, bb_middle: float) -> Tuple[SignalDirection, float, float]:
        """Analyze Bollinger Bands for signal generation."""
        bb_width = bb_upper - bb_lower

        if price <= bb_lower:
            # Price at or below lower band - potential bounce
            strength = 3 + min(2, (bb_lower - price) / bb_width * 10)
            confidence = 0.6 + min(0.3, (bb_lower - price) / bb_width)
            return SignalDirection.LONG, strength, confidence
        elif price >= bb_upper:
            # Price at or above upper band - potential reversal
            strength = 3 + min(2, (price - bb_upper) / bb_width * 10)
            confidence = 0.6 + min(0.3, (price - bb_upper) / bb_width)
            return SignalDirection.SHORT, strength, confidence
        else:
            # Inside bands - check position relative to middle
            relative_position = (price - bb_middle) / (bb_width / 2)
            if abs(relative_position) < 0.2:
                return SignalDirection.NEUTRAL, 1, 0.1
            else:
                direction = SignalDirection.SHORT if relative_position > 0 else SignalDirection.LONG
                strength = 1 + abs(relative_position) * 2
                confidence = 0.2 + abs(relative_position) * 0.3
                return direction, strength, confidence


class SmartMoneySignalGenerator:
    """Generates signals from Smart Money analysis."""

    def __init__(self):
        self.structure_detector = MarketStructureDetector(swing_length=8, min_swing_size=0.002)
        self.smz_detector = SmartMoneyZoneDetector()
        self.entry_confirmation = SMZEntryConfirmation(confirmation_bars=3)

    def analyze_smart_money_zones(self, price_data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze Smart Money Zones for signal generation."""
        try:
            # Get SMZ analysis
            smz_result = self.smz_detector.calculate(price_data)

            if smz_result['status'] != 'valid':
                return {'direction': SignalDirection.NEUTRAL, 'strength': 1, 'confidence': 0.1}

            current_price = smz_result['current_price']
            active_zones = smz_result['active_zones']
            market_structure = smz_result['market_structure']

            # Check if price is in Smart Money Zone
            in_smz = False
            smz_direction = SignalDirection.NEUTRAL

            for zone_info in active_zones:
                if zone_info['type'] == 'smart_money':
                    in_smz = True
                    if zone_info['direction'] == 'bullish':
                        smz_direction = SignalDirection.LONG
                    else:
                        smz_direction = SignalDirection.SHORT
                    break

            if in_smz:
                # Strong signal when price is in SMZ with structure alignment
                if market_structure == 'bullish' and smz_direction == SignalDirection.LONG:
                    return {'direction': SignalDirection.LONG, 'strength': 4, 'confidence': 0.8}
                elif market_structure == 'bearish' and smz_direction == SignalDirection.SHORT:
                    return {'direction': SignalDirection.SHORT, 'strength': 4, 'confidence': 0.8}
                else:
                    # Mixed signals
                    return {'direction': smz_direction, 'strength': 2, 'confidence': 0.4}

            # Check proximity to zones
            nearest_institutional = smz_result.get('nearest_institutional_zone')
            if nearest_institutional and nearest_institutional['distance'] < current_price * 0.005:  # Within 0.5%
                direction = SignalDirection.LONG if nearest_institutional['direction'] == 'bullish' else SignalDirection.SHORT
                return {'direction': direction, 'strength': 3, 'confidence': 0.6}

            return {'direction': SignalDirection.NEUTRAL, 'strength': 1, 'confidence': 0.1}

        except Exception:
            return {'direction': SignalDirection.NEUTRAL, 'strength': 1, 'confidence': 0.1}

    def analyze_market_structure(self, price_data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze market structure for trend signals."""
        try:
            structure_result = self.structure_detector.calculate(price_data)

            if structure_result['status'] != 'valid':
                return {'direction': SignalDirection.NEUTRAL, 'strength': 1, 'confidence': 0.1}

            market_structure = structure_result['market_structure']
            structure_change = structure_result['structure_change']
            trend_strength = structure_result.get('trend_strength', 0)

            # Analyze structure change for entry signals
            if structure_change in ['bos_bullish', 'choch_bullish']:
                strength = 4 + min(1, trend_strength * 2)
                confidence = 0.7 + min(0.2, trend_strength)
                return {'direction': SignalDirection.LONG, 'strength': strength, 'confidence': confidence}
            elif structure_change in ['bos_bearish', 'choch_bearish']:
                strength = 4 + min(1, trend_strength * 2)
                confidence = 0.7 + min(0.2, trend_strength)
                return {'direction': SignalDirection.SHORT, 'strength': strength, 'confidence': confidence}

            # Analyze ongoing structure
            if market_structure == 'bullish':
                strength = 2 + trend_strength * 2
                confidence = 0.4 + trend_strength * 0.4
                return {'direction': SignalDirection.LONG, 'strength': strength, 'confidence': confidence}
            elif market_structure == 'bearish':
                strength = 2 + trend_strength * 2
                confidence = 0.4 + trend_strength * 0.4
                return {'direction': SignalDirection.SHORT, 'strength': strength, 'confidence': confidence}

            return {'direction': SignalDirection.NEUTRAL, 'strength': 1, 'confidence': 0.1}

        except Exception:
            return {'direction': SignalDirection.NEUTRAL, 'strength': 1, 'confidence': 0.1}


class VolumeSignalGenerator:
    """Generates signals from volume analysis."""

    def __init__(self):
        self.volume_threshold = 1.5  # Volume spike threshold

    def analyze_volume_signals(self, price_data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze volume for confirmation signals."""
        if 'volume' not in price_data.columns or len(price_data) < 20:
            return {'direction': SignalDirection.NEUTRAL, 'strength': 1, 'confidence': 0.1}

        try:
            # Calculate volume indicators
            recent_volume = price_data['volume'].tail(3).mean()
            avg_volume = price_data['volume'].tail(20).mean()
            volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1

            # Price and volume relationship
            recent_return = (price_data['close'].iloc[-1] / price_data['close'].iloc[-4] - 1)

            # Volume confirmation
            if volume_ratio > self.volume_threshold:
                if recent_return > 0:
                    # Rising price with volume - bullish
                    strength = 2 + min(2, volume_ratio - 1)
                    confidence = 0.5 + min(0.3, (volume_ratio - 1) / 2)
                    return {'direction': SignalDirection.LONG, 'strength': strength, 'confidence': confidence}
                elif recent_return < 0:
                    # Falling price with volume - bearish
                    strength = 2 + min(2, volume_ratio - 1)
                    confidence = 0.5 + min(0.3, (volume_ratio - 1) / 2)
                    return {'direction': SignalDirection.SHORT, 'strength': strength, 'confidence': confidence}

            # Volume divergence
            if volume_ratio < 0.7 and abs(recent_return) > 0.01:
                # Price move without volume confirmation - potential reversal
                direction = SignalDirection.SHORT if recent_return > 0 else SignalDirection.LONG
                strength = 2
                confidence = 0.4
                return {'direction': direction, 'strength': strength, 'confidence': confidence}

            return {'direction': SignalDirection.NEUTRAL, 'strength': 1, 'confidence': 0.1}

        except Exception:
            return {'direction': SignalDirection.NEUTRAL, 'strength': 1, 'confidence': 0.1}


class CompositeSignalGenerator:
    """Combines multiple signal generators for comprehensive analysis."""

    def __init__(self):
        self.weights = IndicatorWeights()
        self.technical_generator = TechnicalSignalGenerator()
        self.smart_money_generator = SmartMoneySignalGenerator()
        self.volume_generator = VolumeSignalGenerator()

    def generate_comprehensive_signal(self, symbol: str, price_data: pd.DataFrame) -> TradingSignal:
        """Generate comprehensive trading signal from all indicators."""
        current_price = price_data['close'].iloc[-1]
        signal_components = {}

        # Technical indicators analysis
        try:
            # Calculate all technical indicators
            tech_indicators = calculate_all_technical_indicators(price_data)

            # RSI Analysis
            if 'RSI_14' in tech_indicators:
                rsi_signal = self.technical_generator.analyze_rsi(tech_indicators['RSI_14'])
                signal_components['rsi'] = {
                    'direction': rsi_signal[0].value,
                    'strength': rsi_signal[1],
                    'confidence': rsi_signal[2]
                }

            # MACD Analysis (if available)
            if all(k in tech_indicators for k in ['MACD_12_26', 'MACD_signal_12_26', 'MACD_histogram_12_26']):
                macd_signal = self.technical_generator.analyze_macd(
                    tech_indicators['MACD_12_26'],
                    tech_indicators['MACD_signal_12_26'],
                    tech_indicators['MACD_histogram_12_26']
                )
                signal_components['macd'] = {
                    'direction': macd_signal[0].value,
                    'strength': macd_signal[1],
                    'confidence': macd_signal[2]
                }

        except Exception as e:
            print(f"Technical analysis error for {symbol}: {e}")

        # Smart Money Analysis
        try:
            smz_signal = self.smart_money_generator.analyze_smart_money_zones(price_data)
            signal_components['smart_money_zones'] = smz_signal

            structure_signal = self.smart_money_generator.analyze_market_structure(price_data)
            signal_components['market_structure'] = structure_signal

        except Exception as e:
            print(f"Smart money analysis error for {symbol}: {e}")

        # Volume Analysis
        try:
            volume_signal = self.volume_generator.analyze_volume_signals(price_data)
            signal_components['volume'] = volume_signal

        except Exception as e:
            print(f"Volume analysis error for {symbol}: {e}")

        # Combine signals
        return self._combine_signals(symbol, current_price, signal_components)

    def _combine_signals(self, symbol: str, current_price: float,
                        signal_components: Dict[str, Dict]) -> TradingSignal:
        """Combine individual signals into composite signal."""
        if not signal_components:
            return TradingSignal(
                symbol=symbol,
                direction=SignalDirection.NEUTRAL,
                strength=SignalStrength.VERY_WEAK,
                confidence=0.1,
                expected_return=0.0,
                forecast_horizon=1,
                signal_components={},
                risk_score=0.5,
                entry_price=current_price
            )

        # Calculate weighted direction
        weighted_direction = 0
        total_confidence = 0
        total_strength = 0
        component_weights = 0

        for component_name, component_signal in signal_components.items():
            weight = self.weights.get_weight(component_name)

            if weight > 0 and isinstance(component_signal, dict):
                direction = component_signal.get('direction', SignalDirection.NEUTRAL)
                strength = component_signal.get('strength', 1)
                confidence = component_signal.get('confidence', 0.1)

                if hasattr(direction, 'value'):
                    direction_value = direction.value
                else:
                    direction_value = direction

                weighted_direction += direction_value * weight * confidence
                total_confidence += confidence * weight
                total_strength += strength * weight
                component_weights += weight

        # Normalize results
        if component_weights > 0:
            weighted_direction /= component_weights
            total_confidence /= component_weights
            total_strength /= component_weights
        else:
            weighted_direction = 0
            total_confidence = 0.1
            total_strength = 1

        # Determine final direction
        if weighted_direction > 0.2:
            final_direction = SignalDirection.LONG
        elif weighted_direction < -0.2:
            final_direction = SignalDirection.SHORT
        else:
            final_direction = SignalDirection.NEUTRAL

        # Determine strength
        if total_strength >= 4:
            strength_category = SignalStrength.VERY_STRONG
        elif total_strength >= 3:
            strength_category = SignalStrength.STRONG
        elif total_strength >= 2:
            strength_category = SignalStrength.MODERATE
        elif total_strength >= 1.5:
            strength_category = SignalStrength.WEAK
        else:
            strength_category = SignalStrength.VERY_WEAK

        # Calculate expected return based on signal strength and direction
        base_return = abs(weighted_direction) * total_strength * 0.002  # Base 0.2% per strength point
        expected_return = base_return * (1 if final_direction == SignalDirection.LONG else -1)

        # Calculate risk score
        signal_consistency = len([s for s in signal_components.values()
                                if isinstance(s, dict) and s.get('direction') == final_direction])
        total_signals = len(signal_components)
        risk_score = 1 - (signal_consistency / max(1, total_signals)) if total_signals > 0 else 0.8

        # Forecast horizon based on signal strength
        forecast_horizon = max(1, int(total_strength * 2))  # 1-10 hours

        return TradingSignal(
            symbol=symbol,
            direction=final_direction,
            strength=strength_category,
            confidence=min(0.95, max(0.05, total_confidence)),
            expected_return=expected_return,
            forecast_horizon=forecast_horizon,
            signal_components=signal_components,
            risk_score=min(0.95, max(0.05, risk_score)),
            entry_price=current_price
        )


class PortfolioSignalManager:
    """Manages signals for entire portfolio of assets."""

    def __init__(self, universe: List[str]):
        self.universe = universe
        self.signal_generator = CompositeSignalGenerator()
        self.signal_history = {}  # Store historical signals

    def generate_portfolio_signals(self, market_data: Dict[str, pd.DataFrame]) -> Dict[str, TradingSignal]:
        """Generate signals for all assets in universe."""
        portfolio_signals = {}

        for symbol in self.universe:
            if symbol in market_data and len(market_data[symbol]) >= 20:
                try:
                    signal = self.signal_generator.generate_comprehensive_signal(
                        symbol, market_data[symbol]
                    )
                    portfolio_signals[symbol] = signal

                    # Store in history
                    if symbol not in self.signal_history:
                        self.signal_history[symbol] = []
                    self.signal_history[symbol].append(signal)

                    # Keep only recent signals
                    self.signal_history[symbol] = self.signal_history[symbol][-100:]

                except Exception as e:
                    print(f"Error generating signal for {symbol}: {e}")
                    # Create neutral signal as fallback
                    portfolio_signals[symbol] = TradingSignal(
                        symbol=symbol,
                        direction=SignalDirection.NEUTRAL,
                        strength=SignalStrength.VERY_WEAK,
                        confidence=0.1,
                        expected_return=0.0,
                        forecast_horizon=1,
                        signal_components={},
                        risk_score=0.8,
                        entry_price=market_data[symbol]['close'].iloc[-1] if symbol in market_data else 0
                    )

        return portfolio_signals

    def get_signal_summary(self, signals: Dict[str, TradingSignal]) -> Dict[str, Any]:
        """Generate summary statistics for portfolio signals."""
        if not signals:
            return {
                'total_signals': 0,
                'long_signals': 0,
                'short_signals': 0,
                'neutral_signals': 0,
                'avg_confidence': 0,
                'avg_expected_return': 0,
                'high_confidence_signals': 0
            }

        long_signals = [s for s in signals.values() if s.direction == SignalDirection.LONG]
        short_signals = [s for s in signals.values() if s.direction == SignalDirection.SHORT]
        neutral_signals = [s for s in signals.values() if s.direction == SignalDirection.NEUTRAL]

        all_signals = list(signals.values())
        avg_confidence = np.mean([s.confidence for s in all_signals])
        avg_expected_return = np.mean([s.expected_return for s in all_signals])
        high_confidence = [s for s in all_signals if s.confidence > 0.7]

        return {
            'total_signals': len(signals),
            'long_signals': len(long_signals),
            'short_signals': len(short_signals),
            'neutral_signals': len(neutral_signals),
            'avg_confidence': avg_confidence,
            'avg_expected_return': avg_expected_return,
            'high_confidence_signals': len(high_confidence),
            'best_long_signal': max(long_signals, key=lambda x: x.signal_score) if long_signals else None,
            'best_short_signal': max(short_signals, key=lambda x: x.signal_score) if short_signals else None
        }