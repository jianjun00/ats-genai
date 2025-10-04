"""
Support/Resistance Feature Extractor for Training Data

Post-facto analysis of historical price action to identify and label
support/resistance levels and tests for training data generation.

Integrates with MultiTimeframeFeatureExtractor to add S/R features
to training examples.
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import gin

logger = logging.getLogger(__name__)

class SRType(Enum):
    """Support/Resistance type"""
    SUPPORT = "support"
    RESISTANCE = "resistance"

class SRTestOutcome(Enum):
    """S/R test outcomes for labeling"""
    HOLD_STRONG = "hold_strong"      # Bounced with high volume
    HOLD_WEAK = "hold_weak"         # Bounced weakly
    BREAK_CLEAN = "break_clean"     # Clean break through
    PENETRATION = "penetration"     # Minor penetration but held
    NO_TEST = "no_test"            # No test occurred

@dataclass
class SRLevel:
    """Simple S/R level for feature extraction"""
    price: float
    sr_type: SRType
    strength: float  # 0.0 to 1.0
    first_seen: datetime
    test_count: int = 0

@dataclass
class SRTest:
    """S/R test for labeling"""
    level_price: float
    test_price: float
    outcome: SRTestOutcome
    confidence: float
    volume_spike: float
    bars_ago: int  # How many bars ago this test occurred

@gin.configurable
class SupportResistanceFeatureExtractor:
    """
    Extract S/R features by analyzing historical price action.

    For each training example, looks back at recent price history to:
    1. Identify significant S/R levels
    2. Detect tests of those levels
    3. Label outcomes and create features
    """

    def __init__(self,
                 lookback_periods: Optional[Dict[str, int]] = None,
                 min_level_strength: float = 0.3,
                 test_proximity_pct: float = 0.005,  # 0.5% proximity to level
                 volume_spike_threshold: float = 1.5):
        """
        Initialize S/R feature extractor.

        Args:
            lookback_periods: Number of periods to look back for each timeframe
            min_level_strength: Minimum strength for a level to be considered
            test_proximity_pct: Price must be within this % of level to be a test
            volume_spike_threshold: Volume multiplier to be considered spike
        """
        self.lookback_periods = lookback_periods or {
            '1m': 240,   # 4 hours of minute data
            '5m': 144,   # 12 hours of 5m data
            '15m': 96,   # 24 hours of 15m data
            '1h': 48,    # 2 days of hourly data
            '1d': 30,    # 30 days of daily data
        }

        self.min_level_strength = min_level_strength
        self.test_proximity_pct = test_proximity_pct
        self.volume_spike_threshold = volume_spike_threshold

        logger.debug(f"SRFeatureExtractor initialized with lookbacks: {self.lookback_periods}")

    def extract_sr_features(self, data: pd.DataFrame, timeframe: str,
                           current_time: Optional[datetime] = None) -> Dict[str, float]:
        """
        Extract S/R features from historical price data.

        Args:
            data: Historical OHLCV data (most recent last)
            timeframe: Timeframe identifier (1m, 5m, 1h, 1d, etc.)
            current_time: Current timestamp for the training example

        Returns:
            Dictionary of S/R features
        """
        if data.empty or len(data) < 10:
            return self._empty_features(timeframe)

        try:
            # Limit data to lookback period
            lookback = self.lookback_periods.get(timeframe, 50)
            recent_data = data.tail(lookback).copy()

            if len(recent_data) < 10:
                return self._empty_features(timeframe)

            # 1. Identify S/R levels from historical highs/lows
            levels = self._identify_sr_levels(recent_data)

            # 2. Find tests of those levels in recent price action
            tests = self._find_sr_tests(recent_data, levels)

            # 3. Create features from levels and tests
            features = self._create_sr_features(levels, tests, recent_data, timeframe)

            return features

        except Exception as e:
            logger.error(f"Error extracting S/R features for {timeframe}: {e}")
            return self._empty_features(timeframe)

    def _identify_sr_levels(self, data: pd.DataFrame) -> List[SRLevel]:
        """Identify significant S/R levels from swing highs/lows."""
        levels = []

        if len(data) < 10:
            return levels

        # Find swing highs and lows using simple pivot detection
        highs = data['high'].values
        lows = data['low'].values

        # Simple pivot detection (look for peaks/troughs with 3-period window)
        for i in range(2, len(data) - 2):
            # Check for swing high (resistance)
            if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and
                highs[i] > highs[i+1] and highs[i] > highs[i+2]):

                # Calculate strength based on how distinct this level is
                strength = self._calculate_level_strength(highs[i], data)

                if strength >= self.min_level_strength:
                    levels.append(SRLevel(
                        price=highs[i],
                        sr_type=SRType.RESISTANCE,
                        strength=strength,
                        first_seen=data.iloc[i].get('timestamp', datetime.now()),
                        test_count=0
                    ))

            # Check for swing low (support)
            if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and
                lows[i] < lows[i+1] and lows[i] < lows[i+2]):

                strength = self._calculate_level_strength(lows[i], data)

                if strength >= self.min_level_strength:
                    levels.append(SRLevel(
                        price=lows[i],
                        sr_type=SRType.SUPPORT,
                        strength=strength,
                        first_seen=data.iloc[i].get('timestamp', datetime.now()),
                        test_count=0
                    ))

        # Add psychological levels (round numbers)
        current_price = data['close'].iloc[-1]
        for level_price in self._get_psychological_levels(current_price):
            strength = 0.4  # Moderate strength for psychological levels

            # Determine if support or resistance based on current price
            sr_type = SRType.SUPPORT if level_price < current_price else SRType.RESISTANCE

            levels.append(SRLevel(
                price=level_price,
                sr_type=sr_type,
                strength=strength,
                first_seen=data.iloc[0].get('timestamp', datetime.now()),
                test_count=0
            ))

        # Remove duplicate levels that are very close to each other
        levels = self._deduplicate_levels(levels)

        # Sort by strength
        levels.sort(key=lambda x: x.strength, reverse=True)

        # Keep only top levels to avoid noise
        return levels[:10]

    def _calculate_level_strength(self, price: float, data: pd.DataFrame) -> float:
        """Calculate strength of an S/R level based on how often it's tested."""
        if data.empty:
            return 0.0

        # Count how many times price came near this level
        proximity = price * self.test_proximity_pct

        touches = 0
        for _, row in data.iterrows():
            high = row.get('high', 0)
            low = row.get('low', 0)

            # Check if price range touched this level
            if low <= price + proximity and high >= price - proximity:
                touches += 1

        # Strength based on number of touches and data length
        strength = min(1.0, touches / (len(data) * 0.1))  # Scale by data length
        return max(0.1, strength)  # Minimum strength

    def _get_psychological_levels(self, current_price: float) -> List[float]:
        """Get psychological round number levels near current price."""
        levels = []

        # Find appropriate round number based on price range
        if current_price < 10:
            step = 1.0
        elif current_price < 100:
            step = 5.0
        else:
            step = 10.0

        # Add round numbers above and below current price
        base = int(current_price / step) * step
        for i in range(-2, 3):  # 2 levels above and below
            level = base + (i * step)
            if level > 0 and abs(level - current_price) / current_price < 0.1:  # Within 10%
                levels.append(level)

        return levels

    def _deduplicate_levels(self, levels: List[SRLevel]) -> List[SRLevel]:
        """Remove levels that are too close to each other."""
        if not levels:
            return levels

        unique_levels = []

        for level in sorted(levels, key=lambda x: x.price):
            is_unique = True

            for existing in unique_levels:
                price_diff_pct = abs(level.price - existing.price) / existing.price
                if price_diff_pct < self.test_proximity_pct * 2:  # Too close
                    is_unique = False
                    # Keep the stronger level
                    if level.strength > existing.strength:
                        unique_levels.remove(existing)
                        unique_levels.append(level)
                    break

            if is_unique:
                unique_levels.append(level)

        return unique_levels

    def _find_sr_tests(self, data: pd.DataFrame, levels: List[SRLevel]) -> List[SRTest]:
        """Find tests of S/R levels in recent price action."""
        tests = []

        if not levels or len(data) < 5:
            return tests

        # Calculate volume moving average for spike detection
        volumes = data['volume'].fillna(0)
        volume_ma = volumes.rolling(window=min(10, len(volumes))).mean()

        # Look for price tests of each level
        for level in levels:
            proximity = level.price * self.test_proximity_pct

            for i, (_, row) in enumerate(data.iterrows()):
                high = row.get('high', 0)
                low = row.get('low', 0)
                close = row.get('close', 0)
                volume = row.get('volume', 0)

                # Check if price tested this level
                outcome = SRTestOutcome.NO_TEST
                confidence = 0.0

                if level.sr_type == SRType.SUPPORT:
                    if low <= level.price + proximity:  # Came close to support
                        outcome, confidence = self._determine_test_outcome(
                            level, row, data.iloc[i:i+5] if i+5 < len(data) else data.iloc[i:], True)
                elif level.sr_type == SRType.RESISTANCE:
                    if high >= level.price - proximity:  # Came close to resistance
                        outcome, confidence = self._determine_test_outcome(
                            level, row, data.iloc[i:i+5] if i+5 < len(data) else data.iloc[i:], False)

                if outcome != SRTestOutcome.NO_TEST:
                    volume_spike = volume / volume_ma.iloc[i] if volume_ma.iloc[i] > 0 else 1.0

                    tests.append(SRTest(
                        level_price=level.price,
                        test_price=close,
                        outcome=outcome,
                        confidence=confidence,
                        volume_spike=volume_spike,
                        bars_ago=len(data) - i - 1
                    ))

                    level.test_count += 1

        return tests

    def _determine_test_outcome(self, level: SRLevel, test_bar: pd.Series,
                               following_bars: pd.DataFrame, is_support: bool) -> Tuple[SRTestOutcome, float]:
        """Determine the outcome of an S/R level test."""
        if following_bars.empty:
            return SRTestOutcome.NO_TEST, 0.0

        test_price = test_bar.get('close', 0)
        test_low = test_bar.get('low', 0)
        test_high = test_bar.get('high', 0)

        # Check what happened after the test
        if len(following_bars) >= 3:
            next_closes = following_bars['close'].head(3)

            if is_support:
                # Support test
                penetration = (level.price - test_low) / level.price

                if penetration < 0.002:  # Less than 0.2% penetration
                    if next_closes.min() > level.price * 0.998:  # Held well
                        return SRTestOutcome.HOLD_STRONG, 0.8
                    else:
                        return SRTestOutcome.HOLD_WEAK, 0.6
                elif penetration < 0.01:  # Minor penetration
                    if next_closes.min() > level.price * 0.995:
                        return SRTestOutcome.PENETRATION, 0.7
                    else:
                        return SRTestOutcome.BREAK_CLEAN, 0.8
                else:  # Significant break
                    return SRTestOutcome.BREAK_CLEAN, 0.9
            else:
                # Resistance test
                penetration = (test_high - level.price) / level.price

                if penetration < 0.002:
                    if next_closes.max() < level.price * 1.002:
                        return SRTestOutcome.HOLD_STRONG, 0.8
                    else:
                        return SRTestOutcome.HOLD_WEAK, 0.6
                elif penetration < 0.01:
                    if next_closes.max() < level.price * 1.005:
                        return SRTestOutcome.PENETRATION, 0.7
                    else:
                        return SRTestOutcome.BREAK_CLEAN, 0.8
                else:
                    return SRTestOutcome.BREAK_CLEAN, 0.9

        return SRTestOutcome.NO_TEST, 0.0

    def _create_sr_features(self, levels: List[SRLevel], tests: List[SRTest],
                           data: pd.DataFrame, timeframe: str) -> Dict[str, float]:
        """Create feature dictionary from S/R levels and tests."""
        current_price = data['close'].iloc[-1] if not data.empty else 0.0
        features = {}

        # Current S/R context features
        nearest_support = self._find_nearest_level(current_price, levels, SRType.SUPPORT)
        nearest_resistance = self._find_nearest_level(current_price, levels, SRType.RESISTANCE)

        # Distance to nearest levels
        if nearest_support:
            support_distance = (current_price - nearest_support.price) / current_price
            features[f'{timeframe}_support_distance'] = support_distance
            features[f'{timeframe}_support_strength'] = nearest_support.strength
        else:
            features[f'{timeframe}_support_distance'] = 0.1  # Far away
            features[f'{timeframe}_support_strength'] = 0.0

        if nearest_resistance:
            resistance_distance = (nearest_resistance.price - current_price) / current_price
            features[f'{timeframe}_resistance_distance'] = resistance_distance
            features[f'{timeframe}_resistance_strength'] = nearest_resistance.strength
        else:
            features[f'{timeframe}_resistance_distance'] = 0.1
            features[f'{timeframe}_resistance_strength'] = 0.0

        # Recent S/R test outcomes (look at last 10 bars)
        recent_tests = [t for t in tests if t.bars_ago <= 10]

        # Count tests by outcome
        outcome_counts = {outcome: 0 for outcome in SRTestOutcome}
        total_confidence = 0.0
        avg_volume_spike = 1.0

        for test in recent_tests:
            outcome_counts[test.outcome] += 1
            total_confidence += test.confidence

        if recent_tests:
            avg_confidence = total_confidence / len(recent_tests)
            avg_volume_spike = sum(t.volume_spike for t in recent_tests) / len(recent_tests)
        else:
            avg_confidence = 0.0

        # Add test outcome features
        features[f'{timeframe}_recent_tests'] = len(recent_tests)
        features[f'{timeframe}_tests_confidence'] = avg_confidence
        features[f'{timeframe}_tests_volume_spike'] = avg_volume_spike

        features[f'{timeframe}_hold_strong_tests'] = outcome_counts[SRTestOutcome.HOLD_STRONG]
        features[f'{timeframe}_break_clean_tests'] = outcome_counts[SRTestOutcome.BREAK_CLEAN]
        features[f'{timeframe}_penetration_tests'] = outcome_counts[SRTestOutcome.PENETRATION]

        # S/R level density (how many levels in price range)
        if current_price > 0:
            price_range = current_price * 0.05  # 5% range
            levels_in_range = len([l for l in levels if abs(l.price - current_price) <= price_range])
            features[f'{timeframe}_sr_level_density'] = levels_in_range
        else:
            features[f'{timeframe}_sr_level_density'] = 0

        # Binary flags for S/R context
        features[f'{timeframe}_near_support'] = 1.0 if (nearest_support and support_distance < 0.02) else 0.0
        features[f'{timeframe}_near_resistance'] = 1.0 if (nearest_resistance and resistance_distance < 0.02) else 0.0

        return features

    def _find_nearest_level(self, price: float, levels: List[SRLevel], sr_type: SRType) -> Optional[SRLevel]:
        """Find nearest S/R level of given type."""
        matching_levels = [l for l in levels if l.sr_type == sr_type]

        if not matching_levels:
            return None

        if sr_type == SRType.SUPPORT:
            # Find highest support below current price
            below_levels = [l for l in matching_levels if l.price < price]
            return max(below_levels, key=lambda x: x.price) if below_levels else None
        else:
            # Find lowest resistance above current price
            above_levels = [l for l in matching_levels if l.price > price]
            return min(above_levels, key=lambda x: x.price) if above_levels else None

    def _empty_features(self, timeframe: str) -> Dict[str, float]:
        """Return empty/default features when insufficient data."""
        return {
            f'{timeframe}_support_distance': 0.1,
            f'{timeframe}_support_strength': 0.0,
            f'{timeframe}_resistance_distance': 0.1,
            f'{timeframe}_resistance_strength': 0.0,
            f'{timeframe}_recent_tests': 0,
            f'{timeframe}_tests_confidence': 0.0,
            f'{timeframe}_tests_volume_spike': 1.0,
            f'{timeframe}_hold_strong_tests': 0,
            f'{timeframe}_break_clean_tests': 0,
            f'{timeframe}_penetration_tests': 0,
            f'{timeframe}_sr_level_density': 0,
            f'{timeframe}_near_support': 0.0,
            f'{timeframe}_near_resistance': 0.0,
        }


def create_sr_feature_extractor(config: Optional[Dict] = None) -> SupportResistanceFeatureExtractor:
    """Factory function to create S/R feature extractor."""
    return SupportResistanceFeatureExtractor(**(config or {}))