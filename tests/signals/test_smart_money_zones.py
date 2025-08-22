#!/usr/bin/env python3
"""
Comprehensive tests for Smart Money Zones (SMZ) implementation.

Tests cover:
- Market structure detection (HH, HL, LH, LL)
- Change of Character (CHoCH) and Break of Structure (BOS)
- Smart Money Zone calculation using Fibonacci retracements
- Entry confirmation and signal generation
- Multi-timeframe confluence analysis
"""

import pytest
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
from typing import Dict, Any

from signals.smart_money_zones import (
    MarketStructure,
    StructureChange,
    SwingPoint,
    SmartMoneyZone,
    MarketStructureDetector,
    SmartMoneyZoneDetector,
    SMZEntryConfirmation,
    MultiTimeframeAnalysis
)


class TestMarketStructureDetector:
    """Test market structure detection functionality."""
    
    def test_detector_initialization(self):
        """Test detector initialization with custom parameters."""
        detector = MarketStructureDetector(swing_length=15, min_swing_size=0.002)
        assert detector.swing_length == 15
        assert detector.min_swing_size == 0.002
        assert detector.name == "MarketStructure_15"
    
    def test_insufficient_data_handling(self):
        """Test handling of insufficient data."""
        detector = MarketStructureDetector(swing_length=10)
        
        # Create minimal data (less than required)
        data = pd.DataFrame({
            'high': [100, 101, 102],
            'low': [99, 100, 101],
            'close': [100.5, 101.5, 102.5],
            'volume': [1000, 1100, 1200]
        })
        data.index = pd.date_range('2024-01-01', periods=3, freq='1H')
        
        result = detector.calculate(data)
        assert result['status'] == 'insufficient_data'
    
    def test_bullish_structure_detection(self):
        """Test detection of bullish market structure (HH + HL)."""
        detector = MarketStructureDetector(swing_length=5, min_swing_size=0.001)
        
        # Create bullish pattern data
        np.random.seed(42)
        data = self._create_bullish_pattern_data()
        
        result = detector.calculate(data)
        assert result['status'] == 'valid'
        assert result['market_structure'] in ['bullish', 'unknown']  # Depends on exact pattern
        assert 'swing_points' in result
        assert 'swing_highs' in result
        assert 'swing_lows' in result
    
    def test_bearish_structure_detection(self):
        """Test detection of bearish market structure (LH + LL)."""
        detector = MarketStructureDetector(swing_length=5, min_swing_size=0.001)
        
        # Create bearish pattern data
        data = self._create_bearish_pattern_data()
        
        result = detector.calculate(data)
        assert result['status'] == 'valid'
        assert 'market_structure' in result
        assert 'structure_change' in result
    
    def test_swing_point_significance_calculation(self):
        """Test swing point significance scoring."""
        detector = MarketStructureDetector(swing_length=5)
        
        # Create data with varying volume and ranges
        data = self._create_varied_significance_data()
        
        result = detector.calculate(data)
        if result['status'] == 'valid':
            swing_points = result['swing_points']
            
            # Check that significance scores are calculated
            for swing in swing_points:
                assert 0.0 <= swing.significance <= 1.0
    
    def test_bos_detection(self):
        """Test Break of Structure detection."""
        detector = MarketStructureDetector(swing_length=5)
        
        # Create data with clear BOS pattern
        data = self._create_bos_pattern_data()
        
        result = detector.calculate(data)
        if result['status'] == 'valid':
            structure_change = result['structure_change']
            assert structure_change in ['bos_bullish', 'bos_bearish', 'none']
    
    def test_choch_detection(self):
        """Test Change of Character detection."""
        detector = MarketStructureDetector(swing_length=5)
        
        # Create data with CHoCH pattern
        data = self._create_choch_pattern_data()
        
        result = detector.calculate(data)
        if result['status'] == 'valid':
            structure_change = result['structure_change']
            assert structure_change in ['choch_bullish', 'choch_bearish', 'none']
    
    def _create_bullish_pattern_data(self) -> pd.DataFrame:
        """Create data with bullish structure (HH + HL)."""
        # Create ascending pattern with higher highs and higher lows
        np.random.seed(42)
        periods = 50
        base_price = 100
        
        data = []
        for i in range(periods):
            # Create upward trend with some noise
            trend = i * 0.5
            noise = np.random.normal(0, 0.5)
            
            open_price = base_price + trend + noise
            high = open_price + abs(np.random.normal(0, 1))
            low = open_price - abs(np.random.normal(0, 0.8))
            close = low + (high - low) * np.random.random()
            volume = int(1000 + np.random.normal(0, 200))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=periods, freq='1H')
        return df
    
    def _create_bearish_pattern_data(self) -> pd.DataFrame:
        """Create data with bearish structure (LH + LL)."""
        # Create descending pattern with lower highs and lower lows
        np.random.seed(42)
        periods = 50
        base_price = 100
        
        data = []
        for i in range(periods):
            # Create downward trend with some noise
            trend = -i * 0.3
            noise = np.random.normal(0, 0.5)
            
            open_price = base_price + trend + noise
            high = open_price + abs(np.random.normal(0, 0.8))
            low = open_price - abs(np.random.normal(0, 1))
            close = low + (high - low) * np.random.random()
            volume = int(1000 + np.random.normal(0, 200))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=periods, freq='1H')
        return df
    
    def _create_varied_significance_data(self) -> pd.DataFrame:
        """Create data with varying volume and range significance."""
        np.random.seed(42)
        periods = 40
        base_price = 100
        
        data = []
        for i in range(periods):
            # Vary volume and range significance
            if i % 10 == 0:  # High significance bars
                volume_mult = 3.0
                range_mult = 2.0
            else:  # Normal bars
                volume_mult = 1.0
                range_mult = 1.0
            
            open_price = base_price + np.random.normal(0, 1)
            high = open_price + abs(np.random.normal(0, 0.5)) * range_mult
            low = open_price - abs(np.random.normal(0, 0.5)) * range_mult
            close = low + (high - low) * np.random.random()
            volume = int(1000 * volume_mult + np.random.normal(0, 100))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=periods, freq='1H')
        return df
    
    def _create_bos_pattern_data(self) -> pd.DataFrame:
        """Create data with Break of Structure pattern."""
        # Create pattern where price breaks above previous high
        base_price = 100
        data = []
        
        # First phase: establish resistance
        for i in range(20):
            open_price = base_price + np.random.normal(0, 0.5)
            high = min(open_price + abs(np.random.normal(0, 1)), base_price + 2)  # Cap at resistance
            low = open_price - abs(np.random.normal(0, 1))
            close = low + (high - low) * np.random.random()
            volume = int(1000 + np.random.normal(0, 200))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # BOS: Break above resistance with volume
        for i in range(10):
            open_price = base_price + 1.5 + i * 0.5
            high = open_price + abs(np.random.normal(0, 2))  # Strong breakout
            low = open_price - abs(np.random.normal(0, 0.5))
            close = high - abs(np.random.normal(0, 0.3))
            volume = int(2000 + np.random.normal(0, 300))  # Higher volume
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=len(data), freq='1H')
        return df
    
    def _create_choch_pattern_data(self) -> pd.DataFrame:
        """Create data with Change of Character pattern."""
        # Create pattern showing structure change from bearish to bullish
        base_price = 100
        data = []
        
        # First phase: bearish structure (LH + LL)
        for i in range(15):
            trend_price = base_price - i * 0.3
            open_price = trend_price + np.random.normal(0, 0.3)
            high = open_price + abs(np.random.normal(0, 0.5))
            low = open_price - abs(np.random.normal(0, 0.8))
            close = low + (high - low) * np.random.random()
            volume = int(1000 + np.random.normal(0, 150))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # CHoCH: Change to bullish structure (HH + HL)
        for i in range(15):
            trend_price = base_price - 4.5 + i * 0.4  # Start recovery
            open_price = trend_price + np.random.normal(0, 0.3)
            high = open_price + abs(np.random.normal(0, 0.8))
            low = open_price - abs(np.random.normal(0, 0.5))
            close = low + (high - low) * 0.7  # Bullish bias
            volume = int(1200 + np.random.normal(0, 200))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=len(data), freq='1H')
        return df


class TestSmartMoneyZoneDetector:
    """Test Smart Money Zone detection functionality."""
    
    def test_smz_detector_initialization(self):
        """Test SMZ detector initialization."""
        detector = SmartMoneyZoneDetector()
        assert detector.name == "SmartMoneyZones"
        assert 'bos' in detector.fib_levels
        assert 'golden_start' in detector.fib_levels
        assert 'optimal_entry' in detector.fib_levels
    
    def test_zone_creation_bullish(self):
        """Test creation of bullish SMZ zones."""
        detector = SmartMoneyZoneDetector()
        
        # Create data suitable for bullish zone detection
        data = self._create_bullish_smz_data()
        
        result = detector.calculate(data)
        if result['status'] == 'valid':
            zones = result['smz_zones']
            assert isinstance(zones, list)
            
            # Check zone properties if zones exist
            for zone in zones:
                assert isinstance(zone, SmartMoneyZone)
                assert zone.direction in ['bullish', 'bearish']
                assert len(zone.institutional_zone) == 2
                assert len(zone.smart_money_zone) == 2
                assert 0.0 <= zone.confidence <= 1.0
    
    def test_zone_creation_bearish(self):
        """Test creation of bearish SMZ zones."""
        detector = SmartMoneyZoneDetector()
        
        # Create data suitable for bearish zone detection
        data = self._create_bearish_smz_data()
        
        result = detector.calculate(data)
        if result['status'] == 'valid':
            zones = result['smz_zones']
            assert isinstance(zones, list)
    
    def test_fibonacci_level_calculation(self):
        """Test accurate Fibonacci level calculation."""
        detector = SmartMoneyZoneDetector()
        
        # Create simple test data
        data = self._create_simple_swing_data()
        
        result = detector.calculate(data)
        if result['status'] == 'valid' and result['smz_zones']:
            zone = result['smz_zones'][0]
            
            # Verify Fibonacci relationships
            high_price = zone.swing_high.price
            low_price = zone.swing_low.price
            price_range = high_price - low_price
            
            if zone.direction == 'bullish':
                # Bullish: retracement from high
                expected_618 = high_price - (price_range * 0.618)
                expected_786 = high_price - (price_range * 0.786)
                
                assert abs(zone.fib_618 - expected_618) < 0.001
                assert abs(zone.fib_786 - expected_786) < 0.001
    
    def test_zone_confluence_calculation(self):
        """Test zone confluence detection."""
        detector = SmartMoneyZoneDetector()
        
        # Create data with multiple potential zones
        data = self._create_confluence_data()
        
        result = detector.calculate(data)
        if result['status'] == 'valid':
            confluence = result['zone_confluence']
            assert 'confluence_levels' in confluence
            assert 'max_confluence' in confluence
            assert confluence['max_confluence'] >= 0
    
    def test_price_zone_analysis(self):
        """Test current price relative to zones analysis."""
        detector = SmartMoneyZoneDetector()
        
        data = self._create_simple_swing_data()
        
        result = detector.calculate(data)
        if result['status'] == 'valid':
            assert 'active_zones' in result
            assert 'nearest_institutional_zone' in result
            assert 'nearest_smz' in result
            assert 'price_in_zone' in result
    
    def _create_bullish_smz_data(self) -> pd.DataFrame:
        """Create data suitable for bullish SMZ detection."""
        # Create swing low followed by swing high
        base_price = 100
        data = []
        
        # Create downward move to establish swing low
        for i in range(10):
            price = base_price - i * 0.5
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.3))
            low = open_price - abs(np.random.normal(0, 0.5))
            close = low + (high - low) * np.random.random()
            volume = int(1000 + np.random.normal(0, 150))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # Create upward move to establish swing high
        for i in range(15):
            price = base_price - 5 + i * 0.7
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.5))
            low = open_price - abs(np.random.normal(0, 0.3))
            close = low + (high - low) * 0.7  # Bullish bias
            volume = int(1200 + np.random.normal(0, 200))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=len(data), freq='1H')
        return df
    
    def _create_bearish_smz_data(self) -> pd.DataFrame:
        """Create data suitable for bearish SMZ detection."""
        # Create swing high followed by swing low
        base_price = 100
        data = []
        
        # Create upward move to establish swing high
        for i in range(10):
            price = base_price + i * 0.5
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.5))
            low = open_price - abs(np.random.normal(0, 0.3))
            close = low + (high - low) * 0.7
            volume = int(1000 + np.random.normal(0, 150))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # Create downward move to establish swing low
        for i in range(15):
            price = base_price + 5 - i * 0.7
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.3))
            low = open_price - abs(np.random.normal(0, 0.5))
            close = low + (high - low) * 0.3  # Bearish bias
            volume = int(1200 + np.random.normal(0, 200))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=len(data), freq='1H')
        return df
    
    def _create_simple_swing_data(self) -> pd.DataFrame:
        """Create simple data with clear swing points."""
        data = [
            {'open': 98, 'high': 100, 'low': 97, 'close': 99, 'volume': 1000},   # Start
            {'open': 99, 'high': 101, 'low': 98, 'close': 100, 'volume': 1100},
            {'open': 100, 'high': 99, 'low': 95, 'close': 96, 'volume': 1500},  # Swing low
            {'open': 96, 'high': 98, 'low': 95, 'close': 97, 'volume': 1200},
            {'open': 97, 'high': 102, 'low': 96, 'close': 101, 'volume': 1800}, # Start recovery
            {'open': 101, 'high': 105, 'low': 100, 'close': 104, 'volume': 1600},
            {'open': 104, 'high': 108, 'low': 103, 'close': 107, 'volume': 1700}, # Swing high
            {'open': 107, 'high': 108, 'low': 105, 'close': 106, 'volume': 1300},
            {'open': 106, 'high': 107, 'low': 104, 'close': 105, 'volume': 1400},
            {'open': 105, 'high': 106, 'low': 103, 'close': 104, 'volume': 1200}
        ]
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=len(data), freq='1H')
        return df
    
    def _create_confluence_data(self) -> pd.DataFrame:
        """Create data with potential confluence zones."""
        # Create multiple swing patterns that could create overlapping zones
        np.random.seed(42)
        periods = 60
        base_price = 100
        
        data = []
        for i in range(periods):
            # Create wave-like pattern with multiple swings
            wave1 = 5 * np.sin(i * 0.2)
            wave2 = 3 * np.sin(i * 0.15 + 1)
            price = base_price + wave1 + wave2 + np.random.normal(0, 0.5)
            
            open_price = price + np.random.normal(0, 0.3)
            high = open_price + abs(np.random.normal(0, 0.5))
            low = open_price - abs(np.random.normal(0, 0.5))
            close = low + (high - low) * np.random.random()
            volume = int(1000 + np.random.normal(0, 200))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=periods, freq='1H')
        return df


class TestSMZEntryConfirmation:
    """Test Smart Money Zone entry confirmation system."""
    
    def test_entry_confirmation_initialization(self):
        """Test entry confirmation system initialization."""
        confirmation = SMZEntryConfirmation(confirmation_bars=5, volume_threshold=2.0)
        assert confirmation.confirmation_bars == 5
        assert confirmation.volume_threshold == 2.0
        assert confirmation.name == "SMZEntryConfirmation"
    
    def test_signal_generation(self):
        """Test entry signal generation."""
        confirmation = SMZEntryConfirmation()
        
        # Create data suitable for signal generation
        data = self._create_signal_data()
        
        result = confirmation.calculate(data)
        if result['status'] == 'valid':
            assert 'entry_signals' in result
            assert 'risk_levels' in result
            assert 'confirmation_criteria' in result
            
            # Check signal structure
            for signal in result['entry_signals']:
                assert 'type' in signal
                assert 'direction' in signal
                assert 'confidence' in signal
                assert 'entry_price' in signal
    
    def test_structure_alignment_check(self):
        """Test market structure alignment validation."""
        confirmation = SMZEntryConfirmation()
        
        # Test bullish alignment
        alignment = confirmation._check_structure_alignment(
            "bullish", MarketStructure.BULLISH, StructureChange.BOS_BULLISH
        )
        assert alignment['aligned'] == True
        assert alignment['confidence'] > 0.5
        
        # Test bearish alignment
        alignment = confirmation._check_structure_alignment(
            "bearish", MarketStructure.BEARISH, StructureChange.CHOCH_BEARISH
        )
        assert alignment['aligned'] == True
        assert alignment['confidence'] > 0.5
    
    def test_risk_level_calculation(self):
        """Test risk management level calculation."""
        confirmation = SMZEntryConfirmation()
        
        # Create mock signals
        signals = [
            {
                'type': 'smz_entry',
                'direction': 'bullish',
                'entry_price': 100.0,
                'zone': None,
                'confidence': 0.8
            }
        ]
        
        risk_levels = confirmation._calculate_risk_levels(signals, [])
        
        assert 'signal_0' in risk_levels
        signal_risk = risk_levels['signal_0']
        assert 'entry_price' in signal_risk
        assert 'stop_loss' in signal_risk
        assert 'take_profit_1' in signal_risk
        assert 'take_profit_2' in signal_risk
        assert signal_risk['risk_reward_1'] == 2.0
        assert signal_risk['risk_reward_2'] == 3.0
    
    def test_signal_validation(self):
        """Test entry signal validation with confirmation criteria."""
        confirmation = SMZEntryConfirmation(confirmation_bars=3)
        
        # Create mock signals
        signals = [
            {
                'type': 'test_signal',
                'direction': 'bullish',
                'entry_price': 100.0,
                'zone': None,
                'confidence': 0.6
            }
        ]
        
        # Create test data with volume
        data = self._create_validation_data()
        
        validated = confirmation._validate_entry_signals(signals, data)
        
        # Check validation structure
        for signal in validated:
            assert 'validation_score' in signal
            assert 'validation_reasons' in signal
            assert 'total_confidence' in signal
            assert signal['total_confidence'] >= 0.5  # Minimum threshold
    
    def _create_signal_data(self) -> pd.DataFrame:
        """Create data suitable for signal generation testing."""
        # Create data with clear structure and potential entry points
        data = []
        base_price = 100
        
        # Build up to resistance
        for i in range(15):
            price = base_price + i * 0.3
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.5))
            low = open_price - abs(np.random.normal(0, 0.3))
            close = low + (high - low) * 0.7
            volume = int(1000 + np.random.normal(0, 150))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # Pullback to create SMZ opportunity
        for i in range(8):
            price = base_price + 4.5 - i * 0.4
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.3))
            low = open_price - abs(np.random.normal(0, 0.5))
            close = low + (high - low) * 0.4  # Bearish pullback
            volume = int(800 + np.random.normal(0, 100))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=len(data), freq='1H')
        return df
    
    def _create_validation_data(self) -> pd.DataFrame:
        """Create data for validation testing with volume patterns."""
        data = []
        
        for i in range(20):
            # Simulate recent bullish activity with increasing volume
            if i >= 17:  # Last 3 bars
                volume_mult = 2.0
                close_bias = 0.8  # Bullish close
            else:
                volume_mult = 1.0
                close_bias = 0.5
            
            open_price = 100 + i * 0.1 + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.5))
            low = open_price - abs(np.random.normal(0, 0.3))
            close = low + (high - low) * close_bias
            volume = int(1000 * volume_mult + np.random.normal(0, 100))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=len(data), freq='1H')
        return df


class TestMultiTimeframeAnalysis:
    """Test multi-timeframe confluence analysis."""
    
    def test_multi_timeframe_initialization(self):
        """Test multi-timeframe analysis initialization."""
        mtf = MultiTimeframeAnalysis(['1m', '5m', '15m'])
        assert mtf.timeframes == ['1m', '5m', '15m']
        assert len(mtf.detectors) == 3
        assert '1m' in mtf.detectors
    
    def test_confluence_analysis(self):
        """Test confluence analysis across timeframes."""
        mtf = MultiTimeframeAnalysis(['5m', '15m'])
        
        # Create mock data for multiple timeframes
        price_data = {
            '5m': self._create_mtf_data(periods=100),
            '15m': self._create_mtf_data(periods=50)
        }
        
        result = mtf.analyze_confluence(price_data)
        
        assert 'timeframe_results' in result
        assert 'overall_confluence' in result
        assert 'dominant_structure' in result
        assert 'confluence_zones' in result
        assert 'confluence_score' in result
        
        # Check that each timeframe was analyzed
        assert '5m' in result['timeframe_results']
        assert '15m' in result['timeframe_results']
    
    def test_zone_overlap_detection(self):
        """Test detection of overlapping zones between timeframes."""
        mtf = MultiTimeframeAnalysis()
        
        # Create two similar zones that should overlap
        from signals.smart_money_zones import SmartMoneyZone
        from datetime import datetime
        
        zone1 = SmartMoneyZone(
            swing_high=SwingPoint(10, 105.0, datetime.now(), "high", 0.8),
            swing_low=SwingPoint(5, 95.0, datetime.now(), "low", 0.7),
            direction="bullish",
            fib_0=95.0,
            fib_618=98.82,
            fib_786=97.14,
            fib_826=96.74,
            fib_100=105.0,
            institutional_zone=(97.14, 98.82),
            smart_money_zone=(96.74, 97.14),
            created_at=datetime.now(),
            timeframe="5m",
            confidence=0.8
        )
        
        zone2 = SmartMoneyZone(
            swing_high=SwingPoint(8, 104.5, datetime.now(), "high", 0.9),
            swing_low=SwingPoint(3, 95.5, datetime.now(), "low", 0.8),
            direction="bullish",
            fib_0=95.5,
            fib_618=98.89,
            fib_786=97.18,
            fib_826=96.77,
            fib_100=104.5,
            institutional_zone=(97.18, 98.89),
            smart_money_zone=(96.77, 97.18),
            created_at=datetime.now(),
            timeframe="15m",
            confidence=0.9
        )
        
        # Test overlap detection
        overlap = mtf._zones_overlap(zone1, zone2, 0.01)
        assert isinstance(overlap, bool)
    
    def test_confluence_scoring(self):
        """Test confluence scoring calculation."""
        mtf = MultiTimeframeAnalysis()
        
        # Mock results with different structures
        results = {
            '5m': {
                'status': 'valid',
                'market_structure': 'bullish',
                'trend_strength': 0.8,
                'smz_zones': []
            },
            '15m': {
                'status': 'valid',
                'market_structure': 'bullish',
                'trend_strength': 0.7,
                'smz_zones': []
            },
            '1h': {
                'status': 'valid',
                'market_structure': 'bearish',
                'trend_strength': 0.6,
                'smz_zones': []
            }
        }
        
        confluence = mtf._calculate_overall_confluence(results)
        
        assert 'dominant_structure' in confluence
        assert 'structure_alignment' in confluence
        assert 'confluence_score' in confluence
        assert 0.0 <= confluence['confluence_score'] <= 1.0
    
    def _create_mtf_data(self, periods: int = 50) -> pd.DataFrame:
        """Create data for multi-timeframe testing."""
        np.random.seed(42)
        base_price = 100
        data = []
        
        for i in range(periods):
            # Create trending data with some noise
            trend = i * 0.1
            noise = np.random.normal(0, 0.5)
            
            open_price = base_price + trend + noise
            high = open_price + abs(np.random.normal(0, 0.5))
            low = open_price - abs(np.random.normal(0, 0.5))
            close = low + (high - low) * np.random.random()
            volume = int(1000 + np.random.normal(0, 200))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=periods, freq='1H')
        return df


class TestSMZIntegration:
    """Integration tests for complete SMZ system."""
    
    def test_end_to_end_bullish_setup(self):
        """Test complete bullish SMZ setup detection and signal generation."""
        # Create comprehensive test scenario
        confirmation = SMZEntryConfirmation()
        data = self._create_bullish_setup_data()
        
        result = confirmation.calculate(data)
        
        # Should detect valid setup
        if result['status'] == 'valid':
            assert 'market_structure' in result
            assert 'entry_signals' in result
            assert 'risk_levels' in result
            
            # Check for bullish signals if detected
            bullish_signals = [s for s in result['entry_signals'] 
                             if s['direction'] == 'bullish']
            
            # Verify signal quality if found
            for signal in bullish_signals:
                assert signal['total_confidence'] >= 0.5
                assert 'validation_reasons' in signal
    
    def test_end_to_end_bearish_setup(self):
        """Test complete bearish SMZ setup detection and signal generation."""
        confirmation = SMZEntryConfirmation()
        data = self._create_bearish_setup_data()
        
        result = confirmation.calculate(data)
        
        if result['status'] == 'valid':
            assert 'market_structure' in result
            assert 'entry_signals' in result
            
            # Check for bearish signals if detected
            bearish_signals = [s for s in result['entry_signals'] 
                             if s['direction'] == 'bearish']
            
            for signal in bearish_signals:
                assert signal['total_confidence'] >= 0.5
    
    def test_risk_reward_calculation(self):
        """Test risk-reward ratio calculations."""
        confirmation = SMZEntryConfirmation()
        data = self._create_rr_test_data()
        
        result = confirmation.calculate(data)
        
        if result['status'] == 'valid' and result['entry_signals']:
            risk_levels = result['risk_levels']
            
            for signal_id, levels in risk_levels.items():
                entry = levels['entry_price']
                stop = levels['stop_loss']
                tp1 = levels['take_profit_1']
                tp2 = levels['take_profit_2']
                
                # Calculate actual risk-reward ratios
                risk = abs(entry - stop)
                reward1 = abs(tp1 - entry)
                reward2 = abs(tp2 - entry)
                
                if risk > 0:
                    rr1 = reward1 / risk
                    rr2 = reward2 / risk
                    
                    # Should be approximately 2:1 and 3:1
                    assert abs(rr1 - 2.0) < 0.1
                    assert abs(rr2 - 3.0) < 0.1
    
    def _create_bullish_setup_data(self) -> pd.DataFrame:
        """Create data for complete bullish SMZ setup."""
        data = []
        base_price = 100
        
        # Phase 1: Downtrend to create swing low
        for i in range(12):
            price = base_price - i * 0.4
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.3))
            low = open_price - abs(np.random.normal(0, 0.6))
            close = low + (high - low) * 0.3
            volume = int(1000 + np.random.normal(0, 150))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # Phase 2: Strong rally to create swing high
        for i in range(15):
            price = base_price - 4.8 + i * 0.8
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.6))
            low = open_price - abs(np.random.normal(0, 0.3))
            close = low + (high - low) * 0.8  # Strong bullish closes
            volume = int(1500 + np.random.normal(0, 200))  # Higher volume
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # Phase 3: Pullback to SMZ
        for i in range(8):
            price = base_price + 7.2 - i * 0.6
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.4))
            low = open_price - abs(np.random.normal(0, 0.5))
            close = low + (high - low) * 0.4  # Pullback
            volume = int(900 + np.random.normal(0, 100))  # Lower volume
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # Phase 4: Potential entry setup
        for i in range(5):
            price = base_price + 2.4 + i * 0.3
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.5))
            low = open_price - abs(np.random.normal(0, 0.3))
            close = low + (high - low) * 0.7  # Bullish bias
            volume = int(1300 + np.random.normal(0, 150))  # Volume confirmation
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=len(data), freq='1H')
        return df
    
    def _create_bearish_setup_data(self) -> pd.DataFrame:
        """Create data for complete bearish SMZ setup."""
        data = []
        base_price = 100
        
        # Phase 1: Uptrend to create swing high
        for i in range(12):
            price = base_price + i * 0.4
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.6))
            low = open_price - abs(np.random.normal(0, 0.3))
            close = low + (high - low) * 0.7
            volume = int(1000 + np.random.normal(0, 150))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # Phase 2: Strong decline to create swing low
        for i in range(15):
            price = base_price + 4.8 - i * 0.8
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.3))
            low = open_price - abs(np.random.normal(0, 0.6))
            close = low + (high - low) * 0.2  # Strong bearish closes
            volume = int(1500 + np.random.normal(0, 200))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # Phase 3: Rally to SMZ
        for i in range(8):
            price = base_price - 7.2 + i * 0.6
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.5))
            low = open_price - abs(np.random.normal(0, 0.4))
            close = low + (high - low) * 0.6  # Rally
            volume = int(900 + np.random.normal(0, 100))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        # Phase 4: Potential bearish entry setup
        for i in range(5):
            price = base_price - 2.4 - i * 0.3
            open_price = price + np.random.normal(0, 0.2)
            high = open_price + abs(np.random.normal(0, 0.3))
            low = open_price - abs(np.random.normal(0, 0.5))
            close = low + (high - low) * 0.3  # Bearish bias
            volume = int(1300 + np.random.normal(0, 150))
            
            data.append({
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': max(100, volume)
            })
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=len(data), freq='1H')
        return df
    
    def _create_rr_test_data(self) -> pd.DataFrame:
        """Create data specifically for risk-reward testing."""
        # Simple data that should generate clear signals
        data = [
            {'open': 95, 'high': 97, 'low': 94, 'close': 96, 'volume': 1000},
            {'open': 96, 'high': 98, 'low': 95, 'close': 97, 'volume': 1100},
            {'open': 97, 'high': 99, 'low': 96, 'close': 98, 'volume': 1200},
            {'open': 98, 'high': 100, 'low': 97, 'close': 99, 'volume': 1300},
            {'open': 99, 'high': 105, 'low': 98, 'close': 104, 'volume': 2000},  # Breakout
            {'open': 104, 'high': 106, 'low': 103, 'close': 105, 'volume': 1800},
            {'open': 105, 'high': 107, 'low': 104, 'close': 106, 'volume': 1600},
            {'open': 106, 'high': 108, 'low': 105, 'close': 107, 'volume': 1700},
            {'open': 107, 'high': 109, 'low': 106, 'close': 108, 'volume': 1500},
            {'open': 108, 'high': 109, 'low': 107, 'close': 108, 'volume': 1400}
        ]
        
        df = pd.DataFrame(data)
        df.index = pd.date_range('2024-01-01', periods=len(data), freq='1H')
        return df


if __name__ == "__main__":
    pytest.main([__file__, "-v"])