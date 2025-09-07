#!/usr/bin/env python3
"""
Comprehensive tests for Support/Resistance detection algorithms
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict
from decimal import Decimal

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from events.analysis.support_resistance_detector import (
    SupportResistanceDetector, SRLevel, SRTest, SREvent,
    SRType, SRLevelType, SRTestOutcome, Timeframe
)

class TestSupportResistanceDetector:
    """Test suite for SupportResistanceDetector"""

    @pytest.fixture
    def detector(self):
        """Create detector instance with test configuration"""
        config = {
            'pivot_lookback': 10,
            'cluster_epsilon': 0.02,
            'proximity_tolerance': 0.005,
            'break_threshold': 0.01,
            'psychological_levels': True,
            'volume_profile_levels': True,
            'min_level_strength': 0.3,
            'confluence_distance': 0.01
        }
        return SupportResistanceDetector(config)

    @pytest.fixture
    def sample_ohlcv_uptrend(self):
        """Generate sample OHLCV data with clear uptrend and S/R levels"""
        np.random.seed(42)
        
        dates = pd.date_range(start='2024-01-01', end='2024-06-30', freq='D')
        base_prices = np.linspace(100, 150, len(dates))
        
        # Add realistic OHLC structure with support/resistance levels
        data = []
        for i, (date, base_price) in enumerate(zip(dates, base_prices)):
            # Add some realistic price action around key levels
            if 119.5 <= base_price <= 120.5:  # Support level at 120
                variation = np.random.normal(0, 0.5)
            elif 149.5 <= base_price <= 150.5:  # Resistance level at 150
                variation = np.random.normal(0, 0.5)
            else:
                variation = np.random.normal(0, 1.5)
            
            close = base_price + variation
            open_price = close + np.random.normal(0, 0.5)
            high = max(open_price, close) + abs(np.random.normal(0, 0.8))
            low = min(open_price, close) - abs(np.random.normal(0, 0.8))
            volume = int(np.random.lognormal(15, 0.5))
            
            data.append({
                'timestamp': date,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })
        
        return pd.DataFrame(data)

    @pytest.fixture
    def sample_ohlcv_ranging(self):
        """Generate sample OHLCV data with ranging market and multiple S/R levels"""
        np.random.seed(123)
        
        dates = pd.date_range(start='2024-01-01', end='2024-06-30', freq='D')
        
        # Create ranging market between 95-105 with clear S/R levels
        data = []
        for i, date in enumerate(dates):
            # Oscillate between support (95-96) and resistance (104-105)
            cycle_position = (i % 40) / 40.0  # 40-day cycle
            base_price = 95 + 10 * (0.5 + 0.4 * np.sin(2 * np.pi * cycle_position))
            
            # Add noise but respect S/R levels
            variation = np.random.normal(0, 0.3)
            close = base_price + variation
            
            # Clamp to S/R levels
            if close < 95.5:
                close = 95.5 + abs(np.random.normal(0, 0.2))  # Bounce off support
            elif close > 104.5:
                close = 104.5 - abs(np.random.normal(0, 0.2))  # Reject at resistance
            
            open_price = close + np.random.normal(0, 0.3)
            high = max(open_price, close) + abs(np.random.normal(0, 0.5))
            low = min(open_price, close) - abs(np.random.normal(0, 0.5))
            volume = int(np.random.lognormal(15, 0.3))
            
            data.append({
                'timestamp': date,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })
        
        return pd.DataFrame(data)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_pivot_point_detection(self, detector, sample_ohlcv_uptrend):
        """Test pivot point S/R level detection"""
        levels = await detector.detect_sr_levels('AAPL', sample_ohlcv_uptrend, Timeframe.DAILY)
        
        # Should detect multiple levels
        assert len(levels) > 0, "Should detect at least some S/R levels"
        
        # Check for pivot-based levels
        pivot_levels = [l for l in levels if l.level_type == SRLevelType.PIVOT_POINT]
        assert len(pivot_levels) > 0, "Should detect pivot-based levels"
        
        # Verify level properties
        for level in pivot_levels:
            assert level.strength > 0, "Level strength should be positive"
            assert level.confidence > 0, "Level confidence should be positive"
            assert level.test_count >= 0, "Test count should be non-negative"
            assert level.first_established is not None, "First established should be set"

    @pytest.mark.asyncio
    async def test_psychological_level_detection(self, detector, sample_ohlcv_uptrend):
        """Test psychological level detection (round numbers)"""
        levels = await detector.detect_sr_levels('MSFT', sample_ohlcv_uptrend, Timeframe.DAILY)
        
        # Should detect psychological levels (round numbers like 120, 150)
        psychological_levels = [l for l in levels if l.level_type == SRLevelType.PSYCHOLOGICAL]
        assert len(psychological_levels) > 0, "Should detect psychological levels"
        
        # Check that detected levels are near round numbers
        for level in psychological_levels:
            price = level.price
            # Should be near multiples of 5 or 10
            assert (abs(price % 5) < 0.5 or abs(price % 10) < 1.0), f"Price {price} should be near round number"

    @pytest.mark.asyncio
    async def test_confluence_level_detection(self, detector, sample_ohlcv_ranging):
        """Test confluence level detection (multiple indicators at same price)"""
        # Set up detector to create confluence conditions
        detector.config['confluence_distance'] = 1.0  # Allow wider confluence
        
        levels = await detector.detect_sr_levels('GOOGL', sample_ohlcv_ranging, Timeframe.DAILY)
        
        # Should detect some levels
        assert len(levels) > 0, "Should detect S/R levels"
        
        # Check for proper level classification
        for level in levels:
            assert level.sr_type in [SRType.SUPPORT, SRType.RESISTANCE], "Should classify as support or resistance"

    @pytest.mark.asyncio
    async def test_sr_test_detection(self, detector, sample_ohlcv_ranging):
        """Test S/R level test detection"""
        # First detect levels
        levels = await detector.detect_sr_levels('TSLA', sample_ohlcv_ranging, Timeframe.DAILY)
        assert len(levels) > 0, "Should detect levels first"
        
        # Then detect tests
        tests = await detector.detect_sr_tests('TSLA', sample_ohlcv_ranging, levels)
        
        # Should find some tests
        assert len(tests) > 0, "Should detect level tests"
        
        # Verify test properties
        for test in tests:
            assert test.test_datetime is not None, "Test should have datetime"
            assert test.test_price > 0, "Test price should be positive"
            assert test.outcome in SRTestOutcome, "Test outcome should be valid"
            assert 0 <= test.confidence <= 1, "Confidence should be between 0 and 1"
            assert test.volume_spike >= 0, "Volume spike should be non-negative"

    @pytest.mark.asyncio
    async def test_level_strength_calculation(self, detector, sample_ohlcv_ranging):
        """Test S/R level strength calculation"""
        levels = await detector.detect_sr_levels('NVDA', sample_ohlcv_ranging, Timeframe.DAILY)
        
        for level in levels:
            # Strength should be between 0 and 1
            assert 0 <= level.strength <= 1, f"Level strength {level.strength} should be between 0 and 1"
            
            # Higher test counts should generally correlate with higher strength
            # (though this isn't a strict rule due to other factors)
            if level.test_count > 0:
                assert level.strength > 0.1, "Tested levels should have some strength"

    @pytest.mark.asyncio
    async def test_volume_confirmation(self, detector, sample_ohlcv_uptrend):
        """Test volume-based level confirmation"""
        # Create data with volume spikes at key levels
        data = sample_ohlcv_uptrend.copy()
        
        # Add volume spikes at specific price levels
        data.loc[data['close'].between(119, 121), 'volume'] *= 3  # Volume spike at 120 level
        data.loc[data['close'].between(149, 151), 'volume'] *= 2.5  # Volume spike at 150 level
        
        levels = await detector.detect_sr_levels('AMD', data, Timeframe.DAILY)
        
        # Should detect volume-confirmed levels
        volume_confirmed = [l for l in levels if l.volume_confirmation]
        assert len(volume_confirmed) > 0, "Should detect volume-confirmed levels"
        
        # Volume-confirmed levels should have higher strength
        for level in volume_confirmed:
            assert level.strength > 0.4, "Volume-confirmed levels should be stronger"

    @pytest.mark.asyncio
    async def test_timeframe_specific_detection(self, detector):
        """Test that detection works for different timeframes"""
        # Generate data for different timeframes
        timeframes = [Timeframe.INTRADAY_5M, Timeframe.INTRADAY_1H, Timeframe.DAILY, Timeframe.WEEKLY]
        
        for timeframe in timeframes:
            # Generate appropriate data frequency
            if timeframe == Timeframe.INTRADAY_5M:
                dates = pd.date_range(start='2024-06-01', end='2024-06-02', freq='5T')
            elif timeframe == Timeframe.INTRADAY_1H:
                dates = pd.date_range(start='2024-05-01', end='2024-06-01', freq='H')
            elif timeframe == Timeframe.DAILY:
                dates = pd.date_range(start='2024-01-01', end='2024-06-01', freq='D')
            else:  # WEEKLY
                dates = pd.date_range(start='2023-01-01', end='2024-06-01', freq='W')
            
            # Generate simple trending data
            base_prices = np.linspace(100, 120, len(dates))
            data = []
            for date, base_price in zip(dates, base_prices):
                variation = np.random.normal(0, 1)
                close = base_price + variation
                data.append({
                    'timestamp': date,
                    'open': close + np.random.normal(0, 0.5),
                    'high': close + abs(np.random.normal(0, 1)),
                    'low': close - abs(np.random.normal(0, 1)),
                    'close': close,
                    'volume': int(np.random.lognormal(15, 0.3))
                })
            
            df = pd.DataFrame(data)
            
            # Should work for each timeframe
            levels = await detector.detect_sr_levels('TEST', df, timeframe)
            
            # Should detect some levels (though exact count depends on data)
            assert isinstance(levels, list), f"Should return list for {timeframe.value}"
            
            # If levels detected, they should have proper timeframe
            for level in levels:
                assert level.timeframe == timeframe, f"Level timeframe should match {timeframe.value}"

    @pytest.mark.asyncio
    async def test_invalid_data_handling(self, detector):
        """Test handling of invalid or insufficient data"""
        
        # Empty DataFrame
        empty_df = pd.DataFrame()
        levels = await detector.detect_sr_levels('EMPTY', empty_df, Timeframe.DAILY)
        assert levels == [], "Should return empty list for empty data"
        
        # Insufficient data points
        small_df = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=5, freq='D'),
            'open': [100, 101, 102, 103, 104],
            'high': [101, 102, 103, 104, 105],
            'low': [99, 100, 101, 102, 103],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5],
            'volume': [1000, 1100, 1200, 1300, 1400]
        })
        
        levels = await detector.detect_sr_levels('SMALL', small_df, Timeframe.DAILY)
        # Should handle gracefully (may return empty list or minimal levels)
        assert isinstance(levels, list), "Should return list even for small data"

    @pytest.mark.asyncio
    async def test_level_clustering(self, detector):
        """Test that nearby levels are properly clustered"""
        # Create data with closely spaced levels that should be clustered
        dates = pd.date_range(start='2024-01-01', end='2024-06-30', freq='D')
        
        data = []
        for i, date in enumerate(dates):
            # Create levels at 100.0, 100.1, 100.2 (should cluster)
            # and 105.0, 105.1 (should cluster)
            base_price = 100 + (i % 50) * 0.1  # Oscillate in tight ranges
            
            if i % 50 < 20:  # First range: 100.0-102.0
                price = 100 + (i % 20) * 0.1
            else:  # Second range: 105.0-107.0
                price = 105 + ((i % 50) - 20) * 0.1
            
            variation = np.random.normal(0, 0.05)  # Small variation
            close = price + variation
            
            data.append({
                'timestamp': date,
                'open': close + np.random.normal(0, 0.02),
                'high': close + abs(np.random.normal(0, 0.05)),
                'low': close - abs(np.random.normal(0, 0.05)),
                'close': close,
                'volume': int(np.random.lognormal(15, 0.2))
            })
        
        df = pd.DataFrame(data)
        
        # Detect levels with tight clustering
        detector.config['cluster_epsilon'] = 0.15  # Allow clustering of nearby levels
        levels = await detector.detect_sr_levels('CLUSTER', df, Timeframe.DAILY)
        
        # Should cluster nearby levels, resulting in fewer distinct levels
        prices = [level.price for level in levels]
        unique_prices = set(round(p, 1) for p in prices)  # Round to 0.1 precision
        
        # Should have clustered similar levels
        assert len(unique_prices) < len(prices) or len(levels) <= 10, "Should cluster nearby levels"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_test_outcome_classification(self, detector, sample_ohlcv_ranging):
        """Test proper classification of S/R test outcomes"""
        # Get levels first
        levels = await detector.detect_sr_levels('OUTCOME', sample_ohlcv_ranging, Timeframe.DAILY)
        
        if not levels:
            pytest.skip("No levels detected for outcome testing")
        
        # Get tests
        tests = await detector.detect_sr_tests('OUTCOME', sample_ohlcv_ranging, levels)
        
        # Should have various outcomes
        outcomes = {test.outcome for test in tests}
        
        # Verify outcome validity
        for outcome in outcomes:
            assert outcome in SRTestOutcome, f"Invalid outcome: {outcome}"
        
        # Check specific outcome properties
        for test in tests:
            if test.outcome == SRTestOutcome.HOLD_STRONG:
                assert test.confidence > 0.6, "Strong holds should have high confidence"
            elif test.outcome == SRTestOutcome.BREAK_CLEAN:
                assert test.max_penetration > 0.005, "Clean breaks should show significant penetration"

    @pytest.mark.asyncio
    async def test_performance_with_large_dataset(self, detector):
        """Test performance with larger dataset"""
        # Generate large dataset (1 year of hourly data)
        dates = pd.date_range(start='2023-01-01', end='2024-01-01', freq='H')
        
        # Simple trending data with noise
        base_prices = 100 + np.cumsum(np.random.normal(0, 0.1, len(dates)))
        
        data = []
        for date, base_price in zip(dates, base_prices):
            variation = np.random.normal(0, 1)
            close = base_price + variation
            data.append({
                'timestamp': date,
                'open': close + np.random.normal(0, 0.5),
                'high': close + abs(np.random.normal(0, 1)),
                'low': close - abs(np.random.normal(0, 1)),
                'close': close,
                'volume': int(np.random.lognormal(15, 0.3))
            })
        
        df = pd.DataFrame(data)
        
        # Time the detection
        start_time = datetime.now()
        levels = await detector.detect_sr_levels('PERF', df, Timeframe.INTRADAY_1H)
        detection_time = (datetime.now() - start_time).total_seconds()
        
        # Performance assertions
        assert detection_time < 10.0, f"Detection took too long: {detection_time:.2f}s"
        assert isinstance(levels, list), "Should return valid results"
        
        print(f"Processed {len(df)} data points in {detection_time:.3f}s, found {len(levels)} levels")

class TestSRDataStructures:
    """Test the S/R data structures and their validation"""

    def test_sr_level_creation(self):
        """Test SRLevel creation and validation"""
        level = SRLevel(
            price=100.50,
            sr_type=SRType.SUPPORT,
            level_type=SRLevelType.PIVOT_POINT,
            timeframe=Timeframe.DAILY,
            strength=0.75,
            first_established=datetime.now(),
            last_tested=datetime.now(),
            test_count=3,
            hold_count=2,
            break_count=1,
            confidence=0.85,
            volume_confirmation=True,
            metadata={'source': 'test'}
        )
        
        assert level.price == 100.50
        assert level.sr_type == SRType.SUPPORT
        assert level.level_type == SRLevelType.PIVOT_POINT
        assert level.strength == 0.75
        assert level.confidence == 0.85

    def test_sr_test_creation(self):
        """Test SRTest creation and validation"""
        test = SRTest(
            level_id='test_level_1',
            test_datetime=datetime.now(),
            test_price=100.25,
            approach_direction='up',
            max_penetration=0.005,
            hold_duration=timedelta(minutes=5),
            volume_spike=2.5,
            outcome=SRTestOutcome.HOLD_STRONG,
            confidence=0.9,
            timeframe=Timeframe.DAILY
        )
        
        assert test.level_id == 'test_level_1'
        assert test.approach_direction == 'up'
        assert test.outcome == SRTestOutcome.HOLD_STRONG
        assert test.confidence == 0.9

    def test_sr_event_creation(self):
        """Test SREvent creation with related objects"""
        level = SRLevel(
            price=100.0,
            sr_type=SRType.RESISTANCE,
            level_type=SRLevelType.PSYCHOLOGICAL,
            timeframe=Timeframe.DAILY,
            strength=0.8,
            first_established=datetime.now(),
            last_tested=datetime.now(),
            test_count=1,
            hold_count=1,
            break_count=0,
            confidence=0.9,
            volume_confirmation=False,
            metadata={}
        )
        
        test = SRTest(
            level_id='test_level_2',
            test_datetime=datetime.now(),
            test_price=100.05,
            approach_direction='up',
            max_penetration=0.002,
            hold_duration=timedelta(minutes=2),
            volume_spike=1.8,
            outcome=SRTestOutcome.HOLD_WEAK,
            confidence=0.7,
            timeframe=Timeframe.DAILY
        )
        
        event = SREvent(
            event_id='sr_test_event_1',
            symbol='AAPL',
            level=level,
            test=test,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        assert event.event_id == 'sr_test_event_1'
        assert event.symbol == 'AAPL'
        assert event.level == level
        assert event.test == test

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])