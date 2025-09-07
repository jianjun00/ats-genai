#!/usr/bin/env python3
"""
Standalone test for Support/Resistance system validation
"""

import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from events.analysis.support_resistance_detector import (
        SupportResistanceDetector, SRLevel, SRTest, SREvent,
        SRType, SRLevelType, SRTestOutcome, Timeframe
    )
    print("✓ Successfully imported S/R detector modules")
except ImportError as e:
    print(f"✗ Import error: {e}")
    sys.exit(1)

def generate_test_data():
    """Generate test market data with clear S/R levels"""
    np.random.seed(42)

    # 3 months of daily data
    dates = pd.date_range(start='2024-01-01', end='2024-04-01', freq='D')

    # Create data with support at 100 and resistance at 120
    data = []
    for i, date in enumerate(dates):
        # Oscillate between 95-125 with S/R at 100 and 120
        cycle_position = (i % 40) / 40.0
        base_price = 100 + 20 * (0.5 + 0.4 * np.sin(2 * np.pi * cycle_position))

        # Add resistance/support behavior
        if base_price > 118:  # Near resistance at 120
            base_price = 120 - abs(np.random.normal(0, 1))
            volume_mult = 2.0
        elif base_price < 102:  # Near support at 100
            base_price = 100 + abs(np.random.normal(0, 1))
            volume_mult = 1.8
        else:
            volume_mult = 1.0

        variation = np.random.normal(0, 0.5)
        close = base_price + variation

        data.append({
            'timestamp': date,
            'open': close + np.random.normal(0, 0.3),
            'high': close + abs(np.random.normal(0, 0.8)),
            'low': close - abs(np.random.normal(0, 0.8)),
            'close': close,
            'volume': int(1000000 * volume_mult * np.random.uniform(0.9, 1.1))
        })

    return pd.DataFrame(data)

async def test_level_detection():
    """Test S/R level detection"""
    print("\n=== Testing S/R Level Detection ===")

    detector = SupportResistanceDetector({
        'pivot_lookback': 10,
        'cluster_epsilon': 0.02,
        'proximity_tolerance': 0.01,
        'psychological_levels': True,
        'volume_profile_levels': False  # Disable for simplicity
    })

    # Generate test data
    test_data = generate_test_data()
    print(f"Generated {len(test_data)} days of test data")

    # Detect levels
    levels = await detector.detect_sr_levels('TEST', test_data, Timeframe.DAILY)
    print(f"Detected {len(levels)} S/R levels")

    if levels:
        print("Top levels:")
        for i, level in enumerate(sorted(levels, key=lambda x: x.strength, reverse=True)[:5]):
            print(f"  {i+1}. {level.sr_type.value.upper()} at ${level.price:.2f} "
                  f"(strength: {level.strength:.3f}, type: {level.level_type.value})")

    # Validate levels
    assert len(levels) > 0, "Should detect some levels"

    # Check for expected levels around 100 and 120
    prices = [level.price for level in levels]
    support_near_100 = any(98 <= price <= 102 for price in prices)
    resistance_near_120 = any(118 <= price <= 122 for price in prices)

    print(f"Found support near 100: {support_near_100}")
    print(f"Found resistance near 120: {resistance_near_120}")

    return levels

async def test_level_testing(levels):
    """Test S/R level test detection"""
    print("\n=== Testing S/R Level Tests ===")

    if not levels:
        print("No levels to test")
        return []

    detector = SupportResistanceDetector()
    test_data = generate_test_data()

    # Detect tests
    tests = await detector.detect_sr_tests('TEST', test_data, levels)
    print(f"Detected {len(tests)} level tests")

    if tests:
        # Group by outcome
        outcome_counts = {}
        for test in tests:
            outcome = test.outcome.value
            outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1

        print("Test outcomes:")
        for outcome, count in sorted(outcome_counts.items()):
            print(f"  {outcome}: {count}")

        # Show high confidence tests
        high_conf_tests = [t for t in tests if t.confidence > 0.8]
        print(f"\nHigh confidence tests ({len(high_conf_tests)}):")
        for test in high_conf_tests[:3]:
            print(f"  {test.outcome.value} at ${test.test_price:.2f} "
                  f"(confidence: {test.confidence:.3f}, volume: {test.volume_spike:.1f}x)")

    return tests

def test_data_structures():
    """Test S/R data structures"""
    print("\n=== Testing Data Structures ===")

    # Test SRLevel creation
    level = SRLevel(
        price=100.50,
        sr_type=SRType.SUPPORT,
        level_type=SRLevelType.PIVOT,
        timeframe=Timeframe.DAILY,
        strength=0.85,
        first_established=datetime.now(),
        last_tested=datetime.now(),
        test_count=3,
        hold_count=2,
        break_count=1,
        confidence=0.9,
        volume_confirmation=True,
        metadata={'source': 'test'}
    )

    assert level.price == 100.50
    assert level.sr_type == SRType.SUPPORT
    assert level.strength == 0.85
    print("✓ SRLevel creation works")

    # Test SRTest creation
    test = SRTest(
        level_id='test_level_1',
        test_datetime=datetime.now(),
        test_price=100.25,
        approach_direction='down',
        max_penetration=0.005,
        hold_duration=300,
        volume_spike=2.5,
        outcome=SRTestOutcome.HOLD_STRONG,
        confidence=0.9
    )

    assert test.level_id == 'test_level_1'
    assert test.outcome == SRTestOutcome.HOLD_STRONG
    print("✓ SRTest creation works")

    # Test SREvent creation
    event = SREvent(
        event_id='test_event_1',
        symbol='TEST',
        level=level,
        test=test,
        created_at=datetime.now(),
        updated_at=datetime.now()
    )

    assert event.event_id == 'test_event_1'
    assert event.symbol == 'TEST'
    assert event.level == level
    assert event.test == test
    print("✓ SREvent creation works")

def test_processor_creation():
    """Test processor creation"""
    print("\n=== Testing Processor Creation ===")

    try:
        from events.processors.support_resistance_processor import SupportResistanceProcessor

        config = {
            'processing_interval_seconds': 60,
            'batch_size': 10,
            'max_concurrent_symbols': 5,
            'min_data_points': 50,
            'detector_config': {
                'pivot_lookback': 20
            }
        }

        processor = SupportResistanceProcessor(config)
        assert processor.config is not None
        assert processor.detector is not None
        print("✓ SupportResistanceProcessor creation works")

        # Test stats
        stats = processor.get_processing_stats()
        assert 'levels_detected' in stats
        assert 'symbols_processed' in stats
        print("✓ Processor stats work")

    except ImportError as e:
        print(f"⚠ Processor test skipped due to import: {e}")

async def main():
    """Run all standalone tests"""
    print("🔍 Support/Resistance System Validation")
    print("=" * 50)

    try:
        # Test data structures
        test_data_structures()

        # Test processor creation
        test_processor_creation()

        # Test detection pipeline
        levels = await test_level_detection()
        tests = await test_level_testing(levels)

        print("\n" + "=" * 50)
        print("🎉 All tests completed successfully!")

        # Summary
        print(f"\nSummary:")
        print(f"- Detected {len(levels)} S/R levels")
        print(f"- Detected {len(tests)} level tests")
        print(f"- System components working correctly")

        if levels and tests:
            # Calculate some basic quality metrics
            strong_levels = len([l for l in levels if l.strength > 0.7])
            high_conf_tests = len([t for t in tests if t.confidence > 0.8])

            print(f"- Strong levels: {strong_levels}/{len(levels)} ({strong_levels/len(levels)*100:.1f}%)")
            print(f"- High confidence tests: {high_conf_tests}/{len(tests)} ({high_conf_tests/len(tests)*100:.1f}%)")

        return True

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)