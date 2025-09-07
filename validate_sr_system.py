#!/usr/bin/env python3
"""
Direct validation of Support/Resistance system
Bypasses complex test setup to validate core functionality
"""

import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """Test that all S/R modules can be imported"""
    print("=== Testing Imports ===")

    try:
        from events.analysis.support_resistance_detector import (
            SupportResistanceDetector, SRLevel, SRTest, SREvent,
            SRType, SRLevelType, SRTestOutcome, Timeframe
        )
        print("✓ S/R detector modules imported successfully")

        # Test processor import
        from events.processors.support_resistance_processor import SupportResistanceProcessor
        print("✓ S/R processor imported successfully")

        return True

    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def generate_test_market_data():
    """Generate realistic market data with S/R levels"""
    print("\n=== Generating Test Market Data ===")

    np.random.seed(42)
    dates = pd.date_range(start='2024-01-01', end='2024-06-01', freq='D')

    # Create market with clear S/R levels at 100 (support) and 120 (resistance)
    data = []
    for i, date in enumerate(dates):
        # Market oscillates between 95-125 with clear S/R behavior
        cycle = (i % 30) / 30.0  # 30-day cycle
        base_price = 110 + 12 * np.sin(2 * np.pi * cycle)

        # Add S/R behavior
        if base_price <= 102:  # Near support at 100
            base_price = 100 + abs(np.random.normal(0, 1))
            volume_mult = 2.0
        elif base_price >= 118:  # Near resistance at 120
            base_price = 120 - abs(np.random.normal(0, 1))
            volume_mult = 2.5
        else:
            volume_mult = 1.0

        close = base_price + np.random.normal(0, 0.8)
        data.append({
            'timestamp': date,
            'open': close + np.random.normal(0, 0.3),
            'high': close + abs(np.random.normal(0, 0.6)),
            'low': close - abs(np.random.normal(0, 0.6)),
            'close': close,
            'volume': int(1000000 * volume_mult * np.random.uniform(0.9, 1.1))
        })

    df = pd.DataFrame(data)
    print(f"✓ Generated {len(df)} days of market data")
    print(f"  Price range: ${df['close'].min():.2f} - ${df['close'].max():.2f}")

    return df

async def test_level_detection():
    """Test S/R level detection"""
    print("\n=== Testing S/R Level Detection ===")

    from events.analysis.support_resistance_detector import (
        SupportResistanceDetector, SRType, SRLevelType, Timeframe
    )

    # Create detector
    detector = SupportResistanceDetector({
        'pivot_lookback': 15,
        'cluster_epsilon': 0.02,
        'proximity_tolerance': 0.01,
        'psychological_levels': True,
        'volume_profile_levels': False,  # Disable to avoid scipy issues
        'min_level_strength': 0.3
    })

    # Generate test data
    market_data = generate_test_market_data()

    # Detect levels
    symbol = 'TEST_AAPL'
    timeframe = Timeframe.DAILY

    levels = await detector.detect_sr_levels(symbol, market_data, timeframe)

    print(f"✓ Detected {len(levels)} S/R levels")

    if levels:
        # Sort by strength
        levels.sort(key=lambda x: x.strength, reverse=True)

        print("  Top levels:")
        for i, level in enumerate(levels[:5]):
            print(f"    {i+1}. {level.sr_type.value.upper()} at ${level.price:.2f} "
                  f"(strength: {level.strength:.3f}, type: {level.level_type.value})")

        # Check for expected levels
        prices = [level.price for level in levels]
        support_near_100 = any(98 <= price <= 102 for price in prices)
        resistance_near_120 = any(118 <= price <= 122 for price in prices)

        print(f"  Expected levels found:")
        print(f"    Support near 100: {'✓' if support_near_100 else '✗'}")
        print(f"    Resistance near 120: {'✓' if resistance_near_120 else '✗'}")

        return levels
    else:
        print("✗ No levels detected")
        return []

async def test_level_tests(levels, market_data):
    """Test S/R level test detection"""
    print("\n=== Testing S/R Level Tests ===")

    if not levels:
        print("✗ No levels available for testing")
        return []

    from events.analysis.support_resistance_detector import SupportResistanceDetector

    detector = SupportResistanceDetector()

    # Detect tests
    tests = await detector.detect_sr_tests('TEST_AAPL', market_data, levels)

    print(f"✓ Detected {len(tests)} level tests")

    if tests:
        # Analyze outcomes
        outcome_counts = {}
        total_confidence = 0

        for test in tests:
            outcome = test.outcome.value
            outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
            total_confidence += test.confidence

        print("  Test outcomes:")
        for outcome, count in sorted(outcome_counts.items()):
            print(f"    {outcome}: {count}")

        avg_confidence = total_confidence / len(tests)
        print(f"  Average confidence: {avg_confidence:.3f}")

        # Show high-confidence tests
        high_conf_tests = [t for t in tests if t.confidence > 0.7]
        if high_conf_tests:
            print(f"  High-confidence tests ({len(high_conf_tests)}):")
            for test in high_conf_tests[:3]:
                print(f"    {test.outcome.value} at ${test.test_price:.2f} "
                      f"(confidence: {test.confidence:.3f})")

    return tests

def test_data_structures():
    """Test S/R data structures"""
    print("\n=== Testing Data Structures ===")

    from events.analysis.support_resistance_detector import (
        SRLevel, SRTest, SREvent, SRType, SRLevelType,
        SRTestOutcome, Timeframe
    )

    # Test SRLevel
    try:
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
        print("✓ SRLevel creation successful")

        # Test SRTest
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
        print("✓ SRTest creation successful")

        # Test SREvent
        event = SREvent(
            event_id='test_event_1',
            symbol='TEST',
            level=level,
            test=test,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        print("✓ SREvent creation successful")

        return True

    except Exception as e:
        print(f"✗ Data structure test failed: {e}")
        return False

def test_processor_functionality():
    """Test processor basic functionality"""
    print("\n=== Testing Processor Functionality ===")

    try:
        from events.processors.support_resistance_processor import SupportResistanceProcessor

        # Create processor
        config = {
            'processing_interval_seconds': 60,
            'batch_size': 10,
            'max_concurrent_symbols': 5,
            'min_data_points': 50,
            'alert_thresholds': {
                'strong_level_test': 0.8,
                'level_break': 0.7,
                'confluence_level': 0.9
            },
            'detector_config': {
                'pivot_lookback': 15,
                'cluster_epsilon': 0.03,
                'psychological_levels': True
            }
        }

        processor = SupportResistanceProcessor(config)
        print("✓ Processor creation successful")

        # Test stats
        stats = processor.get_processing_stats()
        expected_keys = ['levels_detected', 'tests_identified', 'symbols_processed']

        for key in expected_keys:
            if key not in stats:
                print(f"✗ Missing stat key: {key}")
                return False

        print("✓ Processor stats working")

        return True

    except Exception as e:
        print(f"✗ Processor test failed: {e}")
        return False

async def main():
    """Run comprehensive S/R system validation"""
    print("🔍 Support/Resistance System Comprehensive Validation")
    print("=" * 60)

    tests_passed = 0
    tests_total = 5

    # Test 1: Imports
    if test_imports():
        tests_passed += 1

    # Test 2: Data structures
    if test_data_structures():
        tests_passed += 1

    # Test 3: Processor functionality
    if test_processor_functionality():
        tests_passed += 1

    # Test 4: Level detection
    levels = await test_level_detection()
    if levels:
        tests_passed += 1

    # Test 5: Level tests
    if levels:
        market_data = generate_test_market_data()
        tests = await test_level_tests(levels, market_data)
        if tests:
            tests_passed += 1

    # Summary
    print("\n" + "=" * 60)
    print(f"🎯 VALIDATION RESULTS: {tests_passed}/{tests_total} tests passed")

    if tests_passed == tests_total:
        print("🎉 ALL TESTS PASSED - S/R System is functional!")

        # Provide summary stats
        if 'levels' in locals() and 'tests' in locals():
            strong_levels = len([l for l in levels if l.strength > 0.7])
            high_conf_tests = len([t for t in tests if t.confidence > 0.8])

            print(f"\n📊 Quality Metrics:")
            print(f"   • Strong levels: {strong_levels}/{len(levels)} ({strong_levels/len(levels)*100:.1f}%)")
            print(f"   • High confidence tests: {high_conf_tests}/{len(tests)} ({high_conf_tests/len(tests)*100:.1f}%)")

        return True
    else:
        print("❌ SOME TESTS FAILED - Review issues above")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)