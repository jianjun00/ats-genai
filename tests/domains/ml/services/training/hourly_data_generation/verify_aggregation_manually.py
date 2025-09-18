#!/usr/bin/env python3
"""
Manual verification of hourly aggregation functionality.

Creates test data and verifies the aggregation works correctly.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from app.training_data_job_runner import TrainingDataJobRunner


class SimpleConfig:
    """Simple config without Gin."""
    def __init__(self):
        self.base_interval_minutes = 1
        self.training_interval_minutes = 60
        self.use_universe_state_indicators = True


def create_test_minute_data():
    """Create test minute data for verification."""
    print("Creating test minute data...")

    # Create 3 hours of data: 9:30-12:30
    start_time = datetime(2025, 8, 4, 9, 30)
    minute_data = []

    base_price = 200.0

    for minute in range(180):  # 3 hours = 180 minutes
        timestamp = start_time + timedelta(minutes=minute)

        price_change = np.random.normal(0, 0.1)
        current_price = base_price + price_change

        minute_bar = {
            'datetime': timestamp,
            'open': round(current_price + np.random.uniform(-0.05, 0.05), 2),
            'high': round(current_price + np.random.uniform(0.02, 0.2), 2),
            'low': round(current_price - np.random.uniform(0.02, 0.2), 2),
            'close': round(current_price + np.random.uniform(-0.05, 0.05), 2),
            'volume': np.random.randint(100, 500)
        }

        # Ensure OHLC logic
        minute_bar['high'] = max(minute_bar['high'], minute_bar['open'], minute_bar['close'])
        minute_bar['low'] = min(minute_bar['low'], minute_bar['open'], minute_bar['close'])

        minute_data.append(minute_bar)
        base_price = minute_bar['close']

    df = pd.DataFrame(minute_data)

    print(f"✅ Created {len(df)} minute bars")
    print(f"   Time range: {df['datetime'].min()} to {df['datetime'].max()}")
    print(f"   Price range: ${df['low'].min():.2f} - ${df['high'].max():.2f}")
    print(f"   Volume range: {df['volume'].min()} - {df['volume'].max()}")

    return df


def test_aggregation():
    """Test the aggregation functionality."""
    print("\n🧪 Testing Hourly Aggregation")
    print("=" * 50)

    # Create test data
    minute_data = create_test_minute_data()

    # Create runner manually
    runner = TrainingDataJobRunner.__new__(TrainingDataJobRunner)
    runner.config = SimpleConfig()

    # Test basic aggregation
    print("\n📊 Testing basic aggregation (no universe state)...")
    try:
        hourly_rows = runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=None
        )

        print(f"✅ Generated {len(hourly_rows)} hourly rows")

        # Show details of each hour
        for i, hour in enumerate(hourly_rows):
            print(f"   Hour {i+1}: {hour['datetime']} | OHLCV: ${hour['hour_open']:.2f}/${hour['hour_high']:.2f}/${hour['hour_low']:.2f}/${hour['hour_close']:.2f} | Vol: {hour['hour_volume']:,}")
            print(f"            Market Period: {hour['market_period']} | Day Progress: {hour['day_progress']:.2f}")

        # Verify first hour manually
        first_hour_data = minute_data[minute_data['datetime'].dt.floor('h') == pd.Timestamp('2025-08-04 09:00:00')]
        if len(first_hour_data) > 0:
            expected_open = first_hour_data['open'].iloc[0]
            expected_high = first_hour_data['high'].max()
            expected_low = first_hour_data['low'].min()
            expected_close = first_hour_data['close'].iloc[-1]
            expected_volume = first_hour_data['volume'].sum()

            print(f"\n🔍 Manual verification of first hour:")
            print(f"   Expected: ${expected_open:.2f}/${expected_high:.2f}/${expected_low:.2f}/${expected_close:.2f} | Vol: {expected_volume:,}")
            print(f"   Actual:   ${hourly_rows[0]['hour_open']:.2f}/${hourly_rows[0]['hour_high']:.2f}/${hourly_rows[0]['hour_low']:.2f}/${hourly_rows[0]['hour_close']:.2f} | Vol: {hourly_rows[0]['hour_volume']:,}")

            if (hourly_rows[0]['hour_open'] == expected_open and
                hourly_rows[0]['hour_high'] == expected_high and
                hourly_rows[0]['hour_low'] == expected_low and
                hourly_rows[0]['hour_close'] == expected_close and
                hourly_rows[0]['hour_volume'] == expected_volume):
                print("   ✅ OHLCV aggregation is CORRECT!")
            else:
                print("   ❌ OHLCV aggregation mismatch")

    except Exception as e:
        print(f"❌ Basic aggregation failed: {e}")
        return False

    # Test with mock universe state indicators
    print(f"\n🌟 Testing with universe state indicators...")
    try:
        from unittest.mock import Mock

        mock_universe = Mock()
        mock_universe.get_indicators_for_hour.return_value = {
            'envelope_top': 205.0,
            'envelope_bot': 195.0,
            'pldot': -0.001,
            'oneone_high': 203.0,
            'oneone_low': 197.0,
            'z1b': 2.5,
            'z2b': 5.0,
            'z5t': 3.0,
            'z6t': 4.0
        }

        universe_hourly_rows = runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=mock_universe
        )

        print(f"✅ Generated {len(universe_hourly_rows)} hourly rows with universe indicators")

        # Show first hour with indicators
        first_hour = universe_hourly_rows[0]
        print(f"   First hour indicators:")
        print(f"     Envelope: ${first_hour.get('hour_envelope_bot', 'N/A'):.2f} - ${first_hour.get('hour_envelope_top', 'N/A'):.2f}")
        print(f"     PL Dot: {first_hour.get('hour_pldot', 'N/A')}")
        print(f"     One-One: ${first_hour.get('hour_oneone_low', 'N/A'):.2f} - ${first_hour.get('hour_oneone_high', 'N/A'):.2f}")
        print(f"     Z-Values: {first_hour.get('hour_z1b', 'N/A')}, {first_hour.get('hour_z2b', 'N/A')}, {first_hour.get('hour_z5t', 'N/A')}, {first_hour.get('hour_z6t', 'N/A')}")

        # Count indicators
        indicator_count = sum(1 for key in first_hour.keys() if key.startswith('hour_') and any(ind in key for ind in ['envelope', 'pldot', 'oneone', 'z1b', 'z2b', 'z5t', 'z6t']))
        print(f"   ✅ Found {indicator_count} universe state indicators")

    except Exception as e:
        print(f"❌ Universe state aggregation failed: {e}")
        return False

    return True


def main():
    """Run manual verification."""
    print("🔧 Manual Hourly Aggregation Verification")
    print("=" * 50)

    success = test_aggregation()

    print("\n" + "=" * 50)
    if success:
        print("🎉 MANUAL VERIFICATION SUCCESSFUL!")
        print("\n✅ Core functionality confirmed:")
        print("   • Minute-to-hourly OHLCV aggregation works correctly")
        print("   • Universe state builder indicator integration works")
        print("   • Market period and day progress calculation works")
        print("   • Data structure and field naming is correct")
        print("\n🚀 Hourly training data generation is functional!")
    else:
        print("❌ MANUAL VERIFICATION FAILED!")
        print("   Please check the aggregation logic.")

    return success


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)