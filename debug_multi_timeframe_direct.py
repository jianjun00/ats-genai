#!/usr/bin/env python3
"""
Direct test of multi-timeframe functionality
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from services.analytics_service import UnifiedAnalyticsService

def test_multi_timeframe():
    """Test multi-timeframe method directly."""
    print("🔍 TESTING MULTI-TIMEFRAME METHOD DIRECTLY")
    print("=" * 60)

    # Create analytics service instance
    service = UnifiedAnalyticsService()

    # Test parameters
    dataset_id = 63
    sequence_id = "AAPL_20250801_000000_20250801_000000"

    print(f"Dataset ID: {dataset_id}")
    print(f"Sequence ID: {sequence_id}")

    # Call the method directly
    try:
        result = service.get_training_dataset_sequence_multi_timeframe(dataset_id, sequence_id)

        print(f"\nResult keys: {list(result.keys())}")

        if 'error' in result:
            print(f"❌ Error: {result['error']}")
            return False

        if 'ohlc_data' in result:
            ohlc_data = result['ohlc_data']
            print(f"✅ OHLC data found for timeframes: {list(ohlc_data.keys())}")

            for timeframe, data in ohlc_data.items():
                print(f"   {timeframe}: {len(data)} records")
                if len(data) > 0:
                    print(f"      First record: {data[0]}")

        if 'table_data' in result:
            table_data = result['table_data']
            print(f"✅ Table data: {len(table_data)} rows")

        print(f"✅ Success: {result.get('success', False)}")
        return True

    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

if __name__ == "__main__":
    success = test_multi_timeframe()
    sys.exit(0 if success else 1)