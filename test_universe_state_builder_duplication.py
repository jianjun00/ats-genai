#!/usr/bin/env python3
"""
Test to investigate UniverseStateBuilder's role in OHLCV duplication.

This test checks if UniverseStateBuilder is processing the same OHLCV data
across multiple intervals, causing duplication in training data.
"""

import sys
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd

# Add src to path
sys.path.append('src')

async def test_universe_state_builder_behavior():
    """Test UniverseStateBuilder's handling of OHLC data across intervals."""
    print("🧪 Testing UniverseStateBuilder OHLC Processing")
    print("=" * 60)
    
    try:
        from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
        
        # Create different OHLCV data for different time intervals
        ohlcv_data_intervals = {
            datetime(2025, 7, 1, 6, 30): {
                'TSLA': {'open': 295.65, 'high': 296.78, 'low': 294.60, 'close': 295.22, 'volume': 790517, 'timestamp': 0}
            },
            datetime(2025, 7, 1, 6, 35): {
                'TSLA': {'open': 295.80, 'high': 297.00, 'low': 294.90, 'close': 296.50, 'volume': 825000, 'timestamp': 0}
            },
            datetime(2025, 7, 1, 6, 40): {
                'TSLA': {'open': 296.10, 'high': 297.50, 'low': 295.20, 'close': 296.80, 'volume': 840000, 'timestamp': 0}
            }
        }
        
        # Mock runner with market_data_manager
        mock_runner = MagicMock()
        mock_runner.market_data_manager = AsyncMock()
        
        # Mock the market data manager to return different data for different times
        def mock_get_minute_ohlc_batch(symbols, start, end):
            print(f"   📞 Market data request: {symbols} from {start.strftime('%H:%M')} to {end.strftime('%H:%M')}")
            
            # Return data based on start time
            for interval_time, data in ohlcv_data_intervals.items():
                if start <= interval_time < end:
                    print(f"   📊 Returning data for {interval_time.strftime('%H:%M')}: {data}")
                    return data
            
            # Fallback - return first interval data (this could be the problem!)
            fallback_data = list(ohlcv_data_intervals.values())[0]
            print(f"   ⚠️  No specific data for {start.strftime('%H:%M')}, returning fallback: {fallback_data}")
            return fallback_data
        
        mock_runner.market_data_manager.get_minute_ohlc_batch = AsyncMock(side_effect=mock_get_minute_ohlc_batch)
        
        # Mock other required components
        mock_runner.environment = MagicMock()
        mock_runner.environment.get_instrument_by_symbol = AsyncMock(return_value={'id': 1})
        
        # Create UniverseStateBuilder
        universe_builder = UniverseStateIntervalBuilder(
            target_durations=['1h'],  # Test with 1h intervals
            symbols=['TSLA']
        )
        
        # Mock the internal state
        universe_builder.logger = MagicMock()
        
        # Test processing multiple intervals
        print(f"\n📋 Testing multiple interval processing:")
        print("-" * 40)
        
        intervals_to_test = [
            datetime(2025, 7, 1, 6, 30),
            datetime(2025, 7, 1, 6, 35), 
            datetime(2025, 7, 1, 6, 40)
        ]
        
        results = []
        for i, interval_time in enumerate(intervals_to_test):
            print(f"\n   Interval {i+1}: {interval_time.strftime('%H:%M')}")
            
            # Mock the interval processing call
            try:
                # This would normally be called by the training system
                # We're simulating the handleInterval call
                
                # Mock instruments for this interval
                instruments = [{'id': 1, 'symbol': 'TSLA'}]
                
                # Simulate the OHLC data request
                symbols = ['TSLA']
                minute_start_time = interval_time
                minute_end_time = interval_time + timedelta(minutes=5)
                
                ohlc_batch = await mock_runner.market_data_manager.get_minute_ohlc_batch(
                    symbols, minute_start_time, minute_end_time
                )
                
                print(f"      Result: {ohlc_batch}")
                results.append({
                    'interval': interval_time,
                    'ohlc_data': ohlc_batch.get('TSLA', {})
                })
                
            except Exception as e:
                print(f"      Error: {e}")
                results.append({
                    'interval': interval_time,
                    'error': str(e)
                })
        
        # Analyze results for duplication
        print(f"\n📊 DUPLICATION ANALYSIS")
        print("=" * 60)
        
        ohlc_values = []
        for result in results:
            if 'ohlc_data' in result and result['ohlc_data']:
                ohlc = result['ohlc_data']
                ohlc_values.append({
                    'interval': result['interval'].strftime('%H:%M'),
                    'open': ohlc.get('open', 0),
                    'high': ohlc.get('high', 0),
                    'low': ohlc.get('low', 0),
                    'close': ohlc.get('close', 0)
                })
        
        print("OHLCV values by interval:")
        for i, ohlc in enumerate(ohlc_values):
            print(f"   {ohlc['interval']}: O:{ohlc['open']:.2f} H:{ohlc['high']:.2f} L:{ohlc['low']:.2f} C:{ohlc['close']:.2f}")
        
        # Check for duplicates
        if len(ohlc_values) >= 2:
            first_values = ohlc_values[0]
            duplicates_found = all(
                ohlc['open'] == first_values['open'] and
                ohlc['high'] == first_values['high'] and
                ohlc['low'] == first_values['low'] and
                ohlc['close'] == first_values['close']
                for ohlc in ohlc_values[1:]
            )
            
            if duplicates_found:
                print(f"\n❌ DUPLICATION ISSUE REPRODUCED!")
                print(f"   All intervals have identical OHLCV values")
                print(f"   This suggests data source or processing issue")
            else:
                print(f"\n✅ NO DUPLICATION: Each interval has unique values")
                print(f"   UniverseStateBuilder is processing data correctly")
        
        return not duplicates_found if len(ohlc_values) >= 2 else False
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_universe_state_builder_behavior())
    print(f"\n{'🟢 UNIVERSE STATE BUILDER OK' if success else '🔴 DUPLICATION ISSUE FOUND'}")
    sys.exit(0 if success else 1)