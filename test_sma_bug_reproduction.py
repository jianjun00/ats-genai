#!/usr/bin/env python3
"""
Test to reproduce and catch the SMA bug you mentioned.

This creates a version with the bug to verify our tests can catch it.
"""

import unittest
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

# Add src to path
sys.path.append('src')

def generate_hourly_data_with_sma_bug(symbol: str = "AAPL", days: int = 3) -> pd.DataFrame:
    """Generate data with the SMA bug - SMA gets volume values instead of price values."""
    
    start_date = date.today() - timedelta(days=days)
    end_date = date.today()
    
    # Generate hourly datetime range (market hours only)
    datetimes = []
    current = datetime.combine(start_date, datetime.min.time().replace(hour=9))
    end_datetime = datetime.combine(end_date, datetime.min.time().replace(hour=16))
    
    while current <= end_datetime:
        if current.weekday() < 5 and 9 <= current.hour <= 16:
            datetimes.append(current)
        current += timedelta(hours=1)
    
    rows = []
    base_price = 150.0
    current_price = base_price
    
    for i, dt in enumerate(datetimes):
        # Random walk price movement
        price_change = np.random.normal(0, 0.02)
        current_price = max(50.0, current_price * (1 + price_change))
        
        # Generate OHLCV
        volatility = 0.015
        high = current_price * (1 + abs(np.random.normal(0, volatility)))
        low = current_price * (1 - abs(np.random.normal(0, volatility)))
        open_price = current_price * (1 + np.random.normal(0, volatility / 2))
        close = current_price
        volume = int(1000000 * (1 + np.random.exponential(0.5)))
        
        # 🚨 BUG: SMA calculation gets volume instead of price!
        sma_20_BUGGY = volume + np.random.normal(0, 50000)  # This is the bug!
        ema_12 = current_price + np.random.normal(0, 3)      # This is correct
        ema_26 = current_price + np.random.normal(0, 4)      # This is correct
        
        # Create row
        row = {
            'datetime': dt.isoformat(),
            'symbol': symbol,
            'timestamp': dt.timestamp(),
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'hour': dt.hour,
            'weekday': dt.weekday(),
            'close': round(close, 2),
            'volume': volume,
            'sma_20': round(sma_20_BUGGY, 2),  # 🚨 BUG: This is volume-based!
            'ema_12': round(ema_12, 2),        # ✅ Correct: price-based
            'ema_26': round(ema_26, 2),        # ✅ Correct: price-based
        }
        
        rows.append(row)
    
    return pd.DataFrame(rows)


class TestSMABugDetection(unittest.TestCase):
    """Test that our validation can catch the SMA bug."""
    
    def test_sma_bug_detection(self):
        """Test that the validation catches SMA values in volume range."""
        
        # Generate data with the deliberate SMA bug
        df_buggy = generate_hourly_data_with_sma_bug("AAPL", 2)
        
        print("\n🐛 Testing with deliberately buggy data:")
        print(f"Close price range: {df_buggy['close'].min():.2f} to {df_buggy['close'].max():.2f}")
        print(f"Volume range: {df_buggy['volume'].min():,} to {df_buggy['volume'].max():,}")
        print(f"SMA_20 (BUGGY) range: {df_buggy['sma_20'].min():,.2f} to {df_buggy['sma_20'].max():,.2f}")
        print(f"EMA_12 (correct) range: {df_buggy['ema_12'].min():.2f} to {df_buggy['ema_12'].max():.2f}")
        
        # Show first few rows
        print("\nFirst 3 rows:")
        print(df_buggy[['datetime', 'close', 'volume', 'sma_20', 'ema_12']].head(3))
        
        # This test should FAIL with the buggy data
        with self.assertRaises(AssertionError) as context:
            # Check that SMA is in reasonable price range (this should fail)
            for idx, row in df_buggy.iterrows():
                sma_20 = row['sma_20']
                close = row['close']
                volume = row['volume']
                
                # This assertion should fail because SMA is in volume range
                assert sma_20 < 1000, f"Row {idx}: SMA_20 = {sma_20} is too high (volume-like), should be ~{close}"
                
                # This assertion should also fail
                price_diff = abs(sma_20 - close)
                volume_diff = abs(sma_20 - volume)
                assert price_diff < volume_diff, f"Row {idx}: SMA_20 ({sma_20}) closer to volume ({volume}) than price ({close})"
        
        # If we get here, the bug was detected
        error_message = str(context.exception)
        print(f"\n✅ BUG DETECTED: {error_message}")
        self.assertIn("too high", error_message, "Test should detect SMA values that are too high")


def main():
    """Run the SMA bug detection test."""
    print("🧪 Testing SMA Bug Detection...")
    print("=" * 60)
    
    # Run the test
    unittest.main(verbosity=2)


if __name__ == "__main__":
    main()