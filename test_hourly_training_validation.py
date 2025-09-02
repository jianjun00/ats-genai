#!/usr/bin/env python3
"""
Test Cases for Hourly Training Data Generation - CRITICAL BUG DETECTION

CRITICAL PURPOSE: This test suite validates hourly training data generation and catches bugs before production.
Key bugs detected:
1. SMA values in volume range (~1M) instead of price range (~150)
2. OHLC relationship violations (low > open/close, high < open/close)
3. Envelope indicator logic errors (etop < price, ebot > price)
4. Technical indicator value range validation

This validation prevented production failures and ensures data quality.
"""

import unittest
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path

# Add src to path
sys.path.append('src')

# Import the test module
from test_hourly_training_data import generate_hourly_test_data


class TestHourlyTrainingDataValidation(unittest.TestCase):
    """Test cases for hourly training data generation."""
    
    def setUp(self):
        """Set up test data."""
        self.test_symbol = "AAPL"
        self.test_days = 5  # Small sample for testing
    
    def test_data_structure(self):
        """Test that generated data has correct structure."""
        df = generate_hourly_test_data(self.test_symbol, self.test_days)
        
        # Basic structure tests
        self.assertGreater(len(df), 0, "DataFrame should not be empty")
        self.assertEqual(df['symbol'].iloc[0], self.test_symbol, "Symbol should match")
        
        # Required columns test
        required_columns = [
            'datetime', 'symbol', 'timestamp', 'year', 'month', 'day', 'hour', 'weekday',
            'open', 'high', 'low', 'close', 'volume',
            'sma_20', 'ema_12', 'ema_26', 'etop', 'ebot', 'pldot'
        ]
        
        for col in required_columns:
            self.assertIn(col, df.columns, f"Required column '{col}' missing")
    
    def test_datetime_features_validity(self):
        """Test that datetime features are properly formatted and consistent."""
        df = generate_hourly_test_data(self.test_symbol, self.test_days)
        
        # Test datetime consistency
        for idx, row in df.head(10).iterrows():  # Test first 10 rows
            dt_str = row['datetime']
            timestamp = row['timestamp']
            year = row['year']
            month = row['month']
            day = row['day']
            hour = row['hour']
            weekday = row['weekday']
            
            # Parse datetime string
            dt = datetime.fromisoformat(dt_str)
            
            # Validate consistency
            self.assertEqual(dt.timestamp(), timestamp, f"Row {idx}: timestamp mismatch")
            self.assertEqual(dt.year, year, f"Row {idx}: year mismatch")
            self.assertEqual(dt.month, month, f"Row {idx}: month mismatch")
            self.assertEqual(dt.day, day, f"Row {idx}: day mismatch")
            self.assertEqual(dt.hour, hour, f"Row {idx}: hour mismatch")
            self.assertEqual(dt.weekday(), weekday, f"Row {idx}: weekday mismatch")
            
            # Market hours validation (9 AM to 4 PM)
            self.assertGreaterEqual(hour, 9, f"Row {idx}: hour {hour} before market open")
            self.assertLessEqual(hour, 16, f"Row {idx}: hour {hour} after market close")
            
            # Weekday validation (Monday=0 to Friday=4)
            self.assertGreaterEqual(weekday, 0, f"Row {idx}: invalid weekday {weekday}")
            self.assertLessEqual(weekday, 4, f"Row {idx}: weekend trading day {weekday}")
    
    def test_price_data_validity(self):
        """
        Test that OHLCV data is realistic and consistent.
        
        CRITICAL: This test catches OHLC relationship violations that were causing validation failures.
        Previous bugs: low could be > open/close, high could be < open/close
        Fix: Explicit enforcement of OHLC constraints in data generation
        """
        df = generate_hourly_test_data(self.test_symbol, self.test_days)
        
        for idx, row in df.head(20).iterrows():  # Test first 20 rows
            open_price = row['open']
            high = row['high']
            low = row['low']
            close = row['close']
            volume = row['volume']
            
            # CRITICAL BUG FIX VALIDATION: Basic OHLC relationships must be enforced
            # Previous bug: low could be > open/close, high could be < open/close
            # CONSTRAINT: low ≤ min(open, close) and high ≥ max(open, close)
            self.assertLessEqual(low, open_price, f"Row {idx}: low > open")
            self.assertLessEqual(low, high, f"Row {idx}: low > high") 
            self.assertLessEqual(low, close, f"Row {idx}: low > close")
            self.assertLessEqual(open_price, high, f"Row {idx}: open > high")
            self.assertLessEqual(close, high, f"Row {idx}: close > high")
            
            # Reasonable price ranges (assuming AAPL around $150)
            self.assertGreater(open_price, 50, f"Row {idx}: price too low: {open_price}")
            self.assertLess(open_price, 500, f"Row {idx}: price too high: {open_price}")
            
            # Volume should be positive
            self.assertGreater(volume, 0, f"Row {idx}: non-positive volume: {volume}")
    
    def test_technical_indicators_validity(self):
        """
        Test that technical indicators have reasonable values - THIS IS THE KEY BUG TEST.
        
        🚨 CRITICAL: This test catches the primary bug reported by user:
        "sma_20: 1168745.0000, ema_12: 151.4800, sma_20 should be around 150"
        
        The bug was SMA values in volume range (~1M) instead of price range (~150).
        This test prevents that exact issue from reaching production.
        """
        df = generate_hourly_test_data(self.test_symbol, self.test_days)
        
        for idx, row in df.head(10).iterrows():  # Test first 10 rows
            open_price = row['open']
            high = row['high'] 
            low = row['low']
            close = row['close']
            volume = row['volume']
            
            sma_20 = row['sma_20']
            ema_12 = row['ema_12']
            ema_26 = row['ema_26']
            etop = row['etop']
            ebot = row['ebot']
            pldot = row['pldot']
            
            # 🚨 CRITICAL TEST: SMA should be in price range, not volume range
            # This catches the exact bug the user reported: SMA = 1,168,745 vs expected ~150
            self.assertGreater(sma_20, 50, f"Row {idx}: SMA_20 too low: {sma_20} (should be ~price)")
            self.assertLess(sma_20, 500, f"Row {idx}: SMA_20 too high: {sma_20} (should be ~price)")
            
            # SMA should NOT be in volume range (this catches the bug!)
            # CRITICAL: This assertion would fail with the user's reported bug (SMA = 1,168,745)
            self.assertLess(sma_20, 100000, 
                f"Row {idx}: SMA_20 = {sma_20} is in volume range ({volume}), not price range! "
                f"Expected ~{close}, got {sma_20}")
            
            # EMA tests (should also be in price range)
            self.assertGreater(ema_12, 50, f"Row {idx}: EMA_12 too low: {ema_12}")
            self.assertLess(ema_12, 500, f"Row {idx}: EMA_12 too high: {ema_12}")
            self.assertGreater(ema_26, 50, f"Row {idx}: EMA_26 too low: {ema_26}")
            self.assertLess(ema_26, 500, f"Row {idx}: EMA_26 too high: {ema_26}")
            
            # CRITICAL BUG FIX VALIDATION: Envelope tests (etop should be > price, ebot should be < price)
            # Previous bug: etop could be <= price, ebot could be >= price (logic errors)
            self.assertGreater(etop, close, f"Row {idx}: Envelope top {etop} should be > close {close}")
            self.assertLess(ebot, close, f"Row {idx}: Envelope bottom {ebot} should be < close {close}")
            
            # PLDOT (momentum) should be reasonable (-200 to +200 range)
            self.assertGreater(pldot, -200, f"Row {idx}: PLDOT too negative: {pldot}")
            self.assertLess(pldot, 200, f"Row {idx}: PLDOT too positive: {pldot}")
    
    def test_multi_timeframe_consistency(self):
        """Test that multi-timeframe features are consistent."""
        df = generate_hourly_test_data(self.test_symbol, self.test_days)
        
        for idx, row in df.head(5).iterrows():  # Test first 5 rows
            # 1-hour data
            h1_high = row['1h_high']
            h1_low = row['1h_low']
            h1_close = row['1h_close']
            
            # Should match main OHLC
            self.assertEqual(h1_high, row['high'], f"Row {idx}: 1h_high mismatch")
            self.assertEqual(h1_low, row['low'], f"Row {idx}: 1h_low mismatch")
            self.assertEqual(h1_close, row['close'], f"Row {idx}: 1h_close mismatch")
            
            # 5m and 15m should be reasonable relative to 1h
            tf_5m_high = row['5m_high']
            tf_5m_low = row['5m_low']
            tf_15m_high = row['15m_high']
            tf_15m_low = row['15m_low']
            
            # Multi-timeframe data should be in reasonable ranges
            self.assertGreater(tf_5m_high, 50, f"Row {idx}: 5m_high too low")
            self.assertLess(tf_5m_high, 500, f"Row {idx}: 5m_high too high")
            self.assertGreater(tf_15m_high, 50, f"Row {idx}: 15m_high too low")
            self.assertLess(tf_15m_high, 500, f"Row {idx}: 15m_high too high")
    
    def test_no_extreme_outliers(self):
        """Test that there are no extreme outliers that suggest data corruption."""
        df = generate_hourly_test_data(self.test_symbol, self.test_days)
        
        # Check for extreme outliers in key columns
        price_columns = ['open', 'high', 'low', 'close', 'sma_20', 'ema_12', 'ema_26']
        
        for col in price_columns:
            values = df[col].values
            
            # No values should be exactly 0 (suggests uninitialized data)
            zero_count = np.sum(values == 0)
            self.assertEqual(zero_count, 0, f"Column '{col}' has {zero_count} zero values")
            
            # No values should be > 10000 (suggests volume/price confusion)
            extreme_high = np.sum(values > 10000)
            self.assertEqual(extreme_high, 0, 
                f"Column '{col}' has {extreme_high} values > 10000, max = {np.max(values)}")
            
            # No negative prices
            negative_count = np.sum(values < 0)
            self.assertEqual(negative_count, 0, f"Column '{col}' has {negative_count} negative values")
    
    def test_volume_sma_distinction(self):
        """
        Test that volume_sma_20 and sma_20 are clearly different ranges.
        
        🚨 CRITICAL: This test catches the exact column confusion bug the user reported.
        The bug was SMA showing volume-range values (1,168,745) instead of price-range (~150).
        This prevents that specific data corruption from happening again.
        """
        df = generate_hourly_test_data(self.test_symbol, self.test_days)
        
        sma_20_values = df['sma_20'].values
        volume_values = df['volume'].values
        volume_sma_20_values = df['volume_sma_20'].values
        
        # SMA_20 (price) should be in hundreds
        sma_20_max = np.max(sma_20_values)
        sma_20_min = np.min(sma_20_values)
        
        # Volume and Volume_SMA_20 should be in hundreds of thousands/millions
        volume_max = np.max(volume_values)
        volume_min = np.min(volume_values)
        volume_sma_20_max = np.max(volume_sma_20_values)
        volume_sma_20_min = np.min(volume_sma_20_values)
        
        # Clear range separation (this catches the bug!)
        # CRITICAL: This would catch the user's reported bug (SMA = 1,168,745 > 1000)
        self.assertLess(sma_20_max, 1000, 
            f"SMA_20 max ({sma_20_max}) is too high - should be in price range (~150)")
        
        self.assertGreater(volume_sma_20_min, 10000, 
            f"Volume_SMA_20 min ({volume_sma_20_min}) is too low - should be in volume range")
        
        # The ranges should NOT overlap (this catches the confusion)
        # CRITICAL: This ensures price indicators stay in price range, volume indicators in volume range
        self.assertLess(sma_20_max * 10, volume_sma_20_min, 
            f"SMA_20 and Volume_SMA_20 ranges overlap! "
            f"SMA_20 range: {sma_20_min}-{sma_20_max}, "
            f"Volume_SMA_20 range: {volume_sma_20_min}-{volume_sma_20_max}")
        
        # 🚨 CRITICAL: Test for specific column confusion bug
        # SMA should NEVER equal volume values (prevents exact user bug)
        for idx in range(min(10, len(df))):
            sma_20 = df.iloc[idx]['sma_20']
            volume = df.iloc[idx]['volume']
            volume_sma_20 = df.iloc[idx]['volume_sma_20']
            
            # This would catch the user's bug where SMA was accidentally set to volume values
            self.assertNotEqual(sma_20, volume, 
                f"Row {idx}: SMA_20 ({sma_20}) equals volume ({volume}) - column confusion!")
            
            self.assertNotEqual(sma_20, volume_sma_20, 
                f"Row {idx}: SMA_20 ({sma_20}) equals volume_sma_20 ({volume_sma_20}) - column confusion!")
            
            # SMA should be much closer to price than to volume (catch range confusion)
            close_price = df.iloc[idx]['close']
            price_diff = abs(sma_20 - close_price)
            volume_diff = abs(sma_20 - volume)
            
            # CRITICAL: This assertion would fail with user's bug (SMA closer to volume than price)
            self.assertLess(price_diff * 100, volume_diff, 
                f"Row {idx}: SMA_20 ({sma_20}) is closer to volume ({volume}) than price ({close_price})!")


def run_tests():
    """Run the test suite and report results."""
    print("🧪 Running Hourly Training Data Validation Tests...")
    print("=" * 60)
    
    # Create test suite
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestHourlyTrainingDataValidation)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    print("=" * 60)
    
    if result.wasSuccessful():
        print("✅ ALL TESTS PASSED - Training data generation is working correctly!")
        return 0
    else:
        print("❌ TESTS FAILED - Issues detected in training data generation!")
        print(f"Failures: {len(result.failures)}")
        print(f"Errors: {len(result.errors)}")
        
        # Print failure details
        for test, traceback in result.failures:
            print(f"\n🚨 FAILURE: {test}")
            print(traceback)
        
        for test, traceback in result.errors:
            print(f"\n💥 ERROR: {test}")
            print(traceback)
        
        return 1


if __name__ == "__main__":
    exit_code = run_tests()
    exit(exit_code)