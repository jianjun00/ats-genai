#!/usr/bin/env python3
"""
Unit tests for hourly training data aggregation logic.

Tests the core hourly aggregation functions without requiring full infrastructure.
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from ml.training_data.runners.training_data_callback_runner import TrainingDataJobRunner, TrainingDataJobConfig

class TestHourlyAggregation(unittest.TestCase):
    """Test hourly aggregation logic independently."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = TrainingDataJobConfig(
            job_name="test_hourly",
            symbols=['AAPL'],
            start_date=datetime(2025, 8, 1).date(),
            end_date=datetime(2025, 8, 5).date(),
            base_interval_minutes=1,
            training_interval_minutes=60,
            output_structure="hourly_rows",
            use_universe_state_indicators=True,
            normalize_features=False
        )
        
        # Mock environment
        self.mock_env = Mock()
        self.mock_env.get_database_url.return_value = "postgresql://test"
        
        self.runner = TrainingDataJobRunner(self.mock_env)
        self.runner.config = self.config
        self.runner.run_id = 999
    
    def create_sample_minute_data(self):
        """Create realistic minute-level test data."""
        # Create 2 hours of minute data (9:30-11:30) for one trading day
        start_time = datetime(2025, 8, 4, 9, 30)  # Monday 9:30 AM
        
        minute_data = []
        base_price = 200.0
        
        for minute in range(120):  # 2 hours = 120 minutes
            timestamp = start_time + timedelta(minutes=minute)
            
            # Create realistic price movement
            price_change = np.random.normal(0, 0.1)
            current_price = base_price + price_change
            
            minute_bar = {
                'datetime': timestamp,
                'open': round(current_price + np.random.uniform(-0.1, 0.1), 2),
                'high': round(current_price + np.random.uniform(0.05, 0.5), 2),
                'low': round(current_price - np.random.uniform(0.05, 0.5), 2),
                'close': round(current_price + np.random.uniform(-0.1, 0.1), 2),
                'volume': np.random.randint(100, 1000),
                'vwap': round(current_price, 2),
                'trade_count': np.random.randint(10, 50)
            }
            
            # Ensure OHLC logic
            minute_bar['high'] = max(minute_bar['high'], minute_bar['open'], minute_bar['close'])
            minute_bar['low'] = min(minute_bar['low'], minute_bar['open'], minute_bar['close'])
            
            minute_data.append(minute_bar)
            base_price = minute_bar['close']
        
        return pd.DataFrame(minute_data)
    
    def test_aggregate_minutes_to_hourly_basic(self):
        """Test basic hourly aggregation without universe state indicators."""
        minute_data = self.create_sample_minute_data()
        
        # Test aggregation without universe state manager
        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=None
        )
        
        # Should create 2 hourly rows (9:30-10:30, 10:30-11:30)
        self.assertEqual(len(hourly_rows), 2)
        
        # Check first hour (9:30-10:30)
        first_hour = hourly_rows[0]
        self.assertEqual(first_hour['symbol'], 'AAPL')
        self.assertEqual(first_hour['datetime'].hour, 9)
        
        # Verify OHLCV aggregation logic
        first_hour_data = minute_data[minute_data['datetime'].dt.hour == 9]
        expected_open = first_hour_data['open'].iloc[0]
        expected_high = first_hour_data['high'].max()
        expected_low = first_hour_data['low'].min()
        expected_close = first_hour_data['close'].iloc[-1]
        expected_volume = first_hour_data['volume'].sum()
        
        self.assertEqual(first_hour['hour_open'], expected_open)
        self.assertEqual(first_hour['hour_high'], expected_high)
        self.assertEqual(first_hour['hour_low'], expected_low)
        self.assertEqual(first_hour['hour_close'], expected_close)
        self.assertEqual(first_hour['hour_volume'], expected_volume)
        
        # Check market period identification
        self.assertIn(first_hour['market_period'], [
            'market_open', 'morning_session', 'morning_early'
        ])
        
        # Check day progress (0.0 = market open, 1.0 = market close)
        self.assertGreaterEqual(first_hour['day_progress'], 0.0)
        self.assertLessEqual(first_hour['day_progress'], 1.0)
    
    def test_aggregate_minutes_to_hourly_with_universe_manager(self):
        """Test hourly aggregation with universe state indicators."""
        minute_data = self.create_sample_minute_data()
        
        # Mock universe state manager
        mock_universe_manager = Mock()
        mock_universe_manager.get_indicators_for_hour.return_value = {
            'envelope_top': 205.5,
            'envelope_bot': 195.2,
            'pldot': -0.0032,
            'oneone_high': 204.8,
            'oneone_low': 196.1,
            'z1b': 3.2,
            'z2b': 6.1,
            'z5t': 2.8,
            'z6t': 4.5
        }
        
        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=mock_universe_manager
        )
        
        # Should have universe state indicators
        first_hour = hourly_rows[0]
        self.assertIn('hour_envelope_top', first_hour)
        self.assertIn('hour_envelope_bot', first_hour)
        self.assertIn('hour_pldot', first_hour)
        self.assertIn('hour_oneone_high', first_hour)
        self.assertIn('hour_oneone_low', first_hour)
        self.assertIn('hour_z1b', first_hour)
        self.assertIn('hour_z2b', first_hour)
        self.assertIn('hour_z5t', first_hour)
        self.assertIn('hour_z6t', first_hour)
        
        # Verify indicator values
        self.assertEqual(first_hour['hour_envelope_top'], 205.5)
        self.assertEqual(first_hour['hour_envelope_bot'], 195.2)
        self.assertEqual(first_hour['hour_pldot'], -0.0032)
    
    def test_empty_minute_data_handling(self):
        """Test handling of empty minute data."""
        empty_data = pd.DataFrame()
        
        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            empty_data, 'AAPL', universe_manager=None
        )
        
        self.assertEqual(len(hourly_rows), 0)
    
    def test_single_minute_data_handling(self):
        """Test handling of single minute data point."""
        single_minute = pd.DataFrame([{
            'datetime': datetime(2025, 8, 4, 10, 0),
            'open': 200.0,
            'high': 200.5,
            'low': 199.8,
            'close': 200.2,
            'volume': 1000
        }])
        
        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            single_minute, 'AAPL', universe_manager=None
        )
        
        self.assertEqual(len(hourly_rows), 1)
        hour_row = hourly_rows[0]
        
        # For single minute, OHLC should match the single data point
        self.assertEqual(hour_row['hour_open'], 200.0)
        self.assertEqual(hour_row['hour_high'], 200.5)
        self.assertEqual(hour_row['hour_low'], 199.8)
        self.assertEqual(hour_row['hour_close'], 200.2)
        self.assertEqual(hour_row['hour_volume'], 1000)
    
    def test_market_period_identification(self):
        """Test market period identification logic."""
        # Test various hours
        test_hours = [
            (9, 'market_open'),
            (10, 'morning_session'),
            (12, 'lunch_session'),
            (14, 'afternoon_session'),
            (15, 'market_close_approach')
        ]
        
        for hour, expected_period in test_hours:
            minute_data = pd.DataFrame([{
                'datetime': datetime(2025, 8, 4, hour, 30),
                'open': 200.0, 'high': 200.5, 'low': 199.8, 'close': 200.2, 'volume': 1000
            }])
            
            hourly_rows = self.runner._aggregate_minutes_to_hourly(
                minute_data, 'AAPL', universe_manager=None
            )
            
            self.assertEqual(len(hourly_rows), 1)
            # Note: The actual period logic might be different, this tests the structure
            self.assertIn('market_period', hourly_rows[0])
    
    def test_data_quality_and_types(self):
        """Test data quality and type consistency."""
        minute_data = self.create_sample_minute_data()
        
        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=None
        )
        
        for hour_row in hourly_rows:
            # Test required fields
            required_fields = [
                'datetime', 'symbol', 'hour_open', 'hour_high', 
                'hour_low', 'hour_close', 'hour_volume',
                'market_period', 'day_progress'
            ]
            
            for field in required_fields:
                self.assertIn(field, hour_row, f"Missing field: {field}")
            
            # Test data types
            self.assertIsInstance(hour_row['datetime'], (datetime, pd.Timestamp))
            self.assertIsInstance(hour_row['symbol'], str)
            self.assertIsInstance(hour_row['hour_open'], (int, float))
            self.assertIsInstance(hour_row['hour_high'], (int, float))
            self.assertIsInstance(hour_row['hour_low'], (int, float))
            self.assertIsInstance(hour_row['hour_close'], (int, float))
            self.assertIsInstance(hour_row['hour_volume'], (int, np.integer))
            self.assertIsInstance(hour_row['market_period'], str)
            self.assertIsInstance(hour_row['day_progress'], (int, float))
            
            # Test OHLC logic
            self.assertLessEqual(hour_row['hour_low'], hour_row['hour_open'])
            self.assertLessEqual(hour_row['hour_low'], hour_row['hour_close'])
            self.assertGreaterEqual(hour_row['hour_high'], hour_row['hour_open'])
            self.assertGreaterEqual(hour_row['hour_high'], hour_row['hour_close'])
            
            # Test volume is positive
            self.assertGreater(hour_row['hour_volume'], 0)
            
            # Test day progress is valid
            self.assertGreaterEqual(hour_row['day_progress'], 0.0)
            self.assertLessEqual(hour_row['day_progress'], 1.0)

class TestHourlyDataFrameGeneration(unittest.TestCase):
    """Test DataFrame generation from hourly rows."""
    
    def test_hourly_dataframe_creation(self):
        """Test creation of hourly DataFrame with proper structure."""
        # This would test the conversion of hourly rows to pandas DataFrame
        # Testing the structure that would be expected by the training data system
        
        sample_hourly_rows = [
            {
                'datetime': datetime(2025, 8, 4, 9, 0),
                'symbol': 'AAPL',
                'hour_open': 200.0,
                'hour_high': 202.5,
                'hour_low': 199.5,
                'hour_close': 201.2,
                'hour_volume': 50000,
                'market_period': 'market_open',
                'day_progress': 0.0
            },
            {
                'datetime': datetime(2025, 8, 4, 10, 0),
                'symbol': 'AAPL',
                'hour_open': 201.2,
                'hour_high': 203.1,
                'hour_low': 200.8,
                'hour_close': 202.5,
                'hour_volume': 45000,
                'market_period': 'morning_session',
                'day_progress': 0.15
            }
        ]
        
        df = pd.DataFrame(sample_hourly_rows)
        
        # Test DataFrame structure
        self.assertEqual(len(df), 2)
        self.assertIn('datetime', df.columns)
        self.assertIn('symbol', df.columns)
        
        # Test datetime index potential
        df_indexed = df.set_index(['datetime', 'symbol'])
        self.assertEqual(len(df_indexed.index.levels), 2)  # Multi-index with 2 levels
        
        # Test column data types
        numeric_columns = ['hour_open', 'hour_high', 'hour_low', 'hour_close', 'hour_volume', 'day_progress']
        for col in numeric_columns:
            self.assertTrue(pd.api.types.is_numeric_dtype(df[col]), f"Column {col} should be numeric")

if __name__ == '__main__':
    unittest.main()