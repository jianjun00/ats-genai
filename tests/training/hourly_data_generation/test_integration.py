#!/usr/bin/env python3
"""
Integration tests for hourly training data generation with FileBasedMinuteManager.

Tests the complete pipeline from minute data files to hourly training data.
"""

import unittest
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
import sys
import os
import tempfile
import shutil

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from app.training_data_job_runner import TrainingDataJobRunner, TrainingDataJobConfig
from storage.file_based_minute_manager import FileBasedMinuteManager


class TestHourlyTrainingDataIntegration(unittest.TestCase):
    """Integration tests for complete hourly training data generation."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class with temporary data directory."""
        cls.test_data_dir = tempfile.mkdtemp(prefix='hourly_test_')
        cls.addClassCleanup(shutil.rmtree, cls.test_data_dir)
        
    def setUp(self):
        """Set up test fixtures for each test."""
        self.config = TrainingDataJobConfig(
            job_name="test_integration",
            symbols=['AAPL', 'MSFT'],
            start_date=datetime(2025, 8, 4).date(),
            end_date=datetime(2025, 8, 6).date(),
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
        self.runner.run_id = 888
    
    def create_test_minute_files(self, base_path: Path, symbol: str, start_date: datetime, end_date: datetime):
        """Create test minute data files in the expected format."""
        current_date = start_date
        
        while current_date <= end_date:
            # Skip weekends
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue
            
            year = current_date.year
            month = current_date.month
            
            # Create directory structure
            symbol_dir = base_path / symbol / str(year) / f"{month:02d}"
            symbol_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate minute data for this trading day
            minute_data = []
            market_open = current_date.replace(hour=9, minute=30, second=0, microsecond=0)
            
            base_price = 200.0 + np.random.normal(0, 5)
            
            # Generate 6.5 hours of trading (390 minutes)
            for minute in range(390):
                timestamp = market_open + timedelta(minutes=minute)
                
                price_change = np.random.normal(0, 0.2)
                current_price = base_price + price_change
                
                minute_bar = {
                    'timestamp': timestamp,
                    'open': round(current_price + np.random.uniform(-0.1, 0.1), 2),
                    'high': round(current_price + np.random.uniform(0.05, 0.3), 2),
                    'low': round(current_price - np.random.uniform(0.05, 0.3), 2),
                    'close': round(current_price + np.random.uniform(-0.1, 0.1), 2),
                    'volume': np.random.randint(100, 2000),
                    'vwap': round(current_price, 2),
                    'trade_count': np.random.randint(10, 100),
                    'vendor': 'test_data',
                    'quality_score': 1.0
                }
                
                # Ensure OHLC logic
                minute_bar['high'] = max(minute_bar['high'], minute_bar['open'], minute_bar['close'])
                minute_bar['low'] = min(minute_bar['low'], minute_bar['open'], minute_bar['close'])
                
                minute_data.append(minute_bar)
                base_price = minute_bar['close']
            
            # Save to Parquet file
            if minute_data:
                df = pd.DataFrame(minute_data)
                df.set_index('timestamp', inplace=True)
                
                file_path = symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"
                df.to_parquet(file_path, engine='pyarrow')
            
            current_date += timedelta(days=1)
    
    @patch('storage.file_based_minute_manager.FileBasedMinuteManager')
    async def test_hourly_generation_with_real_minute_manager(self, MockMinuteManager):
        """Test hourly generation with mocked FileBasedMinuteManager."""
        
        # Create realistic minute data
        minute_data_rows = []
        start_time = datetime(2025, 8, 4, 9, 30)
        
        for minute in range(120):  # 2 hours of data
            timestamp = start_time + timedelta(minutes=minute)
            minute_data_rows.append({
                'datetime': timestamp,
                'open': 200.0 + np.random.uniform(-1, 1),
                'high': 201.0 + np.random.uniform(-1, 1), 
                'low': 199.0 + np.random.uniform(-1, 1),
                'close': 200.5 + np.random.uniform(-1, 1),
                'volume': np.random.randint(100, 1000)
            })
        
        mock_minute_data = pd.DataFrame(minute_data_rows)
        
        # Configure mock
        mock_manager_instance = AsyncMock()
        mock_manager_instance.get_minute_data.return_value = mock_minute_data
        MockMinuteManager.return_value = mock_manager_instance
        
        # Mock universe state manager  
        mock_universe_manager = Mock()
        mock_universe_manager.get_indicators_for_hour.return_value = {
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
        
        with patch('state.universe_state_manager.UniverseStateManager', return_value=mock_universe_manager):
            # Test the hourly generation
            hourly_df, metadata = await self.runner._generate_hourly_training_data()
            
            # Verify results
            self.assertIsInstance(hourly_df, pd.DataFrame)
            self.assertGreater(len(hourly_df), 0, "Should generate hourly rows")
            
            # Check structure
            expected_columns = [
                'datetime', 'symbol', 'hour_open', 'hour_high', 'hour_low', 
                'hour_close', 'hour_volume', 'market_period', 'day_progress'
            ]
            
            for col in expected_columns:
                self.assertIn(col, hourly_df.columns, f"Missing column: {col}")
            
            # Check metadata
            self.assertEqual(metadata['structure'], 'hourly_rows')
            self.assertEqual(metadata['base_interval_minutes'], 1)
            self.assertEqual(metadata['training_interval_minutes'], 60)
            self.assertIn('datetime', metadata['primary_keys'])
            self.assertIn('symbol', metadata['primary_keys'])
    
    @patch('storage.file_based_minute_manager.FileBasedMinuteManager')
    async def test_no_minute_data_available(self, MockMinuteManager):
        """Test behavior when no minute data is available."""
        
        # Configure mock to return empty data
        mock_manager_instance = AsyncMock()
        mock_manager_instance.get_minute_data.return_value = pd.DataFrame()  # Empty DataFrame
        MockMinuteManager.return_value = mock_manager_instance
        
        # Test the hourly generation
        hourly_df, metadata = await self.runner._generate_hourly_training_data()
        
        # Should return empty DataFrame but with proper structure
        self.assertIsInstance(hourly_df, pd.DataFrame)
        self.assertEqual(len(hourly_df), 0, "Should return empty DataFrame when no minute data")
        
        # Metadata should still be valid
        self.assertEqual(metadata['structure'], 'hourly_rows')
    
    def test_file_based_minute_manager_initialization(self):
        """Test FileBasedMinuteManager can be initialized with custom path."""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test custom base path
            manager = FileBasedMinuteManager(base_path=temp_dir)
            self.assertEqual(str(manager.base_path), temp_dir)
            
            # Test directory creation
            self.assertTrue(manager.base_path.exists())
    
    async def test_minute_data_retrieval_format(self):
        """Test that minute data retrieval returns expected format."""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            base_path = Path(temp_dir)
            
            # Create test data files
            self.create_test_minute_files(
                base_path, 'AAPL', 
                datetime(2025, 8, 4), datetime(2025, 8, 4)
            )
            
            # Initialize manager
            manager = FileBasedMinuteManager(base_path=temp_dir)
            
            # Retrieve data
            minute_data = await manager.get_minute_data(
                symbol='AAPL',
                start_date=datetime(2025, 8, 4).date(),
                end_date=datetime(2025, 8, 4).date()
            )
            
            if minute_data is not None and not minute_data.empty:
                # Check expected columns
                expected_columns = ['open', 'high', 'low', 'close', 'volume']
                for col in expected_columns:
                    self.assertIn(col, minute_data.columns, f"Missing column: {col}")
                
                # Check data types
                for col in expected_columns[:-1]:  # OHLC should be float
                    self.assertTrue(pd.api.types.is_numeric_dtype(minute_data[col]))
                
                # Volume should be integer
                self.assertTrue(pd.api.types.is_integer_dtype(minute_data['volume']))
                
                # Check index is datetime
                if hasattr(minute_data.index, 'dtype'):
                    self.assertTrue(pd.api.types.is_datetime64_any_dtype(minute_data.index))


class TestAsyncHourlyGeneration(unittest.TestCase):
    """Test async behavior of hourly generation."""
    
    def setUp(self):
        self.config = TrainingDataJobConfig(
            job_name="test_async",
            symbols=['AAPL'],
            start_date=datetime(2025, 8, 4).date(),
            end_date=datetime(2025, 8, 4).date(),
            base_interval_minutes=1,
            training_interval_minutes=60,
            output_structure="hourly_rows",
            use_universe_state_indicators=False,  # Disable for simpler testing
            normalize_features=False
        )
        
        self.mock_env = Mock()
        self.mock_env.get_database_url.return_value = "postgresql://test"
        
        self.runner = TrainingDataJobRunner(self.mock_env)
        self.runner.config = self.config
        self.runner.run_id = 777
    
    async def test_async_hourly_generation_without_universe_state(self):
        """Test async hourly generation without universe state indicators."""
        
        # This test should fail because we require minute data but have none
        # This verifies the "no fallback, no mock data" requirement
        
        with self.assertRaises(Exception):
            await self.runner._generate_hourly_training_data()


def run_async_test(test_func):
    """Helper to run async tests."""
    return asyncio.run(test_func())


if __name__ == '__main__':
    # Set up test suite with async support
    unittest.main()