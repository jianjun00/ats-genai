#!/usr/bin/env python3
"""
Tests for universe state builder integration with hourly training data generation.

Tests the integration between FileBasedMinuteManager, UniverseStateManager, 
and the hourly aggregation logic.
"""

import unittest
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os
import tempfile
import shutil
from unittest.mock import Mock, AsyncMock, patch, MagicMock

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from ml.training_data.runners.training_data_callback_runner import TrainingDataJobRunner, TrainingDataJobConfig


class TestUniverseStateBuilderIntegration(unittest.TestCase):
    """Test integration with universe state builder for technical indicators."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = TrainingDataJobConfig(
            job_name="universe_state_test",
            symbols=['AAPL'],
            start_date=datetime(2025, 8, 4).date(),
            end_date=datetime(2025, 8, 4).date(),
            base_interval_minutes=1,
            training_interval_minutes=60,
            output_structure="hourly_rows",
            use_universe_state_indicators=True,
            normalize_features=False
        )
        
        self.mock_env = Mock()
        self.mock_env.get_database_url.return_value = "postgresql://test"
        
        self.runner = TrainingDataJobRunner(self.mock_env)
        self.runner.config = self.config
        self.runner.run_id = 444
    
    def create_realistic_minute_data(self):
        """Create realistic minute data for universe state testing."""
        start_time = datetime(2025, 8, 4, 9, 30)
        minute_data = []
        
        base_price = 200.0
        
        for minute in range(390):  # Full trading day
            timestamp = start_time + timedelta(minutes=minute)
            
            # Create realistic price movements with trends
            price_change = np.random.normal(0, 0.3)
            current_price = base_price + price_change
            
            minute_bar = {
                'datetime': timestamp,
                'open': round(current_price + np.random.uniform(-0.1, 0.1), 2),
                'high': round(current_price + np.random.uniform(0.1, 0.5), 2),
                'low': round(current_price - np.random.uniform(0.1, 0.5), 2),
                'close': round(current_price + np.random.uniform(-0.1, 0.1), 2),
                'volume': np.random.randint(500, 2000)
            }
            
            # Ensure OHLC logic
            minute_bar['high'] = max(minute_bar['high'], minute_bar['open'], minute_bar['close'])
            minute_bar['low'] = min(minute_bar['low'], minute_bar['open'], minute_bar['close'])
            
            minute_data.append(minute_bar)
            base_price = minute_bar['close']
        
        return pd.DataFrame(minute_data)
    
    def test_universe_state_indicators_structure(self):
        """Test that universe state indicators are properly structured."""
        
        minute_data = self.create_realistic_minute_data()
        
        # Mock universe state manager with realistic indicator values
        mock_universe_manager = Mock()
        
        # Mock the get_indicators_for_hour method to return expected indicators
        def mock_get_indicators(symbol, hour_datetime):
            # Return realistic technical indicator values
            return {
                'envelope_top': round(205.5 + np.random.uniform(-2, 2), 2),
                'envelope_bot': round(194.8 + np.random.uniform(-2, 2), 2), 
                'pldot': round(np.random.uniform(-0.01, 0.01), 4),
                'oneone_high': round(204.2 + np.random.uniform(-1, 1), 2),
                'oneone_low': round(195.8 + np.random.uniform(-1, 1), 2),
                'z1b': round(np.random.uniform(1, 5), 1),
                'z2b': round(np.random.uniform(3, 8), 1),
                'z5t': round(np.random.uniform(2, 6), 1),
                'z6t': round(np.random.uniform(3, 7), 1)
            }
        
        mock_universe_manager.get_indicators_for_hour = Mock(side_effect=mock_get_indicators)
        
        # Test aggregation with universe state indicators
        hourly_rows = self.runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=mock_universe_manager
        )
        
        self.assertGreater(len(hourly_rows), 0, "Should generate hourly rows")
        
        # Test that all expected universe state indicators are present
        expected_indicators = [
            'hour_envelope_top', 'hour_envelope_bot', 'hour_pldot',
            'hour_oneone_high', 'hour_oneone_low',
            'hour_z1b', 'hour_z2b', 'hour_z5t', 'hour_z6t'
        ]
        
        first_hour = hourly_rows[0]
        for indicator in expected_indicators:
            self.assertIn(indicator, first_hour, f"Missing universe state indicator: {indicator}")
            self.assertIsInstance(first_hour[indicator], (int, float), f"Indicator {indicator} should be numeric")
        
        # Test indicator value ranges (basic sanity checks)
        self.assertGreater(first_hour['hour_envelope_top'], first_hour['hour_envelope_bot'])
        self.assertGreater(first_hour['hour_oneone_high'], first_hour['hour_oneone_low'])
        
        # Test that universe state manager was called for each hour
        expected_calls = len(hourly_rows)
        self.assertEqual(
            mock_universe_manager.get_indicators_for_hour.call_count, 
            expected_calls,
            f"Universe state manager should be called once per hour ({expected_calls} times)"
        )
    
    def test_universe_state_indicators_vs_basic_aggregation(self):
        """Test difference between universe state indicators and basic aggregation."""
        
        minute_data = self.create_realistic_minute_data()
        
        # Test without universe state indicators
        basic_hourly_rows = self.runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=None
        )
        
        # Test with universe state indicators
        mock_universe_manager = Mock()
        mock_universe_manager.get_indicators_for_hour.return_value = {
            'envelope_top': 205.0, 'envelope_bot': 195.0, 'pldot': -0.0025,
            'oneone_high': 203.5, 'oneone_low': 196.5,
            'z1b': 2.8, 'z2b': 5.2, 'z5t': 3.1, 'z6t': 4.4
        }
        
        universe_hourly_rows = self.runner._aggregate_minutes_to_hourly(
            minute_data, 'AAPL', universe_manager=mock_universe_manager
        )
        
        # Both should have same number of rows
        self.assertEqual(len(basic_hourly_rows), len(universe_hourly_rows))
        
        # Basic version should not have universe state indicators
        basic_first_hour = basic_hourly_rows[0]
        universe_indicators = [
            'hour_envelope_top', 'hour_envelope_bot', 'hour_pldot',
            'hour_oneone_high', 'hour_oneone_low',
            'hour_z1b', 'hour_z2b', 'hour_z5t', 'hour_z6t'
        ]
        
        for indicator in universe_indicators:
            self.assertNotIn(indicator, basic_first_hour, f"Basic aggregation should not have {indicator}")
        
        # Universe version should have universe state indicators
        universe_first_hour = universe_hourly_rows[0]
        for indicator in universe_indicators:
            self.assertIn(indicator, universe_first_hour, f"Universe aggregation should have {indicator}")
        
        # Common fields should be identical
        common_fields = ['datetime', 'symbol', 'hour_open', 'hour_high', 'hour_low', 'hour_close', 'hour_volume']
        for field in common_fields:
            self.assertEqual(
                basic_first_hour[field], universe_first_hour[field],
                f"Common field {field} should be identical between basic and universe aggregation"
            )
    
    @patch('state.universe_state_manager.UniverseStateManager')
    @patch('storage.file_based_minute_manager.FileBasedMinuteManager')
    async def test_full_pipeline_with_universe_state_mocking(self, MockMinuteManager, MockUniverseManager):
        """Test full pipeline with both FileBasedMinuteManager and UniverseStateManager mocked."""
        
        # Mock minute data
        minute_data = self.create_realistic_minute_data()
        mock_minute_manager = AsyncMock()
        mock_minute_manager.get_minute_data.return_value = minute_data
        MockMinuteManager.return_value = mock_minute_manager
        
        # Mock universe state manager
        mock_universe_manager = Mock()
        mock_universe_manager.get_indicators_for_hour.return_value = {
            'envelope_top': 206.2, 'envelope_bot': 194.1, 'pldot': 0.0018,
            'oneone_high': 204.9, 'oneone_low': 195.3,
            'z1b': 3.4, 'z2b': 6.7, 'z5t': 2.9, 'z6t': 5.1
        }
        MockUniverseManager.return_value = mock_universe_manager
        
        # Test the full hourly generation pipeline
        hourly_df, metadata = await self.runner._generate_hourly_training_data()
        
        # Verify results
        self.assertIsInstance(hourly_df, pd.DataFrame)
        self.assertGreater(len(hourly_df), 0, "Should generate hourly DataFrame")
        
        # Check that universe state indicators are in the DataFrame
        universe_indicator_columns = [
            'hour_envelope_top', 'hour_envelope_bot', 'hour_pldot',
            'hour_oneone_high', 'hour_oneone_low',
            'hour_z1b', 'hour_z2b', 'hour_z5t', 'hour_z6t'
        ]
        
        for col in universe_indicator_columns:
            self.assertIn(col, hourly_df.columns, f"DataFrame should contain universe indicator: {col}")
        
        # Check metadata indicates universe state builder usage
        self.assertEqual(metadata['indicators_source'], 'universe_state_builder')
        self.assertEqual(metadata['base_interval_minutes'], 1)
        self.assertEqual(metadata['training_interval_minutes'], 60)
        
        # Verify mock was called appropriately
        mock_minute_manager.get_minute_data.assert_called_once()
        self.assertGreater(
            mock_universe_manager.get_indicators_for_hour.call_count, 0,
            "Universe state manager should be called for indicator calculation"
        )
    
    def test_universe_state_error_handling(self):
        """Test error handling when universe state manager fails."""
        
        minute_data = self.create_realistic_minute_data()
        
        # Mock universe state manager that raises exception
        mock_universe_manager = Mock()
        mock_universe_manager.get_indicators_for_hour.side_effect = Exception("Universe state calculation failed")
        
        # Should not raise exception, but should handle gracefully
        with self.assertLogs() as log_context:
            hourly_rows = self.runner._aggregate_minutes_to_hourly(
                minute_data, 'AAPL', universe_manager=mock_universe_manager
            )
        
        # Should still generate hourly rows, but without universe indicators
        self.assertGreater(len(hourly_rows), 0, "Should still generate hourly rows despite universe state failure")
        
        # Universe indicators should be None or default values
        first_hour = hourly_rows[0]
        universe_indicators = [
            'hour_envelope_top', 'hour_envelope_bot', 'hour_pldot',
            'hour_oneone_high', 'hour_oneone_low',
            'hour_z1b', 'hour_z2b', 'hour_z5t', 'hour_z6t'
        ]
        
        for indicator in universe_indicators:
            if indicator in first_hour:
                # If present, should be None or a default value
                self.assertIsNone(first_hour[indicator], f"Failed universe indicator {indicator} should be None")
    
    def test_indicator_value_validation(self):
        """Test validation of universe state indicator values."""
        
        minute_data = self.create_realistic_minute_data()
        
        # Test with various indicator value scenarios
        test_scenarios = [
            {
                'name': 'normal_values',
                'indicators': {
                    'envelope_top': 205.5, 'envelope_bot': 194.8, 'pldot': -0.0032,
                    'oneone_high': 204.2, 'oneone_low': 195.8,
                    'z1b': 2.8, 'z2b': 5.2, 'z5t': 3.1, 'z6t': 4.4
                },
                'should_pass': True
            },
            {
                'name': 'extreme_values',
                'indicators': {
                    'envelope_top': 999.99, 'envelope_bot': 0.01, 'pldot': -0.5,
                    'oneone_high': 500.0, 'oneone_low': 1.0,
                    'z1b': 50.0, 'z2b': 100.0, 'z5t': 75.0, 'z6t': 80.0
                },
                'should_pass': True  # Should handle extreme values
            },
            {
                'name': 'zero_values',
                'indicators': {
                    'envelope_top': 0.0, 'envelope_bot': 0.0, 'pldot': 0.0,
                    'oneone_high': 0.0, 'oneone_low': 0.0,
                    'z1b': 0.0, 'z2b': 0.0, 'z5t': 0.0, 'z6t': 0.0
                },
                'should_pass': True  # Should handle zero values
            }
        ]
        
        for scenario in test_scenarios:
            with self.subTest(scenario=scenario['name']):
                mock_universe_manager = Mock()
                mock_universe_manager.get_indicators_for_hour.return_value = scenario['indicators']
                
                try:
                    hourly_rows = self.runner._aggregate_minutes_to_hourly(
                        minute_data, 'AAPL', universe_manager=mock_universe_manager
                    )
                    
                    if scenario['should_pass']:
                        self.assertGreater(len(hourly_rows), 0, f"Scenario {scenario['name']} should generate hourly rows")
                        
                        # Verify indicator values were preserved
                        first_hour = hourly_rows[0]
                        for indicator_name, expected_value in scenario['indicators'].items():
                            hour_indicator_name = f"hour_{indicator_name}"
                            if hour_indicator_name in first_hour:
                                self.assertEqual(
                                    first_hour[hour_indicator_name], expected_value,
                                    f"Indicator {hour_indicator_name} should have expected value in scenario {scenario['name']}"
                                )
                    
                except Exception as e:
                    if scenario['should_pass']:
                        self.fail(f"Scenario {scenario['name']} should not raise exception: {e}")


def run_async_universe_state_tests():
    """Run async universe state tests."""
    
    async def run_test():
        test_instance = TestUniverseStateBuilderIntegration()
        test_instance.setUp()
        
        print("🧪 Testing Universe State Builder Integration")
        print("=" * 50)
        
        try:
            await test_instance.test_full_pipeline_with_universe_state_mocking()
            print("✅ Full pipeline with universe state mocking PASSED")
        except Exception as e:
            print(f"❌ Full pipeline test FAILED: {e}")
            raise
    
    asyncio.run(run_test())


if __name__ == '__main__':
    # Run both sync and async tests
    print("Running synchronous universe state tests...")
    unittest.main(exit=False)
    
    print("\nRunning asynchronous universe state tests...")
    run_async_universe_state_tests()