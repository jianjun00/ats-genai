"""
Integration tests for Volume Profile with training dataset pipeline.
"""
import unittest
import pandas as pd
import numpy as np

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from domains.trading.signals.enhanced_indicators import (
    VolumeProfileIndicator,
    ResidualReturnIndicatorConfig,
    calculate_all_technical_indicators
)

class TestVolumeProfileIntegration(unittest.TestCase):
    """Integration tests for Volume Profile with training pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        np.random.seed(42)

        # Create comprehensive test data
        periods = 100
        self.test_data = self._create_realistic_market_data(periods)

        # Create different configurations
        self.comprehensive_config = ResidualReturnIndicatorConfig.comprehensive_config()
        self.minimal_config = ResidualReturnIndicatorConfig()
        self.minimal_config.add_indicator('VolumeProfile_20_50', lambda: VolumeProfileIndicator(20, 50))

    def _create_realistic_market_data(self, periods: int) -> pd.DataFrame:
        """Create realistic market data for testing."""
        data = []
        base_price = 100.0
        base_volume = 50000

        for i in range(periods):
            # Simulate realistic price action with trends and volatility
            trend = np.sin(i * 0.1) * 0.5  # Cyclical trend
            noise = np.random.normal(0, 0.3)
            price_change = trend + noise

            base_price += price_change
            base_price = max(base_price, 50)  # Ensure positive prices

            # Generate OHLC with realistic relationships
            open_price = base_price + np.random.uniform(-0.2, 0.2)
            close_price = base_price + np.random.uniform(-0.2, 0.2)
            high_price = max(open_price, close_price) + abs(np.random.uniform(0, 0.5))
            low_price = min(open_price, close_price) - abs(np.random.uniform(0, 0.5))

            # Volume with some correlation to price movement
            volume_factor = 1 + abs(price_change) * 0.5  # Higher volume on bigger moves
            volume = int(base_volume * volume_factor * (0.8 + np.random.uniform(0, 0.4)))

            data.append({
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': max(volume, 1000)  # Ensure minimum volume
            })

        return pd.DataFrame(data)

    def test_comprehensive_config_integration(self):
        """Test Volume Profile integration with comprehensive configuration."""
        # Get all Volume Profile indicators from config
        vp_indicators = {name: factory for name, factory in self.comprehensive_config.indicators.items()
                        if 'VolumeProfile' in name}

        self.assertGreater(len(vp_indicators), 0, "Volume Profile indicators should be in comprehensive config")

        # Test each Volume Profile indicator
        for name, factory in vp_indicators.items():
            with self.subTest(indicator=name):
                indicator = factory()
                result = indicator.calculate(self.test_data)

                self.assertEqual(result['status'], 'valid', f"{name} should calculate successfully")
                self.assertIsNotNone(result['value'], f"{name} should return POC value")
                self.assertIsNotNone(result['poc'], f"{name} should return POC")
                self.assertIsNotNone(result['vah'], f"{name} should return VAH")
                self.assertIsNotNone(result['val'], f"{name} should return VAL")

    def test_calculate_all_indicators_integration(self):
        """Test Volume Profile through calculate_all_technical_indicators function."""
        try:
            # Use minimal config to avoid dependency issues
            all_results = calculate_all_technical_indicators(self.test_data, self.minimal_config)

            self.assertIsInstance(all_results, dict)

            # calculate_all_technical_indicators flattens results with prefixes
            vp_keys = [key for key in all_results.keys() if key.startswith('VolumeProfile_20_50')]
            self.assertGreater(len(vp_keys), 0, "Should have Volume Profile results")

            # Check for essential Volume Profile features
            self.assertIn('VolumeProfile_20_50_poc', all_results)
            self.assertIn('VolumeProfile_20_50_status', all_results)

            self.assertEqual(all_results['VolumeProfile_20_50_status'], 'valid')
            self.assertIsNotNone(all_results['VolumeProfile_20_50_poc'])

        except Exception as e:
            self.fail(f"calculate_all_technical_indicators failed with Volume Profile: {e}")

    def test_multi_timeframe_consistency(self):
        """Test Volume Profile consistency across different timeframe-like periods."""
        periods_to_test = [20, 30, 40]
        results = {}

        for period in periods_to_test:
            indicator = VolumeProfileIndicator(period=period, bin_count=50)
            result = indicator.calculate(self.test_data)
            results[period] = result

            self.assertEqual(result['status'], 'valid', f"Period {period} should be valid")

        # All results should be valid but potentially different
        for period, result in results.items():
            self.assertIsNotNone(result['poc'])
            self.assertGreaterEqual(result['vah'], result['val'])

    def test_training_dataset_feature_generation(self):
        """Test Volume Profile features suitable for training dataset generation."""
        indicator = VolumeProfileIndicator(period=20, bin_count=50)
        result = indicator.calculate(self.test_data)

        self.assertEqual(result['status'], 'valid')

        # Check all features that would be useful for ML training
        required_features = [
            'poc', 'vah', 'val', 'total_volume', 'volume_concentration',
            'profile_shape', 'dominant_side', 'avg_volume_per_bin'
        ]

        for feature in required_features:
            self.assertIn(feature, result, f"Feature {feature} should be in results")
            self.assertIsNotNone(result[feature], f"Feature {feature} should not be None")

        # Test feature types for ML compatibility
        self.assertIsInstance(result['poc'], (int, float))
        self.assertIsInstance(result['vah'], (int, float))
        self.assertIsInstance(result['val'], (int, float))
        self.assertIsInstance(result['total_volume'], (int, float))
        self.assertIsInstance(result['volume_concentration'], (int, float))
        self.assertIsInstance(result['profile_shape'], str)
        self.assertIsInstance(result['dominant_side'], str)

    def test_volume_profile_with_bx_trender_compatibility(self):
        """Test Volume Profile works alongside BX Trender indicators."""
        # Create config with both Volume Profile and BX Trender
        config = ResidualReturnIndicatorConfig()
        config.add_indicator('VolumeProfile_20_50', lambda: VolumeProfileIndicator(20, 50))

        # Import BX Trender for compatibility test
        try:
            from domains.trading.signals.enhanced_indicators import BXTrenderIndicator
            config.add_indicator('BXTrender_basic_14', lambda: BXTrenderIndicator(14, 'basic'))

            # Calculate all indicators
            results = calculate_all_technical_indicators(self.test_data, config)

            # Both should work together (check for flattened keys)
            vp_keys = [key for key in results.keys() if key.startswith('VolumeProfile_20_50')]
            bx_keys = [key for key in results.keys() if key.startswith('BXTrender_basic_14')]

            self.assertGreater(len(vp_keys), 0, "Should have Volume Profile results")
            self.assertGreater(len(bx_keys), 0, "Should have BX Trender results")

            self.assertEqual(results['VolumeProfile_20_50_status'], 'valid')
            self.assertEqual(results['BXTrender_basic_14_status'], 'valid')

        except ImportError:
            self.skipTest("BX Trender not available for compatibility test")

    def test_performance_with_large_dataset(self):
        """Test Volume Profile performance with larger datasets."""
        # Create larger dataset
        large_data = self._create_realistic_market_data(1000)

        indicator = VolumeProfileIndicator(period=50, bin_count=100)

        import time
        start_time = time.time()
        result = indicator.calculate(large_data)
        end_time = time.time()

        calculation_time = end_time - start_time

        self.assertEqual(result['status'], 'valid')
        self.assertLess(calculation_time, 1.0, "Large dataset calculation should be under 1 second")

        # Verify result quality
        self.assertIsNotNone(result['poc'])
        self.assertGreater(result['total_volume'], 0)

    def test_error_handling_integration(self):
        """Test error handling in integrated environment."""
        indicator = VolumeProfileIndicator(period=20, bin_count=50)

        # Test with insufficient data
        small_data = self.test_data.head(5)
        result = indicator.calculate(small_data)
        self.assertEqual(result['status'], 'insufficient_data')

        # Test with missing columns
        incomplete_data = self.test_data.drop('volume', axis=1)
        result = indicator.calculate(incomplete_data)
        self.assertEqual(result['status'], 'missing_columns')

        # Test with invalid data - need to make ALL values invalid
        invalid_data = self.test_data.copy()
        invalid_data['close'] = -100  # All negative prices
        invalid_data['open'] = -100   # All negative prices
        invalid_data['high'] = -100   # All negative prices
        invalid_data['low'] = -100    # All negative prices
        result = indicator.calculate(invalid_data)
        self.assertEqual(result['status'], 'invalid_data')

    def test_volume_profile_metadata_completeness(self):
        """Test completeness of Volume Profile metadata for training pipeline."""
        indicator = VolumeProfileIndicator(period=20, bin_count=50)
        result = indicator.calculate(self.test_data)

        self.assertEqual(result['status'], 'valid')

        # Check distribution summary
        self.assertIn('volume_distribution_summary', result)
        summary = result['volume_distribution_summary']

        self.assertIn('total_bins', summary)
        self.assertIn('active_bins', summary)
        self.assertIn('top_volume_levels', summary)

        # Validate top volume levels structure
        top_levels = summary['top_volume_levels']
        self.assertIsInstance(top_levels, list)

        if top_levels:  # If there are top levels
            level = top_levels[0]
            self.assertIn('price', level)
            self.assertIn('volume', level)
            self.assertIn('volume_pct', level)

            # Check data types
            self.assertIsInstance(level['price'], (int, float))
            self.assertIsInstance(level['volume'], (int, float))
            self.assertIsInstance(level['volume_pct'], (int, float))

    def test_profile_shape_classification_integration(self):
        """Test profile shape classification in integrated environment."""
        # Test different market conditions
        test_scenarios = [
            ('trending_up', self._create_trending_data(50, 'up')),
            ('trending_down', self._create_trending_data(50, 'down')),
            ('sideways', self._create_sideways_data(50)),
        ]

        indicator = VolumeProfileIndicator(period=20, bin_count=30)

        for scenario_name, data in test_scenarios:
            with self.subTest(scenario=scenario_name):
                result = indicator.calculate(data)

                self.assertEqual(result['status'], 'valid')
                self.assertIn(result['profile_shape'],
                            ['balanced', 'trending', 'rotational', 'double_distribution', 'undefined'])
                self.assertIn(result['dominant_side'], ['bullish', 'bearish', 'neutral'])

    def _create_trending_data(self, periods: int, direction: str) -> pd.DataFrame:
        """Create trending market data."""
        data = []
        base_price = 100.0
        trend_factor = 0.5 if direction == 'up' else -0.5

        for i in range(periods):
            price_change = trend_factor + np.random.uniform(-0.2, 0.2)
            base_price += price_change
            base_price = max(base_price, 50)

            open_price = base_price + np.random.uniform(-0.1, 0.1)
            close_price = base_price + np.random.uniform(-0.1, 0.1)
            high_price = max(open_price, close_price) + abs(np.random.uniform(0, 0.3))
            low_price = min(open_price, close_price) - abs(np.random.uniform(0, 0.3))

            data.append({
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': 10000 + np.random.randint(-2000, 5000)
            })

        return pd.DataFrame(data)

    def _create_sideways_data(self, periods: int) -> pd.DataFrame:
        """Create sideways market data."""
        data = []
        base_price = 100.0

        for i in range(periods):
            # Small random movements around base price
            price_change = np.random.uniform(-0.3, 0.3)
            current_price = base_price + price_change

            open_price = current_price + np.random.uniform(-0.1, 0.1)
            close_price = current_price + np.random.uniform(-0.1, 0.1)
            high_price = max(open_price, close_price) + abs(np.random.uniform(0, 0.2))
            low_price = min(open_price, close_price) - abs(np.random.uniform(0, 0.2))

            data.append({
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': 10000 + np.random.randint(-2000, 5000)
            })

        return pd.DataFrame(data)

if __name__ == '__main__':
    unittest.main()