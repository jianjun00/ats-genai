"""
Integration tests for BX Trender indicators with the full framework.
"""
import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from types import SimpleNamespace

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from domains.trading.signals.indicator import BXTrenderBasic, BXTrenderDirectional, BXTrenderVolumeWeighted
from domains.trading.services.indicators.enhanced_indicators import BXTrenderIndicator, calculate_all_technical_indicators, ResidualReturnIndicatorConfig
from domains.trading.signals.indicator_config import IndicatorConfig

class TestBXTrenderIntegration(unittest.TestCase):
    """Integration tests for BX Trender indicators across both frameworks."""

    def setUp(self):
        """Set up test fixtures."""
        np.random.seed(42)
        self.sample_size = 50

    def create_comprehensive_test_data(self):
        """Create comprehensive test data for integration testing."""
        # Create realistic market data with various patterns
        data = []
        base_price = 100
        base_volume = 100000

        # Phase 1: Uptrend with increasing volume (bars 0-15)
        for i in range(16):
            price = base_price + i * 0.8 + np.random.uniform(-0.3, 0.3)
            volume = base_volume * (1 + i * 0.1) + np.random.uniform(-10000, 10000)

            data.append({
                'open': price * (0.998 + np.random.uniform(0, 0.004)),
                'high': price * (1.002 + np.random.uniform(0, 0.006)),
                'low': price * (0.996 - np.random.uniform(0, 0.004)),
                'close': price,
                'volume': max(volume, 1000),
                'timestamp': datetime(2024, 1, 1) + timedelta(minutes=i)
            })

        # Phase 2: Sideways with normal volume (bars 16-30)
        sideways_price = data[-1]['close']
        for i in range(16, 31):
            price = sideways_price + np.random.uniform(-1, 1)
            volume = base_volume + np.random.uniform(-20000, 20000)

            data.append({
                'open': price * (0.998 + np.random.uniform(0, 0.004)),
                'high': price * (1.001 + np.random.uniform(0, 0.003)),
                'low': price * (0.999 - np.random.uniform(0, 0.003)),
                'close': price,
                'volume': max(volume, 1000),
                'timestamp': datetime(2024, 1, 1) + timedelta(minutes=i)
            })

        # Phase 3: Downtrend with high volume (bars 31-50)
        start_price = data[-1]['close']
        for i in range(31, 51):
            price = start_price - (i - 30) * 0.6 + np.random.uniform(-0.2, 0.4)
            volume = base_volume * 1.5 + np.random.uniform(-15000, 25000)

            data.append({
                'open': price * (0.999 + np.random.uniform(0, 0.002)),
                'high': price * (1.001 + np.random.uniform(0, 0.003)),
                'low': price * (0.997 - np.random.uniform(0, 0.006)),
                'close': price,
                'volume': max(volume, 1000),
                'timestamp': datetime(2024, 1, 1) + timedelta(minutes=i)
            })

        return pd.DataFrame(data)

    def dataframe_to_intervals(self, df):
        """Convert DataFrame to InstrumentInterval objects."""
        intervals = []

        for _, row in df.iterrows():
            interval = SimpleNamespace()
            interval.open = row['open']
            interval.high = row['high']
            interval.low = row['low']
            interval.close = row['close']
            interval.traded_volume = row['volume']
            interval.start_date_time = row['timestamp']
            interval.status = 'ok'
            intervals.append(interval)

        return intervals

    def test_all_variants_consistency(self):
        """Test that all BX Trender variants produce reasonable results."""
        df_data = self.create_comprehensive_test_data()
        interval_data = self.dataframe_to_intervals(df_data)

        # Test all three framework indicators
        basic_indicator = BXTrenderBasic(period=14)
        directional_indicator = BXTrenderDirectional(period=14)
        volume_indicator = BXTrenderVolumeWeighted(period=14)

        basic_indicator.update(interval_data)
        directional_indicator.update(interval_data)
        volume_indicator.update(interval_data)

        # All should successfully calculate
        self.assertEqual(basic_indicator.status, 'ok')
        self.assertEqual(directional_indicator.status, 'ok')
        self.assertEqual(volume_indicator.status, 'ok')

        # All should return valid values
        basic_value = basic_indicator.get_value()
        directional_value = directional_indicator.get_value()
        volume_value = volume_indicator.get_value()

        self.assertIsNotNone(basic_value)
        self.assertIsNotNone(directional_value)
        self.assertIsNotNone(volume_value)

        # Values should be in expected ranges
        self.assertGreaterEqual(basic_value, 0)
        self.assertLessEqual(basic_value, 100)
        self.assertGreaterEqual(directional_value, 0)
        self.assertLessEqual(directional_value, 100)
        self.assertGreaterEqual(volume_value, 0)
        self.assertLessEqual(volume_value, 100)

    def test_enhanced_framework_variants_consistency(self):
        """Test that enhanced framework variants work consistently."""
        df_data = self.create_comprehensive_test_data()

        # Test all three enhanced variants
        basic_enhanced = BXTrenderIndicator(period=14, variant='basic')
        directional_enhanced = BXTrenderIndicator(period=14, variant='directional')
        volume_enhanced = BXTrenderIndicator(period=14, variant='volume_weighted')

        basic_result = basic_enhanced.calculate(df_data)
        directional_result = directional_enhanced.calculate(df_data)
        volume_result = volume_enhanced.calculate(df_data)

        # All should successfully calculate
        self.assertEqual(basic_result['status'], 'valid')
        self.assertEqual(directional_result['status'], 'valid')
        self.assertEqual(volume_result['status'], 'valid')

        # All should return valid values
        self.assertIsNotNone(basic_result['value'])
        self.assertIsNotNone(directional_result['value'])
        self.assertIsNotNone(volume_result['value'])

    def test_framework_consistency(self):
        """Test consistency between framework and enhanced implementations."""
        df_data = self.create_comprehensive_test_data()
        interval_data = self.dataframe_to_intervals(df_data)

        # Compare basic implementations
        framework_basic = BXTrenderBasic(period=14)
        framework_basic.update(interval_data)

        enhanced_basic = BXTrenderIndicator(period=14, variant='basic')
        enhanced_result = enhanced_basic.calculate(df_data)

        # Values should be very close (allowing for small numerical differences)
        framework_value = framework_basic.get_value()
        enhanced_value = enhanced_result['value']

        self.assertIsNotNone(framework_value)
        self.assertIsNotNone(enhanced_value)

        # Allow small numerical difference due to implementation variations
        difference = abs(framework_value - enhanced_value)
        self.assertLess(difference, 2.0, "Framework and enhanced implementations should be consistent")

    def test_comprehensive_technical_indicators(self):
        """Test BX Trender integration with comprehensive technical indicators."""
        df_data = self.create_comprehensive_test_data()

        # Create configuration with BX Trender indicators
        config = ResidualReturnIndicatorConfig.comprehensive_config()

        # Calculate all technical indicators
        results = calculate_all_technical_indicators(df_data, config)

        # Check that BX Trender indicators are included
        bx_indicator_keys = [key for key in results.keys() if 'BXTrender' in key]
        self.assertGreater(len(bx_indicator_keys), 0, "BX Trender indicators should be included")

        # Check specific BX Trender indicators
        expected_indicators = [
            'BXTrender_basic_14_value',
            'BXTrender_directional_14_value',
            'BXTrender_volume_weighted_14_value'
        ]

        for indicator in expected_indicators:
            self.assertIn(indicator, results, f"{indicator} should be in results")
            self.assertIsNotNone(results[indicator], f"{indicator} should have a value")

    def test_indicator_config_integration(self):
        """Test BX Trender indicators in IndicatorConfig."""
        # Test BX Trender specific configuration
        bx_config = IndicatorConfig.bx_trender_config()
        self.assertGreater(len(bx_config), 0)

        expected_indicators = [
            'BXTrenderBasic_14',
            'BXTrenderBasic_21',
            'BXTrenderDirectional_14',
            'BXTrenderDirectional_21',
            'BXTrenderVolumeWeighted_14',
            'BXTrenderVolumeWeighted_21'
        ]

        for indicator_name in expected_indicators:
            self.assertIn(indicator_name, bx_config, f"{indicator_name} should be in BX config")

        # Test comprehensive configuration
        comprehensive_config = IndicatorConfig.comprehensive_config()
        bx_indicators_in_comprehensive = [name for name in comprehensive_config.get_indicator_names() if 'BXTrender' in name]
        self.assertEqual(len(bx_indicators_in_comprehensive), 6, "Should have 6 BX Trender indicators in comprehensive config")

    def test_different_period_consistency(self):
        """Test that different periods work consistently across variants."""
        df_data = self.create_comprehensive_test_data()

        periods_to_test = [7, 14, 21, 30]

        for period in periods_to_test:
            if len(df_data) > period + 1:  # Ensure sufficient data
                # Test basic variant
                basic = BXTrenderIndicator(period=period, variant='basic')
                basic_result = basic.calculate(df_data)
                self.assertEqual(basic_result['status'], 'valid', f"Basic BX Trender period {period} should work")

                # Test directional variant
                directional = BXTrenderIndicator(period=period, variant='directional')
                directional_result = directional.calculate(df_data)
                self.assertEqual(directional_result['status'], 'valid', f"Directional BX Trender period {period} should work")

                # Test volume weighted variant
                volume = BXTrenderIndicator(period=period, variant='volume_weighted')
                volume_result = volume.calculate(df_data)
                self.assertEqual(volume_result['status'], 'valid', f"Volume weighted BX Trender period {period} should work")

    def test_trend_detection_consistency(self):
        """Test that all variants consistently detect major trend changes."""
        # Create data with clear trend phases
        data = []
        base_price = 100

        # Strong uptrend (20 bars)
        for i in range(20):
            price = base_price + i * 1.0  # Strong uptrend
            volume = 100000 + i * 5000  # Increasing volume
            data.append({
                'open': price * 0.999,
                'high': price * 1.003,
                'low': price * 0.996,
                'close': price,
                'volume': volume,
                'timestamp': datetime(2024, 1, 1) + timedelta(minutes=i)
            })

        df_data = pd.DataFrame(data)
        interval_data = self.dataframe_to_intervals(df_data)

        # Test all variants
        basic = BXTrenderBasic(period=14)
        basic.update(interval_data)

        directional = BXTrenderDirectional(period=14)
        directional.update(interval_data)

        volume_weighted = BXTrenderVolumeWeighted(period=14)
        volume_weighted.update(interval_data)

        # All should detect bullish trend
        self.assertEqual(basic.get_trend_direction(), 1, "Basic should detect bullish trend")
        self.assertEqual(directional.trend_direction, 1, "Directional should detect bullish trend")
        self.assertEqual(volume_weighted.get_trend_direction(), 1, "Volume weighted should detect bullish trend")

        # Values should indicate bullish sentiment
        self.assertGreater(basic.get_value(), 55, "Basic value should be bullish")
        self.assertGreater(directional.get_value(), 52, "Directional value should be bullish")
        self.assertGreater(volume_weighted.get_value(), 55, "Volume weighted value should be bullish")

    def test_error_propagation_consistency(self):
        """Test that error handling is consistent across variants."""
        # Test with insufficient data
        small_data = pd.DataFrame({
            'open': [100, 101],
            'high': [102, 103],
            'low': [99, 100],
            'close': [101, 102],
            'volume': [100000, 105000],
            'timestamp': [datetime(2024, 1, 1), datetime(2024, 1, 1, 0, 1)]
        })

        basic = BXTrenderIndicator(period=14, variant='basic')
        directional = BXTrenderIndicator(period=14, variant='directional')
        volume_weighted = BXTrenderIndicator(period=14, variant='volume_weighted')

        basic_result = basic.calculate(small_data)
        directional_result = directional.calculate(small_data)
        volume_result = volume_weighted.calculate(small_data)

        # All should handle insufficient data gracefully
        self.assertEqual(basic_result['status'], 'insufficient_data')
        self.assertEqual(directional_result['status'], 'insufficient_data')
        self.assertEqual(volume_result['status'], 'insufficient_data')

        # All should return None for value
        self.assertIsNone(basic_result['value'])
        self.assertIsNone(directional_result['value'])
        self.assertIsNone(volume_result['value'])

    def test_performance_consistency(self):
        """Test that all variants perform reasonably on large datasets."""
        import time

        # Create larger dataset
        large_data = []
        for i in range(200):
            price = 100 + np.sin(i * 0.1) * 10 + np.random.uniform(-1, 1)
            volume = 100000 + np.random.uniform(-20000, 50000)
            large_data.append({
                'open': price * 0.999,
                'high': price * 1.002,
                'low': price * 0.998,
                'close': price,
                'volume': max(volume, 1000),
                'timestamp': datetime(2024, 1, 1) + timedelta(minutes=i)
            })

        df_data = pd.DataFrame(large_data)

        # Time each variant
        variants = ['basic', 'directional', 'volume_weighted']
        execution_times = {}

        for variant in variants:
            indicator = BXTrenderIndicator(period=14, variant=variant)

            start_time = time.time()
            result = indicator.calculate(df_data)
            end_time = time.time()

            execution_times[variant] = end_time - start_time

            # Should complete successfully
            self.assertEqual(result['status'], 'valid', f"{variant} should handle large dataset")
            self.assertIsNotNone(result['value'], f"{variant} should return value for large dataset")

        # All variants should complete in reasonable time (< 1 second for 200 data points)
        for variant, exec_time in execution_times.items():
            self.assertLess(exec_time, 1.0, f"{variant} should execute in reasonable time")

    def test_real_world_scenario(self):
        """Test BX Trender indicators with realistic market scenario."""
        # Simulate a realistic trading day with opening gap, intraday volatility, and volume patterns
        data = []

        # Market open with gap up and high volume
        base_price = 150
        for i in range(10):
            price = base_price + i * 0.3 + np.random.uniform(-0.2, 0.5)
            volume = 200000 + np.random.uniform(0, 100000)  # High opening volume
            data.append({
                'open': price * (0.998 if i > 0 else 1.005),  # Gap up on first bar
                'high': price * (1.004 + np.random.uniform(0, 0.004)),
                'low': price * (0.996 - np.random.uniform(0, 0.002)),
                'close': price,
                'volume': volume,
                'timestamp': datetime(2024, 1, 1, 9, 30) + timedelta(minutes=i)
            })

        # Mid-day consolidation with lower volume
        consolidation_price = data[-1]['close']
        for i in range(10, 25):
            price = consolidation_price + np.random.uniform(-0.8, 0.8)
            volume = 80000 + np.random.uniform(-20000, 40000)  # Lower consolidation volume
            data.append({
                'open': price * 0.9995,
                'high': price * (1.002 + np.random.uniform(0, 0.002)),
                'low': price * (0.998 - np.random.uniform(0, 0.002)),
                'close': price,
                'volume': max(volume, 5000),
                'timestamp': datetime(2024, 1, 1, 9, 30) + timedelta(minutes=i)
            })

        # Late day sell-off with increasing volume
        selloff_start = data[-1]['close']
        for i in range(25, 40):
            price = selloff_start - (i - 24) * 0.4 + np.random.uniform(-0.3, 0.1)
            volume = 150000 + (i - 24) * 8000 + np.random.uniform(-10000, 30000)
            data.append({
                'open': price * 1.001,
                'high': price * (1.002 + np.random.uniform(0, 0.001)),
                'low': price * (0.994 - np.random.uniform(0, 0.004)),
                'close': price,
                'volume': volume,
                'timestamp': datetime(2024, 1, 1, 9, 30) + timedelta(minutes=i)
            })

        df_data = pd.DataFrame(data)

        # Test all variants can handle this realistic scenario
        basic = BXTrenderIndicator(period=14, variant='basic')
        directional = BXTrenderIndicator(period=14, variant='directional')
        volume_weighted = BXTrenderIndicator(period=14, variant='volume_weighted')

        basic_result = basic.calculate(df_data)
        directional_result = directional.calculate(df_data)
        volume_result = volume_weighted.calculate(df_data)

        # All should successfully process realistic data
        self.assertEqual(basic_result['status'], 'valid')
        self.assertEqual(directional_result['status'], 'valid')
        self.assertEqual(volume_result['status'], 'valid')

        # Given the sell-off at the end, indicators should reflect bearish sentiment
        self.assertLess(basic_result['value'], 50, "Basic should detect end-of-day bearish sentiment")
        self.assertLess(volume_result['value'], 50, "Volume weighted should detect bearish sentiment with volume confirmation")

        # Volume weighted should show significant bearish volume given the sell-off pattern
        self.assertGreater(volume_result['bearish_volume_ratio'], volume_result['bullish_volume_ratio'])

if __name__ == '__main__':
    unittest.main()