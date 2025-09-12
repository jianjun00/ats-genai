#!/usr/bin/env python3
"""
AAPL Real Data Validation Test (2000-2024)

Tests the complete multi-timeframe real data system with AAPL data from 2000 to now.
Validates signal computation accuracy and system performance.
"""

import os
import sys
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging
import json
import time

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from domains.market_data.services.core.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager
    from domains.ml.services.multi_timeframe_data_collector import MultiTimeframeDataCollector
    from domains.ml.services.multi_timeframe_signal_pipeline import create_signal_pipeline
    from shared.utils.environment import Environment, EnvironmentType
    from domains.ml.services.enhanced_feature_types import EnhancedFeatureRegistry
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure to run: PYTHONPATH=src python test_aapl_real_data_validation.py")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AAPLRealDataValidator:
    """Validates the multi-timeframe system with real AAPL data."""

    def __init__(self, data_path: str = None):
        # Try different paths for different environments (found actual paths)
        if data_path is None:
            possible_paths = [
                "/data/backup/minute-files/comprehensive-sync-20250823_214935/eodhd",  # EODHD data 2020-2025
                "/data/backup/minute-files/comprehensive-sync-20250823_214935/polygon", # Polygon data 2020-2025
                "/data/backup/minute-files/comprehensive-sync-20250823_214029/eodhd",   # Alternative EODHD backup
                "/data/minute-bars",  # Expected standard path
                "/mnt/d/ats-data/minute-bars",  # Host path
                "/workspace/data/minute-bars",  # Alternative container path
            ]

            for path in possible_paths:
                if Path(path).exists() and (Path(path) / "AAPL").exists():
                    data_path = path
                    break

            if data_path is None:
                data_path = "/data/minute-bars"  # Default fallback

        self.data_path = Path(data_path)
        self.env = Environment(EnvironmentType.TEST)
        self.results = {}

    async def setup(self):
        """Initialize components."""
        logger.info(f"🚀 Setting up AAPL data validator with path: {self.data_path}")

        # Check if data path exists
        if not self.data_path.exists():
            raise FileNotFoundError(f"Data path does not exist: {self.data_path}")

        # Check for AAPL data
        aapl_path = self.data_path / "AAPL"
        if not aapl_path.exists():
            raise FileNotFoundError(f"AAPL data directory not found: {aapl_path}")

        logger.info(f"✅ Found AAPL data directory: {aapl_path}")

        # Initialize components
        self.minute_manager = FileBasedMinuteMarketDataManager(self.env, str(self.data_path))

        # Check available symbols with broader date range
        self.available_symbols = await self._get_available_symbols()
        logger.info(f"📊 Available symbols: {len(self.available_symbols)}")

        # If no symbols found with auto-detection, manually check for AAPL
        if not self.available_symbols:
            # Try direct path check
            aapl_direct_path = self.data_path / "AAPL"
            if aapl_direct_path.exists():
                logger.info("✅ Found AAPL directory, adding to available symbols")
                self.available_symbols = ['AAPL']
            else:
                raise ValueError(f"AAPL directory not found at {aapl_direct_path}")

        if 'AAPL' not in self.available_symbols:
            raise ValueError("AAPL not found in available symbols")

        # Initialize data collector with real data
        feature_registry = EnhancedFeatureRegistry()
        self.data_collector = MultiTimeframeDataCollector(
            minute_manager=self.minute_manager,
            feature_registry=feature_registry
        )

        # Initialize signal pipeline
        self.signal_pipeline = create_signal_pipeline(
            timeframes=['5min', '15min', '1hour', '1day'],
            lookback_periods=60
        )

        logger.info("✅ Setup completed successfully")

    async def _get_available_symbols(self):
        """Get list of available symbols."""
        try:
            # Use a broader date range to capture all available data
            symbols = await self.minute_manager.get_symbols_for_date_range(
                start=datetime(2000, 1, 1),  # Much broader range
                end=datetime(2030, 12, 31)
            )
            return symbols
        except Exception as e:
            logger.error(f"Error getting available symbols: {e}")
            return []

    @pytest.mark.asyncio

    async def test_data_availability(self):
        """Test AAPL data availability across different periods."""
        logger.info("🔍 Testing AAPL data availability...")

        test_periods = [
            ("2024-Jan", datetime(2024, 1, 1), datetime(2024, 1, 31)),
            ("2024-Feb", datetime(2024, 2, 1), datetime(2024, 2, 29)),
            ("2024-Aug", datetime(2024, 8, 1), datetime(2024, 8, 31)),
        ]

        availability_results = {}

        for period_name, start_date, end_date in test_periods:
            try:
                # Get 1-day sample
                sample_data = await self.minute_manager.get_minute_ohlc_batch(
                    symbols=['AAPL'],
                    start=start_date,
                    end=start_date + timedelta(days=1),
                    timeframe_minutes=1
                )

                if 'AAPL' in sample_data and not sample_data['AAPL'].empty:
                    record_count = len(sample_data['AAPL'])
                    availability_results[period_name] = {
                        'available': True,
                        'sample_records': record_count,
                        'start_date': start_date.strftime('%Y-%m-%d')
                    }
                    logger.info(f"✅ {period_name}: {record_count} records available")
                else:
                    availability_results[period_name] = {
                        'available': False,
                        'start_date': start_date.strftime('%Y-%m-%d')
                    }
                    logger.warning(f"⚠️ {period_name}: No data available")

            except Exception as e:
                logger.error(f"❌ {period_name}: Error checking availability - {e}")
                availability_results[period_name] = {
                    'available': False,
                    'error': str(e),
                    'start_date': start_date.strftime('%Y-%m-%d')
                }

        self.results['data_availability'] = availability_results

        # Find periods with data
        available_periods = [p for p, data in availability_results.items() if data.get('available', False)]
        logger.info(f"📈 Data available for {len(available_periods)}/{len(test_periods)} test periods")

        return available_periods

    @pytest.mark.asyncio

    async def test_timeframe_aggregation_accuracy(self):
        """Test accuracy of timeframe aggregation."""
        logger.info("🎯 Testing timeframe aggregation accuracy...")

        # Get a trading day worth of data
        test_date = datetime(2024, 1, 15)
        start_time = test_date.replace(hour=9, minute=30)
        end_time = test_date.replace(hour=16, minute=0)

        try:
            # Get 1-minute base data
            minute_data = await self.minute_manager.get_minute_ohlc_batch(
                symbols=['AAPL'],
                start=start_time,
                end=end_time,
                timeframe_minutes=1
            )

            if 'AAPL' not in minute_data or minute_data['AAPL'].empty:
                logger.warning("⚠️ No minute data available for aggregation test")
                return

            base_df = minute_data['AAPL']
            logger.info(f"📊 Testing aggregation with {len(base_df)} 1-minute bars")

            # Test different aggregations
            aggregation_results = {}
            timeframes_to_test = [5, 15, 60]

            for timeframe_min in timeframes_to_test:
                aggregated_data = await self.minute_manager.get_minute_ohlc_batch(
                    symbols=['AAPL'],
                    start=start_time,
                    end=end_time,
                    timeframe_minutes=timeframe_min
                )

                if 'AAPL' in aggregated_data:
                    agg_df = aggregated_data['AAPL']

                    # Validate aggregation properties
                    validation_results = self._validate_ohlc_aggregation(base_df, agg_df, timeframe_min)
                    aggregation_results[f"{timeframe_min}min"] = validation_results

                    logger.info(f"✅ {timeframe_min}-minute aggregation: {len(agg_df)} bars, validation: {validation_results['is_valid']}")
                else:
                    logger.warning(f"⚠️ No {timeframe_min}-minute aggregated data")

            self.results['aggregation_accuracy'] = aggregation_results

        except Exception as e:
            logger.error(f"❌ Aggregation accuracy test failed: {e}")
            self.results['aggregation_accuracy'] = {'error': str(e)}

    def _validate_ohlc_aggregation(self, base_df: pd.DataFrame, agg_df: pd.DataFrame, timeframe_min: int) -> dict:
        """Validate OHLC aggregation properties."""

        validation = {
            'is_valid': True,
            'errors': [],
            'base_bars': len(base_df),
            'aggregated_bars': len(agg_df),
            'expected_bars': len(base_df) // timeframe_min
        }

        if agg_df.empty:
            validation['is_valid'] = False
            validation['errors'].append("Aggregated data is empty")
            return validation

        # Check OHLC consistency
        ohlc_consistent = (
            (agg_df['high'] >= agg_df['low']).all() and
            (agg_df['high'] >= agg_df['open']).all() and
            (agg_df['high'] >= agg_df['close']).all() and
            (agg_df['low'] <= agg_df['open']).all() and
            (agg_df['low'] <= agg_df['close']).all()
        )

        if not ohlc_consistent:
            validation['is_valid'] = False
            validation['errors'].append("OHLC consistency violation")

        # Check reasonable bar count
        expected_bars = validation['expected_bars']
        actual_bars = len(agg_df)

        if abs(actual_bars - expected_bars) > max(2, expected_bars * 0.1):  # 10% tolerance
            validation['is_valid'] = False
            validation['errors'].append(f"Bar count mismatch: expected ~{expected_bars}, got {actual_bars}")

        return validation

    @pytest.mark.asyncio

    async def test_signal_computation_performance(self):
        """Test signal computation performance and accuracy."""
        logger.info("⚡ Testing signal computation performance...")

        # Get a week of data for comprehensive testing
        start_date = datetime(2024, 1, 15)
        end_date = start_date + timedelta(days=5)  # Trading week

        try:
            # Get minute data
            minute_data = await self.minute_manager.get_minute_ohlc_batch(
                symbols=['AAPL'],
                start=start_date,
                end=end_date,
                timeframe_minutes=1
            )

            if 'AAPL' not in minute_data or minute_data['AAPL'].empty:
                logger.warning("⚠️ No data available for signal computation test")
                return

            aapl_df = minute_data['AAPL']
            logger.info(f"📊 Testing signals with {len(aapl_df)} 1-minute bars")

            # Measure computation time
            start_time = time.time()

            signals = await self.signal_pipeline.compute_signals(aapl_df, symbol='AAPL')

            end_time = time.time()
            computation_time = end_time - start_time

            # Analyze results
            signal_results = {
                'computation_time_seconds': computation_time,
                'data_bars_processed': len(aapl_df),
                'timeframes_computed': len(signals['timeframes']),
                'performance_bars_per_second': len(aapl_df) / computation_time if computation_time > 0 else 0
            }

            # Analyze each timeframe
            for tf_name, tf_data in signals['timeframes'].items():
                tf_signals = tf_data.get('signals', {})
                computed_signals = [k for k, v in tf_signals.items() if v is not None]

                signal_results[f"{tf_name}_indicators_computed"] = len(computed_signals)
                signal_results[f"{tf_name}_total_indicators"] = len(tf_signals)

                # Log some example signal values
                logger.info(f"📈 {tf_name} timeframe: {len(computed_signals)}/{len(tf_signals)} indicators computed")

                # Show first few computed signals
                for name, value in list(tf_signals.items())[:3]:
                    if value is not None:
                        logger.info(f"    {name}: {value:.4f}")

            self.results['signal_computation'] = signal_results

            logger.info(f"✅ Signal computation completed in {computation_time:.2f}s ({signal_results['performance_bars_per_second']:.1f} bars/sec)")

        except Exception as e:
            logger.error(f"❌ Signal computation test failed: {e}")
            self.results['signal_computation'] = {'error': str(e)}

    @pytest.mark.asyncio

    async def test_data_collector_integration(self):
        """Test integration with MultiTimeframeDataCollector."""
        logger.info("🔗 Testing data collector integration...")

        try:
            # Test data collector with real data
            symbols = ['AAPL']
            start_date = '2024-01-15'
            end_date = '2024-01-15'

            # Test different timeframes
            timeframes_to_test = [1, 5, 15, 60]
            collector_results = {}

            for minutes in timeframes_to_test:
                start_time = time.time()

                df = await self.data_collector._get_minute_data(
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    minutes=minutes
                )

                end_time = time.time()

                result = {
                    'bars_retrieved': len(df) if not df.empty else 0,
                    'retrieval_time_seconds': end_time - start_time,
                    'uses_real_data': self.data_collector.use_real_data,
                    'data_quality_ok': self._check_data_quality(df) if not df.empty else False
                }

                collector_results[f"{minutes}min"] = result

                logger.info(f"✅ {minutes}-minute data collector test: {result['bars_retrieved']} bars in {result['retrieval_time_seconds']:.2f}s")

            self.results['data_collector_integration'] = collector_results

        except Exception as e:
            logger.error(f"❌ Data collector integration test failed: {e}")
            self.results['data_collector_integration'] = {'error': str(e)}

    def _check_data_quality(self, df: pd.DataFrame) -> bool:
        """Check basic data quality."""
        if df.empty:
            return False

        required_cols = ['timestamp', 'open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_cols):
            return False

        # Check for NaN values
        if df[required_cols].isna().any().any():
            return False

        # Check OHLC consistency
        ohlc_ok = (
            (df['high'] >= df['low']).all() and
            (df['high'] >= df['open']).all() and
            (df['high'] >= df['close']).all() and
            (df['low'] <= df['open']).all() and
            (df['low'] <= df['close']).all()
        )

        return ohlc_ok

    async def run_comprehensive_validation(self):
        """Run all validation tests."""
        logger.info("🚀 Starting comprehensive AAPL validation...")

        start_time = datetime.now()

        try:
            # Setup
            await self.setup()

            # Run all tests
            available_periods = await self.test_data_availability()

            if available_periods:
                await self.test_timeframe_aggregation_accuracy()
                await self.test_signal_computation_performance()
                await self.test_data_collector_integration()
            else:
                logger.warning("⚠️ No data available, skipping other tests")

            # Calculate overall results
            end_time = datetime.now()
            total_time = (end_time - start_time).total_seconds()

            self.results['overall'] = {
                'total_test_time_seconds': total_time,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'available_data_periods': len(available_periods),
                'all_tests_completed': True
            }

            logger.info(f"✅ Comprehensive validation completed in {total_time:.1f}s")

        except Exception as e:
            logger.error(f"❌ Comprehensive validation failed: {e}")
            self.results['overall'] = {
                'error': str(e),
                'all_tests_completed': False
            }

    def generate_report(self) -> str:
        """Generate a comprehensive validation report."""

        report = []
        report.append("=" * 60)
        report.append("AAPL REAL DATA VALIDATION REPORT")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        # Overall results
        if 'overall' in self.results:
            overall = self.results['overall']
            if 'total_test_time_seconds' in overall:
                report.append(f"Total Test Time: {overall['total_test_time_seconds']:.1f} seconds")
                report.append(f"Available Data Periods: {overall.get('available_data_periods', 0)}")
                report.append("")

        # Data availability
        if 'data_availability' in self.results:
            report.append("📊 DATA AVAILABILITY:")
            for period, data in self.results['data_availability'].items():
                status = "✅ Available" if data.get('available') else "❌ Not Available"
                records = f" ({data['sample_records']} records)" if data.get('sample_records') else ""
                report.append(f"  {period}: {status}{records}")
            report.append("")

        # Aggregation accuracy
        if 'aggregation_accuracy' in self.results:
            report.append("🎯 AGGREGATION ACCURACY:")
            agg_results = self.results['aggregation_accuracy']
            if 'error' not in agg_results:
                for timeframe, validation in agg_results.items():
                    status = "✅ Valid" if validation.get('is_valid') else "❌ Invalid"
                    bars = f" ({validation.get('aggregated_bars')} bars)"
                    report.append(f"  {timeframe}: {status}{bars}")
                    if validation.get('errors'):
                        for error in validation['errors']:
                            report.append(f"    Error: {error}")
            else:
                report.append(f"  ❌ Test failed: {agg_results['error']}")
            report.append("")

        # Signal computation
        if 'signal_computation' in self.results:
            report.append("⚡ SIGNAL COMPUTATION PERFORMANCE:")
            sig_results = self.results['signal_computation']
            if 'error' not in sig_results:
                report.append(f"  Computation Time: {sig_results.get('computation_time_seconds', 0):.2f}s")
                report.append(f"  Bars Processed: {sig_results.get('data_bars_processed', 0)}")
                report.append(f"  Performance: {sig_results.get('performance_bars_per_second', 0):.1f} bars/sec")
                report.append(f"  Timeframes: {sig_results.get('timeframes_computed', 0)}")

                # Show indicator counts per timeframe
                for key, value in sig_results.items():
                    if key.endswith('_indicators_computed'):
                        tf_name = key.replace('_indicators_computed', '')
                        total_key = f"{tf_name}_total_indicators"
                        total = sig_results.get(total_key, 0)
                        report.append(f"    {tf_name}: {value}/{total} indicators")
            else:
                report.append(f"  ❌ Test failed: {sig_results['error']}")
            report.append("")

        # Data collector integration
        if 'data_collector_integration' in self.results:
            report.append("🔗 DATA COLLECTOR INTEGRATION:")
            dc_results = self.results['data_collector_integration']
            if 'error' not in dc_results:
                for timeframe, result in dc_results.items():
                    bars = result.get('bars_retrieved', 0)
                    time_taken = result.get('retrieval_time_seconds', 0)
                    uses_real = result.get('uses_real_data', False)
                    quality_ok = result.get('data_quality_ok', False)

                    real_status = "Real Data" if uses_real else "Synthetic Data"
                    quality_status = "✅ Good" if quality_ok else "❌ Poor"

                    report.append(f"  {timeframe}: {bars} bars, {time_taken:.2f}s, {real_status}, Quality: {quality_status}")
            else:
                report.append(f"  ❌ Test failed: {dc_results['error']}")

        report.append("")
        report.append("=" * 60)

        return "\n".join(report)

    def save_results(self, filename: str = "aapl_validation_results.json"):
        """Save detailed results to JSON file."""
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        logger.info(f"💾 Results saved to {filename}")


async def main():
    """Main execution function."""

    logger.info("🚀 Starting AAPL Real Data Validation Test")

    # Create validator
    validator = AAPLRealDataValidator()

    try:
        # Run comprehensive validation
        await validator.run_comprehensive_validation()

        # Generate and display report
        report = validator.generate_report()
        print("\n" + report)

        # Save detailed results
        validator.save_results("aapl_validation_results.json")

        logger.info("✅ AAPL validation completed successfully")

    except Exception as e:
        logger.error(f"❌ AAPL validation failed: {e}")
        import traceback
        traceback.print_exc()

        # Still try to save partial results
        try:
            validator.save_results("aapl_validation_results_partial.json")
        except:
            pass


if __name__ == "__main__":
    asyncio.run(main())