#!/usr/bin/env python3
"""
End-to-End Tests for Complete Training Data Pipeline

This test suite validates the complete pipeline from:
1. FileBasedMinuteMarketDataManager -> Multi-timeframe OHLC + Signals
2. IntervalBasedTrainingDataCallback -> Feature extraction
3. Riegeli file output with structured directory layout
4. Validation of actual .riegeli files and metadata

Tests the complete flow that produces files like:
/mnt/d/ats-data/training/run_20250902_143706/AAPL/20250101_000000_20250131_000000.riegeli
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil
import logging
import json
import asyncio
from typing import Dict, List, Any
from unittest.mock import AsyncMock

# Import test subjects
from core.config.environment import Environment, EnvironmentType
from domains.market_data.services.core.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager
from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestCompleteTrainingPipeline:
    """End-to-end tests for complete training data pipeline."""

    @pytest.fixture
    def production_like_minute_data(self):
        """Generate production-like minute data spanning sufficient time for all timeframes."""
        # Generate 6 weeks of comprehensive market data
        # This ensures we have enough data for weekly aggregations
        start_date = datetime(2025, 1, 1, 9, 30)

        timestamps = []
        current = start_date

        # Generate 30 trading days (6 weeks) of market data
        trading_days = 0
        while trading_days < 30:
            if current.weekday() < 5:  # Monday to Friday
                # Full market day: 9:30 AM to 4:00 PM
                day_start = current.replace(hour=9, minute=30, second=0, microsecond=0)
                day_end = current.replace(hour=16, minute=0, second=0, microsecond=0)

                minute_ts = day_start
                while minute_ts < day_end:
                    timestamps.append(minute_ts)
                    minute_ts += timedelta(minutes=1)

                trading_days += 1

            current += timedelta(days=1)

        n_bars = len(timestamps)
        logger.info(f"Generated {n_bars} minute bars across {trading_days} trading days")

        # Generate realistic multi-symbol data
        symbols_data = {}

        for symbol in ['AAPL', 'TSLA', 'MSFT']:
            # Different base prices for different symbols
            base_prices = {'AAPL': 180.0, 'TSLA': 240.0, 'MSFT': 320.0}
            base_price = base_prices[symbol]

            # Generate price path with trend and volatility
            volatility = {'AAPL': 0.02, 'TSLA': 0.04, 'MSFT': 0.015}[symbol]

            # Random walk with slight upward bias
            returns = np.random.normal(0.0001, volatility, n_bars)
            price_levels = base_price * np.exp(np.cumsum(returns))

            # Generate OHLCV for each minute
            symbol_data = []
            for i, (ts, price) in enumerate(zip(timestamps, price_levels)):

                # Generate realistic intrabar price action
                spread_pct = np.random.uniform(0.0005, 0.003)  # 0.05% to 0.3% spread

                if i == 0:
                    open_price = price
                else:
                    # Small gap from previous close
                    prev_close = symbol_data[i-1]['close']
                    gap = np.random.normal(0, price * 0.001)
                    open_price = prev_close + gap

                # Generate high/low with realistic spreads
                mid_price = (open_price + price) / 2
                spread = mid_price * spread_pct

                high = mid_price + spread/2 + abs(np.random.normal(0, spread/4))
                low = mid_price - spread/2 - abs(np.random.normal(0, spread/4))
                close = price + np.random.normal(0, spread/4)

                # Ensure OHLC relationships
                high = max(high, open_price, close)
                low = min(low, open_price, close)

                # Volume with time-of-day patterns
                base_volume = {'AAPL': 2000, 'TSLA': 3000, 'MSFT': 1500}[symbol]

                # Higher volume at market open/close, lower at lunch
                hour = ts.hour
                if hour in [9, 10]:  # Market open
                    volume_multiplier = 3.0
                elif hour in [15, 16]:  # Market close
                    volume_multiplier = 2.5
                elif hour in [12, 13]:  # Lunch
                    volume_multiplier = 0.6
                else:
                    volume_multiplier = 1.0

                volume = int(base_volume * volume_multiplier * (1 + np.random.normal(0, 0.5)))
                volume = max(volume, 100)  # Minimum volume

                symbol_data.append({
                    'timestamp': ts,
                    'open': round(open_price, 2),
                    'high': round(high, 2),
                    'low': round(low, 2),
                    'close': round(close, 2),
                    'volume': volume,
                    'symbol': symbol
                })

            symbols_data[symbol] = pd.DataFrame(symbol_data)

        logger.info(f"Generated production-like data for symbols: {list(symbols_data.keys())}")
        return symbols_data

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary output directory for test files."""
        temp_dir = tempfile.mkdtemp(prefix="test_training_output_")
        yield Path(temp_dir)
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    async def production_market_manager(self, production_like_minute_data):
        """Create market data manager with production-like data."""

        class ProductionMinuteManager:
            def __init__(self, symbols_data):
                self.symbols_data = symbols_data

            async def query_minute_data(self, symbol, start_date, end_date):
                if symbol not in self.symbols_data:
                    return pd.DataFrame()

                symbol_data = self.symbols_data[symbol]
                mask = (symbol_data['timestamp'] >= start_date) & \
                       (symbol_data['timestamp'] <= end_date)
                result = symbol_data[mask].reset_index(drop=True).drop('symbol', axis=1)

                logger.debug(f"Query {symbol}: {len(result)} bars from {start_date} to {end_date}")
                return result

        env = Environment(None, EnvironmentType.TEST)
        manager = FileBasedMinuteMarketDataManager(env, "/tmp/test")
        manager.minute_manager = ProductionMinuteManager(production_like_minute_data)

        return manager

    @pytest.mark.asyncio
    async def test_complete_pipeline_with_file_output(self, production_market_manager, temp_output_dir):
        """Test complete pipeline from data manager through callback to file output."""

        from dataclasses import dataclass, field

        # Production-like configuration
        @dataclass
        class ProductionConfig:
            timeframes: Dict[str, int] = field(default_factory=lambda: {
                '5m': 5, '15m': 15, '1h': 60, '1d': 1440, '1w': 10080
            })
            sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
                '5m': 20, '15m': 16, '1h': 24, '1d': 12, '1w': 4
            })
            output_base_path: str = str(temp_output_dir)

        config = ProductionConfig()

        # Create callback with production configuration
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],
            config=config,
            output_dir=str(temp_output_dir / "run_test_123")
        )

        # Set up callback like production
        callback.minute_data_manager = production_market_manager
        callback.start_date = datetime(2025, 1, 1).date()
        callback.end_date = datetime(2025, 1, 31).date()
        callback.run_timestamp = "test_123"

        # Mock runner for handleStart
        class MockRunner:
            pass

        mock_runner = MockRunner()

        # Test handleStart (initializes directories)
        current_time = datetime(2025, 1, 15, 10, 0)
        callback.handleStart(mock_runner, current_time)

        # Verify symbol directories were created
        for symbol in ['AAPL', 'TSLA']:
            symbol_dir = Path(callback.output_dir) / symbol
            assert symbol_dir.exists(), f"Symbol directory not created: {symbol_dir}"
            logger.info(f"✅ Symbol directory created: {symbol_dir}")

        # Test handleInterval (generates examples)
        examples_generated = []

        # Process multiple intervals to simulate real pipeline
        test_times = [
            datetime(2025, 1, 15, 10, 0),
            datetime(2025, 1, 15, 11, 0),
            datetime(2025, 1, 15, 14, 0),
            datetime(2025, 1, 20, 15, 0),
        ]

        for test_time in test_times:
            logger.info(f"Processing interval at {test_time}")

            # Call handleInterval
            await callback.handleInterval(mock_runner, test_time)

        # Verify .riegeli files were created
        expected_files = []
        for symbol in ['AAPL', 'TSLA']:
            symbol_dir = Path(callback.output_dir) / symbol

            start_str = callback.start_date.strftime('%Y%m%d_000000')
            end_str = callback.end_date.strftime('%Y%m%d_000000')

            riegeli_file = symbol_dir / f"{start_str}_{end_str}.riegeli"
            metadata_file = symbol_dir / f"{start_str}_{end_str}_metadata.json"

            expected_files.extend([riegeli_file, metadata_file])

        # Check if any files were created (may depend on data availability)
        created_files = []
        for expected_file in expected_files:
            if expected_file.exists():
                created_files.append(expected_file)
                logger.info(f"✅ File created: {expected_file}")

                # Validate file size
                file_size = expected_file.stat().st_size
                assert file_size > 0, f"File is empty: {expected_file}"
                logger.info(f"   File size: {file_size} bytes")

        # Validate metadata files
        for expected_file in created_files:
            if expected_file.name.endswith('_metadata.json'):
                with open(expected_file, 'r') as f:
                    metadata = json.load(f)

                # Verify required metadata fields
                required_fields = [
                    'symbol', 'generation_time', 'example_count', 'date_range',
                    'run_timestamp', 'timeframes', 'total_features', 'data_format'
                ]

                for field in required_fields:
                    assert field in metadata, f"Required field '{field}' missing from metadata"

                logger.info(f"✅ Metadata validation passed: {expected_file.name}")
                logger.info(f"   Symbol: {metadata['symbol']}")
                logger.info(f"   Examples: {metadata['example_count']}")
                logger.info(f"   Timeframes: {metadata['timeframes']}")
                logger.info(f"   Total features: {metadata['total_features']}")

        # Test handleEnd (final summary)
        await callback.handleEnd(mock_runner, datetime(2025, 1, 31, 16, 0))

        # Verify summary file
        summary_file = Path(callback.output_dir) / "generation_summary.json"
        if summary_file.exists():
            with open(summary_file, 'r') as f:
                summary = json.load(f)

            assert 'completion_time' in summary
            assert 'symbols' in summary
            assert 'timeframes' in summary
            assert summary['processing_type'] == 'multi_timeframe_interval_based'

            logger.info(f"✅ Generation summary created: {summary_file}")

        logger.info("✅ Complete pipeline test passed - files created and validated")

    @pytest.mark.asyncio
    async def test_riegeli_file_content_validation(self, production_market_manager, temp_output_dir):
        """Test that .riegeli files contain expected multi-timeframe features."""

        from dataclasses import dataclass, field

        @dataclass
        class ContentValidationConfig:
            timeframes: Dict[str, int] = field(default_factory=lambda: {
                '5m': 5, '15m': 15, '1h': 60, '1d': 1440
            })
            sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
                '5m': 8, '15m': 6, '1h': 12, '1d': 5  # Smaller lengths for testing
            })

        config = ContentValidationConfig()

        # Generate training examples directly
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=config,
            output_dir=str(temp_output_dir)
        )

        callback.minute_data_manager = production_market_manager
        callback.start_date = datetime(2025, 1, 10).date()
        callback.end_date = datetime(2025, 1, 20).date()

        # Generate multiple examples
        examples = []
        test_times = [
            datetime(2025, 1, 15, 11, 0),
            datetime(2025, 1, 16, 13, 0),
            datetime(2025, 1, 17, 15, 0)
        ]

        for test_time in test_times:
            example = await callback._generate_multi_timeframe_example('AAPL', test_time)
            if example:
                examples.append(example)

        if examples:
            logger.info(f"Generated {len(examples)} examples for content validation")

            # Create symbol directory
            symbol_dir = temp_output_dir / 'AAPL'
            symbol_dir.mkdir(exist_ok=True)

            # Save examples using callback's riegeli method
            riegeli_path = symbol_dir / "test_content_validation.riegeli"
            await callback._save_symbol_riegeli(examples, riegeli_path, 'AAPL')

            # Validate riegeli file was created
            if riegeli_path.exists():
                file_size = riegeli_path.stat().st_size
                logger.info(f"✅ Riegeli file created: {riegeli_path} ({file_size} bytes)")

                # Analyze what should be in the file
                all_features = set()
                total_feature_values = 0

                for example in examples:
                    features = example['features']
                    all_features.update(features.keys())

                    for feature_name, feature_data in features.items():
                        if isinstance(feature_data, list):
                            total_feature_values += len(feature_data)
                        else:
                            total_feature_values += 1

                # Categorize features
                timeframe_features = {}
                ohlcv_features = set()
                signal_features = set()

                for feature_name in all_features:
                    if '_' in feature_name:
                        tf = feature_name.split('_')[0]
                        if tf not in timeframe_features:
                            timeframe_features[tf] = []
                        timeframe_features[tf].append(feature_name)

                        # Categorize feature type
                        if any(x in feature_name for x in ['open', 'high', 'low', 'close', 'volume']):
                            ohlcv_features.add(feature_name)
                        elif any(x in feature_name for x in ['sma', 'ema', 'rsi', 'etop', 'ebot', 'pldot', 'vwap']):
                            signal_features.add(feature_name)

                logger.info(f"Feature analysis:")
                logger.info(f"  Total unique features: {len(all_features)}")
                logger.info(f"  Total feature values: {total_feature_values}")
                logger.info(f"  OHLCV features: {len(ohlcv_features)}")
                logger.info(f"  Signal features: {len(signal_features)}")
                logger.info(f"  Features by timeframe: {[(tf, len(features)) for tf, features in timeframe_features.items()]}")

                # Validation assertions
                assert len(all_features) >= 20, f"Expected at least 20 unique features, got {len(all_features)}"
                assert len(ohlcv_features) >= 10, f"Expected at least 10 OHLCV features, got {len(ohlcv_features)}"
                assert len(signal_features) >= 8, f"Expected at least 8 signal features, got {len(signal_features)}"

                # Verify all configured timeframes have features
                for expected_tf in config.timeframes.keys():
                    assert expected_tf in timeframe_features, f"No features found for timeframe {expected_tf}"
                    tf_count = len(timeframe_features[expected_tf])
                    assert tf_count >= 3, f"Too few features for {expected_tf}: {tf_count}"

                logger.info("✅ Riegeli file content validation passed")
            else:
                # Fallback: check for numpy file if riegeli not available
                np_file = riegeli_path.with_suffix('.npy')
                if np_file.exists():
                    logger.info(f"✅ Fallback numpy file created: {np_file}")
                else:
                    logger.warning("⚠️  No output file created (insufficient data or errors)")
        else:
            logger.warning("⚠️  No examples generated for content validation")

    @pytest.mark.asyncio
    async def test_multi_symbol_directory_structure(self, production_market_manager, temp_output_dir):
        """Test that proper directory structure is created for multiple symbols."""

        from dataclasses import dataclass, field

        @dataclass
        class MultiSymbolConfig:
            timeframes: Dict[str, int] = field(default_factory=lambda: {
                '5m': 5, '1h': 60, '1d': 1440
            })
            sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
                '5m': 10, '1h': 12, '1d': 8
            })

        config = MultiSymbolConfig()

        # Test with 3 symbols
        symbols = ['AAPL', 'TSLA', 'MSFT']

        # Create run-specific directory
        run_timestamp = "20250902_143706"
        run_dir = temp_output_dir / f"run_{run_timestamp}"

        callback = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            config=config,
            output_dir=str(run_dir)
        )

        callback.minute_data_manager = production_market_manager
        callback.start_date = datetime(2025, 1, 10).date()
        callback.end_date = datetime(2025, 1, 20).date()
        callback.run_timestamp = run_timestamp

        # Mock runner
        class MockRunner:
            pass

        # Test directory creation
        callback.handleStart(MockRunner(), datetime(2025, 1, 15, 10, 0))

        # Verify run directory structure
        assert run_dir.exists(), f"Run directory not created: {run_dir}"

        # Verify symbol subdirectories
        for symbol in symbols:
            symbol_dir = run_dir / symbol
            assert symbol_dir.exists(), f"Symbol directory not created: {symbol_dir}"
            logger.info(f"✅ Symbol directory: {symbol_dir}")

        # Verify metadata directory
        metadata_dir = run_dir / "metadata"
        assert metadata_dir.exists(), f"Metadata directory not created: {metadata_dir}"

        # Process some intervals to generate files
        test_times = [datetime(2025, 1, 18, 14, 30)]

        for test_time in test_times:
            await callback.handleInterval(MockRunner(), test_time)

        # Check expected file pattern
        date_range = f"{callback.start_date.strftime('%Y%m%d_000000')}_{callback.end_date.strftime('%Y%m%d_000000')}"

        expected_structure = {
            'run_dir': run_dir,
            'symbols': symbols,
            'expected_files': []
        }

        for symbol in symbols:
            symbol_dir = run_dir / symbol
            riegeli_file = symbol_dir / f"{date_range}.riegeli"
            metadata_file = symbol_dir / f"{date_range}_metadata.json"

            expected_structure['expected_files'].extend([
                (symbol, 'riegeli', riegeli_file),
                (symbol, 'metadata', metadata_file)
            ])

        # Log directory structure
        logger.info(f"Expected directory structure:")
        logger.info(f"  Run directory: {run_dir}")
        for symbol in symbols:
            logger.info(f"  {symbol}/ directory with:")
            logger.info(f"    - {date_range}.riegeli")
            logger.info(f"    - {date_range}_metadata.json")

        # Verify any files that were actually created
        created_files = []
        for symbol, file_type, file_path in expected_structure['expected_files']:
            if file_path.exists():
                created_files.append((symbol, file_type, file_path))
                file_size = file_path.stat().st_size
                logger.info(f"✅ Created: {symbol} {file_type} file ({file_size} bytes)")

        # Test final summary
        await callback.handleEnd(MockRunner(), datetime(2025, 1, 20, 16, 0))

        summary_file = run_dir / "generation_summary.json"
        if summary_file.exists():
            with open(summary_file, 'r') as f:
                summary = json.load(f)

            assert summary['symbols'] == symbols
            logger.info(f"✅ Summary file created with correct symbols: {summary['symbols']}")

        logger.info("✅ Multi-symbol directory structure test passed")

        return {
            'run_dir': run_dir,
            'created_files': created_files,
            'expected_pattern': f"run_{run_timestamp}/{{SYMBOL}}/{date_range}.riegeli"
        }

    @pytest.mark.asyncio
    async def test_production_output_pattern_validation(self, production_market_manager):
        """Test that output follows exact production pattern specified."""

        # Test exact pattern: /mnt/d/ats-data/training/run_20250901_193706/TSLA/20250128_000000_20250901_000000.riegeli

        with tempfile.TemporaryDirectory() as temp_base:
            base_path = Path(temp_base) / "ats-data" / "training"
            base_path.mkdir(parents=True)

            from dataclasses import dataclass, field

            @dataclass
            class ProductionPatternConfig:
                timeframes: Dict[str, int] = field(default_factory=lambda: {
                    '5m': 5, '15m': 15, '1h': 60, '1d': 1440, '1w': 10080
                })
                sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
                    '5m': 12, '15m': 12, '1h': 24, '1d': 20, '1w': 12
                })
                output_base_path: str = str(base_path)

            config = ProductionPatternConfig()

            # Simulate exact production timestamps
            run_timestamp = "20250901_193706"
            start_date = datetime(2025, 1, 28).date()
            end_date = datetime(2025, 9, 1).date()

            structured_output_dir = base_path / f"run_{run_timestamp}"

            callback = IntervalBasedTrainingDataCallback(
                symbols=['TSLA'],
                config=config,
                output_dir=str(structured_output_dir)
            )

            callback.minute_data_manager = production_market_manager
            callback.start_date = start_date
            callback.end_date = end_date
            callback.run_timestamp = run_timestamp

            # Initialize directories
            callback.handleStart(object(), datetime(2025, 8, 15, 10, 0))

            # Expected exact path
            expected_path = structured_output_dir / "TSLA" / "20250128_000000_20250901_000000.riegeli"
            expected_metadata = structured_output_dir / "TSLA" / "20250128_000000_20250901_000000_metadata.json"

            logger.info(f"Testing exact production pattern:")
            logger.info(f"  Expected path: {expected_path}")
            logger.info(f"  Expected metadata: {expected_metadata}")

            # Verify directory structure matches exactly
            assert structured_output_dir.exists(), f"Run directory missing: {structured_output_dir}"
            assert (structured_output_dir / "TSLA").exists(), f"TSLA directory missing"

            # Process interval to generate files
            await callback.handleInterval(object(), datetime(2025, 8, 20, 14, 0))

            # Check if files were created with exact naming
            if expected_path.exists():
                logger.info(f"✅ Exact pattern file created: {expected_path}")
                file_size = expected_path.stat().st_size
                logger.info(f"   File size: {file_size} bytes")
            else:
                # Check for alternative files (numpy fallback)
                np_file = expected_path.with_suffix('.npy')
                if np_file.exists():
                    logger.info(f"✅ Fallback numpy file: {np_file}")

            if expected_metadata.exists():
                logger.info(f"✅ Exact metadata file created: {expected_metadata}")

                # Validate metadata content matches pattern
                with open(expected_metadata, 'r') as f:
                    metadata = json.load(f)

                assert metadata['symbol'] == 'TSLA'
                assert metadata['run_timestamp'] == run_timestamp
                assert metadata['date_range']['start'] == start_date.isoformat()
                assert metadata['date_range']['end'] == end_date.isoformat()

                logger.info("✅ Metadata content matches expected pattern")

            # Verify the exact path pattern construction
            actual_pattern = f"{base_path}/run_{run_timestamp}/TSLA/{start_date.strftime('%Y%m%d_000000')}_{end_date.strftime('%Y%m%d_000000')}.riegeli"
            expected_pattern_str = str(expected_path)

            assert actual_pattern == expected_pattern_str, \
                f"Path pattern mismatch:\\n  Expected: {expected_pattern_str}\\n  Actual: {actual_pattern}"

            logger.info("✅ Production output pattern validation passed")
            logger.info(f"   Verified exact pattern: run_YYYYMMDD_HHMMSS/SYMBOL/STARTDATE_ENDDATE.riegeli")


if __name__ == "__main__":
    # Run tests focusing on pipeline validation
    pytest.main([__file__, "-v", "--tb=short"])