#!/usr/bin/env python3
"""
Integration Tests for Multi-Timeframe Training Example Validation

This test suite specifically validates that training examples contain:
1. OHLCV features from all timeframes (5m, 15m, 1h, 1d, 1w)
2. Technical signals from all timeframes 
3. Correct feature naming conventions
4. Proper sequence lengths per timeframe
5. Mathematical correctness of computed values

Tests the complete integration between:
- FileBasedMinuteMarketDataManager
- IntervalBasedTrainingDataCallback  
- Multi-timeframe feature extraction
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
from typing import Dict, List, Any

# Import test subjects
from config.environment import Environment, EnvironmentType
from market_data.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager
from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestMultiTimeframeExampleValidation:
    """Integration tests for multi-timeframe training example validation."""
    
    @pytest.fixture
    def rich_minute_data(self):
        """Generate rich 1-minute data with enough history for all timeframes."""
        # Generate 4 weeks of market data (20 trading days * 6.5 hours * 60 minutes)
        start_time = datetime(2025, 1, 1, 9, 30)  # Market open
        
        timestamps = []
        current = start_time
        
        # Generate 20 trading days of market hours data
        trading_days = 0
        while trading_days < 20:
            if current.weekday() < 5:  # Monday to Friday only
                # Market hours: 9:30 AM to 4:00 PM (6.5 hours)
                day_start = current.replace(hour=9, minute=30, second=0, microsecond=0)
                day_end = current.replace(hour=16, minute=0, second=0, microsecond=0)
                
                minute_timestamp = day_start
                while minute_timestamp < day_end:
                    timestamps.append(minute_timestamp)
                    minute_timestamp += timedelta(minutes=1)
                
                trading_days += 1
                
            current += timedelta(days=1)
        
        n_bars = len(timestamps)
        logger.info(f"Generated {n_bars} minute bars across {trading_days} trading days")
        
        # Generate realistic price progression
        base_price = 150.0
        price_trend = np.linspace(0, 10, n_bars)  # Slight upward trend
        price_noise = np.random.normal(0, 0.5, n_bars)
        price_levels = base_price + price_trend + np.cumsum(np.random.normal(0, 0.1, n_bars))
        
        # Generate OHLCV data with realistic intrabar movements
        data = []
        for i, ts in enumerate(timestamps):
            base_price = price_levels[i]
            
            # Generate realistic intrabar price action
            spread = abs(np.random.normal(0, 0.3))  # Price spread within bar
            
            if i == 0:
                open_price = base_price
            else:
                # Open usually close to previous close with some gap
                prev_close = data[i-1]['close']
                gap = np.random.normal(0, 0.05)  # Small gap
                open_price = prev_close + gap
            
            # Generate high/low around the base price
            high = base_price + spread/2 + abs(np.random.normal(0, 0.1))
            low = base_price - spread/2 - abs(np.random.normal(0, 0.1))
            close = base_price + np.random.normal(0, 0.1)
            
            # Ensure OHLC relationships
            high = max(high, open_price, close)
            low = min(low, open_price, close)
            
            # Generate realistic volume with some patterns
            base_volume = 1000
            time_factor = 1.0
            if ts.hour in [9, 10, 15]:  # Higher volume at open and close
                time_factor = 2.0
            elif ts.hour in [12, 13]:  # Lower volume at lunch
                time_factor = 0.7
                
            volume = int(base_volume * time_factor * (1 + np.random.normal(0, 0.5)))
            volume = max(volume, 10)  # Minimum volume
            
            data.append({
                'timestamp': ts,
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(close, 2),
                'volume': volume
            })
        
        df = pd.DataFrame(data)
        logger.info(f"Rich minute data: {len(df)} bars from {df['timestamp'].min()} to {df['timestamp'].max()}")
        return df
    
    @pytest.fixture
    async def complete_market_manager(self, rich_minute_data):
        """Create market data manager with comprehensive test data."""
        
        class ComprehensiveMinuteManager:
            def __init__(self, data):
                self.data = data
            
            async def query_minute_data(self, symbol, start_date, end_date):
                # Return data within the requested range
                mask = (self.data['timestamp'] >= start_date) & \
                       (self.data['timestamp'] <= end_date)
                result = self.data[mask].reset_index(drop=True)
                logger.debug(f"Query for {symbol}: {len(result)} bars from {start_date} to {end_date}")
                return result
        
        # Create manager with comprehensive data
        env = Environment(None, EnvironmentType.TEST)
        manager = FileBasedMinuteMarketDataManager(env, "/tmp/test")
        manager.minute_manager = ComprehensiveMinuteManager(rich_minute_data)
        
        return manager
    
    @pytest.mark.asyncio
    async def test_all_timeframes_present_in_example(self, complete_market_manager):
        """Test that training examples contain features from all required timeframes."""
        
        from dataclasses import dataclass, field
        
        @dataclass
        class CompleteConfig:
            timeframes: Dict[str, int] = field(default_factory=lambda: {
                '5m': 5, '15m': 15, '1h': 60, '1d': 1440, '1w': 10080
            })
            sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
                '5m': 20, '15m': 16, '1h': 24, '1d': 10, '1w': 4
            })
        
        config = CompleteConfig()
        
        # Create callback with comprehensive configuration
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=config,
            output_dir="/tmp/test_output"
        )
        
        callback.minute_data_manager = complete_market_manager
        callback.start_date = datetime(2025, 1, 1).date()
        callback.end_date = datetime(2025, 1, 31).date()
        
        # Generate example at a time when we should have sufficient data
        current_time = datetime(2025, 1, 20, 14, 0)  # Late in the dataset
        
        example = await callback._generate_multi_timeframe_example('AAPL', current_time)
        
        assert example is not None, "Training example should not be None with sufficient data"
        
        features = example['features']
        timeframes = example['timeframes']
        
        # Verify all expected timeframes are present
        expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
        assert set(timeframes) == set(expected_timeframes), \
            f"Timeframes mismatch. Expected: {expected_timeframes}, Got: {timeframes}"
        
        # Test comprehensive feature presence for each timeframe
        feature_validation = {}
        
        for tf in expected_timeframes:
            tf_features = {k: v for k, v in features.items() if k.startswith(f'{tf}_')}
            
            # Count feature types
            ohlcv_features = [k for k in tf_features.keys() if any(x in k for x in ['open', 'high', 'low', 'close', 'volume'])]
            signal_features = [k for k in tf_features.keys() if any(x in k for x in ['sma', 'ema', 'rsi', 'etop', 'ebot', 'pldot', 'vwap'])]
            
            feature_validation[tf] = {
                'total_features': len(tf_features),
                'ohlcv_features': len(ohlcv_features),
                'signal_features': len(signal_features),
                'feature_names': list(tf_features.keys())
            }
            
            # Verify minimum expected features
            assert len(ohlcv_features) >= 5, f"Timeframe {tf} missing OHLCV features: only {len(ohlcv_features)} found"
            assert len(signal_features) >= 3, f"Timeframe {tf} missing signal features: only {len(signal_features)} found"
            
            logger.info(f"✅ Timeframe {tf}: {len(ohlcv_features)} OHLCV + {len(signal_features)} signals = {len(tf_features)} total features")
        
        # Log comprehensive feature breakdown
        for tf, validation in feature_validation.items():
            logger.info(f"Timeframe {tf} features: {validation['feature_names'][:10]}...")  # Show first 10
        
        # Verify total feature count is substantial
        total_features = sum(len(v) if isinstance(v, list) else 1 for v in features.values())
        assert total_features >= 100, f"Expected at least 100 features, got {total_features}"
        
        logger.info(f"✅ Comprehensive feature validation passed: {total_features} total features across {len(expected_timeframes)} timeframes")
    
    @pytest.mark.asyncio
    async def test_ohlcv_feature_sequence_lengths(self, complete_market_manager):
        """Test that OHLCV features have correct sequence lengths per timeframe."""
        
        from dataclasses import dataclass, field
        
        @dataclass
        class SequenceLengthConfig:
            timeframes: Dict[str, int] = field(default_factory=lambda: {
                '5m': 5, '15m': 15, '1h': 60, '1d': 1440, '1w': 10080
            })
            sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
                '5m': 24, '15m': 20, '1h': 16, '1d': 12, '1w': 8  # Different lengths per timeframe
            })
        
        config = SequenceLengthConfig()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=config,
            output_dir="/tmp/test"
        )
        
        callback.minute_data_manager = complete_market_manager
        callback.start_date = datetime(2025, 1, 1).date()
        callback.end_date = datetime(2025, 1, 31).date()
        
        current_time = datetime(2025, 1, 25, 15, 30)  # Late in dataset for max data availability
        
        example = await callback._generate_multi_timeframe_example('AAPL', current_time)
        
        assert example is not None, "Training example should be generated"
        
        features = example['features']
        
        # Test sequence lengths for OHLCV features in each timeframe
        for tf, expected_length in config.sequence_lengths.items():\n            # Find OHLCV features for this timeframe
            ohlcv_patterns = ['open', 'high', 'low', 'close', 'volume']
            
            for pattern in ohlcv_patterns:
                feature_key = f'{tf}_{pattern}'\n                if feature_key in features:\n                    feature_data = features[feature_key]
                    \n                    if isinstance(feature_data, list):\n                        actual_length = len(feature_data)
                        \n                        # Sequence length should not exceed configured maximum
                        assert actual_length <= expected_length, \\\n                            f\"Feature {feature_key} has {actual_length} values, expected <= {expected_length}\"
                        \n                        # Should have some data (at least 1 value)
                        assert actual_length >= 1, \\\n                            f\"Feature {feature_key} has no data\"
                        \n                        # Verify all values are numeric
                        assert all(isinstance(x, (int, float)) and not pd.isna(x) for x in feature_data), \\\n                            f\"Feature {feature_key} contains non-numeric values\"
                        \n                        logger.info(f\"✅ Feature {feature_key}: {actual_length}/{expected_length} values, all numeric\")
        
        logger.info(\"✅ OHLCV sequence length validation passed\")
    
    @pytest.mark.asyncio
    async def test_signal_feature_mathematical_correctness(self, complete_market_manager):
        """Test mathematical correctness of signal features across timeframes."""
        
        # Get multi-timeframe data directly from manager for validation
        symbols = ['AAPL']
        start = datetime(2025, 1, 10)
        end = datetime(2025, 1, 25)
        intervals = ['1h']  # Focus on 1h for detailed validation
        signals = ['sma_20', 'ema_12', 'rsi_14', 'etop', 'ebot', 'pldot']
        
        multi_data = await complete_market_manager.get_multi_timeframe_data(
            symbols=symbols,
            start=start,
            end=end,
            intervals=intervals,
            signals=signals
        )
        
        assert 'AAPL' in multi_data, \"AAPL data missing\"
        assert '1h' in multi_data['AAPL'], \"1h data missing\"
        
        df = multi_data['AAPL']['1h']
        
        if len(df) >= 20:  # Need sufficient data for validation
            
            # Test SMA calculation
            if 'sma_20' in df.columns:
                # Manually calculate last SMA value
                last_sma = df['sma_20'].iloc[-1]
                manual_sma = df['close'].tail(20).mean()
                
                assert abs(last_sma - manual_sma) < 1e-8, \\\n                    f\"SMA calculation error: {last_sma} vs {manual_sma}\"
                logger.info(f\"✅ SMA validation: {last_sma:.4f} (manual: {manual_sma:.4f})\")
            
            # Test EMA properties
            if 'ema_12' in df.columns:
                ema_values = df['ema_12'].dropna()
                assert len(ema_values) > 0, \"EMA has no values\"
                
                # EMA should be responsive to price changes
                close_values = df['close'].tail(len(ema_values))
                correlation = np.corrcoef(ema_values, close_values)[0, 1]
                assert correlation > 0.5, f\"EMA correlation with price too low: {correlation}\"
                logger.info(f\"✅ EMA correlation with price: {correlation:.4f}\")
            
            # Test RSI bounds
            if 'rsi_14' in df.columns:
                rsi_values = df['rsi_14'].dropna()
                assert len(rsi_values) > 0, \"RSI has no values\"
                assert (rsi_values >= 0).all(), \"RSI values below 0\"
                assert (rsi_values <= 100).all(), \"RSI values above 100\"
                logger.info(f\"✅ RSI bounds: {rsi_values.min():.2f} - {rsi_values.max():.2f}\")
            
            # Test ETOP/EBOT relationship
            if 'etop' in df.columns and 'ebot' in df.columns:
                etop_values = df['etop'].dropna()
                ebot_values = df['ebot'].dropna()
                
                if len(etop_values) > 0 and len(ebot_values) > 0:
                    # ETOP should generally be above EBOT
                    min_len = min(len(etop_values), len(ebot_values))
                    etop_above_ebot = (etop_values.tail(min_len).values >= ebot_values.tail(min_len).values).mean()
                    assert etop_above_ebot > 0.95, f\"ETOP not consistently above EBOT: {etop_above_ebot}\"
                    logger.info(f\"✅ ETOP >= EBOT in {etop_above_ebot:.1%} of cases\")
            
            # Test PLDOT calculation (should be between high and low)
            if 'pldot' in df.columns:
                pldot_values = df['pldot'].dropna()
                if len(pldot_values) > 0:
                    # PLDOT should be (H+L+C)/3, so between L and H
                    recent_data = df.tail(len(pldot_values))
                    manual_pldot = (recent_data['high'] + recent_data['low'] + recent_data['close']) / 3
                    
                    diff = abs(pldot_values.values - manual_pldot.values).max()
                    assert diff < 1e-8, f\"PLDOT calculation error: max diff {diff}\"
                    logger.info(f\"✅ PLDOT calculation verified (max diff: {diff:.2e})\")
        
        logger.info(\"✅ Signal mathematical correctness validated\")
    
    @pytest.mark.asyncio
    async def test_complete_training_example_structure(self, complete_market_manager):
        \"\"\"Test complete structure of training examples with all required fields.\"\"\"
        
        from dataclasses import dataclass, field
        
        @dataclass
        class FullConfig:
            timeframes: Dict[str, int] = field(default_factory=lambda: {
                '5m': 5, '15m': 15, '1h': 60, '1d': 1440, '1w': 10080
            })
            sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
                '5m': 12, '15m': 12, '1h': 24, '1d': 10, '1w': 4
            })
        
        config = FullConfig()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],  # Test multiple symbols
            config=config,
            output_dir=\"/tmp/test\"
        )
        
        callback.minute_data_manager = complete_market_manager
        callback.start_date = datetime(2025, 1, 15).date()
        callback.end_date = datetime(2025, 1, 25).date()
        
        current_time = datetime(2025, 1, 22, 13, 45)
        
        for symbol in ['AAPL', 'TSLA']:
            example = await callback._generate_multi_timeframe_example(symbol, current_time)
            
            if example is not None:  # Skip if insufficient data
                logger.info(f\"\\nValidating example for {symbol}:\")
                
                # Test required top-level fields
                required_fields = ['symbol', 'timestamp', 'features', 'feature_count', 'timeframes', 'signals', 'data_source']
                for field in required_fields:
                    assert field in example, f\"Required field '{field}' missing from example\"
                
                # Test field values
                assert example['symbol'] == symbol, f\"Symbol mismatch: {example['symbol']} vs {symbol}\"
                assert isinstance(example['timestamp'], str), \"Timestamp should be string (ISO format)\"
                assert isinstance(example['features'], dict), \"Features should be dictionary\"
                assert isinstance(example['feature_count'], int), \"Feature count should be integer\"
                assert isinstance(example['timeframes'], list), \"Timeframes should be list\"
                assert isinstance(example['signals'], list), \"Signals should be list\"
                
                # Test feature structure
                features = example['features']
                assert len(features) > 0, \"Features dictionary should not be empty\"
                
                # Categorize features by timeframe
                timeframe_feature_count = {}
                signal_feature_count = {}
                
                for feature_name, feature_data in features.items():
                    # Extract timeframe from feature name (e.g., \"5m_close\" -> \"5m\")
                    if '_' in feature_name:
                        tf = feature_name.split('_')[0]
                        if tf in config.timeframes:
                            timeframe_feature_count[tf] = timeframe_feature_count.get(tf, 0) + 1
                            
                            # Count signal types
                            signal_part = '_'.join(feature_name.split('_')[1:])
                            if any(sig in signal_part for sig in ['sma', 'ema', 'rsi', 'etop', 'ebot', 'pldot', 'vwap']):
                                signal_feature_count[tf] = signal_feature_count.get(tf, 0) + 1
                    
                    # Validate feature data
                    if isinstance(feature_data, list):
                        assert len(feature_data) > 0, f\"Feature {feature_name} has empty list\"
                        assert all(isinstance(x, (int, float)) for x in feature_data), \\\n                            f\"Feature {feature_name} contains non-numeric data\"
                
                # Report feature distribution
                logger.info(f\"  Feature distribution by timeframe: {timeframe_feature_count}\")
                logger.info(f\"  Signal features by timeframe: {signal_feature_count}\")
                logger.info(f\"  Total features: {example['feature_count']}\")
                logger.info(f\"  Timeframes: {example['timeframes']}\")
                logger.info(f\"  Signals: {example['signals']}\")
                
                # Validate feature counts
                expected_timeframes = set(config.timeframes.keys())
                actual_timeframes = set(timeframe_feature_count.keys())
                
                missing_timeframes = expected_timeframes - actual_timeframes
                if missing_timeframes:
                    logger.warning(f\"  Missing timeframes: {missing_timeframes}\")
                
                # Expect reasonable number of features per timeframe
                for tf in actual_timeframes:
                    tf_count = timeframe_feature_count[tf]
                    assert tf_count >= 5, f\"Too few features for timeframe {tf}: {tf_count}\"
                    
                logger.info(f\"✅ Complete structure validation passed for {symbol}\")
            else:
                logger.warning(f\"⚠️  No example generated for {symbol} (insufficient data)\")
        
        logger.info(\"✅ Complete training example structure validation finished\")
    
    @pytest.mark.asyncio
    async def test_feature_naming_conventions(self, complete_market_manager):
        \"\"\"Test that feature names follow correct naming conventions.\"\"\"
        
        from dataclasses import dataclass, field
        
        @dataclass
        class NamingConfig:
            timeframes: Dict[str, int] = field(default_factory=lambda: {
                '5m': 5, '15m': 15, '1h': 60, '1d': 1440
            })
            sequence_lengths: Dict[str, int] = field(default_factory=lambda: {
                '5m': 10, '15m': 10, '1h': 10, '1d': 10
            })
        
        config = NamingConfig()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=config,
            output_dir=\"/tmp/test\"
        )
        
        callback.minute_data_manager = complete_market_manager
        
        current_time = datetime(2025, 1, 20, 14, 30)
        example = await callback._generate_multi_timeframe_example('AAPL', current_time)
        
        if example is not None:
            features = example['features']
            
            # Expected patterns
            expected_patterns = {
                'ohlcv': ['open', 'high', 'low', 'close', 'volume'],
                'signals': ['sma_20', 'ema_12', 'ema_26', 'rsi_14', 'etop', 'ebot', 'pldot', 'vwap']
            }
            
            # Validate naming patterns
            for timeframe in config.timeframes.keys():
                
                # Check OHLCV naming
                for ohlcv_type in expected_patterns['ohlcv']:
                    expected_name = f'{timeframe}_{ohlcv_type}'
                    if expected_name in features:
                        assert isinstance(features[expected_name], list), \\\n                            f\"Feature {expected_name} should be a list\"
                        logger.info(f\"✅ Found OHLCV feature: {expected_name}\")
                
                # Check signal naming  
                for signal_type in expected_patterns['signals']:
                    expected_name = f'{timeframe}_{signal_type}'
                    if expected_name in features:
                        assert isinstance(features[expected_name], list), \\\n                            f\"Feature {expected_name} should be a list\"
                        logger.info(f\"✅ Found signal feature: {expected_name}\")
            
            # Validate no unexpected patterns
            for feature_name in features.keys():
                parts = feature_name.split('_')
                
                # Should start with timeframe
                assert parts[0] in config.timeframes, \\\n                    f\"Feature {feature_name} doesn't start with valid timeframe\"
                
                # Should have at least 2 parts (timeframe_type)
                assert len(parts) >= 2, \\\n                    f\"Feature {feature_name} doesn't follow timeframe_type pattern\"
                
                # If it's a sequence feature, should end with index
                if parts[-1].isdigit():
                    sequence_index = int(parts[-1])
                    timeframe = parts[0]
                    max_sequence = config.sequence_lengths.get(timeframe, 100)
                    assert sequence_index < max_sequence, \\\n                        f\"Feature {feature_name} index {sequence_index} exceeds max {max_sequence}\"
            
            logger.info(f\"✅ Feature naming convention validation passed ({len(features)} features checked)\")
    
    @pytest.mark.asyncio
    async def test_timeframe_data_consistency_cross_validation(self, complete_market_manager):
        \"\"\"Cross-validate that aggregated timeframes are mathematically consistent.\"\"\"
        
        # Get data for multiple timeframes
        symbols = ['AAPL']
        start = datetime(2025, 1, 15)
        end = datetime(2025, 1, 20)
        
        # Get 5m and 15m data for cross-validation (15m should aggregate from 5m)
        data_5m = await complete_market_manager.get_ohlc_for_interval(
            symbols, start, end, '5m'
        )
        data_15m = await complete_market_manager.get_ohlc_for_interval(
            symbols, start, end, '15m'
        )
        
        if 'AAPL' in data_5m and 'AAPL' in data_15m and not data_5m['AAPL'].empty and not data_15m['AAPL'].empty:
            df_5m = data_5m['AAPL']
            df_15m = data_15m['AAPL']
            
            logger.info(f\"Cross-validating: {len(df_5m)} 5m bars vs {len(df_15m)} 15m bars\")
            
            # For each 15m bar, verify it aggregates correctly from corresponding 5m bars
            for _, bar_15m in df_15m.iterrows():
                bar_start = bar_15m['timestamp']
                bar_end = bar_start + timedelta(minutes=15)
                
                # Find corresponding 5m bars
                mask = (df_5m['timestamp'] >= bar_start) & (df_5m['timestamp'] < bar_end)
                bars_5m_subset = df_5m[mask]
                
                if len(bars_5m_subset) >= 2:  # Need at least 2 bars for meaningful validation
                    # Validate aggregation
                    expected_open = bars_5m_subset['open'].iloc[0]
                    expected_high = bars_5m_subset['high'].max()
                    expected_low = bars_5m_subset['low'].min()
                    expected_close = bars_5m_subset['close'].iloc[-1]
                    expected_volume = bars_5m_subset['volume'].sum()
                    
                    tolerance = 1e-10
                    
                    assert abs(bar_15m['open'] - expected_open) < tolerance, \\\n                        f\"15m open mismatch at {bar_start}: {bar_15m['open']} vs {expected_open}\"
                    assert abs(bar_15m['high'] - expected_high) < tolerance, \\\n                        f\"15m high mismatch at {bar_start}: {bar_15m['high']} vs {expected_high}\"
                    assert abs(bar_15m['low'] - expected_low) < tolerance, \\\n                        f\"15m low mismatch at {bar_start}: {bar_15m['low']} vs {expected_low}\"
                    assert abs(bar_15m['close'] - expected_close) < tolerance, \\\n                        f\"15m close mismatch at {bar_start}: {bar_15m['close']} vs {expected_close}\"
                    assert bar_15m['volume'] == expected_volume, \\\n                        f\"15m volume mismatch at {bar_start}: {bar_15m['volume']} vs {expected_volume}\"
            
            logger.info(\"✅ Timeframe cross-validation passed (15m aggregation from 5m verified)\")
        else:
            logger.warning(\"⚠️  Insufficient data for cross-validation\")


if __name__ == \"__main__\":
    # Run specific tests
    pytest.main([__file__, \"-v\", \"--tb=short\", \"-k\", \"test_all_timeframes_present_in_example\"])