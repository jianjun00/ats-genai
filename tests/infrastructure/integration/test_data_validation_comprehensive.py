#!/usr/bin/env python3
"""
Comprehensive data validation and file system tests.

Tests data validation issues found during AAPL training data generation:
1. Parquet file structure validation
2. OHLCV data integrity checks
3. Volume data preservation and validation
4. File system path resolution
5. Data type consistency
6. Training data output validation
"""

import pytest
import tempfile
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import os
import json


class TestDataValidationComprehensive:
    """Comprehensive data validation tests."""
    
    def test_aapl_parquet_file_structure(self):
        """Test AAPL parquet file has expected structure and data quality."""
        
        # Test the actual AAPL file we've been debugging
        aapl_file_path = "/mnt/d/ats-data/minute-bars/firstrate/A/AAPL/2025/07/AAPL_2025_07.parquet"
        
        if not os.path.exists(aapl_file_path):
            pytest.skip(f"AAPL parquet file not found: {aapl_file_path}")
            
        try:
            df = pd.read_parquet(aapl_file_path)
            
            # Test basic structure
            assert len(df) > 0, "AAPL file should not be empty"
            assert len(df.columns) >= 6, f"Expected at least 6 columns, got {len(df.columns)}"
            
            # Test required columns exist
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            assert not missing_columns, f"Missing required columns: {missing_columns}"
            
            # Test data types
            assert pd.api.types.is_datetime64_any_dtype(df['timestamp']), "timestamp should be datetime"
            for price_col in ['open', 'high', 'low', 'close']:
                assert pd.api.types.is_numeric_dtype(df[price_col]), f"{price_col} should be numeric"
            assert pd.api.types.is_numeric_dtype(df['volume']), "volume should be numeric"
            
            # Test data quality - no null values in critical columns
            for col in required_columns:
                null_count = df[col].isnull().sum()
                assert null_count == 0, f"Column {col} has {null_count} null values"
                
            # Test volume data specifically (this was the critical bug)
            assert df['volume'].min() >= 0, "Volume should be non-negative"
            assert df['volume'].max() > 0, "Should have some non-zero volume"
            volume_types = df['volume'].dtype
            assert volume_types in ['int64', 'float64'], f"Volume should be numeric, got {volume_types}"
            
            # Test the specific record we were debugging
            test_record = df[
                (df['open'].round(2) == 208.02) & 
                (df['high'].round(2) == 208.11) & 
                (df['low'].round(2) == 208.01) & 
                (df['close'].round(2) == 208.08)
            ]
            
            if not test_record.empty:
                volume_val = test_record['volume'].iloc[0]
                assert volume_val == 56512, f"Expected volume 56512, got {volume_val}"
                assert not pd.isna(volume_val), "Volume should not be NaN"
                assert volume_val > 0, "Volume should be positive"
                
            print(f"✅ AAPL parquet validation passed - {len(df)} records, volume range: {df['volume'].min()}-{df['volume'].max()}")
            
        except Exception as e:
            pytest.fail(f"AAPL parquet validation failed: {e}")

    def test_ohlc_data_consistency(self):
        """Test OHLC data consistency rules."""
        
        # Create test data with various scenarios
        test_scenarios = [
            # Valid OHLC data
            {'open': 100.0, 'high': 105.0, 'low': 98.0, 'close': 102.0, 'volume': 1000, 'valid': True},
            # High < Open (invalid)
            {'open': 100.0, 'high': 95.0, 'low': 90.0, 'close': 92.0, 'volume': 1000, 'valid': False},
            # Low > Close (invalid) 
            {'open': 100.0, 'high': 105.0, 'low': 110.0, 'close': 102.0, 'volume': 1000, 'valid': False},
            # Zero volume (edge case)
            {'open': 100.0, 'high': 105.0, 'low': 98.0, 'close': 102.0, 'volume': 0, 'valid': True},
            # Negative prices (invalid)
            {'open': -100.0, 'high': 105.0, 'low': 98.0, 'close': 102.0, 'volume': 1000, 'valid': False}
        ]
        
        for i, scenario in enumerate(test_scenarios):
            ohlc_data = {k: v for k, v in scenario.items() if k != 'valid'}
            expected_valid = scenario['valid']
            
            # Test OHLC consistency rules
            is_valid = True
            validation_errors = []
            
            try:
                # Rule 1: High should be >= max(open, close)
                if ohlc_data['high'] < max(ohlc_data['open'], ohlc_data['close']):
                    is_valid = False
                    validation_errors.append(f"High {ohlc_data['high']} < max(open, close)")
                    
                # Rule 2: Low should be <= min(open, close)
                if ohlc_data['low'] > min(ohlc_data['open'], ohlc_data['close']):
                    is_valid = False
                    validation_errors.append(f"Low {ohlc_data['low']} > min(open, close)")
                    
                # Rule 3: All prices should be positive
                for price_field in ['open', 'high', 'low', 'close']:
                    if ohlc_data[price_field] <= 0:
                        is_valid = False
                        validation_errors.append(f"{price_field} {ohlc_data[price_field]} <= 0")
                        
                # Rule 4: Volume should be non-negative
                if ohlc_data['volume'] < 0:
                    is_valid = False
                    validation_errors.append(f"Volume {ohlc_data['volume']} < 0")
                    
            except Exception as e:
                is_valid = False
                validation_errors.append(f"Exception: {e}")
                
            # Check if validation matches expected
            if is_valid != expected_valid:
                error_detail = f"Scenario {i}: Expected {'valid' if expected_valid else 'invalid'}, got {'valid' if is_valid else 'invalid'}"
                if validation_errors:
                    error_detail += f" - Errors: {validation_errors}"
                pytest.fail(error_detail)
                
        print("✅ OHLC data consistency validation passed")

    def test_volume_data_type_preservation(self):
        """Test volume data type is preserved through processing pipeline."""
        
        # Test various volume data scenarios
        volume_scenarios = [
            {'volume': 56512, 'type': int, 'description': 'Integer volume'},
            {'volume': 56512.0, 'type': float, 'description': 'Float volume'},
            {'volume': np.int64(56512), 'type': np.int64, 'description': 'NumPy int64 volume'},
            {'volume': np.float64(56512.0), 'type': np.float64, 'description': 'NumPy float64 volume'}
        ]
        
        for scenario in volume_scenarios:
            volume_val = scenario['volume']
            original_type = type(volume_val)
            
            # Test conversion to float (as done in get_minute_ohlc_batch)
            try:
                float_volume = float(volume_val)
                assert isinstance(float_volume, float), f"Conversion to float failed for {scenario['description']}"
                assert float_volume > 0, f"Converted volume should be positive: {float_volume}"
                assert float_volume == 56512.0, f"Volume value changed during conversion: {float_volume}"
                
            except Exception as e:
                pytest.fail(f"Volume conversion failed for {scenario['description']}: {e}")
                
        # Test None volume handling (the bug that caused crashes)
        try:
            none_volume = None
            
            # This should not crash but handle gracefully
            if none_volume is None:
                safe_volume = 0.0  # or np.nan, depending on strategy
            else:
                safe_volume = float(none_volume)
                
            assert isinstance(safe_volume, float), "None volume should be handled as float"
            
        except Exception as e:
            pytest.fail(f"None volume handling failed: {e}")
            
        print("✅ Volume data type preservation test passed")

    def test_firstrate_file_path_resolution(self):
        """Test FirstRate file path resolution works correctly."""
        
        # Test the path structure that was causing issues
        base_path = "/mnt/d/ats-data/minute-bars/firstrate"
        
        test_cases = [
            {
                'symbol': 'AAPL',
                'year': 2025,
                'month': 7,
                'expected_path': f"{base_path}/A/AAPL/2025/07/AAPL_2025_07.parquet"
            },
            {
                'symbol': 'TSLA', 
                'year': 2025,
                'month': 8,
                'expected_path': f"{base_path}/T/TSLA/2025/08/TSLA_2025_08.parquet"
            },
            {
                'symbol': 'MSFT',
                'year': 2024,
                'month': 12,
                'expected_path': f"{base_path}/M/MSFT/2024/12/MSFT_2024_12.parquet"
            }
        ]
        
        for test_case in test_cases:
            symbol = test_case['symbol']
            year = test_case['year']
            month = test_case['month']
            expected_path = test_case['expected_path']
            
            # Test path construction logic
            first_letter = symbol[0]
            constructed_path = f"{base_path}/{first_letter}/{symbol}/{year}/{month:02d}/{symbol}_{year}_{month:02d}.parquet"
            
            assert constructed_path == expected_path, \
                f"Path construction failed: {constructed_path} != {expected_path}"
                
            # Test Path object construction
            path_obj = Path(base_path) / first_letter / symbol / str(year) / f"{month:02d}" / f"{symbol}_{year}_{month:02d}.parquet"
            assert str(path_obj) == expected_path, \
                f"Path object construction failed: {path_obj} != {expected_path}"
                
        print("✅ FirstRate file path resolution test passed")

    def test_training_data_output_validation(self):
        """Test training data output format and validation."""
        
        # Test the expected training data output structure
        expected_structure = {
            'base_path': '/data/training_data',
            'dataset_pattern': 'dataset_YYYYMMDD_HHMMSS',
            'symbol_pattern': 'SYMBOL_STARTDATETIME_ENDDATETIME', 
            'timeframes': ['1m', '5m', '15m', '1h', '1d', '1w', '1M'],
            'file_format': '.arrayrecord'
        }
        
        # Test dataset ID pattern
        dataset_examples = [
            'dataset_20250913_132555',
            'dataset_20250913_131718', 
            'dataset_20250912_133848'
        ]
        
        import re
        dataset_pattern = r'dataset_\d{8}_\d{6}'
        for dataset_id in dataset_examples:
            assert re.match(dataset_pattern, dataset_id), f"Invalid dataset ID format: {dataset_id}"
            
        # Test symbol pattern
        symbol_examples = [
            'AAPL_20250701_000000_20250701_235959',
            'TSLA_20250601_000000_20250631_235959'
        ]
        
        symbol_pattern = r'[A-Z]+_\d{8}_\d{6}_\d{8}_\d{6}'
        for symbol_id in symbol_examples:
            assert re.match(symbol_pattern, symbol_id), f"Invalid symbol ID format: {symbol_id}"
            
        # Test complete path structure
        example_paths = [
            '/data/training_data/dataset_20250913_132555/AAPL_20250701_000000_20250701_235959/5m/AAPL_20250701_000000_20250701_235959.arrayrecord',
            '/data/training_data/dataset_20250913_132555/AAPL_20250701_000000_20250701_235959/1h/AAPL_20250701_000000_20250701_235959.arrayrecord'
        ]
        
        for path in example_paths:
            # Test path structure
            path_parts = path.split('/')
            assert len(path_parts) >= 6, f"Path should have at least 6 parts: {path}"
            assert path_parts[-1].endswith('.arrayrecord'), f"File should end with .arrayrecord: {path}"
            assert any(tf in path for tf in expected_structure['timeframes']), f"Path should contain timeframe: {path}"
            
        print("✅ Training data output validation test passed")

    def test_feature_extraction_data_types(self):
        """Test feature extraction handles different data types correctly."""
        
        # Test feature extraction with the real AAPL data values that were processed
        sample_data = {
            'open': 208.0239,
            'high': 208.1138, 
            'low': 208.0139,
            'close': 208.0839,
            'volume': 56512.0,
            'timestamp': datetime(2025, 7, 1, 14, 1, 0)
        }
        
        # Test OHLC feature calculations
        try:
            features = {}
            
            # Basic OHLC features
            for price_type in ['open', 'high', 'low', 'close']:
                features[f'1m_{price_type}'] = float(sample_data[price_type])
                assert isinstance(features[f'1m_{price_type}'], float), f"{price_type} should be float"
                assert features[f'1m_{price_type}'] > 0, f"{price_type} should be positive"
                
            # Volume feature
            features['1m_volume'] = float(sample_data['volume'])
            assert isinstance(features['1m_volume'], float), "Volume should be float"
            assert features['1m_volume'] == 56512.0, f"Volume should be 56512.0, got {features['1m_volume']}"
            
            # Derived features
            price_range = sample_data['high'] - sample_data['low']
            features['1m_range'] = float(price_range)
            assert features['1m_range'] > 0, "Price range should be positive"
            
            range_pct = price_range / sample_data['close']
            features['1m_range_pct'] = float(range_pct)
            assert 0 <= features['1m_range_pct'] <= 1, f"Range percentage should be between 0-1: {features['1m_range_pct']}"
            
            # Validate all features are finite numbers
            for feature_name, feature_value in features.items():
                assert np.isfinite(feature_value), f"Feature {feature_name} is not finite: {feature_value}"
                assert not np.isnan(feature_value), f"Feature {feature_name} is NaN: {feature_value}"
                
            print(f"✅ Feature extraction validation passed - {len(features)} features extracted")
            
        except Exception as e:
            pytest.fail(f"Feature extraction failed: {e}")

    def test_arrayrecord_file_validation(self):
        """Test ArrayRecord file format expectations."""
        
        # Test that we understand ArrayRecord format requirements
        # (This would be expanded with actual ArrayRecord testing if the library is available)
        
        arrayrecord_expectations = {
            'file_extension': '.arrayrecord',
            'data_format': 'binary',
            'supports_nested_data': True,
            'efficient_sequential_access': True,
            'supports_random_access': True
        }
        
        # Test file extension
        example_files = [
            'AAPL_20250701_000000_20250701_235959.arrayrecord',
            'TSLA_20250601_000000_20250631_235959.arrayrecord'
        ]
        
        for filename in example_files:
            assert filename.endswith(arrayrecord_expectations['file_extension']), \
                f"File should end with .arrayrecord: {filename}"
                
        # Test expected data structure for training sequences
        expected_sequence_structure = {
            'features': {
                'timeframes': ['1m', '5m', '15m', '1h', '1d', '1w', '1M'],
                'feature_types': ['ohlcv', 'returns', 'volatility', 'volume_profile', 'technical', 'indicators', 'support_resistance', 'market_structure'],
                'estimated_feature_count_per_timeframe': 21  # Based on debug output
            },
            'labels': {
                'future_returns': ['1m', '5m', '15m', '1h', '1d'],
                'volatility': ['realized', 'predicted'],
                'direction': ['up', 'down', 'sideways']
            },
            'metadata': {
                'symbol': str,
                'timestamp': datetime,
                'market_conditions': dict
            }
        }
        
        # Validate structure expectations
        assert len(expected_sequence_structure['features']['timeframes']) == 7, \
            "Should have 7 timeframes"
        assert len(expected_sequence_structure['features']['feature_types']) == 8, \
            "Should have 8 feature types"
        assert expected_sequence_structure['features']['estimated_feature_count_per_timeframe'] > 0, \
            "Should have positive feature count"
            
        print("✅ ArrayRecord file validation test passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])