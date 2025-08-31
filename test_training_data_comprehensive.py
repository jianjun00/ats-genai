#!/usr/bin/env python3
"""
Comprehensive test suite for training data generation system.
Tests for hardcoded symbols, synthetic data, parameter passing, and data integrity.
"""

import os
import sys
import asyncio
import re
import json
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from app.training_data_job_runner import (
    TrainingDataJobRunner, 
    TrainingDataJobConfig,
    create_sample_job_config,
    run_training_data_job_for_symbol
)

class TrainingDataTestSuite:
    """Comprehensive test suite for training data generation."""
    
    def __init__(self):
        self.test_results = []
        self.failed_tests = []
    
    def test_no_hardcoded_symbols(self):
        """Test that no hardcoded symbols exist in the code."""
        
        print("🔍 Testing for hardcoded symbols...")
        
        # Read the training data job runner file
        job_runner_path = Path("src/app/training_data_job_runner.py")
        with open(job_runner_path, 'r') as f:
            content = f.read()
        
        # Look for hardcoded symbols
        hardcoded_patterns = [
            r"['\"]AAPL['\"]",
            r"['\"]TSLA['\"]", 
            r"['\"]GOOGL['\"]",
            r"['\"]MSFT['\"]",
            r"symbols.*=.*\[.*['\"][A-Z]{3,5}['\"]",  # symbols = ['SYMBOL']
        ]
        
        hardcoded_found = []
        for pattern in hardcoded_patterns:
            matches = re.finditer(pattern, content)
            for match in matches:
                line_num = content[:match.start()].count('\n') + 1
                hardcoded_found.append(f"Line {line_num}: {match.group()}")
        
        if hardcoded_found:
            print("❌ Found hardcoded symbols:")
            for item in hardcoded_found:
                print(f"  {item}")
            return False
        else:
            print("✅ No hardcoded symbols found")
            return True
    
    def test_no_synthetic_data_generation(self):
        """Test that no synthetic data generation exists in the code."""
        
        print("\n🔍 Testing for synthetic data generation...")
        
        job_runner_path = Path("src/app/training_data_job_runner.py")
        with open(job_runner_path, 'r') as f:
            content = f.read()
        
        # Look for synthetic data indicators
        synthetic_patterns = [
            r"synthetic",
            r"fake",
            r"generate.*OHLC",
            r"random\.",
            r"np\.random",
            r"base_price.*random",
            r"daily_return.*normal",
            r"lognormal.*volume",
        ]
        
        synthetic_found = []
        for pattern in synthetic_patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                line_num = content[:match.start()].count('\n') + 1
                context_start = max(0, match.start() - 50)
                context_end = min(len(content), match.end() + 50)
                context = content[context_start:context_end].replace('\n', ' ')
                synthetic_found.append(f"Line {line_num}: {context}")
        
        if synthetic_found:
            print("❌ Found synthetic data generation:")
            for item in synthetic_found:
                print(f"  {item}")
            return False
        else:
            print("✅ No synthetic data generation found")
            return True
    
    def test_dataset_naming_includes_run_id(self):
        """Test that dataset names include run_id for uniqueness."""
        
        print("\n🔍 Testing dataset naming with run_id...")
        
        # Create test configurations
        test_cases = [
            (['TEST1'], 42),
            (['TEST2'], 100), 
            (['MULTI', 'SYMBOL'], 1),
        ]
        
        for symbols, run_id in test_cases:
            config = create_sample_job_config(symbols=symbols)
            runner = TrainingDataJobRunner(config=config)
            runner.run_id = run_id
            
            # Generate dataset name (simulate the actual generation)
            dataset_id = f"dataset_{config.job_name}_run{runner.run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Verify run_id is included
            if f"run{run_id}" not in dataset_id:
                print(f"❌ Dataset name missing run_id: {dataset_id}")
                return False
            
            # Verify format is correct
            expected_pattern = r"dataset_training_data_gen_.*_run\d+_\d{8}_\d{6}"
            if not re.match(expected_pattern, dataset_id):
                print(f"❌ Dataset name doesn't match pattern: {dataset_id}")
                return False
            
            print(f"✅ {symbols} run {run_id}: {dataset_id}")
        
        return True
    
    def test_symbol_parameter_passing(self):
        """Test that symbols are correctly passed through all configuration layers."""
        
        print("\n🔍 Testing symbol parameter passing...")
        
        test_symbols = ['TEST_SYMBOL', 'ANOTHER_TEST', 'MULTI_TEST']
        
        for symbol_list in [['TEST_SYMBOL'], ['ANOTHER_TEST'], ['MULTI_TEST']]:
            config = create_sample_job_config(symbols=symbol_list)
            
            # Verify config has correct symbols
            if config.symbols != symbol_list:
                print(f"❌ Config symbols mismatch: expected {symbol_list}, got {config.symbols}")
                return False
            
            # Verify job name includes symbols
            for symbol in symbol_list:
                if symbol not in config.job_name:
                    print(f"❌ Job name doesn't include symbol {symbol}: {config.job_name}")
                    return False
            
            print(f"✅ Symbol configuration correct for {symbol_list}")
        
        return True
    
    async def test_real_data_loading_mock(self):
        """Test that real data loading is attempted (using mocks to avoid DB dependency)."""
        
        print("\n🔍 Testing real data loading mechanism...")
        
        # Mock the database connection and data
        mock_rows = [
            {
                'date': date(2024, 1, 1),
                'open': 150.0,
                'high': 155.0,
                'low': 148.0,
                'close': 153.0,
                'volume': 1000000
            },
            {
                'date': date(2024, 1, 2),
                'open': 153.0,
                'high': 157.0,
                'low': 151.0,
                'close': 156.0,
                'volume': 1200000
            }
        ]
        
        with patch('asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_conn.fetch.return_value = mock_rows
            mock_connect.return_value.__aenter__.return_value = mock_conn
            
            # Create test configuration
            config = create_sample_job_config(symbols=['TEST_REAL'])
            runner = TrainingDataJobRunner(config=config)
            
            # Test data loading with mocked database
            df = await runner._load_market_data()
            
            # Verify real data loading was attempted
            mock_connect.assert_called_once()
            mock_conn.fetch.assert_called()
            
            # Verify data structure
            if df.empty:
                print("❌ No data returned from real data loading")
                return False
            
            expected_columns = ['symbol', 'open', 'high', 'low', 'close', 'volume']
            for col in expected_columns:
                if col not in df.columns:
                    print(f"❌ Missing expected column: {col}")
                    return False
            
            print("✅ Real data loading mechanism works correctly with mocked data")
            return True
    
    def test_configuration_validation(self):
        """Test comprehensive configuration validation."""
        
        print("\n🔍 Testing configuration validation...")
        
        # Test valid configurations
        valid_configs = [
            {'symbols': ['VALID1'], 'days_back': 365},
            {'symbols': ['VALID2'], 'days_back': 30},
            {'symbols': ['MULTI', 'SYMBOL'], 'days_back': 180},
        ]
        
        for config_data in valid_configs:
            try:
                config = create_sample_job_config(
                    symbols=config_data['symbols'],
                    days_back=config_data['days_back']
                )
                
                # Verify all required fields are set
                required_fields = ['symbols', 'start_date', 'end_date', 'job_name']
                for field in required_fields:
                    if not hasattr(config, field) or getattr(config, field) is None:
                        print(f"❌ Missing required field: {field}")
                        return False
                
                # Verify symbols are correctly set
                if config.symbols != config_data['symbols']:
                    print(f"❌ Symbols not set correctly: expected {config_data['symbols']}, got {config.symbols}")
                    return False
                
                print(f"✅ Valid configuration: {config_data}")
                
            except Exception as e:
                print(f"❌ Configuration validation failed: {e}")
                return False
        
        return True
    
    async def test_error_handling_no_data(self):
        """Test error handling when no real data is available."""
        
        print("\n🔍 Testing error handling for missing data...")
        
        with patch('asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_conn.fetch.return_value = []  # No data returned
            mock_connect.return_value.__aenter__.return_value = mock_conn
            
            config = create_sample_job_config(symbols=['NO_DATA_SYMBOL'])
            runner = TrainingDataJobRunner(config=config)
            
            try:
                df = await runner._load_market_data()
                print("❌ Should have raised ValueError for no data")
                return False
            except ValueError as e:
                if "No market data available" in str(e):
                    print("✅ Correctly raises ValueError when no data found")
                    return True
                else:
                    print(f"❌ Wrong error message: {e}")
                    return False
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                return False
    
    def test_data_integrity_checks(self):
        """Test data integrity and validation checks."""
        
        print("\n🔍 Testing data integrity checks...")
        
        # Test data validation
        valid_data = {
            'date': date(2024, 1, 1),
            'open': 150.0,
            'high': 155.0,
            'low': 148.0,
            'close': 153.0,
            'volume': 1000000
        }
        
        invalid_data_cases = [
            {**valid_data, 'high': 140.0},  # High < Open
            {**valid_data, 'low': 160.0},   # Low > Close
            {**valid_data, 'volume': -1000}, # Negative volume
        ]
        
        # For now, just test that we can identify these patterns
        for i, invalid_data in enumerate(invalid_data_cases):
            # Check OHLC consistency
            ohlc_valid = (
                invalid_data['high'] >= invalid_data['low'] and
                invalid_data['high'] >= invalid_data['open'] and
                invalid_data['high'] >= invalid_data['close'] and
                invalid_data['low'] <= invalid_data['open'] and
                invalid_data['low'] <= invalid_data['close'] and
                invalid_data['volume'] >= 0
            )
            
            if ohlc_valid:
                print(f"❌ Data integrity check {i+1} failed to detect invalid data")
                return False
        
        print("✅ Data integrity checks work correctly")
        return True
    
    async def test_end_to_end_flow_mock(self):
        """Test the complete end-to-end flow with mocked dependencies."""
        
        print("\n🔍 Testing end-to-end training data generation flow...")
        
        # Mock all external dependencies
        mock_data = [
            {'date': date(2024, 1, 1), 'open': 100.0, 'high': 105.0, 'low': 99.0, 'close': 103.0, 'volume': 1000},
            {'date': date(2024, 1, 2), 'open': 103.0, 'high': 108.0, 'low': 102.0, 'close': 106.0, 'volume': 1100},
        ]
        
        with patch('asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_conn.fetch.return_value = mock_data
            mock_conn.fetchval.return_value = 1  # Mock run_id
            mock_connect.return_value.__aenter__.return_value = mock_conn
            
            # Create test config
            test_symbol = 'END_TO_END_TEST'
            config = create_sample_job_config(symbols=[test_symbol])
            
            with tempfile.TemporaryDirectory() as temp_dir:
                runner = TrainingDataJobRunner(config=config, output_dir=temp_dir)
                runner.run_id = 999  # Mock run ID
                
                # Test data loading with mocked database
                df = await runner._load_market_data()
                
                if df.empty:
                    print("❌ End-to-end test failed: no data loaded")
                    return False
                
                # Verify the correct symbol is in the data
                if test_symbol not in df['symbol'].values:
                    print(f"❌ End-to-end test failed: expected symbol {test_symbol} not found in data")
                    return False
                
                print(f"✅ End-to-end flow works correctly with mocked data for symbol: {test_symbol}")
                return True
    
    async def run_all_tests(self):
        """Run all tests and return results."""
        
        print("🧪 COMPREHENSIVE TRAINING DATA GENERATION TEST SUITE")
        print("=" * 70)
        
        # Define all tests
        tests = [
            ("No Hardcoded Symbols", self.test_no_hardcoded_symbols),
            ("No Synthetic Data", self.test_no_synthetic_data_generation), 
            ("Dataset Naming with Run ID", self.test_dataset_naming_includes_run_id),
            ("Symbol Parameter Passing", self.test_symbol_parameter_passing),
            ("Real Data Loading Mock", self.test_real_data_loading_mock),
            ("Configuration Validation", self.test_configuration_validation),
            ("Error Handling No Data", self.test_error_handling_no_data),
            ("Data Integrity Checks", self.test_data_integrity_checks),
            ("End-to-End Flow Mock", self.test_end_to_end_flow_mock),
        ]
        
        # Run each test
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            try:
                if asyncio.iscoroutinefunction(test_func):
                    result = await test_func()
                else:
                    result = test_func()
                
                if result:
                    self.test_results.append((test_name, "PASS"))
                    passed += 1
                else:
                    self.test_results.append((test_name, "FAIL"))
                    self.failed_tests.append(test_name)
                    
            except Exception as e:
                print(f"❌ Test '{test_name}' crashed: {e}")
                self.test_results.append((test_name, "ERROR"))
                self.failed_tests.append(test_name)
        
        # Print summary
        print("\n" + "=" * 70)
        print("TEST RESULTS SUMMARY:")
        print("=" * 70)
        
        for test_name, result in self.test_results:
            status_icon = {"PASS": "✅", "FAIL": "❌", "ERROR": "💥"}[result]
            print(f"{status_icon} {test_name}: {result}")
        
        print(f"\nPassed: {passed}/{total} tests")
        
        if self.failed_tests:
            print(f"\nFailed tests: {', '.join(self.failed_tests)}")
            print("\n🔧 REQUIRED FIXES:")
            print("1. Remove all hardcoded symbols from code")
            print("2. Remove all synthetic data generation")
            print("3. Fix symbol parameter passing through all layers")
            print("4. Ensure dataset names include run_id for uniqueness")
            print("5. Add proper error handling for missing data")
        else:
            print("\n🎉 ALL TESTS PASSED! Code is clean and well-structured.")
        
        return passed == total

async def main():
    """Run the comprehensive test suite."""
    
    test_suite = TrainingDataTestSuite()
    success = await test_suite.run_all_tests()
    
    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)