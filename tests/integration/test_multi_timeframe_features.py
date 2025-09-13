#!/usr/bin/env python3
"""
Integration tests for multi-timeframe feature issues identified in training EDA

Tests cover:
1. Missing multi-timeframe features (5m, 15m, 1h, 1d, 1w)
2. Missing technical indicators (pldot, z1b, z2b, z5t, z6t)
3. Envelope scaling issues (should be actual price levels, not codes)
4. DateTime metadata inclusion
5. Parquet vs numpy file handling
"""

import pytest
import requests
import pandas as pd
from datetime import datetime

class TestMultiTimeframeFeatures:
    """Test multi-timeframe feature generation and display"""

    BASE_URL = "http://localhost:3000"

    @pytest.fixture(scope="class")
    def training_datasets(self):
        """Get list of available training datasets"""
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        assert response.status_code == 200, f"Failed to get datasets: {response.text}"
        data = response.json()
        return data.get('datasets', [])

    @pytest.fixture(scope="class")
    def hourly_datasets(self, training_datasets):
        """Get hourly datasets specifically"""
        return [d for d in training_datasets if 'hourly' in d['dataset_name'].lower()]

    @pytest.fixture(scope="class")
    def sample_dataset_data(self, hourly_datasets):
        """Get sample data from hourly dataset"""
        if not hourly_datasets:
            pytest.skip("No hourly datasets available for testing")

        dataset_id = hourly_datasets[0]['id']
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/data?page=1&limit=5")
        assert response.status_code == 200, f"Failed to get dataset data: {response.text}"
        return response.json()

    def test_datasets_available(self, training_datasets):
        """Test that training datasets are available"""
        assert len(training_datasets) > 0, "No training datasets available"

        # Check for hourly datasets specifically
        hourly_count = len([d for d in training_datasets if 'hourly' in d['dataset_name'].lower()])
        assert hourly_count > 0, "No hourly training datasets available"

        print(f"✅ Found {len(training_datasets)} total datasets, {hourly_count} hourly datasets")

    def test_datetime_metadata_included(self, sample_dataset_data):
        """Test that datetime metadata is included in training data"""
        data_samples = sample_dataset_data.get('data', [])
        assert len(data_samples) > 0, "No data samples available"

        for i, sample in enumerate(data_samples):
            assert 'datetime' in sample, f"Sample {i} missing datetime field"

            # Validate datetime format
            datetime_str = sample['datetime']
            assert isinstance(datetime_str, str), f"Sample {i} datetime should be string"

            # Should be parseable as ISO datetime
            try:
                parsed_dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                assert parsed_dt.year >= 2020, f"Sample {i} datetime seems too old: {datetime_str}"
            except ValueError as e:
                pytest.fail(f"Sample {i} has invalid datetime format '{datetime_str}': {e}")

        print(f"✅ All {len(data_samples)} samples include valid datetime metadata")

    def test_envelope_scaling_fixed(self, sample_dataset_data):
        """Test that envelope indicators are not scaled/converted incorrectly"""
        data_samples = sample_dataset_data.get('data', [])
        assert len(data_samples) > 0, "No data samples available"

        for i, sample in enumerate(data_samples):
            close_price = sample.get('hour_close') or sample.get('close')
            assert close_price is not None, f"Sample {i} missing close price"
            assert close_price > 10, f"Sample {i} close price seems too low: {close_price}"

            # Check if envelope indicators exist and are reasonable
            if 'envelope_top' in sample:
                envelope_top = sample['envelope_top']
                # Should be near close price, not a small categorical code
                if envelope_top < 10:
                    pytest.fail(f"Sample {i} envelope_top={envelope_top} appears to be categorical code, should be price level near close={close_price}")

                # Should be above close price
                assert envelope_top >= close_price * 0.95, f"Sample {i} envelope_top={envelope_top} should be near/above close={close_price}"

            if 'envelope_bot' in sample:
                envelope_bot = sample['envelope_bot']
                if envelope_bot < 10 and envelope_bot != 0:  # 0 might be valid
                    pytest.fail(f"Sample {i} envelope_bot={envelope_bot} appears to be categorical code, should be price level near close={close_price}")

        print(f"✅ Envelope scaling issue resolved - no categorical codes found")

    def test_proper_parquet_feature_names(self, sample_dataset_data):
        """Test that parquet files show actual column names (not hardcoded names)"""
        data_samples = sample_dataset_data.get('data', [])
        assert len(data_samples) > 0, "No data samples available"

        sample = data_samples[0]
        feature_keys = [k for k in sample.keys() if k not in ['sequence_id', 'datetime']]

        # Should have actual parquet column names for hourly data
        expected_hourly_features = ['hour_open', 'hour_high', 'hour_low', 'hour_close', 'hour_volume']

        for feature in expected_hourly_features:
            if feature not in sample:
                # Check if it's the alternative naming
                alt_feature = feature.replace('hour_', '')
                assert alt_feature in sample, f"Missing expected feature: {feature} or {alt_feature}"

        print(f"✅ Proper feature names extracted from parquet: {feature_keys}")

    def test_categorical_conversion_selective(self, sample_dataset_data):
        """Test that only market context fields are converted to categorical codes"""
        data_samples = sample_dataset_data.get('data', [])
        assert len(data_samples) > 0, "No data samples available"

        sample = data_samples[0]

        # Market period should be converted to categorical code
        if 'market_period' in sample:
            market_period = sample['market_period']
            assert isinstance(market_period, (int, float)), "market_period should be converted to numeric code"
            assert market_period >= 0, "market_period code should be non-negative"

        # Price fields should remain as actual values
        price_fields = ['hour_open', 'hour_high', 'hour_low', 'hour_close', 'open', 'high', 'low', 'close']
        for field in price_fields:
            if field in sample:
                price_value = sample[field]
                assert price_value > 10, f"{field}={price_value} should be actual price, not categorical code"

        print(f"✅ Selective categorical conversion working properly")

    def test_missing_multi_timeframe_features(self, sample_dataset_data):
        """Test for presence of multi-timeframe features (5m, 15m, 1h, 1d, 1w)"""
        data_samples = sample_dataset_data.get('data', [])
        assert len(data_samples) > 0, "No data samples available"

        sample = data_samples[0]
        feature_keys = list(sample.keys())

        # Expected multi-timeframe patterns
        timeframe_patterns = ['5m_', '15m_', '1h_', '1d_', '1w_']

        found_timeframes = []
        for pattern in timeframe_patterns:
            matching_features = [k for k in feature_keys if k.startswith(pattern)]
            if matching_features:
                found_timeframes.append(pattern)

        # Current expectation: should find multi-timeframe features
        if not found_timeframes:
            pytest.fail(
                f"❌ ISSUE DETECTED: Missing multi-timeframe features!\n"
                f"   Expected patterns: {timeframe_patterns}\n"
                f"   Available features: {feature_keys[:10]}...\n"
                f"   This indicates training data generation is not using multi-timeframe configuration.\n"
                f"   Should have features like: 5m_open_lag_0, 15m_close_lag_1, etc."
            )

        print(f"✅ Found multi-timeframe features: {found_timeframes}")

    def test_missing_technical_indicators(self, sample_dataset_data):
        """Test for presence of required technical indicators"""
        data_samples = sample_dataset_data.get('data', [])
        assert len(data_samples) > 0, "No data samples available"

        sample = data_samples[0]
        feature_keys = list(sample.keys())

        # Expected technical indicators
        expected_indicators = ['pldot', 'z1b', 'z2b', 'z5t', 'z6t', 'envelope_top', 'envelope_bot']

        found_indicators = []
        missing_indicators = []

        for indicator in expected_indicators:
            # Check direct match or in multi-timeframe format
            direct_match = indicator in feature_keys
            timeframe_match = any(k for k in feature_keys if indicator in k)

            if direct_match or timeframe_match:
                found_indicators.append(indicator)
            else:
                missing_indicators.append(indicator)

        if missing_indicators:
            pytest.fail(
                f"❌ ISSUE DETECTED: Missing technical indicators!\n"
                f"   Missing: {missing_indicators}\n"
                f"   Found: {found_indicators}\n"
                f"   Available features: {feature_keys[:10]}...\n"
                f"   This indicates training data generation is not including all configured indicators."
            )

        print(f"✅ Found technical indicators: {found_indicators}")

    def test_feature_count_expectation(self, sample_dataset_data, hourly_datasets):
        """Test that feature count matches multi-timeframe expectations"""
        data_samples = sample_dataset_data.get('data', [])
        assert len(data_samples) > 0, "No data samples available"

        sample = data_samples[0]
        actual_feature_count = len([k for k in sample.keys() if k not in ['sequence_id', 'datetime']])

        # From training_data.gin: Expected ~1036 multi-timeframe features
        # 5m(364) + 15m(364) + 1h(168) + 1d(140) = 1036 features per training row
        expected_min_features = 100  # Conservative minimum
        expected_full_features = 1000  # Full multi-timeframe expectation

        if actual_feature_count < expected_min_features:
            pytest.fail(
                f"❌ ISSUE DETECTED: Too few features!\n"
                f"   Actual: {actual_feature_count} features\n"
                f"   Expected minimum: {expected_min_features}\n"
                f"   Expected full multi-timeframe: {expected_full_features}\n"
                f"   This suggests training data is not using multi-timeframe generation."
            )
        elif actual_feature_count < expected_full_features:
            print(f"⚠️  WARNING: Partial feature set detected")
            print(f"   Actual: {actual_feature_count} features")
            print(f"   Expected full multi-timeframe: {expected_full_features}")
            print(f"   Training data may not be using complete multi-timeframe configuration")
        else:
            print(f"✅ Full feature set detected: {actual_feature_count} features")

    def test_parquet_vs_numpy_handling(self, training_datasets):
        """Test that both parquet and numpy dataset formats work correctly"""
        parquet_datasets = []
        numpy_datasets = []

        for dataset in training_datasets:
            features_path = dataset.get('features_file_path', '')
            if features_path.endswith('.parquet'):
                parquet_datasets.append(dataset)
            elif features_path.endswith('.npy'):
                numpy_datasets.append(dataset)

        print(f"Found {len(parquet_datasets)} parquet datasets, {len(numpy_datasets)} numpy datasets")

        # Test parquet dataset if available
        if parquet_datasets:
            dataset_id = parquet_datasets[0]['id']
            response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/data?page=1&limit=2")
            assert response.status_code == 200, f"Parquet dataset {dataset_id} failed: {response.text}"

            data = response.json()
            assert len(data.get('data', [])) > 0, "Parquet dataset returned no data"
            print(f"✅ Parquet dataset {dataset_id} works correctly")

        # Test numpy dataset if available
        if numpy_datasets:
            dataset_id = numpy_datasets[0]['id']
            response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/data?page=1&limit=2")
            assert response.status_code == 200, f"Numpy dataset {dataset_id} failed: {response.text}"

            data = response.json()
            assert len(data.get('data', [])) > 0, "Numpy dataset returned no data"
            print(f"✅ Numpy dataset {dataset_id} works correctly")

    def test_data_quality_metrics(self, sample_dataset_data):
        """Test data quality and completeness"""
        data_samples = sample_dataset_data.get('data', [])
        assert len(data_samples) > 0, "No data samples available"

        # Check for missing/null values
        for i, sample in enumerate(data_samples):
            for key, value in sample.items():
                if key == 'datetime':
                    continue  # Skip datetime validation here

                assert value is not None, f"Sample {i} has null value for {key}"

                if isinstance(value, (int, float)):
                    assert not (pd.isna(value) or pd.isnull(value)), f"Sample {i} has NaN value for {key}"

        # Check for reasonable price ranges
        price_fields = ['hour_open', 'hour_high', 'hour_low', 'hour_close', 'open', 'high', 'low', 'close']
        for sample in data_samples:
            for field in price_fields:
                if field in sample:
                    price = sample[field]
                    assert 1 < price < 10000, f"Price {field}={price} seems unreasonable"

        print(f"✅ Data quality checks passed for {len(data_samples)} samples")


class TestTrainingDataGeneration:
    """Test training data generation configuration and setup"""

    def test_gin_configuration_available(self):
        """Test that Gin configuration files exist and are parseable"""
        from pathlib import Path

        config_dir = Path("/home/jianjun/ats-genai-data/config")

        # Check for key configuration files
        required_configs = [
            "hourly_training.gin",
            "training_data.gin",
            "app_dev.gin"
        ]

        for config_file in required_configs:
            config_path = config_dir / config_file
            assert config_path.exists(), f"Missing configuration file: {config_path}"

            # Check file is not empty
            content = config_path.read_text()
            assert len(content.strip()) > 0, f"Configuration file is empty: {config_path}"

            print(f"✅ Configuration file exists: {config_file}")

    def test_minute_data_availability(self):
        """Test that minute data is available for training data generation"""
        import os

        minute_data_path = "/mnt/d/ats-data/minute-bars/AAPL"

        assert os.path.exists(minute_data_path), f"Minute data directory not found: {minute_data_path}"

        # Check for recent data directories
        years = [d for d in os.listdir(minute_data_path) if d.isdigit()]
        assert len(years) > 0, "No year directories found in minute data"

        # Check for 2024 data specifically
        if "2024" in years:
            year_2024_path = os.path.join(minute_data_path, "2024")
            months = [d for d in os.listdir(year_2024_path) if d.isdigit()]
            assert len(months) > 0, "No month directories found in 2024 minute data"
            print(f"✅ Found minute data for AAPL: {len(years)} years, 2024 has {len(months)} months")
        else:
            print(f"⚠️  No 2024 minute data found, available years: {years}")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v", "--tb=short"])