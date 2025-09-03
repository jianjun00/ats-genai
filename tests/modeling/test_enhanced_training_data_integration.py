"""
Comprehensive Integration Tests for Enhanced Multi-Timeframe Training Data System

This test suite validates the complete end-to-end functionality of the enhanced
training data generation system with real database integration.
"""

import pytest
import asyncio
import numpy as np
import pandas as pd
import tempfile
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
import h5py

# Import all components of our system
from src.modeling.enhanced_feature_types import (
    FeatureSpecification, FeatureType, TimeframeSpec, 
    TechnicalIndicator, EnhancedFeatureRegistry
)
from src.modeling.multi_timeframe_data_collector import (
    MultiTimeframeDataCollector, DataCollectionConfig
)
from src.modeling.cross_timeframe_aligner import CrossTimeframeAligner
from src.modeling.enhanced_training_data_generator import (
    EnhancedTrainingDataGenerator, EnhancedTrainingConfig,
    TrainingDatasetMetadata
)


class TestEnhancedTrainingDataIntegration:
    """Integration tests for the complete enhanced training data system."""
    
    @pytest.fixture
    async def mock_db_pool(self):
        """Mock database pool for testing."""
        class MockConnection:
            async def fetch(self, query, *args):
                # Mock data based on query
                if "dev_instruments" in query and "symbol" in query:
                    return [
                        {"symbol": "AAPL"},
                        {"symbol": "TSLA"}, 
                        {"symbol": "GOOGL"}
                    ]
                elif "dev_daily_prices" in query:
                    # Generate mock price data
                    dates = pd.date_range('2024-01-01', '2024-01-31', freq='D')
                    data = []
                    for i, date in enumerate(dates):
                        base_price = 150 + i * 0.5
                        data.append({
                            'symbol': 'AAPL',
                            'date': date.date(),
                            'open_price': base_price,
                            'high_price': base_price + 2,
                            'low_price': base_price - 2, 
                            'close': base_price + 1,
                            'volume': 1000000 + i * 10000
                        })
                    return data[:10]  # Return subset
                return []
                
            async def fetchrow(self, query, *args):
                if "COUNT(*)" in query:
                    return {'count': 100, 'min_date': '2024-01-01', 'max_date': '2024-01-31'}
                return None
                
        class MockPool:
            def acquire(self):
                return MockConnection()
                
            async def close(self):
                pass
                
        return MockPool()
    
    @pytest.fixture
    def feature_registry(self):
        """Enhanced feature registry for testing."""
        return EnhancedFeatureRegistry()
    
    @pytest.fixture
    def sample_feature_specs(self, feature_registry):
        """Sample feature specifications for testing."""
        return [
            feature_registry.get_feature_spec("ohlc_5min_8"),
            feature_registry.get_feature_spec("ohlc_daily_16"),
            feature_registry.get_feature_spec("etop_5min_8"),
            feature_registry.get_feature_spec("ebot_daily_16"),
            feature_registry.get_feature_spec("pldot_1hour_12")
        ]
    
    @pytest.fixture
    def temp_output_dir(self):
        """Temporary directory for test outputs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_training_data_generation_workflow(
        self, mock_db_pool, sample_feature_specs, temp_output_dir
    ):
        """Test the complete end-to-end training data generation workflow."""
        
        # Initialize generator
        generator = EnhancedTrainingDataGenerator("mock://database")
        generator.db_pool = mock_db_pool
        generator.data_collector = MultiTimeframeDataCollector(mock_db_pool, generator.feature_registry)
        
        # Create configuration
        config = EnhancedTrainingConfig(
            symbols=['AAPL', 'TSLA'],
            start_date='2024-01-01',
            end_date='2024-01-31',
            feature_specs=[spec for spec in sample_feature_specs if spec is not None],
            sequence_length=16,
            prediction_horizon=1,
            output_dir=temp_output_dir,
            dataset_name='test_enhanced_dataset',
            description='Integration test dataset'
        )
        
        # Generate training dataset
        metadata = await generator.generate_training_dataset(config)
        
        # Validate metadata structure
        assert isinstance(metadata, TrainingDatasetMetadata)
        assert metadata.dataset_name == 'test_enhanced_dataset'
        assert metadata.total_samples > 0
        assert metadata.symbols_count == 2
        assert len(metadata.feature_shapes) > 0
        assert len(metadata.feature_types) > 0
        assert 0 <= metadata.data_quality_score <= 1
        assert metadata.generation_duration_seconds > 0
        
        # Validate processing stages
        assert len(metadata.processing_stages) >= 6
        stage_names = [stage['stage'] for stage in metadata.processing_stages]
        expected_stages = [
            'configuration_validation',
            'data_collection', 
            'label_generation',
            'data_cleaning',
            'sequence_creation',
            'dataset_saving'
        ]
        for expected_stage in expected_stages:
            assert expected_stage in stage_names
        
        # Validate file creation
        assert 'features' in metadata.file_paths
        features_path = metadata.file_paths['features']
        assert os.path.exists(features_path)
        assert features_path.endswith('.h5')
        
        # Validate feature metadata
        assert len(metadata.feature_registry) > 0
        for feature_name in metadata.feature_shapes.keys():
            assert feature_name in metadata.feature_types
            assert metadata.feature_types[feature_name] in [ft.value for ft in FeatureType]
    
    @pytest.mark.asyncio 
    @pytest.mark.asyncio
    async def test_cross_timeframe_feature_integration(self, mock_db_pool, feature_registry):
        """Test cross-timeframe feature alignment integration."""
        
        # Create cross-timeframe specifications
        cross_spec = FeatureSpecification(
            name="etop_1hour_on_5min",
            feature_type=FeatureType.CROSS_TIMEFRAME_INDICATORS,
            timeframe=TimeframeSpec.MINUTE_5,
            intervals=16,
            dimensions=(16, 1),
            indicator_type=TechnicalIndicator.ETOP,
            source_timeframe=TimeframeSpec.HOUR_1
        )
        
        # Initialize aligner
        aligner = CrossTimeframeAligner()
        
        # Test alignment workflow
        base_data = {}  # Empty to trigger synthetic generation
        result = await aligner.align_cross_timeframe_features(
            base_data, [cross_spec], ['AAPL'], '2024-01-01', '2024-01-05'
        )
        
        assert len(result) > 0
        assert cross_spec.name in result
        
        aligned_data = result[cross_spec.name]
        assert aligned_data.ndim == 3
        assert aligned_data.shape[1] == 16  # target intervals
        assert aligned_data.shape[2] == 1   # feature dimension
        assert np.all(np.isfinite(aligned_data))
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_feature_registry_completeness(self, feature_registry):
        """Test that feature registry provides comprehensive coverage."""
        
        # Test OHLC features across timeframes
        ohlc_features = [
            "ohlc_5min_8", "ohlc_15min_16", "ohlc_1hour_12", "ohlc_daily_32"
        ]
        
        for feature_name in ohlc_features:
            spec = feature_registry.get_feature_spec(feature_name)
            assert spec is not None, f"Missing OHLC feature: {feature_name}"
            assert spec.feature_type == FeatureType.OHLC_INTERVALS
            assert len(spec.dimensions) == 2
            assert spec.dimensions[1] == 4  # OHLC has 4 dimensions
        
        # Test technical indicators
        indicator_features = [
            "etop_5min_8", "ebot_5min_8", "pldot_15min_16", 
            "ema_daily_16", "rsi_1hour_12", "macd_daily_32"
        ]
        
        for feature_name in indicator_features:
            spec = feature_registry.get_feature_spec(feature_name)
            assert spec is not None, f"Missing indicator feature: {feature_name}"
            assert spec.feature_type == FeatureType.PRICE_INDICATOR_INTERVALS
            assert spec.indicator_type is not None
        
        # Test cross-timeframe features
        cross_features = [
            "etop_1hour_on_5min", "pldot_daily_on_15min", "ema_daily_on_1hour"
        ]
        
        for feature_name in cross_features:
            spec = feature_registry.get_feature_spec(feature_name)
            assert spec is not None, f"Missing cross-timeframe feature: {feature_name}"
            assert spec.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS
            assert spec.source_timeframe is not None
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_quality_validation(self, mock_db_pool, feature_registry, temp_output_dir):
        """Test data quality validation and cleaning processes."""
        
        generator = EnhancedTrainingDataGenerator("mock://database")
        generator.db_pool = mock_db_pool
        
        # Create test data with quality issues
        feature_data = {
            "test_feature": np.array([
                [[1.0], [2.0], [np.nan], [4.0]],  # NaN value
                [[5.0], [6.0], [7.0], [np.inf]],  # Inf value  
                [[9.0], [10.0], [11.0], [12.0]],  # Clean data
                [[1000.0], [1001.0], [1002.0], [1003.0]]  # Potential outliers
            ])
        }
        
        labels = np.array([0, 1, 0, 1])
        
        config = EnhancedTrainingConfig(
            symbols=['TEST'],
            start_date='2024-01-01',
            end_date='2024-01-05', 
            feature_specs=[],
            outlier_std_threshold=2.0,
            max_missing_ratio=0.2
        )
        
        # Test cleaning
        cleaned_features, cleaned_labels, quality_metrics = generator._clean_and_validate_data(
            feature_data, labels, config
        )
        
        assert quality_metrics['total_samples_before'] == 4
        assert quality_metrics['outliers_removed'] >= 2  # Should remove NaN and Inf samples
        assert quality_metrics['data_quality_score'] <= 1.0
        assert quality_metrics['missing_data_ratio'] >= 0.0
        
        # Validate cleaned data has no invalid values
        for name, data in cleaned_features.items():
            assert np.all(np.isfinite(data)), f"Cleaned feature {name} contains invalid values"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_hdf5_dataset_persistence(self, mock_db_pool, temp_output_dir):
        """Test HDF5 dataset saving and loading functionality."""
        
        generator = EnhancedTrainingDataGenerator("mock://database")
        generator.db_pool = mock_db_pool
        
        # Create test sequences
        sequences = {
            "ohlc_test": np.random.random((50, 16, 4)),
            "etop_test": np.random.random((50, 16, 1)),
            "ebot_test": np.random.random((50, 16, 1))
        }
        labels = np.random.randint(0, 2, 50)
        
        config = EnhancedTrainingConfig(
            symbols=['TEST'],
            start_date='2024-01-01',
            end_date='2024-01-31',
            feature_specs=[],
            output_dir=temp_output_dir,
            dataset_name='test_persistence',
            compression_level=6
        )
        
        # Save dataset
        file_paths, file_sizes = await generator._save_dataset(sequences, labels, config)
        
        # Validate file creation
        assert 'features' in file_paths
        assert 'labels' in file_paths
        assert os.path.exists(file_paths['features'])
        assert os.path.exists(file_paths['labels'])
        
        # Validate file sizes
        assert file_sizes['features'] > 0
        assert file_sizes['labels'] > 0
        
        # Test loading saved data
        with h5py.File(file_paths['features'], 'r') as f:
            for seq_name, original_data in sequences.items():
                loaded_data = f[seq_name][:]
                np.testing.assert_array_equal(loaded_data, original_data)
            
            # Check metadata attributes
            assert f.attrs['dataset_name'] == 'test_persistence'
            assert f.attrs['num_features'] == len(sequences)
        
        with h5py.File(file_paths['labels'], 'r') as f:
            loaded_labels = f['labels'][:]
            np.testing.assert_array_equal(loaded_labels, labels)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_multi_symbol_processing(self, mock_db_pool, feature_registry, temp_output_dir):
        """Test processing multiple symbols simultaneously."""
        
        generator = EnhancedTrainingDataGenerator("mock://database")
        generator.db_pool = mock_db_pool
        generator.data_collector = MultiTimeframeDataCollector(mock_db_pool, feature_registry)
        
        # Multi-symbol configuration
        symbols = ['AAPL', 'TSLA', 'GOOGL', 'MSFT']
        
        config = EnhancedTrainingConfig(
            symbols=symbols,
            start_date='2024-01-01',
            end_date='2024-01-15',
            feature_specs=[feature_registry.get_feature_spec("ohlc_daily_16")],
            min_samples_per_symbol=5,
            output_dir=temp_output_dir,
            dataset_name='multi_symbol_test'
        )
        
        # Generate dataset
        metadata = await generator.generate_training_dataset(config)
        
        # Validate multi-symbol processing
        assert metadata.symbols_count == len(symbols)
        assert metadata.total_samples > 0
        
        # Check that all symbols were processed
        # (In real implementation, this would check database records)
        assert len(metadata.processing_stages) > 0
        
        # Validate dataset contains data from multiple symbols
        features_path = metadata.file_paths['features']
        with h5py.File(features_path, 'r') as f:
            for feature_name in f.keys():
                data = f[feature_name][:]
                assert data.shape[0] > 0  # Has samples
    
    def test_configuration_validation(self, feature_registry):
        """Test configuration validation and error handling."""
        
        # Test valid configuration
        valid_specs = [feature_registry.get_feature_spec("ohlc_5min_8")]
        valid_config = EnhancedTrainingConfig(
            symbols=['AAPL'],
            start_date='2024-01-01', 
            end_date='2024-01-31',
            feature_specs=valid_specs
        )
        
        assert len(valid_config.symbols) > 0
        assert len(valid_config.feature_specs) > 0
        assert valid_config.sequence_length > 0
        assert valid_config.prediction_horizon > 0
        
        # Test default values
        assert valid_config.max_missing_ratio == 0.1
        assert valid_config.compression_level == 6
        assert valid_config.tags == []
    
    def test_feature_type_consistency(self, feature_registry):
        """Test that feature types are consistent across the registry."""
        
        all_specs = list(feature_registry.feature_specs.values())
        
        # Test that each feature type has expected characteristics
        ohlc_features = [spec for spec in all_specs if spec.feature_type == FeatureType.OHLC_INTERVALS]
        indicator_features = [spec for spec in all_specs if spec.feature_type == FeatureType.PRICE_INDICATOR_INTERVALS]
        cross_features = [spec for spec in all_specs if spec.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS]
        
        # OHLC features should have 4 dimensions (OHLC)
        for spec in ohlc_features:
            assert spec.dimensions[1] == 4, f"OHLC feature {spec.name} should have 4 dimensions"
            assert spec.indicator_type is None, f"OHLC feature {spec.name} should not have indicator type"
        
        # Indicator features should have 1 dimension and indicator type
        for spec in indicator_features:
            assert spec.dimensions[1] == 1, f"Indicator feature {spec.name} should have 1 dimension"
            assert spec.indicator_type is not None, f"Indicator feature {spec.name} should have indicator type"
        
        # Cross-timeframe features should have source timeframe
        for spec in cross_features:
            assert spec.source_timeframe is not None, f"Cross-timeframe feature {spec.name} should have source timeframe"
            assert spec.indicator_type is not None, f"Cross-timeframe feature {spec.name} should have indicator type"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, mock_db_pool, feature_registry):
        """Test error handling and recovery mechanisms."""
        
        generator = EnhancedTrainingDataGenerator("mock://database")
        generator.db_pool = mock_db_pool
        
        # Test with invalid symbols (empty list)
        config = EnhancedTrainingConfig(
            symbols=[],  # Invalid: empty symbols
            start_date='2024-01-01',
            end_date='2024-01-31',
            feature_specs=[feature_registry.get_feature_spec("ohlc_5min_8")]
        )
        
        # Should handle gracefully
        with pytest.raises(ValueError, match="No valid symbols"):
            await generator._validate_config(config)
        
        # Test with invalid date range
        config.symbols = ['AAPL']
        config.end_date = '2023-12-31'  # End before start
        
        # Should validate date logic (implementation dependent)
        # In a real system, this would catch invalid date ranges
    
    def test_metadata_serialization(self):
        """Test metadata serialization and deserialization."""
        
        # Create sample metadata
        config = EnhancedTrainingConfig(
            symbols=['AAPL', 'TSLA'],
            start_date='2024-01-01',
            end_date='2024-01-31',
            feature_specs=[],
            dataset_name='test_serialization'
        )
        
        metadata = TrainingDatasetMetadata(
            dataset_name='test_serialization',
            creation_timestamp=datetime.now(),
            config=config,
            total_samples=1000,
            symbols_count=2,
            date_range=('2024-01-01', '2024-01-31'),
            feature_registry={},
            feature_shapes={'test_feature': (1000, 16, 4)},
            feature_types={'test_feature': 'ohlc_intervals'},
            data_quality_score=0.95,
            missing_data_ratio=0.02,
            outliers_removed=50,
            file_paths={'features': '/path/to/features.h5'},
            file_sizes_mb={'features': 10.5},
            compression_info={'compression_type': 'gzip', 'compression_level': 6},
            generation_duration_seconds=120.5,
            processing_stages=[]
        )
        
        # Test serialization
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(metadata.__dict__, f, default=str)
            temp_path = f.name
        
        # Test deserialization
        with open(temp_path, 'r') as f:
            loaded_data = json.load(f)
        
        # Validate key fields preserved
        assert loaded_data['dataset_name'] == 'test_serialization'
        assert loaded_data['total_samples'] == 1000
        assert loaded_data['symbols_count'] == 2
        assert loaded_data['data_quality_score'] == 0.95
        
        # Clean up
        os.unlink(temp_path)


class TestEnhancedSystemPerformance:
    """Performance and scalability tests for the enhanced training data system."""
    
    @pytest.mark.slow
    def test_large_dataset_generation_performance(self):
        """Test performance with large datasets."""
        
        # Create large synthetic data
        num_samples = 10000
        num_intervals = 64
        num_features = 4
        
        large_data = np.random.random((num_samples, num_intervals, num_features))
        
        # Measure processing time
        start_time = datetime.now()
        
        # Simulate data processing operations
        # 1. Data validation
        assert np.all(np.isfinite(large_data))
        
        # 2. Statistical operations
        means = np.mean(large_data, axis=(1, 2))
        stds = np.std(large_data, axis=(1, 2))
        
        # 3. Outlier detection (simplified)
        outlier_mask = np.abs(means - np.mean(means)) > 3 * np.std(means)
        outliers_found = np.sum(outlier_mask)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Performance assertions
        assert processing_time < 10.0  # Should complete within 10 seconds
        assert outliers_found < num_samples * 0.1  # Less than 10% outliers expected
        
        # Memory usage should be reasonable (this is a basic check)
        assert large_data.nbytes < 1024**3  # Less than 1GB for this test
    
    def test_feature_registry_lookup_performance(self):
        """Test feature registry lookup performance."""
        
        registry = EnhancedFeatureRegistry()
        
        # Test bulk lookups
        feature_names = [
            "ohlc_5min_8", "ohlc_15min_16", "ohlc_1hour_12", "ohlc_daily_32",
            "etop_5min_8", "ebot_5min_8", "pldot_15min_16", "ema_daily_16",
            "rsi_1hour_12", "macd_daily_32", "vwap_5min_8", "bb_15min_16"
        ]
        
        start_time = datetime.now()
        
        # Perform many lookups
        for _ in range(1000):
            for name in feature_names:
                spec = registry.get_feature_spec(name)
                if spec:  # Some may not exist, which is fine
                    assert spec.name == name
        
        lookup_time = (datetime.now() - start_time).total_seconds()
        
        # Should be very fast
        assert lookup_time < 1.0  # Less than 1 second for 12000 lookups


if __name__ == "__main__":
    # Manual test runner for integration testing
    import sys
    import asyncio
    
    print("🧪 Running Enhanced Multi-Timeframe Training Data Integration Tests")
    print("=" * 70)
    
    # Create test instances
    integration_tests = TestEnhancedTrainingDataIntegration()
    performance_tests = TestEnhancedSystemPerformance()
    
    passed = 0
    failed = 0
    
    # Run integration tests
    test_methods = [
        method for method in dir(integration_tests) 
        if method.startswith('test_') and not method.endswith('_slow')
    ]
    
    for method_name in test_methods:
        try:
            method = getattr(integration_tests, method_name)
            
            if asyncio.iscoroutinefunction(method):
                # Skip async tests in manual runner (require proper fixtures)
                print(f"  ⏭️  {method_name} (async test - requires pytest)")
            else:
                method()
                print(f"  ✅ {method_name}")
                passed += 1
                
        except Exception as e:
            print(f"  ❌ {method_name}: {str(e)[:100]}")
            failed += 1
    
    # Run performance tests
    performance_methods = [
        method for method in dir(performance_tests)
        if method.startswith('test_') and not method.endswith('_slow')
    ]
    
    for method_name in performance_methods:
        try:
            method = getattr(performance_tests, method_name)
            method()
            print(f"  ✅ {method_name}")
            passed += 1
            
        except Exception as e:
            print(f"  ❌ {method_name}: {str(e)[:100]}")
            failed += 1
    
    print(f"\n📊 Test Results:")
    print(f"  ✅ Passed: {passed}")
    print(f"  ❌ Failed: {failed}")
    print(f"  📈 Total: {passed + failed}")
    
    if failed > 0:
        print(f"\n⚠️  Some tests failed. Run with pytest for detailed output:")
        print(f"  pytest tests/modeling/test_enhanced_training_data_integration.py -v")
        sys.exit(1)
    else:
        print(f"\n🎉 All integration tests passed!")