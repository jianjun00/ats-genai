#!/usr/bin/env python3
"""
Performance Tests for Feature Metadata Operations
"""

import pytest
import numpy as np
import tempfile
import shutil
import time
import sys
import os
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ml.training_data.generators.training_data_metadata import (
    TrainingDataMetadataManager, FeatureType
)
from services.dataset_service import DatasetService


class TestFeatureMetadataPerformance:
    """Test performance of feature metadata operations."""
    
    @pytest.fixture
    def large_dataset(self):
        """Generate large dataset for performance testing."""
        np.random.seed(42)
        
        # Large dataset: 10,000 sequences, 200 timesteps, 50 features
        n_sequences = 10000
        sequence_length = 200
        n_features = 50
        n_labels = 5
        
        features_data = np.random.normal(0, 1, (n_sequences, sequence_length, n_features))
        labels_data = np.random.normal(0, 0.02, (n_sequences, n_labels))
        
        # Add some realistic structure
        for i in range(n_features):
            if i % 5 == 0:  # Every 5th feature has trending behavior
                trend = np.linspace(-0.1, 0.1, sequence_length)
                features_data[:, :, i] += trend[np.newaxis, :]
            
            if i % 7 == 0:  # Every 7th feature has outliers
                outlier_mask = np.random.random((n_sequences, sequence_length)) < 0.001
                features_data[:, :, i] = np.where(outlier_mask, 
                                                features_data[:, :, i] * 10, 
                                                features_data[:, :, i])
        
        feature_names = [f"feature_{i:03d}" for i in range(n_features)]
        label_names = [f"label_{i}" for i in range(n_labels)]
        
        return {
            'features_data': features_data,
            'labels_data': labels_data,
            'feature_names': feature_names,
            'label_names': label_names,
            'n_sequences': n_sequences,
            'sequence_length': sequence_length,
            'n_features': n_features
        }
    
    @pytest.fixture
    def temp_workspace(self):
        """Create temporary workspace."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    def test_large_metadata_generation_performance(self, large_dataset, temp_workspace):
        """Test performance of metadata generation for large datasets."""
        
        metadata_manager = TrainingDataMetadataManager(temp_workspace)
        
        # Create configurations for all features
        feature_configs = []
        for i, feature_name in enumerate(large_dataset['feature_names']):
            config = {'feature_index': i}
            
            # Add different configurations to simulate real scenarios
            if i % 10 == 0:
                config['indicator_type'] = 'sma'
                config['window_size'] = 20
            elif i % 15 == 0:
                config['indicator_type'] = 'ema'
                config['window_size'] = 14
            elif i % 20 == 0:
                config['indicator_type'] = 'rsi'
                config['window_size'] = 14
            
            feature_configs.append(config)
        
        label_configs = [{'label_type': 'return', 'lead_periods': i+1} 
                        for i in range(len(large_dataset['label_names']))]
        
        # Measure metadata generation time
        start_time = time.time()
        
        metadata = metadata_manager.create_training_metadata(
            dataset_name="performance_test_dataset",
            features_data=large_dataset['features_data'],
            labels_data=large_dataset['labels_data'],
            feature_names=large_dataset['feature_names'],
            label_names=large_dataset['label_names'],
            feature_configs=feature_configs,
            label_configs=label_configs,
            symbols=['PERF_TEST'],
            date_range={'start': '2020-01-01', 'end': '2024-12-31'}
        )
        
        generation_time = time.time() - start_time
        
        # Performance assertions
        assert generation_time < 10.0  # Should complete within 10 seconds
        assert len(metadata.features) == large_dataset['n_features']
        assert len(metadata.labels) == len(large_dataset['label_names'])
        assert metadata.total_sequences == large_dataset['n_sequences']
        assert metadata.sequence_length == large_dataset['sequence_length']
        
        print(f"✅ Large metadata generation completed in {generation_time:.2f}s")
        print(f"   Dataset size: {large_dataset['n_sequences']:,} sequences x {large_dataset['sequence_length']} timesteps x {large_dataset['n_features']} features")
        print(f"   Throughput: {(large_dataset['n_sequences'] * large_dataset['n_features']) / generation_time:,.0f} feature-sequences/second")
        
        # Test serialization performance
        start_time = time.time()
        metadata_file = metadata_manager.save_metadata(metadata, "large_metadata.json")
        serialization_time = time.time() - start_time
        
        assert serialization_time < 2.0  # Should serialize within 2 seconds
        
        print(f"✅ Large metadata serialization completed in {serialization_time:.2f}s")
        
        # Check file size
        file_size_mb = os.path.getsize(metadata_file) / (1024 * 1024)
        print(f"   Metadata file size: {file_size_mb:.2f} MB")
        
        return generation_time, serialization_time
    
    def test_feature_statistics_calculation_performance(self, temp_workspace):
        """Test performance of enhanced statistics calculation."""
        
        metadata_manager = TrainingDataMetadataManager(temp_workspace)
        
        # Test with various data sizes and characteristics
        test_cases = [
            {"size": 1000, "outlier_ratio": 0.0, "name": "small_clean"},
            {"size": 10000, "outlier_ratio": 0.0, "name": "medium_clean"},
            {"size": 100000, "outlier_ratio": 0.0, "name": "large_clean"},
            {"size": 100000, "outlier_ratio": 0.01, "name": "large_with_outliers"},
            {"size": 1000000, "outlier_ratio": 0.001, "name": "xlarge_sparse_outliers"}
        ]
        
        performance_results = []
        
        for case in test_cases:
            np.random.seed(42)
            
            # Generate test data
            data = np.random.normal(100, 15, case["size"])
            
            # Add outliers
            if case["outlier_ratio"] > 0:
                n_outliers = int(case["size"] * case["outlier_ratio"])
                outlier_indices = np.random.choice(case["size"], n_outliers, replace=False)
                data[outlier_indices] = np.random.choice([-500, 500], n_outliers)  # Extreme outliers
            
            # Measure statistics calculation time
            start_time = time.time()
            
            valid_data = data[~np.isnan(data)]  # Remove NaNs (none in this case)
            stats = metadata_manager._calculate_enhanced_statistics(valid_data)
            
            calculation_time = time.time() - start_time
            
            performance_results.append({
                'case': case["name"],
                'size': case["size"],
                'time': calculation_time,
                'throughput': case["size"] / calculation_time if calculation_time > 0 else float('inf'),
                'outliers_detected': stats['outlier_count']
            })
            
            # Performance assertions
            assert calculation_time < 1.0  # Should complete within 1 second
            assert stats['min_value'] is not None
            assert stats['max_value'] is not None
            assert stats['mean_value'] is not None
            assert stats['std_value'] is not None
            
            if case["outlier_ratio"] > 0:
                assert stats['outlier_count'] > 0  # Should detect outliers
        
        # Print performance summary
        print("\n📊 Statistics Calculation Performance:")
        print("-" * 70)
        print(f"{'Case':<20} {'Size':<10} {'Time (s)':<10} {'Throughput':<15} {'Outliers':<10}")
        print("-" * 70)
        
        for result in performance_results:
            print(f"{result['case']:<20} {result['size']:<10,} {result['time']:<10.4f} {result['throughput']:<15,.0f} {result['outliers_detected']:<10}")
        
        return performance_results
    
    def test_visualization_hints_generation_performance(self, temp_workspace):
        """Test performance of visualization hints generation."""
        
        metadata_manager = TrainingDataMetadataManager(temp_workspace)
        
        # Test different feature types and statistics combinations
        feature_types = [
            FeatureType.OHLC,
            FeatureType.PRICE_INDICATOR,
            FeatureType.VOLUME_INDICATOR,
            FeatureType.RETURN,
            FeatureType.BINARY,
            FeatureType.NORMALIZED
        ]
        
        statistics_variants = [
            {'min_value': 100.0, 'max_value': 200.0},     # Normal range
            {'min_value': 0.1, 'max_value': 1000.0},      # High ratio (log scale)
            {'min_value': -0.1, 'max_value': 0.1},        # Symmetric range
            {'min_value': 0.0, 'max_value': 1.0},         # Normalized range
        ]
        
        feature_names = [
            'close_price', 'sma_indicator', 'volume_data', 
            'return_1h', 'binary_signal', 'normalized_feature'
        ]
        
        # Measure bulk hint generation
        start_time = time.time()
        
        total_hints_generated = 0
        for feature_type in feature_types:
            for stats in statistics_variants:
                for name in feature_names:
                    hints = metadata_manager._generate_visualization_hints(name, feature_type, stats)
                    total_hints_generated += 1
                    
                    # Validate hint structure
                    assert 'color_scheme' in hints
                    assert 'scale_type' in hints
                    assert 'is_primary_indicator' in hints
        
        generation_time = time.time() - start_time
        
        # Performance assertions
        assert generation_time < 0.1  # Should be very fast
        throughput = total_hints_generated / generation_time if generation_time > 0 else float('inf')
        
        print(f"✅ Visualization hints generation performance:")
        print(f"   Generated: {total_hints_generated} hints in {generation_time:.4f}s")
        print(f"   Throughput: {throughput:,.0f} hints/second")
        
        return generation_time, throughput
    
    def test_technical_indicator_params_extraction_performance(self, temp_workspace):
        """Test performance of technical indicator parameter extraction."""
        
        metadata_manager = TrainingDataMetadataManager(temp_workspace)
        
        # Generate large number of diverse indicator names
        indicator_names = []
        
        # SMA/EMA variants
        for window in [5, 10, 14, 20, 50, 100, 200]:
            indicator_names.extend([f'SMA_{window}', f'EMA_{window}'])
        
        # RSI variants
        for period in [7, 14, 21]:
            indicator_names.append(f'RSI_{period}')
        
        # Bollinger bands
        for period, std_dev in [(20, 2), (20, 2.5), (50, 2)]:
            indicator_names.append(f'BBands_{period}_{std_dev}')
        
        # Envelope indicators
        for pct in [1.0, 2.5, 5.0]:
            indicator_names.extend([f'envelope_top_{pct}', f'envelope_bot_{pct}'])
        
        # BX Trender variants
        for variant in ['Basic', 'Directional', 'VolumeWeighted']:
            for period in [7, 14, 21]:
                indicator_names.append(f'BXTrender{variant}_{period}')
        
        # Custom indicators
        custom_indicators = [
            'MACD_12_26_9', 'Stoch_14_3_3', 'Williams_R_14',
            'CCI_20', 'ADX_14', 'PSAR_0.02_0.2', 'OBV',
            'custom_momentum_indicator', 'proprietary_signal_v2'
        ]
        indicator_names.extend(custom_indicators)
        
        print(f"Testing parameter extraction for {len(indicator_names)} indicators...")
        
        # Measure parameter extraction performance
        start_time = time.time()
        
        extracted_params = []
        for name in indicator_names:
            config = {'test_param': 'test_value'}
            params = metadata_manager._extract_technical_indicator_params(name, config)
            extracted_params.append(params)
        
        extraction_time = time.time() - start_time
        
        # Performance assertions
        assert extraction_time < 0.5  # Should be very fast
        assert len(extracted_params) == len(indicator_names)
        
        throughput = len(indicator_names) / extraction_time if extraction_time > 0 else float('inf')
        
        print(f"✅ Technical indicator parameter extraction performance:")
        print(f"   Processed: {len(indicator_names)} indicators in {extraction_time:.4f}s")
        print(f"   Throughput: {throughput:,.0f} indicators/second")
        
        # Validate some extracted parameters
        sma_params = next((p for i, p in enumerate(extracted_params) if 'SMA_20' in indicator_names[i]), {})
        assert sma_params.get('window_size') == 20
        
        bx_params = next((p for i, p in enumerate(extracted_params) if 'BXTrenderBasic_14' in indicator_names[i]), {})
        assert bx_params.get('indicator_type') == 'bx_trender'
        assert bx_params.get('variant') == 'basic'
        
        return extraction_time, throughput
    
    def test_dataset_service_api_performance(self):
        """Test performance of dataset service feature metadata API operations."""
        
        dataset_service = DatasetService({
            'host': 'localhost', 'port': 3432, 'database': 'test_db',
            'user': 'test_user', 'password': 'test_password'
        })
        
        # Create large feature metadata for testing
        large_feature_metadata = {
            "features": [
                {
                    "name": f"feature_{i:04d}",
                    "feature_type": "PRICE_INDICATOR" if i % 2 == 0 else "VOLUME_INDICATOR",
                    "data_type": "float64",
                    "shape": [100, 1],
                    "description": f"Feature {i}",
                    "statistics": {
                        "min_value": float(i),
                        "max_value": float(i + 100),
                        "mean_value": float(i + 50),
                        "std_value": float(i + 10),
                        "outlier_count": i % 10
                    },
                    "visualization_hints": {
                        "visualization_type": "LINE_CHART",
                        "color_scheme": "blue",
                        "is_primary_indicator": i < 10
                    },
                    "technical_indicator_params": {
                        "indicator_type": f"custom_indicator_{i}",
                        "window_size": 14 + (i % 20)
                    }
                }
                for i in range(1000)  # 1000 features
            ],
            "labels": [
                {
                    "name": f"label_{i}",
                    "label_type": "return",
                    "data_type": "float64",
                    "shape": [1],
                    "description": f"Label {i}",
                    "lead_periods": i + 1,
                    "statistics": {
                        "min_value": -0.1,
                        "max_value": 0.1,
                        "mean_value": 0.001,
                        "std_value": 0.02
                    }
                }
                for i in range(50)  # 50 labels
            ],
            "metadata_version": "1.0",
            "creation_timestamp": "2024-09-06T20:00:00Z",
            "total_features": 1000,
            "total_labels": 50,
            "data_quality_metrics": {
                "feature_completeness": 0.98,
                "label_completeness": 0.95,
                "overall_quality_score": 0.96
            }
        }
        
        mock_db_result = {
            'feature_metadata': large_feature_metadata,
            'dataset_name': 'large_test_dataset',
            'technical_indicators': ','.join([f'indicator_{i}' for i in range(100)])
        }
        
        performance_results = {}
        
        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            
            # Test get_feature_metadata performance
            mock_cursor.fetchone.return_value = mock_db_result
            
            start_time = time.time()
            result = dataset_service.get_feature_metadata(1)
            get_metadata_time = time.time() - start_time
            
            performance_results['get_feature_metadata'] = {
                'time': get_metadata_time,
                'features_processed': len(result['features']),
                'throughput': len(result['features']) / get_metadata_time if get_metadata_time > 0 else float('inf')
            }
            
            assert get_metadata_time < 1.0  # Should handle large metadata quickly
            assert len(result['features']) == 1000
            assert len(result['labels']) == 50
            
            # Test find_datasets_by_features performance
            mock_datasets_rows = [
                {
                    'id': i, 'dataset_name': f'dataset_{i}', 'symbols': f'SYMBOL_{i}',
                    'total_sequences': 1000 + i, 'feature_count': 50 + i, 'label_count': 5,
                    'data_quality_score': 0.9, 'feature_completeness': 0.95,
                    'label_completeness': 0.93, 'file_size_mb': 100.0,
                    'technical_indicators': f'indicator_{i}', 'sequence_length': 50,
                    'date_range_start': '2024-01-01', 'date_range_end': '2024-12-31',
                    'creation_timestamp': '2024-09-06T20:00:00Z', 'run_id': i
                }
                for i in range(100)  # 100 datasets
            ]
            
            mock_cursor.fetchall.return_value = mock_datasets_rows
            
            start_time = time.time()
            datasets = dataset_service.find_datasets_by_features(['feature_001', 'feature_002'])
            search_time = time.time() - start_time
            
            performance_results['find_datasets_by_features'] = {
                'time': search_time,
                'datasets_processed': len(datasets),
                'throughput': len(datasets) / search_time if search_time > 0 else float('inf')
            }
            
            assert search_time < 0.5  # Should search quickly
            assert len(datasets) == 100
            
            # Test basic metadata generation performance
            large_indicators = ','.join([f'indicator_{i}' for i in range(500)])
            
            start_time = time.time()
            basic_metadata = dataset_service._generate_basic_feature_metadata(1, large_indicators)
            generation_time = time.time() - start_time
            
            performance_results['generate_basic_metadata'] = {
                'time': generation_time,
                'features_generated': len(basic_metadata['features']),
                'throughput': len(basic_metadata['features']) / generation_time if generation_time > 0 else float('inf')
            }
            
            assert generation_time < 0.1  # Should be very fast
            assert len(basic_metadata['features']) == 505  # 5 OHLCV + 500 indicators
        
        # Print performance summary
        print("\n🚀 Dataset Service API Performance:")
        print("-" * 60)
        print(f"{'Operation':<30} {'Time (s)':<10} {'Items':<10} {'Throughput':<15}")
        print("-" * 60)
        
        for operation, metrics in performance_results.items():
            items_key = 'features_processed' if 'features' in operation else 'datasets_processed' if 'datasets' in operation else 'features_generated'
            print(f"{operation:<30} {metrics['time']:<10.4f} {metrics[items_key]:<10} {metrics['throughput']:<15,.0f}")
        
        return performance_results
    
    def test_concurrent_metadata_operations_performance(self, temp_workspace):
        """Test performance of concurrent metadata operations."""
        
        metadata_manager = TrainingDataMetadataManager(temp_workspace)
        
        # Create multiple datasets for concurrent processing
        def create_dataset_metadata(dataset_id):
            np.random.seed(dataset_id)
            
            n_features = 20 + (dataset_id % 30)  # Variable feature count
            features_data = np.random.normal(0, 1, (100, 50, n_features))
            labels_data = np.random.normal(0, 0.02, (100, 2))
            
            feature_names = [f"dataset_{dataset_id}_feature_{i}" for i in range(n_features)]
            label_names = [f"dataset_{dataset_id}_label_{i}" for i in range(2)]
            
            return metadata_manager.create_training_metadata(
                dataset_name=f"concurrent_test_{dataset_id}",
                features_data=features_data,
                labels_data=labels_data,
                feature_names=feature_names,
                label_names=label_names,
                feature_configs=[{} for _ in range(n_features)],
                label_configs=[{} for _ in range(2)],
                symbols=[f'SYM_{dataset_id}'],
                date_range={'start': '2024-01-01', 'end': '2024-12-31'}
            )
        
        # Test sequential processing
        start_time = time.time()
        sequential_results = []
        for i in range(10):
            metadata = create_dataset_metadata(i)
            sequential_results.append(metadata)
        sequential_time = time.time() - start_time
        
        # Test concurrent processing
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=4) as executor:
            concurrent_results = list(executor.map(create_dataset_metadata, range(10)))
        concurrent_time = time.time() - start_time
        
        # Performance assertions
        assert len(sequential_results) == 10
        assert len(concurrent_results) == 10
        assert concurrent_time < sequential_time  # Concurrent should be faster
        
        speedup = sequential_time / concurrent_time
        
        print(f"\n⚡ Concurrent Processing Performance:")
        print(f"   Sequential time: {sequential_time:.2f}s")
        print(f"   Concurrent time: {concurrent_time:.2f}s")
        print(f"   Speedup: {speedup:.2f}x")
        print(f"   Datasets processed: {len(concurrent_results)}")
        
        # Validate results are identical
        for seq_result, conc_result in zip(sequential_results, concurrent_results):
            assert seq_result.feature_count == conc_result.feature_count
            assert seq_result.total_sequences == conc_result.total_sequences
        
        return {
            'sequential_time': sequential_time,
            'concurrent_time': concurrent_time,
            'speedup': speedup,
            'datasets_processed': len(concurrent_results)
        }
    
    def test_memory_usage_during_large_operations(self, large_dataset, temp_workspace):
        """Test memory usage during large metadata operations."""
        
        import psutil
        import gc
        
        # Get initial memory usage
        process = psutil.Process()
        initial_memory_mb = process.memory_info().rss / (1024 * 1024)
        
        metadata_manager = TrainingDataMetadataManager(temp_workspace)
        
        # Process large dataset
        feature_configs = [{'index': i} for i in range(large_dataset['n_features'])]
        label_configs = [{'index': i} for i in range(len(large_dataset['label_names']))]
        
        # Monitor memory during processing
        start_time = time.time()
        peak_memory_mb = initial_memory_mb
        
        def monitor_memory():
            nonlocal peak_memory_mb
            current_memory_mb = process.memory_info().rss / (1024 * 1024)
            peak_memory_mb = max(peak_memory_mb, current_memory_mb)
        
        # Create metadata with memory monitoring
        monitor_memory()
        
        metadata = metadata_manager.create_training_metadata(
            dataset_name="memory_test_dataset",
            features_data=large_dataset['features_data'],
            labels_data=large_dataset['labels_data'],
            feature_names=large_dataset['feature_names'],
            label_names=large_dataset['label_names'],
            feature_configs=feature_configs,
            label_configs=label_configs,
            symbols=['MEM_TEST'],
            date_range={'start': '2020-01-01', 'end': '2024-12-31'}
        )
        
        monitor_memory()
        processing_time = time.time() - start_time
        
        # Save metadata and monitor memory
        start_time = time.time()
        metadata_file = metadata_manager.save_metadata(metadata, "memory_test_metadata.json")
        monitor_memory()
        save_time = time.time() - start_time
        
        # Clean up and measure final memory
        del metadata
        gc.collect()
        monitor_memory()
        
        final_memory_mb = process.memory_info().rss / (1024 * 1024)
        memory_used_mb = peak_memory_mb - initial_memory_mb
        memory_efficiency = (large_dataset['n_sequences'] * large_dataset['n_features']) / memory_used_mb
        
        # Memory usage assertions
        assert memory_used_mb < 1000  # Should use less than 1GB for this dataset
        assert final_memory_mb < initial_memory_mb + 100  # Should clean up most memory
        
        print(f"\n💾 Memory Usage Analysis:")
        print(f"   Initial memory: {initial_memory_mb:.1f} MB")
        print(f"   Peak memory: {peak_memory_mb:.1f} MB")
        print(f"   Memory used: {memory_used_mb:.1f} MB")
        print(f"   Final memory: {final_memory_mb:.1f} MB")
        print(f"   Memory efficiency: {memory_efficiency:.0f} feature-sequences/MB")
        print(f"   Processing time: {processing_time:.2f}s")
        print(f"   Save time: {save_time:.2f}s")
        
        return {
            'initial_memory_mb': initial_memory_mb,
            'peak_memory_mb': peak_memory_mb,
            'memory_used_mb': memory_used_mb,
            'final_memory_mb': final_memory_mb,
            'memory_efficiency': memory_efficiency,
            'processing_time': processing_time,
            'save_time': save_time
        }


if __name__ == "__main__":
    # Run performance tests
    pytest.main([__file__, "-v", "-s"])  # -s to show print outputs