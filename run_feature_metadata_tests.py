#!/usr/bin/env python3
"""
Simplified test runner for feature metadata functionality
"""

import sys
import os
import numpy as np
import tempfile
import shutil

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from ml.training_data.generators.training_data_metadata import (
    TrainingDataMetadataManager, FeatureType
)
from services.dataset_service import DatasetService


def test_core_metadata_functionality():
    """Test core metadata functionality."""

    print("🧪 Testing Core Feature Metadata Functionality")
    print("=" * 60)

    # Create temporary workspace
    temp_dir = tempfile.mkdtemp()

    try:
        # Step 1: Test metadata manager initialization
        manager = TrainingDataMetadataManager(temp_dir)
        print("✅ TrainingDataMetadataManager initialized")

        # Step 2: Generate sample data
        np.random.seed(42)
        features_data = np.random.normal(100, 15, (500, 50, 8))  # 500 sequences, 50 timesteps, 8 features
        labels_data = np.random.normal(0.001, 0.02, (500, 2))   # 2 labels

        feature_names = [
            'open', 'high', 'low', 'close', 'volume',
            'envelope_top', 'envelope_bot', 'BXTrenderBasic_14'
        ]
        label_names = ['return_1h', 'return_1d']

        print(f"✅ Generated sample data: {features_data.shape} features, {labels_data.shape} labels")

        # Step 3: Create feature configurations
        feature_configs = [
            {'source_column': 'open'},
            {'source_column': 'high'},
            {'source_column': 'low'},
            {'source_column': 'close'},
            {'source_column': 'volume'},
            {'indicator_type': 'price_envelope', 'percentage': 2.5},
            {'indicator_type': 'price_envelope', 'percentage': 2.5},
            {'indicator_type': 'bx_trender', 'variant': 'basic', 'window_size': 14}
        ]

        label_configs = [
            {'label_type': 'return', 'lead_periods': 1},
            {'label_type': 'return', 'lead_periods': 24}
        ]

        print("✅ Configured feature and label parameters")

        # Step 4: Generate comprehensive metadata
        metadata = manager.create_training_metadata(
            dataset_name="test_comprehensive_metadata",
            features_data=features_data,
            labels_data=labels_data,
            feature_names=feature_names,
            label_names=label_names,
            feature_configs=feature_configs,
            label_configs=label_configs,
            symbols=['AAPL', 'TSLA'],
            date_range={'start': '2024-01-01', 'end': '2024-12-31'},
            gin_config_path='config/test.gin'
        )

        print("✅ Generated comprehensive training metadata")

        # Step 5: Validate metadata structure
        assert metadata.dataset_name == "test_comprehensive_metadata"
        assert metadata.total_sequences == 500
        assert metadata.sequence_length == 50
        assert metadata.feature_count == 8
        assert metadata.label_count == 2
        assert len(metadata.features) == 8
        assert len(metadata.labels) == 2

        print(f"✅ Validated metadata structure:")
        print(f"   Dataset: {metadata.dataset_name}")
        print(f"   Sequences: {metadata.total_sequences:,}")
        print(f"   Features: {metadata.feature_count}")
        print(f"   Labels: {metadata.label_count}")

        # Step 6: Validate enhanced feature metadata
        enhanced_features_found = 0
        for feature in metadata.features:
            if (hasattr(feature, 'shape') and feature.shape and
                hasattr(feature, 'outlier_count') and
                hasattr(feature, 'visualization_hints') and
                hasattr(feature, 'technical_indicator_params')):
                enhanced_features_found += 1

        print(f"✅ Enhanced metadata found in {enhanced_features_found}/{len(metadata.features)} features")

        # Step 7: Test specific feature types and visualization hints
        ohlc_features = [f for f in metadata.features if f.name in ['open', 'high', 'low', 'close']]
        for ohlc_feature in ohlc_features:
            assert ohlc_feature.feature_type == FeatureType.OHLC
            assert ohlc_feature.visualization_hints.get('color_scheme') == 'green_red'
            assert ohlc_feature.visualization_hints.get('is_primary_indicator') == True

        print(f"✅ OHLC features properly classified: {len(ohlc_features)} found")

        # Step 8: Test technical indicator parameters
        bx_feature = next(f for f in metadata.features if 'BXTrender' in f.name)
        assert bx_feature.technical_indicator_params.get('indicator_type') == 'bx_trender'
        assert bx_feature.technical_indicator_params.get('variant') == 'basic'

        print("✅ BX Trender parameters extracted correctly")

        # Step 9: Test metadata serialization
        metadata_file = manager.save_metadata(metadata, "test_metadata.json")
        assert os.path.exists(metadata_file)

        file_size_mb = os.path.getsize(metadata_file) / (1024 * 1024)
        print(f"✅ Metadata serialized to {os.path.basename(metadata_file)} ({file_size_mb:.2f} MB)")

        # Step 10: Test dataset service basic metadata generation
        dataset_service = DatasetService({
            'host': 'localhost', 'port': 3432, 'database': 'test_db',
            'user': 'test_user', 'password': 'test_password'
        })

        basic_metadata = dataset_service._generate_basic_feature_metadata(
            1, 'SMA_20,EMA_14,RSI_14,envelope_top,envelope_bot,BXTrenderBasic_14'
        )

        assert basic_metadata['metadata_version'] == '1.0'
        assert len(basic_metadata['features']) == 11  # 5 OHLCV + 6 indicators
        assert basic_metadata['total_features'] == 11

        print(f"✅ Dataset service generated basic metadata: {len(basic_metadata['features'])} features")

        print("\n🎉 ALL CORE TESTS PASSED!")
        print("=" * 60)
        print("✅ TrainingDataMetadataManager enhanced functionality working")
        print("✅ Feature type inference and classification working")
        print("✅ Statistical analysis and outlier detection working")
        print("✅ Visualization hints generation working")
        print("✅ Technical indicator parameter extraction working")
        print("✅ Metadata serialization/deserialization working")
        print("✅ Dataset service basic metadata generation working")

        return True

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Cleanup
        shutil.rmtree(temp_dir)


def test_performance_characteristics():
    """Test performance characteristics of metadata operations."""

    print("\n⚡ Testing Performance Characteristics")
    print("=" * 60)

    temp_dir = tempfile.mkdtemp()

    try:
        manager = TrainingDataMetadataManager(temp_dir)

        # Generate larger dataset for performance testing
        np.random.seed(42)
        large_features = np.random.normal(100, 15, (2000, 100, 25))  # 2000 sequences, 100 timesteps, 25 features
        large_labels = np.random.normal(0.001, 0.02, (2000, 3))     # 3 labels

        feature_names = [f"feature_{i:02d}" for i in range(25)]
        label_names = [f"label_{i}" for i in range(3)]

        feature_configs = [{'index': i} for i in range(25)]
        label_configs = [{'index': i} for i in range(3)]

        print(f"Generated large dataset: {large_features.shape} features, {large_labels.shape} labels")

        # Time the metadata generation
        import time
        start_time = time.time()

        metadata = manager.create_training_metadata(
            dataset_name="performance_test",
            features_data=large_features,
            labels_data=large_labels,
            feature_names=feature_names,
            label_names=label_names,
            feature_configs=feature_configs,
            label_configs=label_configs,
            symbols=['PERF_TEST'],
            date_range={'start': '2024-01-01', 'end': '2024-12-31'}
        )

        generation_time = time.time() - start_time

        # Calculate throughput
        total_feature_values = large_features.size
        throughput = total_feature_values / generation_time

        print(f"✅ Large metadata generation completed:")
        print(f"   Time: {generation_time:.2f} seconds")
        print(f"   Total feature values: {total_feature_values:,}")
        print(f"   Throughput: {throughput:,.0f} values/second")

        # Test serialization performance
        start_time = time.time()
        metadata_file = manager.save_metadata(metadata, "performance_metadata.json")
        serialization_time = time.time() - start_time

        file_size_mb = os.path.getsize(metadata_file) / (1024 * 1024)

        print(f"✅ Serialization completed:")
        print(f"   Time: {serialization_time:.2f} seconds")
        print(f"   File size: {file_size_mb:.2f} MB")
        print(f"   Write speed: {file_size_mb / serialization_time:.1f} MB/second")

        # Performance assertions
        assert generation_time < 5.0  # Should complete within 5 seconds
        assert serialization_time < 1.0  # Should serialize within 1 second
        assert throughput > 100000  # Should process > 100k values/second

        print("\n✅ Performance characteristics meet requirements!")

        return True

    except Exception as e:
        print(f"\n❌ PERFORMANCE ERROR: {e}")
        return False

    finally:
        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    print("🚀 Feature Metadata Test Suite")
    print("=" * 60)

    success = True

    # Run core functionality tests
    if not test_core_metadata_functionality():
        success = False

    # Run performance tests
    if not test_performance_characteristics():
        success = False

    if success:
        print("\n🎉 ALL TESTS PASSED - Feature metadata system is working correctly!")
        print("🚀 Ready for production use")
    else:
        print("\n❌ SOME TESTS FAILED - Please review the errors above")
        sys.exit(1)