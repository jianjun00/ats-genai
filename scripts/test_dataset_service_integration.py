#!/usr/bin/env python3
"""
Test Dataset Service Integration
Validates that dataset service and client work correctly with existing data.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.clients.dataset_client import DatasetClient
from src.services.dataset_service import DatasetService, DatasetMetadata

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_dataset_service_basic():
    """Test basic dataset service functionality."""
    
    logger.info("🧪 Testing basic dataset service functionality")
    
    try:
        # Initialize service
        service = DatasetService()
        logger.info("✅ Dataset service initialized")
        
        # Test database connection
        datasets = service.list_datasets(limit=5)
        logger.info(f"✅ Retrieved {len(datasets)} existing datasets")
        
        if datasets:
            # Test metadata retrieval
            first_dataset = datasets[0]
            logger.info(f"✅ Sample dataset: {first_dataset.dataset_name} (ID: {first_dataset.dataset_id})")
            
            # Test file iterators
            iterators = service.get_file_iterators(first_dataset.dataset_id)
            logger.info(f"✅ Created {len(iterators)} file iterators")
            
            # Test statistics
            stats = service.get_dataset_statistics(first_dataset.dataset_id)
            logger.info(f"✅ Generated statistics for {stats.get('dataset_info', {}).get('name', 'unknown')}")
            
            # Test validation
            validation = service.validate_dataset_availability(first_dataset.dataset_id)
            logger.info(f"✅ Validation result: {validation['accessible_files']}/{validation['total_files']} files accessible")
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Dataset service test failed: {e}")
        return False

def test_dataset_client():
    """Test dataset client functionality."""
    
    logger.info("🧪 Testing dataset client functionality")
    
    try:
        # Initialize client
        client = DatasetClient()
        logger.info("✅ Dataset client initialized")
        
        # Test dataset discovery
        available_datasets = client.list_available_datasets(['AAPL'])
        logger.info(f"✅ Found {len(available_datasets)} datasets for AAPL")
        
        if available_datasets:
            # Test training config generation
            config = client.get_training_data_config(
                symbols=['AAPL'],
                min_sequences=100
            )
            
            if config:
                logger.info(f"✅ Generated training config for {config['dataset_name']}")
                logger.info(f"   📊 {config['total_sequences']} sequences, {config['feature_count']} features")
                logger.info(f"   💾 {config['estimated_memory_mb']:.1f} MB estimated memory")
                
                # Test EDA config generation
                eda_config = client.get_eda_data_config(['AAPL'])
                if eda_config:
                    logger.info(f"✅ Generated EDA config for {eda_config['dataset_name']}")
                    logger.info(f"   🔍 Recommended sample size: {eda_config['recommended_sample_size']}")
                
            else:
                logger.warning("⚠️ No suitable training dataset found")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Dataset client test failed: {e}")
        return False

def test_data_loading():
    """Test actual data loading through dataset service."""
    
    logger.info("🧪 Testing data loading through dataset service")
    
    try:
        client = DatasetClient()
        
        # Find a training configuration
        config = client.get_training_data_config(
            symbols=['AAPL', 'TSLA'],
            min_sequences=10  # Low requirement for testing
        )
        
        if not config:
            logger.warning("⚠️ No dataset available for data loading test")
            return False
        
        # Create data loader
        data_loader = client.create_data_loader(config)
        logger.info(f"✅ Created data loader for {config['dataset_name']}")
        
        # Test sample loading
        X_sample, y_sample = data_loader.get_sample(sample_size=100)
        
        if len(X_sample) > 0:
            logger.info(f"✅ Loaded sample data: X{X_sample.shape}, y{y_sample.shape}")
            logger.info(f"   📊 Data types: X={X_sample.dtype}, y={y_sample.dtype}")
            logger.info(f"   📈 Value ranges: X=[{X_sample.min():.3f}, {X_sample.max():.3f}], y=[{y_sample.min():.3f}, {y_sample.max():.3f}]")
            
            # Test batch iterator
            batch_count = 0
            for X_batch, y_batch in data_loader.get_batch_iterator(batch_size=32):
                batch_count += 1
                if batch_count >= 3:  # Test first few batches
                    break
            
            logger.info(f"✅ Batch iterator working: processed {batch_count} batches")
            
        else:
            logger.error("❌ No sample data loaded")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"❌ Data loading test failed: {e}")
        return False

def test_metadata_operations():
    """Test metadata-related operations."""
    
    logger.info("🧪 Testing metadata operations")
    
    try:
        client = DatasetClient()
        
        # Test dataset search
        datasets = client.dataset_client.service.search_datasets('AAPL', limit=5)
        logger.info(f"✅ Search found {len(datasets)} datasets containing 'AAPL'")
        
        if datasets:
            dataset = datasets[0]
            
            # Test dataset summary
            summary = client.get_dataset_summary(dataset.dataset_id)
            if summary:
                logger.info(f"✅ Generated summary for {summary['name']}")
                logger.info(f"   📊 Size: {summary['size']}, Quality: {summary['quality']}")
                logger.info(f"   📅 Created: {summary['created']}, Timeframes: {summary['timeframes']}")
                
            # Test validation
            validation = client.validate_dataset_for_training(
                dataset_id=dataset.dataset_id,
                required_features=5,
                min_sequences=100
            )
            logger.info(f"✅ Validation result: {validation['valid']}")
            if not validation['valid']:
                logger.info(f"   Issues: {[check for check, passed in validation['checks'].items() if not passed]}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Metadata operations test failed: {e}")
        return False

def run_all_tests():
    """Run all dataset service integration tests."""
    
    logger.info("🚀 Starting Dataset Service Integration Tests")
    
    test_results = {
        'dataset_service_basic': test_dataset_service_basic(),
        'dataset_client': test_dataset_client(),
        'data_loading': test_data_loading(),
        'metadata_operations': test_metadata_operations()
    }
    
    # Summary
    passed_tests = sum(test_results.values())
    total_tests = len(test_results)
    
    logger.info(f"\n📋 Test Results Summary:")
    logger.info(f"   ✅ Passed: {passed_tests}/{total_tests}")
    
    for test_name, result in test_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"   {status}: {test_name}")
    
    if passed_tests == total_tests:
        logger.info("🎉 All dataset service integration tests passed!")
        return True
    else:
        logger.error(f"❌ {total_tests - passed_tests} tests failed")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)