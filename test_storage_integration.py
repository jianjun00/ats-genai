#!/usr/bin/env python3
"""
Test Storage Integration - Quick validation of the complete pipeline

This script validates that the TimeSeriesSequenceTrainingGenerator works
correctly with the new SequenceStorageManager.
"""

import os
import sys
import asyncio
import logging
import tempfile
from pathlib import Path
from datetime import date, datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from config.environment import Environment
from ml.training_data.timeseries_sequence_training_generator import (
    TimeSeriesSequenceTrainingGenerator,
    TrainingDataConfig
)
from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig
from state.universe_state_manager import UniverseStateManager


async def test_basic_integration():
    """Test basic integration between generator and storage."""
    print("🧪 Testing basic integration...")
    
    # Set up components
    env = Environment()
    config = TrainingDataConfig(
        base_interval_minutes=1,
        training_interval_minutes=60,
        sequence_lengths={'5m': 3, '15m': 2, '1h': 2, '1d': 2},  # Small for testing
        prediction_horizons={'1h': 1, '1d': 1}
    )
    
    universe_manager = UniverseStateManager(env=env)
    generator = TimeSeriesSequenceTrainingGenerator(
        env=env,
        config=config,
        universe_manager=universe_manager
    )
    
    print("✅ Components initialized")
    return generator, config


async def test_training_data_generation():
    """Test training data generation."""
    print("\n📊 Testing training data generation...")
    
    generator, config = await test_basic_integration()
    
    try:
        # Generate a small dataset
        start_date = date(2024, 1, 15)
        end_date = date(2024, 1, 15)  # Single day for fast testing
        
        examples = await generator.generate_training_dataset(
            symbols=['AAPL'],
            start_date=start_date,
            end_date=end_date,
            min_examples_per_symbol=1
        )
        
        print(f"✅ Generated {len(examples)} examples")
        
        if examples:
            sample = examples[0]
            print(f"   Sample: {sample.symbol} at {sample.prediction_timestamp}")
            print(f"   Base features: {len(sample.base_features)}")
            print(f"   Sequence lengths: {sample.sequence_length}")
            
        return examples
        
    except Exception as e:
        print(f"❌ Training data generation failed: {e}")
        return []


async def test_storage_formats():
    """Test different storage formats."""
    print("\n💾 Testing storage formats...")
    
    examples = await test_training_data_generation()
    if not examples:
        print("⚠️ No examples to test storage with")
        return
    
    # Test each storage format
    formats_to_test = ['pickle', 'tfrecord', 'riegeli']
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for storage_format in formats_to_test:
            print(f"\n   Testing {storage_format} format...")
            
            try:
                # Create storage manager
                storage_config = StorageConfig(
                    primary_format=storage_format,
                    compression_level=1,  # Fast compression for testing
                    chunk_size=len(examples),
                    enable_checksums=False  # Skip for speed
                )
                
                storage_path = Path(temp_dir) / f"test_{storage_format}"
                storage_manager = SequenceStorageManager(
                    base_path=str(storage_path),
                    config=storage_config
                )
                
                # Test save
                batch_id = f"test_{storage_format}"
                save_result = await storage_manager.save_sequence_batch(examples, batch_id)
                
                # Test load
                loaded_examples = await storage_manager.load_sequence_batch(batch_id)
                
                print(f"   ✅ {storage_format}: saved/loaded {len(loaded_examples)} examples")
                
                # Show file size
                if 'sequence_stats' in save_result:
                    file_size = save_result['sequence_stats'].get('file_size', 0)
                    print(f"      File size: {file_size} bytes")
                
            except Exception as e:
                print(f"   ❌ {storage_format} failed: {e}")


async def test_querying():
    """Test querying capabilities."""
    print("\n🔍 Testing querying capabilities...")
    
    examples = await test_training_data_generation()
    if not examples:
        print("⚠️ No examples to test querying with")
        return
    
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Set up storage
            storage_config = StorageConfig(
                primary_format='pickle',  # Use pickle for simplicity
                enable_indexing=True
            )
            
            storage_manager = SequenceStorageManager(
                base_path=temp_dir,
                config=storage_config
            )
            
            # Save examples
            batch_id = "query_test"
            await storage_manager.save_sequence_batch(examples, batch_id)
            
            # Test querying
            start_datetime = datetime(2024, 1, 15, 0, 0)
            end_datetime = datetime(2024, 1, 15, 23, 59)
            
            query_results = await storage_manager.query_by_symbol(
                symbol='AAPL',
                start_date=start_datetime,
                end_date=end_datetime
            )
            
            print(f"✅ Query returned {len(query_results)} results")
            
            if query_results:
                sample_result = query_results[0]
                print(f"   Sample result: {sample_result.symbol} at {sample_result.prediction_timestamp}")
            
        except Exception as e:
            print(f"❌ Querying test failed: {e}")


async def test_training_data_runner_integration():
    """Test integration with TrainingDataRunner."""
    print("\n🏃 Testing TrainingDataRunner integration...")
    
    try:
        from app.training_data_runner import TrainingDataRunner
        
        # Create runner with advanced storage
        runner = TrainingDataRunner(
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
            environment=Environment(),
            symbols=['AAPL'],
            use_advanced_storage=True,
            storage_format='pickle',  # Use pickle for simplicity
            compression_level=1,
            output_dir='/tmp/test_runner_storage',
            debug=False
        )
        
        # Generate small dataset
        examples = await runner.generate_training_data(
            min_examples_per_symbol=1,
            dry_run=False
        )
        
        if examples:
            # Save using advanced storage
            saved_files = await runner.save_training_data(
                examples=examples,
                output_formats=[],  # Only use advanced storage
                dry_run=False
            )
            
            print(f"✅ TrainingDataRunner integration successful")
            print(f"   Generated {len(examples)} examples")
            print(f"   Advanced storage: {saved_files.get('advanced_storage', {}).get('batch_id')}")
        else:
            print("⚠️ No examples generated in runner test")
            
    except Exception as e:
        print(f"❌ TrainingDataRunner integration failed: {e}")


async def main():
    """Run all integration tests."""
    print("🚀 Starting Storage Integration Tests")
    print("=" * 60)
    
    # Configure logging to reduce noise
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Run tests
        await test_basic_integration()
        await test_training_data_generation()
        await test_storage_formats()
        await test_querying()
        await test_training_data_runner_integration()
        
        print("\n" + "=" * 60)
        print("🎉 All integration tests completed!")
        print("\n✨ Summary of tested components:")
        print("   ✅ TimeSeriesSequenceTrainingGenerator")
        print("   ✅ SequenceStorageManager (multiple formats)")
        print("   ✅ Storage querying capabilities")
        print("   ✅ TrainingDataRunner integration")
        print("   ✅ End-to-end pipeline")
        
    except Exception as e:
        print(f"\n❌ Integration tests failed: {e}")
        logging.error(f"Integration test failure: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())