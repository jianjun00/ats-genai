#!/usr/bin/env python3
"""
Pure Callback-Based Training Data Generation Example

This example shows how to generate training data using ONLY a callback
with the existing Runner framework - no separate TrainingDataRunner class needed.
"""

import asyncio
import tempfile
from pathlib import Path
from datetime import datetime, date

# Add src to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from app.runner import Runner
from config.environment import Environment, EnvironmentType
from state.training_data_callback import DateBasedTrainingDataCallback
from ml.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig


async def example_pure_callback_training():
    """
    Example: Generate training data using PURE callback approach.
    
    NO TrainingDataRunner class is created or used.
    ALL logic is handled by DateBasedTrainingDataCallback.
    """
    print("🎯 Pure Callback Training Data Generation Example")
    print("=" * 60)
    
    # 1. Create training data configuration
    config = TrainingDataConfig(
        base_interval_minutes=1,
        training_interval_minutes=60,
        sequence_lengths={'5m': 12, '15m': 12, '1h': 6, '1d': 5},
        prediction_horizons={'1h': 3, '1d': 2}
    )
    
    with tempfile.TemporaryDirectory() as output_dir:
        print(f"📁 Output directory: {output_dir}")
        
        # 2. ✅ Create ONLY the callback - no runner class!
        training_callback = DateBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],
            config=config,
            output_dir=output_dir,
            save_format="pickle"
        )
        
        print(f"✅ Created training callback: {type(training_callback).__name__}")
        print(f"   Symbols: {training_callback.symbols}")
        print(f"   Save format: {training_callback.save_format}")
        print(f"   ❌ NO TrainingDataRunner class created")
        
        # 3. ✅ Use existing Runner framework with the callback
        runner = Runner(
            start_date="2024-01-15",
            end_date="2024-01-15",
            environment=Environment(env_type=EnvironmentType.TEST),
            universe_id=1,
            callbacks=[training_callback],  # ✅ ONLY the callback
            base_duration="1h"
        )
        
        print(f"✅ Created Runner with callback")
        print(f"   Callbacks: {len(runner.callbacks)} (DateBasedTrainingDataCallback)")
        print(f"   Base duration: 1h")
        print(f"   ❌ NO separate runner class used")
        
        print(f"\n🚀 Running training data generation...")
        print(f"   Method: Pure callback approach")
        print(f"   Framework: Existing Runner + DateBasedTrainingDataCallback")
        
        # 4. ✅ Execute using existing framework
        await runner.run()
        
        print(f"\n✅ Training data generation completed!")
        
        # 5. Verify output structure
        output_path = Path(output_dir)
        daily_dir = output_path / "daily"
        metadata_dir = output_path / "metadata"
        
        print(f"\n📂 Generated Output Structure:")
        if daily_dir.exists():
            daily_subdirs = list(daily_dir.iterdir())
            print(f"   Daily directories: {len(daily_subdirs)}")
            for subdir in daily_subdirs:
                files = list(subdir.iterdir()) if subdir.is_dir() else []
                print(f"      {subdir.name}: {len(files)} files")
        
        if metadata_dir.exists():
            metadata_files = list(metadata_dir.glob("*.json"))
            print(f"   Metadata files: {len(metadata_files)}")
            for f in metadata_files:
                print(f"      {f.name}")
        
        print(f"\n🎯 Key Points:")
        print(f"   ✅ ALL training data logic is in callback handlers:")
        print(f"      • handleStart: Initialize training generator")
        print(f"      • handleStartOfDay: Open daily data collection")
        print(f"      • handleInterval: Generate training examples")
        print(f"      • handleEndOfDay: Save daily data")
        print(f"      • handleEnd: Final summary")
        print(f"   ✅ Uses existing Runner framework")
        print(f"   ❌ NO separate TrainingDataRunner class needed")
        print(f"   ✅ Date-based file organization")
        print(f"   ✅ Follows indicator_runner pattern")


async def example_with_advanced_storage():
    """
    Example: Pure callback training with advanced storage.
    """
    print(f"\n🔬 Advanced Storage Example")
    print("=" * 60)
    
    config = TrainingDataConfig(
        sequence_lengths={'5m': 24, '15m': 24, '1h': 12, '1d': 10},
        prediction_horizons={'1h': 6, '1d': 5}
    )
    
    with tempfile.TemporaryDirectory() as output_dir:
        # Create advanced storage manager
        storage_config = StorageConfig(
            primary_format='pickle',  # Use pickle for testing
            compression_level=6,
            chunk_size=1000,
            enable_indexing=True,
            enable_checksums=True
        )
        storage_manager = SequenceStorageManager(
            base_path=output_dir,
            config=storage_config
        )
        
        # ✅ Create callback with advanced storage
        training_callback = DateBasedTrainingDataCallback(
            symbols=['AAPL'],
            config=config,
            output_dir=output_dir,
            save_format="advanced",
            storage_manager=storage_manager
        )
        
        print(f"✅ Created callback with advanced storage")
        print(f"   Storage format: {storage_config.primary_format}")
        print(f"   Compression: Level {storage_config.compression_level}")
        print(f"   Chunking: {storage_config.chunk_size} examples per chunk")
        
        # ✅ Use with existing Runner framework
        runner = Runner(
            start_date="2024-01-15",
            end_date="2024-01-15",
            environment=Environment(env_type=EnvironmentType.TEST),
            universe_id=1,
            callbacks=[training_callback],
            base_duration="30m"
        )
        
        print(f"✅ Running with advanced storage...")
        await runner.run()
        print(f"✅ Advanced storage training completed!")


async def main():
    """Run all examples."""
    try:
        # Basic example
        await example_pure_callback_training()
        
        # Advanced storage example
        await example_with_advanced_storage()
        
        print(f"\n" + "=" * 60)
        print(f"🎉 ALL EXAMPLES COMPLETED!")
        print(f"=" * 60)
        print(f"✅ Pure callback-based training data generation works perfectly")
        print(f"✅ No TrainingDataRunner class needed")
        print(f"✅ Uses existing Runner framework + DateBasedTrainingDataCallback")
        print(f"✅ All logic in callback handlers (handleStart, handleInterval, etc.)")
        print(f"✅ Date-based file organization")
        print(f"✅ Advanced storage support")
        print(f"✅ Follows established patterns")
        
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())