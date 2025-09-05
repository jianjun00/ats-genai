#!/usr/bin/env python3

import sys
import os
import json
import asyncio
from pathlib import Path
from datetime import datetime

# Add src to Python path
sys.path.insert(0, 'src')

async def test_arrayrecord_storage():
    """Test the fixed ArrayRecord storage manager."""
    
    print("Testing ArrayRecord storage manager...")
    
    # Import the storage manager
    try:
        from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig
        print("✅ Successfully imported SequenceStorageManager")
    except ImportError as e:
        print(f"❌ Failed to import SequenceStorageManager: {e}")
        return
    
    # Create a temporary storage directory
    storage_dir = Path("/tmp/test_arrayrecord_storage")
    storage_dir.mkdir(exist_ok=True)
    
    # Initialize the storage manager
    config = StorageConfig(primary_format="arrayrecord")
    storage_manager = SequenceStorageManager(str(storage_dir), config)
    
    print(f"✅ Created storage manager with base path: {storage_dir}")
    
    # Create test data that matches the expected format
    test_examples = []
    
    class MockSequenceExample:
        def __init__(self):
            self.symbol = "TSLA"
            self.prediction_timestamp = datetime(2025, 8, 1, 10, 30, 0)
            self.instrument_id = 12345
            self.base_features = [1.0, 2.0, 3.0, 4.0, 5.0]
            self.sequence_5m = [{"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000}]
            self.sequence_15m = [{"open": 100.5, "high": 102.0, "low": 100.0, "close": 101.5, "volume": 3000}]
            self.sequence_1h = [{"open": 101.0, "high": 103.0, "low": 101.0, "close": 102.0, "volume": 10000}]
            self.sequence_1d = [{"open": 102.0, "high": 105.0, "low": 102.0, "close": 104.0, "volume": 50000}]
            self.timeframe_features = {"5m": 1, "15m": 1, "1h": 1, "1d": 1}
            self.future_1h = [{"return": 0.05}]
            self.future_1d = [{"return": 0.10}]
            self.sequence_length = {"5m": 1, "15m": 1, "1h": 1, "1d": 1}
            self.prediction_horizon = {"1h": 1, "1d": 1}
    
    # Create a test example
    test_example = MockSequenceExample()
    test_examples.append(test_example)
    
    print("✅ Created test sequence example")
    
    # Test the storage
    try:
        batch_id = "test_batch_001"
        result = await storage_manager.save_sequence_batch(test_examples, batch_id)
        
        print("✅ Successfully saved sequence batch")
        print(f"Result: {json.dumps(result, indent=2, default=str)}")
        
        # Check if files were created
        sequence_files = list(storage_dir.glob("sequences/*"))
        metadata_files = list(storage_dir.glob("metadata/*"))
        
        print(f"✅ Created {len(sequence_files)} sequence files")
        print(f"✅ Created {len(metadata_files)} metadata files")
        
        for seq_file in sequence_files:
            print(f"  - Sequence file: {seq_file} ({seq_file.stat().st_size} bytes)")
        
        for meta_file in metadata_files:
            print(f"  - Metadata file: {meta_file} ({meta_file.stat().st_size} bytes)")
            
        return True
        
    except Exception as e:
        print(f"❌ Failed to save sequence batch: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(test_arrayrecord_storage())
    
    if success:
        print("\n🎉 ArrayRecord storage test passed!")
    else:
        print("\n❌ ArrayRecord storage test failed!")
        sys.exit(1)