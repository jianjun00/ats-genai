#!/usr/bin/env python3
"""
Test to reproduce timezone mismatch error in FirstRate backfill processing.

This test reproduces the exact production error:
TypeError: Cannot compare tz-naive and tz-aware timestamps

The bug occurs when:
1. New data has timezone-naive timestamps (pd.to_datetime without tz)
2. Existing parquet data has timezone-aware timestamps
3. pd.concat merges them and sort_values fails on comparison
"""

import pytest
import pandas as pd
from datetime import datetime
from pathlib import Path
import tempfile
import shutil


def test_timezone_mismatch_in_merge():
    """
    Reproduce exact error: Cannot compare tz-naive and tz-aware timestamps
    
    This simulates the FirstRate backfill process where:
    - Existing parquet file has timezone-aware timestamps
    - New data from zip has timezone-naive timestamps
    - Merge and sort fails on comparison
    """
    
    existing_data = pd.DataFrame({
        'timestamp': pd.to_datetime(['2025-08-01 09:30:00', '2025-08-01 09:31:00'], utc=True),
        'open': [150.0, 151.0],
        'high': [152.0, 153.0],
        'low': [149.0, 150.0],
        'close': [151.0, 152.0],
        'volume': [1000, 1100],
        'symbol': ['AAPL', 'AAPL'],
        'vendor': ['firstrate', 'firstrate']
    })
    
    new_data = pd.DataFrame({
        'timestamp': pd.to_datetime(['2025-08-01 09:32:00', '2025-08-01 09:33:00']),
        'open': [152.0, 153.0],
        'high': [154.0, 155.0],
        'low': [151.0, 152.0],
        'close': [153.0, 154.0],
        'volume': [1200, 1300],
        'symbol': ['AAPL', 'AAPL'],
        'vendor': ['firstrate', 'firstrate']
    })
    
    print(f"Existing timestamp dtype: {existing_data['timestamp'].dtype}")
    print(f"Existing timestamp tz: {existing_data['timestamp'].dt.tz}")
    print(f"New timestamp dtype: {new_data['timestamp'].dtype}")
    print(f"New timestamp tz: {new_data['timestamp'].dt.tz}")
    
    merged_df = pd.concat([existing_data, new_data], ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset=['timestamp'], keep='last')
    
    with pytest.raises(TypeError, match="Cannot compare tz-naive and tz-aware"):
        merged_df = merged_df.sort_values('timestamp')


def test_timezone_mismatch_from_parquet_roundtrip():
    """
    Test that reproduces the bug through actual parquet read/write cycle.
    
    This simulates the exact production scenario:
    1. Write data with timezone-aware timestamps to parquet
    2. Read it back
    3. Try to merge with new timezone-naive data
    4. Sort fails
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = Path(tmpdir) / "test.parquet"
        
        existing_data = pd.DataFrame({
            'timestamp': pd.to_datetime(['2025-08-01 09:30:00', '2025-08-01 09:31:00'], utc=True),
            'open': [150.0, 151.0],
            'high': [152.0, 153.0],
            'low': [149.0, 150.0],
            'close': [151.0, 152.0],
            'volume': [1000, 1100],
            'symbol': ['AAPL', 'AAPL'],
            'vendor': ['firstrate', 'firstrate']
        })
        
        existing_data.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
        
        loaded_df = pd.read_parquet(parquet_path)
        
        new_data = pd.DataFrame({
            'timestamp': pd.to_datetime(['2025-08-01 09:32:00', '2025-08-01 09:33:00']),
            'open': [152.0, 153.0],
            'high': [154.0, 155.0],
            'low': [151.0, 152.0],
            'close': [153.0, 154.0],
            'volume': [1200, 1300],
            'symbol': ['AAPL', 'AAPL'],
            'vendor': ['firstrate', 'firstrate']
        })
        
        print(f"Loaded timestamp tz: {loaded_df['timestamp'].dt.tz}")
        print(f"New timestamp tz: {new_data['timestamp'].dt.tz}")
        
        merged_df = pd.concat([loaded_df, new_data], ignore_index=True)
        merged_df = merged_df.drop_duplicates(subset=['timestamp'], keep='last')
        
        with pytest.raises(TypeError, match="Cannot compare tz-naive and tz-aware"):
            merged_df = merged_df.sort_values('timestamp')


def test_timezone_normalization_fix():
    """
    Test the FIX: Normalize all timestamps to timezone-naive before merging.
    
    This demonstrates the correct solution:
    - Convert timezone-aware to naive using .dt.tz_localize(None)
    - Ensures consistent timezone handling across all data
    """
    
    existing_data = pd.DataFrame({
        'timestamp': pd.to_datetime(['2025-08-01 09:30:00', '2025-08-01 09:31:00'], utc=True),
        'open': [150.0, 151.0],
        'high': [152.0, 153.0],
        'low': [149.0, 150.0],
        'close': [151.0, 152.0],
        'volume': [1000, 1100],
        'symbol': ['AAPL', 'AAPL'],
        'vendor': ['firstrate', 'firstrate']
    })
    
    if existing_data['timestamp'].dt.tz is not None:
        existing_data['timestamp'] = existing_data['timestamp'].dt.tz_localize(None)
    
    new_data = pd.DataFrame({
        'timestamp': pd.to_datetime(['2025-08-01 09:32:00', '2025-08-01 09:33:00']),
        'open': [152.0, 153.0],
        'high': [154.0, 155.0],
        'low': [151.0, 152.0],
        'close': [153.0, 154.0],
        'volume': [1200, 1300],
        'symbol': ['AAPL', 'AAPL'],
        'vendor': ['firstrate', 'firstrate']
    })
    
    if new_data['timestamp'].dt.tz is not None:
        new_data['timestamp'] = new_data['timestamp'].dt.tz_localize(None)
    
    merged_df = pd.concat([existing_data, new_data], ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset=['timestamp'], keep='last')
    
    merged_df = merged_df.sort_values('timestamp')
    
    assert len(merged_df) == 4
    assert merged_df['timestamp'].dt.tz is None
    assert merged_df['timestamp'].is_monotonic_increasing


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])