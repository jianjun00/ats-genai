#!/usr/bin/env python3
"""
Debug the FileBasedMinuteManager integration with existing AAPL data.
"""

import asyncio
import sys
sys.path.append('src')

from datetime import datetime
from storage.file_based_minute_manager import FileBasedMinuteManager

async def test_minute_manager_access():
    """Test accessing existing AAPL minute-level data."""
    
    print("🔍 Testing FileBasedMinuteManager with existing AAPL data")
    print("=" * 60)
    
    # Use the correct path where AAPL data exists
    base_path = "/mnt/d/ats-data/minute-bars"
    manager = FileBasedMinuteManager(base_path=base_path)
    
    print(f"Base path: {base_path}")
    print(f"Expected AAPL file: {base_path}/AAPL/2024/01/AAPL_2024_01.parquet")
    
    try:
        # Test 1: Query data from January 2024 (we know this file exists)
        print("\n📊 Test 1: Querying AAPL data for January 2024")
        
        start_date = datetime(2024, 1, 1, 9, 30)
        end_date = datetime(2024, 1, 31, 16, 0)
        
        result_df = await manager.query_minute_data(
            symbol='AAPL',
            start_date=start_date,
            end_date=end_date
        )
        
        if result_df.empty:
            print("❌ No data returned - investigating...")
            
            # Check if files are found correctly
            print(f"\n🔎 Debug: Finding relevant files...")
            files = manager._find_relevant_monthly_files('AAPL', start_date, end_date)
            print(f"Files found: {files}")
            
        else:
            print(f"✅ SUCCESS: Retrieved {len(result_df)} minute bars")
            print(f"Date range: {result_df['timestamp'].min()} to {result_df['timestamp'].max()}")
            print(f"Columns: {list(result_df.columns)}")
            print(f"Sample data:")
            print(result_df.head())
            
        # Test 2: Get storage stats
        print("\n📈 Test 2: Getting storage statistics")
        stats = await manager.get_storage_stats()
        
        print(f"Storage stats:")
        print(f"  Symbols: {stats.get('symbols', 0)}")
        print(f"  Files: {stats.get('files', 0)}")
        print(f"  Total records: {stats.get('total_records', 0)}")
        print(f"  AAPL details: {stats.get('symbols_detail', {}).get('AAPL', 'Not found')}")
        
        await manager.close()
        
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_minute_manager_access())