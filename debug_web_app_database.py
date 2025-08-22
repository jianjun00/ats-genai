#!/usr/bin/env python3
"""
Debug script to test web app database connection and training dataset retrieval.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from unified_backtest_analytics_webapp import UnifiedAnalyticsEngine

async def debug_web_app_database():
    """Debug web app database connection and training dataset retrieval."""
    
    print("🔍 Debugging Web App Database Connection")
    print("=" * 50)
    
    engine = UnifiedAnalyticsEngine()
    
    print(f"1. Environment database URL: {engine.env.get_database_url()}")
    
    try:
        print("2. Initializing engine...")
        await engine.initialize()
        
        print(f"3. Pool initialized: {engine.pool is not None}")
        
        if engine.pool:
            print("4. Testing direct database connection...")
            async with engine.pool.acquire() as conn:
                # Test basic connection
                result = await conn.fetchval("SELECT 1")
                print(f"   Basic query result: {result}")
                
                # Check training dataset table exists
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'dev_training_dataset'
                    )
                """)
                print(f"   dev_training_dataset table exists: {table_exists}")
                
                # Count records
                count = await conn.fetchval("SELECT COUNT(*) FROM dev_training_dataset")
                print(f"   Records in dev_training_dataset: {count}")
                
                # Get sample record
                sample = await conn.fetchrow("SELECT * FROM dev_training_dataset LIMIT 1")
                if sample:
                    print(f"   Sample record ID: {sample['id']}")
                    print(f"   Sample dataset name: {sample['dataset_name']}")
                    print(f"   Sample symbols: {sample['symbols']}")
                else:
                    print("   No sample records found")
                
        print("5. Testing get_training_datasets method...")
        datasets = await engine.get_training_datasets(limit=10)
        print(f"   Returned {len(datasets)} datasets")
        
        for i, dataset in enumerate(datasets):
            print(f"   Dataset {i+1}: {dataset.dataset_name}")
            print(f"     Sequences: {dataset.total_sequences}")
            print(f"     Symbols: {dataset.symbols}")
            print(f"     Quality: {dataset.data_quality_score}")
        
        if len(datasets) == 0:
            print("   ❌ No datasets returned - this explains the issue!")
        else:
            print("   ✅ Datasets returned successfully")
            
    except Exception as e:
        print(f"❌ Error during debugging: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await engine.close()
        print("\n🔍 Debug complete")

if __name__ == "__main__":
    asyncio.run(debug_web_app_database())