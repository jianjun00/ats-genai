#!/usr/bin/env python3
"""
Debug script to identify why training data API returns empty array.

This script will test the actual web app API method that's running in Kubernetes
and compare it to direct database queries to identify the disconnect.
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from unified_backtest_analytics_webapp import UnifiedAnalyticsEngine
from dao.training_dataset_dao import TrainingDatasetDAO
from config.environment import Environment, EnvironmentType

async def debug_training_data_api_issue():
    """Debug the training data API issue step by step."""
    
    print("🔍 Debugging Training Data API Issue")
    print("=" * 50)
    
    # Step 1: Direct database query to verify data exists
    print("1. Testing direct database connection...")
    
    # Use the correct Kubernetes database connection via port-forward
    db_url = "postgresql://postgres:dev_password@localhost:5433/dev_db"
    
    try:
        conn = await asyncpg.connect(db_url)
        
        # Check if table exists
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'dev_training_dataset'
            )
        """)
        print(f"   dev_training_dataset table exists: {table_exists}")
        
        if table_exists:
            # Count total records
            count = await conn.fetchval("SELECT COUNT(*) FROM dev_training_dataset")
            print(f"   Total records: {count}")
            
            # Get sample records
            sample_records = await conn.fetch("SELECT id, dataset_name, symbols, status, created_by FROM dev_training_dataset LIMIT 3")
            if sample_records:
                print("   Sample records:")
                for record in sample_records:
                    print(f"     ID {record['id']}: {record['dataset_name']} ({record['symbols']}) - {record['status']}")
            else:
                print("   No records found in table")
        
        await conn.close()
        
    except Exception as e:
        print(f"   ❌ Direct database connection failed: {e}")
        return False
    
    # Step 2: Test DAO layer
    print("\n2. Testing DAO layer...")
    
    try:
        # Create environment with DEV configuration to match Kubernetes
        db_url = "postgresql://postgres:dev_password@localhost:5433/dev_db"
        env = Environment(env_type=EnvironmentType.DEV, db_url=db_url)
        dao = TrainingDatasetDAO(env=env)
        
        print(f"   Environment type: {env.env_type}")
        print(f"   Database URL: {env.get_database_url()}")
        print(f"   Table name: {env.get_table_name('training_dataset')}")
        
        # Get datasets via DAO
        datasets = await dao.list_training_datasets(limit=5)
        print(f"   DAO returned {len(datasets)} datasets")
        
        for dataset in datasets:
            print(f"     Dataset: {dataset.dataset_name} ({dataset.symbols})")
        
    except Exception as e:
        print(f"   ❌ DAO layer failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 3: Test web app engine directly
    print("\n3. Testing UnifiedAnalyticsEngine...")
    
    try:
        # Create engine with DEV environment configuration
        db_url = "postgresql://postgres:dev_password@localhost:5433/dev_db"
        engine = UnifiedAnalyticsEngine()
        # Override the environment to use DEV configuration
        engine.env = Environment(env_type=EnvironmentType.DEV, db_url=db_url)
        await engine.initialize()
        
        print(f"   Engine initialized: {engine.pool is not None}")
        print(f"   Database URL: {engine.env.get_database_url()}")
        
        # Test the exact method called by the API
        datasets = await engine.get_training_datasets(limit=10)
        print(f"   Engine returned {len(datasets)} datasets")
        
        for dataset in datasets:
            print(f"     Dataset: {dataset.dataset_name} ({dataset.symbols}) - Quality: {dataset.data_quality_score:.2%}")
        
        await engine.close()
        
    except Exception as e:
        print(f"   ❌ UnifiedAnalyticsEngine failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 4: Compare database environments
    print("\n4. Checking database environment consistency...")
    
    try:
        # Check local environment
        local_env = Environment(env_type=EnvironmentType.DEV, db_url="postgresql://postgres:dev_password@localhost:5433/dev_db")
        print(f"   Local env type: {local_env.env_type}")
        print(f"   Local database URL: {local_env.get_database_url()}")
        print(f"   Local table name: {local_env.get_table_name('training_dataset')}")
        
        # Check what the Kubernetes web app would use
        k8s_env = Environment(env_type=EnvironmentType.DEV, db_url="postgresql://postgres:dev_password@postgres-simple:5432/dev_db")
        print(f"   K8s env type: {k8s_env.env_type}")
        print(f"   K8s database URL: {k8s_env.get_database_url()}")
        print(f"   K8s table name: {k8s_env.get_table_name('training_dataset')}")
        
        # Check what table the web app is actually querying
        print("\n5. Verifying web app table name...")
        
        # Test the exact query the web app uses
        conn = await asyncpg.connect(db_url)
        
        # Try different possible table names
        possible_tables = [
            "dev_training_dataset", 
            "training_dataset", 
            "test_training_dataset",
            "intg_training_dataset"
        ]
        
        for table_name in possible_tables:
            try:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                print(f"   {table_name}: {count} records")
            except Exception:
                print(f"   {table_name}: table does not exist")
        
        await conn.close()
        
    except Exception as e:
        print(f"   ❌ Environment check failed: {e}")
        return False
    
    print("\n🎯 Debugging Complete")
    return True

async def main():
    """Main debug function."""
    print("Training Data API Debug - Comprehensive Analysis")
    print("Testing all layers: Database → DAO → Engine → API")
    print()
    
    success = await debug_training_data_api_issue()
    
    if success:
        print("\n✅ Debug analysis complete - check output for insights")
    else:
        print("\n❌ Debug analysis failed - check errors above")

if __name__ == "__main__":
    asyncio.run(main())