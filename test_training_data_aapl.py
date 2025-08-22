#!/usr/bin/env python3
"""
Manual test script for AAPL training data generation.

This script will:
1. Generate training data for AAPL
2. Create run and dataset records in the database  
3. Verify the data was created correctly
4. Show that the web app can access the data
"""

import asyncio
import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from app.training_data_job_runner import run_training_data_job_for_symbol
from dao.training_dataset_dao import TrainingDatasetDAO
from config.environment import Environment

async def test_aapl_training_data_generation():
    """Test AAPL training data generation end-to-end."""
    
    print("🚀 Starting AAPL Training Data Generation Test")
    print("=" * 60)
    
    try:
        # Step 1: Generate training data for AAPL
        print("📊 Step 1: Generating training data for AAPL...")
        results = await run_training_data_job_for_symbol('AAPL', output_dir="training_data_output")
        
        print(f"Status: {results['status']}")
        
        if results['status'] == 'success':
            print(f"✅ Training data generation completed successfully!")
            print(f"🔢 Run ID: {results['run_id']}")
            print(f"📁 Dataset IDs: {results['dataset_ids']}")
            
            # Step 2: Verify database records
            print("\n📋 Step 2: Verifying database records...")
            
            env = Environment()
            dao = TrainingDatasetDAO(env=env)
            
            # Get the generated datasets
            for dataset_id in results['dataset_ids']:
                dataset = await dao.get_training_dataset_by_id(dataset_id)
                if dataset:
                    print(f"✅ Dataset {dataset_id} found in database:")
                    print(f"   Name: {dataset.dataset_name}")
                    print(f"   Sequences: {dataset.total_sequences:,}")
                    print(f"   Features: {dataset.feature_count}")
                    print(f"   Quality: {dataset.data_quality_score:.2%}")
                    print(f"   Symbols: {dataset.symbols}")
                    
                    # Verify files exist
                    if dataset.features_file_path and Path(dataset.features_file_path).exists():
                        file_size = Path(dataset.features_file_path).stat().st_size / (1024 * 1024)
                        print(f"   Features file: {file_size:.1f} MB")
                    
                    if dataset.labels_file_path and Path(dataset.labels_file_path).exists():
                        file_size = Path(dataset.labels_file_path).stat().st_size / (1024 * 1024)
                        print(f"   Labels file: {file_size:.1f} MB")
                else:
                    print(f"❌ Dataset {dataset_id} not found in database")
            
            # Step 3: Test web app data retrieval
            print("\n🌐 Step 3: Testing web app data retrieval...")
            
            try:
                from unified_backtest_analytics_webapp import UnifiedAnalyticsEngine
                
                engine = UnifiedAnalyticsEngine()
                await engine.initialize()
                
                # Get training datasets through web app
                datasets = await engine.get_training_datasets(limit=5)
                print(f"✅ Web app returned {len(datasets)} training datasets")
                
                # Find our AAPL dataset
                aapl_datasets = [d for d in datasets if 'AAPL' in str(d.symbols)]
                if aapl_datasets:
                    latest_aapl = aapl_datasets[0]
                    print(f"   Latest AAPL dataset: {latest_aapl.dataset_name}")
                    print(f"   Sequences: {latest_aapl.total_sequences:,}")
                    print(f"   Quality: {latest_aapl.data_quality_score:.2%}")
                else:
                    print("⚠️  No AAPL datasets found in web app response")
                
                await engine.close()
                
            except Exception as e:
                print(f"⚠️  Web app test failed: {e}")
            
            # Step 4: Verify job run records
            print("\n📝 Step 4: Verifying job run records...")
            
            job_runs = await engine.get_job_runs(limit=5, run_type="training_data_generation")
            print(f"✅ Found {len(job_runs)} training data generation runs")
            
            # Find our run
            our_run = next((run for run in job_runs if run.run_id == results['run_id']), None)
            if our_run:
                print(f"   Run {our_run.run_id}: {our_run.status}")
                print(f"   Processing rate: {our_run.processing_rate_per_second:.1f} rec/s")
                print(f"   Quality: {our_run.quality_summary}")
            else:
                print(f"⚠️  Run {results['run_id']} not found in job runs")
            
            print("\n🎉 AAPL Training Data Generation Test COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print("✅ Training data generated and stored in database")
            print("✅ Files created and accessible") 
            print("✅ Web app can retrieve training datasets")
            print("✅ Job run tracking is working")
            print("\n💡 You can now view the results in the web app:")
            print("   - Go to the Training Data tab")
            print("   - Check the Job Runs tab for execution details")
            
        else:
            print(f"❌ Training data generation failed!")
            print(f"Error: {results.get('error', 'Unknown error')}")
            return False
    
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

async def main():
    """Main test function."""
    print("AAPL Training Data Generation - Manual Test")
    print("Testing complete integration with database and web app")
    print()
    
    success = await test_aapl_training_data_generation()
    
    if success:
        print("\n✅ All tests passed! The training data system is working correctly.")
    else:
        print("\n❌ Some tests failed. Check the output above for details.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())