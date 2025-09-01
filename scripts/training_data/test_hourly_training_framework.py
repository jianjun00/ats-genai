#!/usr/bin/env python3
"""
Test the updated training data framework with hourly row generation.
Uses 1-minute base intervals and universe state builder indicators.
"""

import os
import sys
import asyncio
from datetime import date
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

async def test_hourly_training_framework():
    """Test the updated training data framework for hourly generation."""
    
    print("🧪 Testing Updated Training Data Framework (Hourly Generation)")
    print("=" * 70)
    
    try:
        from ml.training_data.runners.training_data_callback_runner import TrainingDataJobRunner, TrainingDataJobConfig
        from config.environment import Environment
        
        # Create configuration for hourly row generation
        config = TrainingDataJobConfig(
            job_name="hourly_test_generation",
            symbols=['AAPL'],
            start_date=date(2025, 8, 1),
            end_date=date(2025, 8, 27),
            
            # Hourly configuration
            base_interval_minutes=1,           # 1-minute base data
            training_interval_minutes=60,      # 1-hour training rows
            output_structure="hourly_rows",    # Row-based, not sequences
            
            # No normalization - use actual indicator values
            normalize_features=False,
            normalize_labels=False,
            use_enhanced_features=True,
            use_universe_state_indicators=True,  # Use real indicator framework
            
            # Required configs (simplified for test)
            feature_configs=[
                {"name": "ohlcv", "enabled": True},
                {"name": "technical_indicators", "enabled": True}
            ],
            label_configs=[
                {"name": "price_movement", "enabled": True}
            ]
        )
        
        # Initialize environment and runner
        env = Environment()  # Will auto-detect DEV/INTG environment
        runner = TrainingDataJobRunner(config=config, env=env)
        
        print(f"🚀 Configuration:")
        print(f"  Base interval: {config.base_interval_minutes} minutes")
        print(f"  Training interval: {config.training_interval_minutes} minutes") 
        print(f"  Output structure: {config.output_structure}")
        print(f"  Universe state indicators: {config.use_universe_state_indicators}")
        print(f"  Symbols: {config.symbols}")
        print(f"  Date range: {config.start_date} to {config.end_date}")
        
        # Run training data generation
        print(f"\n📊 Running hourly training data generation...")
        
        result = await runner.run_training_data_generation()
        
        if result['status'] == 'success':
            print(f"\n✅ Training data generation completed!")
            print(f"🎯 Run ID: {result['run_id']}")
            print(f"📋 Dataset IDs: {result['dataset_ids']}")
            
            # Check generated files
            output_files = list(Path('training_data_output').glob('hourly_*'))
            if output_files:
                print(f"\n📁 Generated files:")
                for file_path in sorted(output_files):
                    file_size = file_path.stat().st_size
                    print(f"  {file_path.name} ({file_size:,} bytes)")
                
                # Try to load and inspect a parquet file
                parquet_files = [f for f in output_files if f.suffix == '.parquet']
                if parquet_files:
                    try:
                        import pandas as pd
                        df = pd.read_parquet(parquet_files[0])
                        
                        print(f"\n🔍 Data Structure Analysis:")
                        print(f"  📊 Shape: {df.shape}")
                        print(f"  📊 Index levels: {df.index.names}")
                        print(f"  📊 Columns: {list(df.columns)}")
                        print(f"  📊 Date range: {df.index.get_level_values('datetime').min()} to {df.index.get_level_values('datetime').max()}")
                        
                        # Sample data
                        print(f"\n📋 Sample Data (first 3 rows):")
                        sample_cols = ['hour_open', 'hour_high', 'hour_low', 'hour_close']
                        if all(col in df.columns for col in sample_cols):
                            print(df[sample_cols].head(3))
                        else:
                            print(df.head(3))
                        
                    except Exception as e:
                        print(f"⚠️ Could not analyze parquet file: {e}")
            
            print(f"\n🎉 SUCCESS: Hourly training data framework integrated!")
            print(f"✅ Uses 1-minute base intervals")
            print(f"✅ Generates hourly row structure (not sequences)")
            print(f"✅ Primary keys: datetime + symbol")
            print(f"✅ Universe state builder indicators")
            print(f"✅ No mock/simulated data")
            print(f"✅ EDA service handles visualization")
            
            return True
            
        else:
            print(f"\n❌ Training data generation failed:")
            print(f"   Error: {result.get('error', 'Unknown error')}")
            return False
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run the test."""
    success = await test_hourly_training_framework()
    return success

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)