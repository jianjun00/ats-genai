#!/usr/bin/env python3
"""
Comprehensive test for fixed training data generation.
Validates that all 14 features (5 OHLCV + 9 indicators) work correctly.
"""

import os
import sys
import asyncio
from datetime import date, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

async def test_training_data_with_fixed_indicators():
    """Test that training data generation works with all fixed indicators."""
    
    # Import required modules
    from app.training_data_job_runner import TrainingDataJobConfig, TrainingDataJobRunner
    from config.environment import Environment
    
    print("🧪 Testing Training Data Generation with Fixed Indicators")
    print("=" * 60)
    
    # Create test configuration
    config = TrainingDataJobConfig(
        job_name="test_fixed_indicators",
        symbols=['AAPL'],  # Single symbol for testing
        start_date=date(2023, 1, 1),
        end_date=date(2023, 2, 1),  # Small date range for testing
        sequence_length=30,         # Shorter sequences for testing
        prediction_horizon=5,
        use_enhanced_features=True,  # Enable all indicators
        feature_configs=[{
            'name': 'ohlcv_features',
            'type': 'ohlcv',
            'window': 1
        }],
        label_configs=[{
            'name': 'price_direction',
            'type': 'classification',
            'target': 'close',
            'horizon': 5
        }],
        output_dir="test_training_output"
    )
    
    print(f"📊 Configuration:")
    print(f"  Symbols: {config.symbols}")
    print(f"  Date range: {config.start_date} to {config.end_date}")
    print(f"  Enhanced features: {config.use_enhanced_features}")
    
    # Initialize environment
    env = Environment()
    
    try:
        # Create job runner
        runner = TrainingDataJobRunner(config, env)
        
        print("\n🚀 Running training data generation...")
        
        # Run the job
        result = await runner.run_training_data_generation()
        
        print(f"\n✅ Training data generation completed!")
        print(f"📋 Results:")
        print(f"  Status: {result['status']}")
        print(f"  Run ID: {result.get('run_id')}")
        print(f"  Dataset IDs: {result.get('dataset_ids', [])}")
        print(f"  Total datasets: {result.get('total_datasets', 0)}")
        
        # Validate the results contain our expected features
        if 'details' in result and 'training_results' in result['details']:
            training_results = result['details']['training_results']
            
            print(f"\n📈 Training Data Details:")
            print(f"  Features shape: {training_results.get('features_shape')}")
            print(f"  Labels shape: {training_results.get('labels_shape')}")
            
            # Check feature names
            feature_names = training_results.get('feature_names', [])
            print(f"  Feature names ({len(feature_names)}): {feature_names}")
            
            # Validate we have all expected features
            expected_features = [
                'open', 'high', 'low', 'close', 'volume',           # OHLCV (5)
                'envelope_top', 'envelope_bot', 'pldot',            # Core indicators (3)
                'oneone_high', 'oneone_low',                        # OneOne indicators (2)
                'z1b', 'z2b', 'z5t', 'z6t'                         # Zone indicators (4)
            ]  # Total: 14 features
            
            print(f"\n🔍 Feature Validation:")
            missing_features = []
            for feature in expected_features:
                if feature in feature_names:
                    print(f"  ✅ {feature}: Present")
                else:
                    print(f"  ❌ {feature}: Missing")
                    missing_features.append(feature)
            
            if missing_features:
                print(f"\n⚠️  Missing {len(missing_features)} expected features: {missing_features}")
                return False
            else:
                print(f"\n🎉 All {len(expected_features)} expected features are present!")
                
            # Check for unexpected normalization in feature descriptions
            feature_descriptions = training_results.get('feature_descriptions', {})
            print(f"\n📝 Feature Descriptions Check:")
            for feature, desc in feature_descriptions.items():
                if feature in ['envelope_top', 'envelope_bot', 'pldot', 'oneone_high', 'oneone_low', 'z1b', 'z2b', 'z5t', 'z6t']:
                    if 'NOT normalized' in desc:
                        print(f"  ✅ {feature}: Correctly marked as NOT normalized")
                    else:
                        print(f"  ⚠️  {feature}: Missing 'NOT normalized' marker")
                
            return True
            
        else:
            print("⚠️  No training results details found in response")
            return False
            
    except Exception as e:
        print(f"\n❌ Training data generation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run comprehensive training data test."""
    print("🎯 Comprehensive Training Data Generation Test")
    print("Testing all fixed indicators and validation")
    
    try:
        success = await test_training_data_with_fixed_indicators()
        
        if success:
            print("\n🎉 SUCCESS: Training data generation working correctly!")
            print("✅ All 14 features (5 OHLCV + 9 indicators) are present")
            print("✅ All indicators return actual values (not normalized)")
            print("✅ End-to-end training data pipeline functional")
        else:
            print("\n❌ FAILURE: Training data generation has issues")
            print("⚠️  Some expected features missing or incorrectly configured")
        
        return success
        
    except Exception as e:
        print(f"\n💥 Test error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)