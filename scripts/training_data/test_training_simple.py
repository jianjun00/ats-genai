#!/usr/bin/env python3
"""
Simple test for training data generation with fixed indicators.
"""

import os
import sys
import asyncio

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

async def main():
    """Run training data generation test."""
    
    from ml.training_data.runners.training_data_callback_runner import run_training_data_job_for_symbol
    
    print("🚀 Testing Training Data Generation")
    print("=" * 40)
    
    try:
        # Run training data generation for AAPL
        print("Generating training data for AAPL...")
        result = await run_training_data_job_for_symbol('AAPL')
        
        print(f"\n📋 Results:")
        print(f"Status: {result['status']}")
        
        if result['status'] == 'success':
            print(f"✅ SUCCESS: Training data generated!")
            print(f"Run ID: {result.get('run_id')}")
            print(f"Dataset IDs: {result.get('dataset_ids', [])}")
            
            # Check if details are available
            if 'details' in result:
                details = result['details']
                if 'training_results' in details:
                    training_results = details['training_results'] 
                    print(f"Features shape: {training_results.get('features_shape')}")
                    print(f"Feature names: {training_results.get('feature_names')}")
                    
                    # Check for our expected indicators
                    feature_names = training_results.get('feature_names', [])
                    expected_indicators = ['envelope_top', 'envelope_bot', 'pldot', 'oneone_high', 'oneone_low', 'z1b', 'z2b', 'z5t', 'z6t']
                    
                    print(f"\n🔍 Indicator Check:")
                    for indicator in expected_indicators:
                        if indicator in feature_names:
                            print(f"  ✅ {indicator}")
                        else:
                            print(f"  ❌ {indicator} missing")
        else:
            print(f"❌ FAILED: {result}")
            
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())