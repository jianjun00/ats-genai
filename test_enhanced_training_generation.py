#!/usr/bin/env python3
"""
Test enhanced training data generation with AAPL using run_dev infrastructure.
"""

import asyncio
from src.app.enhanced_training_data_generator import run_enhanced_training_data_job_for_symbol

async def test_enhanced_training_generation():
    """Test enhanced training data generation with comprehensive features."""
    
    print("🚀 Starting Enhanced Training Data Generation Test")
    print("=" * 60)
    print("Features: OHLC + etop, ebot, pldot, oneonedot for 21 bars")
    print()
    
    try:
        results = await run_enhanced_training_data_job_for_symbol('AAPL', days_back=90)
        
        print("✅ Enhanced Training Data Generation Results:")
        print(f"  Status: {results['status']}")
        
        if results['status'] == 'success':
            print(f"  Run ID: {results['run_id']}")
            print(f"  Dataset IDs: {results['dataset_ids']}")
            print(f"  Features Shape: {results['features_shape']}")
            print(f"  Labels Shape: {results['labels_shape']}")
            
            metadata = results.get('metadata', {})
            if 'feature_names' in metadata:
                print(f"  Feature Names: {metadata['feature_names']}")
            if 'technical_indicators' in metadata:
                indicators = list(metadata['technical_indicators'].keys())
                print(f"  Technical Indicators: {indicators}")
            
            print("\n🎉 Enhanced training data generation completed successfully!")
            print("Features include:")
            print("  • OHLC sequences for past 21 bars")
            print("  • Elliott Top (etop) reversal indicator")
            print("  • Elliott Bottom (ebot) reversal indicator") 
            print("  • Pivot Line Dot (pldot) momentum indicator")
            print("  • One-One-Dot (oneonedot) custom oscillator")
        else:
            print(f"  Error: {results.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    asyncio.run(test_enhanced_training_generation())