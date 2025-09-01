#!/usr/bin/env python3
"""
Test script for multi-scale training data generation.
"""

import sys
import asyncio
import logging

# Add src to Python path
sys.path.insert(0, 'src')

from app.training_data_job_runner import run_multi_scale_training_data_job_for_symbol


async def main():
    """Test multi-scale training data generation."""
    logging.basicConfig(level=logging.INFO)
    
    print("🚀 Testing Multi-Scale Training Data Generation")
    print("=" * 60)
    
    try:
        # Test basic multi-scale generation (no advanced features)
        print("\n📊 Generating basic multi-scale training data...")
        result = await run_multi_scale_training_data_job_for_symbol(
            symbol='AAPL',
            scales=['hourly', 'daily'],
            days_back=30,
            enable_all_features=False
        )
        
        if result['status'] == 'success':
            print(f"✅ Success!")
            training_results = result['results']['training_results']
            print(f"Features shape: {training_results['features_shape']}")
            print(f"Labels shape: {training_results['labels_shape']}")
            print(f"Dataset ID: {training_results['dataset_id']}")
            
            if 'multi_scale_metadata' in training_results:
                metadata = training_results['multi_scale_metadata']
                print(f"Scales: {metadata['scales']}")
                print(f"Total sequences: {metadata['total_sequences']}")
                print(f"Events enabled: {metadata['events_enabled']}")
                print(f"Agent features: {metadata['agent_features_enabled']}")
                print(f"LLM events: {metadata['llm_events_enabled']}")
            
        else:
            print(f"❌ Failed: {result.get('error', 'Unknown error')}")
    
    except Exception as e:
        print(f"❌ Script execution failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())