#!/usr/bin/env python3
"""
Test if the analytics service can now discover sequences for dataset 65
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_dataset_sequences():
    """Test if sequences are discovered for dataset 65"""
    try:
        from services.analytics_service import UnifiedAnalyticsService
        
        analytics = UnifiedAnalyticsService()
        
        print("Testing dataset 65 sequence discovery...")
        result = analytics.get_training_dataset_sequences(65)
        
        print("Result:")
        import json
        print(json.dumps(result, indent=2))
        
        return result
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = test_dataset_sequences()
    if result and 'sequences' in result:
        print(f"\n✅ Found {len(result['sequences'])} sequences!")
        for seq in result['sequences']:
            print(f"  - {seq}")
    else:
        print("\n❌ No sequences found or error occurred")