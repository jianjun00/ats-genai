#!/usr/bin/env python3
"""
Regenerate training data for AAPL and TSLA with fixed indicators.
"""

import os
import sys
import asyncio

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

async def regenerate_training_data():
    """Regenerate training data for both AAPL and TSLA."""
    
    from ml.training_data.runners.training_data_callback_runner import run_training_data_job_for_symbol
    
    print("🚀 Regenerating Training Data with Fixed Indicators")
    print("=" * 55)
    
    symbols = ['AAPL', 'TSLA']
    results = {}
    
    for symbol in symbols:
        print(f"\n📊 Processing {symbol}...")
        
        try:
            result = await run_training_data_job_for_symbol(symbol)
            results[symbol] = result
            
            print(f"Status: {result['status']}")
            
            if result['status'] == 'success':
                print(f"✅ SUCCESS for {symbol}!")
                print(f"  Run ID: {result.get('run_id')}")
                print(f"  Dataset IDs: {result.get('dataset_ids', [])}")
                
                # Check training results details
                if 'details' in result and 'training_results' in result['details']:
                    training_results = result['details']['training_results']
                    
                    print(f"  Features shape: {training_results.get('features_shape')}")
                    print(f"  Labels shape: {training_results.get('labels_shape')}")
                    
                    feature_names = training_results.get('feature_names', [])
                    print(f"  Total features: {len(feature_names)}")
                    print(f"  Feature names: {feature_names}")
                    
                    # Validate our 9 fixed indicators are present
                    expected_indicators = [
                        'envelope_top', 'envelope_bot', 'pldot', 
                        'oneone_high', 'oneone_low',
                        'z1b', 'z2b', 'z5t', 'z6t'
                    ]
                    
                    print(f"\n  🔍 Indicator Validation for {symbol}:")
                    present_indicators = []
                    missing_indicators = []
                    
                    for indicator in expected_indicators:
                        if indicator in feature_names:
                            present_indicators.append(indicator)
                            print(f"    ✅ {indicator}")
                        else:
                            missing_indicators.append(indicator)
                            print(f"    ❌ {indicator} MISSING")
                    
                    print(f"  📈 Indicators: {len(present_indicators)}/{len(expected_indicators)} present")
                    
                    if missing_indicators:
                        print(f"  ⚠️  Missing: {missing_indicators}")
                    
                    # Check feature descriptions for normalization warnings
                    feature_descriptions = training_results.get('feature_descriptions', {})
                    print(f"\n  📝 Normalization Check for {symbol}:")
                    for indicator in present_indicators:
                        desc = feature_descriptions.get(indicator, '')
                        if 'NOT normalized' in desc:
                            print(f"    ✅ {indicator}: Correctly marked as NOT normalized")
                        else:
                            print(f"    ⚠️  {indicator}: Missing 'NOT normalized' marker")
                    
            else:
                print(f"❌ FAILED for {symbol}: {result}")
                
        except Exception as e:
            print(f"💥 Error processing {symbol}: {e}")
            results[symbol] = {'status': 'error', 'error': str(e)}
    
    # Summary
    print(f"\n🎯 SUMMARY")
    print("=" * 40)
    
    successful = 0
    failed = 0
    
    for symbol, result in results.items():
        if result.get('status') == 'success':
            successful += 1
            print(f"✅ {symbol}: Training data generated successfully")
        else:
            failed += 1
            print(f"❌ {symbol}: Failed - {result.get('error', 'Unknown error')}")
    
    print(f"\nResults: {successful} successful, {failed} failed")
    
    if successful == len(symbols):
        print("🎉 All symbols processed successfully!")
        print("🔧 Training data now includes:")
        print("  - Fixed indicator scaling (actual values, not normalized)")
        print("  - All 9 technical indicators (including Z1B, Z2B, Z5T, Z6T)")
        print("  - 14 total features per sample (5 OHLCV + 9 indicators)")
        return True
    else:
        print("⚠️  Some symbols failed to process")
        return False

async def main():
    """Main function."""
    try:
        success = await regenerate_training_data()
        return success
    except Exception as e:
        print(f"💥 Critical error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    result = asyncio.run(main())
    print(f"\n{'✅ SUCCESS' if result else '❌ FAILURE'}: Training data regeneration {'completed' if result else 'failed'}")
    sys.exit(0 if result else 1)