#!/usr/bin/env python3
"""
Test to verify indicator scaling fixes in training data generation.
Ensures all indicators return actual values, not normalized ones.
"""

import os
import sys
import numpy as np
import pandas as pd

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_technical_indicators_not_normalized():
    """Test that TechnicalIndicators class returns actual values, not normalized."""
    
    # Import the fixed TechnicalIndicators class
    from app.training_data_job_runner import TechnicalIndicators
    
    # Create sample realistic stock price data (around $100-150 range)
    np.random.seed(42)
    n_periods = 100
    base_price = 120.0
    
    # Generate realistic OHLCV data
    close_prices = []
    high_prices = []
    low_prices = []
    
    current_price = base_price
    for _ in range(n_periods):
        # Random walk with realistic daily moves (-2% to +2%)
        daily_change = np.random.normal(0, 0.01)  # 1% daily volatility
        new_price = current_price * (1 + daily_change)
        
        # Generate OHLC around the close
        daily_range = new_price * 0.02  # 2% daily range
        high = new_price + np.random.uniform(0, daily_range)
        low = new_price - np.random.uniform(0, daily_range)
        
        close_prices.append(new_price)
        high_prices.append(high)
        low_prices.append(max(low, 0.01))  # Ensure positive prices
        
        current_price = new_price
    
    close = np.array(close_prices)
    high = np.array(high_prices)  
    low = np.array(low_prices)
    
    print(f"🧪 Testing with price range: {close.min():.2f} - {close.max():.2f}")
    
    # Initialize TechnicalIndicators
    indicators = TechnicalIndicators()
    
    # Test all indicators
    test_results = {}
    
    print("\n📊 Testing indicators...")
    
    # EnvelopeTop - should return actual price levels
    envelope_top = indicators.calculate_envelope_top(high, low, close)
    envelope_top_values = envelope_top[~np.isnan(envelope_top)]
    if len(envelope_top_values) > 0:
        etop_min, etop_max = envelope_top_values.min(), envelope_top_values.max()
        test_results['envelope_top'] = {
            'min': etop_min,
            'max': etop_max,
            'normalized': 0.0 <= etop_min <= 1.0 and 0.0 <= etop_max <= 1.0,
            'realistic': etop_min > 50 and etop_max > 50  # Should be in price range
        }
        print(f"✓ EnvelopeTop: {etop_min:.2f} - {etop_max:.2f} (normalized: {test_results['envelope_top']['normalized']})")
    
    # EnvelopeBot - should return actual price levels  
    envelope_bot = indicators.calculate_envelope_bot(high, low, close)
    envelope_bot_values = envelope_bot[~np.isnan(envelope_bot)]
    if len(envelope_bot_values) > 0:
        ebot_min, ebot_max = envelope_bot_values.min(), envelope_bot_values.max()
        test_results['envelope_bot'] = {
            'min': ebot_min,
            'max': ebot_max,
            'normalized': 0.0 <= ebot_min <= 1.0 and 0.0 <= ebot_max <= 1.0,
            'realistic': ebot_min > 50 and ebot_max > 50
        }
        print(f"✓ EnvelopeBot: {ebot_min:.2f} - {ebot_max:.2f} (normalized: {test_results['envelope_bot']['normalized']})")
    
    # PLDOT - should return actual momentum values (can be small but not 0-1 normalized)
    pldot = indicators.calculate_pldot(high, low, close)
    pldot_values = pldot[~np.isnan(pldot)]
    if len(pldot_values) > 0:
        pldot_min, pldot_max = pldot_values.min(), pldot_values.max()
        test_results['pldot'] = {
            'min': pldot_min,
            'max': pldot_max,
            'normalized': 0.0 <= pldot_min <= 1.0 and 0.0 <= pldot_max <= 1.0,
            'reasonable': abs(pldot_min) < 1000 and abs(pldot_max) < 1000  # Should be reasonable values
        }
        print(f"✓ PLDOT: {pldot_min:.4f} - {pldot_max:.4f} (normalized: {test_results['pldot']['normalized']})")
    
    # OneOneHigh - should return actual price levels
    oneone_high = indicators.calculate_oneone_high(high, low, close)
    oneone_high_values = oneone_high[~np.isnan(oneone_high)]
    if len(oneone_high_values) > 0:
        ooh_min, ooh_max = oneone_high_values.min(), oneone_high_values.max()
        test_results['oneone_high'] = {
            'min': ooh_min,
            'max': ooh_max,
            'normalized': 0.0 <= ooh_min <= 1.0 and 0.0 <= ooh_max <= 1.0,
            'realistic': ooh_min > 50 and ooh_max > 50
        }
        print(f"✓ OneOneHigh: {ooh_min:.2f} - {ooh_max:.2f} (normalized: {test_results['oneone_high']['normalized']})")
    
    # Test zone indicators  
    for indicator_name, calc_method in [
        ('z1b', 'calculate_z1b'),
        ('z2b', 'calculate_z2b'), 
        ('z5t', 'calculate_z5t'),
        ('z6t', 'calculate_z6t')
    ]:
        calc_func = getattr(indicators, calc_method)
        zone_values = calc_func(high, low, close)
        zone_values_clean = zone_values[~np.isnan(zone_values)]
        
        if len(zone_values_clean) > 0:
            zone_min, zone_max = zone_values_clean.min(), zone_values_clean.max()
            test_results[indicator_name] = {
                'min': zone_min,
                'max': zone_max,
                'normalized': 0.0 <= zone_min <= 1.0 and 0.0 <= zone_max <= 1.0,
                'realistic': zone_min > 10 and zone_max > 10  # Should be in reasonable price range
            }
            print(f"✓ {indicator_name.upper()}: {zone_min:.2f} - {zone_max:.2f} (normalized: {test_results[indicator_name]['normalized']})")
    
    # Summary
    print("\n🎯 Results Summary:")
    normalized_count = 0
    total_count = 0
    
    for indicator, results in test_results.items():
        total_count += 1
        if results['normalized']:
            normalized_count += 1
            print(f"❌ {indicator}: INCORRECTLY normalized between 0-1")
        else:
            print(f"✅ {indicator}: Correctly returns actual values")
    
    print(f"\n📈 Overall: {total_count - normalized_count}/{total_count} indicators return actual values")
    
    if normalized_count == 0:
        print("🎉 SUCCESS: All indicators return actual values, not normalized!")
        return True
    else:
        print(f"⚠️  FAILURE: {normalized_count} indicators are still normalized")
        return False

def main():
    """Run indicator scaling tests."""
    print("🚀 Testing Technical Indicator Scaling Fixes")
    print("=" * 50)
    
    try:
        success = test_technical_indicators_not_normalized()
        if success:
            print("\n✅ All tests passed! Indicator scaling is fixed.")
            sys.exit(0)
        else:
            print("\n❌ Some tests failed! Indicator scaling needs more work.")
            sys.exit(1)
    except Exception as e:
        print(f"\n💥 Test error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()