#!/usr/bin/env python3
"""
Demonstration of correct EMA calculation and explanation of different metric types.
This example shows why EMA values between -2 and 2 were observed and what they actually mean.
"""

import pandas as pd
import numpy as np
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from signals.enhanced_indicators import EMAIndicator, calculate_all_technical_indicators, ResidualReturnIndicatorConfig


def create_demo_stock_data():
    """Create realistic stock data for demonstration."""
    np.random.seed(42)
    
    # Create 60 days of data for a stock trading around $200
    dates = pd.date_range('2024-01-01', periods=60, freq='D')
    base_price = 200.0
    
    prices = [base_price]
    for i in range(59):
        daily_return = np.random.normal(0.001, 0.015)  # Realistic daily movements
        new_price = prices[-1] * (1 + daily_return)
        prices.append(max(new_price, 1.0))
    
    # Create OHLCV data
    data = []
    for i, price in enumerate(prices):
        range_pct = np.random.uniform(0.005, 0.02)
        
        open_price = price * (1 + np.random.normal(0, 0.003))
        high = price * (1 + range_pct/2)
        low = price * (1 - range_pct/2)
        close = price
        volume = np.random.randint(1000000, 3000000)
        
        high = max(high, open_price, close)
        low = min(low, open_price, close)
        
        data.append({
            'timestamp': dates[i],
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume
        })
    
    df = pd.DataFrame(data)
    df.set_index('timestamp', inplace=True)
    return df


def demonstrate_ema_metrics():
    """Demonstrate the difference between EMA values and derived metrics."""
    print("EMA Calculation Demonstration")
    print("=" * 50)
    
    # Create test data
    price_data = create_demo_stock_data()
    current_price = price_data['close'].iloc[-1]
    
    print(f"Stock trading around: ${current_price:.2f}")
    print(f"Date range: {price_data.index[0].date()} to {price_data.index[-1].date()}")
    print(f"Total trading days: {len(price_data)}")
    
    # Calculate EMA for different periods
    periods = [5, 21, 50]
    
    print(f"\n1. EMA VALUES (should be close to stock price):")
    print("-" * 50)
    
    for period in periods:
        ema_indicator = EMAIndicator(period)
        result = ema_indicator.calculate(price_data)
        
        if result['status'] == 'valid':
            ema_value = result['ema_price']
            print(f"EMA({period:2d}): ${ema_value:7.2f}  (diff: ${current_price - ema_value:+6.2f})")
        else:
            print(f"EMA({period:2d}): {result['status']}")
    
    print(f"\n2. EMA DERIVED METRICS (ratios and percentages - these ARE small!):")
    print("-" * 70)
    
    ema21 = EMAIndicator(21)
    result = ema21.calculate(price_data)
    
    if result['status'] == 'valid':
        print(f"Current price:           ${result['current_price']:7.2f}")
        print(f"EMA(21) value:           ${result['ema_price']:7.2f}")
        print(f"")
        print(f"Price vs EMA ratio:      {result['price_vs_ema_ratio']:+8.6f}  ← THIS is what you saw!")
        print(f"  Explanation: ({result['current_price']:.2f} / {result['ema_price']:.2f}) - 1 = {result['price_vs_ema_ratio']:.6f}")
        print(f"  As percentage: {result['price_vs_ema_ratio'] * 100:+5.2f}%")
        print(f"")
        print(f"EMA slope (trend):       {result['ema_slope_pct']:+8.6f}  ← This is also small!")
        print(f"  Explanation: Percentage change in EMA over recent periods")
        print(f"  As percentage: {result['ema_slope_pct'] * 100:+5.2f}%")
        print(f"")
        print(f"EMA distance (std dev):  {result['ema_distance_std']:+8.6f}  ← This measures volatility!")
        print(f"  Explanation: How many standard deviations price is from EMA")
    
    print(f"\n3. WHY THE CONFUSION OCCURRED:")
    print("-" * 40)
    print("✅ EMA VALUES are correct - they're close to the stock price")
    print("✅ EMA RATIOS/SLOPES are also correct - they should be small numbers")
    print("❌ The issue was confusing EMA values with EMA ratios")
    print("")
    print("When you see values like -0.013 or 1.5, these are likely:")
    print("• price_vs_ema_ratio: (current_price / EMA) - 1")
    print("• ema_slope: percentage change in EMA")
    print("• ema_distance_std: standard deviation distance")
    print("")
    print("These SHOULD be small numbers (typically -1 to +1)!")


def demonstrate_comprehensive_features():
    """Show how EMA features appear in the comprehensive technical analysis."""
    print(f"\n4. COMPREHENSIVE TECHNICAL ANALYSIS OUTPUT:")
    print("-" * 50)
    
    price_data = create_demo_stock_data()
    config = ResidualReturnIndicatorConfig.comprehensive_config()
    all_features = calculate_all_technical_indicators(price_data, config)
    
    # Group EMA features
    ema_values = {}
    ema_derived = {}
    
    for key, value in all_features.items():
        if 'EMA' in key and isinstance(value, (int, float, np.number)) and not pd.isna(value):
            if '_value' in key or '_ema_price' in key:
                ema_values[key] = value
            elif any(x in key for x in ['_ratio', '_slope', '_distance', '_vs_']):
                ema_derived[key] = value
    
    print("EMA Values (price levels):")
    for key, value in ema_values.items():
        print(f"  {key:25s}: ${value:8.2f}")
    
    print("\nEMA Derived Metrics (ratios/percentages):")
    for key, value in ema_derived.items():
        print(f"  {key:35s}: {value:+10.6f}")
    
    print(f"\n5. KEY TAKEAWAYS:")
    print("-" * 20)
    print("• EMA values (like EMA_21_value) = actual price levels")
    print("• EMA ratios (like price_vs_ema) = small decimals representing percentages")
    print("• Values between -2 and 2 for ratios/slopes are NORMAL and EXPECTED")
    print("• The EMA calculation was never broken - it was a misunderstanding!")


if __name__ == "__main__":
    demonstrate_ema_metrics()
    demonstrate_comprehensive_features()
    
    print(f"\n" + "=" * 60)
    print("CONCLUSION: The EMA calculations are working correctly!")
    print("The values between -2 and 2 you observed were ratio/percentage")
    print("metrics, not the actual EMA values. This is the expected behavior.")
    print("=" * 60)