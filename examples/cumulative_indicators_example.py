#!/usr/bin/env python3
"""
Example usage of Cumulative Volume and Cumulative Dollars indicators.

This script demonstrates how to use the new cumulative indicators for
technical analysis of market data.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from signals.enhanced_indicators import (
    CumulativeVolumeIndicator,
    CumulativeDollarsIndicator,
    calculate_all_technical_indicators
)


def create_sample_data():
    """Create sample OHLCV data for demonstration."""
    np.random.seed(42)  # For reproducible results
    
    # Create timestamps for one trading day
    start_time = datetime(2024, 8, 17, 9, 30)  # 9:30 AM
    timestamps = [start_time + timedelta(minutes=i) for i in range(0, 390, 5)]  # 5-minute bars
    
    # Generate realistic OHLCV data
    base_price = 150.0
    data = []
    
    for i, ts in enumerate(timestamps):
        # Simulate price movement
        price_change = np.random.normal(0, 0.5)
        open_price = base_price + price_change
        
        # Generate realistic OHLC
        high = open_price + abs(np.random.normal(0, 0.3))
        low = open_price - abs(np.random.normal(0, 0.3))
        close = low + (high - low) * np.random.random()
        
        # Generate volume (higher volume during market open/close)
        hour = ts.hour
        if 9 <= hour <= 10 or 15 <= hour <= 16:  # High volume periods
            volume = int(np.random.normal(2000, 500))
        else:
            volume = int(np.random.normal(1000, 300))
        volume = max(100, volume)  # Ensure positive volume
        
        data.append({
            'timestamp': ts,
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': volume
        })
        
        base_price = close  # Next bar starts where this one ended
    
    df = pd.DataFrame(data)
    df.index = pd.to_datetime(df['timestamp'])
    return df


def demonstrate_cumulative_volume():
    """Demonstrate cumulative volume indicator usage."""
    print("=" * 60)
    print("CUMULATIVE VOLUME INDICATOR DEMONSTRATION")
    print("=" * 60)
    
    data = create_sample_data()
    
    # Test different reset intervals
    reset_intervals = ['never', 'daily', 'session']
    
    for interval in reset_intervals:
        print(f"\n--- Cumulative Volume ({interval} reset) ---")
        
        indicator = CumulativeVolumeIndicator(reset_interval=interval)
        result = indicator.calculate(data)
        
        if result['status'] == 'valid':
            print(f"Cumulative Volume: {result['cumulative_volume']:,}")
            print(f"Total Session Volume: {result['total_session_volume']:,}")
            print(f"Volume Balance: {result['volume_balance']:.3f}")
            print(f"Positive Flow Ratio: {result['positive_flow_ratio']:.3f}")
            print(f"Volume Acceleration: {result['volume_acceleration']:.3f}")
            print(f"Volume Percentile: {result['volume_percentile']:.3f}")
        else:
            print(f"Error: {result['status']}")


def demonstrate_cumulative_dollars():
    """Demonstrate cumulative dollars indicator usage."""
    print("\n" + "=" * 60)
    print("CUMULATIVE DOLLARS INDICATOR DEMONSTRATION")
    print("=" * 60)
    
    data = create_sample_data()
    
    # Test different price methods
    price_methods = ['typical', 'close', 'vwap']
    
    for method in price_methods:
        print(f"\n--- Cumulative Dollars (daily reset, {method} price) ---")
        
        indicator = CumulativeDollarsIndicator(
            reset_interval='daily', 
            price_method=method
        )
        result = indicator.calculate(data)
        
        if result['status'] == 'valid':
            print(f"Cumulative Dollars: ${result['cumulative_dollars']:,.2f}")
            print(f"Total Session Dollars: ${result['total_session_dollars']:,.2f}")
            print(f"Dollar Balance: {result['dollar_balance']:.3f}")
            print(f"Avg Dollar per Share: ${result['avg_dollar_per_share']:.2f}")
            print(f"Liquidity Score: {result['liquidity_score']:.3f}")
            print(f"Dollar Acceleration: {result['dollar_acceleration']:.3f}")
        else:
            print(f"Error: {result['status']}")


def demonstrate_comprehensive_analysis():
    """Demonstrate comprehensive technical analysis including cumulative indicators."""
    print("\n" + "=" * 60)
    print("COMPREHENSIVE TECHNICAL ANALYSIS")
    print("=" * 60)
    
    data = create_sample_data()
    
    # Calculate all technical indicators
    results = calculate_all_technical_indicators(data)
    
    # Filter and display cumulative indicator results
    print("\nCumulative Indicators Summary:")
    print("-" * 40)
    
    for key, value in results.items():
        if 'Cum' in key and 'status' not in key:
            # Format the output nicely
            if 'value' in key or 'cumulative' in key:
                if 'Dollar' in key:
                    print(f"{key}: ${value:,.2f}")
                else:
                    print(f"{key}: {value:,}")
            elif any(word in key for word in ['ratio', 'balance', 'percentile', 'score', 'acceleration']):
                print(f"{key}: {value:.3f}")
    
    # Display some key insights
    print("\nKey Insights:")
    print("-" * 20)
    
    cum_vol = results.get('CumVolume_daily_cumulative_volume', 0)
    cum_dollars = results.get('CumDollars_daily_typical_cumulative_dollars', 0)
    vol_balance = results.get('CumVolume_daily_volume_balance', 0)
    liquidity = results.get('CumDollars_daily_typical_liquidity_score', 0)
    
    print(f"• Total Volume Traded: {cum_vol:,} shares")
    print(f"• Total Dollar Volume: ${cum_dollars:,.2f}")
    
    if vol_balance > 0.1:
        print(f"• Volume Flow: Bullish (balance: {vol_balance:.3f})")
    elif vol_balance < -0.1:
        print(f"• Volume Flow: Bearish (balance: {vol_balance:.3f})")
    else:
        print(f"• Volume Flow: Neutral (balance: {vol_balance:.3f})")
    
    print(f"• Liquidity Score: {liquidity:.3f}")


def main():
    """Run all demonstrations."""
    print("Cumulative Technical Indicators Example")
    print("This example demonstrates the new cumulative volume and dollar indicators.")
    
    demonstrate_cumulative_volume()
    demonstrate_cumulative_dollars()
    demonstrate_comprehensive_analysis()
    
    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("These indicators can be used for:")
    print("• Identifying volume patterns and trends")
    print("• Measuring liquidity and market participation")
    print("• Analyzing money flow and institutional activity")
    print("• Building trading signals based on volume/price relationships")
    print("=" * 60)


if __name__ == "__main__":
    main()