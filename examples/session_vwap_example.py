#!/usr/bin/env python3
"""
Example usage of Session VWAP indicators for smart money analysis.

This script demonstrates how to use session-based VWAP indicators to identify
institutional activity during key market periods: US open, US close, and London close.
"""

import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
from signals.enhanced_indicators import (
    SessionVWAPIndicator,
    calculate_all_technical_indicators
)


def create_realistic_intraday_data():
    """Create realistic intraday data with multiple sessions."""
    np.random.seed(42)  # For reproducible results
    
    # Create a full trading day (US Eastern Time)
    et_tz = pytz.timezone('US/Eastern')
    
    # Start from pre-market (7:00 AM ET) to after-hours (8:00 PM ET)
    start_time = et_tz.localize(datetime(2024, 8, 19, 7, 0))  # Monday
    
    # Generate 1-minute bars for 13 hours (780 minutes)
    timestamps = pd.date_range(start_time, periods=780, freq='1min')
    
    data = []
    base_price = 150.0
    
    for i, ts in enumerate(timestamps):
        hour = ts.hour
        minute = ts.minute
        
        # Simulate different volume patterns during different sessions
        if 7 <= hour < 9:  # Pre-market
            volume_multiplier = 0.3
            volatility = 0.2
        elif 9 <= hour < 10:  # US Open (high activity)
            volume_multiplier = 2.5
            volatility = 0.8
        elif 10 <= hour < 15:  # Regular trading
            volume_multiplier = 1.0
            volatility = 0.4
        elif 15 <= hour < 16:  # Pre-close
            volume_multiplier = 1.5
            volatility = 0.6
        elif hour == 16 and minute < 30:  # US Close (high activity)
            volume_multiplier = 2.0
            volatility = 0.7
        else:  # After hours
            volume_multiplier = 0.4
            volatility = 0.3
        
        # Generate price movement
        price_change = np.random.normal(0, volatility * 0.1)
        open_price = base_price + price_change
        
        # Generate realistic OHLC
        intrabar_volatility = volatility * 0.05
        high = open_price + abs(np.random.normal(0, intrabar_volatility))
        low = open_price - abs(np.random.normal(0, intrabar_volatility))
        close = low + (high - low) * np.random.random()
        
        # Generate volume
        base_volume = 1500
        volume = int(base_volume * volume_multiplier * (1 + np.random.normal(0, 0.3)))
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


def demonstrate_session_vwap_analysis():
    """Demonstrate session VWAP analysis for smart money identification."""
    print("=" * 70)
    print("SESSION VWAP INDICATORS - SMART MONEY ANALYSIS")
    print("=" * 70)
    
    # Create realistic intraday data
    data = create_realistic_intraday_data()
    print(f"Generated {len(data)} 1-minute bars of intraday data")
    print(f"Time range: {data.index[0]} to {data.index[-1]}")
    print(f"Timezone: {data.index[0].tzinfo}")
    
    # Analyze each session type with both 30 and 60-minute windows
    sessions = [
        ('us_open', 'US Market Open', '9:30 AM ET'),
        ('us_close', 'US Market Close', '4:00 PM ET'),
        ('london_close', 'London Market Close', '4:30 PM GMT')
    ]
    
    durations = [30, 60]
    
    print("\n" + "=" * 70)
    print("SESSION VWAP ANALYSIS RESULTS")
    print("=" * 70)
    
    for session_type, session_name, session_time in sessions:
        print(f"\n🕐 {session_name} ({session_time})")
        print("-" * 50)
        
        for duration in durations:
            print(f"\n📊 {duration}-Minute Window Analysis:")
            
            # Calculate session VWAP
            indicator = SessionVWAPIndicator(
                session_type=session_type,
                duration_minutes=duration
            )
            result = indicator.calculate(data)
            
            if result['status'] == 'valid':
                # Display key metrics
                print(f"   Session VWAP: ${result['session_vwap']:.2f}")
                print(f"   Current Price vs VWAP: {result['price_vs_session_vwap']*100:+.2f}%")
                print(f"   Volume Balance: {result['session_volume_balance']:+.3f}")
                print(f"   Total Session Volume: {result['total_session_volume']:,}")
                print(f"   Session Range: ${result['session_range']:.2f}")
                print(f"   VWAP Position in Range: {result['vwap_position_in_range']:.1%}")
                print(f"   Session Bars: {result['session_bar_count']}")
                
                # Smart money interpretation
                interpret_smart_money_signals(result, session_name, duration)
                
            else:
                print(f"   ❌ No data available (Status: {result['status']})")


def interpret_smart_money_signals(result, session_name, duration):
    """Interpret session VWAP results for smart money insights."""
    print(f"\n   💡 Smart Money Insights ({duration}min):")
    
    # Volume balance interpretation
    vol_balance = result['session_volume_balance']
    if vol_balance > 0.1:
        print(f"   • Strong buying pressure (volume balance: +{vol_balance:.2f})")
    elif vol_balance < -0.1:
        print(f"   • Strong selling pressure (volume balance: {vol_balance:.2f})")
    else:
        print(f"   • Balanced volume flow (volume balance: {vol_balance:.2f})")
    
    # VWAP position interpretation
    vwap_pos = result['vwap_position_in_range']
    if vwap_pos > 0.7:
        print(f"   • VWAP near session highs - institutional buying interest")
    elif vwap_pos < 0.3:
        print(f"   • VWAP near session lows - institutional selling interest")
    else:
        print(f"   • VWAP in middle of range - balanced institutional activity")
    
    # Volume concentration
    avg_vol = result['avg_volume_per_bar']
    if avg_vol > 2000:
        print(f"   • High volume concentration ({avg_vol:.0f}/bar) - significant institutional activity")
    elif avg_vol > 1000:
        print(f"   • Moderate volume concentration ({avg_vol:.0f}/bar) - normal institutional activity")
    else:
        print(f"   • Low volume concentration ({avg_vol:.0f}/bar) - limited institutional activity")
    
    # Price vs VWAP deviation
    price_vs_vwap = result['price_vs_session_vwap']
    if abs(price_vs_vwap) > 0.005:  # 0.5%
        direction = "above" if price_vs_vwap > 0 else "below"
        print(f"   • Price {abs(price_vs_vwap)*100:.1f}% {direction} session VWAP - potential reversion setup")


def demonstrate_comprehensive_session_analysis():
    """Demonstrate comprehensive analysis using all session VWAPs."""
    print("\n" + "=" * 70)
    print("COMPREHENSIVE SESSION ANALYSIS")
    print("=" * 70)
    
    data = create_realistic_intraday_data()
    
    # Calculate all technical indicators including session VWAPs
    results = calculate_all_technical_indicators(data)
    
    # Filter session VWAP results
    session_results = {k: v for k, v in results.items() 
                      if 'SessionVWAP' in k and 'status' not in k}
    
    print(f"\nFound {len(session_results)} session VWAP metrics")
    print("\nSession VWAP Summary:")
    print("-" * 40)
    
    # Group by session type for comparison
    sessions = {}
    for key, value in session_results.items():
        if 'session_vwap' in key and not pd.isna(value):
            # Extract session info from key
            parts = key.split('_')
            if len(parts) >= 4:
                session = f"{parts[1]}_{parts[2]}"
                duration = parts[3].replace('min', '')
                
                if session not in sessions:
                    sessions[session] = {}
                sessions[session][f'{duration}min_vwap'] = value
    
    # Display session comparison
    for session, data_dict in sessions.items():
        print(f"\n{session.replace('_', ' ').title()}:")
        for duration, vwap in data_dict.items():
            print(f"  {duration}: ${vwap:.2f}")
    
    print("\n" + "=" * 70)
    print("KEY INSIGHTS FOR SMART MONEY TRADING:")
    print("=" * 70)
    
    print("""
📈 INSTITUTIONAL ACTIVITY INDICATORS:
• High volume concentration during session windows indicates smart money participation
• VWAP position in session range shows institutional bias (buying vs selling)
• Volume balance reveals directional pressure from large players
• Cross-session VWAP comparison helps identify institutional rotation

🎯 TRADING APPLICATIONS:
• Use session VWAPs as dynamic support/resistance levels
• Monitor volume balance for confirmation of institutional moves
• Look for price rejections from session VWAP levels
• Compare multiple session VWAPs to identify trend changes

⚠️  RISK MANAGEMENT:
• Avoid trading against strong institutional flow (high volume balance)
• Use session VWAP deviations for position sizing
• Monitor session volume concentration for liquidity assessment
• Combine with other indicators for confirmation
    """)


def main():
    """Run all session VWAP demonstrations."""
    print("Session VWAP Indicators - Smart Money Analysis Example")
    print("This example demonstrates session-based VWAP analysis for institutional activity detection.")
    
    try:
        demonstrate_session_vwap_analysis()
        demonstrate_comprehensive_session_analysis()
        
        print("\n" + "=" * 70)
        print("EXAMPLE COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("Make sure all required dependencies are installed (pandas, numpy, pytz)")


if __name__ == "__main__":
    main()