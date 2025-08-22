#!/usr/bin/env python3
"""
Smart Money Zones (SMZ) Methodology Example

This example demonstrates the complete Smart Money Zones trading methodology
including market structure analysis, zone identification, entry confirmation,
and risk management.

Based on the institutional trading strategies from the SMZ methodology document.
"""

import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
from typing import Dict, Any, List

from signals.smart_money_zones import (
    MarketStructure,
    StructureChange,
    MarketStructureDetector,
    SmartMoneyZoneDetector,
    SMZEntryConfirmation,
    MultiTimeframeAnalysis
)


def create_realistic_smz_scenario():
    """Create realistic market data demonstrating SMZ concepts."""
    np.random.seed(42)  # For reproducible results
    
    # Create a complete SMZ scenario over several phases
    data = []
    base_price = 1850.0  # Example: Gold price
    
    # Phase 1: Establish range and accumulation (20 bars)
    print("Phase 1: Range and Accumulation")
    for i in range(20):
        # Sideways accumulation with slight upward bias
        price = base_price + np.random.normal(0, 2) + i * 0.1
        open_price = price + np.random.normal(0, 1)
        high = open_price + abs(np.random.normal(0, 3))
        low = open_price - abs(np.random.normal(0, 2.5))
        close = low + (high - low) * (0.5 + np.random.normal(0, 0.2))
        volume = int(5000 + np.random.normal(0, 1000))
        
        data.append({
            'timestamp': datetime(2024, 8, 19, 9) + timedelta(hours=i),
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': max(1000, volume)
        })
    
    # Phase 2: Institutional markup - strong bullish move (15 bars)
    print("Phase 2: Institutional Markup")
    for i in range(15):
        # Strong upward movement with increasing volume
        price = base_price + 2 + i * 4  # 4 points per bar average
        open_price = price + np.random.normal(0, 1)
        high = open_price + abs(np.random.normal(0, 5))  # Larger ranges
        low = open_price - abs(np.random.normal(0, 2))   # Smaller pullbacks
        close = low + (high - low) * (0.7 + np.random.normal(0, 0.15))  # Bullish bias
        volume = int(8000 + i * 200 + np.random.normal(0, 1500))  # Increasing volume
        
        data.append({
            'timestamp': datetime(2024, 8, 19, 9) + timedelta(hours=20 + i),
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': max(1000, volume)
        })
    
    # Phase 3: Distribution and pullback to SMZ (12 bars)
    print("Phase 3: Distribution and Pullback")
    peak_price = base_price + 62  # Around 1912
    for i in range(12):
        # Pullback with decreasing volume
        pullback_ratio = 0.618  # Golden ratio retracement
        target_price = peak_price - (peak_price - base_price) * pullback_ratio
        price = peak_price - i * ((peak_price - target_price) / 12)
        
        open_price = price + np.random.normal(0, 1)
        high = open_price + abs(np.random.normal(0, 2.5))
        low = open_price - abs(np.random.normal(0, 4))    # Larger wicks down
        close = low + (high - low) * (0.4 + np.random.normal(0, 0.2))  # Bearish bias
        volume = int(6000 - i * 150 + np.random.normal(0, 1000))  # Decreasing volume
        
        data.append({
            'timestamp': datetime(2024, 8, 19, 9) + timedelta(hours=35 + i),
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': max(1000, volume)
        })
    
    # Phase 4: SMZ reaction and potential entry (8 bars)
    print("Phase 4: SMZ Reaction and Entry Setup")
    smz_price = target_price
    for i in range(8):
        if i < 3:
            # Initial reaction at SMZ
            price = smz_price + np.random.normal(0, 1.5)
            close_bias = 0.3 + i * 0.1  # Gradually more bullish
        else:
            # Bullish continuation
            price = smz_price + (i - 2) * 2.5
            close_bias = 0.7 + np.random.normal(0, 0.1)
        
        open_price = price + np.random.normal(0, 1)
        high = open_price + abs(np.random.normal(0, 3))
        low = open_price - abs(np.random.normal(0, 2))
        close = low + (high - low) * close_bias
        
        # Volume spike on reversal
        if i == 2:  # Reversal bar
            volume = int(12000 + np.random.normal(0, 2000))
        else:
            volume = int(7000 + np.random.normal(0, 1500))
        
        data.append({
            'timestamp': datetime(2024, 8, 19, 9) + timedelta(hours=47 + i),
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(close, 2),
            'volume': max(1000, volume)
        })
    
    df = pd.DataFrame(data)
    df.index = pd.to_datetime(df['timestamp'])
    return df


def analyze_market_structure(data: pd.DataFrame):
    """Demonstrate market structure analysis."""
    print("\n" + "="*70)
    print("MARKET STRUCTURE ANALYSIS")
    print("="*70)
    
    detector = MarketStructureDetector(swing_length=5, min_swing_size=0.002)
    result = detector.calculate(data)
    
    if result['status'] == 'valid':
        print(f"Market Structure: {result['market_structure'].upper()}")
        print(f"Structure Change: {result['structure_change'].upper()}")
        print(f"Current Price: ${result['current_price']:.2f}")
        
        if result['bos_level']:
            print(f"BOS Level: ${result['bos_level']:.2f}")
        
        print(f"\nSwing Points Detected: {len(result['swing_points'])}")
        print(f"Swing Highs: {len(result['swing_highs'])}")
        print(f"Swing Lows: {len(result['swing_lows'])}")
        
        print(f"\nTrend Strength: {result['trend_strength']:.2f}")
        print(f"Structure Quality: {result['structure_quality']:.2f}")
        
        # Display recent swing points
        if result['swing_points']:
            print("\nRecent Swing Points:")
            for i, swing in enumerate(result['swing_points'][-5:]):
                print(f"  {i+1}. {swing.type.upper()}: ${swing.price:.2f} "
                      f"at {swing.timestamp.strftime('%H:%M')} "
                      f"(Significance: {swing.significance:.2f})")
        
        # Display untaken levels
        if result['untaken_highs']:
            print(f"\nUntaken Highs: {[f'${h:.2f}' for h in result['untaken_highs'][:3]]}")
        if result['untaken_lows']:
            print(f"Untaken Lows: {[f'${l:.2f}' for l in result['untaken_lows'][:3]]}")
    
    else:
        print(f"Analysis failed: {result['status']}")
    
    return result


def analyze_smart_money_zones(data: pd.DataFrame):
    """Demonstrate Smart Money Zone detection."""
    print("\n" + "="*70)
    print("SMART MONEY ZONES ANALYSIS")
    print("="*70)
    
    detector = SmartMoneyZoneDetector()
    result = detector.calculate(data)
    
    if result['status'] == 'valid':
        zones = result['smz_zones']
        print(f"Smart Money Zones Detected: {len(zones)}")
        
        for i, zone in enumerate(zones):
            print(f"\n📍 Zone {i+1} ({zone.direction.upper()}):")
            print(f"   Swing Range: ${zone.swing_low.price:.2f} - ${zone.swing_high.price:.2f}")
            print(f"   Institutional Zone: ${zone.institutional_zone[0]:.2f} - ${zone.institutional_zone[1]:.2f}")
            print(f"   Smart Money Zone: ${zone.smart_money_zone[0]:.2f} - ${zone.smart_money_zone[1]:.2f}")
            print(f"   Confidence: {zone.confidence:.2f}")
            print(f"   Created: {zone.created_at.strftime('%H:%M')}")
            
            # Fibonacci levels
            print(f"   Fibonacci Levels:")
            print(f"     0.0%: ${zone.fib_0:.2f}")
            print(f"     61.8%: ${zone.fib_618:.2f}")
            print(f"     78.6%: ${zone.fib_786:.2f}")
            print(f"     82.6%: ${zone.fib_826:.2f}")
            print(f"     100.0%: ${zone.fib_100:.2f}")
        
        # Current price analysis
        current_price = result['current_price']
        print(f"\n💰 Current Price Analysis (${current_price:.2f}):")
        
        active_zones = result['active_zones']
        if active_zones:
            for zone_info in active_zones:
                zone_type = zone_info['type']
                direction = zone_info['direction']
                print(f"   🎯 Price is in {zone_type} zone ({direction})")
        else:
            print("   📍 Price is not currently in any zone")
        
        if result['nearest_institutional_zone']:
            nearest = result['nearest_institutional_zone']
            print(f"   🏛️ Nearest Institutional Zone: {nearest['distance']:.2f} points away ({nearest['direction']})")
        
        if result['nearest_smz']:
            nearest_smz = result['nearest_smz']
            print(f"   💎 Nearest SMZ: {nearest_smz['distance']:.2f} points away ({nearest_smz['direction']})")
        
        # Zone confluence
        confluence = result['zone_confluence']
        if confluence['max_confluence'] > 1:
            print(f"\n🔥 Zone Confluence Detected:")
            print(f"   Maximum Confluence: {confluence['max_confluence']} levels")
            for level, count in confluence['confluence_levels'][:3]:
                print(f"   ${level:.2f}: {count} confluent levels")
    
    else:
        print(f"SMZ Analysis failed: {result['status']}")
    
    return result


def analyze_entry_signals(data: pd.DataFrame):
    """Demonstrate entry signal generation and validation."""
    print("\n" + "="*70)
    print("ENTRY SIGNAL ANALYSIS")
    print("="*70)
    
    confirmation = SMZEntryConfirmation(confirmation_bars=3, volume_threshold=1.5)
    result = confirmation.calculate(data)
    
    if result['status'] == 'valid':
        signals = result['entry_signals']
        print(f"Entry Signals Generated: {len(signals)}")
        
        if signals:
            for i, signal in enumerate(signals):
                print(f"\n🚨 Signal {i+1}: {signal['type'].upper()}")
                print(f"   Direction: {signal['direction'].upper()}")
                print(f"   Entry Price: ${signal['entry_price']:.2f}")
                print(f"   Confidence: {signal['confidence']:.2f}")
                print(f"   Total Confidence: {signal['total_confidence']:.2f}")
                print(f"   Reason: {signal['reason']}")
                
                if signal['validation_reasons']:
                    print(f"   Validation: {', '.join(signal['validation_reasons'])}")
                
                # Risk management
                signal_id = f"signal_{i}"
                if signal_id in result['risk_levels']:
                    risk = result['risk_levels'][signal_id]
                    print(f"   💰 Risk Management:")
                    print(f"     Stop Loss: ${risk['stop_loss']:.2f}")
                    print(f"     Take Profit 1: ${risk['take_profit_1']:.2f} (RR: {risk['risk_reward_1']:.1f}:1)")
                    print(f"     Take Profit 2: ${risk['take_profit_2']:.2f} (RR: {risk['risk_reward_2']:.1f}:1)")
                    print(f"     Risk Amount: ${risk['risk_amount']:.2f}")
        else:
            print("   ❌ No valid entry signals detected at current price levels")
        
        # Display confirmation criteria
        criteria = result['confirmation_criteria']
        print(f"\n⚙️ Confirmation Criteria:")
        print(f"   Confirmation Bars: {criteria['confirmation_bars']}")
        print(f"   Volume Threshold: {criteria['volume_threshold']}x")
        print(f"   Minimum Confidence: {criteria['minimum_confidence']}")
        print(f"   Risk-Reward Targets: {criteria['risk_reward_targets']}")
    
    else:
        print(f"Entry analysis failed: {result['status']}")
    
    return result


def demonstrate_multi_timeframe_analysis():
    """Demonstrate multi-timeframe confluence analysis."""
    print("\n" + "="*70)
    print("MULTI-TIMEFRAME CONFLUENCE ANALYSIS")
    print("="*70)
    
    # Create data for multiple timeframes (simplified example)
    base_data = create_realistic_smz_scenario()
    
    # Simulate different timeframe data
    price_data = {
        '1h': base_data,
        '4h': base_data.iloc[::4].copy(),  # Every 4th bar for 4h timeframe
        '1d': base_data.iloc[::24].copy()   # Every 24th bar for daily timeframe
    }
    
    # Ensure we have enough data for analysis
    for tf, data in price_data.items():
        if len(data) < 30:
            print(f"⚠️ Warning: {tf} timeframe has only {len(data)} bars")
    
    mtf = MultiTimeframeAnalysis(['1h', '4h', '1d'])
    result = mtf.analyze_confluence(price_data)
    
    print(f"Timeframes Analyzed: {list(result['timeframe_results'].keys())}")
    print(f"Overall Confluence Score: {result['confluence_score']:.2f}")
    
    if result['dominant_structure']:
        print(f"Dominant Structure: {result['dominant_structure'].upper()}")
    
    # Display timeframe-specific results
    for timeframe, tf_result in result['timeframe_results'].items():
        if tf_result.get('status') == 'valid':
            print(f"\n📊 {timeframe.upper()} Timeframe:")
            print(f"   Structure: {tf_result['market_structure']}")
            print(f"   Structure Change: {tf_result['structure_change']}")
            print(f"   Trend Strength: {tf_result.get('trend_strength', 0):.2f}")
            print(f"   SMZ Zones: {len(tf_result.get('smz_zones', []))}")
        else:
            print(f"\n📊 {timeframe.upper()} Timeframe: {tf_result.get('status', 'No data')}")
    
    # Confluence zones
    confluence_zones = result['confluence_zones']
    if confluence_zones:
        print(f"\n🔥 Multi-Timeframe Confluence Zones:")
        for i, cz in enumerate(confluence_zones):
            zone = cz['zone']
            print(f"   Zone {i+1}: ${zone.institutional_zone[0]:.2f} - ${zone.institutional_zone[1]:.2f}")
            print(f"   Timeframes: {', '.join(cz['timeframes'])}")
            print(f"   Confluence Strength: {cz['confluence_strength']}")
    else:
        print("\n📍 No multi-timeframe confluence zones detected")
    
    return result


def interpret_trading_insights(structure_result, smz_result, signals_result, mtf_result):
    """Provide comprehensive trading insights and recommendations."""
    print("\n" + "="*70)
    print("TRADING INSIGHTS & RECOMMENDATIONS")
    print("="*70)
    
    current_price = smz_result.get('current_price', 0)
    
    print(f"💹 Current Market Context (${current_price:.2f}):")
    
    # Market bias
    market_structure = structure_result.get('market_structure', 'unknown')
    structure_change = structure_result.get('structure_change', 'none')
    
    if market_structure == 'bullish':
        bias = "📈 BULLISH BIAS"
        bias_desc = "Higher highs and higher lows suggest upward momentum"
    elif market_structure == 'bearish':
        bias = "📉 BEARISH BIAS"
        bias_desc = "Lower highs and lower lows suggest downward pressure"
    elif market_structure == 'compression':
        bias = "🔄 CONSOLIDATION"
        bias_desc = "Mixed structure suggests range-bound or transitional phase"
    else:
        bias = "❓ UNCLEAR BIAS"
        bias_desc = "Insufficient data or unclear market structure"
    
    print(f"   {bias}")
    print(f"   {bias_desc}")
    
    if structure_change != 'none':
        print(f"   🚨 Recent Structure Change: {structure_change.upper()}")
    
    # SMZ assessment
    active_zones = smz_result.get('active_zones', [])
    if active_zones:
        print(f"\n🎯 Smart Money Zone Status:")
        for zone_info in active_zones:
            zone_type = zone_info['type'].replace('_', ' ').title()
            direction = zone_info['direction'].upper()
            print(f"   • Price is in {zone_type} ({direction} bias)")
    
    # Entry opportunities
    signals = signals_result.get('entry_signals', [])
    if signals:
        best_signal = signals[0]  # Highest confidence
        print(f"\n💡 Best Trading Opportunity:")
        print(f"   Signal: {best_signal['type'].replace('_', ' ').title()}")
        print(f"   Direction: {best_signal['direction'].upper()}")
        print(f"   Confidence: {best_signal['total_confidence']:.1%}")
        print(f"   Entry: ${best_signal['entry_price']:.2f}")
        
        # Risk management from best signal
        if 'risk_levels' in signals_result and 'signal_0' in signals_result['risk_levels']:
            risk = signals_result['risk_levels']['signal_0']
            print(f"   Stop: ${risk['stop_loss']:.2f}")
            print(f"   Target 1: ${risk['take_profit_1']:.2f}")
            print(f"   Target 2: ${risk['take_profit_2']:.2f}")
    
    # Multi-timeframe alignment
    confluence_score = mtf_result.get('confluence_score', 0)
    dominant_structure = mtf_result.get('dominant_structure')
    
    print(f"\n🔍 Multi-Timeframe Assessment:")
    print(f"   Confluence Score: {confluence_score:.1%}")
    
    if confluence_score > 0.7:
        alignment = "🟢 STRONG ALIGNMENT"
    elif confluence_score > 0.5:
        alignment = "🟡 MODERATE ALIGNMENT"
    else:
        alignment = "🔴 WEAK ALIGNMENT"
    
    print(f"   {alignment}")
    
    if dominant_structure:
        print(f"   Dominant Structure: {dominant_structure.upper()}")
    
    # Trading recommendations
    print(f"\n📋 Trading Recommendations:")
    
    if signals and confluence_score > 0.6:
        print("   ✅ High-probability setup identified")
        print("   ✅ Multi-timeframe confluence supports trade")
        print("   📝 Consider position sizing based on confidence level")
    elif signals and confluence_score > 0.4:
        print("   ⚠️ Setup detected but limited multi-timeframe support")
        print("   📝 Consider smaller position size or wait for better alignment")
    elif signals:
        print("   ⚠️ Setup detected but weak multi-timeframe alignment")
        print("   📝 High risk - consider avoiding or very small position")
    else:
        print("   ❌ No clear setup at current levels")
        print("   📝 Wait for price to reach key zones or structure changes")
    
    # Risk management guidelines
    print(f"\n⚠️ Risk Management Guidelines:")
    print("   • Never risk more than 1-2% of account per trade")
    print("   • Use proper position sizing based on stop distance")
    print("   • Consider partial profits at first target")
    print("   • Trail stops after reaching 1:1 risk-reward")
    print("   • Monitor for structure changes that invalidate setup")


def main():
    """Run complete Smart Money Zones analysis demonstration."""
    print("="*70)
    print("SMART MONEY ZONES (SMZ) METHODOLOGY DEMONSTRATION")
    print("="*70)
    print("Analyzing institutional trading patterns and zone-based entries")
    
    try:
        # Create realistic market scenario
        print("\n🔄 Generating realistic market scenario...")
        data = create_realistic_smz_scenario()
        print(f"📊 Created {len(data)} bars of price data")
        print(f"📅 Time range: {data.index[0].strftime('%H:%M')} to {data.index[-1].strftime('%H:%M')}")
        print(f"💰 Price range: ${data['low'].min():.2f} - ${data['high'].max():.2f}")
        
        # Step 1: Market Structure Analysis
        structure_result = analyze_market_structure(data)
        
        # Step 2: Smart Money Zones Detection
        smz_result = analyze_smart_money_zones(data)
        
        # Step 3: Entry Signal Generation
        signals_result = analyze_entry_signals(data)
        
        # Step 4: Multi-Timeframe Analysis
        mtf_result = demonstrate_multi_timeframe_analysis()
        
        # Step 5: Trading Insights
        interpret_trading_insights(structure_result, smz_result, signals_result, mtf_result)
        
        print("\n" + "="*70)
        print("✅ SMART MONEY ZONES ANALYSIS COMPLETED!")
        print("="*70)
        print("This methodology provides a systematic approach to:")
        print("• Identifying institutional price levels")
        print("• Timing entries based on market structure")
        print("• Managing risk with proper reward ratios")
        print("• Confirming setups across multiple timeframes")
        
    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        print("Please check the implementation and try again.")


if __name__ == "__main__":
    main()