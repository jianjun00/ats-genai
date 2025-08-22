"""
Simple Demo: OHLC Chart with PLDOT, ETOP, EBOT and Events

This demonstrates creating focused trading charts with:
- OHLC candlestick price action
- PLDOT (Pivot Low Dots) - Support levels
- ETOP (Expected Tops) - Resistance levels
- EBOT (Expected Bottoms) - Support levels
- Major market events and news
- Trade entry/exit signals

Usage:
    PYTHONPATH=src python examples/simple_chart_demo.py
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from ml.evaluation.simple_trade_chart import (
    SimpleTradeChart, 
    TechnicalIndicators, 
    MarketEvent,
    generate_sample_data
)


def create_aapl_chart_with_events():
    """Create AAPL chart with realistic events and indicators"""
    print("📊 Creating AAPL Chart with Indicators and Events")
    print("=" * 50)
    
    chart_creator = SimpleTradeChart()
    symbol = "AAPL"
    start_date = datetime(2024, 1, 15)
    
    # Generate realistic price data
    price_data, auto_indicators, auto_events = generate_sample_data(symbol, start_date, 25)
    
    # Override with more realistic indicators
    indicators = TechnicalIndicators(
        # PLDOT - Pivot Low support levels
        pldot=[
            (datetime(2024, 1, 18), 182.50),  # Support after dip
            (datetime(2024, 2, 2), 179.25),   # Lower support  
            (datetime(2024, 2, 8), 180.10),   # Support retest
        ],
        
        # ETOP - Expected resistance tops
        etop=[
            (datetime(2024, 1, 22), 192.75),  # Previous high resistance
            (datetime(2024, 2, 5), 188.50),   # Lower resistance
            (datetime(2024, 2, 12), 194.25),  # New resistance
        ],
        
        # EBOT - Expected support bottoms
        ebot=[
            (datetime(2024, 1, 25), 181.00),  # Expected support
            (datetime(2024, 2, 9), 178.50),   # Lower expected support
        ]
    )
    
    # Realistic market events
    events = [
        MarketEvent(
            date=datetime(2024, 1, 16),
            event_type="earnings",
            description="Q1 Earnings Preview",
            impact="neutral",
            importance=3
        ),
        MarketEvent(
            date=datetime(2024, 1, 19),
            event_type="fed",
            description="Fed Official Speech",
            impact="negative", 
            importance=2
        ),
        MarketEvent(
            date=datetime(2024, 1, 23),
            event_type="news",
            description="iPhone Sales Report",
            impact="positive",
            importance=4
        ),
        MarketEvent(
            date=datetime(2024, 2, 1),
            event_type="earnings",
            description="Q1 Earnings Release",
            impact="positive",
            importance=5
        ),
        MarketEvent(
            date=datetime(2024, 2, 6),
            event_type="economic",
            description="Employment Report", 
            impact="negative",
            importance=3
        ),
        MarketEvent(
            date=datetime(2024, 2, 10),
            event_type="news",
            description="AI Partnership News",
            impact="positive",
            importance=3
        )
    ]
    
    # Create chart with buy signal
    trade_date = datetime(2024, 1, 24)  # Buy after positive iPhone news
    output_path = chart_creator.create_ohlc_chart(
        symbol=symbol,
        price_data=price_data,
        indicators=indicators,
        events=events,
        trade_date=trade_date,
        trade_action="buy",
        output_path=f"{symbol}_trade_analysis.png"
    )
    
    print(f"✅ Chart created: {output_path}")
    
    # Print chart interpretation
    print("\n📈 Chart Analysis:")
    print("=" * 50)
    print(f"🎯 **Trade Signal**: BUY {symbol} on {trade_date.strftime('%m/%d/%Y')}")
    print(f"📊 **Key Indicators**:")
    print(f"   • PLDOT Support Levels: {len(indicators.pldot)} levels identified")
    print(f"   • ETOP Resistance: {len(indicators.etop)} resistance zones")
    print(f"   • EBOT Expected Support: {len(indicators.ebot)} support projections")
    print(f"📰 **Major Events**: {len(events)} relevant events tracked")
    
    # Event summary
    print(f"\n🗞️ **Event Timeline**:")
    for event in sorted(events, key=lambda x: x.date):
        impact_icon = "📈" if event.impact == "positive" else "📉" if event.impact == "negative" else "➖"
        importance_stars = "⭐" * event.importance
        print(f"   {event.date.strftime('%m/%d')}: {impact_icon} {event.description} {importance_stars}")
    
    return output_path


def create_multiple_stock_comparison():
    """Create comparison charts for multiple stocks"""
    print("\n🔍 Creating Multi-Stock Comparison Charts")
    print("=" * 50)
    
    chart_creator = SimpleTradeChart()
    symbols = ["AAPL", "MSFT", "GOOGL"]
    charts_created = []
    
    for symbol in symbols:
        print(f"\n📊 Processing {symbol}...")
        
        start_date = datetime(2024, 1, 10)
        price_data, indicators, events = generate_sample_data(symbol, start_date, 20)
        
        # Create unique events per stock
        if symbol == "AAPL":
            events.append(MarketEvent(
                date=price_data.index[8],
                event_type="news",
                description="iPhone Production Update",
                impact="positive",
                importance=3
            ))
        elif symbol == "MSFT":
            events.append(MarketEvent(
                date=price_data.index[10],
                event_type="news", 
                description="Azure Revenue Growth",
                impact="positive",
                importance=4
            ))
        elif symbol == "GOOGL":
            events.append(MarketEvent(
                date=price_data.index[12],
                event_type="news",
                description="AI Breakthrough Announcement",
                impact="positive",
                importance=5
            ))
        
        # Different trade signals
        trade_actions = {"AAPL": "buy", "MSFT": "buy", "GOOGL": "sell"}
        trade_dates = {s: price_data.index[10 + i] for i, s in enumerate(symbols)}
        
        output_path = chart_creator.create_ohlc_chart(
            symbol=symbol,
            price_data=price_data,
            indicators=indicators,
            events=events,
            trade_date=trade_dates[symbol],
            trade_action=trade_actions[symbol],
            output_path=f"{symbol}_comparison_chart.png"
        )
        
        charts_created.append(output_path)
        print(f"   ✅ {symbol} chart saved: {output_path}")
    
    print(f"\n🎉 Created {len(charts_created)} comparison charts")
    return charts_created


def demonstrate_different_market_conditions():
    """Demonstrate charts under different market conditions"""
    print("\n🌊 Market Conditions Demonstration")
    print("=" * 50)
    
    chart_creator = SimpleTradeChart()
    
    conditions = {
        "bull_market": {
            "symbol": "TSLA",
            "description": "Strong Uptrend with Breakouts",
            "events": [
                MarketEvent(datetime(2024, 1, 5), "news", "Production Record", "positive", 4),
                MarketEvent(datetime(2024, 1, 12), "news", "Expansion Announcement", "positive", 3),
            ]
        },
        "bear_market": {
            "symbol": "META", 
            "description": "Downtrend with Support Tests",
            "events": [
                MarketEvent(datetime(2024, 1, 8), "news", "Regulatory Concerns", "negative", 3),
                MarketEvent(datetime(2024, 1, 15), "economic", "Ad Revenue Decline", "negative", 4),
            ]
        },
        "sideways": {
            "symbol": "JPM",
            "description": "Range-Bound Trading",
            "events": [
                MarketEvent(datetime(2024, 1, 10), "fed", "Rate Decision", "neutral", 3),
                MarketEvent(datetime(2024, 1, 20), "earnings", "Mixed Results", "neutral", 2),
            ]
        }
    }
    
    for condition, config in conditions.items():
        print(f"\n📈 {condition.replace('_', ' ').title()}: {config['description']}")
        
        start_date = datetime(2024, 1, 1)
        price_data, indicators, _ = generate_sample_data(config['symbol'], start_date, 22)
        
        output_path = chart_creator.create_ohlc_chart(
            symbol=config['symbol'],
            price_data=price_data,
            indicators=indicators,
            events=config['events'],
            trade_date=price_data.index[15],
            trade_action="buy" if condition == "bull_market" else "sell" if condition == "bear_market" else "hold",
            output_path=f"{config['symbol']}_{condition}_scenario.png"
        )
        
        print(f"   📊 Chart: {output_path}")


def main():
    """Main demonstration function"""
    print("🚀 Simple OHLC Chart with Indicators Demo")
    print("=" * 60)
    print("Creating focused trading charts with:")
    print("• OHLC candlestick price action")
    print("• PLDOT (Pivot Low Dots) - Support levels")  
    print("• ETOP (Expected Tops) - Resistance levels")
    print("• EBOT (Expected Bottoms) - Support levels")
    print("• Major market events and news")
    print("• Trade entry/exit signals")
    print()
    
    try:
        # 1. Main AAPL example
        aapl_chart = create_aapl_chart_with_events()
        
        # 2. Multi-stock comparison
        comparison_charts = create_multiple_stock_comparison()
        
        # 3. Different market conditions
        demonstrate_different_market_conditions()
        
        print(f"\n🎉 Demo completed successfully!")
        print(f"📁 Check the current directory for generated chart files:")
        print(f"   • {aapl_chart}")
        for chart in comparison_charts:
            print(f"   • {chart}")
        print(f"   • Plus scenario-specific charts")
        
        print(f"\n💡 **How to Use These Charts**:")
        print(f"   • 🔵 Blue dots (PLDOT) = Support levels to watch")
        print(f"   • 🔺 Red triangles (ETOP) = Resistance levels")
        print(f"   • 🔻 Green triangles (EBOT) = Expected support")
        print(f"   • 📰 Colored markers = Market events (hover for details)")
        print(f"   • 🎯 Large colored triangles = Trade signals")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)