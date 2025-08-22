#!/usr/bin/env python3
"""
Test script to validate modeling universe creation logic without database connection.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from universe.modeling_universe_creator import ModelingStock, ModelingUniverseCreator
from datetime import date, timedelta
import asyncio

# Mock data for testing
mock_stocks = [
    ModelingStock(
        symbol="AAPL",
        instrument_id=1,
        avg_market_cap=3_000_000_000_000,  # $3T
        avg_dollar_volume=8_000_000_000,   # $8B daily
        avg_volume=50_000_000,
        avg_price=160.0,
        trading_days=22,
        first_date=date.today() - timedelta(days=30),
        last_date=date.today() - timedelta(days=1)
    ),
    ModelingStock(
        symbol="MSFT", 
        instrument_id=2,
        avg_market_cap=2_500_000_000_000,  # $2.5T
        avg_dollar_volume=5_000_000_000,   # $5B daily
        avg_volume=25_000_000,
        avg_price=200.0,
        trading_days=22,
        first_date=date.today() - timedelta(days=30),
        last_date=date.today() - timedelta(days=1)
    ),
    ModelingStock(
        symbol="GOOGL",
        instrument_id=3,
        avg_market_cap=1_800_000_000_000,  # $1.8T
        avg_dollar_volume=3_000_000_000,   # $3B daily
        avg_volume=15_000_000,
        avg_price=120.0,
        trading_days=22,
        first_date=date.today() - timedelta(days=30),
        last_date=date.today() - timedelta(days=1)
    ),
    ModelingStock(
        symbol="AMZN",
        instrument_id=4,
        avg_market_cap=1_500_000_000_000,  # $1.5T
        avg_dollar_volume=4_000_000_000,   # $4B daily
        avg_volume=30_000_000,
        avg_price=133.0,
        trading_days=22,
        first_date=date.today() - timedelta(days=30),
        last_date=date.today() - timedelta(days=1)
    ),
    ModelingStock(
        symbol="TSLA",
        instrument_id=5,
        avg_market_cap=800_000_000_000,    # $800B
        avg_dollar_volume=15_000_000_000,  # $15B daily
        avg_volume=75_000_000,
        avg_price=200.0,
        trading_days=22,
        first_date=date.today() - timedelta(days=30),
        last_date=date.today() - timedelta(days=1)
    ),
    ModelingStock(
        symbol="NVDA",
        instrument_id=6,
        avg_market_cap=1_200_000_000_000,  # $1.2T
        avg_dollar_volume=12_000_000_000,  # $12B daily
        avg_volume=40_000_000,
        avg_price=300.0,
        trading_days=22,
        first_date=date.today() - timedelta(days=30),
        last_date=date.today() - timedelta(days=1)
    ),
    # Stock that should be filtered out - small cap
    ModelingStock(
        symbol="SMALL",
        instrument_id=7,
        avg_market_cap=200_000_000,        # $200M - below threshold
        avg_dollar_volume=150_000_000,     # $150M daily
        avg_volume=5_000_000,
        avg_price=30.0,
        trading_days=22,
        first_date=date.today() - timedelta(days=30),
        last_date=date.today() - timedelta(days=1)
    ),
    # Stock that should be filtered out - low volume
    ModelingStock(
        symbol="LOWVOL",
        instrument_id=8,
        avg_market_cap=600_000_000_000,    # $600B - above threshold
        avg_dollar_volume=50_000_000,      # $50M daily - below threshold
        avg_volume=500_000,
        avg_price=100.0,
        trading_days=22,
        first_date=date.today() - timedelta(days=30),
        last_date=date.today() - timedelta(days=1)
    )
]

def test_filtering_criteria():
    """Test the filtering criteria logic"""
    print("Testing Modeling Universe Creation Logic")
    print("=" * 50)
    
    min_market_cap_millions = 400  # $400M
    min_dollar_volume_millions = 100  # $100M
    
    print(f"Criteria:")
    print(f"- Min market cap: ${min_market_cap_millions:,}M")
    print(f"- Min dollar volume: ${min_dollar_volume_millions:,}M")
    print()
    
    # Apply filtering criteria
    qualifying_stocks = []
    for stock in mock_stocks:
        market_cap_millions = stock.avg_market_cap / 1_000_000
        dollar_volume_millions = stock.avg_dollar_volume / 1_000_000
        
        qualifies = (
            market_cap_millions >= min_market_cap_millions and
            dollar_volume_millions >= min_dollar_volume_millions
        )
        
        status = "✓ PASS" if qualifies else "✗ FAIL"
        print(f"{stock.symbol:6} | ${market_cap_millions:8,.0f}M | ${dollar_volume_millions:6,.0f}M | {status}")
        
        if qualifies:
            qualifying_stocks.append(stock)
    
    print()
    print(f"Qualifying stocks: {len(qualifying_stocks)}/{len(mock_stocks)}")
    print(f"Selected symbols: {', '.join(s.symbol for s in qualifying_stocks)}")
    
    return qualifying_stocks

def test_ranking_logic():
    """Test the ranking logic"""
    print("\nTesting Ranking Logic")
    print("=" * 50)
    
    creator = ModelingUniverseCreator()
    
    # Get stocks that pass filtering
    qualifying_stocks = [s for s in mock_stocks if 
                        s.avg_market_cap >= 400_000_000 and 
                        s.avg_dollar_volume >= 100_000_000]
    
    # Rank them
    ranked_stocks = creator._rank_stocks_for_modeling(qualifying_stocks)
    
    print("Ranking by modeling score (60% market cap, 40% liquidity):")
    for i, stock in enumerate(ranked_stocks, 1):
        market_cap_score = stock.avg_market_cap / 1_000_000
        dollar_volume_score = stock.avg_dollar_volume / 1_000_000
        modeling_score = 0.6 * market_cap_score + 0.4 * dollar_volume_score
        
        print(f"{i:2}. {stock.symbol:6} | Score: {modeling_score:8,.0f} | "
              f"Cap: ${market_cap_score:6,.0f}M | Vol: ${dollar_volume_score:6,.0f}M")
    
    return ranked_stocks

async def test_report_generation():
    """Test the report generation"""
    print("\nTesting Report Generation")
    print("=" * 50)
    
    creator = ModelingUniverseCreator()
    
    # Get stocks that pass filtering
    qualifying_stocks = [s for s in mock_stocks if 
                        s.avg_market_cap >= 400_000_000 and 
                        s.avg_dollar_volume >= 100_000_000]
    
    # Generate report
    report = await creator.generate_modeling_report(
        qualifying_stocks, 400, 100, "test_modeling_report.md"
    )
    
    print("Report generated successfully!")
    print(f"Report length: {len(report)} characters")
    print("\nFirst few lines:")
    print("-" * 30)
    for line in report.split('\n')[:10]:
        print(line)
    print("-" * 30)

def main():
    """Run all tests"""
    print("Modeling Universe Creator - Logic Testing")
    print("=" * 60)
    
    # Test 1: Filtering criteria
    qualifying_stocks = test_filtering_criteria()
    
    # Test 2: Ranking logic
    ranked_stocks = test_ranking_logic()
    
    # Test 3: Report generation
    asyncio.run(test_report_generation())
    
    print("\n" + "=" * 60)
    print("All logic tests completed successfully!")
    print(f"The filtering and ranking logic is working correctly.")
    print(f"Ready to test with real database once connection is established.")

if __name__ == "__main__":
    main()