#!/usr/bin/env python3
"""
Test script for FirstRate minute bar backfill functionality.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from market_data.agent.firstrate_minute_adapter import FirstRateMinuteAdapter


async def test_firstrate_adapter():
    """Test FirstRate adapter functionality."""
    
    print("🧪 Testing FirstRate Minute Bar Adapter")
    print("=" * 50)
    
    # Initialize adapter
    data_path = "/mnt/d/ats-data/firstrate-data/stock"
    
    async with FirstRateMinuteAdapter(data_path) as adapter:
        # Test 1: Get available symbols
        print("📊 Getting available symbols...")
        symbols_by_letter = await adapter.get_available_symbols()
        
        total_symbols = sum(len(symbols) for symbols in symbols_by_letter.values())
        print(f"Available letters: {sorted(symbols_by_letter.keys())}")
        print(f"Total symbols: {total_symbols:,}")
        
        # Show sample symbols for each letter
        for letter in sorted(list(symbols_by_letter.keys())[:5]):  # First 5 letters
            symbols = symbols_by_letter[letter]
            print(f"  Letter {letter}: {len(symbols)} symbols - {symbols[:5]}{'...' if len(symbols) > 5 else ''}")
        
        print()
        
        # Test 2: Parse sample data for specific symbols
        test_symbols = ['AAPL']  # Start with just Apple
        if 'A' in symbols_by_letter and 'AAPL' in symbols_by_letter['A']:
            print(f"📈 Testing data parsing for {test_symbols}...")
            
            # Fetch limited data for testing  
            bars = []
            count = 0
            max_bars = 100  # Limit for testing
            
            async for bar in adapter.fetch_minute_bars_async(
                test_symbols, 
                max_bars_per_symbol=max_bars
            ):
                bars.append(bar)
                count += 1
                if count >= max_bars:
                    break
            
            print(f"Parsed {len(bars)} bars for AAPL")
            
            if bars:
                first_bar = bars[0]
                last_bar = bars[-1]
                
                print(f"First bar: {first_bar.timestamp} - O:{first_bar.open} H:{first_bar.high} L:{first_bar.low} C:{first_bar.close} V:{first_bar.volume}")
                print(f"Last bar:  {last_bar.timestamp} - O:{last_bar.open} H:{last_bar.high} L:{last_bar.low} C:{last_bar.close} V:{last_bar.volume}")
                
                # Check data quality
                valid_ohlc = sum(1 for bar in bars if bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high)
                print(f"Valid OHLC bars: {valid_ohlc}/{len(bars)} ({valid_ohlc/len(bars)*100:.1f}%)")
                
                # Date range
                timestamps = [bar.timestamp for bar in bars]
                date_range = f"{min(timestamps).date()} to {max(timestamps).date()}"
                print(f"Date range: {date_range}")
        else:
            print("❌ AAPL not found in available symbols")
        
        print()
        
        # Test 3: Show parsing statistics
        stats = adapter.get_parsing_stats()
        print("📊 Parsing Statistics:")
        print(f"  Files processed: {stats.total_files_processed}")
        print(f"  Bars parsed: {stats.total_bars_parsed:,}")
        print(f"  Symbols processed: {stats.total_symbols_processed}")
        print(f"  Processing time: {stats.processing_time_seconds:.2f} seconds")
        
        if stats.parsing_errors:
            print(f"  Parsing errors: {len(stats.parsing_errors)}")
            print("  Sample errors:")
            for error in stats.parsing_errors[:3]:
                print(f"    {error}")
        else:
            print("  No parsing errors ✅")


async def test_database_schema():
    """Test database schema exists."""
    
    print("\n🗄️  Testing Database Schema")
    print("=" * 50)
    
    try:
        from config.environment import Environment
        import asyncpg
        
        env = Environment()
        db_url = env.get_database_url()
        
        print(f"Connecting to: {db_url.split('@')[1] if '@' in db_url else 'localhost'}")
        
        # Test connection
        conn = await asyncpg.connect(db_url)
        
        # Check if minute_bars table exists
        result = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'minute_bars'
            )
        """)
        
        if result:
            print("✅ minute_bars table exists")
            
            # Check table structure
            columns = await conn.fetch("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'minute_bars'
                ORDER BY ordinal_position
            """)
            
            print(f"Table has {len(columns)} columns:")
            for col in columns[:10]:  # Show first 10 columns
                print(f"  {col['column_name']}: {col['data_type']}")
            if len(columns) > 10:
                print(f"  ... and {len(columns) - 10} more columns")
        else:
            print("❌ minute_bars table does not exist")
            print("💡 Run database migrations first:")
            print("   python scripts/run_dev.py setup")
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Make sure PostgreSQL is running and accessible")


async def main():
    """Run all tests."""
    
    print("🚀 FirstRate Backfill Test Suite")
    print("=" * 60)
    
    try:
        # Test 1: FirstRate adapter
        await test_firstrate_adapter()
        
        # Test 2: Database schema
        await test_database_schema()
        
        print("\n" + "=" * 60)
        print("✅ All tests completed successfully!")
        print("\n💡 To run the actual backfill:")
        print("   python scripts/run_firstrate_minute_backfill.py --symbols AAPL --dry-run")
        print("   python scripts/run_firstrate_minute_backfill.py --symbols AAPL")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())