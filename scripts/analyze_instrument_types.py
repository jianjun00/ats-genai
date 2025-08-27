#!/usr/bin/env python3
"""
Analyze Instrument Types and Composition

Examines what types of instruments we have in dev_instruments and provides
detailed breakdown by instrument type, exchange, and other characteristics.
"""

import sys
import asyncio

# Add src to Python path
sys.path.insert(0, '/workspace/src')

async def analyze_instrument_types():
    """Analyze instrument types and composition"""
    try:
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)
        
        async with pool.acquire() as conn:
            print("📊 INSTRUMENT TYPE AND COMPOSITION ANALYSIS")
            print("=" * 60)
            
            # 1. Current total count
            total_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
            print(f"\n1️⃣ TOTAL INSTRUMENTS: {total_count:,}")
            
            # 2. Breakdown by instrument type
            print(f"\n2️⃣ INSTRUMENT TYPES:")
            type_breakdown = await conn.fetch("""
                SELECT 
                    COALESCE(type, 'Unknown') as instrument_type,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dev_instruments), 2) as percentage
                FROM dev_instruments 
                GROUP BY type 
                ORDER BY count DESC
            """)
            
            stocks_count = 0
            for row in type_breakdown:
                instrument_type = row['instrument_type']
                count = row['count']
                percentage = row['percentage']
                print(f"  {instrument_type:15} {count:8,} ({percentage:5.1f}%)")
                
                # Count stock-like instruments
                if instrument_type.upper() in ['STOCK', 'COMMON STOCK', 'CS', 'EQUITY']:
                    stocks_count += count
            
            print(f"\n  📈 Estimated Stocks: {stocks_count:,}")
            
            # 3. Breakdown by exchange (top 10)
            print(f"\n3️⃣ TOP EXCHANGES:")
            exchange_breakdown = await conn.fetch("""
                SELECT 
                    COALESCE(exchange, 'Unknown') as exchange_name,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dev_instruments), 2) as percentage
                FROM dev_instruments 
                GROUP BY exchange 
                ORDER BY count DESC
                LIMIT 10
            """)
            
            for row in exchange_breakdown:
                exchange = row['exchange_name']
                count = row['count']
                percentage = row['percentage']
                print(f"  {exchange:15} {count:8,} ({percentage:5.1f}%)")
            
            # 4. Currency breakdown
            print(f"\n4️⃣ CURRENCY BREAKDOWN:")
            currency_breakdown = await conn.fetch("""
                SELECT 
                    COALESCE(currency, 'Unknown') as currency_code,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dev_instruments), 2) as percentage
                FROM dev_instruments 
                GROUP BY currency 
                ORDER BY count DESC
                LIMIT 10
            """)
            
            for row in currency_breakdown:
                currency = row['currency_code']
                count = row['count']
                percentage = row['percentage']
                print(f"  {currency:15} {count:8,} ({percentage:5.1f}%)")
            
            # 5. Sample of actual instruments
            print(f"\n5️⃣ SAMPLE INSTRUMENTS:")
            sample_instruments = await conn.fetch("""
                SELECT symbol, name, exchange, type, currency
                FROM dev_instruments 
                WHERE name IS NOT NULL AND name != ''
                ORDER BY RANDOM()
                LIMIT 10
            """)
            
            print(f"  {'Symbol':8} {'Name':30} {'Exchange':10} {'Type':12} {'Currency'}")
            print(f"  {'-'*8} {'-'*30} {'-'*10} {'-'*12} {'-'*8}")
            for row in sample_instruments:
                symbol = (row['symbol'] or '')[:7]
                name = (row['name'] or '')[:29]
                exchange = (row['exchange'] or '')[:9]
                inst_type = (row['type'] or '')[:11]
                currency = (row['currency'] or '')[:7]
                print(f"  {symbol:8} {name:30} {exchange:10} {inst_type:12} {currency}")
            
            # 6. US Major Exchange Count
            print(f"\n6️⃣ US MAJOR EXCHANGES:")
            us_exchanges = await conn.fetch("""
                SELECT 
                    exchange,
                    COUNT(*) as count
                FROM dev_instruments 
                WHERE exchange IN ('NASDAQ', 'NYSE', 'XNAS', 'XNYS', 'AMEX')
                   OR exchange LIKE '%NASDAQ%' 
                   OR exchange LIKE '%NYSE%'
                GROUP BY exchange
                ORDER BY count DESC
            """)
            
            total_us_major = 0
            for row in us_exchanges:
                exchange = row['exchange']
                count = row['count']
                total_us_major += count
                print(f"  {exchange:15} {count:8,}")
            
            print(f"\n  Total US Major: {total_us_major:,}")
            
            # 7. Instruments with price data
            print(f"\n7️⃣ INSTRUMENTS WITH PRICE DATA:")
            with_price_data = await conn.fetchval("""
                SELECT COUNT(DISTINCT i.symbol) FROM dev_instruments i
                JOIN dev_daily_prices_polygon p ON i.id = p.instrument_id
            """)
            
            price_coverage = (with_price_data / total_count * 100) if total_count > 0 else 0
            print(f"  Instruments with price data: {with_price_data:,} ({price_coverage:.1f}%)")
            
            # 8. Active vs inactive
            print(f"\n8️⃣ ACTIVE STATUS:")
            active_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments WHERE active = true")
            inactive_count = total_count - active_count
            print(f"  Active instruments:   {active_count:,}")
            print(f"  Inactive instruments: {inactive_count:,}")
            
            # 9. Summary assessment
            print(f"\n9️⃣ SUMMARY ASSESSMENT:")
            print("=" * 40)
            
            # Estimate stock percentage
            stock_indicators = ['stock', 'common stock', 'cs', 'equity']
            likely_stocks = 0
            for row in type_breakdown:
                if any(indicator in row['instrument_type'].lower() for indicator in stock_indicators):
                    likely_stocks += row['count']
            
            stock_percentage = (likely_stocks / total_count * 100) if total_count > 0 else 0
            
            print(f"  📊 Total Instruments: {total_count:,}")
            print(f"  📈 Likely Stocks: {likely_stocks:,} ({stock_percentage:.1f}%)")
            print(f"  🇺🇸 US Major Exchanges: {total_us_major:,}")
            print(f"  💰 With Price Data: {with_price_data:,} ({price_coverage:.1f}%)")
            print(f"  ✅ Active: {active_count:,}")
            
            if stock_percentage > 50:
                print(f"  🎯 CONCLUSION: Majority are stocks/equities")
            else:
                print(f"  🎯 CONCLUSION: Mixed instrument types (not just stocks)")
        
        await pool.close()
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(analyze_instrument_types())