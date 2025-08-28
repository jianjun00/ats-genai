#!/usr/bin/env python3
"""
Simple validation script for Tiingo end_date issues
Runs without external dependencies - pure asyncpg and psycopg2
"""

import sys
import os
import asyncio
from datetime import datetime, date, timedelta

# Add src to path for database utilities if needed
sys.path.insert(0, '/workspace/src')

async def main():
    """Main validation function"""
    
    # Use psycopg2 since it's more likely to be available
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    print("🧪 Tiingo End Date Validation Test")
    print("=" * 50)
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host='postgres',  # Docker service name
            port=5432,
            user='postgres',
            password='dev_password', 
            database='dev_db'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        print("✅ Database connection established")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    try:
        # Test 1: Check active companies with problematic end_dates
        print("\n📊 Test 1: Active companies with recent end_dates (PROBLEMATIC)")
        
        active_symbols = ['GM', 'DELL', 'F', 'AAPL', 'MSFT', 'GOOGL', 'TSLA']
        placeholders = ', '.join(['%s'] * len(active_symbols))
        
        query = f"""
        SELECT symbol, end_date, name,
               CASE WHEN end_date IS NULL THEN 'NULL' ELSE end_date::text END as end_date_str
        FROM dev_instrument_tiingo 
        WHERE symbol IN ({placeholders})
        ORDER BY symbol
        """
        
        cur.execute(query, active_symbols)
        results = cur.fetchall()
        
        issues_found = 0
        for row in results:
            symbol = row['symbol']
            end_date = row['end_date']
            end_date_str = row['end_date_str']
            
            if end_date is None:
                print(f"✅ {symbol}: Correctly has NULL end_date")
            else:
                # Check if it's a recent date (problematic)
                if isinstance(end_date, date):
                    days_ago = (datetime.now().date() - end_date).days
                    if days_ago <= 7:
                        print(f"❌ {symbol}: Has recent end_date {end_date_str} ({days_ago} days ago) - PROBLEMATIC")
                        issues_found += 1
                    else:
                        print(f"⚠️ {symbol}: Has old end_date {end_date_str} ({days_ago} days ago)")
                else:
                    print(f"🔍 {symbol}: Has end_date {end_date_str}")
        
        print(f"\n📋 Found {issues_found} active stocks with recent end_dates (should be 0)")
        
        # Test 2: Overall data quality metrics
        print("\n📊 Test 2: Overall Tiingo data quality metrics")
        
        cur.execute("""
        SELECT 
            COUNT(*) as total_instruments,
            COUNT(CASE WHEN end_date IS NULL THEN 1 END) as active_instruments,
            COUNT(CASE WHEN end_date IS NOT NULL THEN 1 END) as delisted_instruments,
            COUNT(CASE WHEN end_date > CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_end_dates,
            ROUND(
                COUNT(CASE WHEN end_date IS NULL THEN 1 END)::numeric / COUNT(*)::numeric * 100, 2
            ) as active_percentage
        FROM dev_instrument_tiingo
        """)
        
        metrics = cur.fetchone()
        
        total = metrics['total_instruments']
        active = metrics['active_instruments']
        delisted = metrics['delisted_instruments'] 
        recent_end_dates = metrics['recent_end_dates']
        active_pct = metrics['active_percentage']
        
        print(f"   Total instruments: {total:,}")
        print(f"   Active (NULL end_date): {active:,} ({active_pct}%)")
        print(f"   Delisted (has end_date): {delisted:,}")
        print(f"   Recent end_dates (PROBLEMATIC): {recent_end_dates:,}")
        
        # Test 3: Known historical validations
        print("\n📊 Test 3: Historical accuracy validation")
        
        historical_cases = [
            ('BBBYQ', 'Bed Bath & Beyond', date(2023, 9, 29)),
            ('GM', 'General Motors', None),  # Should be active
            ('DELL', 'Dell Technologies', None),  # Should be active
            ('F', 'Ford Motor Company', None),  # Should be active
        ]
        
        for symbol, description, expected_end_date in historical_cases:
            cur.execute("SELECT symbol, end_date FROM dev_instrument_tiingo WHERE symbol = %s", (symbol,))
            result = cur.fetchone()
            
            if result:
                actual_end_date = result['end_date']
                if expected_end_date is None:
                    # Should be active (NULL end_date)
                    if actual_end_date is None:
                        print(f"✅ {symbol} ({description}): Correctly active (NULL end_date)")
                    else:
                        print(f"❌ {symbol} ({description}): Should be active but has end_date {actual_end_date}")
                else:
                    # Should have specific end_date
                    if actual_end_date == expected_end_date:
                        print(f"✅ {symbol} ({description}): Correctly delisted on {actual_end_date}")
                    else:
                        print(f"❌ {symbol} ({description}): Expected {expected_end_date}, got {actual_end_date}")
            else:
                print(f"⚠️ {symbol} ({description}): Not found in database")
        
        # Test 4: Identify fix requirements
        print("\n📊 Test 4: Fix requirements analysis")
        
        cur.execute("""
        SELECT 
            COUNT(CASE WHEN end_date > CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as needs_null_fix,
            COUNT(CASE WHEN end_date <= CURRENT_DATE - INTERVAL '7 days' AND end_date IS NOT NULL THEN 1 END) as keep_old_dates
        FROM dev_instrument_tiingo
        """)
        
        fix_analysis = cur.fetchone()
        needs_null = fix_analysis['needs_null_fix']
        keep_old = fix_analysis['keep_old_dates']
        
        print(f"   Instruments needing NULL fix (recent end_dates): {needs_null:,}")
        print(f"   Instruments keeping old end_dates: {keep_old:,}")
        
        # Summary
        print("\n" + "=" * 50)
        if issues_found > 0 or recent_end_dates > 0:
            print(f"❌ VALIDATION FAILED: Found {max(issues_found, recent_end_dates)} data quality issues")
            print("🔧 FIX REQUIRED: Implement Tiingo end_date correction logic")
        else:
            print("✅ VALIDATION PASSED: Tiingo end_date data looks correct")
            
    except Exception as e:
        print(f"💥 Test execution error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        cur.close()
        conn.close()
        print("🔌 Database connection closed")

if __name__ == "__main__":
    asyncio.run(main())