#!/usr/bin/env python3
"""
Fix Tiingo End Date Misinterpretation Issue

CRITICAL BUG FIX:
- Tiingo's 'endDate' field represents data feed availability, NOT stock delisting
- Recent endDate (within 7 days) = active data feed -> should be NULL
- Old endDate (> 7 days ago) = actual delisting -> preserve date

IMPACT:
- 5,822 instruments currently have incorrect recent end_dates (2025-08-26)
- Major stocks like GM, DELL, F incorrectly marked as "delisted"
- Active percentage incorrectly low at 53.28% (should be >70%)

SOLUTION:
- Set end_date = NULL for instruments with recent endDate (active data feed)  
- Preserve end_date for instruments with old endDate (actual delisting)
- Validate against known market facts
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date, timedelta

def main():
    print("🔧 Fixing Tiingo End Date Misinterpretation Issue")
    print("=" * 60)
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            user='postgres', 
            password='dev_password',
            database='dev_db'
        )
        conn.autocommit = False  # Use transactions
        cur = conn.cursor(cursor_factory=RealDictCursor)
        print("✅ Database connection established")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    try:
        # Step 1: Analyze current state
        print("\n📊 Step 1: Analyzing current data state")
        
        cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN end_date IS NULL THEN 1 END) as currently_null,
            COUNT(CASE WHEN end_date > CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_dates,
            COUNT(CASE WHEN end_date <= CURRENT_DATE - INTERVAL '7 days' AND end_date IS NOT NULL THEN 1 END) as old_dates,
            ROUND(COUNT(CASE WHEN end_date IS NULL THEN 1 END)::numeric / COUNT(*)::numeric * 100, 2) as current_active_pct
        FROM dev_instrument_tiingo
        """)
        
        before_stats = cur.fetchone()
        print(f"   Total instruments: {before_stats['total']:,}")
        print(f"   Currently NULL end_date: {before_stats['currently_null']:,}")
        print(f"   Recent end_dates (will fix): {before_stats['recent_dates']:,}")
        print(f"   Old end_dates (will preserve): {before_stats['old_dates']:,}")
        print(f"   Current active percentage: {before_stats['current_active_pct']}%")
        
        # Step 2: Create backup of current state
        print(f"\n💾 Step 2: Creating backup of current end_date values")
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dev_instrument_tiingo_end_date_backup AS
        SELECT symbol, end_date, NOW() as backup_timestamp
        FROM dev_instrument_tiingo
        WHERE end_date IS NOT NULL
        """)
        
        backup_count = cur.rowcount
        print(f"   ✅ Backed up {backup_count:,} non-null end_date values")
        
        # Step 3: Apply the fix
        print(f"\n🔧 Step 3: Applying end_date interpretation fix")
        
        # Calculate cutoff date (7 days ago)
        cutoff_date = datetime.now().date() - timedelta(days=7)
        print(f"   Using cutoff date: {cutoff_date}")
        print(f"   Logic: end_date > {cutoff_date} -> NULL (active)")
        print(f"   Logic: end_date <= {cutoff_date} -> preserve (delisted)")
        
        # Fix: Set recent end_dates to NULL
        cur.execute("""
        UPDATE dev_instrument_tiingo
        SET end_date = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE end_date > %s
        """, (cutoff_date,))
        
        fixed_count = cur.rowcount
        print(f"   ✅ Fixed {fixed_count:,} instruments (set recent end_dates to NULL)")
        
        # Step 4: Validate critical symbols
        print(f"\n✅ Step 4: Validating critical symbols")
        
        critical_symbols = [
            ('GM', 'General Motors', None),  # Should be active
            ('DELL', 'Dell Technologies', None),  # Should be active  
            ('F', 'Ford Motor Company', None),  # Should be active
            ('AAPL', 'Apple Inc', None),  # Should be active
            ('BBBYQ', 'Bed Bath & Beyond', date(2023, 9, 29)),  # Should be delisted
        ]
        
        validation_passed = True
        for symbol, name, expected_end_date in critical_symbols:
            cur.execute("SELECT symbol, end_date FROM dev_instrument_tiingo WHERE symbol = %s", (symbol,))
            result = cur.fetchone()
            
            if result:
                actual_end_date = result['end_date']
                if expected_end_date is None:
                    # Should be active (NULL)
                    if actual_end_date is None:
                        print(f"   ✅ {symbol} ({name}): Correctly active (NULL)")
                    else:
                        print(f"   ❌ {symbol} ({name}): Should be NULL, got {actual_end_date}")
                        validation_passed = False
                else:
                    # Should have specific date
                    if actual_end_date == expected_end_date:
                        print(f"   ✅ {symbol} ({name}): Correctly delisted on {actual_end_date}")
                    else:
                        print(f"   ❌ {symbol} ({name}): Expected {expected_end_date}, got {actual_end_date}")
                        validation_passed = False
            else:
                print(f"   ⚠️ {symbol} ({name}): Not found")
        
        # Step 5: Final metrics
        print(f"\n📊 Step 5: Final data quality metrics")
        
        cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN end_date IS NULL THEN 1 END) as now_null,
            COUNT(CASE WHEN end_date IS NOT NULL THEN 1 END) as now_delisted,
            COUNT(CASE WHEN end_date > CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as still_recent,
            ROUND(COUNT(CASE WHEN end_date IS NULL THEN 1 END)::numeric / COUNT(*)::numeric * 100, 2) as new_active_pct
        FROM dev_instrument_tiingo
        """)
        
        after_stats = cur.fetchone()
        print(f"   Total instruments: {after_stats['total']:,}")
        print(f"   Active (NULL end_date): {after_stats['now_null']:,} ({after_stats['new_active_pct']}%)")
        print(f"   Delisted (has end_date): {after_stats['now_delisted']:,}")
        print(f"   Still recent (should be 0): {after_stats['still_recent']:,}")
        
        # Calculate improvement
        improvement = after_stats['new_active_pct'] - before_stats['current_active_pct']
        print(f"   📈 Active percentage improvement: +{improvement:.2f}%")
        
        # Step 6: Commit or rollback decision
        if validation_passed and after_stats['still_recent'] == 0 and after_stats['new_active_pct'] > 70:
            print(f"\n✅ All validations passed - COMMITTING changes")
            conn.commit()
            
            print(f"\n🎉 SUCCESS: Tiingo end_date fix completed!")
            print(f"   • Fixed {fixed_count:,} instruments")
            print(f"   • Active rate improved: {before_stats['current_active_pct']}% -> {after_stats['new_active_pct']}%")
            print(f"   • Recent end_dates eliminated: {before_stats['recent_dates']:,} -> {after_stats['still_recent']}")
            print(f"   • Historical dates preserved: {after_stats['now_delisted']:,}")
            
        else:
            print(f"\n❌ Validation failed - ROLLING BACK changes")
            conn.rollback()
            print(f"   Issues found:")
            if not validation_passed:
                print(f"   • Critical symbol validation failed")
            if after_stats['still_recent'] > 0:
                print(f"   • Still have {after_stats['still_recent']} recent end_dates")
            if after_stats['new_active_pct'] <= 70:
                print(f"   • Active percentage too low: {after_stats['new_active_pct']}%")
            
    except Exception as e:
        print(f"💥 Error during fix: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        print("🔄 Changes rolled back due to error")
        
    finally:
        cur.close()
        conn.close()
        print("🔌 Database connection closed")

if __name__ == "__main__":
    main()