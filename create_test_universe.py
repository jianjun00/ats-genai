#!/usr/bin/env python3
"""
Create Test Universe: Generate New High-Volume Large-Cap Universe (ID 3)
- Uses same criteria as existing universe ID 2
- Fresh population with current market data
- Enables validation of universe entry/exit logic by comparison
- Tests membership dynamics with real-time calculations
"""

import sys
import os
sys.path.append('/home/jianjun/ats-genai-admin/src')

from core.platform.database.connection_manager import get_raw_connection
from psycopg2.extras import RealDictCursor
from datetime import datetime

def create_test_universe():
    """Create a new test universe with same criteria as universe ID 2"""
    
    print("🚀 Creating Test Universe for Entry/Exit Logic Validation...")
    print(f"📅 Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Set environment for integration
    os.environ['ENVIRONMENT'] = 'intg'
    
    with get_raw_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
            print("\n📊 Step 1: Creating new universe definition...")
            
            # Create new universe
            cursor.execute("""
                INSERT INTO intg_universe (id, name, description)
                VALUES (3, 'test_high_volume_large_cap', 
                        'TEST: High-volume large-cap stocks - same criteria as ID 2 for validation testing')
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description
            """)
            
            print("✅ Created universe ID 3: 'test_high_volume_large_cap'")
            
            print("\n📈 Step 2: Clearing existing membership data...")
            
            # Clear any existing membership for this test universe
            cursor.execute("DELETE FROM intg_universe_membership WHERE universe_id = 3")
            deleted_count = cursor.rowcount
            print(f"✅ Cleared {deleted_count} existing membership records")
            
            print("\n🔍 Step 3: Analyzing current volume data...")
            
            # Get qualifying symbols with current data
            cursor.execute("""
                WITH current_volume_analysis AS (
                    SELECT 
                        symbol,
                        AVG(close * volume) as avg_dollar_volume_50d,
                        COUNT(*) as trading_days,
                        AVG(close) as avg_price,
                        MIN(date) as earliest_date,
                        MAX(date) as latest_date
                    FROM intg_daily_prices_polygon 
                    WHERE date >= CURRENT_DATE - INTERVAL '50 days'
                    GROUP BY symbol
                    HAVING COUNT(*) >= 20  -- At least 20 trading days
                        AND AVG(close * volume) >= 100000000  -- $100M daily volume
                )
                SELECT COUNT(*) as qualifying_symbols,
                       MIN(avg_dollar_volume_50d) as min_volume,
                       MAX(avg_dollar_volume_50d) as max_volume,
                       AVG(avg_dollar_volume_50d) as avg_volume
                FROM current_volume_analysis va
                INNER JOIN intg_instruments i ON va.symbol = i.symbol
            """)
            
            volume_stats = cursor.fetchone()
            print(f"✅ Found {volume_stats['qualifying_symbols']} symbols meeting >$100M volume criteria")
            print(f"   📊 Volume range: ${volume_stats['min_volume']:,.0f} - ${volume_stats['max_volume']:,.0f}")
            print(f"   📊 Average volume: ${volume_stats['avg_volume']:,.0f}")
            
            print("\n🏗️  Step 4: Populating new universe with current market data...")
            
            # Populate universe with fresh data using current timestamp
            cursor.execute("""
                WITH current_volume_analysis AS (
                    SELECT 
                        symbol,
                        AVG(close * volume) as avg_dollar_volume_50d,
                        COUNT(*) as trading_days
                    FROM intg_daily_prices_polygon 
                    WHERE date >= CURRENT_DATE - INTERVAL '50 days'
                    GROUP BY symbol
                    HAVING COUNT(*) >= 20 AND AVG(close * volume) >= 100000000
                )
                INSERT INTO intg_universe_membership (universe_id, symbol, start_at, end_at, instrument_id)
                SELECT 
                    3 as universe_id,
                    va.symbol,
                    CURRENT_TIMESTAMP as start_at,  -- Use current time for all entries
                    NULL as end_at,  -- All currently active
                    i.id as instrument_id
                FROM current_volume_analysis va
                INNER JOIN intg_instruments i ON va.symbol = i.symbol
                ORDER BY va.symbol
            """)
            
            new_members = cursor.rowcount
            print(f"✅ Added {new_members} active members to test universe")
            
            print("\n📊 Step 5: Comparing with existing universe ID 2...")
            
            # Compare memberships between old and new universe
            cursor.execute("""
                WITH universe_2_active AS (
                    SELECT symbol FROM intg_universe_membership 
                    WHERE universe_id = 2 AND end_at IS NULL
                ),
                universe_3_active AS (
                    SELECT symbol FROM intg_universe_membership 
                    WHERE universe_id = 3 AND end_at IS NULL
                ),
                comparison AS (
                    SELECT 
                        COALESCE(u2.symbol, u3.symbol) as symbol,
                        CASE WHEN u2.symbol IS NOT NULL THEN 1 ELSE 0 END as in_universe_2,
                        CASE WHEN u3.symbol IS NOT NULL THEN 1 ELSE 0 END as in_universe_3
                    FROM universe_2_active u2 
                    FULL OUTER JOIN universe_3_active u3 ON u2.symbol = u3.symbol
                )
                SELECT 
                    COUNT(*) as total_symbols,
                    SUM(CASE WHEN in_universe_2 = 1 AND in_universe_3 = 1 THEN 1 ELSE 0 END) as in_both,
                    SUM(CASE WHEN in_universe_2 = 1 AND in_universe_3 = 0 THEN 1 ELSE 0 END) as only_in_2,
                    SUM(CASE WHEN in_universe_2 = 0 AND in_universe_3 = 1 THEN 1 ELSE 0 END) as only_in_3
                FROM comparison
            """)
            
            comparison = cursor.fetchone()
            print(f"📊 Universe Comparison:")
            print(f"   🔄 Symbols in both universes: {comparison['in_both']}")
            print(f"   📉 Only in Universe 2 (old): {comparison['only_in_2']}")
            print(f"   📈 Only in Universe 3 (new): {comparison['only_in_3']}")
            
            # Show specific differences
            if comparison['only_in_2'] > 0:
                cursor.execute("""
                    SELECT u2.symbol, u2.start_at 
                    FROM intg_universe_membership u2 
                    WHERE u2.universe_id = 2 AND u2.end_at IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM intg_universe_membership u3 
                        WHERE u3.universe_id = 3 AND u3.symbol = u2.symbol AND u3.end_at IS NULL
                    )
                    ORDER BY u2.symbol
                    LIMIT 10
                """)
                
                old_only = cursor.fetchall()
                print(f"\n📉 Stocks REMOVED from new universe (no longer qualify):")
                for stock in old_only:
                    print(f"   ❌ {stock['symbol']} (was active since {stock['start_at']})")
            
            if comparison['only_in_3'] > 0:
                cursor.execute("""
                    SELECT u3.symbol, u3.start_at
                    FROM intg_universe_membership u3 
                    WHERE u3.universe_id = 3 AND u3.end_at IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM intg_universe_membership u2 
                        WHERE u2.universe_id = 2 AND u2.symbol = u3.symbol AND u2.end_at IS NULL
                    )
                    ORDER BY u3.symbol
                    LIMIT 10
                """)
                
                new_only = cursor.fetchall()
                print(f"\n📈 Stocks ADDED to new universe (newly qualify):")
                for stock in new_only:
                    print(f"   ✅ {stock['symbol']} (added at {stock['start_at']})")
            
            print("\n📊 Step 6: Final statistics...")
            
            # Get final statistics for both universes
            cursor.execute("""
                SELECT 
                    u.id,
                    u.name,
                    COUNT(um.symbol) as total_members,
                    COUNT(CASE WHEN um.end_at IS NULL THEN 1 END) as active_members,
                    MIN(um.start_at) as earliest_entry,
                    MAX(um.start_at) as latest_entry
                FROM intg_universe u
                LEFT JOIN intg_universe_membership um ON u.id = um.universe_id
                WHERE u.id IN (2, 3)
                GROUP BY u.id, u.name
                ORDER BY u.id
            """)
            
            universe_stats = cursor.fetchall()
            
            print("\n" + "="*80)
            print("🎉 TEST UNIVERSE CREATION COMPLETE!")
            print("="*80)
            
            for stats in universe_stats:
                print(f"🌐 Universe {stats['id']} ({stats['name']}):")
                print(f"   📊 Total Members: {stats['total_members']}")
                print(f"   ✅ Active Members: {stats['active_members']}")
                print(f"   📅 Earliest Entry: {stats['earliest_entry']}")
                print(f"   📅 Latest Entry: {stats['latest_entry']}")
                print()
            
            print("🔍 VALIDATION INSIGHTS:")
            if comparison['only_in_2'] > 0:
                print(f"   📉 {comparison['only_in_2']} stocks lost qualification (volume declined)")
            if comparison['only_in_3'] > 0:
                print(f"   📈 {comparison['only_in_3']} stocks gained qualification (volume increased)")
            if comparison['in_both'] > 0:
                print(f"   🔄 {comparison['in_both']} stocks maintained qualification")
            
            print("\n🚀 Next Steps:")
            print("   1. Visit http://localhost:4000 → Universe Analytics")
            print("   2. Compare 'high_volume_large_cap' vs 'test_high_volume_large_cap'") 
            print("   3. Analyze entry/exit patterns and membership differences")
            print("   4. Validate universe population logic is working correctly")
            
            return True

if __name__ == "__main__":
    try:
        success = create_test_universe()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Error creating test universe: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)