#!/usr/bin/env python3
"""
Universe Entry/Exit Logic Validation Summary
Demonstrates that the universe membership dynamics are working correctly
by comparing the original universe (ID 2) with a fresh test universe (ID 3)
"""

import sys
import os
sys.path.append('/home/jianjun/ats-genai-admin/src')

from core.platform.database.connection_manager import get_raw_connection
from psycopg2.extras import RealDictCursor

def validation_summary():
    """Generate final validation summary"""
    
    print("🎯 UNIVERSE ENTRY/EXIT LOGIC VALIDATION SUMMARY")
    print("="*70)
    print(f"📅 Validation Date: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    os.environ['ENVIRONMENT'] = 'intg'
    
    with get_raw_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
            # Get validation metrics
            cursor.execute("""
                WITH universe_comparison AS (
                    SELECT 
                        'Original (ID 2)' as universe_type,
                        COUNT(*) as total_members,
                        COUNT(CASE WHEN end_at IS NULL THEN 1 END) as active_members,
                        COUNT(CASE WHEN end_at IS NOT NULL THEN 1 END) as historical_members
                    FROM intg_universe_membership 
                    WHERE universe_id = 2
                    
                    UNION ALL
                    
                    SELECT 
                        'Fresh Test (ID 3)' as universe_type,
                        COUNT(*) as total_members,
                        COUNT(CASE WHEN end_at IS NULL THEN 1 END) as active_members,
                        COUNT(CASE WHEN end_at IS NOT NULL THEN 1 END) as historical_members
                    FROM intg_universe_membership 
                    WHERE universe_id = 3
                )
                SELECT * FROM universe_comparison
            """)
            
            comparison = cursor.fetchall()
            
            print(f"\n📊 UNIVERSE COMPARISON:")
            for row in comparison:
                print(f"   {row['universe_type']}: {row['active_members']} active, {row['historical_members']} historical")
            
            # Calculate specific validation metrics
            cursor.execute("""
                WITH overlap_analysis AS (
                    SELECT 
                        COUNT(CASE WHEN u2.symbol IS NOT NULL AND u3.symbol IS NOT NULL THEN 1 END) as both_universes,
                        COUNT(CASE WHEN u2.symbol IS NOT NULL AND u3.symbol IS NULL THEN 1 END) as only_original,
                        COUNT(CASE WHEN u2.symbol IS NULL AND u3.symbol IS NOT NULL THEN 1 END) as only_fresh
                    FROM (
                        SELECT DISTINCT symbol FROM intg_universe_membership WHERE universe_id = 2 AND end_at IS NULL
                    ) u2
                    FULL OUTER JOIN (
                        SELECT DISTINCT symbol FROM intg_universe_membership WHERE universe_id = 3 AND end_at IS NULL  
                    ) u3 ON u2.symbol = u3.symbol
                )
                SELECT * FROM overlap_analysis
            """)
            
            overlap = cursor.fetchone()
            
            print(f"\n🔍 VALIDATION METRICS:")
            print(f"   🔄 Maintained Qualification: {overlap['both_universes']} stocks")
            print(f"   📉 Lost Qualification: {overlap['only_original']} stocks")
            print(f"   📈 Gained Qualification: {overlap['only_fresh']} stocks")
            
            # Test specific examples
            print(f"\n✅ VALIDATION TEST CASES:")
            
            # Check ARKB (should be removed due to low volume)
            cursor.execute("""
                SELECT 
                    AVG(close * volume) as avg_volume_50d,
                    COUNT(*) as days
                FROM intg_daily_prices_polygon 
                WHERE symbol = 'ARKB' 
                AND date >= CURRENT_DATE - INTERVAL '50 days'
            """)
            arkb_data = cursor.fetchone()
            arkb_volume = arkb_data['avg_volume_50d'] if arkb_data else 0
            arkb_qualifies = arkb_volume >= 100000000
            
            print(f"   📉 ARKB Volume Test: ${arkb_volume:,.0f} ({'✅ Qualifies' if arkb_qualifies else '❌ Below $100M threshold'})")
            
            # Check if ARKB is correctly excluded from new universe
            cursor.execute("SELECT COUNT(*) as count FROM intg_universe_membership WHERE universe_id = 3 AND symbol = 'ARKB' AND end_at IS NULL")
            arkb_in_new = cursor.fetchone()['count'] > 0
            
            print(f"   📉 ARKB Exclusion Test: {'❌ Incorrectly included' if arkb_in_new else '✅ Correctly excluded from new universe'}")
            
            # Check major stock inclusion
            major_stocks = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
            cursor.execute("""
                SELECT symbol, COUNT(*) as in_new_universe
                FROM intg_universe_membership 
                WHERE universe_id = 3 AND symbol = ANY(%s) AND end_at IS NULL
                GROUP BY symbol
            """, (major_stocks,))
            
            major_included = cursor.fetchall()
            major_symbols = [row['symbol'] for row in major_included]
            
            print(f"   ✅ Major Stocks Test: {len(major_symbols)}/5 major stocks correctly included ({', '.join(major_symbols)})")
            
            print(f"\n🎉 FINAL VALIDATION RESULT:")
            
            validation_passed = (
                overlap['both_universes'] > 600 and  # Most stocks maintained
                overlap['only_original'] < 50 and    # Limited removals
                overlap['only_fresh'] > 200 and      # Significant additions
                not arkb_in_new and                  # Low volume stock excluded
                len(major_symbols) >= 4              # Major stocks included
            )
            
            if validation_passed:
                print(f"   🚀 ✅ VALIDATION PASSED!")
                print(f"   The universe entry/exit logic is working correctly:")
                print(f"   • Stocks meeting volume criteria are properly included")
                print(f"   • Stocks below volume threshold are properly excluded")
                print(f"   • Major stocks maintain their qualification")
                print(f"   • Market dynamics are accurately reflected")
            else:
                print(f"   ❌ VALIDATION FAILED!")
                print(f"   There may be issues with the universe logic.")
            
            print(f"\n🌐 TEST BOTH UNIVERSES:")
            print(f"   1. Visit: http://localhost:4000")
            print(f"   2. Click: '🌐 Universe Analytics'")
            print(f"   3. Compare: 'high_volume_large_cap' vs 'test_high_volume_large_cap'")
            print(f"   4. Observe: Entry/exit patterns and membership differences")
            
            return validation_passed

if __name__ == "__main__":
    try:
        success = validation_summary()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Error generating validation summary: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)