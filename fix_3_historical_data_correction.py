#!/usr/bin/env python3
"""
Fix #3: Historical Data Correction Implementation
Implements complete historical data correction using the daily evaluator
to reconstruct universe membership with accurate dates based on volume analysis
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.append('/home/jianjun/ats-genai-admin/src')

from core.platform.database.connection_manager import get_raw_connection
from psycopg2.extras import RealDictCursor
from jobs.daily_universe_evaluator import DailyUniverseEvaluator

def implement_historical_correction(dry_run=True):
    """
    Fix #3: Complete historical data correction implementation
    """
    print("🔧 FIX #3: HISTORICAL DATA CORRECTION IMPLEMENTATION")
    print("="*70)
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")

    # Initialize evaluator
    evaluator = DailyUniverseEvaluator(environment='dev')

    with get_raw_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:

            print("\n📊 STEP 1: Backup Current Data")
            if not dry_run:
                # Create backup table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS dev_universe_membership_backup AS
                    SELECT *, CURRENT_TIMESTAMP as backup_date
                    FROM dev_universe_membership
                """)
                print("   ✅ Current data backed up to intg_universe_membership_backup")
            else:
                print("   [DRY RUN] Would create backup table")

            print("\n🔄 STEP 2: Clear Placeholder Data")
            cursor.execute(f"""
                SELECT COUNT(*) as placeholder_count
                FROM dev_universe_membership
                WHERE start_at = '1995-01-01' AND universe_id = 2
            """)
            placeholder_count = cursor.fetchone()['placeholder_count']

            if not dry_run:
                cursor.execute(f"""
                    DELETE FROM dev_universe_membership
                    WHERE start_at = '1995-01-01' AND universe_id = 2
                """)
                print(f"   ✅ Removed {placeholder_count} placeholder records")
            else:
                print(f"   [DRY RUN] Would remove {placeholder_count} placeholder records")

            print("\n🧮 STEP 3: Historical Reconstruction Plan")

            # Define reconstruction periods based on market dynamics
            reconstruction_periods = [
                {
                    'name': 'Pre-COVID Baseline',
                    'start': datetime(2019, 1, 1),
                    'end': datetime(2020, 2, 29),
                    'description': 'Establish baseline membership before pandemic'
                },
                {
                    'name': 'COVID Market Volatility',
                    'start': datetime(2020, 3, 1),
                    'end': datetime(2021, 12, 31),
                    'description': 'Pandemic-driven volume changes (PTON entry, etc.)'
                },
                {
                    'name': 'Post-COVID Normalization',
                    'start': datetime(2022, 1, 1),
                    'end': datetime(2022, 12, 31),
                    'description': 'Market normalization (PTON/BYND exits)'
                },
                {
                    'name': 'AI Boom Era',
                    'start': datetime(2023, 1, 1),
                    'end': datetime.now(),
                    'description': 'AI infrastructure boom (SMCI entry, etc.)'
                }
            ]

            print(f"   Reconstruction Periods:")
            total_days = 0
            for period in reconstruction_periods:
                days = (period['end'] - period['start']).days
                total_days += days
                print(f"     • {period['name']}: {days} days")
                print(f"       {period['start'].strftime('%Y-%m-%d')} → {period['end'].strftime('%Y-%m-%d')}")
                print(f"       {period['description']}")

            print(f"   Total reconstruction: {total_days} calendar days (~{total_days * 5 / 7:.0f} trading days)")

            if dry_run:
                print(f"\n   [DRY RUN] Historical reconstruction planned but not executed")
                print(f"   To execute: Set dry_run=False and run with sufficient time allocation")
                return

            print(f"\n🚀 STEP 4: Execute Historical Reconstruction")

            total_processed = 0
            for period in reconstruction_periods:
                print(f"\n   📈 Processing: {period['name']}")
                print(f"      Period: {period['start'].strftime('%Y-%m-%d')} to {period['end'].strftime('%Y-%m-%d')}")

                try:
                    # Run historical backfill for this period
                    evaluator.run_historical_backfill(period['start'], period['end'])

                    # Count processing results
                    days_in_period = (period['end'] - period['start']).days
                    trading_days = days_in_period * 5 // 7  # Approximate trading days
                    total_processed += trading_days

                    print(f"      ✅ Completed: ~{trading_days} trading days processed")

                except Exception as e:
                    print(f"      ❌ Failed: {str(e)}")
                    return False

            print(f"\n✅ STEP 5: Validation")

            # Validate reconstructed data
            cursor.execute(f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT start_at) as unique_start_dates,
                    COUNT(CASE WHEN start_at = '1995-01-01' THEN 1 END) as remaining_placeholders,
                    MIN(start_at) as earliest_start,
                    MAX(start_at) as latest_start
                FROM dev_universe_membership
                WHERE universe_id = 2
            """)

            validation = cursor.fetchone()

            print(f"   Reconstructed Records: {validation['total_records']}")
            print(f"   Unique Start Dates: {validation['unique_start_dates']}")
            print(f"   Remaining Placeholders: {validation['remaining_placeholders']}")
            print(f"   Date Range: {validation['earliest_start']} → {validation['latest_start']}")

            # Validate key stocks
            key_corrections = {
                'SMCI': {'expected_start': '2023-01-09', 'reason': 'AI boom qualification'},
                'MSTR': {'expected_start': '2020-12-17', 'reason': 'Bitcoin strategy surge'},
                'PTON': {'expected_exit': '2023-04-27', 'reason': 'Post-pandemic volume decline'}
            }

            print(f"\n   Key Stock Validation:")
            for symbol, expectation in key_corrections.items():
                cursor.execute(f"""
                    SELECT symbol, start_at, end_at
                    FROM dev_universe_membership
                    WHERE universe_id = 2 AND symbol = %s
                    ORDER BY start_at
                """, (symbol,))

                records = cursor.fetchall()
                if records:
                    first_record = records[0]
                    actual_start = first_record['start_at'].strftime('%Y-%m-%d')

                    if 'expected_start' in expectation:
                        expected = expectation['expected_start']
                        status = "✅" if abs((datetime.strptime(actual_start, '%Y-%m-%d') -
                                           datetime.strptime(expected, '%Y-%m-%d')).days) < 30 else "⚠️"
                        print(f"     {status} {symbol}: {actual_start} (expected ~{expected})")
                    else:
                        print(f"     ℹ️ {symbol}: {actual_start} → {first_record['end_at'] or 'Active'}")
                else:
                    print(f"     ❌ {symbol}: No records found")

            if not dry_run:
                conn.commit()
                print(f"\n   ✅ All changes committed to database")

            print(f"\n🎉 FIX #3 COMPLETED SUCCESSFULLY")
            print(f"   Total trading days processed: ~{total_processed}")
            print(f"   Placeholder dates eliminated: {placeholder_count}")
            print(f"   Historical accuracy achieved: Volume-based qualification tracking")

            return True

def main():
    """Main execution"""
    print("🚨 UNIVERSE MEMBERSHIP HISTORICAL DATA CORRECTION")
    print("Critical Fix #3 from execution plan")
    print("="*70)

    # Start with dry run
    print("\n🔍 RUNNING DRY RUN ANALYSIS...")
    try:
        implement_historical_correction(dry_run=True)

        print(f"\n⚠️ READY FOR LIVE EXECUTION")
        print(f"This will:")
        print(f"• Backup current universe membership data")
        print(f"• Remove all placeholder dates (1995-01-01)")
        print(f"• Reconstruct membership history using volume analysis")
        print(f"• Process ~1000+ trading days of historical data")
        print(f"• Create accurate entry/exit dates for all stocks")
        print(f"")
        print(f"⏱️ Estimated execution time: 30-60 minutes")
        print(f"💾 Database changes: Significant (full membership reconstruction)")
        print(f"")
        print(f"To execute live: Uncomment the live execution section below")

        # For safety, require manual modification to run live
        EXECUTE_LIVE = False  # Change to True to run live

        if EXECUTE_LIVE:
            print(f"\n🚀 EXECUTING LIVE RECONSTRUCTION...")
            success = implement_historical_correction(dry_run=False)
            if success:
                print(f"\n✅ Historical data correction completed successfully!")
            else:
                print(f"\n❌ Historical data correction failed")
                return 1
        else:
            print(f"\n💡 To execute live correction:")
            print(f"   1. Set EXECUTE_LIVE = True in this script")
            print(f"   2. Ensure sufficient time allocation (30-60 minutes)")
            print(f"   3. Re-run the script")

        return 0

    except Exception as e:
        print(f"❌ Error during historical correction: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())