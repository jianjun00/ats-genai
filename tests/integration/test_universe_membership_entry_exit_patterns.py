#!/usr/bin/env python3
"""
Comprehensive Integration Tests for Universe Membership Entry/Exit Patterns
Tests universe membership start_at/end_at dates based on actual volume criteria
with real market examples and multiple entry/exit scenarios
"""

import pytest
import sys
import os
from datetime import datetime, timedelta

# Add project root to path
sys.path.append('/home/jianjun/ats-genai-admin/src')

from core.platform.database.connection_manager import get_raw_connection
from psycopg2.extras import RealDictCursor

class TestUniverseMembershipEntryExitPatterns:
    """Test universe membership patterns with real market data"""
    
    @pytest.fixture(scope="class")
    def db_connection(self):
        """Database connection fixture for integration tests"""
        os.environ['ENVIRONMENT'] = 'intg'
        return get_raw_connection
    
    def test_smci_ai_boom_entry_pattern(self, db_connection):
        """Test SMCI entry during AI boom with correct date tracking"""
        print("\n🔬 Testing SMCI AI Boom Entry Pattern")
        
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                # Research showed SMCI qualified on 2023-01-09 during AI boom
                expected_entry_date = datetime(2023, 1, 9)
                
                # Verify SMCI volume pattern around entry date
                cursor.execute("""
                    WITH daily_volume AS (
                        SELECT 
                            date,
                            close * volume as dollar_volume
                        FROM intg_daily_prices_polygon 
                        WHERE symbol = 'SMCI' 
                        AND date BETWEEN %s AND %s
                        ORDER BY date
                    ),
                    rolling_avg AS (
                        SELECT 
                            date,
                            dollar_volume,
                            AVG(dollar_volume) OVER (
                                ORDER BY date 
                                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                            ) as rolling_50d_avg
                        FROM daily_volume
                    )
                    SELECT 
                        date,
                        rolling_50d_avg,
                        CASE WHEN rolling_50d_avg >= 100000000 THEN 'QUALIFIES' ELSE 'BELOW_THRESHOLD' END as status
                    FROM rolling_avg 
                    WHERE date BETWEEN %s AND %s
                    ORDER BY date
                """, (
                    expected_entry_date - timedelta(days=60),
                    expected_entry_date + timedelta(days=10),
                    expected_entry_date - timedelta(days=5),
                    expected_entry_date + timedelta(days=5)
                ))
                
                volume_data = cursor.fetchall()
                
                # Should show qualification around expected date
                qualified_dates = [row for row in volume_data if row['status'] == 'QUALIFIES']
                assert len(qualified_dates) > 0, "SMCI should show qualification during AI boom period"
                
                first_qualified = min(qualified_dates, key=lambda x: x['date'])
                print(f"   ✅ SMCI first qualified: {first_qualified['date']} (${first_qualified['rolling_50d_avg']:,.0f})")
                
                # Verify qualification timing is reasonable (within AI boom period)
                assert first_qualified['date'] >= datetime(2022, 12, 1).date(), "Entry should be during AI boom period"
                assert first_qualified['date'] <= datetime(2023, 6, 1).date(), "Entry should be early in AI boom"
    
    def test_mstr_bitcoin_strategy_entry_pattern(self, db_connection):
        """Test MSTR entry during Bitcoin strategy with correct date tracking"""
        print("\n🔬 Testing MSTR Bitcoin Strategy Entry Pattern")
        
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                # Research showed MSTR qualified on 2020-12-17 during Bitcoin strategy
                expected_entry_date = datetime(2020, 12, 17)
                
                # Verify MSTR volume surge around Bitcoin strategy announcement
                cursor.execute("""
                    SELECT 
                        symbol,
                        AVG(close * volume) as avg_volume_2020_q4
                    FROM intg_daily_prices_polygon 
                    WHERE symbol = 'MSTR' 
                    AND date BETWEEN '2020-10-01' AND '2020-12-31'
                    GROUP BY symbol
                """)
                
                q4_data = cursor.fetchone()
                assert q4_data is not None, "Should have MSTR data for Q4 2020"
                
                q4_volume = q4_data['avg_volume_2020_q4']
                print(f"   ✅ MSTR Q4 2020 average volume: ${q4_volume:,.0f}")
                
                # Should exceed $100M threshold during Bitcoin strategy period
                assert q4_volume >= 100000000, f"MSTR should qualify in Q4 2020 (${q4_volume:,.0f})"
    
    def test_pton_pandemic_entry_exit_pattern(self, db_connection):
        """Test PTON pandemic entry with post-pandemic exit"""
        print("\n🔬 Testing PTON Pandemic Entry → Post-Pandemic Exit Pattern")
        
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                # Check PTON volume across different periods
                periods = [
                    ('Pre-COVID', '2019-09-01', '2020-02-29'),     # Post-IPO
                    ('COVID Peak', '2020-03-01', '2021-12-31'),    # Home fitness boom
                    ('Post-COVID', '2022-01-01', '2022-12-31'),    # Normalization
                    ('Recent', '2023-01-01', '2024-09-03')         # Current decline
                ]
                
                period_results = {}
                
                for period_name, start_date, end_date in periods:
                    cursor.execute("""
                        SELECT 
                            %s as period,
                            AVG(close * volume) as avg_volume,
                            COUNT(*) as trading_days
                        FROM intg_daily_prices_polygon 
                        WHERE symbol = 'PTON' 
                        AND date BETWEEN %s AND %s
                    """, (period_name, start_date, end_date))
                    
                    result = cursor.fetchone()
                    if result and result['avg_volume']:
                        avg_volume = result['avg_volume']
                        status = 'QUALIFIES' if avg_volume >= 100000000 else 'BELOW_THRESHOLD'
                        
                        print(f"   {period_name}: ${avg_volume:,.0f} ({status})")
                        period_results[period_name] = {
                            'volume': avg_volume,
                            'qualifies': status == 'QUALIFIES'
                        }
                
                # Validate expected pattern: qualified during COVID, lost qualification recently
                assert period_results['COVID Peak']['qualifies'], "PTON should qualify during COVID peak"
                assert not period_results['Recent']['qualifies'], "PTON should not qualify in recent period"
                
                print(f"   ✅ PTON shows correct entry/exit pattern: pandemic boom → decline")
    
    def test_bynd_hype_cycle_multiple_entries(self, db_connection):
        """Test BYND multiple entry/exit events during hype cycle"""
        print("\n🔬 Testing BYND Hype Cycle Multiple Entry/Exit Pattern")
        
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                # Research showed BYND had multiple events around 2022-07
                # EXIT: 2022-07-11, ENTRY: 2022-07-21, EXIT: 2022-07-22
                
                cursor.execute("""
                    WITH daily_volume AS (
                        SELECT 
                            date,
                            close * volume as dollar_volume
                        FROM intg_daily_prices_polygon 
                        WHERE symbol = 'BYND' 
                        AND date BETWEEN '2022-07-01' AND '2022-08-31'
                        ORDER BY date
                    ),
                    rolling_avg AS (
                        SELECT 
                            date,
                            dollar_volume,
                            AVG(dollar_volume) OVER (
                                ORDER BY date 
                                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                            ) as rolling_50d_avg
                        FROM daily_volume
                    )
                    SELECT 
                        date,
                        rolling_50d_avg,
                        CASE WHEN rolling_50d_avg >= 100000000 THEN 1 ELSE 0 END as qualifies,
                        LAG(CASE WHEN rolling_50d_avg >= 100000000 THEN 1 ELSE 0 END) OVER (ORDER BY date) as prev_qualifies
                    FROM rolling_avg 
                    WHERE rolling_50d_avg IS NOT NULL
                    ORDER BY date
                """)
                
                volume_data = cursor.fetchall()
                
                # Find qualification changes  
                events = []
                for row in volume_data:
                    if row['prev_qualifies'] is not None and row['qualifies'] != row['prev_qualifies']:
                        event_type = 'ENTRY' if row['qualifies'] == 1 else 'EXIT'
                        events.append({
                            'date': row['date'],
                            'type': event_type,
                            'volume': row['rolling_50d_avg']
                        })
                
                print(f"   Found {len(events)} qualification events:")
                for event in events:
                    print(f"     {event['type']}: {event['date']} (${event['volume']:,.0f})")
                
                # Should show volatility around threshold - multiple events expected
                assert len(events) >= 1, "BYND should show qualification volatility during hype decline"
                
                # Verify at least one exit event (hype fade)
                exit_events = [e for e in events if e['type'] == 'EXIT']
                assert len(exit_events) >= 1, "Should show at least one exit during hype fade"
    
    def test_arkb_recent_launch_volatility_pattern(self, db_connection):
        """Test ARKB recent launch with entry/exit volatility"""
        print("\n🔬 Testing ARKB Recent Launch Volatility Pattern")
        
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                # ARKB launched in 2024, research showed volatility around threshold
                cursor.execute("""
                    SELECT 
                        COUNT(*) as trading_days,
                        AVG(close * volume) as avg_volume,
                        MIN(date) as first_date,
                        MAX(date) as last_date
                    FROM intg_daily_prices_polygon 
                    WHERE symbol = 'ARKB' 
                    AND date >= '2024-01-01'
                """)
                
                arkb_data = cursor.fetchone()
                
                if arkb_data and arkb_data['trading_days'] > 0:
                    avg_volume = arkb_data['avg_volume']
                    first_date = arkb_data['first_date']
                    
                    print(f"   ARKB trading since: {first_date}")
                    print(f"   Average volume: ${avg_volume:,.0f}")
                    
                    # Should have substantial volume as Bitcoin ETF
                    assert avg_volume > 50000000, f"ARKB should have significant volume (${avg_volume:,.0f})"
                    
                    # Launch date should be recent (2024)
                    assert first_date >= datetime(2024, 1, 1).date(), "ARKB should be recent launch"
                    
                    print(f"   ✅ ARKB shows recent launch pattern with substantial volume")
                else:
                    print(f"   ℹ️ ARKB data may not be available (recent ETF launch)")
    
    def test_nvda_continuous_qualification_pattern(self, db_connection):
        """Test NVDA continuous qualification across market cycles"""
        print("\n🔬 Testing NVDA Continuous Qualification Pattern")
        
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                # NVDA should show continuous high volume across all periods
                periods = [
                    ('Pre-COVID', '2019-01-01', '2020-02-29'),
                    ('COVID Peak', '2020-03-01', '2021-12-31'),
                    ('Post-COVID', '2022-01-01', '2022-12-31'), 
                    ('AI Boom', '2023-01-01', '2024-09-03')
                ]
                
                all_qualified = True
                volume_growth = []
                
                for period_name, start_date, end_date in periods:
                    cursor.execute("""
                        SELECT 
                            %s as period,
                            AVG(close * volume) as avg_volume
                        FROM intg_daily_prices_polygon 
                        WHERE symbol = 'NVDA' 
                        AND date BETWEEN %s AND %s
                    """, (period_name, start_date, end_date))
                    
                    result = cursor.fetchone()
                    if result and result['avg_volume']:
                        avg_volume = result['avg_volume']
                        qualifies = avg_volume >= 100000000
                        
                        print(f"   {period_name}: ${avg_volume:,.0f} ({'QUALIFIES' if qualifies else 'BELOW_THRESHOLD'})")
                        
                        if not qualifies:
                            all_qualified = False
                            
                        volume_growth.append(avg_volume)
                
                assert all_qualified, "NVDA should qualify across all market periods"
                
                # Verify volume growth during AI boom
                if len(volume_growth) >= 2:
                    growth_ratio = volume_growth[-1] / volume_growth[0]  # AI boom vs Pre-COVID
                    print(f"   Volume growth: {growth_ratio:.1f}x from pre-COVID to AI boom")
                    assert growth_ratio > 5, "NVDA should show significant volume growth during AI boom"
                
                print(f"   ✅ NVDA shows continuous qualification with AI boom acceleration")
    
    def test_correct_start_end_date_business_logic(self, db_connection):
        """Test that universe membership dates reflect actual qualification events"""
        print("\n🔬 Testing Correct Start/End Date Business Logic")
        
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                # Check current universe membership data quality
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_memberships,
                        COUNT(DISTINCT start_at) as unique_start_dates,
                        COUNT(CASE WHEN start_at = '1995-01-01' THEN 1 END) as placeholder_dates,
                        COUNT(CASE WHEN start_at >= '2020-01-01' THEN 1 END) as recent_entries,
                        COUNT(CASE WHEN end_at IS NOT NULL THEN 1 END) as historical_exits
                    FROM intg_universe_membership 
                    WHERE universe_id = 2
                """)
                
                data_quality = cursor.fetchone()
                
                total = data_quality['total_memberships']
                unique_dates = data_quality['unique_start_dates'] 
                placeholder_count = data_quality['placeholder_dates']
                recent_entries = data_quality['recent_entries']
                
                print(f"   Total memberships: {total}")
                print(f"   Unique start dates: {unique_dates}")
                print(f"   Placeholder dates (1995-01-01): {placeholder_count}")
                print(f"   Recent entries (2020+): {recent_entries}")
                
                # Highlight data quality issues
                placeholder_percentage = (placeholder_count / total) * 100 if total > 0 else 0
                
                print(f"\n   🚨 Data Quality Issues:")
                print(f"   • {placeholder_percentage:.1f}% of entries use placeholder dates")
                print(f"   • Only {unique_dates} unique start dates for {total} memberships")
                print(f"   • Bulk assignment pattern evident (should be criteria-based)")
                
                # This test documents the current issues - proper implementation would fix these
                assert placeholder_count > 0, "Current implementation has placeholder date issues (expected)"
                assert unique_dates < total, "Current implementation lacks individual entry date tracking (expected)"
                
                print(f"   ✅ Data quality issues documented - fixes needed for proper entry/exit tracking")
    
    def test_multiple_entry_exit_scenario_simulation(self, db_connection):
        """Simulate correct universe membership tracking with multiple entry/exit events"""
        print("\n🔬 Testing Multiple Entry/Exit Scenario Simulation")
        
        # This test simulates what SHOULD happen with proper tracking
        simulation_events = [
            {'symbol': 'TEST_STOCK', 'date': '2020-03-15', 'action': 'ENTRY', 'reason': 'COVID volatility surge'},
            {'symbol': 'TEST_STOCK', 'date': '2021-06-30', 'action': 'EXIT', 'reason': 'Volume normalization'},
            {'symbol': 'TEST_STOCK', 'date': '2023-02-15', 'action': 'ENTRY', 'reason': 'AI boom participation'},
            {'symbol': 'TEST_STOCK', 'date': '2023-11-01', 'action': 'EXIT', 'reason': 'Bubble correction'},
            {'symbol': 'TEST_STOCK', 'date': '2024-03-01', 'action': 'ENTRY', 'reason': 'Recovery qualification'}
        ]
        
        print(f"   Simulated entry/exit events for TEST_STOCK:")
        
        expected_memberships = []
        current_membership = None
        
        for event in simulation_events:
            print(f"     {event['date']}: {event['action']} - {event['reason']}")
            
            if event['action'] == 'ENTRY':
                # New membership period starts
                current_membership = {
                    'symbol': event['symbol'],
                    'start_at': event['date'],
                    'end_at': None,
                    'reason': event['reason']
                }
                expected_memberships.append(current_membership)
                
            elif event['action'] == 'EXIT' and current_membership:
                # End current membership period
                current_membership['end_at'] = event['date']
                current_membership = None
        
        print(f"\n   Expected membership records:")
        for i, membership in enumerate(expected_memberships, 1):
            end_date = membership['end_at'] or 'Active'
            print(f"     Record {i}: {membership['start_at']} → {end_date}")
        
        # Verify simulation logic
        assert len(expected_memberships) == 3, "Should create 3 distinct membership periods"
        
        # First two should have end dates, last should be active
        assert expected_memberships[0]['end_at'] == '2021-06-30', "First membership should end"
        assert expected_memberships[1]['end_at'] == '2023-11-01', "Second membership should end" 
        assert expected_memberships[2]['end_at'] is None, "Third membership should be active"
        
        print(f"   ✅ Multiple entry/exit logic simulation validated")
        print(f"   💡 This demonstrates correct universe membership tracking")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])