#!/usr/bin/env python3
"""
Comprehensive Integration Tests for Universe Analytics
Uses real stock examples to validate membership dynamics functionality
"""

import pytest
import asyncio
import sys
import os
from datetime import datetime, timedelta

# Add project root to path
sys.path.append('/home/jianjun/ats-genai-admin/src')

from core.platform.database.connection_manager import get_raw_connection
from psycopg2.extras import RealDictCursor

class TestUniverseAnalyticsComprehensive:
    """Test Universe Analytics with real stock examples and business logic"""
    
    @pytest.fixture(scope="class")
    def db_connection(self):
        """Database connection fixture for integration tests"""
        os.environ['ENVIRONMENT'] = 'intg'
        # Return the connection manager context
        return get_raw_connection
    
    def test_universe_structure(self, db_connection):
        """Test basic universe structure and schema"""
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Test universe exists
                cursor.execute("SELECT * FROM intg_universe WHERE id = 2")
                universe = cursor.fetchone()
                
                assert universe is not None, "Universe ID 2 should exist"
                assert universe['name'] == 'high_volume_large_cap', "Universe name should match"
                assert 'comprehensive' in universe['description'].lower(), "Description should indicate comprehensive coverage"
    
    def test_comprehensive_stock_coverage(self, db_connection):
        """Test that universe includes major expected stocks across A-Z"""
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Test major tech stocks are included
                expected_major_stocks = [
                    'AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'META', 'AMZN', 
                    'AMD', 'NFLX', 'SPY', 'QQQ'  # Major stocks and ETFs
                ]
                
                for symbol in expected_major_stocks:
                    cursor.execute("""
                        SELECT * FROM intg_universe_membership 
                        WHERE universe_id = 2 AND symbol = %s AND end_at IS NULL
                    """, (symbol,))
                    
                    member = cursor.fetchone()
                    assert member is not None, f"Major stock {symbol} should be active in universe"
                    print(f"✅ {symbol}: Active since {member['start_at']}")
    
    def test_alphabet_coverage(self, db_connection):
        """Test that universe has stocks across A-Z (not just A-B)"""
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        LEFT(symbol, 1) as first_letter,
                        COUNT(*) as count
                    FROM intg_universe_membership 
                    WHERE universe_id = 2 AND end_at IS NULL
                    GROUP BY LEFT(symbol, 1)
                    ORDER BY first_letter
                """)
                
                letter_coverage = cursor.fetchall()
                letters_present = [row['first_letter'] for row in letter_coverage]
                
                # Should have substantial alphabet coverage (not just A-B)
                assert len(letters_present) >= 15, f"Should have A-Z coverage, got: {letters_present}"
                assert 'A' in letters_present, "Should include A stocks"
                assert 'M' in letters_present, "Should include M stocks (MSFT, META)"
                assert 'T' in letters_present, "Should include T stocks (TSLA)"
                assert 'N' in letters_present, "Should include N stocks (NVDA, NFLX)"
                
                print(f"✅ Alphabet coverage: {len(letters_present)} letters represented")
                for row in letter_coverage[:10]:  # Show first 10
                    print(f"   {row['first_letter']}: {row['count']} stocks")
    
    def test_historical_membership_exits(self, db_connection):
        """Test stocks that exited the universe with real business reasons"""
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Test specific stocks that should have historical exits
            historical_stocks = {
                'PTON': {
                    'reason': 'Post-pandemic fitness decline',
                    'expected_start': '2019-09-26',  # Peloton IPO
                    'expected_end': '2022-06-15',    # Post-pandemic decline
                    'volume_drop': 93
                },
                'BYND': {
                    'reason': 'Plant-based hype faded', 
                    'expected_start': '2019-05-02',  # Beyond Meat IPO
                    'expected_end': '2022-03-30',    # Hype decline
                    'volume_drop': 97
                },
                'TDOC': {
                    'reason': 'Telehealth normalization',
                    'expected_start': '2020-03-15',  # COVID telehealth boom
                    'expected_end': '2023-01-15',    # Post-COVID normalization  
                    'volume_drop': 88
                }
            }
            
            for symbol, details in historical_stocks.items():
                cursor.execute("""
                    SELECT * FROM intg_universe_membership 
                    WHERE universe_id = 2 AND symbol = %s AND end_at IS NOT NULL
                """, (symbol,))
                
                member = cursor.fetchone()
                assert member is not None, f"Historical stock {symbol} should exist with end_at"
                assert member['end_at'] is not None, f"{symbol} should have exit date"
                
                # Validate timeframe makes business sense
                start_year = member['start_at'].year
                end_year = member['end_at'].year
                
                if symbol == 'PTON':
                    assert start_year == 2019, f"PTON should start in 2019 (IPO), got {start_year}"
                    assert end_year == 2022, f"PTON should exit in 2022 (post-pandemic), got {end_year}"
                elif symbol == 'BYND':
                    assert start_year == 2019, f"BYND should start in 2019 (IPO), got {start_year}" 
                    assert end_year == 2022, f"BYND should exit in 2022 (hype decline), got {end_year}"
                elif symbol == 'TDOC':
                    assert start_year == 2020, f"TDOC should start in 2020 (COVID), got {start_year}"
                    assert end_year == 2023, f"TDOC should exit in 2023 (normalization), got {end_year}"
                
                print(f"✅ {symbol}: {details['reason']} ({start_year}→{end_year})")
    
    def test_ai_boom_additions(self, db_connection):
        """Test stocks added during AI boom with proper entry dates"""
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            ai_boom_stocks = {
                'SMCI': {
                    'reason': 'AI infrastructure boom',
                    'expected_surge': 56828,  # Volume surge %
                    'entry_year': 2023
                },
                'MSTR': {
                    'reason': 'Bitcoin/AI strategy pivot',
                    'expected_surge': 5468,
                    'entry_year': 2023  
                },
                'MARA': {
                    'reason': 'Crypto mining/AI convergence',
                    'expected_surge': 2178,
                    'entry_year': 2023
                }
            }
            
            for symbol, details in ai_boom_stocks.items():
                cursor.execute("""
                    SELECT * FROM intg_universe_membership 
                    WHERE universe_id = 2 AND symbol = %s 
                    AND start_at >= '2023-01-01' AND end_at IS NULL
                """, (symbol,))
                
                member = cursor.fetchone()
                # Note: These might not exist if not in current qualifying volume criteria
                # But if they exist, they should have correct entry timing
                if member:
                    entry_year = member['start_at'].year
                    assert entry_year == 2023, f"{symbol} AI boom entry should be 2023, got {entry_year}"
                    print(f"✅ {symbol}: {details['reason']} (entered {entry_year})")
                else:
                    print(f"ℹ️  {symbol}: Not currently in universe (may not meet current volume criteria)")
    
    def test_ipo_date_accuracy(self, db_connection):
        """Test that major stocks have accurate IPO/listing dates"""
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Test major stocks with well-known IPO dates
            known_ipo_dates = {
                'AAPL': datetime(1980, 12, 12),  # Apple IPO
                'MSFT': datetime(1986, 3, 13),   # Microsoft IPO  
                'AMZN': datetime(1997, 5, 15),   # Amazon IPO
                'GOOGL': datetime(2004, 8, 19),  # Google IPO
                'TSLA': datetime(2010, 6, 29),   # Tesla IPO
                'META': datetime(2012, 5, 18),   # Meta/Facebook IPO
                'NVDA': datetime(1999, 1, 22),   # NVIDIA IPO
            }
            
            for symbol, expected_date in known_ipo_dates.items():
                cursor.execute("""
                    SELECT start_at FROM intg_universe_membership 
                    WHERE universe_id = 2 AND symbol = %s AND end_at IS NULL
                """, (symbol,))
                
                member = cursor.fetchone()
                assert member is not None, f"{symbol} should be in universe"
                
                actual_date = member['start_at']
                # Allow some flexibility (same year is good enough for validation)
                assert actual_date.year == expected_date.year, \
                    f"{symbol} IPO year should be {expected_date.year}, got {actual_date.year}"
                
                print(f"✅ {symbol}: IPO {expected_date.strftime('%Y-%m-%d')} → Universe {actual_date.strftime('%Y-%m-%d')}")
    
    def test_membership_statistics(self, db_connection):
        """Test overall universe membership statistics"""
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN end_at IS NULL THEN 1 END) as active_members,
                    COUNT(CASE WHEN end_at IS NOT NULL THEN 1 END) as historical_exits,
                    MIN(start_at) as earliest_entry,
                    MAX(CASE WHEN end_at IS NOT NULL THEN end_at END) as latest_exit
                FROM intg_universe_membership 
                WHERE universe_id = 2
            """)
            
            stats = cursor.fetchone()
            
            # Validate expected ranges based on our comprehensive universe
            assert stats['total_records'] >= 650, f"Should have 650+ total records, got {stats['total_records']}"
            assert stats['active_members'] >= 600, f"Should have 600+ active members, got {stats['active_members']}"
            assert stats['historical_exits'] >= 3, f"Should have some historical exits, got {stats['historical_exits']}"
            assert stats['earliest_entry'].year <= 1990, f"Should have entries from 1980s or earlier"
            
            print(f"✅ Universe Statistics:")
            print(f"   Total Records: {stats['total_records']}")
            print(f"   Active Members: {stats['active_members']}") 
            print(f"   Historical Exits: {stats['historical_exits']}")
            print(f"   Date Range: {stats['earliest_entry']} → {stats['latest_exit']}")
    
    def test_volume_criteria_logic(self, db_connection):
        """Test that universe members meet volume criteria using real data"""
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Get recent volume data for active universe members
            cursor.execute("""
                WITH recent_volume AS (
                    SELECT 
                        dp.symbol,
                        AVG(dp.close * dp.volume) as avg_dollar_volume,
                        COUNT(*) as trading_days
                    FROM intg_daily_prices_polygon dp
                    INNER JOIN intg_universe_membership um ON dp.symbol = um.symbol
                    WHERE um.universe_id = 2 AND um.end_at IS NULL
                    AND dp.date >= '2024-08-01' AND dp.date <= '2024-09-03'
                    GROUP BY dp.symbol
                    HAVING COUNT(*) >= 10  -- At least 10 days of data
                )
                SELECT 
                    COUNT(*) as members_with_data,
                    COUNT(CASE WHEN avg_dollar_volume >= 100000000 THEN 1 END) as members_over_100m,
                    AVG(avg_dollar_volume) as avg_volume,
                    MIN(avg_dollar_volume) as min_volume,
                    MAX(avg_dollar_volume) as max_volume
                FROM recent_volume
            """)
            
            volume_stats = cursor.fetchone()
            
            # Most active members should meet the $100M volume criteria
            qualification_rate = (volume_stats['members_over_100m'] / volume_stats['members_with_data']) * 100
            
            assert volume_stats['members_with_data'] > 100, "Should have volume data for many members"
            assert qualification_rate >= 80, f"80%+ of members should meet volume criteria, got {qualification_rate:.1f}%"
            assert volume_stats['max_volume'] > 1000000000, "Should include very high volume stocks (>$1B daily)"
            
            print(f"✅ Volume Criteria Validation:")
            print(f"   Members with recent data: {volume_stats['members_with_data']}")
            print(f"   Meeting $100M criteria: {volume_stats['members_over_100m']} ({qualification_rate:.1f}%)")
            print(f"   Volume range: ${volume_stats['min_volume']:,.0f} → ${volume_stats['max_volume']:,.0f}")
    
    def test_business_sector_diversity(self, db_connection):
        """Test that universe includes diverse business sectors (not just tech)"""
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Check for sector diversity by looking at known stocks from different sectors
            sector_representatives = {
                'Technology': ['AAPL', 'MSFT', 'GOOGL', 'NVDA', 'META'],
                'E-commerce': ['AMZN'],
                'Automotive': ['TSLA'], 
                'Aerospace': ['BA'],  # Boeing
                'Finance': ['BAC', 'JPM'],  # Bank of America, JP Morgan
                'Healthcare': ['JNJ', 'PFE'],  # Johnson & Johnson, Pfizer
                'Consumer': ['KO', 'PG'],  # Coca-Cola, Procter & Gamble
                'ETFs': ['SPY', 'QQQ']
            }
            
            sectors_found = {}
            for sector, symbols in sector_representatives.items():
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM intg_universe_membership 
                    WHERE universe_id = 2 AND end_at IS NULL
                    AND symbol = ANY(%s)
                """, (symbols,))
                
                count = cursor.fetchone()['count']
                if count > 0:
                    sectors_found[sector] = count
            
            # Should have representation from multiple sectors
            assert len(sectors_found) >= 4, f"Should have 4+ sectors represented, got: {list(sectors_found.keys())}"
            assert 'Technology' in sectors_found, "Should include major tech stocks"
            assert 'ETFs' in sectors_found, "Should include major ETFs"
            
            print(f"✅ Sector Diversity:")
            for sector, count in sectors_found.items():
                print(f"   {sector}: {count} stocks")
    
    def test_data_integrity_constraints(self, db_connection):
        """Test database integrity and constraint validation"""
        with db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Test no orphaned memberships (all should have valid instrument_ids)
            cursor.execute("""
                SELECT COUNT(*) as orphaned_count
                FROM intg_universe_membership um
                LEFT JOIN intg_instruments i ON um.instrument_id = i.id
                WHERE um.universe_id = 2 AND i.id IS NULL
            """)
            
            orphaned = cursor.fetchone()['orphaned_count']
            assert orphaned == 0, f"Should have no orphaned memberships, found {orphaned}"
            
            # Test no invalid date ranges (start_at should be <= end_at when end_at exists)
            cursor.execute("""
                SELECT COUNT(*) as invalid_dates
                FROM intg_universe_membership
                WHERE universe_id = 2 AND end_at IS NOT NULL 
                AND start_at > end_at
            """)
            
            invalid_dates = cursor.fetchone()['invalid_dates']
            assert invalid_dates == 0, f"Should have no invalid date ranges, found {invalid_dates}"
            
            # Test no duplicate active memberships (same symbol with multiple NULL end_at)
            cursor.execute("""
                SELECT symbol, COUNT(*) as duplicate_count
                FROM intg_universe_membership
                WHERE universe_id = 2 AND end_at IS NULL
                GROUP BY symbol
                HAVING COUNT(*) > 1
            """)
            
            duplicates = cursor.fetchall()
            assert len(duplicates) == 0, f"Should have no duplicate active memberships, found: {[d['symbol'] for d in duplicates]}"
            
            print("✅ Data Integrity: No orphaned records, invalid dates, or duplicates")


class TestUniverseAnalyticsAPI:
    """Test Universe Analytics API endpoints"""
    
    def test_universes_api_endpoint(self):
        """Test /api/universes endpoint returns proper universe data"""
        import subprocess
        
        # Test API endpoint directly
        result = subprocess.run([
            'curl', '-s', 'http://localhost:4000/api/universes'
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, "API endpoint should be accessible"
        
        # Parse JSON response
        import json
        try:
            data = json.loads(result.stdout)
            assert data['success'] == True, "API should return success=true"
            assert len(data['universes']) >= 1, "Should have at least one universe"
            
            # Find our high volume universe
            high_vol_universe = None
            for universe in data['universes']:
                if universe['id'] == 2:
                    high_vol_universe = universe
                    break
            
            assert high_vol_universe is not None, "Should find high volume universe (ID=2)"
            assert high_vol_universe['name'] == 'high_volume_large_cap', "Universe name should match"
            assert 'comprehensive' in high_vol_universe['description'].lower(), "Should have comprehensive description"
            
            print(f"✅ Universe API: Found universe '{high_vol_universe['name']}'")
            
        except json.JSONDecodeError as e:
            pytest.fail(f"API returned invalid JSON: {e}")
    
    def test_universe_members_api_endpoint(self):
        """Test /api/universe-members/{id} endpoint returns member data"""
        import subprocess
        
        # Test with date range that captures historical changes
        result = subprocess.run([
            'curl', '-s', 
            'http://localhost:4000/api/universe-members/2?date_from=2019-01-01&date_to=2024-12-31'
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, "Members API endpoint should be accessible"
        
        import json
        try:
            data = json.loads(result.stdout)
            assert data['success'] == True, "Members API should return success=true"
            assert 'universe_info' in data, "Should include universe info"
            assert 'members' in data, "Should include members array"
            assert len(data['members']) >= 100, f"Should have substantial member count, got {len(data['members'])}"
            
            # Check for expected stocks
            symbols = [member['symbol'] for member in data['members']]
            expected_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
            
            for symbol in expected_symbols:
                assert symbol in symbols, f"Major stock {symbol} should be in members"
            
            # Check for both active and historical members
            active_members = [m for m in data['members'] if m['end_at'] is None]
            historical_members = [m for m in data['members'] if m['end_at'] is not None]
            
            assert len(active_members) >= 600, f"Should have 600+ active members, got {len(active_members)}"
            assert len(historical_members) >= 3, f"Should have some historical members, got {len(historical_members)}"
            
            print(f"✅ Members API: {len(active_members)} active + {len(historical_members)} historical")
            
        except json.JSONDecodeError as e:
            pytest.fail(f"Members API returned invalid JSON: {e}")


# Pytest configuration
def pytest_configure(config):
    """Configure pytest for integration tests"""
    import os
    os.environ['ENVIRONMENT'] = 'intg'


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v", "--tb=short"])