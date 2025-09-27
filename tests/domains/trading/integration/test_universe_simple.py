#!/usr/bin/env python3
"""
Simplified Integration Tests for Universe Analytics
Uses subprocess to avoid database connection complexity
"""

import subprocess
import pytest
import json

class TestUniverseAnalyticsSimple:
    """Simplified tests using command-line queries"""

    def test_universe_exists_and_populated(self):
        """Test that universe ID 2 exists and has substantial membership"""
        result = subprocess.run([
            'python3', 'scripts/run_intg.py', 'query', '--query',
            'SELECT COUNT(*) as total, COUNT(CASE WHEN end_at IS NULL THEN 1 END) as active FROM intg_universe_membership WHERE universe_id = 2'
        ], capture_output=True, text=True, cwd='/home/jianjun/ats-genai-admin')

        assert result.returncode == 0, "Query should succeed"

        # Parse result
        lines = result.stdout.strip().split('\n')
        data_line = None
        for line in lines:
            if '|' in line and 'total' not in line and '--' not in line:
                data_line = line
                break

        assert data_line is not None, "Should find data line"
        parts = [p.strip() for p in data_line.split('|')]
        total_count = int(parts[0])
        active_count = int(parts[1])

        assert total_count >= 650, f"Should have 650+ total records, got {total_count}"
        assert active_count >= 600, f"Should have 600+ active members, got {active_count}"
        print(f"✅ Universe membership: {active_count} active / {total_count} total")

    def test_major_stocks_included(self):
        """Test that major expected stocks are in the universe"""
        major_stocks = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'META', 'AMZN']

        for symbol in major_stocks:
            result = subprocess.run([
                'python3', 'scripts/run_intg.py', 'query', '--query',
                f"SELECT COUNT(*) FROM intg_universe_membership WHERE universe_id = 2 AND symbol = '{symbol}' AND end_at IS NULL"
            ], capture_output=True, text=True, cwd='/home/jianjun/ats-genai-admin')

            assert result.returncode == 0, f"Query for {symbol} should succeed"

            # Parse count
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if line.strip().isdigit():
                    count = int(line.strip())
                    assert count >= 1, f"Major stock {symbol} should be active in universe"
                    break
            else:
                pytest.fail(f"Could not parse count for {symbol}")

        print(f"✅ Major stocks verified: {major_stocks}")

    def test_alphabet_coverage(self):
        """Test that universe has A-Z coverage (not just A-B)"""
        result = subprocess.run([
            'python3', 'scripts/run_intg.py', 'query', '--query',
            "SELECT LEFT(symbol, 1) as letter, COUNT(*) as count FROM intg_universe_membership WHERE universe_id = 2 AND end_at IS NULL GROUP BY LEFT(symbol, 1) ORDER BY letter"
        ], capture_output=True, text=True, cwd='/home/jianjun/ats-genai-admin')

        assert result.returncode == 0, "Alphabet coverage query should succeed"

        # Count distinct letters
        letters = []
        lines = result.stdout.strip().split('\n')
        for line in lines:
            if '|' in line and 'letter' not in line and '--' not in line:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 2 and parts[0].isalpha():
                    letters.append(parts[0])

        assert len(letters) >= 15, f"Should have 15+ letters represented, got {len(letters)}: {letters}"
        assert 'A' in letters, "Should include A stocks"
        assert 'T' in letters, "Should include T stocks (TSLA)"
        assert 'M' in letters, "Should include M stocks (MSFT, META)"

        print(f"✅ Alphabet coverage: {len(letters)} letters ({letters[:10]}...)")

    def test_historical_memberships_exist(self):
        """Test that historical memberships exist (stocks with exit dates)"""
        result = subprocess.run([
            'python3', 'scripts/run_intg.py', 'query', '--query',
            "SELECT COUNT(*) FROM intg_universe_membership WHERE universe_id = 2 AND end_at IS NOT NULL"
        ], capture_output=True, text=True, cwd='/home/jianjun/ats-genai-admin')

        assert result.returncode == 0, "Historical membership query should succeed"

        lines = result.stdout.strip().split('\n')
        for line in lines:
            if line.strip().isdigit():
                count = int(line.strip())
                assert count >= 3, f"Should have some historical exits, got {count}"
                break
        else:
            pytest.fail("Could not parse historical count")

        print(f"✅ Historical exits: {count} stocks removed from universe")

    def test_ipo_date_ranges(self):
        """Test that IPO dates span realistic historical ranges"""
        result = subprocess.run([
            'python3', 'scripts/run_intg.py', 'query', '--query',
            "SELECT MIN(EXTRACT(YEAR FROM start_at)) as min_year, MAX(EXTRACT(YEAR FROM start_at)) as max_year FROM intg_universe_membership WHERE universe_id = 2"
        ], capture_output=True, text=True, cwd='/home/jianjun/ats-genai-admin')

        assert result.returncode == 0, "Date range query should succeed"

        lines = result.stdout.strip().split('\n')
        data_line = None
        for line in lines:
            if '|' in line and 'min_year' not in line and '--' not in line:
                data_line = line
                break

        assert data_line is not None, "Should find date range data"
        parts = [p.strip() for p in data_line.split('|')]
        min_year = int(float(parts[0]))  # Handle potential decimal
        max_year = int(float(parts[1]))

        assert min_year <= 1990, f"Should have entries from 1980s or earlier, got {min_year}"
        assert max_year >= 2010, f"Should have entries from 2010s or later, got {max_year}"
        assert (max_year - min_year) >= 20, f"Should span 20+ years, got {max_year - min_year} years"

        print(f"✅ Date range: {min_year} → {max_year} ({max_year - min_year} year span)")


class TestUniverseAnalyticsAPI:
    """Test API endpoints directly"""

    def test_universes_api_response(self):
        """Test /api/universes endpoint"""
        result = subprocess.run([
            'curl', '-s', 'http://localhost:4000/api/universes'
        ], capture_output=True, text=True)

        assert result.returncode == 0, "API endpoint should be accessible"

        # Parse JSON
        data = json.loads(result.stdout)
        assert data['success'] == True, "API should return success"
        assert len(data['universes']) >= 1, "Should have at least one universe"

        # Find high volume universe
        high_vol = None
        for universe in data['universes']:
            if universe['id'] == 2:
                high_vol = universe
                break

        assert high_vol is not None, "Should find universe ID 2"
        assert high_vol['name'] == 'high_volume_large_cap', "Name should match"
        print(f"✅ Universe API: {high_vol['name']}")

    def test_universe_members_api_response(self):
        """Test /api/universe-members/2 endpoint"""
        result = subprocess.run([
            'curl', '-s',
            'http://localhost:4000/api/universe-members/2?date_from=2020-01-01&date_to=2024-12-31'
        ], capture_output=True, text=True)

        assert result.returncode == 0, "Members API should be accessible"

        data = json.loads(result.stdout)
        assert data['success'] == True, "Members API should return success"
        assert 'members' in data, "Should include members"
        assert len(data['members']) >= 100, f"Should have substantial members, got {len(data['members'])}"

        # Check for expected symbols
        symbols = [m['symbol'] for m in data['members']]
        expected = ['AAPL', 'MSFT', 'TSLA']

        for symbol in expected:
            assert symbol in symbols, f"Should include {symbol}"

        print(f"✅ Members API: {len(data['members'])} total members")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])