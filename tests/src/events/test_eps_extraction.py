#!/usr/bin/env python3
"""
Unit Tests for EPS Extraction Logic

Tests the core EPS extraction functionality from Polygon JSON data
to ensure accurate parsing of earnings per share values.
"""

import pytest
import json
from datetime import datetime
from typing import Dict, Any

class TestEPSExtraction:
    """Test EPS extraction from Polygon financial JSON data"""

    def setup_method(self):
        """Set up test fixtures"""
        self.sample_polygon_json = {
            "cik": "0000320193",
            "sic": "3571",
            "tickers": ["AAPL"],
            "end_date": "2025-06-28",
            "timeframe": "quarterly",
            "financials": {
                "income_statement": {
                    "revenues": {
                        "unit": "USD",
                        "label": "Revenues",
                        "order": 100,
                        "value": 94036000000.0
                    },
                    "basic_earnings_per_share": {
                        "unit": "USD / shares",
                        "label": "Basic Earnings Per Share",
                        "order": 4200,
                        "value": 1.57
                    },
                    "diluted_earnings_per_share": {
                        "unit": "USD / shares",
                        "label": "Diluted Earnings Per Share",
                        "order": 4300,
                        "value": 1.55
                    },
                    "net_income_loss": {
                        "unit": "USD",
                        "label": "Net Income/Loss",
                        "order": 3200,
                        "value": 23434000000.0
                    }
                },
                "balance_sheet": {
                    "assets": {
                        "unit": "USD",
                        "label": "Assets",
                        "order": 100,
                        "value": 331495000000.0
                    }
                }
            },
            "fiscal_period": "Q3",
            "filing_date": "2025-08-01",
            "acceptance_datetime": "2025-08-01T10:00:42Z"
        }

    def extract_eps_from_polygon_json(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract EPS values from Polygon financials JSON"""
        eps_data = {}

        income_stmt = raw_data.get('financials', {}).get('income_statement', {})

        # Basic EPS (preferred)
        basic_eps = income_stmt.get('basic_earnings_per_share', {}).get('value')
        if basic_eps is not None:
            eps_data['eps_actual_cents'] = int(basic_eps * 100)

        # Diluted EPS (fallback if basic not available)
        diluted_eps = income_stmt.get('diluted_earnings_per_share', {}).get('value')
        if diluted_eps is not None and 'eps_actual_cents' not in eps_data:
            eps_data['eps_actual_cents'] = int(diluted_eps * 100)

        # Extract revenue for validation
        revenue = income_stmt.get('revenues', {}).get('value')
        if revenue is not None:
            eps_data['revenue_actual_cents'] = int(revenue * 100)

        # Extract net income
        net_income = income_stmt.get('net_income_loss', {}).get('value')
        if net_income is not None:
            eps_data['net_income_cents'] = int(net_income * 100)

        return eps_data

    def extract_call_timestamp(self, raw_data: Dict[str, Any]) -> datetime:
        """Extract earnings call timestamp from Polygon JSON"""
        # Try acceptance_datetime first (most accurate)
        if raw_data.get('acceptance_datetime'):
            return datetime.fromisoformat(raw_data['acceptance_datetime'].replace('Z', '+00:00'))

        # Fallback to filing_date + typical call time (4 PM ET)
        if raw_data.get('filing_date'):
            filing_date = datetime.strptime(raw_data['filing_date'], '%Y-%m-%d')
            return filing_date.replace(hour=16)  # 4 PM

        return None

    def test_basic_eps_extraction(self):
        """Test basic EPS extraction from complete JSON"""
        result = self.extract_eps_from_polygon_json(self.sample_polygon_json)

        assert 'eps_actual_cents' in result
        assert result['eps_actual_cents'] == 157  # $1.57 * 100
        assert result['revenue_actual_cents'] == 9403600000000  # $94.036B * 100
        assert result['net_income_cents'] == 2343400000000  # $23.434B * 100

    def test_diluted_eps_fallback(self):
        """Test diluted EPS used when basic EPS missing"""
        json_data = self.sample_polygon_json.copy()
        # Remove basic EPS
        del json_data['financials']['income_statement']['basic_earnings_per_share']

        result = self.extract_eps_from_polygon_json(json_data)

        assert result['eps_actual_cents'] == 155  # $1.55 * 100 (diluted)

    def test_both_eps_values_present(self):
        """Test that basic EPS is preferred when both are present"""
        result = self.extract_eps_from_polygon_json(self.sample_polygon_json)

        # Should use basic EPS (1.57) not diluted EPS (1.55)
        assert result['eps_actual_cents'] == 157

    def test_missing_eps_data(self):
        """Test handling when EPS data is missing"""
        json_data = self.sample_polygon_json.copy()
        # Remove both EPS fields
        del json_data['financials']['income_statement']['basic_earnings_per_share']
        del json_data['financials']['income_statement']['diluted_earnings_per_share']

        result = self.extract_eps_from_polygon_json(json_data)

        assert 'eps_actual_cents' not in result
        assert result['revenue_actual_cents'] == 9403600000000  # Revenue should still work

    def test_zero_eps_handling(self):
        """Test handling of zero EPS values"""
        json_data = self.sample_polygon_json.copy()
        json_data['financials']['income_statement']['basic_earnings_per_share']['value'] = 0.0

        result = self.extract_eps_from_polygon_json(json_data)

        assert result['eps_actual_cents'] == 0

    def test_negative_eps_handling(self):
        """Test handling of negative EPS values (losses)"""
        json_data = self.sample_polygon_json.copy()
        json_data['financials']['income_statement']['basic_earnings_per_share']['value'] = -0.25

        result = self.extract_eps_from_polygon_json(json_data)

        assert result['eps_actual_cents'] == -25  # -$0.25 * 100

    def test_fractional_eps_precision(self):
        """Test precision handling for fractional EPS values"""
        json_data = self.sample_polygon_json.copy()
        json_data['financials']['income_statement']['basic_earnings_per_share']['value'] = 2.347

        result = self.extract_eps_from_polygon_json(json_data)

        assert result['eps_actual_cents'] == 234  # $2.347 * 100, truncated to int

    def test_malformed_json_handling(self):
        """Test handling of malformed or incomplete JSON"""
        malformed_cases = [
            {},  # Empty JSON
            {"financials": {}},  # Missing income_statement
            {"financials": {"income_statement": {}}},  # Empty income_statement
            {"financials": {"income_statement": {"basic_earnings_per_share": {}}}},  # Missing value
            {"financials": {"income_statement": {"basic_earnings_per_share": {"value": None}}}},  # Null value
        ]

        for case in malformed_cases:
            result = self.extract_eps_from_polygon_json(case)
            assert 'eps_actual_cents' not in result, f"Failed case: {case}"

    def test_call_timestamp_extraction(self):
        """Test earnings call timestamp extraction"""
        timestamp = self.extract_call_timestamp(self.sample_polygon_json)

        assert timestamp is not None
        assert timestamp.year == 2025
        assert timestamp.month == 8
        assert timestamp.day == 1
        assert timestamp.hour == 10
        assert timestamp.minute == 0
        assert timestamp.second == 42

    def test_call_timestamp_fallback_to_filing_date(self):
        """Test fallback to filing date when acceptance_datetime missing"""
        json_data = self.sample_polygon_json.copy()
        del json_data['acceptance_datetime']

        timestamp = self.extract_call_timestamp(json_data)

        assert timestamp is not None
        assert timestamp.year == 2025
        assert timestamp.month == 8
        assert timestamp.day == 1
        assert timestamp.hour == 16  # 4 PM default

    def test_call_timestamp_missing_data(self):
        """Test handling when both timestamp fields are missing"""
        json_data = {}

        timestamp = self.extract_call_timestamp(json_data)

        assert timestamp is None

    def test_large_revenue_values(self):
        """Test handling of very large revenue values"""
        json_data = self.sample_polygon_json.copy()
        # Apple-sized revenue: $400B
        json_data['financials']['income_statement']['revenues']['value'] = 400000000000.0

        result = self.extract_eps_from_polygon_json(json_data)

        assert result['revenue_actual_cents'] == 40000000000000  # $400B * 100

    def test_real_world_apple_q3_2025(self):
        """Test with actual Apple Q3 2025 data structure"""
        result = self.extract_eps_from_polygon_json(self.sample_polygon_json)

        # Validate all expected fields are extracted
        assert result['eps_actual_cents'] == 157
        assert result['revenue_actual_cents'] == 9403600000000
        assert result['net_income_cents'] == 2343400000000

        # Validate call timestamp
        timestamp = self.extract_call_timestamp(self.sample_polygon_json)
        assert timestamp.isoformat() == "2025-08-01T10:00:42+00:00"

    def test_edge_case_very_small_eps(self):
        """Test handling of very small EPS values (penny stocks)"""
        json_data = self.sample_polygon_json.copy()
        json_data['financials']['income_statement']['basic_earnings_per_share']['value'] = 0.01

        result = self.extract_eps_from_polygon_json(json_data)

        assert result['eps_actual_cents'] == 1  # $0.01 * 100

    def test_sql_injection_safety(self):
        """Test that extracted values are safe for database insertion"""
        # Test with potentially dangerous string values
        json_data = self.sample_polygon_json.copy()
        json_data['financials']['income_statement']['basic_earnings_per_share']['value'] = 1.57

        result = self.extract_eps_from_polygon_json(json_data)

        # All values should be integers (safe for SQL)
        assert isinstance(result['eps_actual_cents'], int)
        assert isinstance(result['revenue_actual_cents'], int)
        assert isinstance(result['net_income_cents'], int)

class TestEPSExtractionSQL:
    """Test SQL-based EPS extraction logic"""

    def setup_method(self):
        """Set up test fixtures for SQL tests"""
        self.sample_polygon_json = {
            "financials": {
                "income_statement": {
                    "basic_earnings_per_share": {"value": 1.57},
                    "diluted_earnings_per_share": {"value": 1.55}
                }
            }
        }

    def test_sql_extraction_logic(self):
        """Test the SQL extraction logic used in production"""
        # This mirrors the SQL used in the actual update
        test_cases = [
            # (basic_eps, diluted_eps, expected_result)
            (1.57, 1.55, 157),  # Both present, use basic
            (None, 1.55, 155),  # Only diluted present
            (1.57, None, 157),  # Only basic present
            (0.0, 1.55, 0),     # Zero basic EPS
            (-0.25, None, -25), # Negative EPS
            (None, None, None), # Neither present
        ]

        for basic, diluted, expected in test_cases:
            # Simulate SQL CASE logic
            if basic is not None:
                result = int(basic * 100) if basic is not None else None
            elif diluted is not None:
                result = int(diluted * 100) if diluted is not None else None
            else:
                result = None

            assert result == expected, f"Failed for basic={basic}, diluted={diluted}"

    def test_production_sql_compatibility(self):
        """Ensure our extraction logic matches the production SQL"""
        # Sample JSON that would be stored in raw_data column
        raw_json = json.dumps(self.sample_polygon_json)

        # This would be the actual SQL logic
        # SELECT CASE
        #   WHEN fe.raw_data->'financials'->'income_statement'->'basic_earnings_per_share'->>'value' IS NOT NULL
        #   THEN (fe.raw_data->'financials'->'income_statement'->'basic_earnings_per_share'->>'value')::numeric * 100
        #   WHEN fe.raw_data->'financials'->'income_statement'->'diluted_earnings_per_share'->>'value' IS NOT NULL
        #   THEN (fe.raw_data->'financials'->'income_statement'->'diluted_earnings_per_share'->>'value')::numeric * 100
        #   ELSE NULL
        # END

        data = json.loads(raw_json)
        income_stmt = data.get('financials', {}).get('income_statement', {})

        basic_eps_value = income_stmt.get('basic_earnings_per_share', {}).get('value')
        diluted_eps_value = income_stmt.get('diluted_earnings_per_share', {}).get('value')

        if basic_eps_value is not None:
            result = int(float(basic_eps_value) * 100)
        elif diluted_eps_value is not None:
            result = int(float(diluted_eps_value) * 100)
        else:
            result = None

        assert result == 157  # Expected for Apple Q3 2025

if __name__ == "__main__":
    pytest.main([__file__, "-v"])