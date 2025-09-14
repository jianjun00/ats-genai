#!/usr/bin/env python3
"""
Integration Test for Financial Events with Analytics Service
Following TDD - Write failing test FIRST, then implement integration

Test Requirements:
1. Analytics service should accept POST requests for financial events
2. Financial events should be stored in intg database
3. Financial events should be queryable via GET requests
4. Cache performance should be tracked
"""

import pytest
import asyncio
import json
import requests
import time
from datetime import datetime, timedelta

ANALYTICS_BASE_URL = "http://localhost:4000"

class TestFinancialEventsAnalyticsIntegration:
    """Test financial events integration with analytics service"""

    def test_analytics_service_health(self):
        """Test: Analytics service should be running and healthy"""

        response = requests.get(f"{ANALYTICS_BASE_URL}/health", timeout=5)
        assert response.status_code == 200

        health_data = response.json()
        assert health_data["status"] == "healthy"
        assert "ats-unified-analytics" in health_data["service"]
        print("✅ Analytics service is healthy")

    def test_financial_events_table_creation(self):
        """Test: Should create financial events table in database"""

        # This test expects the analytics service to handle table creation
        create_table_payload = {
            "action": "create_financial_events_table"
        }

        response = requests.post(
            f"{ANALYTICS_BASE_URL}/financial_events/setup",
            json=create_table_payload,
            timeout=30
        )

        # EXPECTED TO FAIL INITIALLY - endpoint doesn't exist yet
        # This drives implementation of the endpoint
        assert response.status_code == 200, f"Table creation failed: {response.status_code}"

        result = response.json()
        assert result["success"] == True
        assert "table_created" in result
        print("✅ Financial events table created successfully")

    def test_extract_and_store_events(self):
        """Test: Should extract events from xAI and store in database"""

        extract_payload = {
            "start_date": "2025-09-01",
            "end_date": "2025-09-13",
            "symbols": ["AAPL", "TSLA", "MSFT"],
            "force_refresh": False
        }

        response = requests.post(
            f"{ANALYTICS_BASE_URL}/financial_events/extract",
            json=extract_payload,
            timeout=60  # xAI API calls may take time
        )

        # EXPECTED TO FAIL INITIALLY - endpoint doesn't exist yet
        assert response.status_code == 200, f"Event extraction failed: {response.status_code}"

        result = response.json()
        assert result["success"] == True
        assert result["events_extracted"] >= 0
        assert result["events_stored"] >= 0
        assert "events_preview" in result

        print(f"✅ Extracted {result['events_extracted']} events, stored {result['events_stored']}")

        # Store results for next test
        self.extraction_result = result

    def test_query_stored_events(self):
        """Test: Should query stored financial events with filters"""

        # Test basic query
        response = requests.get(
            f"{ANALYTICS_BASE_URL}/financial_events?limit=10",
            timeout=30
        )

        # EXPECTED TO FAIL INITIALLY
        assert response.status_code == 200, f"Event query failed: {response.status_code}"

        result = response.json()
        assert result["success"] == True
        assert "events" in result
        assert "count" in result

        print(f"✅ Queried events successfully, found {result['count']} events")

        # Test filtered query
        filtered_response = requests.get(
            f"{ANALYTICS_BASE_URL}/financial_events?impact_level=high&limit=5",
            timeout=30
        )

        assert filtered_response.status_code == 200
        filtered_result = filtered_response.json()
        assert filtered_result["success"] == True

        # Verify filtering worked
        if filtered_result["events"]:
            for event in filtered_result["events"]:
                assert event["impact_level"] == "high"

        print(f"✅ High-impact filter working, found {filtered_result['count']} events")

    def test_events_summary_statistics(self):
        """Test: Should provide summary statistics for stored events"""

        response = requests.get(
            f"{ANALYTICS_BASE_URL}/financial_events/summary",
            timeout=30
        )

        # EXPECTED TO FAIL INITIALLY
        assert response.status_code == 200, f"Summary request failed: {response.status_code}"

        result = response.json()
        assert result["success"] == True
        assert "summary" in result

        summary = result["summary"]
        if summary:
            stats = summary[0] if isinstance(summary, list) else summary
            assert "total_events" in stats
            assert "unique_symbols" in stats
            assert "high_impact_events" in stats

            print(f"✅ Summary statistics: {stats.get('total_events', 0)} total events")

    def test_cache_performance_tracking(self):
        """Test: Should track and report cache performance"""

        response = requests.get(
            f"{ANALYTICS_BASE_URL}/financial_events/cache/stats",
            timeout=30
        )

        # EXPECTED TO FAIL INITIALLY
        assert response.status_code == 200, f"Cache stats failed: {response.status_code}"

        result = response.json()
        assert result["success"] == True
        assert "cache_statistics" in result

        cache_stats = result["cache_statistics"]
        assert "cache_enabled" in cache_stats
        assert "hit_rate" in cache_stats
        assert "total_requests" in cache_stats

        print(f"✅ Cache stats: {cache_stats.get('hit_rate', 'N/A')} hit rate")

    def test_cache_clearing(self):
        """Test: Should be able to clear cache when needed"""

        response = requests.post(
            f"{ANALYTICS_BASE_URL}/financial_events/cache/clear",
            timeout=30
        )

        # EXPECTED TO FAIL INITIALLY
        assert response.status_code == 200, f"Cache clear failed: {response.status_code}"

        result = response.json()
        assert result["success"] == True
        assert "message" in result

        print("✅ Cache cleared successfully")

    def test_event_type_filtering(self):
        """Test: Should filter events by type (earnings, fed_announcement, etc)"""

        event_types = ["earnings", "fed_announcement", "stock_event", "economic_indicator"]

        for event_type in event_types:
            response = requests.get(
                f"{ANALYTICS_BASE_URL}/financial_events?event_type={event_type}&limit=5",
                timeout=30
            )

            assert response.status_code == 200, f"Event type filter failed for {event_type}"

            result = response.json()
            assert result["success"] == True

            # If events found, verify they're the right type
            if result.get("events"):
                for event in result["events"]:
                    assert event["event_type"] == event_type

            print(f"✅ Event type filter '{event_type}' working")

    def test_symbol_filtering(self):
        """Test: Should filter events by company symbol"""

        test_symbols = ["AAPL", "TSLA", "MSFT"]

        for symbol in test_symbols:
            response = requests.get(
                f"{ANALYTICS_BASE_URL}/financial_events?symbol={symbol}&limit=5",
                timeout=30
            )

            assert response.status_code == 200, f"Symbol filter failed for {symbol}"

            result = response.json()
            assert result["success"] == True

            # If events found, verify they're for the right symbol
            if result.get("events"):
                for event in result["events"]:
                    assert event["company_symbol"] == symbol

            print(f"✅ Symbol filter '{symbol}' working")

    def test_date_range_filtering(self):
        """Test: Should filter events by date range"""

        response = requests.get(
            f"{ANALYTICS_BASE_URL}/financial_events?start_date=2025-09-01&end_date=2025-09-13&limit=10",
            timeout=30
        )

        assert response.status_code == 200, f"Date range filter failed: {response.status_code}"

        result = response.json()
        assert result["success"] == True

        # If events found, verify they're in the right date range
        if result.get("events"):
            for event in result["events"]:
                event_date = datetime.fromisoformat(event["event_date"]).date()
                start_date = datetime.fromisoformat("2025-09-01").date()
                end_date = datetime.fromisoformat("2025-09-13").date()

                assert start_date <= event_date <= end_date

        print(f"✅ Date range filtering working, found {result.get('count', 0)} events")

def run_failing_tests():
    """
    Run the failing tests to drive implementation
    Following TDD: Write failing test FIRST
    """

    print("🧪 Running Financial Events Analytics Integration Tests")
    print("=" * 70)
    print("🔥 EXPECTED TO FAIL - This drives the implementation!")
    print("=" * 70)

    test_instance = TestFinancialEventsAnalyticsIntegration()

    tests = [
        ("Analytics Service Health", test_instance.test_analytics_service_health),
        ("Financial Events Table Creation", test_instance.test_financial_events_table_creation),
        ("Extract and Store Events", test_instance.test_extract_and_store_events),
        ("Query Stored Events", test_instance.test_query_stored_events),
        ("Events Summary Statistics", test_instance.test_events_summary_statistics),
        ("Cache Performance Tracking", test_instance.test_cache_performance_tracking),
        ("Cache Clearing", test_instance.test_cache_clearing),
        ("Event Type Filtering", test_instance.test_event_type_filtering),
        ("Symbol Filtering", test_instance.test_symbol_filtering),
        ("Date Range Filtering", test_instance.test_date_range_filtering)
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        try:
            print(f"\n🔍 Testing: {test_name}")
            test_func()
            print(f"   ✅ PASSED: {test_name}")
            passed += 1
        except Exception as e:
            print(f"   ❌ FAILED: {test_name}")
            print(f"      Error: {str(e)}")
            failed += 1

    print(f"\n📊 Test Results:")
    print(f"   ✅ Passed: {passed}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📝 Total: {len(tests)}")

    if failed > 0:
        print(f"\n🚨 {failed} tests failed - This is EXPECTED!")
        print("💡 Now implement the analytics service endpoints to make tests pass")
        return False
    else:
        print("\n🎉 All tests passed!")
        return True

if __name__ == "__main__":
    success = run_failing_tests()
    exit(0 if success else 1)