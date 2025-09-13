#!/usr/bin/env python3
"""
Test script for API Status Tracker

Generates sample API requests to test the tracking system and verify
that metrics are properly collected and exposed via Prometheus endpoint.

Usage:
    python3 scripts/test_api_status_tracker.py --simulate-requests 100
    python3 scripts/test_api_status_tracker.py --test-all-vendors
"""

import asyncio
import sys
import argparse
import random
import time
from datetime import datetime

# Add src and scripts to path
sys.path.insert(0, '/workspace/src')
sys.path.append('/workspace/scripts')
sys.path.append(str(Path(__file__).parent.parent.parent / "scripts"))

from pathlib import Path
from api_status_tracker import APIStatusTracker, initialize_global_tracker, get_global_tracker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def simulate_api_requests(tracker: APIStatusTracker, num_requests: int = 100):
    """Simulate various API requests for testing."""

    vendors = ['tiingo', 'polygon', 'eodhd', 'alpha_vantage', 'fmp', 'firstrate']
    endpoints = {
        'tiingo': ['daily_prices', 'fundamentals', 'news', 'instruments'],
        'polygon': ['daily_prices', 'fundamentals', 'news', 'minute_bars'],
        'eodhd': ['daily_prices', 'fundamentals', 'news', 'instruments'],
        'alpha_vantage': ['fundamentals', 'economic_indicators'],
        'fmp': ['fundamentals', 'earnings'],
        'firstrate': ['minute_bars', 'daily_download']
    }

    status_codes = [200, 200, 200, 200, 200, 200, 200, 200, 429, 404, 500, 503]  # Weighted towards success
    symbols = ['AAPL', 'TSLA', 'SPY', 'QQQ', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'BRK.B']

    logger.info(f"🚀 Simulating {num_requests} API requests...")

    for i in range(num_requests):
        # Choose random vendor and endpoint
        vendor = random.choice(vendors)
        endpoint = random.choice(endpoints[vendor])
        status_code = random.choice(status_codes)
        symbol = random.choice(symbols)

        # Generate realistic latency (varies by status code)
        if status_code == 200:
            latency_ms = random.uniform(50, 800)  # Normal response times
        elif status_code == 429:
            latency_ms = random.uniform(20, 100)   # Fast rejection
        else:
            latency_ms = random.uniform(100, 5000) # Slow errors/timeouts

        # Generate response size (only for successful requests)
        response_size = random.randint(512, 8192) if status_code == 200 else None

        # Generate error message for failures
        error_messages = {
            429: "Rate limit exceeded",
            404: "Symbol not found",
            500: "Internal server error",
            503: "Service temporarily unavailable"
        }
        error_message = error_messages.get(status_code) if status_code >= 400 else None

        # Track the request
        tracker.track_request(
            vendor=vendor,
            api_endpoint=endpoint,
            status_code=status_code,
            latency_ms=latency_ms,
            response_size_bytes=response_size,
            error_message=error_message,
            symbol=symbol,
            request_url=f"https://api.{vendor}.com/{endpoint}?symbol={symbol}"
        )

        # Brief pause to avoid overwhelming logs
        if i % 10 == 0:
            logger.info(f"📊 Generated {i+1}/{num_requests} requests...")
            await asyncio.sleep(0.1)

    logger.info(f"✅ Simulation complete! Generated {num_requests} API requests")

@pytest.mark.asyncio

async def test_all_vendors(tracker: APIStatusTracker):
    """Test API tracking for all configured vendors with realistic patterns."""

    logger.info("🧪 Testing API tracking for all vendors...")

    # Simulate different vendor behavior patterns
    test_scenarios = [
        # Tiingo - mostly successful with occasional rate limiting
        {'vendor': 'tiingo', 'endpoint': 'daily_prices', 'requests': 20, 'success_rate': 0.85, 'rate_limit_rate': 0.10},
        {'vendor': 'tiingo', 'endpoint': 'fundamentals', 'requests': 15, 'success_rate': 0.90, 'rate_limit_rate': 0.05},

        # Polygon - good success rate but some rate limiting
        {'vendor': 'polygon', 'endpoint': 'daily_prices', 'requests': 25, 'success_rate': 0.80, 'rate_limit_rate': 0.15},
        {'vendor': 'polygon', 'endpoint': 'minute_bars', 'requests': 30, 'success_rate': 0.75, 'rate_limit_rate': 0.20},

        # EODHD - reliable but slower
        {'vendor': 'eodhd', 'endpoint': 'daily_prices', 'requests': 18, 'success_rate': 0.95, 'rate_limit_rate': 0.02},
        {'vendor': 'eodhd', 'endpoint': 'instruments', 'requests': 10, 'success_rate': 0.90, 'rate_limit_rate': 0.05},

        # Alpha Vantage - limited requests, some failures
        {'vendor': 'alpha_vantage', 'endpoint': 'economic_indicators', 'requests': 8, 'success_rate': 0.70, 'rate_limit_rate': 0.25},

        # FMP - moderate usage
        {'vendor': 'fmp', 'endpoint': 'fundamentals', 'requests': 12, 'success_rate': 0.85, 'rate_limit_rate': 0.10},

        # FirstRate - file-based, different error patterns
        {'vendor': 'firstrate', 'endpoint': 'minute_bars', 'requests': 40, 'success_rate': 0.90, 'rate_limit_rate': 0.02},
    ]

    symbols = ['AAPL', 'TSLA', 'SPY', 'QQQ', 'MSFT', 'GOOGL', 'AMZN']

    for scenario in test_scenarios:
        vendor = scenario['vendor']
        endpoint = scenario['endpoint']
        num_requests = scenario['requests']
        success_rate = scenario['success_rate']
        rate_limit_rate = scenario['rate_limit_rate']

        logger.info(f"📊 Testing {vendor} {endpoint}: {num_requests} requests (success: {success_rate*100:.0f}%, rate limit: {rate_limit_rate*100:.0f}%)")

        for i in range(num_requests):
            # Determine status code based on rates
            rand = random.random()
            if rand < success_rate:
                status_code = 200
                latency_ms = random.uniform(100, 1000)
                response_size = random.randint(1024, 16384)
                error_message = None
            elif rand < success_rate + rate_limit_rate:
                status_code = 429
                latency_ms = random.uniform(50, 200)
                response_size = None
                error_message = "Rate limit exceeded"
            else:
                # Other errors
                status_code = random.choice([404, 500, 503])
                latency_ms = random.uniform(200, 3000)
                response_size = None
                error_message = f"HTTP {status_code} error"

            symbol = random.choice(symbols)

            # Track the request
            tracker.track_request(
                vendor=vendor,
                api_endpoint=endpoint,
                status_code=status_code,
                latency_ms=latency_ms,
                response_size_bytes=response_size,
                error_message=error_message,
                symbol=symbol,
                request_url=f"https://api.{vendor}.com/{endpoint}?symbol={symbol}"
            )

        # Small delay between scenarios
        await asyncio.sleep(0.2)

    logger.info("✅ All vendor testing complete!")

async def display_metrics_summary(tracker: APIStatusTracker):
    """Display current metrics summary."""

    logger.info("📈 Current Metrics Summary:")
    logger.info("=" * 60)

    # Get summary
    summary = tracker.get_metrics_summary()

    # Display totals
    totals = summary.get('totals', {})
    logger.info(f"📊 TOTAL METRICS:")
    logger.info(f"  Total Requests: {totals.get('total_requests', 0):,}")
    logger.info(f"  Total Errors: {totals.get('total_errors', 0):,}")
    logger.info(f"  Total Rate Limits: {totals.get('total_rate_limits', 0):,}")
    logger.info(f"  Avg Latency: {totals.get('avg_latency_ms', 0):.1f}ms")
    logger.info("")

    # Display by vendor
    vendors = summary.get('vendors', {})
    for vendor, data in vendors.items():
        logger.info(f"🏢 {vendor.upper()}:")
        logger.info(f"  Total Requests: {data.get('total_requests', 0):,}")
        logger.info(f"  Success Rate: {data.get('success_rate', 0):.1f}%")
        logger.info(f"  Rate Limits: {data.get('rate_limits', 0):,}")
        logger.info(f"  Avg Latency: {data.get('avg_latency_ms', 0):.1f}ms")

        # Status code breakdown
        status_codes = data.get('status_codes', {})
        if status_codes:
            logger.info(f"  Status Codes: {dict(status_codes)}")

        # Endpoint breakdown
        endpoints = data.get('endpoints', {})
        if endpoints:
            logger.info(f"  Endpoints: {list(endpoints.keys())}")

        logger.info("")

@pytest.mark.asyncio

async def test_prometheus_metrics(tracker: APIStatusTracker):
    """Test Prometheus metrics generation."""

    logger.info("🔬 Testing Prometheus metrics generation...")

    # Generate metrics
    metrics_text = tracker.get_prometheus_metrics()

    if metrics_text:
        # Count metrics
        metrics_lines = [line for line in metrics_text.split('\n') if line and not line.startswith('#')]
        logger.info(f"✅ Generated {len(metrics_lines)} Prometheus metrics")

        # Show first few metrics as example
        logger.info("📊 Sample metrics:")
        for line in metrics_lines[:10]:
            logger.info(f"  {line}")
        if len(metrics_lines) > 10:
            logger.info(f"  ... and {len(metrics_lines) - 10} more")

    else:
        logger.error("❌ Failed to generate Prometheus metrics")

async def main():
    """Main test function."""
    parser = argparse.ArgumentParser(description='Test API Status Tracker')
    parser.add_argument('--simulate-requests', type=int, default=50, help='Number of random requests to simulate')
    parser.add_argument('--test-all-vendors', action='store_true', help='Test all vendors with realistic patterns')
    parser.add_argument('--skip-db', action='store_true', help='Skip database operations for testing')

    args = parser.parse_args()

    logger.info("🚀 Starting API Status Tracker tests...")

    # Initialize tracker
    tracker = APIStatusTracker()

    if not args.skip_db:
        try:
            await tracker.initialize()
            logger.info("✅ Database connection initialized")
        except Exception as e:
            logger.warning(f"⚠️ Database initialization failed: {e}")
            logger.info("📝 Continuing with in-memory tracking only...")
    else:
        logger.info("⚠️ Skipping database operations (--skip-db)")

    try:
        # Run tests based on arguments
        if args.test_all_vendors:
            await test_all_vendors(tracker)
        else:
            await simulate_api_requests(tracker, args.simulate_requests)

        # Display results
        await display_metrics_summary(tracker)
        await test_prometheus_metrics(tracker)

        logger.info("✅ All tests completed successfully!")

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise
    finally:
        await tracker.close()

if __name__ == "__main__":
    asyncio.run(main())