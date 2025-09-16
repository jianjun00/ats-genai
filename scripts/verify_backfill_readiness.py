#!/usr/bin/env python3
"""
Backfill System Readiness Verification

Verifies that the daily backfill system is ready for deployment by checking:
- Database connectivity and table schemas
- API key availability and validity
- Sample data collection functionality
- Container environment setup

Usage:
    python3 scripts/verify_backfill_readiness.py
    docker exec ats-intg-analytics python3 scripts/verify_backfill_readiness.py
"""

import asyncio
import asyncpg
import requests
import os
import sys
from datetime import datetime, date, timedelta

async def test_database_connectivity():
    """Test database connection and table schemas."""
    print("🔍 Testing database connectivity...")

    db_url = "postgresql://postgres:intg_password@ats-intg-postgres:5432/intg_db"

    try:
        conn = await asyncpg.connect(db_url)
        print("✅ Database connection successful")

        # Check required tables exist
        tables = ['intg_daily_price_tiingo', 'intg_daily_price_polygon', 'intg_daily_price_eodhd', 'intg_instruments']
        for table in tables:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table} LIMIT 1")
            print(f"✅ Table {table}: accessible")

        # Check instrument data
        instrument_count = await conn.fetchval("SELECT COUNT(*) FROM intg_instrument WHERE active = true")
        print(f"📊 Active instruments: {instrument_count}")

        if instrument_count == 0:
            print("⚠️ Warning: No active instruments found")
            return False

        await conn.close()
        return True

    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_api_keys():
    """Test vendor API key availability and validity."""
    print("\n🔑 Testing API keys...")

    api_keys = {
        'TIINGO_API_KEY': os.getenv('TIINGO_API_KEY'),
        'POLYGON_API_KEY': os.getenv('POLYGON_API_KEY'),
        'EODHD_API_KEY': os.getenv('EODHD_API_KEY')
    }

    results = {}

    for key_name, key_value in api_keys.items():
        if not key_value:
            print(f"❌ {key_name}: Not found in environment")
            results[key_name] = False
            continue

        print(f"✅ {key_name}: Available ({key_value[:10]}...)")

        # Test API connectivity
        try:
            if key_name == 'TIINGO_API_KEY':
                response = requests.get(f"https://api.tiingo.com/tiingo/daily/AAPL/prices?startDate=2025-09-01&endDate=2025-09-01&token={key_value}", timeout=10)
                results[key_name] = response.status_code == 200
                print(f"  {'✅' if results[key_name] else '❌'} Tiingo API: {response.status_code}")

            elif key_name == 'POLYGON_API_KEY':
                response = requests.get(f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2025-09-01/2025-09-01?apikey={key_value}", timeout=10)
                results[key_name] = response.status_code == 200
                print(f"  {'✅' if results[key_name] else '❌'} Polygon API: {response.status_code}")

            elif key_name == 'EODHD_API_KEY':
                response = requests.get(f"https://eodhd.com/api/eod/AAPL.US?from=2025-09-01&to=2025-09-01&api_token={key_value}&fmt=json", timeout=10)
                results[key_name] = response.status_code == 200
                print(f"  {'✅' if results[key_name] else '❌'} EODHD API: {response.status_code}")

        except Exception as e:
            print(f"  ❌ {key_name.split('_')[0]} API test failed: {e}")
            results[key_name] = False

    return all(results.values())

def test_script_availability():
    """Test that required scripts are available."""
    print("\n📄 Testing script availability...")

    required_scripts = [
        'scripts/multi_vendor_daily_collector.py',
        'scripts/tiingo_data_collector_intg.py',
        'scripts/manage_daily_backfill_jobs.py'
    ]

    all_available = True

    for script in required_scripts:
        if os.path.exists(script):
            print(f"✅ {script}: Available")
        else:
            print(f"❌ {script}: Missing")
            all_available = False

    return all_available

async def test_sample_collection():
    """Test sample data collection."""
    print("\n🧪 Testing sample data collection...")

    try:
        # Test with a small sample
        from multi_vendor_daily_collector import MultiVendorDailyCollector

        collector = MultiVendorDailyCollector()
        await collector.initialize()

        # Get 1 symbol
        symbols = await collector.get_active_symbols(limit=1)
        if not symbols:
            print("❌ No symbols available for testing")
            return False

        test_symbol, test_id = symbols[0]
        print(f"🎯 Testing with symbol: {test_symbol} (ID: {test_id})")

        # Test date range (last 2 days)
        end_date = date.today()
        start_date = end_date - timedelta(days=2)

        # Test Tiingo collection
        result = await collector.collect_vendor_data('tiingo', [symbols[0]], start_date, end_date)

        await collector.cleanup()

        success = result.symbols_processed > 0 or result.records_inserted > 0 or result.records_updated > 0
        print(f"{'✅' if success else '❌'} Sample collection: {result.symbols_processed} symbols, {result.records_inserted} inserted, {result.records_updated} updated")

        if result.errors:
            print(f"⚠️ Errors encountered: {len(result.errors)}")
            for error in result.errors[:3]:  # Show first 3 errors
                print(f"  • {error}")

        return success

    except Exception as e:
        print(f"❌ Sample collection failed: {e}")
        return False

def check_environment():
    """Check container environment setup."""
    print("\n🐳 Checking environment setup...")

    checks = [
        ('PYTHONPATH', os.getenv('PYTHONPATH')),
        ('Container working dir', os.getcwd()),
        ('Python version', sys.version.split()[0]),
    ]

    for check_name, value in checks:
        if value:
            print(f"✅ {check_name}: {value}")
        else:
            print(f"⚠️ {check_name}: Not set")

    # Check if we're in container
    in_container = os.path.exists('/.dockerenv') or os.path.exists('/workspace')
    print(f"{'✅' if in_container else '⚠️'} Running in container: {in_container}")

    return True

async def main():
    """Run all readiness checks."""
    print("🚀 ATS-INTG Daily Backfill Readiness Check")
    print("=" * 60)

    checks = [
        ("Database Connectivity", test_database_connectivity()),
        ("API Keys", test_api_keys()),
        ("Script Availability", test_script_availability()),
        ("Environment Setup", check_environment()),
        ("Sample Collection", test_sample_collection()),
    ]

    results = {}
    for check_name, check_func in checks:
        if asyncio.iscoroutine(check_func):
            results[check_name] = await check_func
        else:
            results[check_name] = check_func

    # Summary
    print("\n" + "=" * 60)
    print("📋 READINESS SUMMARY")
    print("=" * 60)

    all_passed = True
    for check_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} {check_name}")
        if not passed:
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 SYSTEM READY FOR DEPLOYMENT")
        print("✅ All checks passed - daily backfill can be deployed")
        print("\nNext steps:")
        print("1. Deploy: python3 scripts/manage_daily_backfill_jobs.py deploy")
        print("2. Monitor: python3 scripts/manage_daily_backfill_jobs.py status")
        print("3. Test: python3 scripts/manage_daily_backfill_jobs.py test-run")
    else:
        print("⚠️ SYSTEM NOT READY")
        print("❌ Some checks failed - resolve issues before deployment")

    return all_passed

if __name__ == "__main__":
    # Add src to path
    sys.path.insert(0, 'src')
    success = asyncio.run(main())
    sys.exit(0 if success else 1)