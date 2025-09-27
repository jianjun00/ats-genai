#!/usr/bin/env python3
"""
Setup ATS News Ingestion Dashboard in Grafana
"""

import requests
import json
import sys
import argparse

def setup_news_dashboard():
    """Set up the news dashboard in Grafana."""

    grafana_url = "http://localhost:4002"  # ATS-INTG Grafana
    dashboard_file = "/home/jianjun/ats-genai-oncall/grafana-news-dashboard.json"

    print(f"🎛️  Setting up ATS News Dashboard in Grafana...")
    print(f"📊 Grafana URL: {grafana_url}")
    print(f"📁 Dashboard file: {dashboard_file}")

    # Load dashboard JSON
    with open(dashboard_file, 'r') as f:
        dashboard_data = json.load(f)
    payload = {
        "dashboard": dashboard_data["dashboard"],
        "overwrite": True,
        "inputs": [
            {
                "name": "DS_ATS_INTG_POSTGRESQL",
                "type": "datasource",
                "pluginId": "postgres",
                "value": "ATS-INTG-PostgreSQL"
            }
        ]
    }

    # Try to create the dashboard (no authentication for basic setup)
    response = requests.post(
        f"{grafana_url}/api/dashboards/db",
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=10
    )

    if response.status_code == 200:
        result = response.json()
        dashboard_url = f"{grafana_url}/d/{result.get('uid', 'unknown')}/ats-news-ingestion-dashboard"
        print(f"✅ News dashboard created successfully!")
        print(f"🔗 Dashboard URL: {dashboard_url}")
        return True
    else:
        print(f"⚠️  Dashboard creation returned status {response.status_code}")
        if response.status_code == 401:
            print("🔑 Authentication required. Please manually import the dashboard:")
            print(f"   1. Go to: {grafana_url}/dashboard/import")
            print(f"   2. Upload file: {dashboard_file}")
            print(f"   3. Select 'ATS-INTG-PostgreSQL' as the data source")
        else:
            print(f"   Response: {response.text[:200]}...")
        return False

def verify_data_source():
    """Verify PostgreSQL data source is configured."""
    print(f"🔍 Verifying PostgreSQL data source...")

    # Test database connection
    import os
    db_test_cmd = f"""
    PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "
    SELECT 'Connection successful' as status, COUNT(*) as total_articles
    FROM intg_realtime_news
    " 2>/dev/null
    """

    result = os.system(db_test_cmd)
    if result == 0:
        print("✅ PostgreSQL connection successful")
        return True
    else:
        print("❌ PostgreSQL connection failed")
        print("   Check that ATS-INTG PostgreSQL is running:")
        print("   docker ps | grep ats-intg-postgres")
        return False

def main():
    parser = argparse.ArgumentParser(description="Setup ATS News Dashboard in Grafana")
    parser.add_argument("--verify-only", action="store_true", help="Only verify data source")
    args = parser.parse_args()

    print("================================================================================")
    print("ATS NEWS DASHBOARD SETUP")
    print("================================================================================")

    # Verify data source first
    if not verify_data_source():
        print("❌ Data source verification failed")
        return 1

    if args.verify_only:
        print("✅ Data source verification complete")
        return 0

    # Setup dashboard
    if setup_news_dashboard():
        print("")
        print("🎉 ATS News Dashboard setup complete!")
        print("")
        print("📊 Dashboard Features:")
        print("   - Total articles by vendor")
        print("   - Today's article count")
        print("   - Article collection timeline")
        print("   - Latest articles table")
        print("   - API success rates")
        print("   - Data freshness metrics")
        print("")
        print("🔧 Access Dashboard:")
        print("   URL: http://10.0.0.79:4002/dashboards")
        print("   Login: admin/admin")
        print("   Search: 'ATS News Ingestion'")
        return 0
    else:
        print("❌ Dashboard setup failed - see manual instructions above")
        return 1

if __name__ == "__main__":
    sys.exit(main())