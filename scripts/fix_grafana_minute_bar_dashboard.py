#!/usr/bin/env python3
"""
Fix Grafana Minute Bar Dashboard - Add Working Panels
Updates the vendor monitoring dashboard with panels that query actual data tables.
"""

import json
import requests
from pathlib import Path

def main():
    # Grafana connection settings
    GRAFANA_URL = "http://localhost:4002"
    USERNAME = "admin"
    PASSWORD = "admin"

    print("🔧 Fixing Grafana Minute Bar Dashboard...")

    # Load panel configurations
    config_dir = Path("config/grafana")

    try:
        with open(config_dir / "minute-bar-live-data-panel.json", "r") as f:
            live_data_panel = json.load(f)

        with open(config_dir / "minute-bar-quality-panel.json", "r") as f:
            quality_panel = json.load(f)

        print("✅ Loaded panel configurations")

        # Get existing dashboard
        dashboard_uid = "f9afe708-9be9-4c39-b901-f5c43a0a479f"

        session = requests.Session()
        session.auth = (USERNAME, PASSWORD)

        # Get current dashboard
        resp = session.get(f"{GRAFANA_URL}/api/dashboards/uid/{dashboard_uid}")
        if resp.status_code != 200:
            print(f"❌ Failed to get dashboard: {resp.status_code}")
            print(f"Response: {resp.text}")
            return

        dashboard_data = resp.json()
        dashboard = dashboard_data["dashboard"]

        print(f"✅ Retrieved dashboard: {dashboard['title']}")

        # Add new panels
        dashboard["panels"].append(live_data_panel)
        dashboard["panels"].append(quality_panel)

        # Update version
        dashboard["version"] += 1

        # Save updated dashboard
        update_data = {
            "dashboard": dashboard,
            "message": "Added working minute bar panels with real data queries",
            "overwrite": True
        }

        resp = session.post(f"{GRAFANA_URL}/api/dashboards/db", json=update_data)
        if resp.status_code != 200:
            print(f"❌ Failed to update dashboard: {resp.status_code}")
            print(f"Response: {resp.text}")
            return

        result = resp.json()
        print(f"✅ Dashboard updated successfully!")
        print(f"🌐 URL: {GRAFANA_URL}/d/{result['uid']}/{result['slug']}")

        # Test data query
        print("\n📊 Testing data availability...")
        import subprocess
        result = subprocess.run([
            "bash", "-c",
            "export PGPASSWORD=intg_password && psql -h localhost -p 4432 -U postgres -d intg_db -c \"SELECT vendor, symbol, COUNT(*) FROM (SELECT vendor, symbol FROM intg_one_minute_live_polygon UNION ALL SELECT vendor, symbol FROM intg_one_minute_live_tiingo) combined GROUP BY vendor, symbol;\""
        ], capture_output=True, text=True)

        if result.returncode == 0:
            print("✅ Database query successful:")
            print(result.stdout)
        else:
            print("❌ Database query failed:")
            print(result.stderr)

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()