#!/usr/bin/env python3
"""
Update Grafana Dashboard Time Range to Show Historical Data
"""

import json
import requests

def main():
    GRAFANA_URL = "http://localhost:4002"
    USERNAME = "admin"
    PASSWORD = "admin"
    
    print("🔧 Updating Grafana dashboard time range...")
    
    dashboard_uid = "f9afe708-9be9-4c39-b901-f5c43a0a479f"
    
    session = requests.Session()
    session.auth = (USERNAME, PASSWORD)
    
    # Get current dashboard
    resp = session.get(f"{GRAFANA_URL}/api/dashboards/uid/{dashboard_uid}")
    if resp.status_code != 200:
        print(f"❌ Failed to get dashboard: {resp.status_code}")
        return
        
    dashboard_data = resp.json()
    dashboard = dashboard_data["dashboard"]
    
    # Update time range to show last 7 days instead of 24h
    dashboard["time"] = {
        "from": "now-7d",
        "to": "now"
    }
    
    # Update version
    dashboard["version"] += 1
    
    # Save updated dashboard
    update_data = {
        "dashboard": dashboard,
        "message": "Updated time range to show last 7 days of data",
        "overwrite": True
    }
    
    resp = session.post(f"{GRAFANA_URL}/api/dashboards/db", json=update_data)
    if resp.status_code != 200:
        print(f"❌ Failed to update dashboard: {resp.status_code}")
        print(f"Response: {resp.text}")
        return
        
    result = resp.json()
    print(f"✅ Dashboard time range updated to 7 days!")
    print(f"🌐 URL: {GRAFANA_URL}/d/{result['uid']}/{result['slug']}")
    
    print("\n📊 Data should now be visible for dates:")
    print("- September 2-3, 2025 (existing data)")
    print("- Time range: Last 7 days instead of 24 hours")

if __name__ == "__main__":
    main()