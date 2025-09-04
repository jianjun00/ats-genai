#!/usr/bin/env python3
"""
Add Dual Time Dimension Panels to Grafana Dashboard
Shows both bar occurring time and bar collection time
"""

import json
import requests
from pathlib import Path

def main():
    GRAFANA_URL = "http://localhost:4002"
    USERNAME = "admin"
    PASSWORD = "admin"
    
    print("🔧 Adding dual time dimension panels to Grafana dashboard...")
    
    # Load panel configurations
    config_dir = Path("config/grafana")
    
    try:
        with open(config_dir / "collection-latency-panel.json", "r") as f:
            latency_panel = json.load(f)
            
        with open(config_dir / "dual-timeline-panel.json", "r") as f:
            timeline_panel = json.load(f)
            
        with open(config_dir / "collection-vs-bar-time-stats.json", "r") as f:
            stats_panel = json.load(f)
            
        print("✅ Loaded 3 new panel configurations")
        
        # Get existing dashboard
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
        
        print(f"✅ Retrieved dashboard: {dashboard['title']}")
        
        # Add new panels
        dashboard["panels"].extend([latency_panel, timeline_panel, stats_panel])
        
        # Update version
        dashboard["version"] += 1
        
        # Save updated dashboard
        update_data = {
            "dashboard": dashboard,
            "message": "Added dual time dimension panels: bar time vs collection time",
            "overwrite": True
        }
        
        resp = session.post(f"{GRAFANA_URL}/api/dashboards/db", json=update_data)
        if resp.status_code != 200:
            print(f"❌ Failed to update dashboard: {resp.status_code}")
            print(f"Response: {resp.text}")
            return
            
        result = resp.json()
        print(f"✅ Dashboard updated with dual time panels!")
        print(f"🌐 URL: {GRAFANA_URL}/d/{result['uid']}/{result['slug']}")
        
        print("\n📊 New panels added:")
        print("1. 📈 Data Collection Latency - Shows delay between bar time and collection time")
        print("2. 🕐 Bar Timeline - Dual view of market time vs collection time") 
        print("3. 📋 Collection vs Bar Time Stats - Summary statistics")
        
        print("\n🎯 What you can now see:")
        print("- Bar occurring time: When the 1-minute bar actually happened in the market")
        print("- Bar collection time: When our system received and stored the data")
        print("- Collection latency: How long it took to collect each bar")
        print("- Timeline comparison: Visual comparison of both time dimensions")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()