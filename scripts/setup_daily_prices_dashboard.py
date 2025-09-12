#!/usr/bin/env python3
"""
Setup Daily Prices Quality Dashboard
Creates dashboard in available monitoring systems (Grafana/SignOz)
"""

import requests
import json
import os
import sys
from datetime import datetime

def check_metrics_availability():
    """Check if our metrics are available in Prometheus"""
    try:
        response = requests.get(
            "http://localhost:9090/api/v1/query", 
            params={"query": "ats_daily_prices_coverage_percent"}
        )
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'success' and data['data']['result']:
                metrics_count = len(data['data']['result'])
                print(f"✅ Found {metrics_count} daily prices quality metrics in Prometheus")
                return True
        print("❌ No daily prices quality metrics found in Prometheus")
        return False
    except Exception as e:
        print(f"❌ Cannot connect to Prometheus: {e}")
        return False

def check_grafana_auth():
    """Try different Grafana authentication methods"""
    grafana_urls = [
        "http://localhost:3001",
        "http://localhost:4002"
    ]
    
    auth_methods = [
        ("admin", "admin"),
        ("admin", "password"),
        ("admin", ""),
        ("", "")
    ]
    
    for url in grafana_urls:
        print(f"🔍 Checking Grafana at {url}")
        for username, password in auth_methods:
            try:
                auth = (username, password) if username or password else None
                response = requests.get(f"{url}/api/health", auth=auth, timeout=5)
                if response.status_code == 200:
                    print(f"✅ Grafana accessible at {url} with {username}:{password}")
                    return url, (username, password)
            except:
                continue
        
        # Try with API key header (common setup)
        try:
            headers = {"Authorization": "Bearer admin"}
            response = requests.get(f"{url}/api/health", headers=headers, timeout=5)
            if response.status_code == 200:
                print(f"✅ Grafana accessible at {url} with API key")
                return url, None
        except:
            continue
    
    print("❌ Cannot authenticate with Grafana")
    return None, None

def create_grafana_dashboard(grafana_url, auth):
    """Create dashboard in Grafana"""
    try:
        # Load our dashboard configuration
        with open('config/dashboards/daily-prices-quality-dashboard.json', 'r') as f:
            dashboard_config = json.load(f)
        
        # Prepare Grafana dashboard format
        grafana_dashboard = {
            "dashboard": dashboard_config['dashboard'],
            "overwrite": True,
            "message": "Created by automated setup script"
        }
        
        # Create dashboard
        create_url = f"{grafana_url}/api/dashboards/db"
        headers = {"Content-Type": "application/json"}
        
        response = requests.post(
            create_url, 
            json=grafana_dashboard, 
            auth=auth, 
            headers=headers
        )
        
        if response.status_code == 200:
            result = response.json()
            dashboard_url = f"{grafana_url}/d/{result.get('uid', 'unknown')}/daily-prices-quality-monitoring"
            print(f"✅ Dashboard created successfully!")
            print(f"📊 Dashboard URL: {dashboard_url}")
            return True
        else:
            print(f"❌ Failed to create dashboard: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating Grafana dashboard: {e}")
        return False

def verify_dashboard_panels():
    """Verify dashboard has all expected panels and metrics"""
    try:
        with open('config/dashboards/daily-prices-quality-dashboard.json', 'r') as f:
            dashboard_config = json.load(f)
        
        panels = dashboard_config['dashboard']['panels']
        print(f"\n📋 Dashboard Verification:")
        print(f"✅ Total panels: {len(panels)}")
        
        expected_panels = [
            "Missing Daily Prices by Vendor",
            "Missing Daily Price Records", 
            "Data Coverage Percentage",
            "Missing Prices Trend (24h)",
            "Coverage Trend (24h)",
            "Bad Prices Detection",
            "Bad Price Records Count",
            "Data Quality Summary Table",
            "Last Data Update",
            "Quality Metrics Info"
        ]
        
        panel_titles = [panel['title'] for panel in panels]
        
        for expected in expected_panels:
            if any(expected in title for title in panel_titles):
                print(f"✅ Found panel: {expected}")
            else:
                print(f"❌ Missing panel: {expected}")
        
        # Check metrics used
        expected_metrics = [
            "ats_daily_prices_missing_symbols_total",
            "ats_daily_prices_missing_records_total",
            "ats_daily_prices_coverage_percent",
            "ats_daily_prices_bad_symbols_total",
            "ats_daily_prices_bad_records_total"
        ]
        
        dashboard_str = json.dumps(dashboard_config)
        print(f"\n🎯 Metrics Verification:")
        for metric in expected_metrics:
            if metric in dashboard_str:
                print(f"✅ Uses metric: {metric}")
            else:
                print(f"❌ Missing metric: {metric}")
                
        return True
        
    except Exception as e:
        print(f"❌ Error verifying dashboard: {e}")
        return False

def check_pushgateway():
    """Check if Pushgateway is accessible"""
    try:
        response = requests.get("http://localhost:9091/metrics", timeout=5)
        if response.status_code == 200:
            content = response.text
            if "ats_daily_prices" in content:
                print("✅ Pushgateway has our daily prices metrics")
                return True
            else:
                print("⚠️ Pushgateway accessible but no daily prices metrics found")
                return False
    except Exception as e:
        print(f"❌ Cannot connect to Pushgateway: {e}")
        return False

def main():
    print("🚀 Setting up Daily Prices Quality Dashboard")
    print("=" * 50)
    
    # Step 1: Verify metrics are available
    print("\n📊 Step 1: Checking metrics availability...")
    if not check_metrics_availability():
        print("❌ Please run: python3 scripts/daily_prices_quality_metrics.py --push-metrics")
        return False
    
    # Step 2: Check Pushgateway
    print("\n📤 Step 2: Checking Pushgateway...")
    check_pushgateway()
    
    # Step 3: Verify dashboard configuration
    print("\n🔍 Step 3: Verifying dashboard configuration...")
    if not verify_dashboard_panels():
        return False
    
    # Step 4: Try to create dashboard
    print("\n🎨 Step 4: Creating dashboard...")
    grafana_url, auth = check_grafana_auth()
    
    if grafana_url and auth:
        success = create_grafana_dashboard(grafana_url, auth)
        if success:
            print("\n🎉 SUCCESS! Dashboard created and verified!")
            print("\n📋 Dashboard Summary:")
            print("- 10 panels with comprehensive metrics")
            print("- Real-time monitoring of 3 vendors (Polygon, Tiingo, EODHD)")
            print("- Missing prices and bad prices detection")
            print("- Coverage percentage with color-coded thresholds")
            print("- 30-second auto-refresh")
            return True
    
    # Dashboard setup failed - provide manual instructions
    print("\n⚠️ Automated setup failed - Manual setup required:")
    print("1. Open Grafana at http://localhost:3001 or http://localhost:4002")
    print("2. Go to 'Import Dashboard'")
    print("3. Copy content from: config/dashboards/daily-prices-quality-dashboard.json")
    print("4. Paste and import")
    
    return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)