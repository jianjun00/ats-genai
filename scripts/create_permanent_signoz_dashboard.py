#!/usr/bin/env python3

import requests
import json
import sys
import uuid
import time
from datetime import datetime

class SignOzDashboardCreator:
    def __init__(self, base_url="http://localhost:8080", api_key="9RbijHam3W4B0a8h5fFB+7NgUgmXV+hFnzIPQUqtc6M="):
        self.base_url = base_url
        self.api_key = api_key
        self.headers = {
            "SIGNOZ-API-KEY": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def delete_existing_dashboard(self, dashboard_id):
        """Delete existing dashboard if it exists"""
        try:
            print(f"🗑️ Deleting existing dashboard: {dashboard_id}")
            response = requests.delete(
                f"{self.base_url}/api/v1/dashboards/{dashboard_id}",
                headers=self.headers,
                timeout=30
            )
            if response.status_code in [200, 404]:
                print(f"✅ Dashboard deleted or didn't exist")
            else:
                print(f"⚠️ Delete response: {response.status_code}")
        except Exception as e:
            print(f"⚠️ Delete error: {e}")

    def create_dashboard_payload(self):
        """Create comprehensive dashboard payload with proper SignOz v4 schema"""
        dashboard_id = str(uuid.uuid4())

        # Create panels for the dashboard
        panels = []

        # Panel 1: Missing Symbols by Vendor
        panels.append({
            "id": str(uuid.uuid4()),
            "title": "🔴 Missing Symbols by Vendor",
            "description": "Count of symbols with missing daily prices data",
            "type": "value",
            "targets": [{
                "query": "ats_daily_prices_missing_symbols_total",
                "legend": "",
                "disabled": False
            }],
            "options": {
                "reduceOptions": {
                    "values": False,
                    "calcs": ["lastNotNull"],
                    "fields": ""
                },
                "orientation": "auto",
                "textMode": "auto",
                "colorMode": "value",
                "graphMode": "area",
                "justifyMode": "auto"
            },
            "pluginVersion": "7.0.0",
            "targets": [{
                "expr": "ats_daily_prices_missing_symbols_total",
                "refId": "A"
            }],
            "gridPos": {"x": 0, "y": 0, "w": 8, "h": 6}
        })

        # Panel 2: Missing Records by Vendor
        panels.append({
            "id": str(uuid.uuid4()),
            "title": "📊 Missing Records by Vendor",
            "description": "Total count of missing daily price records",
            "type": "value",
            "targets": [{
                "query": "ats_daily_prices_missing_records_total",
                "legend": "",
                "disabled": False
            }],
            "options": {
                "reduceOptions": {
                    "values": False,
                    "calcs": ["lastNotNull"],
                    "fields": ""
                },
                "orientation": "auto",
                "textMode": "auto",
                "colorMode": "value",
                "graphMode": "area",
                "justifyMode": "auto"
            },
            "pluginVersion": "7.0.0",
            "targets": [{
                "expr": "ats_daily_prices_missing_records_total",
                "refId": "B"
            }],
            "gridPos": {"x": 8, "y": 0, "w": 8, "h": 6}
        })

        # Panel 3: Coverage Percentage
        panels.append({
            "id": str(uuid.uuid4()),
            "title": "✅ Coverage % by Vendor",
            "description": "Percentage of coverage for daily prices",
            "type": "value",
            "targets": [{
                "query": "ats_daily_prices_coverage_percent",
                "legend": "",
                "disabled": False
            }],
            "options": {
                "reduceOptions": {
                    "values": False,
                    "calcs": ["lastNotNull"],
                    "fields": ""
                },
                "orientation": "auto",
                "textMode": "auto",
                "colorMode": "value",
                "graphMode": "area",
                "justifyMode": "auto",
                "unit": "percent"
            },
            "pluginVersion": "7.0.0",
            "targets": [{
                "expr": "ats_daily_prices_coverage_percent",
                "refId": "C"
            }],
            "gridPos": {"x": 16, "y": 0, "w": 8, "h": 6}
        })

        # Panel 4: Bad Symbols
        panels.append({
            "id": str(uuid.uuid4()),
            "title": "⚠️ Bad Symbols by Vendor",
            "description": "Count of symbols with bad/invalid daily prices",
            "type": "value",
            "targets": [{
                "query": "ats_daily_prices_bad_symbols_total",
                "legend": "",
                "disabled": False
            }],
            "options": {
                "reduceOptions": {
                    "values": False,
                    "calcs": ["lastNotNull"],
                    "fields": ""
                },
                "orientation": "auto",
                "textMode": "auto",
                "colorMode": "value",
                "graphMode": "area",
                "justifyMode": "auto"
            },
            "pluginVersion": "7.0.0",
            "targets": [{
                "expr": "ats_daily_prices_bad_symbols_total",
                "refId": "D"
            }],
            "gridPos": {"x": 0, "y": 6, "w": 12, "h": 6}
        })

        # Panel 5: Dashboard Information
        panels.append({
            "id": str(uuid.uuid4()),
            "title": "ℹ️ Dashboard Information",
            "description": "Dashboard metadata and information",
            "type": "text",
            "options": {
                "content": f"""
### Daily Prices Quality Monitoring

**Dashboard ID:** {dashboard_id}
**Created:** {datetime.now().strftime('%m/%d/%Y, %I:%M:%S %p')}
**Status:** ✅ Successfully created via Playwright API
**Metrics Source:** Pushgateway (localhost:9091)

**Monitoring Coverage:**
- 🔴 Missing symbols tracking
- 📊 Missing records analysis
- ✅ Coverage percentage calculation
- ⚠️ Bad data detection

**📝 To update metrics:**
```bash
PROMETHEUS_GATEWAY=localhost:9091 python3 scripts/daily_prices_quality_metrics.py
```
                """,
                "mode": "markdown"
            },
            "gridPos": {"x": 12, "y": 6, "w": 12, "h": 6}
        })

        return {
            "id": dashboard_id,
            "title": "📊 Daily Prices Quality Monitoring",
            "description": "Comprehensive monitoring of daily prices quality across Polygon, Tiingo, and EODHD",
            "tags": ["daily-prices", "quality", "monitoring", "ats"],
            "timezone": "browser",
            "panels": panels,
            "time": {
                "from": "now-24h",
                "to": "now"
            },
            "timepicker": {},
            "templating": {
                "list": []
            },
            "annotations": {
                "list": []
            },
            "refresh": "30s",
            "schemaVersion": 4,
            "version": 1,
            "links": [],
            "layout": [
                {"x": 0, "y": 0, "w": 8, "h": 6, "i": panels[0]["id"]},
                {"x": 8, "y": 0, "w": 8, "h": 6, "i": panels[1]["id"]},
                {"x": 16, "y": 0, "w": 8, "h": 6, "i": panels[2]["id"]},
                {"x": 0, "y": 6, "w": 12, "h": 6, "i": panels[3]["id"]},
                {"x": 12, "y": 6, "w": 12, "h": 6, "i": panels[4]["id"]}
            ]
        }

    def create_dashboard(self):
        """Create dashboard using SignOz API"""
        print("🚀 Creating permanent SignOz dashboard...")

        # First delete existing dashboard
        existing_id = "01993c69-1998-7731-aa31-d008d3218445"
        self.delete_existing_dashboard(existing_id)

        # Create new dashboard
        payload = self.create_dashboard_payload()
        dashboard_id = payload["id"]

        print(f"📋 Dashboard ID: {dashboard_id}")
        print(f"📋 Title: {payload['title']}")
        print(f"📋 Panels: {len(payload['panels'])}")

        try:
            print("📤 Sending dashboard creation request...")
            response = requests.post(
                f"{self.base_url}/api/v1/dashboards",
                headers=self.headers,
                json=payload,
                timeout=30
            )

            print(f"📥 Response Status: {response.status_code}")
            print(f"📥 Response Headers: {dict(response.headers)}")

            if response.status_code in [200, 201]:
                print(f"✅ Dashboard created successfully!")
                print(f"🌐 URL: {self.base_url}/dashboard/{dashboard_id}")
                return dashboard_id
            else:
                print(f"❌ Failed to create dashboard")
                print(f"Response: {response.text}")
                return None

        except Exception as e:
            print(f"❌ Error creating dashboard: {e}")
            return None

    def verify_dashboard(self, dashboard_id):
        """Verify dashboard exists and has panels"""
        try:
            print(f"🔍 Verifying dashboard: {dashboard_id}")
            response = requests.get(
                f"{self.base_url}/api/v1/dashboards/{dashboard_id}",
                headers=self.headers,
                timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                panels_count = len(data.get("panels", []))
                print(f"✅ Dashboard verified: {panels_count} panels")
                return True
            else:
                print(f"❌ Dashboard verification failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Verification error: {e}")
            return False

def main():
    creator = SignOzDashboardCreator()

    # Create the dashboard
    dashboard_id = creator.create_dashboard()

    if dashboard_id:
        print(f"\n🎉 SUCCESS! Dashboard created:")
        print(f"🆔 ID: {dashboard_id}")
        print(f"🌐 URL: http://localhost:8080/dashboard/{dashboard_id}")

        # Wait a moment and verify
        print("\n⏳ Waiting 3 seconds before verification...")
        time.sleep(3)

        if creator.verify_dashboard(dashboard_id):
            print(f"✅ Dashboard is ready and accessible!")
        else:
            print(f"⚠️ Dashboard created but verification failed")
    else:
        print("❌ Failed to create dashboard")
        sys.exit(1)

if __name__ == "__main__":
    main()