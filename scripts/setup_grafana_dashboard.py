#!/usr/bin/env python3
"""
Setup Grafana Dashboard for ATS Daily Prices Quality Metrics

Since SignOz UI has a locale error preventing React rendering, this script sets up
an alternative Grafana dashboard that can display the same metrics from Pushgateway.
"""

import json
import subprocess
import time
import requests
import sys

class GrafanaDashboardSetup:
    def __init__(self):
        self.grafana_url = "http://localhost:3001"
        self.grafana_user = "admin"
        self.grafana_pass = "admin"
        self.pushgateway_url = "http://localhost:9091"

    def setup_grafana_with_dashboard(self):
        """Setup Grafana with Prometheus datasource and ATS dashboard"""
        print("🚀 Setting up Grafana dashboard for ATS Daily Prices Quality...")

        # Step 1: Start Grafana if not running
        if not self._check_grafana_running():
            print("📦 Starting Grafana container...")
            self._start_grafana_container()

        # Step 2: Wait for Grafana to be ready
        print("⏳ Waiting for Grafana to be ready...")
        if not self._wait_for_grafana():
            print("❌ Grafana failed to start")
            return False

        # Step 3: Configure Prometheus datasource
        print("🔗 Configuring Prometheus datasource...")
        if not self._configure_prometheus_datasource():
            print("❌ Failed to configure datasource")
            return False

        # Step 4: Import dashboard
        print("📊 Importing ATS Daily Prices Quality dashboard...")
        dashboard_id = self._import_dashboard()
        if dashboard_id:
            print(f"✅ Dashboard imported successfully!")
            print(f"🌐 Dashboard URL: {self.grafana_url}/d/{dashboard_id}/ats-daily-prices-quality-monitoring")
            return True
        else:
            print("❌ Failed to import dashboard")
            return False

    def _check_grafana_running(self):
        """Check if Grafana is already running"""
        response = requests.get(f"{self.grafana_url}/api/health", timeout=5)
        return response.status_code == 200
    def _start_grafana_container(self):
        """Start Grafana container"""
        cmd = [
            "docker", "run", "-d",
            "--name", "ats-grafana",
            "--network", "ats-network",
            "-p", "3000:3000",
            "-e", "GF_SECURITY_ADMIN_PASSWORD=admin",
            "-v", "grafana-storage:/var/lib/grafana",
            "grafana/grafana:latest"
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Grafana container started")
        else:
            print(f"⚠️ Grafana container might already exist: {result.stderr}")
            # Try to start existing container
            subprocess.run(["docker", "start", "ats-grafana"], capture_output=True)
    def _wait_for_grafana(self, max_attempts=30):
        """Wait for Grafana to be ready"""
        for i in range(max_attempts):
            response = requests.get(f"{self.grafana_url}/api/health", timeout=2)
            if response.status_code == 200:
                print("✅ Grafana is ready")
                return True
            time.sleep(2)
            print(f"⏳ Waiting for Grafana... ({i+1}/{max_attempts})")
        return False

    def _configure_prometheus_datasource(self):
        """Configure Prometheus datasource to read from Pushgateway"""
        datasource_config = {
            "name": "Prometheus-Pushgateway",
            "type": "prometheus",
            "url": self.pushgateway_url,
            "access": "proxy",
            "basicAuth": False,
            "isDefault": True,
            "jsonData": {
                "httpMethod": "GET",
                "manageAlerts": False,
                "prometheusType": "Prometheus",
                "prometheusVersion": "2.40.0"
            }
        }

        response = requests.post(
            f"{self.grafana_url}/api/datasources",
            auth=(self.grafana_user, self.grafana_pass),
            headers={"Content-Type": "application/json"},
            json=datasource_config
        )

        if response.status_code in [200, 409]:  # 409 = already exists
            print("✅ Prometheus datasource configured")
            return True
        else:
            print(f"❌ Datasource configuration failed: {response.status_code} {response.text}")
            return False

    def _import_dashboard(self):
        """Import the ATS Daily Prices Quality dashboard"""
        dashboard_file = "/home/jianjun/ats-genai-model/config/dashboards/ats-daily-prices-quality-grafana.json"

        # Load dashboard JSON
        with open(dashboard_file, 'r') as f:
            dashboard_json = json.load(f)

        # Prepare import payload
        import_payload = {
            "dashboard": dashboard_json["dashboard"],
            "overwrite": True,
            "inputs": [
                {
                    "name": "DS_PROMETHEUS",
                    "type": "datasource",
                    "pluginId": "prometheus",
                    "value": "Prometheus-Pushgateway"
                }
            ]
        }

        response = requests.post(
            f"{self.grafana_url}/api/dashboards/import",
            auth=(self.grafana_user, self.grafana_pass),
            headers={"Content-Type": "application/json"},
            json=import_payload
        )

        if response.status_code == 200:
            result = response.json()
            dashboard_uid = result.get('uid', result.get('slug', 'unknown'))
            print(f"✅ Dashboard imported with UID: {dashboard_uid}")
            return dashboard_uid
        else:
            print(f"❌ Dashboard import failed: {response.status_code} {response.text}")
            return None

    def verify_dashboard_working(self):
        """Verify the dashboard is working and displaying data"""
        print("🔍 Verifying dashboard functionality...")

        # Check if metrics are available
        response = requests.get(f"{self.pushgateway_url}/metrics")
        metrics_text = response.text

        # Check for our specific metrics
        required_metrics = [
            "ats_daily_price_polygon_coverage_percent",
            "ats_daily_price_polygon_missing_symbols_total"
        ]

        found_metrics = []
        for metric in required_metrics:
            if metric in metrics_text:
                found_metrics.append(metric)
                print(f"✅ Found metric: {metric}")
            else:
                print(f"❌ Missing metric: {metric}")

        if len(found_metrics) == len(required_metrics):
            print("✅ All required metrics are available")
            return True
        else:
            print(f"⚠️ Only {len(found_metrics)}/{len(required_metrics)} metrics found")
            return False

def main():
    setup = GrafanaDashboardSetup()

    print("📊 ATS Daily Prices Quality - Grafana Dashboard Setup")
    print("="*60)
    print("Since SignOz UI has a locale error, we're setting up Grafana as alternative")
    print()

    # Setup Grafana dashboard
    success = setup.setup_grafana_with_dashboard()

    if success:
        # Verify metrics
        setup.verify_dashboard_working()

        print("\n" + "="*60)
        print("🎉 GRAFANA DASHBOARD SETUP COMPLETE!")
        print("="*60)
        print(f"🌐 Grafana URL: {setup.grafana_url}")
        print(f"👤 Username: {setup.grafana_user}")
        print(f"🔐 Password: {setup.grafana_pass}")
        print(f"📊 Dashboard: Look for 'ATS Daily Prices Quality Monitoring'")
        print()
        print("🔧 Next steps:")
        print("1. Open Grafana in your browser")
        print("2. Login with admin/admin")
        print("3. Navigate to Dashboards > ATS Daily Prices Quality Monitoring")
        print("4. Verify panels show data from Pushgateway")
        print()
        print("📈 To update metrics, run:")
        print("   PYTHONPATH=src python3 scripts/daily_price_polygon_quality_metrics.py --environment intg --push-metrics")
    else:
        print("\n❌ Dashboard setup failed. Check logs above for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()