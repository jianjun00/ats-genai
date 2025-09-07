#!/usr/bin/env python3
"""
Setup Grafana Vendor Monitoring Dashboard

Configures Grafana with PostgreSQL data source and imports vendor monitoring dashboard.
This replaces the custom web dashboard with proper Grafana integration.
"""

import json
import requests
import sys
from pathlib import Path

# Grafana configuration
GRAFANA_URL = "http://localhost:4002"
GRAFANA_USER = "admin"
GRAFANA_PASS = "admin"  # Default Grafana password

def setup_grafana_datasource():
    """Add PostgreSQL data source to Grafana."""

    print("🔌 Setting up PostgreSQL data source in Grafana...")

    datasource_config = {
        "name": "ATS-INTG-PostgreSQL",
        "type": "postgres",
        "url": "ats-intg-postgres:5432",
        "database": "intg_db",
        "user": "postgres",
        "secureJsonData": {
            "password": "intg_password"
        },
        "access": "proxy",
        "isDefault": False,
        "jsonData": {
            "sslmode": "disable",
            "maxOpenConns": 100,
            "maxIdleConns": 100,
            "connMaxLifetime": 14400,
            "postgresVersion": 1300,
            "timescaledb": False
        }
    }

    try:
        response = requests.post(
            f"{GRAFANA_URL}/api/datasources",
            auth=(GRAFANA_USER, GRAFANA_PASS),
            headers={"Content-Type": "application/json"},
            json=datasource_config,
            timeout=10
        )

        if response.status_code == 200:
            print("✅ PostgreSQL data source added successfully")
            return True
        elif response.status_code == 409:
            print("ℹ️  PostgreSQL data source already exists")
            return True
        else:
            print(f"❌ Failed to add data source: {response.status_code} - {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to connect to Grafana: {e}")
        return False

def import_dashboard():
    """Import vendor monitoring dashboard."""

    print("📊 Importing vendor monitoring dashboard...")

    # Load dashboard JSON
    dashboard_path = Path(__file__).parent.parent / "config/grafana/ats-vendor-monitoring-dashboard-postgres.json"

    try:
        with open(dashboard_path, 'r') as f:
            dashboard_data = json.load(f)

        # Wrap in import format
        import_payload = {
            "dashboard": dashboard_data["dashboard"],
            "overwrite": True,
            "inputs": [],
            "folderId": 0
        }

        response = requests.post(
            f"{GRAFANA_URL}/api/dashboards/import",
            auth=(GRAFANA_USER, GRAFANA_PASS),
            headers={"Content-Type": "application/json"},
            json=import_payload,
            timeout=10
        )

        if response.status_code == 200:
            result = response.json()
            dashboard_url = f"{GRAFANA_URL}/d/{result['dashboardId']}/ats-vendor-monitoring-dashboard-postgresql"
            print(f"✅ Dashboard imported successfully")
            print(f"🎯 Access at: {dashboard_url}")
            return True
        else:
            print(f"❌ Failed to import dashboard: {response.status_code} - {response.text}")
            return False

    except FileNotFoundError:
        print(f"❌ Dashboard file not found: {dashboard_path}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to connect to Grafana: {e}")
        return False
    except Exception as e:
        print(f"❌ Error importing dashboard: {e}")
        return False

def test_grafana_connection():
    """Test connection to Grafana."""

    print("🔍 Testing Grafana connection...")

    try:
        response = requests.get(
            f"{GRAFANA_URL}/api/health",
            timeout=5
        )

        if response.status_code == 200:
            health_data = response.json()
            print(f"✅ Grafana is running (version {health_data.get('version', 'unknown')})")
            return True
        else:
            print(f"❌ Grafana health check failed: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot connect to Grafana at {GRAFANA_URL}: {e}")
        print("💡 Make sure Grafana is running: docker ps | grep grafana")
        return False

def main():
    """Main setup process."""

    print("🚀 Setting up Grafana Vendor Monitoring Dashboard")
    print("=" * 60)

    # Test connection
    if not test_grafana_connection():
        sys.exit(1)

    # Setup data source
    if not setup_grafana_datasource():
        print("⚠️  Warning: Data source setup failed, but continuing...")

    # Import dashboard
    if not import_dashboard():
        sys.exit(1)

    print("\n" + "=" * 60)
    print("✅ Grafana vendor monitoring setup complete!")
    print(f"🎯 Access Grafana at: {GRAFANA_URL}")
    print("📊 Look for 'ATS Vendor Monitoring Dashboard (PostgreSQL)'")
    print("\n💡 This replaces the custom web dashboard - use Grafana instead!")

if __name__ == "__main__":
    main()