#!/usr/bin/env python3
"""
Test Daily Sync Configuration

Validates the daily sync setup and monitoring integration.
"""

import sys
import os
import subprocess
from datetime import datetime

def test_sync_scripts():
    """Test that sync scripts exist and are executable."""

    print("🧪 Testing Daily Sync Configuration")
    print("=" * 50)

    # Test script existence
    sync_script = "scripts/eodhd_database_sync.py"
    if not os.path.exists(sync_script):
        print(f"❌ Sync script not found: {sync_script}")
        return False

    print(f"✅ Sync script found: {sync_script}")

    # Test systemd files
    service_file = "config/systemd/ats-daily-sync.service"
    timer_file = "config/systemd/ats-daily-sync.timer"

    if os.path.exists(service_file):
        print(f"✅ Service file created: {service_file}")
    else:
        print(f"❌ Service file missing: {service_file}")
        return False

    if os.path.exists(timer_file):
        print(f"✅ Timer file created: {timer_file}")
    else:
        print(f"❌ Timer file missing: {timer_file}")
        return False

    # Test log directory
    log_dir = "/mnt/d/ats-logs"
    if os.path.exists(log_dir) and os.access(log_dir, os.W_OK):
        print(f"✅ Log directory ready: {log_dir}")
    else:
        print(f"❌ Log directory not accessible: {log_dir}")
        return False

    # Test Prometheus connectivity
    import requests
    response = requests.get("http://localhost:9091/metrics", timeout=5)
    if response.status_code == 200:
        print("✅ Pushgateway accessible")
    else:
        print(f"⚠️  Pushgateway returned status {response.status_code}")
    print()
    print("📋 Setup Summary:")
    print(f"   Service: {service_file}")
    print(f"   Timer: {timer_file}")
    print(f"   Schedule: Monday-Friday at 1:00 AM")
    print(f"   Logs: {log_dir}/daily-sync.log")
    print(f"   Metrics: http://localhost:9091/metrics")
    print()
    print("🚀 Next Steps:")
    print("   1. Run: sudo ./scripts/setup_daily_sync.sh")
    print("   2. Start timer: sudo systemctl start ats-daily-sync.timer")
    print("   3. Test service: sudo systemctl start ats-daily-sync.service")
    print("   4. Check dashboard: http://10.0.0.79:4002/d/a94a33f2-aeea-4b56-93c4-4d22a0cf1c2b")

    return True

if __name__ == "__main__":
    success = test_sync_scripts()
    sys.exit(0 if success else 1)