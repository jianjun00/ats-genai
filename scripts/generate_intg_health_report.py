#!/usr/bin/env python3
"""
Generate comprehensive health report for ATS-INTG environment
"""

import subprocess
import sys
import json
from datetime import datetime, timezone

def check_containers():
    """Check container status"""
    result = subprocess.run(
        ["docker", "ps", "--filter", "name=ats-intg", "--format", "{{.Names}}\t{{.Status}}\t{{.Ports}}"],
        capture_output=True, text=True
    )
    
    containers = {}
    for line in result.stdout.strip().split('\n'):
        if line:
            parts = line.split('\t')
            name = parts[0]
            status = parts[1] if len(parts) > 1 else "unknown"
            ports = parts[2] if len(parts) > 2 else "none"
            containers[name] = {"status": status, "ports": ports}
    
    return containers

def check_database_data():
    """Check database data counts"""
    queries = [
        ("Daily Prices", "SELECT COUNT(*) FROM intg_daily_prices"),
        ("Instruments", "SELECT COUNT(*) FROM intg_instruments"),
        ("Polygon RT", "SELECT COUNT(*) FROM intg_one_minute_live_polygon"),
        ("Tiingo RT", "SELECT COUNT(*) FROM intg_one_minute_live_tiingo"),
        ("FMP RT", "SELECT COUNT(*) FROM intg_one_minute_live_fmp"),
    ]
    
    data_counts = {}
    for name, query in queries:
        try:
            result = subprocess.run([
                "bash", "-c", f"PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -t -c \"{query}\""
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                count = int(result.stdout.strip())
                data_counts[name] = count
            else:
                data_counts[name] = "error"
        except:
            data_counts[name] = "error"
    
    return data_counts

def check_recent_data():
    """Check for recent data activity"""
    recent_data = {}
    vendors = ['polygon', 'tiingo', 'fmp']
    
    for vendor in vendors:
        try:
            result = subprocess.run([
                "bash", "-c", 
                f"PGPASSWORD=intg_password psql -h localhost -p 5434 -U postgres -d intg_db -t -c \"SELECT MAX(received_at) FROM intg_one_minute_live_{vendor}\""
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                timestamp = result.stdout.strip()
                recent_data[vendor] = timestamp if timestamp else "no data"
            else:
                recent_data[vendor] = "error"
        except:
            recent_data[vendor] = "error"
    
    return recent_data

def main():
    """Generate and display health report"""
    print("🏥 ATS-INTG COMPREHENSIVE HEALTH REPORT")
    print("=" * 60)
    print(f"📅 Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print()
    
    # Container Status
    print("🐳 CONTAINER STATUS:")
    containers = check_containers()
    for name, info in containers.items():
        status_icon = "✅" if "Up" in info["status"] else "❌"
        print(f"   {status_icon} {name}: {info['status']}")
        if info["ports"] != "none":
            print(f"      Ports: {info['ports']}")
    print()
    
    # Database Data
    print("🗄️ DATABASE DATA COUNTS:")
    data_counts = check_database_data()
    total_records = 0
    for dataset, count in data_counts.items():
        if isinstance(count, int):
            status_icon = "✅" if count > 0 else "⚠️"
            print(f"   {status_icon} {dataset}: {count:,} records")
            total_records += count
        else:
            print(f"   ❌ {dataset}: {count}")
    print(f"   📊 Total Records: {total_records:,}")
    print()
    
    # Recent Activity
    print("⚡ REAL-TIME DATA ACTIVITY:")
    recent_data = check_recent_data()
    for vendor, timestamp in recent_data.items():
        if timestamp and timestamp != "no data" and timestamp != "error":
            print(f"   ✅ {vendor.upper()}: Latest data at {timestamp}")
        elif timestamp == "no data":
            print(f"   ⚠️ {vendor.upper()}: No timestamp data available")
        else:
            print(f"   ❌ {vendor.upper()}: {timestamp}")
    print()
    
    # Overall Health Assessment
    print("🎯 OVERALL HEALTH ASSESSMENT:")
    
    # Check critical components
    postgres_running = any("postgres-intg" in name and "Up" in info["status"] for name, info in containers.items())
    dashboard_running = any("ats-intg-dashboard" in name and "Up" in info["status"] for name, info in containers.items())
    scheduler_running = any("ats-intg-scheduler" in name and "Up" in info["status"] for name, info in containers.items())
    
    has_daily_data = data_counts.get("Daily Prices", 0) > 0
    has_rt_data = (data_counts.get("Polygon RT", 0) > 0 and 
                   data_counts.get("Tiingo RT", 0) > 0 and 
                   data_counts.get("FMP RT", 0) > 0)
    
    health_checks = [
        ("PostgreSQL Database", postgres_running),
        ("Dashboard Service", dashboard_running),
        ("Scheduler Service", scheduler_running),
        ("Daily Prices Data", has_daily_data),
        ("Real-time Data Tables", has_rt_data),
    ]
    
    passed_checks = 0
    for check_name, passed in health_checks:
        icon = "✅" if passed else "❌"
        print(f"   {icon} {check_name}")
        if passed:
            passed_checks += 1
    
    print(f"\n   📊 Health Score: {passed_checks}/{len(health_checks)} ({passed_checks/len(health_checks)*100:.0f}%)")
    
    if passed_checks == len(health_checks):
        print("   🎉 SYSTEM HEALTHY: All critical components operational")
    elif passed_checks >= len(health_checks) * 0.8:
        print("   🟡 SYSTEM DEGRADED: Minor issues detected")
    else:
        print("   🔴 SYSTEM UNHEALTHY: Critical issues require attention")
    
    print("\n" + "=" * 60)
    print("📋 RECOMMENDATIONS:")
    
    if not has_daily_data:
        print("   • Run daily price backfill job to populate historical data")
    if not has_rt_data:
        print("   • Start real-time collection jobs for market data")
    if not dashboard_running:
        print("   • Restart dashboard service for monitoring capabilities")
    if passed_checks == len(health_checks):
        print("   • System is fully operational - continue monitoring")
    
    print("=" * 60)

if __name__ == "__main__":
    main()