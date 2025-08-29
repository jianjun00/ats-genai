#!/usr/bin/env python3
"""
Test script for ATS monitoring infrastructure
"""
import requests
import sys
import time
import os

def test_prometheus_endpoint(url, name):
    """Test Prometheus endpoint"""
    print(f"🔍 Testing {name} at {url}")
    try:
        response = requests.get(f"{url}/api/v1/targets", timeout=10)
        if response.status_code == 200:
            targets = response.json().get('data', {}).get('activeTargets', [])
            healthy_targets = [t for t in targets if t.get('health') == 'up']
            print(f"  ✅ {name} accessible - {len(healthy_targets)}/{len(targets)} targets healthy")
            return True
        else:
            print(f"  ❌ {name} returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"  ❌ {name} connection failed: {e}")
        return False

def test_grafana_endpoint(url, name):
    """Test Grafana endpoint"""
    print(f"🔍 Testing {name} at {url}")
    try:
        response = requests.get(f"{url}/api/health", timeout=10)
        if response.status_code == 200:
            print(f"  ✅ {name} accessible")
            return True
        else:
            print(f"  ❌ {name} returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"  ❌ {name} connection failed: {e}")
        return False

def test_alertmanager_endpoint(url, name):
    """Test AlertManager endpoint"""
    print(f"🔍 Testing {name} at {url}")
    try:
        response = requests.get(f"{url}/api/v1/status", timeout=10)
        if response.status_code == 200:
            print(f"  ✅ {name} accessible")
            return True
        else:
            print(f"  ❌ {name} returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"  ❌ {name} connection failed: {e}")
        return False

def test_data_quality_exporter(url, name):
    """Test Data Quality Exporter endpoint"""
    print(f"🔍 Testing {name} at {url}")
    try:
        response = requests.get(f"{url}/metrics", timeout=10)
        if response.status_code == 200:
            metrics_text = response.text
            ats_metrics = [line for line in metrics_text.split('\n') if line.startswith('ats_')]
            print(f"  ✅ {name} accessible - {len(ats_metrics)} ATS metrics found")
            
            # Show sample metrics
            for metric in ats_metrics[:3]:
                print(f"    📊 {metric[:80]}...")
            return True
        else:
            print(f"  ❌ {name} returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"  ❌ {name} connection failed: {e}")
        return False

def main():
    """Main test function"""
    print("🚀 ATS Monitoring Infrastructure Test")
    print("=" * 50)
    
    # Get node IP (you'll need to provide this)
    node_ip = os.getenv('NODE_IP', 'localhost')
    if node_ip == 'localhost':
        print("⚠️  Using localhost - set NODE_IP environment variable for actual cluster IP")
    
    results = []
    
    # Test Development Environment
    print("\n📍 Testing ATS-DEV Environment")
    print("-" * 30)
    results.append(test_prometheus_endpoint(f"http://{node_ip}:30090", "Prometheus (dev)"))
    results.append(test_grafana_endpoint(f"http://{node_ip}:30300", "Grafana (dev)"))
    results.append(test_alertmanager_endpoint(f"http://{node_ip}:30093", "AlertManager (dev)"))
    
    # Test Integration Environment  
    print("\n📍 Testing ATS-INTG Environment")
    print("-" * 30)
    results.append(test_prometheus_endpoint(f"http://{node_ip}:30091", "Prometheus (intg)"))
    results.append(test_grafana_endpoint(f"http://{node_ip}:30301", "Grafana (intg)"))
    results.append(test_alertmanager_endpoint(f"http://{node_ip}:30094", "AlertManager (intg)"))
    
    # Test Data Quality Exporters (via port-forward)
    print("\n📊 Testing Data Quality Exporters")
    print("-" * 30)
    print("💡 Note: Data Quality Exporters require port-forwarding to test:")
    print("   kubectl port-forward -n ats-dev service/ats-data-quality-exporter 8080:8080")
    print("   kubectl port-forward -n ats-intg service/ats-data-quality-exporter 8081:8080")
    
    # Test if port-forwards are active
    results.append(test_data_quality_exporter("http://localhost:8080", "Data Quality Exporter (dev)"))
    results.append(test_data_quality_exporter("http://localhost:8081", "Data Quality Exporter (intg)"))
    
    # Summary
    print("\n📋 Test Summary")
    print("=" * 20)
    passed = sum(results)
    total = len(results)
    
    print(f"✅ Passed: {passed}/{total} tests")
    if passed == total:
        print("🎉 All monitoring components are accessible!")
    else:
        print(f"⚠️  {total - passed} components need attention")
    
    # Next steps
    print("\n🔗 Access URLs:")
    print(f"Grafana (dev):      http://{node_ip}:30300 (admin/ats-dev-password)")
    print(f"Grafana (intg):     http://{node_ip}:30301 (admin/ats-intg-password)")
    print(f"Prometheus (dev):   http://{node_ip}:30090")
    print(f"Prometheus (intg):  http://{node_ip}:30091")
    print(f"AlertManager (dev): http://{node_ip}:30093")
    print(f"AlertManager (intg):http://{node_ip}:30094")
    
    return 0 if passed == total else 1

if __name__ == "__main__":
    sys.exit(main())