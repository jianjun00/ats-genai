#!/usr/bin/env python3
"""
Simple Analytics Service Test - Quick verification
"""

import requests
import time

# Test configuration
MINIKUBE_IP = "192.168.49.2"
ANALYTICS_PORT = "30001"
BASE_URL = f"http://{MINIKUBE_IP}:{ANALYTICS_PORT}"

def test_analytics_service():
    """Simple test to verify analytics service is working"""
    print("🧪 Testing Enhanced Analytics Service")
    print("=" * 50)
    
    try:
        print("1. Testing health endpoint...")
        response = requests.get(f"{BASE_URL}/health", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Health check passed")
            print(f"   Service: {data.get('service', 'unknown')}")
            print(f"   Port: {data.get('port', 'unknown')}")
            print(f"   Features: {len(data.get('features', []))} features")
            
            # Check for new features
            features = data.get('features', [])
            new_indicators = ['etop', 'ebot', 'pldot']
            found_indicators = [ind for ind in new_indicators if ind in features]
            
            if found_indicators:
                print(f"✅ New indicators found: {found_indicators}")
            
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
            
    except requests.RequestException as e:
        print(f"❌ Connection failed: {e}")
        print("🔄 Service may be starting up. This is normal.")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

def test_web_interface():
    """Test web interface accessibility"""
    try:
        print("\n2. Testing web interface...")
        response = requests.get(f"{BASE_URL}/", timeout=10)
        
        if response.status_code == 200:
            content = response.text
            if "ATS Analytics" in content and "STANDARDIZED" in content:
                print("✅ Web interface accessible with standardized branding")
                return True
            else:
                print("⚠️ Web interface accessible but content may be incomplete")
                return False
        else:
            print(f"❌ Web interface failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Web interface test error: {e}")
        return False

def main():
    """Main test function"""
    print(f"🎯 Testing Analytics Service at: {BASE_URL}")
    
    # Give service time to start
    print("⏳ Waiting for service to be ready...")
    time.sleep(5)
    
    health_ok = test_analytics_service()
    web_ok = test_web_interface()
    
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS")
    print("=" * 50)
    
    if health_ok and web_ok:
        print("🎉 ALL TESTS PASSED - Enhanced Analytics Service is working!")
        print("\n🌐 Access Information:")
        print(f"   URL: {BASE_URL}")
        print(f"   Health: {BASE_URL}/health")
        print(f"   Charts: {BASE_URL}/chart/1")
        print(f"   Datasets: {BASE_URL}/dataset/1")
    elif health_ok:
        print("⚠️ PARTIAL SUCCESS - Service is running but web interface may have issues")
    else:
        print("❌ SERVICE NOT READY - May still be starting up")
        print("💡 Try again in a few minutes or check pod logs")
        print("   kubectl logs deployment/ats-analytics-service -n ats-dev")

if __name__ == "__main__":
    main()