#!/usr/bin/env python3
"""
Quick test runner for the Training EDA Dashboard Playwright tests
"""

import subprocess
import sys
import os

def run_playwright_test():
    """Run the Playwright test for training EDA dashboard"""
    
    print("🧪 Running Playwright tests for Training EDA Dashboard...")
    
    # Set environment variables
    env = os.environ.copy()
    env['PYTHONPATH'] = '/home/jianjun/ats-genai-data'
    
    try:
        # Run the specific test
        result = subprocess.run([
            sys.executable, "-m", "pytest", 
            "tests/playwright/test_training_eda_dashboard.py::TestTrainingEDADashboard::test_training_eda_page_loads",
            "-v", "-s"
        ], 
        env=env,
        cwd="/home/jianjun/ats-genai-data",
        capture_output=True, 
        text=True,
        timeout=60
        )
        
        print("📊 Test Results:")
        print("-" * 50)
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")  
            print(result.stderr)
        print("-" * 50)
        print(f"Return code: {result.returncode}")
        
        if result.returncode == 0:
            print("✅ Training EDA Dashboard Playwright test PASSED!")
        else:
            print("❌ Training EDA Dashboard Playwright test FAILED!")
            
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print("⏰ Test timed out after 60 seconds")
        return False
    except Exception as e:
        print(f"❌ Error running test: {e}")
        return False

def check_dashboard_manually():
    """Manually check that the dashboard endpoints work"""
    import requests
    
    print("\n🌐 Manual Dashboard Check:")
    print("-" * 30)
    
    base_url = "http://localhost:3000"
    
    # Check training EDA page
    try:
        response = requests.get(f"{base_url}/training-eda", timeout=10)
        if response.status_code == 200:
            print("✅ Training EDA page accessible")
        else:
            print(f"❌ Training EDA page failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Training EDA page error: {e}")
    
    # Check API endpoints
    try:
        response = requests.get(f"{base_url}/api/v1/training-datasets", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Training datasets API: {len(data.get('datasets', []))} datasets found")
        else:
            print(f"❌ Training datasets API failed: {response.status_code}")
    except Exception as e:
        print(f"❌ Training datasets API error: {e}")
    
    # Check specific dataset data
    try:
        response = requests.get(f"{base_url}/api/v1/training-datasets/8/data?page=1&limit=1", timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('data') and len(data['data']) > 0:
                print("✅ AAPL dataset data accessible")
            else:
                print("❌ AAPL dataset data empty")
        else:
            print(f"❌ AAPL dataset data failed: {response.status_code}")
    except Exception as e:
        print(f"❌ AAPL dataset data error: {e}")

if __name__ == "__main__":
    print("🚀 Training EDA Dashboard Test Runner")
    print("=" * 50)
    
    # First do manual checks
    check_dashboard_manually()
    
    # Then run Playwright test
    success = run_playwright_test()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 All tests completed successfully!")
        sys.exit(0)
    else:
        print("💥 Some tests failed!")
        sys.exit(1)