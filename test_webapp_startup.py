#!/usr/bin/env python3
"""
Simple standalone test to verify webapp startup
Following CLAUDE.md requirement to test actual functionality
"""
import subprocess
import time
import requests
import os

def test_webapp_startup():
    """Test webapp can actually start and respond"""
    print("🚀 Testing webapp startup...")
    
    # Set environment
    env = os.environ.copy()
    env['PYTHONPATH'] = 'src'
    
    # Start webapp
    print("📦 Starting webapp process...")
    process = subprocess.Popen(
        ['python', 'unified_backtest_analytics_webapp.py'],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    try:
        # Wait a bit for startup
        time.sleep(8)
        
        # Check if process is still alive
        if process.poll() is not None:
            stdout, _ = process.communicate()
            print(f"❌ Webapp process died. Output:\n{stdout}")
            return False
        
        print("🔍 Testing health endpoint...")
        # Test health endpoint
        max_retries = 10
        for i in range(max_retries):
            try:
                response = requests.get('http://localhost:3000/health', timeout=3)
                if response.status_code == 200:
                    print("✅ Health endpoint working!")
                    health_data = response.json()
                    print(f"📊 Health data: {health_data}")
                    break
            except requests.exceptions.ConnectionError as e:
                print(f"🔄 Attempt {i+1}/{max_retries} - Connection failed: {e}")
                time.sleep(2)
        else:
            print("❌ Health endpoint never became available")
            return False
        
        # Test job runs endpoint
        print("🔍 Testing job runs endpoint...")
        try:
            response = requests.get('http://localhost:3000/api/v1/job-runs?limit=3')
            print(f"📊 Job runs status: {response.status_code}")
            if response.status_code == 200:
                jobs = response.json()
                print(f"✅ Job runs data: {len(jobs)} jobs found")
            elif response.status_code == 503:
                print("⚠️  Database connection unavailable (expected in some cases)")
            else:
                print(f"❌ Unexpected status: {response.status_code}")
        except Exception as e:
            print(f"❌ Job runs endpoint failed: {e}")
        
        # Test training datasets endpoint
        print("🔍 Testing training datasets endpoint...")
        try:
            response = requests.get('http://localhost:3000/api/v1/training-datasets?limit=3')
            print(f"📊 Training datasets status: {response.status_code}")
            if response.status_code == 200:
                datasets = response.json()
                print(f"✅ Training datasets: {len(datasets)} datasets found")
            else:
                print(f"❌ Unexpected status: {response.status_code}")
        except Exception as e:
            print(f"❌ Training datasets endpoint failed: {e}")
        
        # Test main dashboard
        print("🔍 Testing main dashboard...")
        try:
            response = requests.get('http://localhost:3000/')
            if response.status_code == 200:
                html = response.text
                if 'Job Runs' in html and 'Training Data' in html:
                    print("✅ Main dashboard contains new sections!")
                else:
                    print("❌ Main dashboard missing new sections")
            else:
                print(f"❌ Dashboard failed: {response.status_code}")
        except Exception as e:
            print(f"❌ Dashboard test failed: {e}")
        
        print("✅ Webapp tests completed!")
        return True
        
    finally:
        # Cleanup
        print("🧹 Cleaning up...")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

if __name__ == "__main__":
    success = test_webapp_startup()
    exit(0 if success else 1)