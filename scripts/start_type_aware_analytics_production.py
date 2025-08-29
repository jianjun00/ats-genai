#!/usr/bin/env python3
"""
Start Type-Aware Analytics Service in Production Mode
Uses Docker to run the standalone type-aware analytics service.
"""

import subprocess
import sys
import time

def main():
    print("🚀 DEPLOYING TYPE-AWARE ANALYTICS TO PRODUCTION")
    print("=" * 50)
    
    # Stop any existing analytics service
    try:
        subprocess.run(["python3", "scripts/run_dev.py", "stop", "--service", "analytics"], check=False)
        print("✅ Stopped existing analytics service")
    except:
        pass
    
    # Start the type-aware analytics service using Docker
    print("🐳 Starting type-aware analytics service in Docker...")
    
    cmd = [
        "docker", "run", 
        "--name", "ats-type-aware-analytics",
        "--rm", "-d",
        "-p", "3000:8000",
        "-v", f"{sys.path[0]}:/workspace",
        "-w", "/workspace",
        "dragonflyer762/ats-genai:latest",
        "python", "-c", """
import sys
sys.path.insert(0, 'src')

# Start the type-aware analytics service
from services.analytics.type_aware_analytics_standalone import app

# Run with uvicorn
import uvicorn
uvicorn.run(app, host='0.0.0.0', port=8000, log_level='info')
"""
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Type-aware analytics service started successfully")
            print("🌐 Available at: http://localhost:3000")
            
            # Wait a moment and test the service
            print("⏳ Waiting for service to be ready...")
            time.sleep(3)
            
            # Test the health endpoint
            health_cmd = ["curl", "-s", "http://localhost:3000/health"]
            health_result = subprocess.run(health_cmd, capture_output=True, text=True, timeout=10)
            
            if health_result.returncode == 0:
                print("✅ Service health check passed")
                print(f"📊 Response: {health_result.stdout[:100]}...")
            else:
                print("⚠️  Service may still be starting up")
            
            return 0
        else:
            print(f"❌ Failed to start service: {result.stderr}")
            return 1
            
    except Exception as e:
        print(f"❌ Error starting service: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())