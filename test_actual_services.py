#!/usr/bin/env python3
"""
Test Actual Services

Actually start and test the backend and frontend services to verify they work.
"""

import subprocess
import time
import requests
import socket
import signal
import sys
import os
from pathlib import Path

def is_port_in_use(port):
    """Check if a port is in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(('localhost', port))
            return False
        except OSError:
            return True

def test_backend_startup():
    """Test if backend can actually start and respond"""
    print("🧪 Testing Backend Startup...")
    
    # Find a free port for testing
    test_port = 8000
    if is_port_in_use(8000):
        for port in range(8001, 8010):
            if not is_port_in_use(port):
                test_port = port
                break
        else:
            print("❌ No free ports available for backend testing")
            return False, None
    
    print(f"   Starting backend on port {test_port}...")
    
    # Start backend
    env = os.environ.copy()
    env['PYTHONPATH'] = str(Path('.') / "src")
    env['ENVIRONMENT'] = 'test'
    
    cmd = [
        'python', '-m', 'uvicorn',
        'api.backtest_analytics_api:app',
        '--host', '127.0.0.1',
        '--port', str(test_port),
        '--log-level', 'warning'
    ]
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            universal_newlines=True
        )
        
        # Wait for startup
        print(f"   Waiting for backend to start...")
        for i in range(20):  # Wait up to 20 seconds
            try:
                response = requests.get(f'http://localhost:{test_port}/health', timeout=2)
                if response.status_code == 200:
                    print(f"   ✅ Backend responding on port {test_port}")
                    print(f"   Health check: {response.json()}")
                    
                    # Test a few more endpoints
                    endpoints_to_test = [
                        ('/docs', 'API Documentation'),
                        ('/api/v1/backtests', 'Backtests endpoint')
                    ]
                    
                    for endpoint, description in endpoints_to_test:
                        try:
                            resp = requests.get(f'http://localhost:{test_port}{endpoint}', timeout=2)
                            print(f"   ✅ {description}: HTTP {resp.status_code}")
                        except Exception as e:
                            print(f"   ⚠️  {description}: {e}")
                    
                    return True, process
                    
            except requests.RequestException:
                time.sleep(1)
        
        # Backend didn't start properly
        stdout, stderr = process.communicate(timeout=3)
        print(f"   ❌ Backend failed to start properly")
        print(f"   Error: {stderr}")
        return False, process
        
    except Exception as e:
        print(f"   ❌ Failed to start backend: {e}")
        return False, None

def test_frontend_setup():
    """Test if frontend can be set up and configured"""
    print("\n🧪 Testing Frontend Setup...")
    
    frontend_dir = Path('.') / "frontend"
    
    # Check if frontend directory exists
    if not frontend_dir.exists():
        print("   ❌ Frontend directory does not exist")
        return False
    
    # Check package.json
    package_json = frontend_dir / "package.json"
    if not package_json.exists():
        print("   ❌ package.json not found")
        return False
    
    print("   ✅ Frontend directory and package.json exist")
    
    # Check if dependencies are installed
    node_modules = frontend_dir / "node_modules"
    if not node_modules.exists():
        print("   📦 Installing frontend dependencies...")
        try:
            result = subprocess.run(
                ['npm', 'install'],
                cwd=frontend_dir,
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode == 0:
                print("   ✅ Dependencies installed successfully")
            else:
                print(f"   ❌ Dependency installation failed: {result.stderr}")
                return False
        except Exception as e:
            print(f"   ❌ Failed to install dependencies: {e}")
            return False
    else:
        print("   ✅ Dependencies already installed")
    
    return True

def test_frontend_startup():
    """Test if frontend can actually start"""
    print("\n🧪 Testing Frontend Startup...")
    
    frontend_dir = Path('.') / "frontend"
    
    # Find a free port for testing
    test_port = 3001
    if is_port_in_use(3001):
        for port in range(3002, 3010):
            if not is_port_in_use(port):
                test_port = port
                break
        else:
            print("   ❌ No free ports available for frontend testing")
            return False, None
    
    print(f"   Starting React frontend on port {test_port}...")
    
    # Set up environment
    env = os.environ.copy()
    env['PORT'] = str(test_port)
    env['BROWSER'] = 'none'  # Don't open browser
    env['CI'] = 'true'  # Prevent interactive prompts
    
    try:
        process = subprocess.Popen(
            ['npm', 'start'],
            cwd=frontend_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        print(f"   Waiting for React to compile...")
        
        # Wait for React to start (takes longer than backend)
        for i in range(60):  # Wait up to 60 seconds for React
            try:
                response = requests.get(f'http://localhost:{test_port}', timeout=2)
                if response.status_code == 200:
                    print(f"   ✅ Frontend responding on port {test_port}")
                    
                    # Check if it's actually a React app
                    content = response.text
                    if 'react' in content.lower() or 'webpack' in content.lower() or 'root' in content:
                        print(f"   ✅ Confirmed React application")
                    else:
                        print(f"   ⚠️  Response received but may not be React app")
                    
                    return True, process
                    
            except requests.RequestException:
                time.sleep(1)
                
                # Check if process is still running
                if process.poll() is not None:
                    stdout, stderr = process.communicate()
                    print(f"   ❌ Frontend process died during startup")
                    print(f"   Error: {stderr}")
                    return False, process
        
        # Frontend didn't start properly
        print(f"   ❌ Frontend failed to start within 60 seconds")
        stdout, stderr = process.communicate(timeout=5)
        print(f"   Error: {stderr}")
        return False, process
        
    except Exception as e:
        print(f"   ❌ Failed to start frontend: {e}")
        return False, None

def main():
    """Main testing function"""
    print("🔍 " + "="*50)
    print("   ACTUAL SERVICE VERIFICATION TEST")
    print("="*52)
    
    processes = []
    
    try:
        # Test backend
        backend_success, backend_process = test_backend_startup()
        if backend_process:
            processes.append(backend_process)
        
        # Test frontend setup first
        frontend_setup_success = test_frontend_setup()
        
        if frontend_setup_success:
            # Test frontend startup
            frontend_success, frontend_process = test_frontend_startup()
            if frontend_process:
                processes.append(frontend_process)
        else:
            frontend_success = False
        
        # Summary
        print(f"\n" + "="*52)
        print("📋 VERIFICATION RESULTS:")
        print("="*52)
        
        if backend_success:
            print("✅ Backend: Successfully started and responding")
        else:
            print("❌ Backend: Failed to start or respond")
        
        if frontend_success:
            print("✅ Frontend: Successfully started and responding")
        elif frontend_setup_success:
            print("⚠️  Frontend: Setup OK but startup failed")
        else:
            print("❌ Frontend: Setup failed")
        
        if backend_success and frontend_success:
            print("\n🎉 BOTH SERVICES VERIFIED WORKING!")
            print("The analytics platform can actually run successfully.")
        elif backend_success:
            print("\n⚠️  PARTIAL SUCCESS: Backend works, frontend needs fixes")
        elif frontend_success:
            print("\n⚠️  PARTIAL SUCCESS: Frontend works, backend needs fixes")
        else:
            print("\n❌ BOTH SERVICES FAILED")
            print("The analytics platform needs significant fixes before it can run.")
        
        print("="*52)
        
        # Keep services running briefly for manual verification
        if processes:
            print("Services will run for 10 seconds for manual verification...")
            time.sleep(10)
        
        return backend_success and frontend_success
        
    finally:
        # Clean up processes
        print("\n🛑 Stopping test services...")
        for process in processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                try:
                    process.kill()
                except:
                    pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)