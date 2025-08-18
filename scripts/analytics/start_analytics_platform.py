#!/usr/bin/env python3
"""
Start Backtest Analytics Platform

This script starts both the backend API and frontend dashboard
for the backtest analytics platform.
"""

import os
import sys
import subprocess
import asyncio
import logging
import signal
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

def setup_logging():
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def check_dependencies():
    """Check if required dependencies are installed"""
    logger = logging.getLogger(__name__)
    
    # Check Python dependencies
    try:
        import fastapi
        import uvicorn
        import asyncpg
        import redis
        logger.info("✅ Python dependencies available")
    except ImportError as e:
        logger.error(f"❌ Missing Python dependency: {e}")
        logger.info("Install with: pip install -r requirements.txt")
        return False
    
    # Check Node.js and npm
    try:
        result = subprocess.run(['node', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"✅ Node.js available: {result.stdout.strip()}")
        else:
            raise FileNotFoundError()
    except FileNotFoundError:
        logger.error("❌ Node.js not found")
        logger.info("Install Node.js from: https://nodejs.org/")
        return False
    
    return True

def setup_database():
    """Setup database if needed"""
    logger = logging.getLogger(__name__)
    
    # Check if database is accessible
    try:
        # This would check database connectivity
        # For demo purposes, we'll assume it's available
        logger.info("✅ Database connectivity assumed available")
        return True
    except Exception as e:
        logger.error(f"❌ Database setup failed: {e}")
        return False

def start_backend():
    """Start the FastAPI backend"""
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting backend API server...")
    
    # Set environment variables
    env = os.environ.copy()
    env['PYTHONPATH'] = str(Path(__file__).parent.parent.parent / "src")
    env['ENVIRONMENT'] = 'dev'
    
    # Start FastAPI with uvicorn
    backend_cmd = [
        'python', '-m', 'uvicorn',
        'api.backtest_analytics_api:app',
        '--host', '0.0.0.0',
        '--port', '8000',
        '--reload'
    ]
    
    try:
        backend_process = subprocess.Popen(
            backend_cmd,
            env=env,
            cwd=Path(__file__).parent.parent.parent,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        logger.info("✅ Backend API started on http://localhost:8000")
        return backend_process
        
    except Exception as e:
        logger.error(f"❌ Failed to start backend: {e}")
        return None

def setup_frontend():
    """Setup and start the React frontend"""
    logger = logging.getLogger(__name__)
    
    frontend_dir = Path(__file__).parent.parent.parent / "frontend"
    
    # Install npm dependencies if needed
    if not (frontend_dir / "node_modules").exists():
        logger.info("📦 Installing frontend dependencies...")
        try:
            subprocess.run(['npm', 'install'], cwd=frontend_dir, check=True)
            logger.info("✅ Frontend dependencies installed")
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to install frontend dependencies: {e}")
            return None
    
    # Start React development server
    logger.info("🚀 Starting frontend development server...")
    
    env = os.environ.copy()
    env['REACT_APP_API_URL'] = 'http://localhost:8000'
    env['REACT_APP_WS_URL'] = 'ws://localhost:8000'
    
    try:
        frontend_process = subprocess.Popen(
            ['npm', 'start'],
            cwd=frontend_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        logger.info("✅ Frontend started on http://localhost:3000")
        return frontend_process
        
    except Exception as e:
        logger.error(f"❌ Failed to start frontend: {e}")
        return None

def wait_for_services():
    """Wait for services to be ready"""
    logger = logging.getLogger(__name__)
    
    # Wait for backend
    logger.info("⏳ Waiting for backend to be ready...")
    for i in range(30):  # Wait up to 30 seconds
        try:
            import requests
            response = requests.get('http://localhost:8000/health', timeout=1)
            if response.status_code == 200:
                logger.info("✅ Backend is ready")
                break
        except:
            time.sleep(1)
    else:
        logger.warning("⚠️ Backend may not be ready yet")
    
    # Wait for frontend
    logger.info("⏳ Waiting for frontend to be ready...")
    time.sleep(5)  # Give React time to start
    logger.info("✅ Frontend should be ready")

def main():
    """Main function to start the analytics platform"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    print("="*60)
    print("🚀 STARTING BACKTEST ANALYTICS PLATFORM")
    print("="*60)
    
    # Check dependencies
    if not check_dependencies():
        return 1
    
    # Setup database
    if not setup_database():
        logger.warning("⚠️ Database setup incomplete - some features may not work")
    
    # Start services
    backend_process = start_backend()
    if not backend_process:
        return 1
    
    frontend_process = setup_frontend()
    if not frontend_process:
        backend_process.terminate()
        return 1
    
    # Wait for services to be ready
    wait_for_services()
    
    print("\n" + "="*60)
    print("🎉 ANALYTICS PLATFORM READY!")
    print("="*60)
    print("📊 Dashboard: http://localhost:3000")
    print("🔗 API Docs: http://localhost:8000/docs")
    print("❤️  Health Check: http://localhost:8000/health")
    print("="*60)
    print("Press Ctrl+C to stop all services")
    print("="*60)
    
    # Handle shutdown gracefully
    def signal_handler(sig, frame):
        logger.info("\n🛑 Shutting down services...")
        
        if frontend_process:
            frontend_process.terminate()
            logger.info("✅ Frontend stopped")
        
        if backend_process:
            backend_process.terminate()
            logger.info("✅ Backend stopped")
        
        logger.info("👋 Analytics platform stopped")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Keep the script running
    try:
        while True:
            time.sleep(1)
            
            # Check if processes are still running
            if backend_process.poll() is not None:
                logger.error("❌ Backend process died")
                break
                
            if frontend_process.poll() is not None:
                logger.error("❌ Frontend process died")
                break
                
    except KeyboardInterrupt:
        signal_handler(None, None)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())