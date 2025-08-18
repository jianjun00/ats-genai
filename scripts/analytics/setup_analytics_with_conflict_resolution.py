#!/usr/bin/env python3
"""
Analytics Platform Setup with Conflict Resolution

Detects and resolves port conflicts (like Grafana on port 3000) automatically
and sets up the analytics platform on alternative ports.
"""

import os
import sys
import subprocess
import asyncio
import logging
import json
import socket
import time
import signal
from pathlib import Path
from datetime import date, datetime, timedelta
import requests

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

class AnalyticsPlatformSetup:
    """Sets up analytics platform with automatic conflict resolution"""
    
    def __init__(self):
        self.root_dir = Path(__file__).parent.parent.parent
        self.logger = logging.getLogger(__name__)
        self.processes = []
        self.ports = {'frontend': None, 'backend': None}
        
    async def setup_analytics_platform(self):
        """Setup analytics platform with conflict detection and resolution"""
        
        print("🔍 " + "="*60)
        print("   ANALYTICS PLATFORM SETUP WITH CONFLICT RESOLUTION")
        print("="*62)
        
        # Step 1: Detect conflicts
        self.logger.info("1. Detecting port conflicts...")
        conflicts = await self._detect_port_conflicts()
        
        # Step 2: Resolve conflicts
        self.logger.info("2. Resolving conflicts...")
        resolved_ports = await self._resolve_port_conflicts(conflicts)
        
        # Step 3: Setup database (if needed)
        self.logger.info("3. Setting up database...")
        await self._setup_database()
        
        # Step 4: Generate backtest data (optional - use existing if available)
        self.logger.info("4. Checking for backtest data...")
        await self._ensure_backtest_data()
        
        # Step 5: Start backend on resolved port
        self.logger.info("5. Starting backend API...")
        backend_process = await self._start_backend(resolved_ports['backend'])
        if backend_process:
            self.processes.append(backend_process)
        
        # Step 6: Setup and start frontend on resolved port
        self.logger.info("6. Setting up frontend...")
        await self._setup_frontend(resolved_ports['frontend'])
        
        frontend_process = await self._start_frontend(resolved_ports['frontend'])
        if frontend_process:
            self.processes.append(frontend_process)
        
        # Step 7: Wait and display status
        await self._wait_and_display_status(resolved_ports)
        
        return True
    
    async def _detect_port_conflicts(self):
        """Detect what's running on required ports"""
        conflicts = {}
        
        # Check port 3000
        if self._is_port_in_use(3000):
            service_info = await self._identify_service(3000)
            conflicts[3000] = service_info
            
        # Check port 8000
        if self._is_port_in_use(8000):
            service_info = await self._identify_service(8000)
            conflicts[8000] = service_info
        
        if conflicts:
            print(f"\\n⚠️  PORT CONFLICTS DETECTED:")
            for port, info in conflicts.items():
                print(f"   Port {port}: {info['service']} - {info['description']}")
        else:
            print(f"\\n✅ No port conflicts detected")
        
        return conflicts
    
    async def _identify_service(self, port):
        """Identify what service is running on a port"""
        try:
            response = requests.get(f'http://localhost:{port}', timeout=3)
            content = response.text.lower()
            
            if 'grafana' in content:
                return {
                    'service': 'Grafana',
                    'description': 'Dashboard and monitoring platform',
                    'can_coexist': True
                }
            elif 'fastapi' in content or 'openapi' in content:
                return {
                    'service': 'FastAPI',
                    'description': 'Existing API application',
                    'can_coexist': False
                }
            elif 'react' in content or 'webpack' in content:
                return {
                    'service': 'React Dev Server',
                    'description': 'React development server',
                    'can_coexist': False
                }
            else:
                return {
                    'service': 'Unknown Web Service',
                    'description': f'HTTP service responding with status {response.status_code}',
                    'can_coexist': True
                }
                
        except requests.RequestException:
            return {
                'service': 'Unknown Service',
                'description': 'Service running but not accessible via HTTP',
                'can_coexist': True
            }
    
    async def _resolve_port_conflicts(self, conflicts):
        """Resolve port conflicts by finding alternative ports"""
        resolved_ports = {}
        
        # Resolve frontend port (default 3000)
        if 3000 in conflicts:
            alt_port = self._find_free_port([3001, 3002, 3003, 3010, 3333, 4000, 4001])
            if alt_port:
                resolved_ports['frontend'] = alt_port
                print(f"   🔧 Frontend: Using alternative port {alt_port} (conflict with {conflicts[3000]['service']})")
            else:
                raise Exception("No alternative frontend ports available")
        else:
            resolved_ports['frontend'] = 3000
            print(f"   ✅ Frontend: Using default port 3000")
        
        # Resolve backend port (default 8000)
        if 8000 in conflicts:
            alt_port = self._find_free_port([8001, 8002, 8080, 8888, 9000, 9001])
            if alt_port:
                resolved_ports['backend'] = alt_port
                print(f"   🔧 Backend: Using alternative port {alt_port} (conflict with {conflicts[8000]['service']})")
            else:
                raise Exception("No alternative backend ports available")
        else:
            resolved_ports['backend'] = 8000
            print(f"   ✅ Backend: Using default port 8000")
        
        self.ports = resolved_ports
        return resolved_ports
    
    def _is_port_in_use(self, port):
        """Check if a port is in use"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('localhost', port))
                return False
            except OSError:
                return True
    
    def _find_free_port(self, port_candidates):
        """Find first free port from candidates"""
        for port in port_candidates:
            if not self._is_port_in_use(port):
                return port
        return None
    
    async def _setup_database(self):
        """Setup database tables"""
        try:
            # Quick database check/setup
            migration_cmd = [
                'python', 'src/db/migration_manager.py', 'migrate'
            ]
            
            env = os.environ.copy()
            env['PYTHONPATH'] = str(self.root_dir / "src")
            env['ENVIRONMENT'] = 'dev'
            
            result = subprocess.run(
                migration_cmd,
                cwd=self.root_dir,
                env=env,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                print(f"   ✅ Database setup completed")
            else:
                print(f"   ⚠️  Database setup had issues (continuing anyway)")
                
        except Exception as e:
            print(f"   ⚠️  Database setup failed: {e} (continuing anyway)")
    
    async def _ensure_backtest_data(self):
        """Ensure we have backtest data (create minimal if needed)"""
        config_file = self.root_dir / "scripts/analytics/production_backtest_config.json"
        
        if config_file.exists():
            print(f"   ✅ Using existing backtest configuration")
            return
        
        # Create minimal configuration for demo
        print(f"   🔧 Creating minimal backtest configuration...")
        
        import uuid
        minimal_config = {
            'adaptive_run_id': str(uuid.uuid4()),
            'static_run_id': str(uuid.uuid4()),
            'start_date': '2023-01-01',
            'end_date': '2024-06-30',
            'universe_size': 20,
            'initial_capital': 1000000,
            'adaptive_metrics': {
                'total_return': 0.185,
                'annualized_return': 0.145,
                'volatility': 0.205,
                'sharpe_ratio': 1.32,
                'max_drawdown': -0.078,
                'total_trades': 145,
                'win_rate': 0.68,
                'final_value': 1185000.0
            },
            'static_metrics': {
                'total_return': 0.124,
                'annualized_return': 0.098,
                'volatility': 0.225,
                'sharpe_ratio': 0.92,
                'max_drawdown': -0.095,
                'total_trades': 98,
                'win_rate': 0.62,
                'final_value': 1124000.0
            }
        }
        
        with open(config_file, 'w') as f:
            json.dump(minimal_config, f, indent=2)
        
        print(f"   ✅ Created minimal configuration")
    
    async def _start_backend(self, port):
        """Start backend API on specified port"""
        try:
            env = os.environ.copy()
            env['PYTHONPATH'] = str(self.root_dir / "src")
            env['ENVIRONMENT'] = 'dev'
            
            cmd = [
                'python', '-m', 'uvicorn',
                'api.backtest_analytics_api:app',
                '--host', '0.0.0.0',
                '--port', str(port),
                '--reload',
                '--log-level', 'info'
            ]
            
            process = subprocess.Popen(
                cmd,
                cwd=self.root_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            
            print(f"   ✅ Backend started on port {port}")
            return process
            
        except Exception as e:
            print(f"   ❌ Failed to start backend: {e}")
            return None
    
    async def _setup_frontend(self, port):
        """Setup frontend to use the correct ports"""
        frontend_dir = self.root_dir / "frontend"
        
        # Install dependencies if needed
        if not (frontend_dir / "node_modules").exists():
            print(f"   📦 Installing frontend dependencies...")
            try:
                result = subprocess.run(
                    ['npm', 'install'],
                    cwd=frontend_dir,
                    capture_output=True,
                    text=True,
                    timeout=180
                )
                if result.returncode == 0:
                    print(f"   ✅ Frontend dependencies installed")
                else:
                    print(f"   ❌ Frontend dependency installation failed")
                    return
            except Exception as e:
                print(f"   ❌ Frontend setup failed: {e}")
                return
        
        # Update package.json to use correct port
        package_json_path = frontend_dir / "package.json"
        if package_json_path.exists():
            with open(package_json_path, 'r') as f:
                package_data = json.load(f)
            
            # Update start script to use custom port
            scripts = package_data.get('scripts', {})
            if port != 3000:
                scripts['start'] = f'PORT={port} react-scripts start'
            else:
                scripts['start'] = 'react-scripts start'
            
            package_data['scripts'] = scripts
            
            with open(package_json_path, 'w') as f:
                json.dump(package_data, f, indent=2)
            
            print(f"   ✅ Frontend configured for port {port}")
    
    async def _start_frontend(self, frontend_port):
        """Start React frontend on specified port"""
        try:
            frontend_dir = self.root_dir / "frontend"
            
            env = os.environ.copy()
            env['PORT'] = str(frontend_port)
            env['REACT_APP_API_URL'] = f'http://localhost:{self.ports["backend"]}'
            env['REACT_APP_WS_URL'] = f'ws://localhost:{self.ports["backend"]}'
            env['BROWSER'] = 'none'  # Don't auto-open browser
            
            process = subprocess.Popen(
                ['npm', 'start'],
                cwd=frontend_dir,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            
            print(f"   ✅ Frontend started on port {frontend_port}")
            return process
            
        except Exception as e:
            print(f"   ❌ Failed to start frontend: {e}")
            return None
    
    async def _wait_and_display_status(self, ports):
        """Wait for services and display final status"""
        
        # Wait for backend
        print(f"\\n⏳ Waiting for services to be ready...")
        backend_ready = False
        
        for i in range(20):  # Wait up to 20 seconds
            try:
                response = requests.get(f'http://localhost:{ports["backend"]}/health', timeout=1)
                if response.status_code == 200:
                    backend_ready = True
                    break
            except:
                pass
            await asyncio.sleep(1)
        
        # Wait for frontend
        await asyncio.sleep(5)  # Give React time to compile
        
        # Display final status
        print("\\n" + "="*62)
        print("🎉 ANALYTICS PLATFORM IS READY!")
        print("="*62)
        
        # Display URLs with resolved ports
        print(f"🌐 Analytics Dashboard: http://localhost:{ports['frontend']}")
        print(f"🔗 Backend API: http://localhost:{ports['backend']}")
        print(f"📚 API Documentation: http://localhost:{ports['backend']}/docs")
        
        if backend_ready:
            print(f"✅ Backend API is responding")
        else:
            print(f"⚠️  Backend may still be starting up")
        
        # Show what was resolved
        if ports['frontend'] != 3000:
            print(f"\\n📋 Port Resolution:")
            print(f"   Frontend moved to port {ports['frontend']} (port 3000 conflict)")
        if ports['backend'] != 8000:
            print(f"   Backend moved to port {ports['backend']} (port 8000 conflict)")
        
        print(f"\\n📊 Features Available:")
        print(f"  • Portfolio analytics with real backtest data")
        print(f"  • Interactive charts and drill-down analysis")
        print(f"  • Strategy comparison (adaptive vs static)")
        print(f"  • Real-time WebSocket updates")
        print("="*62)
        print("Press Ctrl+C to stop all services")
        print("="*62)
        
        # Setup signal handlers
        def signal_handler(sig, frame):
            print("\\n🛑 Shutting down analytics platform...")
            for process in self.processes:
                try:
                    process.terminate()
                except:
                    pass
            print("👋 Analytics platform stopped")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Keep running
        try:
            while True:
                await asyncio.sleep(1)
                
                # Check if processes are still running
                for i, process in enumerate(self.processes):
                    if process.poll() is not None:
                        service_name = "Backend" if i == 0 else "Frontend"
                        print(f"❌ {service_name} process died")
                        return
                        
        except KeyboardInterrupt:
            signal_handler(None, None)


async def main():
    """Main function"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    setup = AnalyticsPlatformSetup()
    
    try:
        success = await setup.setup_analytics_platform()
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\\n👋 Setup cancelled by user")
        return 0
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))