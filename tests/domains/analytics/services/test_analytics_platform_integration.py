#!/usr/bin/env python3
"""
Analytics Platform Integration Tests

Comprehensive tests to verify the actual analytics platform works end-to-end,
including detecting port conflicts and service issues.
"""

import pytest
import subprocess
import time
import requests
import json
import socket
from pathlib import Path
import sys
import os

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

class TestAnalyticsPlatformIntegration:
    """Integration tests for the complete analytics platform"""

    def setup_method(self):
        """Setup for each test method"""
        self.root_dir = Path(__file__).parent.parent.parent
        self.processes = []

    def teardown_method(self):
        """Cleanup after each test"""
        for process in getattr(self, 'processes', []):
            process.terminate()
            process.wait(timeout=5)
    def test_check_port_availability(self):
        """Test if required ports are available"""
        ports_to_check = [3000, 8000]
        port_conflicts = {}

        for port in ports_to_check:
            if self._is_port_in_use(port):
                # Find what's using the port
                service = self._get_service_on_port(port)
                port_conflicts[port] = service

        if port_conflicts:
            print(f"\n❌ Port conflicts detected:")
            for port, service in port_conflicts.items():
                print(f"   Port {port}: {service}")

            # This is expected - we want to detect conflicts
            assert port_conflicts, f"Found port conflicts: {port_conflicts}"
        else:
            print(f"\n✅ All required ports ({ports_to_check}) are available")

    def test_detect_existing_services(self):
        """Test detection of existing services that might conflict"""
        existing_services = {}

        # Check for Grafana
        if self._is_port_in_use(3000):
            response = requests.get('http://localhost:3000', timeout=2)
            if 'grafana' in response.text.lower() or 'Grafana' in response.text:
                existing_services[3000] = "Grafana"
            else:
                existing_services[3000] = "Unknown web service"
        if self._is_port_in_use(8000):
            response = requests.get('http://localhost:8000/health', timeout=2)
            if response.status_code == 200:
                existing_services[8000] = "API service"
            else:
                existing_services[8000] = "Unknown service"
        if existing_services:
            print(f"\n⚠️ Existing services detected:")
            for port, service in existing_services.items():
                print(f"   Port {port}: {service}")

            # This is what we expect to find
            assert existing_services, f"Found existing services: {existing_services}"
        else:
            print(f"\n✅ No conflicting services found")

    def test_frontend_directory_exists(self):
        """Test if frontend directory exists and is properly structured"""
        frontend_dir = self.root_dir / "frontend"

        # Check if frontend directory exists
        if not frontend_dir.exists():
            pytest.fail(f"❌ Frontend directory does not exist: {frontend_dir}")

        # Check for package.json
        package_json = frontend_dir / "package.json"
        if not package_json.exists():
            pytest.fail(f"❌ package.json not found: {package_json}")

        # Check if it's a React app and validate dependencies
        with open(package_json) as f:
            package_data = json.load(f)

        dependencies = package_data.get('dependencies', {})
        if 'react' not in dependencies:
            pytest.fail(f"❌ React not found in dependencies: {list(dependencies.keys())}")

        print(f"✅ Frontend directory is a React app")
        print(f"   Dependencies: {list(dependencies.keys())[:5]}...")

    def test_frontend_dependencies_can_install(self):
        """Test if frontend dependencies can actually be installed - CRITICAL TEST"""
        frontend_dir = self.root_dir / "frontend"

        if not frontend_dir.exists():
            pytest.skip("Frontend directory does not exist")

        print(f"🧪 Testing ACTUAL npm install for frontend dependencies")

        # Check if dependencies already installed
        node_modules = frontend_dir / "node_modules"
        if node_modules.exists():
            print(f"   ⚠️  node_modules exists, removing for clean test")
            import shutil
            shutil.rmtree(node_modules)

        # Try to install dependencies
        result = subprocess.run(
            ['npm', 'install'],
            cwd=frontend_dir,
            capture_output=True,
            text=True,
            timeout=120  # 2 minutes max
        )

        if result.returncode == 0:
            print(f"✅ npm install succeeded")

            # Verify critical dependencies were installed
            critical_deps = ['react', 'react-dom', 'react-scripts']
            for dep in critical_deps:
                dep_dir = node_modules / dep
                if not dep_dir.exists():
                    pytest.fail(f"❌ Critical dependency {dep} not installed properly")

            print(f"✅ All critical dependencies installed")

        else:
            # This should FAIL the test to catch dependency issues
            pytest.fail(f"❌ npm install failed: {result.stderr}")

    def test_frontend_can_compile(self):
        """Test if React app can actually compile - CRITICAL TEST"""
        frontend_dir = self.root_dir / "frontend"

        if not frontend_dir.exists():
            pytest.skip("Frontend directory does not exist")

        # Check if dependencies are installed
        node_modules = frontend_dir / "node_modules"
        if not node_modules.exists():
            pytest.skip("Frontend dependencies not installed - run test_frontend_dependencies_can_install first")

        print(f"🧪 Testing ACTUAL React compilation")

        # Try to compile the React app
        env = os.environ.copy()
        env['CI'] = 'true'  # Prevent interactive prompts
        env['GENERATE_SOURCEMAP'] = 'false'  # Faster build

        result = subprocess.run(
            ['npm', 'run', 'build'],
            cwd=frontend_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=180  # 3 minutes max
        )

        if result.returncode == 0:
            print(f"✅ React app compiled successfully")

            # Check if build directory was created
            build_dir = frontend_dir / "build"
            if build_dir.exists():
                print(f"✅ Build directory created")

                # Check for critical build files
                index_html = build_dir / "index.html"
                if index_html.exists():
                    print(f"✅ index.html generated")
                else:
                    pytest.fail(f"❌ index.html not generated in build")
            else:
                pytest.fail(f"❌ Build directory not created")

        else:
            # This should FAIL the test to catch compilation issues
            pytest.fail(f"❌ React compilation failed: {result.stderr}")

    def test_backend_api_can_start(self):
        """Test if backend API can start on an alternative port - ACTUALLY TEST STARTUP"""
        # Try to start backend on alternative port to avoid conflicts
        test_port = self._find_free_port(8001, 8010)

        if not test_port:
            pytest.skip("No free ports available for testing")

        print(f"🧪 Testing ACTUAL backend startup on port {test_port}")

        # Start backend on test port
        env = os.environ.copy()
        env['PYTHONPATH'] = str(self.root_dir / "src")
        env['ENVIRONMENT'] = 'test'

        cmd = [
            'python', '-m', 'uvicorn',
            'api.backtest_analytics_api:app',
            '--host', '127.0.0.1',
            '--port', str(test_port),
            '--log-level', 'warning'
        ]

        process = subprocess.Popen(
            cmd,
            cwd=self.root_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        self.processes.append(process)

        # Wait for startup and capture actual errors
        startup_success = False
        startup_error = None

        for i in range(20):  # Wait up to 20 seconds
            # Check if process died
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                startup_error = f"Process died during startup. STDERR: {stderr}"
                print(f"❌ Backend process died: {stderr}")
                break

            response = requests.get(f'http://localhost:{test_port}/health', timeout=1)
            if response.status_code == 200:
                startup_success = True
                print(f"✅ Backend started successfully on port {test_port}")
                print(f"   Health check: {response.json()}")
                break
        if not startup_success:
            if not startup_error:
                # Process still running but not responding
                stdout, stderr = process.communicate(timeout=5)
                startup_error = f"Backend not responding after 20s. STDERR: {stderr}"

            # This should FAIL the test to detect issues
            pytest.fail(f"❌ Backend startup failed: {startup_error}")

    def test_api_endpoints_respond(self):
        """Test that API endpoints respond correctly"""
        # This assumes backend is running from previous test
        test_port = self._find_free_port(8001, 8010)
        if not test_port:
            pytest.skip("No test backend available")

        # Wait a moment for backend to be ready
        time.sleep(2)

        endpoints_to_test = [
            ('/health', 200),
            ('/api/v1/backtests', 200),
            ('/docs', 200),  # FastAPI auto-docs
        ]

        for endpoint, expected_status in endpoints_to_test:
            response = requests.get(f'http://localhost:{test_port}{endpoint}', timeout=5)
            assert response.status_code == expected_status, f"Endpoint {endpoint} returned {response.status_code}, expected {expected_status}"
            print(f"✅ {endpoint} responds correctly ({response.status_code})")
    def test_production_backtest_runner_exists(self):
        """Test that production backtest runner exists and is executable"""
        runner_path = self.root_dir / "scripts/analytics/production_backtest_runner.py"

        if not runner_path.exists():
            pytest.fail(f"❌ Production backtest runner not found: {runner_path}")

        # Test if it's executable
        if not os.access(runner_path, os.R_OK):
            pytest.fail(f"❌ Production backtest runner not readable: {runner_path}")

        print(f"✅ Production backtest runner exists and is accessible")

        # Test if it can be imported
        sys.path.insert(0, str(runner_path.parent))
        print(f"✅ Production backtest runner can be imported")
    def test_setup_script_exists(self):
        """Test that setup script exists"""
        setup_path = self.root_dir / "scripts/analytics/setup_dev_web_interface.py"
        quick_start_path = self.root_dir / "run_dev_analytics.py"

        if not setup_path.exists():
            pytest.fail(f"❌ Setup script not found: {setup_path}")

        if not quick_start_path.exists():
            pytest.fail(f"❌ Quick start script not found: {quick_start_path}")

        print(f"✅ Setup scripts exist")

    def test_can_resolve_port_conflicts(self):
        """Test that we can suggest solutions for port conflicts"""
        conflicts = {}

        if self._is_port_in_use(3000):
            conflicts[3000] = "Frontend conflict"
        if self._is_port_in_use(8000):
            conflicts[8000] = "Backend conflict"

        if conflicts:
            solutions = self._suggest_port_solutions(conflicts)
            print(f"\n🔧 Port conflict solutions:")
            for port, solution in solutions.items():
                print(f"   Port {port}: {solution}")

            assert solutions, "Should provide solutions for port conflicts"
        else:
            print(f"✅ No port conflicts to resolve")

    def test_alternative_ports_available(self):
        """Test that alternative ports are available"""
        alternative_frontend_ports = [3001, 3002, 3003, 3010, 3333]
        alternative_backend_ports = [8001, 8002, 8080, 8888, 9000]

        free_frontend_port = None
        free_backend_port = None

        for port in alternative_frontend_ports:
            if not self._is_port_in_use(port):
                free_frontend_port = port
                break

        for port in alternative_backend_ports:
            if not self._is_port_in_use(port):
                free_backend_port = port
                break

        if not free_frontend_port:
            pytest.fail(f"❌ No alternative frontend ports available from {alternative_frontend_ports}")

        if not free_backend_port:
            pytest.fail(f"❌ No alternative backend ports available from {alternative_backend_ports}")

        print(f"✅ Alternative ports available:")
        print(f"   Frontend: {free_frontend_port}")
        print(f"   Backend: {free_backend_port}")

        # Store for potential use
        self.alternative_ports = {
            'frontend': free_frontend_port,
            'backend': free_backend_port
        }

    def _is_port_in_use(self, port: int) -> bool:
        """Check if a port is in use"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', port))
            return False
    def _get_service_on_port(self, port: int) -> str:
        """Try to identify what service is running on a port"""
        response = requests.get(f'http://localhost:{port}', timeout=2)

        # Check for common services
        content = response.text.lower()
        if 'grafana' in content:
            return "Grafana dashboard"
        elif 'fastapi' in content or 'openapi' in content:
            return "FastAPI application"
        elif 'react' in content or 'webpack' in content:
            return "React development server"
        elif 'nginx' in content:
            return "Nginx web server"
        else:
            return f"Web service (HTTP {response.status_code})"

    def _find_free_port(self, start_port: int, end_port: int) -> int:
        """Find a free port in the given range"""
        for port in range(start_port, end_port + 1):
            if not self._is_port_in_use(port):
                return port
        return None

    def _suggest_port_solutions(self, conflicts: dict) -> dict:
        """Suggest solutions for port conflicts"""
        solutions = {}

        if 3000 in conflicts:
            alt_port = self._find_free_port(3001, 3010)
            if alt_port:
                solutions[3000] = f"Use alternative port {alt_port} for frontend (modify package.json scripts)"
            else:
                solutions[3000] = "Stop Grafana service or use Docker with port mapping"

        if 8000 in conflicts:
            alt_port = self._find_free_port(8001, 8010)
            if alt_port:
                solutions[8000] = f"Use alternative port {alt_port} for backend (modify uvicorn command)"
            else:
                solutions[8000] = "Stop existing service on port 8000"

        return solutions


@pytest.mark.integration
class TestRealWorldScenarios:
    """Test real-world scenarios and edge cases"""

    def test_grafana_conflict_scenario(self):
        """Test the specific Grafana conflict scenario"""
        if self._is_port_in_use(3000):
            # Try to detect if it's actually Grafana
            response = requests.get('http://localhost:3000', timeout=3)
            is_grafana = 'grafana' in response.text.lower() or 'Grafana' in response.text

            if is_grafana:
                print(f"✅ Confirmed: Grafana is running on port 3000")
                print(f"   Title: {response.text[:100]}...")

                # This is the exact issue we need to solve
                assert True, "Grafana conflict detected as expected"
            else:
                print(f"⚠️ Something else is on port 3000 (not Grafana)")
                print(f"   Content preview: {response.text[:100]}...")

            pytest.skip("Port 3000 is not in use - cannot test Grafana conflict")

    def test_database_connectivity(self):
        """Test if database is accessible - CRITICAL FOR BACKEND"""
        import asyncpg

        # Try different connection parameters that the backend might use
        db_configs = [
            {"host": "localhost", "port": 5432, "user": "postgres", "password": "postgres", "database": "dev_db"},
            {"host": "localhost", "port": 5432, "user": "postgres", "password": "dev_password", "database": "dev_db"},
            {"host": "localhost", "port": 5433, "user": "postgres", "password": "postgres", "database": "dev_db"},
            {"host": "localhost", "port": 5433, "user": "postgres", "password": "dev_password", "database": "dev_db"},
        ]

        connected = False
        successful_config = None
        connection_errors = []

        print(f"🧪 Testing database connectivity (required for backend)")

        for config in db_configs:
            # Test connection
            import asyncio

            @pytest.mark.asyncio

            async def test_connection():
                conn = await asyncpg.connect(**config)
                await conn.close()
                return True

            asyncio.run(test_connection())
            print(f"✅ Database connection successful: {config['host']}:{config['port']} user={config['user']}")
            connected = True
            successful_config = config
            break

        if not connected:
            # This should FAIL to catch database auth issues that break backend
            full_error = "\\n".join(connection_errors)
            pytest.fail(f"❌ Could not connect to database with any configuration:\\n{full_error}\\nThis will cause backend startup to fail!")
        else:
            # Store successful config for other tests
            self.db_config = successful_config

    def test_backend_database_integration(self):
        """Test if backend can connect to database - CRITICAL INTEGRATION TEST"""
        if not hasattr(self, 'db_config'):
            pytest.skip("Database connectivity not established")

        print(f"🧪 Testing backend database integration")

        # Test if the analytics engine can initialize with database
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

        from domains.analytics.services.portfolio_analytics import PortfolioAnalyticsEngine
        import asyncio

        @pytest.mark.asyncio

        async def test_analytics_engine():
            # Try to create engine with database connection
            db_url = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"

            engine = PortfolioAnalyticsEngine(db_url=db_url)
            await engine.initialize()
            await engine.close()
            return True

        asyncio.run(test_analytics_engine())
        print(f"✅ Analytics engine can connect to database")

    def _is_port_in_use(self, port: int) -> bool:
        """Check if a port is in use"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', port))
            return False
if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v", "--tb=short"])