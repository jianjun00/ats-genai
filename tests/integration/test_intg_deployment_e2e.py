#!/usr/bin/env python3
"""
End-to-End Integration Tests for ATS-INTG Deployment
Tests complete deployment workflow including Docker Compose, database, and services
"""

import pytest
import subprocess
import time
import requests
import psycopg2
from psycopg2 import sql
import docker
import os
import sys
from datetime import datetime


class TestDockerComposeDeployment:
    """Test Docker Compose deployment and orchestration."""
    
    @classmethod
    def setup_class(cls):
        """Setup test environment."""
        cls.docker_client = docker.from_env()
        cls.compose_file = 'docker-compose.intg-jobs.yml'
        cls.project_name = 'ats-genai-data'
        
    def test_docker_compose_config_valid(self):
        """Test Docker Compose configuration is valid."""
        result = subprocess.run([
            'docker-compose', '-f', self.compose_file, 'config', '--quiet'
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, f"Docker Compose config invalid: {result.stderr}"
    
    def test_docker_compose_services_defined(self):
        """Test all required services are defined in compose file."""
        result = subprocess.run([
            'docker-compose', '-f', self.compose_file, 'config', '--services'
        ], capture_output=True, text=True)
        
        assert result.returncode == 0
        services = result.stdout.strip().split('\n')
        
        expected_services = ['postgres-intg', 'ats-intg-scheduler', 'ats-intg-dashboard']
        for service in expected_services:
            assert service in services, f"Service {service} not found in compose config"
    
    def test_containers_running(self):
        """Test that all containers are running."""
        expected_containers = [
            'postgres-intg',
            'ats-intg-scheduler', 
            'ats-intg-dashboard'
        ]
        
        for container_name in expected_containers:
            try:
                container = self.docker_client.containers.get(container_name)
                assert container.status == 'running', f"Container {container_name} is not running: {container.status}"
            except docker.errors.NotFound:
                pytest.fail(f"Container {container_name} not found")
    
    def test_container_health_checks(self):
        """Test container health checks are passing."""
        # Wait a bit for health checks to complete
        time.sleep(10)
        
        health_containers = ['postgres-intg']  # Only postgres has health check
        
        for container_name in health_containers:
            container = self.docker_client.containers.get(container_name)
            container.reload()
            
            health_status = container.attrs['State']['Health']['Status']
            assert health_status == 'healthy', f"Container {container_name} health check failed: {health_status}"


class TestPostgreSQLIntegration:
    """Test PostgreSQL database integration and connectivity."""
    
    @classmethod
    def setup_class(cls):
        """Setup database connection parameters."""
        cls.db_params = {
            'host': 'localhost',
            'port': 5434,
            'database': 'intg_db',
            'user': 'postgres',
            'password': 'intg_password'
        }
        
        # Wait for database to be ready
        max_attempts = 30
        for _ in range(max_attempts):
            try:
                conn = psycopg2.connect(**cls.db_params)
                conn.close()
                break
            except psycopg2.OperationalError:
                time.sleep(2)
        else:
            pytest.fail("PostgreSQL database failed to become ready within timeout")
    
    def test_database_connectivity(self):
        """Test database connection works."""
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        
        assert 'PostgreSQL' in version
        
        cur.close()
        conn.close()
    
    def test_required_tables_exist(self):
        """Test that all required INTG tables exist."""
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        # Check for INTG tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name LIKE 'intg_%'
            ORDER BY table_name;
        """)
        
        tables = [row[0] for row in cur.fetchall()]
        expected_tables = [
            'intg_daily_prices',
            'intg_fundamentals_comprehensive',
            'intg_instruments'
        ]
        
        for table in expected_tables:
            assert table in tables, f"Required table {table} not found"
        
        cur.close()
        conn.close()
    
    def test_table_schemas_correct(self):
        """Test that table schemas are correctly defined."""
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        # Test intg_instruments schema
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'intg_instruments'
            ORDER BY ordinal_position;
        """)
        
        columns = cur.fetchall()
        column_names = [col[0] for col in columns]
        
        required_columns = ['id', 'symbol', 'name', 'exchange', 'created_at', 'updated_at']
        for column in required_columns:
            assert column in column_names, f"Required column {column} not found in intg_instruments"
        
        # Test intg_daily_prices schema
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'intg_daily_prices'
            ORDER BY ordinal_position;
        """)
        
        columns = cur.fetchall()
        column_names = [col[0] for col in columns]
        
        required_columns = ['symbol', 'date', 'vendor', 'open_price', 'close_price', 'volume']
        for column in required_columns:
            assert column in column_names, f"Required column {column} not found in intg_daily_prices"
        
        cur.close()
        conn.close()
    
    def test_database_indexes_exist(self):
        """Test that required indexes are created."""
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        # Check for expected indexes
        cur.execute("""
            SELECT indexname, tablename
            FROM pg_indexes
            WHERE schemaname = 'public'
            AND tablename LIKE 'intg_%'
            ORDER BY tablename, indexname;
        """)
        
        indexes = cur.fetchall()
        index_names = [idx[0] for idx in indexes]
        
        expected_indexes = [
            'idx_intg_instruments_active',
            'idx_intg_daily_prices_symbol_date',
            'idx_intg_daily_prices_vendor_date'
        ]
        
        for index in expected_indexes:
            assert any(index in idx_name for idx_name in index_names), f"Required index {index} not found"
        
        cur.close()
        conn.close()
    
    def test_database_permissions(self):
        """Test database user permissions."""
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        # Test insert permission
        cur.execute("""
            INSERT INTO intg_instruments (symbol, name, exchange) 
            VALUES ('TEST_INTG', 'Test Integration Stock', 'NASDAQ')
            ON CONFLICT (symbol) DO NOTHING;
        """)
        
        # Test update permission
        cur.execute("""
            UPDATE intg_instruments 
            SET name = 'Test Integration Stock Updated'
            WHERE symbol = 'TEST_INTG';
        """)
        
        # Test select permission
        cur.execute("SELECT COUNT(*) FROM intg_instruments WHERE symbol = 'TEST_INTG';")
        count = cur.fetchone()[0]
        assert count >= 0
        
        # Clean up test data
        cur.execute("DELETE FROM intg_instruments WHERE symbol = 'TEST_INTG';")
        
        conn.commit()
        cur.close()
        conn.close()


class TestStartupManagerIntegration:
    """Test startup manager integration and functionality."""
    
    def test_startup_manager_logs_generated(self):
        """Test startup manager generates logs."""
        # Check if startup logs exist
        log_file = '/mnt/d/ats-logs/intg/startup.log'
        
        assert os.path.exists(log_file), "Startup log file not found"
        
        with open(log_file, 'r') as f:
            logs = f.read()
        
        # Check for key startup messages
        assert 'ATS-INTG Startup Manager' in logs
        assert 'PostgreSQL is ready' in logs
        assert 'Startup Manager Complete' in logs
    
    def test_startup_report_generated(self):
        """Test startup report is generated."""
        report_file = '/mnt/d/ats-logs/intg/startup_report.md'
        
        assert os.path.exists(report_file), "Startup report file not found"
        
        with open(report_file, 'r') as f:
            report = f.read()
        
        # Check report content
        assert 'ATS-INTG Startup Status Report' in report
        assert 'INTG Database Status' in report
        assert 'DEV Database Connectivity' in report
    
    def test_startup_manager_container_running(self):
        """Test startup manager container is running continuously."""
        docker_client = docker.from_env()
        container = docker_client.containers.get('ats-intg-scheduler')
        
        assert container.status == 'running'
        
        # Check that it's been running for at least a few seconds
        container.reload()
        started_at = container.attrs['State']['StartedAt']
        # Container should be running (basic check)
        assert container.attrs['State']['Running'] is True


class TestDashboardIntegration:
    """Test monitoring dashboard integration."""
    
    @classmethod
    def setup_class(cls):
        """Wait for dashboard to be ready."""
        max_attempts = 30
        for _ in range(max_attempts):
            try:
                response = requests.get('http://localhost:4000/health', timeout=5)
                if response.status_code == 200:
                    break
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        else:
            pytest.fail("Dashboard failed to become ready within timeout")
    
    def test_dashboard_health_endpoint(self):
        """Test dashboard health endpoint."""
        response = requests.get('http://localhost:4000/health')
        
        assert response.status_code == 200
        
        health_data = response.json()
        assert health_data['status'] == 'healthy'
        assert health_data['service'] == 'ats-intg-dashboard'
        assert health_data['environment'] == 'intg'
    
    def test_dashboard_status_endpoint(self):
        """Test dashboard status endpoint."""
        response = requests.get('http://localhost:4000/status')
        
        assert response.status_code == 200
        
        status_data = response.json()
        assert 'timestamp' in status_data
        assert 'services' in status_data
        assert status_data['environment'] == 'intg'
        
        services = status_data['services']
        assert services['startup_manager'] == 'running'
        assert services['postgresql'] == 'connected'
        assert services['dashboard'] == 'active'
    
    def test_dashboard_html_page(self):
        """Test dashboard HTML page loads."""
        response = requests.get('http://localhost:4000/')
        
        assert response.status_code == 200
        assert 'ATS-INTG Monitor Dashboard' in response.text
        assert 'System Status' in response.text
        assert 'Database' in response.text
    
    def test_dashboard_container_logs(self):
        """Test dashboard container has proper logs."""
        docker_client = docker.from_env()
        container = docker_client.containers.get('ats-intg-dashboard')
        
        logs = container.logs(tail=10).decode('utf-8')
        
        assert 'Starting ATS-INTG Monitor Dashboard' in logs
        assert 'Dashboard accessible at' in logs


class TestNetworkConnectivity:
    """Test network connectivity between services."""
    
    def test_scheduler_to_postgres_connectivity(self):
        """Test scheduler can connect to PostgreSQL."""
        docker_client = docker.from_env()
        scheduler_container = docker_client.containers.get('ats-intg-scheduler')
        
        # Test connection from scheduler container
        result = scheduler_container.exec_run([
            'python3', '-c',
            '''
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5)
result = sock.connect_ex(("postgres-intg", 5432))
sock.close()
print("SUCCESS" if result == 0 else "FAILED")
            '''
        ])
        
        output = result.output.decode('utf-8').strip()
        assert 'SUCCESS' in output, f"Scheduler cannot connect to PostgreSQL: {output}"
    
    def test_dashboard_to_postgres_connectivity(self):
        """Test dashboard can connect to PostgreSQL."""
        docker_client = docker.from_env()
        dashboard_container = docker_client.containers.get('ats-intg-dashboard')
        
        # Test connection from dashboard container
        result = dashboard_container.exec_run([
            'python3', '-c',
            '''
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5)
result = sock.connect_ex(("postgres-intg", 5432))
sock.close()
print("SUCCESS" if result == 0 else "FAILED")
            '''
        ])
        
        output = result.output.decode('utf-8').strip()
        assert 'SUCCESS' in output, f"Dashboard cannot connect to PostgreSQL: {output}"
    
    def test_external_port_access(self):
        """Test external access to exposed services."""
        # Test PostgreSQL external access
        conn_params = {
            'host': 'localhost',
            'port': 5434,
            'database': 'intg_db',
            'user': 'postgres',
            'password': 'intg_password'
        }
        
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()[0]
        assert result == 1
        cur.close()
        conn.close()
        
        # Test dashboard external access
        response = requests.get('http://localhost:4000/health')
        assert response.status_code == 200


class TestVolumesMountedCorrectly:
    """Test that volumes are mounted correctly."""
    
    def test_workspace_volume_mounted(self):
        """Test workspace volume is mounted correctly."""
        docker_client = docker.from_env()
        container = docker_client.containers.get('ats-intg-scheduler')
        
        # Check if scripts are accessible
        result = container.exec_run(['ls', '/workspace/scripts/intg_startup_manager.py'])
        assert result.exit_code == 0, "Workspace volume not mounted correctly"
    
    def test_logs_volume_mounted(self):
        """Test logs volume is mounted correctly."""
        docker_client = docker.from_env()
        container = docker_client.containers.get('ats-intg-scheduler')
        
        # Check if logs directory is writable
        result = container.exec_run(['touch', '/logs/test_write_permission'])
        assert result.exit_code == 0, "Logs volume not writable"
        
        # Clean up test file
        container.exec_run(['rm', '-f', '/logs/test_write_permission'])
    
    def test_postgres_data_volume_persistent(self):
        """Test PostgreSQL data volume is persistent."""
        docker_client = docker.from_env()
        
        # Check volume exists
        volumes = docker_client.volumes.list(filters={'name': 'postgres_intg_data'})
        assert len(volumes) > 0, "PostgreSQL data volume not found"
        
        postgres_volume = volumes[0]
        assert postgres_volume.name.endswith('postgres_intg_data')


if __name__ == '__main__':
    # Run with specific markers for integration tests
    pytest.main([
        __file__, 
        '-v',
        '--tb=short',
        '-m', 'not slow'  # Skip slow tests by default
    ])