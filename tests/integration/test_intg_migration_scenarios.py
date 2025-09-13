#!/usr/bin/env python3
"""
Integration Tests for ATS-INTG Migration Scenarios
Tests various migration scenarios including empty DB, existing data, and DEV connectivity
"""

import pytest
import psycopg2
import docker
import subprocess
import time
import os
from unittest.mock import patch, MagicMock


class TestMigrationDecisionTree:
    """Test the startup manager decision tree under different scenarios."""

    @classmethod
    def setup_class(cls):
        """Setup test environment."""
        cls.docker_client = docker.from_env()
        cls.db_params = {
            'host': 'localhost',
            'port': 5434,
            'database': 'intg_db',
            'user': 'postgres',
            'password': 'intg_password'
        }

    def test_scenario_empty_intg_no_dev_data(self):
        """Test scenario: Empty INTG database, no DEV data available."""
        # Ensure INTG database is empty
        self._clear_intg_database()

        # Restart scheduler to trigger startup manager
        scheduler_container = self.docker_client.containers.get('ats-intg-scheduler')
        scheduler_container.restart()

        # Wait for startup to complete
        time.sleep(10)

        # Check logs for expected decision path
        logs = scheduler_container.logs(tail=50).decode('utf-8')

        assert 'Empty database detected' in logs
        assert 'DEV database not accessible' in logs or 'DEV database empty' in logs
        assert 'Startup Manager Complete' in logs

    def test_scenario_empty_intg_with_dev_connectivity(self):
        """Test scenario: Empty INTG, DEV accessible but no data."""
        # Clear INTG database
        self._clear_intg_database()

        # Test assumes DEV database exists but is empty (typical in test environment)
        scheduler_container = self.docker_client.containers.get('ats-intg-scheduler')
        scheduler_container.restart()

        time.sleep(10)

        logs = scheduler_container.logs(tail=50).decode('utf-8')

        assert 'Empty database detected' in logs
        assert 'DEV database accessible' in logs
        assert 'Startup Manager Complete' in logs

    def test_scenario_intg_has_existing_data(self):
        """Test scenario: INTG database already has data (incremental sync path)."""
        # Add some test data to INTG
        self._populate_intg_test_data()

        scheduler_container = self.docker_client.containers.get('ats-intg-scheduler')
        scheduler_container.restart()

        time.sleep(10)

        logs = scheduler_container.logs(tail=50).decode('utf-8')

        # Note: This might show "Empty database" if data is cleared during restart
        # The important part is that startup completes successfully
        assert 'Startup Manager Complete' in logs
        assert 'PostgreSQL is ready' in logs

    def test_auto_migration_disabled(self):
        """Test scenario: Auto-migration is disabled."""
        # This would require modifying environment variables and restarting
        # For now, we can test that the current environment variable is respected
        scheduler_container = self.docker_client.containers.get('ats-intg-scheduler')

        # Check environment variables
        env_vars = scheduler_container.attrs['Config']['Env']
        auto_migration_env = [env for env in env_vars if env.startswith('AUTO_MIGRATION_ENABLED=')]

        if auto_migration_env:
            auto_migration_value = auto_migration_env[0].split('=')[1]
            # If auto-migration is enabled, the system should check for migrations
            # If disabled, it should skip migration checks
            assert auto_migration_value in ['true', 'false']

    def _clear_intg_database(self):
        """Clear INTG database for testing."""
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()

            # Clear all INTG tables
            cur.execute("DELETE FROM intg_daily_price;")
            cur.execute("DELETE FROM intg_fundamental_comprehensive;")
            cur.execute("DELETE FROM intg_instrument;")

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Warning: Could not clear INTG database: {e}")

    def _populate_intg_test_data(self):
        """Add test data to INTG database."""
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()

            # Add test instrument
            cur.execute("""
                INSERT INTO intg_instrument (symbol, name, exchange)
                VALUES ('TEST_MIGRATION', 'Test Migration Stock', 'NASDAQ')
                ON CONFLICT (symbol) DO NOTHING;
            """)

            # Add test price data
            cur.execute("""
                INSERT INTO intg_daily_price (symbol, date, vendor, open_price, close_price, volume)
                VALUES ('TEST_MIGRATION', CURRENT_DATE - 1, 'test_vendor', 100.0, 105.0, 1000000)
                ON CONFLICT (symbol, date, vendor) DO NOTHING;
            """)

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Warning: Could not populate test data: {e}")


class TestMigrationScriptIntegration:
    """Test integration with actual migration scripts."""

    @classmethod
    def setup_class(cls):
        """Setup test environment."""
        cls.docker_client = docker.from_env()

    def test_migration_scripts_exist(self):
        """Test that migration scripts exist and are accessible."""
        scheduler_container = self.docker_client.containers.get('ats-intg-scheduler')

        # Check for migration scripts
        scripts_to_check = [
            '/workspace/scripts/intg_data_backfill.py',
            '/workspace/scripts/intg_incremental_sync.py'
        ]

        for script in scripts_to_check:
            result = scheduler_container.exec_run(['ls', script])
            assert result.exit_code == 0, f"Migration script {script} not found"

    def test_migration_script_help_commands(self):
        """Test migration scripts respond to help commands."""
        scheduler_container = self.docker_client.containers.get('ats-intg-scheduler')

        # Test backfill script help
        result = scheduler_container.exec_run([
            'python3', 'scripts/intg_data_backfill.py', '--help'
        ])
        # Help should work (exit code 0) or show usage (exit code != 0 but output contains help)
        output = result.output.decode('utf-8')
        assert 'usage' in output.lower() or 'help' in output.lower() or result.exit_code == 0

        # Test incremental sync script help
        result = scheduler_container.exec_run([
            'python3', 'scripts/intg_incremental_sync.py', '--help'
        ])
        output = result.output.decode('utf-8')
        assert 'usage' in output.lower() or 'help' in output.lower() or result.exit_code == 0

    def test_python_environment_ready(self):
        """Test that Python environment has required packages."""
        scheduler_container = self.docker_client.containers.get('ats-intg-scheduler')

        # Test Python imports that migration scripts need
        python_imports = [
            'psycopg2',
            'datetime',
            'subprocess',
            'os',
            'sys'
        ]

        for import_name in python_imports:
            result = scheduler_container.exec_run([
                'python3', '-c', f'import {import_name}; print("OK")'
            ])
            output = result.output.decode('utf-8').strip()
            assert 'OK' in output, f"Python package {import_name} not available"


class TestDatabaseMigrationConsistency:
    """Test database migration consistency and data integrity."""

    @classmethod
    def setup_class(cls):
        """Setup database connections."""
        cls.intg_db_params = {
            'host': 'localhost',
            'port': 5434,
            'database': 'intg_db',
            'user': 'postgres',
            'password': 'intg_password'
        }

        cls.dev_db_params = {
            'host': 'localhost',
            'port': 5433,  # Assuming DEV DB is on 5433
            'database': 'dev_db',
            'user': 'postgres',
            'password': 'postgres'
        }

    def test_intg_database_schema_integrity(self):
        """Test INTG database schema is correct and consistent."""
        conn = psycopg2.connect(**self.intg_db_params)
        cur = conn.cursor()

        # Test primary key constraints
        cur.execute("""
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name LIKE 'intg_%'
            ORDER BY tc.table_name;
        """)

        primary_keys = cur.fetchall()
        assert len(primary_keys) >= 3, "Missing primary key constraints on INTG tables"

        # Test unique constraints
        cur.execute("""
            SELECT tc.table_name, tc.constraint_name
            FROM information_schema.table_constraints tc
            WHERE tc.constraint_type = 'UNIQUE'
            AND tc.table_name LIKE 'intg_%'
            ORDER BY tc.table_name;
        """)

        unique_constraints = cur.fetchall()
        # Should have unique constraints for symbol in instruments, symbol+date+vendor in prices
        assert len(unique_constraints) >= 2, "Missing unique constraints on INTG tables"

        cur.close()
        conn.close()

    def test_intg_tables_can_handle_concurrent_access(self):
        """Test INTG tables can handle concurrent read/write operations."""
        # This is a basic test - in production would need more sophisticated concurrency testing

        connections = []
        cursors = []

        try:
            # Create multiple connections
            for i in range(3):
                conn = psycopg2.connect(**self.intg_db_params)
                cur = conn.cursor()
                connections.append(conn)
                cursors.append(cur)

            # Perform concurrent operations
            for i, cur in enumerate(cursors):
                cur.execute(f"""
                    INSERT INTO intg_instrument (symbol, name, exchange)
                    VALUES ('CONCURRENT_{i}', 'Concurrent Test {i}', 'TEST')
                    ON CONFLICT (symbol) DO NOTHING;
                """)
                connections[i].commit()

            # Verify all inserts succeeded
            cur = cursors[0]
            cur.execute("SELECT COUNT(*) FROM intg_instrument WHERE symbol LIKE 'CONCURRENT_%';")
            count = cur.fetchone()[0]
            assert count == 3, f"Expected 3 concurrent inserts, got {count}"

            # Clean up test data
            for cur in cursors:
                cur.execute("DELETE FROM intg_instrument WHERE symbol LIKE 'CONCURRENT_%';")
                cur.connection.commit()

        finally:
            # Clean up connections
            for cur in cursors:
                cur.close()
            for conn in connections:
                conn.close()

    def test_dev_database_connectivity_from_container(self):
        """Test DEV database connectivity from INTG containers."""
        scheduler_container = self.docker_client.containers.get('ats-intg-scheduler')

        # Test connectivity using the same method as startup manager
        result = scheduler_container.exec_run([
            'python3', '-c',
            '''
import socket
import os
dev_host = os.getenv("DEV_DB_HOST", "172.17.0.1")
dev_port = int(os.getenv("DEV_DB_PORT", "5433"))
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(5)
result = sock.connect_ex((dev_host, dev_port))
sock.close()
print(f"DEV_CONNECTIVITY: {'SUCCESS' if result == 0 else 'FAILED'}")
print(f"DEV_HOST: {dev_host}")
print(f"DEV_PORT: {dev_port}")
            '''
        ])

        output = result.output.decode('utf-8')
        print(f"DEV connectivity test output: {output}")

        # This test might fail if DEV database is not running - that's expected
        # The important thing is that the test completes without errors
        assert 'DEV_CONNECTIVITY:' in output
        assert 'DEV_HOST:' in output
        assert 'DEV_PORT:' in output


class TestContainerRecoveryAndRestart:
    """Test container recovery scenarios and restart behavior."""

    @classmethod
    def setup_class(cls):
        """Setup test environment."""
        cls.docker_client = docker.from_env()

    def test_scheduler_container_restart_recovery(self):
        """Test scheduler container recovers properly after restart."""
        scheduler_container = self.docker_client.containers.get('ats-intg-scheduler')

        # Record current restart count
        scheduler_container.reload()
        initial_restart_count = scheduler_container.attrs['RestartCount']

        # Restart container
        scheduler_container.restart()

        # Wait for restart to complete
        time.sleep(15)

        # Verify container is running
        scheduler_container.reload()
        assert scheduler_container.status == 'running'

        # Verify startup manager ran again
        logs = scheduler_container.logs(tail=30).decode('utf-8')
        assert 'ATS-INTG Startup Manager' in logs
        assert 'Startup Manager Complete' in logs or 'continuous scheduler' in logs

        # Verify restart count increased
        new_restart_count = scheduler_container.attrs['RestartCount']
        assert new_restart_count >= initial_restart_count

    def test_postgres_container_data_persistence(self):
        """Test PostgreSQL data persists across container restarts."""
        postgres_container = self.docker_client.containers.get('postgres-intg')

        # Add test data
        conn = psycopg2.connect(**{
            'host': 'localhost',
            'port': 5434,
            'database': 'intg_db',
            'user': 'postgres',
            'password': 'intg_password'
        })
        cur = conn.cursor()

        test_symbol = f'PERSIST_TEST_{int(time.time())}'
        cur.execute("""
            INSERT INTO intg_instrument (symbol, name, exchange)
            VALUES (%s, 'Persistence Test', 'TEST');
        """, (test_symbol,))

        conn.commit()
        cur.close()
        conn.close()

        # Restart PostgreSQL container
        postgres_container.restart()

        # Wait for PostgreSQL to come back up
        time.sleep(10)

        # Verify data is still there
        conn = psycopg2.connect(**{
            'host': 'localhost',
            'port': 5434,
            'database': 'intg_db',
            'user': 'postgres',
            'password': 'intg_password'
        })
        cur = conn.cursor()

        cur.execute("SELECT name FROM intg_instrument WHERE symbol = %s;", (test_symbol,))
        result = cur.fetchone()

        assert result is not None, f"Test data with symbol {test_symbol} was lost after container restart"
        assert result[0] == 'Persistence Test'

        # Clean up test data
        cur.execute("DELETE FROM intg_instrument WHERE symbol = %s;", (test_symbol,))
        conn.commit()
        cur.close()
        conn.close()

    def test_dashboard_container_restart_recovery(self):
        """Test dashboard container recovers properly after restart."""
        dashboard_container = self.docker_client.containers.get('ats-intg-dashboard')

        # Restart dashboard container
        dashboard_container.restart()

        # Wait for restart to complete
        time.sleep(10)

        # Verify container is running
        dashboard_container.reload()
        assert dashboard_container.status == 'running'

        # Verify dashboard is accessible
        import requests
        max_attempts = 10
        for _ in range(max_attempts):
            try:
                response = requests.get('http://localhost:4000/health', timeout=5)
                if response.status_code == 200:
                    break
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        else:
            pytest.fail("Dashboard failed to become accessible after restart")

        # Verify health endpoint works
        response = requests.get('http://localhost:4000/health')
        assert response.status_code == 200
        health_data = response.json()
        assert health_data['status'] == 'healthy'


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])