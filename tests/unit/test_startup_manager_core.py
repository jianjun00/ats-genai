#!/usr/bin/env python3
"""
Unit tests for ATS-INTG Startup Manager core functions
Tests individual functions in isolation with comprehensive edge cases
"""

import pytest
import os
import sys
import subprocess
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime

# Add script path for imports
sys.path.append('/home/jianjun/ats-genai-data/scripts')

from intg_startup_manager import (
    log_info, log_success, log_error, log_warning,
    run_command, wait_for_postgres,
    check_intg_database_status, check_dev_database_connectivity,
    get_dev_data_summary, run_full_migration,
    run_incremental_sync_setup, create_startup_status_report
)


class TestLogging:
    """Test logging functionality."""

    def test_log_info_with_file_logging(self):
        """Test log_info writes to both stdout and file."""
        with patch('builtins.print') as mock_print:
            with patch('builtins.open', mock_open()) as mock_file:
                with patch('datetime.datetime') as mock_datetime:
                    mock_datetime.now.return_value.strftime.return_value = '2025-08-28 12:00:00'

                    log_info("Test message")

                    mock_print.assert_called_once_with("2025-08-28 12:00:00 - STARTUP - Test message")
                    mock_file.assert_called_once_with('/logs/startup.log', 'a')

    def test_log_info_file_write_failure(self):
        """Test log_info handles file write failures gracefully."""
        with patch('builtins.print') as mock_print:
            with patch('builtins.open', side_effect=IOError("Permission denied")):
                with patch('datetime.datetime') as mock_datetime:
                    mock_datetime.now.return_value.strftime.return_value = '2025-08-28 12:00:00'

                    # Should not raise exception
                    log_info("Test message")

                    mock_print.assert_called_once_with("2025-08-28 12:00:00 - STARTUP - Test message")

    def test_log_success(self):
        """Test log_success adds success emoji."""
        with patch('intg_startup_manager.log_info') as mock_log_info:
            log_success("Migration completed")
            mock_log_info.assert_called_once_with("✅ Migration completed")

    def test_log_error(self):
        """Test log_error adds error emoji."""
        with patch('intg_startup_manager.log_info') as mock_log_info:
            log_error("Database connection failed")
            mock_log_info.assert_called_once_with("❌ Database connection failed")

    def test_log_warning(self):
        """Test log_warning adds warning emoji."""
        with patch('intg_startup_manager.log_info') as mock_log_info:
            log_warning("DEV database not accessible")
            mock_log_info.assert_called_once_with("⚠️ DEV database not accessible")


class TestRunCommand:
    """Test command execution functionality."""

    def test_run_command_success(self):
        """Test successful command execution."""
        mock_result = MagicMock(returncode=0, stdout='Success output', stderr='')

        with patch('subprocess.run', return_value=mock_result) as mock_run:
            with patch('intg_startup_manager.log_info'):
                with patch('intg_startup_manager.log_success'):
                    result = run_command(
                        ['echo', 'test'],
                        'Test command',
                        capture_output=True
                    )

        assert result['success'] is True
        assert result['stdout'] == 'Success output'
        assert result['returncode'] == 0
        mock_run.assert_called_once_with(
            ['echo', 'test'],
            capture_output=True,
            text=True,
            cwd='/workspace'
        )

    def test_run_command_failure(self):
        """Test failed command execution."""
        mock_result = MagicMock(returncode=1, stdout='', stderr='Command failed')

        with patch('subprocess.run', return_value=mock_result) as mock_run:
            with patch('intg_startup_manager.log_info'):
                with patch('intg_startup_manager.log_error'):
                    result = run_command(
                        ['false'],
                        'Failing command',
                        capture_output=True
                    )

        assert result['success'] is False
        assert result['stderr'] == 'Command failed'
        assert result['returncode'] == 1

    def test_run_command_exception(self):
        """Test command execution with exception."""
        with patch('subprocess.run', side_effect=FileNotFoundError("Command not found")):
            with patch('intg_startup_manager.log_error'):
                result = run_command(['nonexistent'], 'Non-existent command')

        assert result['success'] is False
        assert result['returncode'] == -1
        assert 'Command not found' in result['stderr']

    def test_run_command_without_description(self):
        """Test command execution without description."""
        mock_result = MagicMock(returncode=0, stdout='', stderr='')

        with patch('subprocess.run', return_value=mock_result):
            result = run_command(['true'])

        assert result['success'] is True


class TestDatabaseConnectivity:
    """Test database connectivity functions."""

    def test_wait_for_postgres_immediate_success(self):
        """Test PostgreSQL check succeeds immediately."""
        with patch('socket.socket') as mock_socket_class:
            mock_socket = MagicMock()
            mock_socket.connect_ex.return_value = 0
            mock_socket_class.return_value = mock_socket

            with patch('intg_startup_manager.log_info'):
                with patch('intg_startup_manager.log_success'):
                    result = wait_for_postgres()

            assert result is True
            mock_socket.connect_ex.assert_called_once_with(('postgres-intg', 5432))
            mock_socket.close.assert_called_once()

    def test_wait_for_postgres_retry_then_success(self):
        """Test PostgreSQL check fails initially then succeeds."""
        with patch('socket.socket') as mock_socket_class:
            mock_socket = MagicMock()
            # Fail first two attempts, succeed on third
            mock_socket.connect_ex.side_effect = [1, 1, 0]
            mock_socket_class.return_value = mock_socket

            with patch('time.sleep'):  # Speed up test
                with patch('intg_startup_manager.log_info'):
                    with patch('intg_startup_manager.log_success'):
                        result = wait_for_postgres()

            assert result is True
            assert mock_socket.connect_ex.call_count == 3

    def test_wait_for_postgres_socket_exception(self):
        """Test PostgreSQL check with socket exception."""
        with patch('socket.socket', side_effect=Exception("Socket error")):
            with patch('time.sleep'):
                with patch('intg_startup_manager.log_info'):
                    with patch('intg_startup_manager.log_warning'):
                        result = wait_for_postgres()

            assert result is False

    def test_check_dev_database_connectivity_custom_host_port(self):
        """Test DEV connectivity with custom host and port."""
        with patch.dict(os.environ, {'DEV_DB_HOST': 'custom-host', 'DEV_DB_PORT': '5555'}):
            with patch('socket.socket') as mock_socket_class:
                mock_socket = MagicMock()
                mock_socket.connect_ex.return_value = 0
                mock_socket_class.return_value = mock_socket

                with patch('intg_startup_manager.log_info'):
                    with patch('intg_startup_manager.log_success'):
                        result = check_dev_database_connectivity()

                assert result is True
                mock_socket.connect_ex.assert_called_with(('custom-host', 5555))

    def test_check_dev_database_connectivity_socket_exception(self):
        """Test DEV connectivity with socket exception."""
        with patch('socket.socket', side_effect=Exception("Network error")):
            with patch('intg_startup_manager.log_info'):
                with patch('intg_startup_manager.log_warning'):
                    result = check_dev_database_connectivity()

            assert result is False


class TestDatabaseStatus:
    """Test database status checking functions."""

    def test_check_intg_database_status_query_failure(self):
        """Test INTG status check when query fails."""
        mock_result = MagicMock(returncode=1, stdout='', stderr='Connection failed')

        with patch('subprocess.run', return_value=mock_result):
            with patch('intg_startup_manager.log_info'):
                with patch('intg_startup_manager.log_error'):
                    status = check_intg_database_status()

        assert status['table_count'] == 0
        assert status['has_schema'] is False
        assert status['has_data'] is False
        assert status['record_count'] == 0

    def test_check_intg_database_status_with_schema_no_data(self):
        """Test INTG status when schema exists but no data."""
        with patch('subprocess.run') as mock_run:
            # Table count query returns 3, but data queries return 0
            mock_responses = [
                MagicMock(returncode=0, stdout='3\n'),  # 3 tables
                MagicMock(returncode=0, stdout='0\n'),  # 0 instruments
                MagicMock(returncode=0, stdout='0\n'),  # 0 prices
                MagicMock(returncode=0, stdout='0\n')   # 0 fundamentals
            ]
            mock_run.side_effect = mock_responses

            with patch('intg_startup_manager.log_info'):
                status = check_intg_database_status()

        assert status['table_count'] == 3
        assert status['has_schema'] is True
        assert status['has_data'] is False
        assert status['record_count'] == 0

    def test_check_intg_database_status_data_query_exception(self):
        """Test INTG status when data queries throw exceptions."""
        with patch('subprocess.run') as mock_run:
            def side_effect(*args, **kwargs):
                if 'information_schema.tables' in args[0][5]:
                    return MagicMock(returncode=0, stdout='3\n')
                else:
                    raise subprocess.TimeoutExpired(args[0], 30)

            mock_run.side_effect = side_effect

            with patch('intg_startup_manager.log_info'):
                status = check_intg_database_status()

        assert status['table_count'] == 3
        assert status['has_schema'] is True
        assert status['record_count'] == 0

    def test_get_dev_data_summary_no_connectivity(self):
        """Test DEV data summary when connectivity fails."""
        with patch('intg_startup_manager.check_dev_database_connectivity', return_value=False):
            with patch('intg_startup_manager.log_info'):
                summary = get_dev_data_summary()

        expected = {
            'instruments': 0,
            'daily_prices': 0,
            'fundamentals': 0,
            'available': False
        }
        assert summary == expected

    def test_get_dev_data_summary_partial_query_failures(self):
        """Test DEV data summary when some queries fail."""
        with patch('intg_startup_manager.check_dev_database_connectivity', return_value=True):
            with patch('subprocess.run') as mock_run:
                def side_effect(*args, **kwargs):
                    query = args[0][7]  # The SQL query
                    if 'dev_instruments' in query:
                        return MagicMock(returncode=0, stdout='100\n')
                    elif 'dev_daily_prices' in query:
                        return MagicMock(returncode=1, stdout='', stderr='Query failed')
                    else:  # fundamentals
                        return MagicMock(returncode=0, stdout='50\n')

                mock_run.side_effect = side_effect

                with patch('intg_startup_manager.log_info'):
                    with patch('intg_startup_manager.log_warning'):
                        summary = get_dev_data_summary()

        assert summary['instruments'] == 100
        assert summary['daily_prices'] == 0  # Failed query
        assert summary['fundamentals'] == 50
        assert summary['available'] is True  # Has some data

    def test_get_dev_data_summary_custom_env_vars(self):
        """Test DEV data summary with custom environment variables."""
        env_vars = {
            'DEV_DB_HOST': 'custom-dev-host',
            'DEV_DB_PORT': '6543',
            'DEV_DB_USER': 'custom_user',
            'DEV_DB_PASSWORD': 'custom_pass',
            'DEV_DB_NAME': 'custom_dev_db'
        }

        with patch.dict(os.environ, env_vars):
            with patch('intg_startup_manager.check_dev_database_connectivity', return_value=True):
                with patch('subprocess.run') as mock_run:
                    mock_run.return_value = MagicMock(returncode=0, stdout='10\n')

                    with patch('intg_startup_manager.log_info'):
                        summary = get_dev_data_summary()

                    # Verify correct connection parameters were used
                    call_args = mock_run.call_args[0][0]
                    assert 'custom-dev-host' in call_args
                    assert '6543' in call_args
                    assert 'custom_user' in call_args
                    assert 'custom_dev_db' in call_args

                    # Verify environment password was set
                    call_env = mock_run.call_args[1]['env']
                    assert call_env['PGPASSWORD'] == 'custom_pass'


class TestMigrationFunctions:
    """Test migration-related functions."""

    def test_run_full_migration_exception(self):
        """Test full migration with exception handling."""
        with patch('intg_startup_manager.run_command', side_effect=Exception("Unexpected error")):
            with patch('intg_startup_manager.log_info'):
                with patch('intg_startup_manager.log_error'):
                    with patch('intg_startup_manager.log_warning'):
                        result = run_full_migration()

        assert result is False

    def test_run_incremental_sync_setup_exception(self):
        """Test incremental sync setup with exception handling."""
        with patch('intg_startup_manager.run_command', side_effect=Exception("Sync error")):
            with patch('intg_startup_manager.log_info'):
                with patch('intg_startup_manager.log_error'):
                    with patch('intg_startup_manager.log_warning'):
                        result = run_incremental_sync_setup()

        # Should return True (graceful failure)
        assert result is True


class TestStatusReport:
    """Test status report generation."""

    def test_create_startup_status_report_with_mocked_functions(self):
        """Test status report generation with all functions mocked."""
        mock_intg_status = {
            'table_count': 5,
            'record_count': 2500,
            'has_schema': True,
            'has_data': True,
            'last_migration': '2025-08-28 10:00:00'
        }

        mock_dev_summary = {
            'instruments': 750,
            'daily_prices': 15000,
            'fundamentals': 300
        }

        with patch('intg_startup_manager.check_intg_database_status', return_value=mock_intg_status):
            with patch('intg_startup_manager.check_dev_database_connectivity', return_value=True):
                with patch('intg_startup_manager.get_dev_data_summary', return_value=mock_dev_summary):
                    with patch.dict(os.environ, {'AUTO_MIGRATION_ENABLED': 'true'}):
                        with patch('datetime.datetime') as mock_datetime:
                            mock_datetime.now.return_value.strftime.return_value = '2025-08-28 14:30:15'

                            report = create_startup_status_report()

        # Check report contains expected sections and values
        assert 'ATS-INTG Startup Status Report' in report
        assert '2025-08-28 14:30:15' in report
        assert '**Tables**: 5 intg_* tables' in report
        assert '**Records**: 2500 total records' in report
        assert '**Has Schema**: True' in report
        assert '**Has Data**: True' in report
        assert '**Accessible**: True' in report
        assert '**Instruments**: 750' in report
        assert '**Daily Prices**: 15000' in report
        assert '**Fundamentals**: 300' in report
        assert '**Auto-Migration**: true' in report

    def test_create_startup_status_report_dev_not_accessible(self):
        """Test status report when DEV database is not accessible."""
        mock_intg_status = {
            'table_count': 0,
            'record_count': 0,
            'has_schema': False,
            'has_data': False,
            'last_migration': 'Never'
        }

        with patch('intg_startup_manager.check_intg_database_status', return_value=mock_intg_status):
            with patch('intg_startup_manager.check_dev_database_connectivity', return_value=False):
                with patch.dict(os.environ, {'AUTO_MIGRATION_ENABLED': 'false'}):
                    with patch('datetime.datetime') as mock_datetime:
                        mock_datetime.now.return_value.strftime.return_value = '2025-08-28 14:30:15'

                        report = create_startup_status_report()

        assert '**Accessible**: False' in report
        assert '**Auto-Migration**: false' in report
        assert '**Has Data**: False' in report


if __name__ == '__main__':
    pytest.main([__file__, '-v'])