#!/usr/bin/env python3
"""
Integration tests for ATS-INTG Startup Manager
Tests the intelligent startup orchestration and decision tree logic
"""

import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Add src path for imports
sys.path.append('/workspace/src')
sys.path.append('/home/jianjun/ats-genai-data/scripts')

from intg_startup_manager import (
    wait_for_postgres,
    check_intg_database_status,
    check_dev_database_connectivity,
    get_dev_data_summary,
    run_full_migration,
    run_incremental_sync_setup,
    create_startup_status_report,
    main
)


class TestStartupManagerDecisionTree:
    """Test the core decision tree logic of the startup manager."""

    def test_wait_for_postgres_success(self):
        """Test PostgreSQL readiness check succeeds."""
        with patch('socket.socket') as mock_socket_class:
            mock_socket = MagicMock()
            mock_socket.connect_ex.return_value = 0
            mock_socket_class.return_value = mock_socket

            result = wait_for_postgres()

            assert result is True
            mock_socket.connect_ex.assert_called_with(('postgres-intg', 5432))
            mock_socket.close.assert_called_once()

    def test_wait_for_postgres_timeout(self):
        """Test PostgreSQL readiness check times out."""
        with patch('socket.socket') as mock_socket_class:
            mock_socket = MagicMock()
            mock_socket.connect_ex.return_value = 1  # Connection failed
            mock_socket_class.return_value = mock_socket

            with patch('time.sleep'):  # Speed up test
                result = wait_for_postgres()

            assert result is False

    def test_check_intg_database_status_empty(self):
        """Test INTG database status when empty."""
        with patch('subprocess.run') as mock_run:
            # Mock table count query (0 tables)
            mock_run.return_value = MagicMock(returncode=0, stdout='0\n')

            status = check_intg_database_status()

            assert status['table_count'] == 0
            assert status['has_schema'] is False
            assert status['has_data'] is False
            assert status['record_count'] == 0

    def test_check_intg_database_status_has_data(self):
        """Test INTG database status when has data."""
        with patch('subprocess.run') as mock_run:
            # Mock responses: table count (5 tables), then data counts
            mock_responses = [
                MagicMock(returncode=0, stdout='5\n'),  # Table count
                MagicMock(returncode=0, stdout='1000\n'),  # Instruments
                MagicMock(returncode=0, stdout='5000\n'),  # Prices
                MagicMock(returncode=0, stdout='500\n')    # Fundamentals
            ]
            mock_run.side_effect = mock_responses

            status = check_intg_database_status()

            assert status['table_count'] == 5
            assert status['has_schema'] is True
            assert status['has_data'] is True
            assert status['record_count'] == 6500

    def test_check_dev_database_connectivity_success(self):
        """Test DEV database connectivity check succeeds."""
        with patch('socket.socket') as mock_socket_class:
            mock_socket = MagicMock()
            mock_socket.connect_ex.return_value = 0
            mock_socket_class.return_value = mock_socket

            result = check_dev_database_connectivity()

            assert result is True
            mock_socket.connect_ex.assert_called_with(('172.17.0.1', 5433))

    def test_check_dev_database_connectivity_failed(self):
        """Test DEV database connectivity check fails."""
        with patch('socket.socket') as mock_socket_class:
            mock_socket = MagicMock()
            mock_socket.connect_ex.return_value = 1  # Connection failed
            mock_socket_class.return_value = mock_socket

            result = check_dev_database_connectivity()

            assert result is False

    def test_get_dev_data_summary_with_data(self):
        """Test DEV database data summary when data available."""
        with patch('intg_startup_manager.check_dev_database_connectivity', return_value=True):
            with patch('subprocess.run') as mock_run:
                # Mock data count queries
                mock_responses = [
                    MagicMock(returncode=0, stdout='500\n'),   # Instruments
                    MagicMock(returncode=0, stdout='10000\n'), # Prices
                    MagicMock(returncode=0, stdout='250\n')    # Fundamentals
                ]
                mock_run.side_effect = mock_responses

                summary = get_dev_data_summary()

                assert summary['instruments'] == 500
                assert summary['daily_price_polygon'] == 10000
                assert summary['fundamentals'] == 250
                assert summary['available'] is True

    def test_get_dev_data_summary_no_connectivity(self):
        """Test DEV database data summary when not accessible."""
        with patch('intg_startup_manager.check_dev_database_connectivity', return_value=False):
            summary = get_dev_data_summary()

            assert summary['instruments'] == 0
            assert summary['daily_price_polygon'] == 0
            assert summary['fundamentals'] == 0
            assert summary['available'] is False


class TestStartupManagerIntegration:
    """Test complete startup scenarios and integration."""

    def test_startup_scenario_auto_migration_disabled(self):
        """Test startup when auto-migration is disabled."""
        with patch.dict(os.environ, {'AUTO_MIGRATION_ENABLED': 'false'}):
            with patch('intg_startup_manager.wait_for_postgres', return_value=True):
                with patch('intg_startup_manager.check_intg_database_status'):
                    with patch('intg_startup_manager.create_startup_status_report', return_value="Test report"):
                        with patch('builtins.open', create=True):
                            with patch('time.sleep'):  # Speed up test
                                with patch('time.strftime', return_value='2025-08-28 12:00:00'):
                                    # Mock the infinite loop to exit after first iteration
                                    call_count = 0
                                    def mock_sleep(duration):
                                        nonlocal call_count
                                        call_count += 1
                                        if call_count >= 1:
                                            raise KeyboardInterrupt("Test exit")

                                    with patch('time.sleep', side_effect=mock_sleep):
                                        result = main()
                                    assert result is True

    def test_startup_scenario_has_existing_data(self):
        """Test startup when INTG already has data (incremental sync)."""
        mock_status = {
            'table_count': 5,
            'record_count': 1000,
            'has_schema': True,
            'has_data': True
        }

        with patch.dict(os.environ, {'AUTO_MIGRATION_ENABLED': 'true'}):
            with patch('intg_startup_manager.wait_for_postgres', return_value=True):
                with patch('intg_startup_manager.check_intg_database_status', return_value=mock_status):
                    with patch('intg_startup_manager.run_incremental_sync_setup', return_value=True):
                        with patch('intg_startup_manager.create_startup_status_report', return_value="Test report"):
                            with patch('builtins.open', create=True):
                                with patch('time.sleep'):  # Speed up test
                                    with patch('time.strftime', return_value='2025-08-28 12:00:00'):
                                        call_count = 0
                                        def mock_sleep(duration):
                                            nonlocal call_count
                                            call_count += 1
                                            if call_count >= 1:
                                                raise KeyboardInterrupt("Test exit")

                                        with patch('time.sleep', side_effect=mock_sleep):
                                            result = main()
                                        assert result is True

    def test_startup_scenario_empty_with_dev_data(self):
        """Test startup when INTG is empty but DEV has data (full migration)."""
        mock_intg_status = {
            'table_count': 0,
            'record_count': 0,
            'has_schema': False,
            'has_data': False
        }

        mock_dev_summary = {
            'instruments': 500,
            'daily_price_polygon': 10000,
            'fundamentals': 250,
            'available': True
        }

        with patch.dict(os.environ, {'AUTO_MIGRATION_ENABLED': 'true'}):
            with patch('intg_startup_manager.wait_for_postgres', return_value=True):
                with patch('intg_startup_manager.check_intg_database_status', return_value=mock_intg_status):
                    with patch('intg_startup_manager.check_dev_database_connectivity', return_value=True):
                        with patch('intg_startup_manager.get_dev_data_summary', return_value=mock_dev_summary):
                            with patch('intg_startup_manager.run_full_migration', return_value=True):
                                with patch('intg_startup_manager.run_incremental_sync_setup', return_value=True):
                                    with patch('intg_startup_manager.create_startup_status_report', return_value="Test report"):
                                        with patch('builtins.open', create=True):
                                            with patch('time.sleep'):
                                                with patch('time.strftime', return_value='2025-08-28 12:00:00'):
                                                    call_count = 0
                                                    def mock_sleep(duration):
                                                        nonlocal call_count
                                                        call_count += 1
                                                        if call_count >= 1:
                                                            raise KeyboardInterrupt("Test exit")

                                                    with patch('time.sleep', side_effect=mock_sleep):
                                                        result = main()
                                                    assert result is True

    def test_startup_scenario_empty_no_dev_data(self):
        """Test startup when both INTG and DEV are empty."""
        mock_intg_status = {
            'table_count': 0,
            'record_count': 0,
            'has_schema': False,
            'has_data': False
        }

        with patch.dict(os.environ, {'AUTO_MIGRATION_ENABLED': 'true'}):
            with patch('intg_startup_manager.wait_for_postgres', return_value=True):
                with patch('intg_startup_manager.check_intg_database_status', return_value=mock_intg_status):
                    with patch('intg_startup_manager.check_dev_database_connectivity', return_value=False):
                        with patch('intg_startup_manager.create_startup_status_report', return_value="Test report"):
                            with patch('builtins.open', create=True):
                                with patch('time.sleep'):
                                    with patch('time.strftime', return_value='2025-08-28 12:00:00'):
                                        call_count = 0
                                        def mock_sleep(duration):
                                            nonlocal call_count
                                            call_count += 1
                                            if call_count >= 1:
                                                raise KeyboardInterrupt("Test exit")

                                        with patch('time.sleep', side_effect=mock_sleep):
                                            result = main()
                                        assert result is True

    def test_startup_postgres_not_ready(self):
        """Test startup failure when PostgreSQL is not ready."""
        with patch('intg_startup_manager.wait_for_postgres', return_value=False):
            result = main()
            assert result is False


class TestStartupManagerMigration:
    """Test migration-specific functionality."""

    def test_run_full_migration_success(self):
        """Test successful full migration."""
        with patch('intg_startup_manager.run_command') as mock_run_command:
            # Mock successful validation and migration
            mock_run_command.side_effect = [
                {'success': True},  # Validation
                {'success': True}   # Migration
            ]

            result = run_full_migration()

            assert result is True
            assert mock_run_command.call_count == 2
            mock_run_command.assert_any_call(
                ['python3', 'scripts/intg_data_backfill.py', 'validate'],
                'Validating migration prerequisites'
            )
            mock_run_command.assert_any_call(
                ['python3', 'scripts/intg_data_backfill.py', 'backfill'],
                'Running full data backfill'
            )

    def test_run_full_migration_validation_failed(self):
        """Test full migration when validation fails."""
        with patch('intg_startup_manager.run_command') as mock_run_command:
            mock_run_command.return_value = {'success': False}

            result = run_full_migration()

            assert result is False
            mock_run_command.assert_called_once_with(
                ['python3', 'scripts/intg_data_backfill.py', 'validate'],
                'Validating migration prerequisites'
            )

    def test_run_incremental_sync_setup_success(self):
        """Test successful incremental sync setup."""
        with patch('intg_startup_manager.run_command') as mock_run_command:
            mock_run_command.side_effect = [
                {'success': True},  # Setup
                {'success': True}   # Initial sync
            ]

            result = run_incremental_sync_setup()

            assert result is True
            assert mock_run_command.call_count == 2
            mock_run_command.assert_any_call(
                ['python3', 'scripts/intg_incremental_sync.py', 'setup'],
                'Setting up incremental sync tables'
            )
            mock_run_command.assert_any_call(
                ['python3', 'scripts/intg_incremental_sync.py', 'sync', '--lookback-hours', '48'],
                'Running initial incremental sync (48h lookback)'
            )

    def test_run_incremental_sync_setup_graceful_failure(self):
        """Test incremental sync setup graceful failure handling."""
        with patch('intg_startup_manager.run_command') as mock_run_command:
            mock_run_command.side_effect = [
                {'success': True},   # Setup succeeds
                {'success': False}   # Initial sync fails
            ]

            result = run_incremental_sync_setup()

            # Should still return True (graceful failure)
            assert result is True


class TestStartupManagerStatusReport:
    """Test status report generation."""

    def test_create_startup_status_report(self):
        """Test startup status report creation."""
        with patch('intg_startup_manager.check_intg_database_status') as mock_intg_status:
            with patch('intg_startup_manager.check_dev_database_connectivity') as mock_dev_conn:
                with patch('intg_startup_manager.get_dev_data_summary') as mock_dev_summary:
                    # Mock return values
                    mock_intg_status.return_value = {
                        'table_count': 3,
                        'record_count': 1500,
                        'has_schema': True,
                        'has_data': True,
                        'last_migration': 'Never'
                    }
                    mock_dev_conn.return_value = True
                    mock_dev_summary.return_value = {
                        'instruments': 500,
                        'daily_price_polygon': 1000,
                        'fundamentals': 100
                    }

                    with patch.dict(os.environ, {'AUTO_MIGRATION_ENABLED': 'true'}):
                        report = create_startup_status_report()

                    assert '# ATS-INTG Startup Status Report' in report
                    assert '**Tables**: 3 intg_* tables' in report
                    assert '**Records**: 1500 total records' in report
                    assert '**Accessible**: True' in report
                    assert '**Instruments**: 500' in report


if __name__ == '__main__':
    pytest.main([__file__, '-v'])