"""
Unit tests for Exchange DAO integration with existing BaseDAO infrastructure.

DISABLED: This test references DAO paths like 'core.dao.vendor_core.dao' and
'core.dao.instrument_xref_core.dao' which no longer exist. The DAO architecture
has been refactored.

Tests validate that the exchange DAOs properly extend BaseDAO and follow
established patterns without duplicating functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import date, datetime

from domains.instruments.repositories.exchange_dao import ExchangeDAO
from domains.instruments.repositories.instrument_xref_dao import InstrumentXrefDAO
from core.dao.infrastructure.vendor_dao import VendorDAO
from services.core.exchange_service import ExchangeService
from core.security.exceptions.custom_exceptions import DataValidationError, DatabaseError
from core.security.validation.data_validators import ValidationResult


# Fixed import issues - tests can now run
class TestExchangeDAOIntegration:
    """Test exchange DAO integration with BaseDAO infrastructure."""

    def test_exchange_dao_extends_base_dao(self):
        """Test that ExchangeDAO properly extends BaseDAO."""
        dao = ExchangeDAO()

        # Should have BaseDAO methods
        assert hasattr(dao, 'create')
        assert hasattr(dao, 'read')
        assert hasattr(dao, 'update')
        assert hasattr(dao, 'delete')
        assert hasattr(dao, 'list_all')
        assert hasattr(dao, 'count')
        assert hasattr(dao, 'bulk_insert')
        assert hasattr(dao, 'execute_query')
        assert hasattr(dao, 'to_dataframe')

        # Should have proper table name
        assert dao.base_table_name == "exchanges"
        assert "exchanges" in dao.table_name  # Will be prefixed with environment

        # Should have schema definition
        schema = dao.get_schema()
        assert isinstance(schema, dict)
        assert 'table_name' in schema
        assert 'columns' in schema

    def test_exchange_dao_validation(self):
        """Test ExchangeDAO data validation."""
        dao = ExchangeDAO()

        # Valid data
        valid_data = {
            'exchange_code': 'NYSE',
            'exchange_name': 'New York Stock Exchange',
            'country': 'USA',
            'timezone': 'EST'
        }

        result = dao.validate_data(valid_data)
        assert result.is_valid == True
        assert len(result.errors) == 0

        # Invalid data - missing required fields
        invalid_data = {
            'country': 'USA'  # Missing exchange_code and exchange_name
        }

        result = dao.validate_data(invalid_data)
        assert result.is_valid == False
        assert len(result.errors) > 0
        assert any('exchange_code is required' in error for error in result.errors)
        assert any('exchange_name is required' in error for error in result.errors)

        # Invalid data - field too long
        long_name_data = {
            'exchange_code': 'TEST',
            'exchange_name': 'A' * 101  # Too long
        }

        result = dao.validate_data(long_name_data)
        assert result.is_valid == False
        assert any('must be 100 characters or less' in error for error in result.errors)

    def test_instrument_xref_dao_extends_base_dao(self):
        """Test that InstrumentXrefDAO properly extends BaseDAO."""
        dao = InstrumentXrefDAO()

        # Should have BaseDAO methods
        assert hasattr(dao, 'create')
        assert hasattr(dao, 'bulk_insert')
        assert hasattr(dao, 'execute_query')

        # Should have proper table name
        assert dao.base_table_name == "instrument_xrefs"

        # Should have business methods
        assert hasattr(dao, 'get_current_exchange')
        assert hasattr(dao, 'get_exchange_history')
        assert hasattr(dao, 'find_exchange_migrations')
        assert hasattr(dao, 'create_exchange_entry')
        assert hasattr(dao, 'close_exchange_entry')

    def test_instrument_xref_dao_validation(self):
        """Test InstrumentXrefDAO data validation."""
        dao = InstrumentXrefDAO()

        # Valid data
        valid_data = {
            'instrument_id': 123,
            'vendor_id': 456,
            'external_symbol': 'NYSE',
            'start_date': date.today()
        }

        result = dao.validate_data(valid_data)
        assert result.is_valid == True

        # Invalid data - end_date before start_date
        invalid_data = {
            'instrument_id': 123,
            'vendor_id': 456,
            'external_symbol': 'NYSE',
            'start_date': date(2023, 12, 1),
            'end_date': date(2023, 11, 1)  # Before start_date
        }

        result = dao.validate_data(invalid_data)
        assert result.is_valid == False
        assert any('end_date must be after start_date' in error for error in result.errors)

    def test_vendor_dao_extends_base_dao(self):
        """Test that VendorDAO properly extends BaseDAO."""
        dao = VendorDAO()

        # Should have BaseDAO methods and proper table name
        assert dao.base_table_name == "vendors"
        assert hasattr(dao, 'get_by_name')
        assert hasattr(dao, 'list_active_vendors')
        assert hasattr(dao, 'get_exchange_vendor_id')

    def test_exchange_dao_crud_operations_use_base_pattern(self):
        """Test that ExchangeDAO CRUD operations follow BaseDAO patterns."""
        dao = ExchangeDAO()

        # Test that the DAO has the required implementation methods
        # These are the abstract methods from BaseDAO that must be implemented
        assert hasattr(dao, '_create_impl')
        assert hasattr(dao, '_read_impl')
        assert hasattr(dao, '_update_impl')
        assert hasattr(dao, '_delete_impl')
        assert hasattr(dao, '_list_all_impl')
        assert hasattr(dao, '_count_impl')
        assert hasattr(dao, '_bulk_insert_impl')

        # Test that these methods are callable (not abstract)
        import inspect
        assert inspect.ismethod(dao._create_impl)
        assert inspect.ismethod(dao._read_impl)
        assert inspect.ismethod(dao._update_impl)
        assert inspect.ismethod(dao._delete_impl)
        assert inspect.ismethod(dao._list_all_impl)
        assert inspect.ismethod(dao._count_impl)
        assert inspect.ismethod(dao._bulk_insert_impl)

        # Test validation is implemented
        test_data = {
            'exchange_code': 'TEST',
            'exchange_name': 'Test Exchange'
        }
        validation_result = dao.validate_data(test_data)
        assert hasattr(validation_result, 'is_valid')
        assert hasattr(validation_result, 'errors')

    def test_exchange_service_uses_daos_not_direct_sql(self):
        """Test that ExchangeService uses DAOs instead of direct SQL."""
        service = ExchangeService()

        # Should have DAO instances, not direct database connections
        assert isinstance(service.exchange_dao, ExchangeDAO)
        assert isinstance(service.instrument_xref_dao, InstrumentXrefDAO)
        assert isinstance(service.vendor_dao, VendorDAO)

        # Should not have direct database connection attributes
        assert not hasattr(service, 'connection')
        assert not hasattr(service, 'cursor')
        assert not hasattr(service, 'session')

    @patch('services.core.exchange_service.ExchangeService.get_exchange_vendor_id')
    @patch('services.core.exchange_service.ExchangeService._get_instrument_by_symbol')
    @patch('domains.instruments.repositories.instrument_xref_dao.InstrumentXrefDAO.get_current_exchange')
    def test_exchange_service_business_logic_separation(self, mock_get_current,
                                                       mock_get_instrument,
                                                       mock_get_vendor_id):
        """Test that ExchangeService properly separates business logic from data access."""
        # Setup mocks
        mock_get_vendor_id.return_value = 1
        mock_get_instrument.return_value = {'id': 123, 'symbol': 'AAPL'}
        mock_get_current.return_value = {
            'exchange_code': 'NASDAQ',
            'exchange_name': 'NASDAQ',
            'start_date': date(2020, 1, 1)
        }

        service = ExchangeService()

        # Test business method
        result = service.get_current_exchange_for_instrument('AAPL')

        # Should have called DAO methods, not executed SQL directly
        mock_get_vendor_id.assert_called_once()
        mock_get_instrument.assert_called_once_with('AAPL')
        mock_get_current.assert_called_once_with(123, 1)

        assert result['exchange_code'] == 'NASDAQ'

    def test_dao_schema_definitions_are_complete(self):
        """Test that DAO schema definitions are complete and consistent."""
        exchange_dao = ExchangeDAO()
        xref_dao = InstrumentXrefDAO()
        vendor_dao = VendorDAO()

        # All DAOs should have complete schema definitions
        exchange_schema = exchange_dao.get_schema()
        assert 'columns' in exchange_schema
        assert 'id' in exchange_schema['columns']
        assert 'exchange_code' in exchange_schema['columns']
        assert 'created_at' in exchange_schema['columns']

        xref_schema = xref_dao.get_schema()
        assert 'columns' in xref_schema
        assert 'instrument_id' in xref_schema['columns']
        assert 'vendor_id' in xref_schema['columns']
        assert 'start_date' in xref_schema['columns']
        assert 'end_date' in xref_schema['columns']

        vendor_schema = vendor_dao.get_schema()
        assert 'columns' in vendor_schema
        assert 'vendor_id' in vendor_schema['columns']
        assert 'vendor_name' in vendor_schema['columns']

    def test_dao_error_handling_uses_base_patterns(self):
        """Test that DAOs use BaseDAO error handling patterns."""
        dao = ExchangeDAO()

        # Should have logger from BaseDAO
        assert hasattr(dao, 'logger')

        # Should have settings from BaseDAO
        assert hasattr(dao, 'settings')

        # Should have validation method that returns ValidationResult
        result = dao.validate_data({'exchange_code': 'TEST'})
        assert isinstance(result, ValidationResult)
        assert hasattr(result, 'is_valid')
        assert hasattr(result, 'errors')

    def test_dao_table_naming_follows_conventions(self):
        """Test that DAO table naming follows existing conventions."""
        exchange_dao = ExchangeDAO()
        xref_dao = InstrumentXrefDAO()
        vendor_dao = VendorDAO()

        # Base table names should be simple
        assert exchange_dao.base_table_name == "exchanges"
        assert xref_dao.base_table_name == "instrument_xrefs"
        assert vendor_dao.base_table_name == "vendors"

        # Full table names should be environment-prefixed (via settings)
        # We can't test the exact prefix without settings, but structure should be consistent
        assert hasattr(exchange_dao, 'table_name')
        assert hasattr(xref_dao, 'table_name')
        assert hasattr(vendor_dao, 'table_name')


class TestExchangeSystemIntegration:
    """Integration tests for the complete exchange vendor system."""

    @patch('services.core.exchange_service.ExchangeService._get_instrument_by_symbol')
    @patch('services.core.exchange_service.ExchangeService.get_exchange_vendor_id')
    def test_exchange_service_validation_system_health(self, mock_vendor_id, mock_get_instrument):
        """Test that ExchangeService can validate system health."""
        mock_vendor_id.return_value = 1
        mock_get_instrument.return_value = {'id': 123}

        service = ExchangeService()

        # Should have system validation method
        assert hasattr(service, 'validate_exchange_system')

        # Validation should check all components
        with patch.object(service.exchange_dao, 'list_active_exchanges') as mock_exchanges:
            with patch.object(service.instrument_xref_dao, 'count') as mock_count:
                mock_exchanges.return_value = [{'exchange_code': 'NYSE'}, {'exchange_code': 'NASDAQ'}]
                mock_count.return_value = 100

                validation = service.validate_exchange_system()

                assert 'system_status' in validation
                assert 'checks' in validation
                assert 'exchange_vendor' in validation['checks']
                assert 'exchanges' in validation['checks']
                assert 'instrument_xrefs' in validation['checks']

    def test_dao_implementations_are_concrete_not_abstract(self):
        """Test that DAO implementations can be instantiated and used."""
        # Should be able to create instances without errors
        exchange_dao = ExchangeDAO()
        xref_dao = InstrumentXrefDAO()
        # Note: BaseVendorDAO is abstract, so we test the concrete implementations via service

        # Should have all required abstract methods implemented
        # (We can't easily test the implementations without database,
        #  but we can verify the methods exist)

        assert hasattr(exchange_dao, '_create_impl')
        assert hasattr(exchange_dao, '_read_impl')
        assert hasattr(exchange_dao, '_update_impl')
        assert hasattr(exchange_dao, '_delete_impl')
        assert hasattr(exchange_dao, '_list_all_impl')
        assert hasattr(exchange_dao, '_count_impl')
        assert hasattr(exchange_dao, '_bulk_insert_impl')

        # Same for other DAOs
        assert hasattr(xref_dao, '_create_impl')