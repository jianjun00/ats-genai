"""
Test analytics events functionality after instrument service migration.

This test verifies that the analytics events system works correctly 
with the new InstrumentService architecture instead of direct DAO usage.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import date

# Import the migrated analytics events module
from domains.analytics.services.events.db import create_analytics_event


class TestAnalyticsEventsServiceMigration:
    """Test analytics events functionality with service architecture"""
    
    @pytest.fixture
    def mock_environment(self):
        """Mock Environment for testing"""
        env = Mock()
        env.get_table_name = Mock(return_value='test_analytics_events')
        return env
    
    @pytest.fixture 
    def mock_instrument_service(self):
        """Mock InstrumentService for testing"""
        service = Mock()
        service.get_instrument_by_symbol = AsyncMock()
        return service
    
    @pytest.fixture
    def mock_instrument_dto(self):
        """Mock InstrumentDTO for testing"""
        from domains.instruments.services.impl.instrument_service_cached import InstrumentDTO
        return InstrumentDTO(
            id=123,
            symbol="AAPL", 
            name="Apple Inc.",
            exchange="NASDAQ",
            instrument_type="stock",
            currency="USD",
            list_date=date(2020, 1, 1)
        )
    
    @pytest.mark.asyncio
    async def test_create_analytics_event_with_valid_symbol(self, mock_environment, mock_instrument_service, mock_instrument_dto):
        """Test that analytics event creation works with valid symbol lookup via service"""
        # Setup mocks
        mock_instrument_service.get_instrument_by_symbol.return_value = mock_instrument_dto
        
        # Mock database connection and execution
        mock_pool = Mock()
        mock_pool.close = AsyncMock()
        mock_conn = Mock()
        mock_conn.execute = AsyncMock()
        mock_pool.acquire = Mock()
        mock_pool.acquire().__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire().__aexit__ = AsyncMock()
        
        with patch('domains.analytics.services.events.db.get_instrument_service', return_value=mock_instrument_service):
            with patch('shared.utils.database.Database.create_connection_pool', return_value=mock_pool):
                # Execute
                await create_analytics_event(
                    env=mock_environment,
                    event_type='test_event',
                    symbol='AAPL',
                    data={'test': 'data'}
                )
        
        # Verify service was called correctly
        mock_instrument_service.get_instrument_by_symbol.assert_called_once_with('AAPL')
        
        # Verify database was called with instrument_id from service
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert 'INSERT INTO test_analytics_events' in call_args[0][0]
        # Check that instrument_id from DTO is used
        assert call_args[0][1] == 123  # instrument_id from mock_instrument_dto
    
    @pytest.mark.asyncio 
    async def test_create_analytics_event_with_invalid_symbol(self, mock_environment, mock_instrument_service):
        """Test that analytics event handles invalid symbol gracefully"""
        # Setup mocks - symbol not found
        mock_instrument_service.get_instrument_by_symbol.return_value = None
        
        # Mock database connection
        mock_pool = Mock()
        mock_pool.close = AsyncMock()
        mock_conn = Mock()
        mock_conn.execute = AsyncMock()
        mock_pool.acquire = Mock()
        mock_pool.acquire().__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire().__aexit__ = AsyncMock()
        
        with patch('domains.analytics.services.events.db.get_instrument_service', return_value=mock_instrument_service):
            with patch('shared.utils.database.Database.create_connection_pool', return_value=mock_pool):
                # Execute
                await create_analytics_event(
                    env=mock_environment, 
                    event_type='test_event',
                    symbol='INVALID',
                    data={'test': 'data'}
                )
        
        # Verify service was called
        mock_instrument_service.get_instrument_by_symbol.assert_called_once_with('INVALID')
        
        # Verify database was called with None instrument_id
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert call_args[0][1] is None  # instrument_id should be None for invalid symbol
    
    @pytest.mark.asyncio
    async def test_create_analytics_event_service_error(self, mock_environment, mock_instrument_service):
        """Test that analytics event handles service errors gracefully"""
        # Setup mocks - service raises exception
        mock_instrument_service.get_instrument_by_symbol.side_effect = Exception("Service error")
        
        # Mock database connection
        mock_pool = Mock()
        mock_pool.close = AsyncMock()
        mock_conn = Mock()
        mock_conn.execute = AsyncMock()
        mock_pool.acquire = Mock()
        mock_pool.acquire().__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire().__aexit__ = AsyncMock()
        
        with patch('domains.analytics.services.events.db.get_instrument_service', return_value=mock_instrument_service):
            with patch('shared.utils.database.Database.create_connection_pool', return_value=mock_pool):
                # Execute - should not raise exception
                await create_analytics_event(
                    env=mock_environment,
                    event_type='test_event', 
                    symbol='AAPL',
                    data={'test': 'data'}
                )
        
        # Verify service was called
        mock_instrument_service.get_instrument_by_symbol.assert_called_once_with('AAPL')
        
        # Verify database was still called with None instrument_id (graceful fallback)
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert call_args[0][1] is None  # instrument_id should be None when service fails