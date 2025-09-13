"""
Integration tests for Instruments API

Tests the complete HTTP API layer with service integration.
These tests verify:
1. HTTP request/response handling
2. Service layer integration
3. Error handling and status codes
4. DTO conversion between HTTP and service layers
5. End-to-end functionality
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi.testclient import TestClient
from fastapi import FastAPI
from datetime import date
import json

from services.web_services.api.instruments_api import instruments_router
from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentDTO,
    InstrumentXrefDTO,
    UnifiedInstrumentDTO,
    InstrumentOperationResult
)


class TestInstrumentsAPIIntegration:
    """Integration test suite for Instruments API"""

    @pytest.fixture
    def mock_service(self):
        """Mock InstrumentServiceInterface for testing"""
        service = Mock()

        # Setup async methods
        service.create_instrument = AsyncMock()
        service.get_instrument_by_id = AsyncMock()
        service.get_instrument_by_symbol = AsyncMock()
        service.list_instruments = AsyncMock()
        service.create_cross_reference = AsyncMock()
        service.get_cross_references = AsyncMock()
        service.get_unified_instrument = AsyncMock()
        service.resolve_instrument_by_vendor_symbol = AsyncMock()
        service.get_all_symbols = AsyncMock()
        service.get_instrument_count = AsyncMock()
        service.validate_symbol = AsyncMock()
        service.create_instruments_batch = AsyncMock()

        return service

    @pytest.fixture
    def client(self, mock_service):
        """Create FastAPI test client with mocked service"""
        app = FastAPI()
        app.include_router(instruments_router)

        # Patch the dependency injection
        with patch('services.web_services.api.instruments_api.get_instrument_service',
                  return_value=mock_service):
            yield TestClient(app)

    @pytest.fixture
    def sample_instrument_data(self):
        """Sample instrument data for testing"""
        return {
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "exchange": "NASDAQ",
            "instrument_type": "stock",
            "currency": "USD",
            "list_date": "2020-01-01"
        }

    @pytest.fixture
    def sample_xref_data(self):
        """Sample cross-reference data for testing"""
        return {
            "instrument_id": 1,
            "vendor_name": "ticker",
            "vendor_symbol": "AAPL",
            "xref_type": "equity",
            "start_date": "2020-01-01"
        }

    # Test create_instrument endpoint

    def test_create_instrument_success(self, client, mock_service, sample_instrument_data):
        """Test successful instrument creation via API"""
        # Setup mock
        mock_service.create_instrument.return_value = InstrumentOperationResult(
            success=True,
            instrument_id=123,
            created_count=1
        )

        # Execute
        response = client.post("/api/v1/instruments/", json=sample_instrument_data)

        # Verify HTTP response
        assert response.status_code == 201
        data = response.json()
        assert data["success"] is True
        assert data["instrument_id"] == 123
        assert data["created_count"] == 1
        assert data["message"] == "Operation completed successfully"

        # Verify service called correctly
        mock_service.create_instrument.assert_called_once()
        call_args = mock_service.create_instrument.call_args[0][0]  # Get the DTO argument
        assert isinstance(call_args, InstrumentDTO)
        assert call_args.symbol == "AAPL"
        assert call_args.name == "Apple Inc."
        assert call_args.list_date == date(2020, 1, 1)

    def test_create_instrument_validation_error(self, client, mock_service):
        """Test instrument creation with validation error"""
        # Setup mock
        mock_service.create_instrument.return_value = InstrumentOperationResult(
            success=False,
            error_message="Symbol is required"
        )

        # Execute with invalid data
        response = client.post("/api/v1/instruments/", json={"name": "Invalid"})

        # Verify HTTP response
        assert response.status_code == 400
        assert "Symbol is required" in response.json()["detail"]

    def test_create_instrument_server_error(self, client, mock_service, sample_instrument_data):
        """Test instrument creation with server error"""
        # Setup mock to raise exception
        mock_service.create_instrument.side_effect = Exception("Database connection failed")

        # Execute
        response = client.post("/api/v1/instruments/", json=sample_instrument_data)

        # Verify HTTP response
        assert response.status_code == 500
        assert "Internal server error" in response.json()["detail"]

    # Test get_instrument endpoint

    def test_get_instrument_success(self, client, mock_service):
        """Test successful instrument retrieval"""
        # Setup mock
        mock_service.get_instrument_by_id.return_value = InstrumentDTO(
            id=123,
            symbol="AAPL",
            name="Apple Inc.",
            exchange="NASDAQ",
            instrument_type="stock",
            currency="USD",
            list_date=date(2020, 1, 1)
        )

        # Execute
        response = client.get("/api/v1/instruments/123")

        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 123
        assert data["symbol"] == "AAPL"
        assert data["name"] == "Apple Inc."
        assert data["list_date"] == "2020-01-01"

        # Verify service called correctly
        mock_service.get_instrument_by_id.assert_called_once_with(123)

    def test_get_instrument_not_found(self, client, mock_service):
        """Test instrument retrieval with not found"""
        # Setup mock
        mock_service.get_instrument_by_id.return_value = None

        # Execute
        response = client.get("/api/v1/instruments/999")

        # Verify HTTP response
        assert response.status_code == 404
        assert "Instrument not found" in response.json()["detail"]

    # Test get_instrument_by_symbol endpoint

    def test_get_instrument_by_symbol_success(self, client, mock_service):
        """Test successful instrument retrieval by symbol"""
        # Setup mock
        mock_service.get_instrument_by_symbol.return_value = InstrumentDTO(
            id=123,
            symbol="AAPL",
            name="Apple Inc.",
            exchange="NASDAQ",
            instrument_type="stock",
            currency="USD"
        )

        # Execute
        response = client.get("/api/v1/instruments/by-symbol/AAPL?vendor_name=ticker")

        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data["symbol"] == "AAPL"
        assert data["name"] == "Apple Inc."

        # Verify service called correctly
        mock_service.get_instrument_by_symbol.assert_called_once_with("AAPL", "ticker")

    def test_get_instrument_by_symbol_not_found(self, client, mock_service):
        """Test instrument by symbol with not found"""
        # Setup mock
        mock_service.get_instrument_by_symbol.return_value = None

        # Execute
        response = client.get("/api/v1/instruments/by-symbol/INVALID")

        # Verify HTTP response
        assert response.status_code == 404
        assert "Instrument not found for symbol INVALID" in response.json()["detail"]

    # Test list_instruments endpoint

    def test_list_instruments_success(self, client, mock_service):
        """Test successful instrument listing"""
        # Setup mock
        mock_service.list_instruments.return_value = [
            InstrumentDTO(id=1, symbol="AAPL", name="Apple Inc."),
            InstrumentDTO(id=2, symbol="GOOGL", name="Alphabet Inc.")
        ]

        # Execute with query parameters
        response = client.get(
            "/api/v1/instruments/?symbols=AAPL&symbols=GOOGL&limit=10&offset=0"
        )

        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 2
        assert len(data["instruments"]) == 2
        assert data["instruments"][0]["symbol"] == "AAPL"
        assert data["instruments"][1]["symbol"] == "GOOGL"

        # Verify service called with correct criteria
        mock_service.list_instruments.assert_called_once()
        criteria = mock_service.list_instruments.call_args[0][0]
        assert criteria.symbols == ["AAPL", "GOOGL"]
        assert criteria.limit == 10
        assert criteria.offset == 0

    # Test create_cross_reference endpoint

    def test_create_cross_reference_success(self, client, mock_service, sample_xref_data):
        """Test successful cross-reference creation"""
        # Setup mock
        mock_service.create_cross_reference.return_value = InstrumentOperationResult(
            success=True,
            instrument_id=789,
            created_count=1
        )

        # Execute
        response = client.post("/api/v1/instruments/cross-references", json=sample_xref_data)

        # Verify HTTP response
        assert response.status_code == 201
        data = response.json()
        assert data["success"] is True
        assert data["instrument_id"] == 789

        # Verify service called correctly
        mock_service.create_cross_reference.assert_called_once()
        call_args = mock_service.create_cross_reference.call_args[0][0]
        assert isinstance(call_args, InstrumentXrefDTO)
        assert call_args.instrument_id == 1
        assert call_args.vendor_symbol == "AAPL"

    # Test get_cross_references endpoint

    def test_get_cross_references_success(self, client, mock_service):
        """Test successful cross-reference retrieval"""
        # Setup mock
        mock_service.get_cross_references.return_value = [
            InstrumentXrefDTO(
                id=1,
                instrument_id=123,
                vendor_name="ticker",
                vendor_symbol="AAPL",
                start_date=date(2020, 1, 1)
            )
        ]

        # Execute
        response = client.get("/api/v1/instruments/123/cross-references")

        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["instrument_id"] == 123
        assert data[0]["vendor_symbol"] == "AAPL"
        assert data[0]["start_date"] == "2020-01-01"

    # Test get_unified_instrument endpoint

    def test_get_unified_instrument_success(self, client, mock_service):
        """Test successful unified instrument retrieval"""
        # Setup mock
        instrument = InstrumentDTO(id=123, symbol="AAPL", name="Apple Inc.")
        xrefs = [InstrumentXrefDTO(id=1, instrument_id=123, vendor_name="ticker", vendor_symbol="AAPL")]

        mock_service.get_unified_instrument.return_value = UnifiedInstrumentDTO(
            instrument=instrument,
            cross_references=xrefs,
            vendor_data={}
        )

        # Execute
        response = client.get("/api/v1/instruments/unified/AAPL?identifier_type=symbol")

        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data["instrument"]["symbol"] == "AAPL"
        assert len(data["cross_references"]) == 1
        assert data["cross_references"][0]["vendor_symbol"] == "AAPL"

        # Verify service called correctly
        mock_service.get_unified_instrument.assert_called_once_with("AAPL", "symbol")

    # Test resolve_vendor_symbol endpoint

    def test_resolve_vendor_symbol_success(self, client, mock_service):
        """Test successful vendor symbol resolution"""
        # Setup mock
        mock_service.resolve_instrument_by_vendor_symbol.return_value = InstrumentDTO(
            id=123,
            symbol="AAPL",
            name="Apple Inc."
        )

        # Execute
        response = client.post(
            "/api/v1/instruments/resolve-vendor-symbol"
            "?vendor_symbol=AAPL&vendor_name=polygon&as_of_date=2020-01-01"
        )

        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data["symbol"] == "AAPL"

        # Verify service called correctly
        mock_service.resolve_instrument_by_vendor_symbol.assert_called_once_with(
            "AAPL", "polygon", date(2020, 1, 1)
        )

    # Test utility endpoints

    def test_get_all_symbols_success(self, client, mock_service):
        """Test getting all symbols"""
        # Setup mock
        mock_service.get_all_symbols.return_value = ["AAPL", "GOOGL", "TSLA"]

        # Execute
        response = client.get("/api/v1/instruments/symbols/all?vendor_name=ticker")

        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data == ["AAPL", "GOOGL", "TSLA"]

        # Verify service called correctly
        mock_service.get_all_symbols.assert_called_once_with("ticker")

    def test_get_instrument_count_success(self, client, mock_service):
        """Test getting instrument count"""
        # Setup mock
        mock_service.get_instrument_count.return_value = 1500

        # Execute
        response = client.get("/api/v1/instruments/count")

        # Verify HTTP response
        assert response.status_code == 200
        assert response.json() == 1500

    def test_validate_symbol_success(self, client, mock_service):
        """Test symbol validation"""
        # Setup mock
        mock_service.validate_symbol.return_value = True

        # Execute
        response = client.post(
            "/api/v1/instruments/validate-symbol?symbol=AAPL&vendor_name=ticker"
        )

        # Verify HTTP response
        assert response.status_code == 200
        assert response.json() is True

        # Verify service called correctly
        mock_service.validate_symbol.assert_called_once_with("AAPL", "ticker")

    # Test batch operations

    def test_create_instruments_batch_success(self, client, mock_service, sample_instrument_data):
        """Test batch instrument creation"""
        # Setup mock
        mock_service.create_instruments_batch.return_value = InstrumentOperationResult(
            success=True,
            created_count=2
        )

        # Execute
        batch_data = [sample_instrument_data, {**sample_instrument_data, "symbol": "GOOGL"}]
        response = client.post("/api/v1/instruments/batch", json=batch_data)

        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["created_count"] == 2

    # Test health check endpoint

    def test_health_check(self, client, mock_service):
        """Test health check endpoint"""
        # Execute
        response = client.get("/api/v1/instruments/health")

        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "instruments-api"

    # Test error scenarios

    def test_service_unavailable_error(self, client):
        """Test API behavior when service is unavailable"""
        # Create client without proper dependency injection
        app = FastAPI()
        app.include_router(instruments_router)

        with TestClient(app) as unavailable_client:
            response = unavailable_client.get("/api/v1/instruments/count")

            # Should return 500 or appropriate error
            assert response.status_code in [500, 501]

    # Test request validation

    def test_invalid_request_data(self, client, mock_service):
        """Test API with invalid request data"""
        # Execute with missing required field
        response = client.post("/api/v1/instruments/", json={})

        # Verify HTTP response - should be validation error
        assert response.status_code == 422  # FastAPI validation error

    def test_path_parameter_validation(self, client, mock_service):
        """Test API with invalid path parameters"""
        # Execute with non-integer instrument ID
        response = client.get("/api/v1/instruments/not-an-integer")

        # Verify HTTP response - should be validation error
        assert response.status_code == 422  # FastAPI validation error

    # Test query parameter handling

    def test_query_parameter_handling(self, client, mock_service):
        """Test proper query parameter parsing"""
        # Setup mock
        mock_service.list_instruments.return_value = []

        # Execute with multiple symbols and other parameters
        response = client.get(
            "/api/v1/instruments/"
            "?symbols=AAPL&symbols=GOOGL&symbols=TSLA"
            "&exchanges=NASDAQ&exchanges=NYSE"
            "&limit=50&offset=25"
        )

        # Verify service called with correct parameters
        assert response.status_code == 200
        mock_service.list_instruments.assert_called_once()

        criteria = mock_service.list_instruments.call_args[0][0]
        assert criteria.symbols == ["AAPL", "GOOGL", "TSLA"]
        assert criteria.exchanges == ["NASDAQ", "NYSE"]
        assert criteria.limit == 50
        assert criteria.offset == 25

    # Test date parsing

    def test_date_parsing_in_requests(self, client, mock_service, sample_instrument_data):
        """Test proper date parsing in API requests"""
        # Setup mock
        mock_service.create_instrument.return_value = InstrumentOperationResult(success=True)

        # Execute with date fields
        data = {**sample_instrument_data, "list_date": "2020-12-31", "delist_date": "2025-01-01"}
        response = client.post("/api/v1/instruments/", json=data)

        # Verify dates parsed correctly
        assert response.status_code == 201

        call_args = mock_service.create_instrument.call_args[0][0]
        assert call_args.list_date == date(2020, 12, 31)
        assert call_args.delist_date == date(2025, 1, 1)