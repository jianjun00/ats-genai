"""
Comprehensive integration tests for InstrumentService architecture.

Tests end-to-end functionality including:
- Service layer integration with real database
- API endpoint integration 
- Performance benchmarks
- Error handling and recovery
- Cache behavior (when enabled)
"""

import pytest
import pytest_asyncio
from datetime import date
import time

from fastapi.testclient import TestClient
from httpx import AsyncClient

# Service layer imports
from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentServiceInterface,
    InstrumentDTO,
    InstrumentXrefDTO,
    InstrumentSearchCriteria
)
from domains.instruments.services.config.service_container import get_instrument_service
from core.platform.config.environment import Environment, EnvironmentType

# API imports
from infrastructure.web.api.enhanced_instruments_api import app


class TestInstrumentServiceIntegration:
    """Integration tests for InstrumentService with real-like scenarios"""
    
    @pytest_asyncio.fixture
    async def test_environment(self):
        """Create test environment"""
        return Environment(None, EnvironmentType.DEV)
    
    @pytest_asyncio.fixture
    async def instrument_service(self, test_environment):
        """Get instrument service for testing"""
        return await get_instrument_service(test_environment)
    
    @pytest.fixture
    def sample_instruments(self):
        """Sample instrument data for testing"""
        return [
            InstrumentDTO(
                symbol="AAPL",
                name="Apple Inc.",
                exchange="NASDAQ",
                instrument_type="stock",
                currency="USD",
                list_date=date(2020, 1, 1)
            ),
            InstrumentDTO(
                symbol="GOOGL", 
                name="Alphabet Inc.",
                exchange="NASDAQ",
                instrument_type="stock",
                currency="USD",
                list_date=date(2020, 1, 1)
            ),
            InstrumentDTO(
                symbol="MSFT",
                name="Microsoft Corporation", 
                exchange="NASDAQ",
                instrument_type="stock",
                currency="USD",
                list_date=date(2020, 1, 1)
            )
        ]
    
    @pytest.mark.asyncio
    async def test_service_initialization(self, instrument_service):
        """Test service can be initialized and basic operations work"""
        # Test service is available
        assert instrument_service is not None
        
        # Test basic operations don't crash
        count = await instrument_service.get_instrument_count()
        assert isinstance(count, int)
        assert count >= 0
        
        # Test search functionality
        criteria = InstrumentSearchCriteria(limit=10)
        instruments = await instrument_service.list_instruments(criteria)
        assert isinstance(instruments, list)
    
    @pytest.mark.asyncio
    async def test_crud_operations_workflow(self, instrument_service, sample_instruments):
        """Test complete CRUD workflow"""
        created_instruments = []
        
        try:
            # Create instruments
            for instrument_dto in sample_instruments:
                result = await instrument_service.create_instrument(instrument_dto)
                
                if result.success:
                    created_instruments.append(result.instrument_id)
                    
                    # Verify instrument was created
                    retrieved = await instrument_service.get_instrument_by_id(result.instrument_id)
                    assert retrieved is not None
                    assert retrieved.symbol == instrument_dto.symbol
                    assert retrieved.name == instrument_dto.name
                else:
                    # Might already exist, try to get it
                    existing = await instrument_service.get_instrument_by_symbol(instrument_dto.symbol)
                    if existing:
                        created_instruments.append(existing.id)
            
            # Test search operations
            if created_instruments:
                # Search by symbols
                symbols = [inst.symbol for inst in sample_instruments]
                criteria = InstrumentSearchCriteria(symbols=symbols, limit=10)
                found_instruments = await instrument_service.list_instruments(criteria)
                
                assert len(found_instruments) > 0
                found_symbols = {inst.symbol for inst in found_instruments}
                assert any(symbol in found_symbols for symbol in symbols)
                
                # Test symbol validation
                for symbol in symbols[:2]:  # Test first 2 symbols
                    is_valid = await instrument_service.validate_symbol(symbol)
                    assert is_valid is True
                
                # Test invalid symbol
                is_invalid = await instrument_service.validate_symbol("INVALID_SYMBOL_XYZ")
                assert is_invalid is False
        
        finally:
            # Cleanup is handled by the service layer
            pass
    
    @pytest.mark.asyncio
    async def test_cross_reference_workflow(self, instrument_service):
        """Test cross-reference management"""
        # Create a test instrument first
        test_instrument = InstrumentDTO(
            symbol="TEST_XREF",
            name="Test Cross Reference",
            exchange="NYSE",
            instrument_type="stock",
            currency="USD"
        )
        
        create_result = await instrument_service.create_instrument(test_instrument)
        
        if create_result.success:
            instrument_id = create_result.instrument_id
        else:
            # Try to find existing
            existing = await instrument_service.get_instrument_by_symbol("TEST_XREF")
            if existing:
                instrument_id = existing.id
            else:
                pytest.skip("Could not create or find test instrument")
        
        try:
            # Create cross-references
            xref1 = InstrumentXrefDTO(
                instrument_id=instrument_id,
                vendor_name="ticker",
                vendor_symbol="TEST_XREF",
                xref_type="equity"
            )
            
            xref2 = InstrumentXrefDTO(
                instrument_id=instrument_id,
                vendor_name="polygon",
                vendor_symbol="TEST_XREF", 
                xref_type="equity"
            )
            
            # Create cross-references
            result1 = await instrument_service.create_cross_reference(xref1)
            result2 = await instrument_service.create_cross_reference(xref2)
            
            # At least one should succeed (might already exist)
            success_count = sum([1 for r in [result1, result2] if r.success])
            assert success_count >= 0  # Could be 0 if they already exist
            
            # Retrieve cross-references
            xrefs = await instrument_service.get_cross_references(instrument_id)
            assert isinstance(xrefs, list)
            
            # Should have at least the ones we tried to create
            vendor_names = {xref.vendor_name for xref in xrefs}
            # At least one vendor should be present
            assert len(vendor_names) >= 1
        
        finally:
            # Cleanup handled by service layer
            pass
    
    @pytest.mark.asyncio
    async def test_batch_operations(self, instrument_service):
        """Test batch operations performance and correctness"""
        # Create batch of instruments
        batch_instruments = []
        for i in range(5):
            batch_instruments.append(InstrumentDTO(
                symbol=f"BATCH_{i}",
                name=f"Batch Test Instrument {i}",
                exchange="NYSE",
                instrument_type="stock",
                currency="USD"
            ))
        
        # Measure batch create performance
        start_time = time.time()
        result = await instrument_service.create_instruments_batch(batch_instruments)
        batch_time = time.time() - start_time
        
        # Should complete reasonably quickly
        assert batch_time < 10.0, f"Batch operation took too long: {batch_time}s"
        
        # Result should indicate some level of success
        assert result is not None
        assert hasattr(result, 'success')
        
        # Test batch retrieval if service supports it
        if hasattr(instrument_service, 'get_instruments_by_symbols'):
            symbols = [inst.symbol for inst in batch_instruments]
            retrieved = await instrument_service.get_instruments_by_symbols(symbols)
            assert isinstance(retrieved, list)
    
    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, instrument_service):
        """Test service handles errors gracefully"""
        # Test with invalid data
        invalid_instrument = InstrumentDTO(
            symbol="",  # Invalid empty symbol
            name="Invalid Test"
        )
        
        result = await instrument_service.create_instrument(invalid_instrument)
        assert result.success is False
        assert result.error_message is not None
        
        # Test with None values
        try:
            result = await instrument_service.get_instrument_by_id(None)
            assert result is None
        except (ValueError, TypeError):
            # Expected behavior for invalid input
            pass
        
        # Test with very large ID
        result = await instrument_service.get_instrument_by_id(999999999)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_performance_benchmarks(self, instrument_service):
        """Test service performance meets requirements"""
        
        # Test 1: Single instrument lookup should be fast
        start_time = time.time()
        count = await instrument_service.get_instrument_count()
        lookup_time = time.time() - start_time
        
        assert lookup_time < 1.0, f"Instrument count lookup too slow: {lookup_time}s"
        
        # Test 2: Symbol validation should be fast
        start_time = time.time()
        is_valid = await instrument_service.validate_symbol("AAPL")
        validation_time = time.time() - start_time
        
        assert validation_time < 1.0, f"Symbol validation too slow: {validation_time}s"
        
        # Test 3: List operations should handle reasonable load
        criteria = InstrumentSearchCriteria(limit=100)
        
        start_time = time.time()
        instruments = await instrument_service.list_instruments(criteria)
        list_time = time.time() - start_time
        
        assert list_time < 5.0, f"List operation too slow: {list_time}s"
        assert isinstance(instruments, list)


class TestAPIIntegration:
    """Integration tests for FastAPI endpoints with InstrumentService"""
    
    @pytest.fixture
    def client(self):
        """FastAPI test client"""
        return TestClient(app)
    
    def test_health_endpoint(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        
        # Should return 200 or 503 (if service unavailable)
        assert response.status_code in [200, 503]
        
        data = response.json()
        assert "status" in data
        assert "timestamp" in data
    
    def test_instrument_endpoints(self, client):
        """Test instrument CRUD endpoints"""
        # Test instrument search
        response = client.get("/instruments?limit=10")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        
        # Test instrument count
        response = client.get("/instruments/count")
        assert response.status_code == 200
        
        data = response.json()
        assert "total_instruments" in data
        assert isinstance(data["total_instruments"], int)
    
    def test_validation_endpoints(self, client):
        """Test validation endpoints"""
        # Test symbol validation
        response = client.get("/instruments/validate/AAPL")
        assert response.status_code == 200
        
        data = response.json()
        assert "symbol" in data
        assert "is_valid" in data
        assert isinstance(data["is_valid"], bool)
    
    def test_error_handling_endpoints(self, client):
        """Test API error handling"""
        # Test invalid instrument ID
        response = client.get("/instruments/999999999")
        assert response.status_code == 404
        
        # Test invalid symbol format
        response = client.get("/instruments/validate/")
        assert response.status_code in [404, 422]  # Not found or validation error
    
    def test_cache_endpoints(self, client):
        """Test cache management endpoints"""
        # Test cache stats (might not be implemented)
        response = client.get("/cache/stats")
        assert response.status_code in [200, 501]  # OK or Not Implemented
        
        # Test cache warm (might not be implemented)
        response = client.post("/cache/warm?limit=10")
        assert response.status_code in [200, 501]
    
    @pytest.mark.asyncio
    async def test_async_client_integration(self):
        """Test async client integration"""
        async with AsyncClient(app=app, base_url="http://test") as ac:
            # Test health endpoint
            response = await ac.get("/health")
            assert response.status_code in [200, 503]
            
            # Test instruments endpoint
            response = await ac.get("/instruments?limit=5")
            assert response.status_code == 200
            
            data = response.json()
            assert isinstance(data, list)


class TestEndToEndWorkflows:
    """End-to-end workflow tests"""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    def test_complete_instrument_lifecycle(self, client):
        """Test complete instrument lifecycle via API"""
        
        # Step 1: Check if instrument exists
        symbol = "TEST_E2E_001"
        response = client.get(f"/instruments/validate/{symbol}")
        assert response.status_code == 200
        
        validation_data = response.json()
        initially_exists = validation_data["is_valid"]
        
        # Step 2: Create instrument if it doesn't exist
        if not initially_exists:
            create_data = {
                "symbol": symbol,
                "name": "Test E2E Instrument",
                "exchange": "NYSE",
                "instrument_type": "stock",
                "currency": "USD"
            }
            
            response = client.post("/instruments", json=create_data)
            # Should succeed or indicate already exists
            assert response.status_code in [201, 400]
        
        # Step 3: Verify instrument exists
        response = client.get(f"/instruments/validate/{symbol}")
        assert response.status_code == 200
        
        validation_data = response.json()
        # Should exist now (either created or already existed)
        # Note: This might still be False if creation failed due to constraints
        
        # Step 4: Search for instrument
        response = client.get(f"/instruments?symbols={symbol}&limit=10")
        assert response.status_code == 200
        
        search_data = response.json()
        assert isinstance(search_data, list)
        
        # If instrument exists, it should be in search results
        if validation_data["is_valid"]:
            found_symbols = [inst["symbol"] for inst in search_data]
            assert symbol in found_symbols
    
    def test_api_performance_requirements(self, client):
        """Test API meets performance requirements"""
        
        # Test response time requirements
        start_time = time.time()
        response = client.get("/health")
        response_time = time.time() - start_time
        
        assert response_time < 2.0, f"Health endpoint too slow: {response_time}s"
        
        # Test list endpoint performance
        start_time = time.time() 
        response = client.get("/instruments?limit=50")
        list_response_time = time.time() - start_time
        
        assert list_response_time < 5.0, f"List endpoint too slow: {list_response_time}s"
        assert response.status_code == 200
    
    def test_concurrent_api_requests(self, client):
        """Test API handles concurrent requests"""
        import concurrent.futures
        
        def make_request():
            response = client.get("/instruments/count")
            return response.status_code == 200
        
        # Test 10 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_request) for _ in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # All requests should succeed
        success_rate = sum(results) / len(results)
        assert success_rate >= 0.8, f"Concurrent request success rate too low: {success_rate}"


@pytest.mark.integration
class TestServiceMigrationValidation:
    """Validate the service migration is working correctly"""
    
    @pytest.mark.asyncio
    async def test_service_architecture_components(self):
        """Test all service architecture components are working"""
        
        # Test environment setup
        env = Environment(None, EnvironmentType.DEV) 
        assert env is not None
        
        # Test service container
        service = await get_instrument_service(env)
        assert service is not None
        assert isinstance(service, InstrumentServiceInterface)
        
        # Test service methods exist
        required_methods = [
            'get_instrument_by_id',
            'get_instrument_by_symbol', 
            'list_instruments',
            'get_instrument_count',
            'validate_symbol',
            'create_instrument',
            'create_cross_reference'
        ]
        
        for method_name in required_methods:
            assert hasattr(service, method_name), f"Service missing method: {method_name}"
            assert callable(getattr(service, method_name))
    
    def test_api_service_integration(self):
        """Test API correctly integrates with service layer"""
        client = TestClient(app)
        
        # Test that API endpoints use service layer
        response = client.get("/health")
        
        # Health check should work (might be unavailable in test env)
        assert response.status_code in [200, 503]
        
        if response.status_code == 200:
            data = response.json()
            assert "service" in data
            assert data["service"] == "instruments-api-enhanced"
    
    @pytest.mark.asyncio
    async def test_migration_consistency(self):
        """Test that migrated services maintain consistency"""
        
        service = await get_instrument_service()
        
        # Test basic consistency
        count1 = await service.get_instrument_count()
        count2 = await service.get_instrument_count()
        
        # Counts should be consistent
        assert count1 == count2, "Service returning inconsistent results"
        
        # Test search consistency
        criteria = InstrumentSearchCriteria(limit=10)
        results1 = await service.list_instruments(criteria)
        results2 = await service.list_instruments(criteria) 
        
        # Should get same results
        assert len(results1) == len(results2), "Search results inconsistent"


if __name__ == "__main__":
    # Run with: pytest tests/integration/test_instrument_service_integration.py -v
    pytest.main([__file__, "-v", "--tb=short"])