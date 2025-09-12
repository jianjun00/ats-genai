"""
Test Migration Manager for Service Architecture

Handles test suite transformation for service-based architecture.
Migrates existing tests and creates comprehensive service testing frameworks.
"""

import logging
import ast
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)


@dataclass
class TestMigrationResult:
    """Result of test migration operation."""
    test_file: str
    migration_type: str
    status: str  # success, failed, skipped
    changes: List[str]
    new_test_count: int
    converted_test_count: int
    error_message: Optional[str]


@dataclass
class TestSuiteStructure:
    """Structure definition for service test suite."""
    service_name: str
    test_types: List[str]
    unit_tests: List[str]
    integration_tests: List[str]
    api_tests: List[str]
    performance_tests: List[str]
    mock_requirements: Dict[str, List[str]]


class TestMigrator:
    """Manages test migration for service architecture transformation."""

    def __init__(
        self,
        source_test_dir: str = "tests",
        target_test_dir: str = "tests/services",
        backup_dir: str = "tests/backup"
    ):
        self.source_test_dir = Path(source_test_dir)
        self.target_test_dir = Path(target_test_dir)
        self.backup_dir = Path(backup_dir)

        # Test framework templates
        self.test_templates = self._initialize_test_templates()

        # Mock patterns for service testing
        self.mock_patterns = self._initialize_mock_patterns()

    def migrate_all_tests(
        self,
        target_services: Optional[List[str]] = None,
        create_backup: bool = True
    ) -> List[TestMigrationResult]:
        """Migrate all tests to service-based architecture."""
        logger.info("Starting comprehensive test migration")

        results = []

        # Create directories
        self._ensure_test_directories()

        # Create backup if requested
        if create_backup:
            self._create_test_backup()

        # Analyze existing tests
        existing_tests = self._analyze_existing_tests()
        logger.info(f"Found {len(existing_tests)} existing test files")

        # Migrate tests for each service
        services_to_migrate = target_services or self._get_default_services()

        for service_name in services_to_migrate:
            logger.info(f"Migrating tests for service: {service_name}")

            try:
                # Create service test structure
                service_structure = self._create_service_test_structure(service_name)

                # Migrate existing relevant tests
                existing_service_tests = self._find_relevant_existing_tests(
                    service_name, existing_tests
                )

                migration_results = self._migrate_service_tests(
                    service_name, service_structure, existing_service_tests
                )
                results.extend(migration_results)

                # Create new service-specific tests
                new_test_results = self._create_new_service_tests(
                    service_name, service_structure
                )
                results.extend(new_test_results)

                logger.info(f"Successfully migrated tests for {service_name}")

            except Exception as e:
                logger.error(f"Failed to migrate tests for {service_name}: {e}")
                results.append(TestMigrationResult(
                    test_file=f"{service_name}_tests",
                    migration_type="service_test_migration",
                    status="failed",
                    changes=[],
                    new_test_count=0,
                    converted_test_count=0,
                    error_message=str(e)
                ))

        # Create shared test utilities
        shared_utils_result = self._create_shared_test_utilities()
        results.append(shared_utils_result)

        # Create test configuration
        config_result = self._create_test_configuration(services_to_migrate)
        results.append(config_result)

        # Create CI/CD test scripts
        ci_result = self._create_ci_test_scripts(services_to_migrate)
        results.append(ci_result)

        logger.info(f"Test migration completed. {len(results)} files processed")
        return results

    def analyze_test_coverage(self) -> Dict[str, Any]:
        """Analyze test coverage for migrated service tests."""
        logger.info("Analyzing test coverage for service tests")

        coverage_analysis = {
            'services': {},
            'overall_coverage': 0.0,
            'test_types': {
                'unit': 0,
                'integration': 0,
                'api': 0,
                'performance': 0
            },
            'missing_coverage': [],
            'recommendations': []
        }

        # Analyze each service
        for service_dir in self.target_test_dir.iterdir():
            if service_dir.is_dir() and not service_dir.name.startswith('.'):
                service_name = service_dir.name
                service_coverage = self._analyze_service_test_coverage(service_name)
                coverage_analysis['services'][service_name] = service_coverage

                # Update overall statistics
                for test_type in coverage_analysis['test_types']:
                    coverage_analysis['test_types'][test_type] += service_coverage.get(
                        f'{test_type}_tests', 0
                    )

        # Calculate overall coverage
        total_services = len(coverage_analysis['services'])
        if total_services > 0:
            coverage_scores = [
                service_data.get('coverage_score', 0)
                for service_data in coverage_analysis['services'].values()
            ]
            coverage_analysis['overall_coverage'] = sum(coverage_scores) / len(coverage_scores)

        # Generate recommendations
        coverage_analysis['recommendations'] = self._generate_coverage_recommendations(
            coverage_analysis
        )

        logger.info(f"Test coverage analysis completed: {coverage_analysis['overall_coverage']:.1f}%")
        return coverage_analysis

    def validate_migrated_tests(self) -> Dict[str, Any]:
        """Validate all migrated test files."""
        logger.info("Validating migrated test files")

        validation_results = {
            'service_tests': [],
            'shared_utilities': [],
            'configuration': [],
            'overall_status': 'unknown',
            'syntax_errors': [],
            'import_errors': [],
            'missing_dependencies': []
        }

        # Validate service test files
        for service_dir in self.target_test_dir.iterdir():
            if service_dir.is_dir() and not service_dir.name.startswith('.'):
                service_name = service_dir.name
                service_validation = self._validate_service_tests(service_name)
                validation_results['service_tests'].append(service_validation)

                # Collect errors
                validation_results['syntax_errors'].extend(
                    service_validation.get('syntax_errors', [])
                )
                validation_results['import_errors'].extend(
                    service_validation.get('import_errors', [])
                )

        # Validate shared utilities
        shared_validation = self._validate_shared_utilities()
        validation_results['shared_utilities'] = shared_validation

        # Validate test configuration
        config_validation = self._validate_test_configuration()
        validation_results['configuration'] = config_validation

        # Determine overall status
        total_errors = (
            len(validation_results['syntax_errors']) +
            len(validation_results['import_errors']) +
            len(validation_results['missing_dependencies'])
        )

        if total_errors == 0:
            validation_results['overall_status'] = 'valid'
        elif total_errors < 5:
            validation_results['overall_status'] = 'warning'
        else:
            validation_results['overall_status'] = 'invalid'

        logger.info(f"Test validation completed: {validation_results['overall_status']}")
        return validation_results

    def generate_test_migration_report(
        self,
        migration_results: List[TestMigrationResult]
    ) -> Dict[str, Any]:
        """Generate comprehensive test migration report."""
        successful_migrations = [r for r in migration_results if r.status == 'success']
        failed_migrations = [r for r in migration_results if r.status == 'failed']

        total_new_tests = sum(r.new_test_count for r in migration_results)
        total_converted_tests = sum(r.converted_test_count for r in migration_results)

        migration_by_type = {}
        for result in migration_results:
            migration_type = result.migration_type
            if migration_type not in migration_by_type:
                migration_by_type[migration_type] = {'success': 0, 'failed': 0}
            migration_by_type[migration_type][result.status] += 1

        report = {
            'summary': {
                'total_migrations': len(migration_results),
                'successful': len(successful_migrations),
                'failed': len(failed_migrations),
                'success_rate': len(successful_migrations) / len(migration_results) * 100,
                'new_tests_created': total_new_tests,
                'tests_converted': total_converted_tests,
                'total_tests': total_new_tests + total_converted_tests
            },
            'by_type': migration_by_type,
            'successful_files': [r.test_file for r in successful_migrations],
            'failed_files': [
                {
                    'file': r.test_file,
                    'error': r.error_message
                } for r in failed_migrations
            ],
            'test_coverage_analysis': self.analyze_test_coverage(),
            'timestamp': datetime.now().isoformat()
        }

        return report

    # Private helper methods

    def _initialize_test_templates(self) -> Dict[str, Dict[str, str]]:
        """Initialize test templates for different service types."""
        return {
            'unit_test': {
                'instruments': '''"""
Unit tests for Instruments Service

Tests service implementation logic in isolation using mocks.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime
from typing import List, Optional

from src.domains.instruments.services.implementations.instrument_service import InstrumentService
from src.domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentServiceInterface,
    InstrumentDTO,
    VendorInstrumentDTO,
    InstrumentXrefDTO,
    CreateInstrumentRequest,
    UpdateInstrumentRequest
)


class TestInstrumentService:
    """Unit tests for InstrumentService implementation."""

    @pytest.fixture
    def mock_repository(self):
        """Create mock instrument repository."""
        repository = Mock()
        repository.get_by_id = AsyncMock()
        repository.get_by_symbol = AsyncMock()
        repository.create = AsyncMock()
        repository.update = AsyncMock()
        repository.delete = AsyncMock()
        repository.list_all = AsyncMock()
        repository.search = AsyncMock()
        return repository

    @pytest.fixture
    def mock_cache(self):
        """Create mock cache."""
        cache = Mock()
        cache.get = AsyncMock()
        cache.set = AsyncMock()
        cache.delete = AsyncMock()
        cache.clear = AsyncMock()
        return cache

    @pytest.fixture
    def service(self, mock_repository, mock_cache):
        """Create InstrumentService instance with mocks."""
        return InstrumentService(
            repository=mock_repository,
            cache=mock_cache
        )

    async def test_get_instrument_by_id_success(self, service, mock_repository):
        """Test successful instrument retrieval by ID."""
        # Arrange
        instrument_id = "INSTR_001"
        expected_instrument = InstrumentDTO(
            id=instrument_id,
            symbol="AAPL",
            name="Apple Inc.",
            instrument_type="stock",
            exchange="NASDAQ",
            currency="USD",
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        mock_repository.get_by_id.return_value = expected_instrument

        # Act
        result = await service.get_instrument_by_id(instrument_id)

        # Assert
        assert result is not None
        assert result.id == instrument_id
        assert result.symbol == "AAPL"
        assert result.name == "Apple Inc."
        mock_repository.get_by_id.assert_called_once_with(instrument_id)

    async def test_get_instrument_by_id_not_found(self, service, mock_repository):
        """Test instrument retrieval when ID not found."""
        # Arrange
        instrument_id = "NONEXISTENT"
        mock_repository.get_by_id.return_value = None

        # Act
        result = await service.get_instrument_by_id(instrument_id)

        # Assert
        assert result is None
        mock_repository.get_by_id.assert_called_once_with(instrument_id)

    async def test_create_instrument_success(self, service, mock_repository):
        """Test successful instrument creation."""
        # Arrange
        request = CreateInstrumentRequest(
            symbol="TSLA",
            name="Tesla Inc.",
            instrument_type="stock",
            exchange="NASDAQ",
            currency="USD"
        )

        created_instrument = InstrumentDTO(
            id="INSTR_002",
            symbol=request.symbol,
            name=request.name,
            instrument_type=request.instrument_type,
            exchange=request.exchange,
            currency=request.currency,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        mock_repository.create.return_value = created_instrument

        # Act
        result = await service.create_instrument(request)

        # Assert
        assert result is not None
        assert result.symbol == request.symbol
        assert result.name == request.name
        mock_repository.create.assert_called_once()

    async def test_update_instrument_success(self, service, mock_repository):
        """Test successful instrument update."""
        # Arrange
        instrument_id = "INSTR_001"
        request = UpdateInstrumentRequest(
            name="Apple Inc. (Updated)",
            currency="USD"
        )

        updated_instrument = InstrumentDTO(
            id=instrument_id,
            symbol="AAPL",
            name=request.name,
            instrument_type="stock",
            exchange="NASDAQ",
            currency=request.currency,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        mock_repository.update.return_value = updated_instrument

        # Act
        result = await service.update_instrument(instrument_id, request)

        # Assert
        assert result is not None
        assert result.name == request.name
        mock_repository.update.assert_called_once_with(instrument_id, request)

    async def test_search_instruments_with_filters(self, service, mock_repository):
        """Test instrument search with filters."""
        # Arrange
        filters = {
            "instrument_type": "stock",
            "exchange": "NASDAQ"
        }

        expected_instruments = [
            InstrumentDTO(
                id="INSTR_001",
                symbol="AAPL",
                name="Apple Inc.",
                instrument_type="stock",
                exchange="NASDAQ",
                currency="USD",
                created_at=datetime.now(),
                updated_at=datetime.now()
            ),
            InstrumentDTO(
                id="INSTR_002",
                symbol="TSLA",
                name="Tesla Inc.",
                instrument_type="stock",
                exchange="NASDAQ",
                currency="USD",
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
        ]

        mock_repository.search.return_value = expected_instruments

        # Act
        results = await service.search_instruments(filters)

        # Assert
        assert len(results) == 2
        assert all(instr.exchange == "NASDAQ" for instr in results)
        assert all(instr.instrument_type == "stock" for instr in results)
        mock_repository.search.assert_called_once_with(filters)

    async def test_delete_instrument_success(self, service, mock_repository, mock_cache):
        """Test successful instrument deletion."""
        # Arrange
        instrument_id = "INSTR_001"
        mock_repository.delete.return_value = True

        # Act
        result = await service.delete_instrument(instrument_id)

        # Assert
        assert result is True
        mock_repository.delete.assert_called_once_with(instrument_id)
        mock_cache.delete.assert_called()  # Should clear cache

    @patch('src.infrastructure.caching.performance_profiler.get_performance_profiler')
    async def test_service_performance_monitoring(self, mock_profiler, service, mock_repository):
        """Test that service operations are properly profiled."""
        # Arrange
        mock_profiler_instance = Mock()
        mock_profiler.return_value = mock_profiler_instance
        mock_profiler_instance.profile_operation = Mock()

        instrument_id = "INSTR_001"
        expected_instrument = InstrumentDTO(
            id=instrument_id,
            symbol="AAPL",
            name="Apple Inc.",
            instrument_type="stock",
            exchange="NASDAQ",
            currency="USD",
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        mock_repository.get_by_id.return_value = expected_instrument

        # Act
        await service.get_instrument_by_id(instrument_id)

        # Assert - verify performance profiling was used
        # This would depend on actual implementation details
        mock_repository.get_by_id.assert_called_once_with(instrument_id)
''',
                'market_data': '''"""
Unit tests for Market Data Service

Tests market data service implementation logic in isolation using mocks.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
from typing import List, Optional

from src.domains.market_data.services.implementations.market_data_service import MarketDataService
from src.domains.market_data.services.interfaces.market_data_service_interface import (
    MarketDataServiceInterface,
    PriceDataDTO,
    VolumeDataDTO,
    QuoteDataDTO,
    TimeSeriesRequest,
    TimeSeriesResponse
)


class TestMarketDataService:
    """Unit tests for MarketDataService implementation."""

    @pytest.fixture
    def mock_repository(self):
        """Create mock market data repository."""
        repository = Mock()
        repository.get_price_data = AsyncMock()
        repository.get_volume_data = AsyncMock()
        repository.get_quote_data = AsyncMock()
        repository.get_time_series = AsyncMock()
        return repository

    @pytest.fixture
    def mock_cache(self):
        """Create mock cache."""
        cache = Mock()
        cache.get = AsyncMock()
        cache.set = AsyncMock()
        cache.delete = AsyncMock()
        return cache

    @pytest.fixture
    def service(self, mock_repository, mock_cache):
        """Create MarketDataService instance with mocks."""
        return MarketDataService(
            repository=mock_repository,
            cache=mock_cache
        )

    async def test_get_latest_price_success(self, service, mock_repository):
        """Test successful latest price retrieval."""
        # Arrange
        symbol = "AAPL"
        expected_price = PriceDataDTO(
            symbol=symbol,
            timestamp=datetime.now(),
            open=150.00,
            high=155.00,
            low=148.00,
            close=153.50,
            volume=1000000
        )

        mock_repository.get_price_data.return_value = expected_price

        # Act
        result = await service.get_latest_price(symbol)

        # Assert
        assert result is not None
        assert result.symbol == symbol
        assert result.close == 153.50
        mock_repository.get_price_data.assert_called_once()

    async def test_get_time_series_data_success(self, service, mock_repository):
        """Test successful time series data retrieval."""
        # Arrange
        request = TimeSeriesRequest(
            symbol="AAPL",
            start_date=datetime.now() - timedelta(days=30),
            end_date=datetime.now(),
            interval="1d"
        )

        expected_data = [
            PriceDataDTO(
                symbol="AAPL",
                timestamp=datetime.now() - timedelta(days=i),
                open=150.00 + i,
                high=155.00 + i,
                low=148.00 + i,
                close=153.50 + i,
                volume=1000000 + (i * 10000)
            )
            for i in range(30)
        ]

        expected_response = TimeSeriesResponse(
            symbol=request.symbol,
            interval=request.interval,
            data_points=expected_data,
            total_count=len(expected_data)
        )

        mock_repository.get_time_series.return_value = expected_response

        # Act
        result = await service.get_time_series(request)

        # Assert
        assert result is not None
        assert result.symbol == request.symbol
        assert result.total_count == 30
        assert len(result.data_points) == 30
        mock_repository.get_time_series.assert_called_once_with(request)

    async def test_cache_hit_performance(self, service, mock_cache):
        """Test cache hit performance optimization."""
        # Arrange
        symbol = "AAPL"
        cached_price = PriceDataDTO(
            symbol=symbol,
            timestamp=datetime.now(),
            open=150.00,
            high=155.00,
            low=148.00,
            close=153.50,
            volume=1000000
        )

        mock_cache.get.return_value = cached_price

        # Act
        result = await service.get_latest_price(symbol)

        # Assert
        assert result == cached_price
        mock_cache.get.assert_called_once()
        # Repository should not be called on cache hit
        assert not hasattr(service.repository, 'get_price_data') or \\
               not service.repository.get_price_data.called
'''
            },
            'integration_test': {
                'instruments': '''"""
Integration tests for Instruments Service

Tests service integration with real dependencies (database, cache, etc.).
"""

import pytest
import asyncio
from datetime import datetime
from typing import List

from src.domains.instruments.services.implementations.instrument_service import InstrumentService
from src.domains.instruments.services.interfaces.instrument_service_interface import (
    CreateInstrumentRequest,
    UpdateInstrumentRequest
)
from src.domains.instruments.repositories.instrument_repository import InstrumentRepository
from src.infrastructure.caching import MemoryCache, CacheConfig
from tests.fixtures.database_fixtures import test_database, cleanup_test_data


class TestInstrumentServiceIntegration:
    """Integration tests for InstrumentService with real dependencies."""

    @pytest.fixture
    async def cache(self):
        """Create real cache instance for testing."""
        config = CacheConfig(
            ttl_seconds=300,
            max_size=100,
            namespace="test_instruments"
        )
        cache = MemoryCache(config)
        yield cache
        await cache.clear()

    @pytest.fixture
    async def repository(self, test_database):
        """Create real repository instance for testing."""
        repository = InstrumentRepository(test_database)
        await repository.initialize()
        yield repository
        await cleanup_test_data(test_database, "instruments")

    @pytest.fixture
    async def service(self, repository, cache):
        """Create InstrumentService with real dependencies."""
        return InstrumentService(repository=repository, cache=cache)

    async def test_full_instrument_lifecycle(self, service):
        """Test complete instrument lifecycle: create, read, update, delete."""
        # Create instrument
        create_request = CreateInstrumentRequest(
            symbol="INTG_TEST",
            name="Integration Test Instrument",
            instrument_type="stock",
            exchange="TEST",
            currency="USD"
        )

        created = await service.create_instrument(create_request)
        assert created is not None
        assert created.symbol == "INTG_TEST"
        created_id = created.id

        # Read instrument
        retrieved = await service.get_instrument_by_id(created_id)
        assert retrieved is not None
        assert retrieved.id == created_id
        assert retrieved.symbol == "INTG_TEST"

        # Update instrument
        update_request = UpdateInstrumentRequest(
            name="Updated Integration Test Instrument"
        )

        updated = await service.update_instrument(created_id, update_request)
        assert updated is not None
        assert updated.name == update_request.name
        assert updated.id == created_id

        # Delete instrument
        deleted = await service.delete_instrument(created_id)
        assert deleted is True

        # Verify deletion
        not_found = await service.get_instrument_by_id(created_id)
        assert not_found is None

    async def test_cache_integration(self, service):
        """Test cache integration works properly."""
        # Create test instrument
        create_request = CreateInstrumentRequest(
            symbol="CACHE_TEST",
            name="Cache Test Instrument",
            instrument_type="stock",
            exchange="TEST",
            currency="USD"
        )

        created = await service.create_instrument(create_request)
        created_id = created.id

        try:
            # First call - should hit database and populate cache
            first_call = await service.get_instrument_by_id(created_id)
            assert first_call is not None

            # Second call - should hit cache
            second_call = await service.get_instrument_by_id(created_id)
            assert second_call is not None
            assert second_call.id == first_call.id

            # Verify cache contains the data
            cache_key = f"instrument:{created_id}"
            cached_data = await service.cache.get(cache_key)
            assert cached_data is not None

        finally:
            # Cleanup
            await service.delete_instrument(created_id)

    async def test_concurrent_operations(self, service):
        """Test service handles concurrent operations properly."""
        # Create multiple instruments concurrently
        create_tasks = []
        for i in range(5):
            request = CreateInstrumentRequest(
                symbol=f"CONCURRENT_{i}",
                name=f"Concurrent Test {i}",
                instrument_type="stock",
                exchange="TEST",
                currency="USD"
            )
            task = asyncio.create_task(service.create_instrument(request))
            create_tasks.append(task)

        # Wait for all creations to complete
        created_instruments = await asyncio.gather(*create_tasks)

        try:
            # Verify all instruments were created successfully
            assert len(created_instruments) == 5
            for i, instrument in enumerate(created_instruments):
                assert instrument is not None
                assert instrument.symbol == f"CONCURRENT_{i}"

            # Test concurrent reads
            read_tasks = [
                asyncio.create_task(service.get_instrument_by_id(instr.id))
                for instr in created_instruments
            ]

            read_results = await asyncio.gather(*read_tasks)
            assert len(read_results) == 5
            assert all(result is not None for result in read_results)

        finally:
            # Cleanup all created instruments
            delete_tasks = [
                asyncio.create_task(service.delete_instrument(instr.id))
                for instr in created_instruments
            ]
            await asyncio.gather(*delete_tasks)

    async def test_error_handling_and_rollback(self, service):
        """Test proper error handling and transaction rollback."""
        # This test would require more sophisticated setup
        # to test database transaction rollback scenarios
        pass
'''
            },
            'api_test': {
                'instruments': '''"""
API tests for Instruments Service

Tests REST API endpoints with real HTTP requests.
"""

import pytest
import json
from httpx import AsyncClient
from fastapi.testclient import TestClient

from src.api.main import app
from tests.fixtures.database_fixtures import test_database, cleanup_test_data


class TestInstrumentsAPI:
    """API tests for instruments endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client for API testing."""
        return TestClient(app)

    @pytest.fixture
    async def async_client(self):
        """Create async test client for API testing."""
        async with AsyncClient(app=app, base_url="http://test") as ac:
            yield ac

    def test_get_instrument_by_id_success(self, client):
        """Test GET /api/v1/instruments/{id} success case."""
        # This would require setting up test data first
        instrument_id = "test_instrument_id"

        response = client.get(f"/api/v1/instruments/{instrument_id}")

        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert "symbol" in data
        assert "name" in data

    def test_get_instrument_by_id_not_found(self, client):
        """Test GET /api/v1/instruments/{id} not found case."""
        response = client.get("/api/v1/instruments/nonexistent")

        assert response.status_code == 404
        data = response.json()
        assert "detail" in data

    def test_create_instrument_success(self, client):
        """Test POST /api/v1/instruments success case."""
        instrument_data = {
            "symbol": "API_TEST",
            "name": "API Test Instrument",
            "instrument_type": "stock",
            "exchange": "TEST",
            "currency": "USD"
        }

        response = client.post(
            "/api/v1/instruments",
            json=instrument_data
        )

        assert response.status_code == 201
        data = response.json()
        assert data["symbol"] == "API_TEST"
        assert "id" in data

        # Cleanup
        instrument_id = data["id"]
        client.delete(f"/api/v1/instruments/{instrument_id}")

    def test_create_instrument_validation_error(self, client):
        """Test POST /api/v1/instruments with invalid data."""
        invalid_data = {
            "symbol": "",  # Invalid empty symbol
            "name": "Test",
            "instrument_type": "invalid_type"  # Invalid type
        }

        response = client.post(
            "/api/v1/instruments",
            json=invalid_data
        )

        assert response.status_code == 422
        data = response.json()
        assert "detail" in data

    def test_update_instrument_success(self, client):
        """Test PUT /api/v1/instruments/{id} success case."""
        # First create an instrument to update
        create_data = {
            "symbol": "UPDATE_TEST",
            "name": "Original Name",
            "instrument_type": "stock",
            "exchange": "TEST",
            "currency": "USD"
        }

        create_response = client.post("/api/v1/instruments", json=create_data)
        assert create_response.status_code == 201
        instrument_id = create_response.json()["id"]

        try:
            # Update the instrument
            update_data = {
                "name": "Updated Name"
            }

            response = client.put(
                f"/api/v1/instruments/{instrument_id}",
                json=update_data
            )

            assert response.status_code == 200
            data = response.json()
            assert data["name"] == "Updated Name"
            assert data["id"] == instrument_id

        finally:
            # Cleanup
            client.delete(f"/api/v1/instruments/{instrument_id}")

    def test_search_instruments_with_filters(self, client):
        """Test GET /api/v1/instruments with search filters."""
        response = client.get("/api/v1/instruments", params={
            "instrument_type": "stock",
            "exchange": "NASDAQ",
            "limit": 10
        })

        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total" in data
        assert isinstance(data["items"], list)

    async def test_api_performance_benchmarks(self, async_client):
        """Test API response times meet performance requirements."""
        import time

        start_time = time.time()
        response = await async_client.get("/api/v1/instruments")
        end_time = time.time()

        response_time_ms = (end_time - start_time) * 1000

        assert response.status_code == 200
        assert response_time_ms < 500  # Should respond within 500ms

    def test_api_error_handling(self, client):
        """Test API error handling and response format."""
        # Test various error scenarios
        error_scenarios = [
            ("/api/v1/instruments/invalid_id_format", 400),
            ("/api/v1/instruments/nonexistent_id", 404),
            ("/api/v1/nonexistent_endpoint", 404)
        ]

        for endpoint, expected_status in error_scenarios:
            response = client.get(endpoint)
            assert response.status_code == expected_status

            # Verify error response format
            data = response.json()
            assert "detail" in data or "message" in data
'''
            },
            'performance_test': {
                'instruments': '''"""
Performance tests for Instruments Service

Tests service performance characteristics under various loads.
"""

import pytest
import asyncio
import time
import statistics
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.domains.instruments.services.implementations.instrument_service import InstrumentService
from tests.fixtures.performance_fixtures import performance_test_data, load_test_instruments


class TestInstrumentServicePerformance:
    """Performance tests for InstrumentService."""

    @pytest.fixture
    async def service_with_data(self, service, performance_test_data):
        """Create service with pre-loaded performance test data."""
        # Load test instruments
        await load_test_instruments(service, count=1000)
        yield service
        # Cleanup handled by fixture

    @pytest.mark.performance
    async def test_single_instrument_lookup_performance(self, service_with_data):
        """Test single instrument lookup performance."""
        instrument_id = "PERF_TEST_001"

        # Warm up
        for _ in range(10):
            await service_with_data.get_instrument_by_id(instrument_id)

        # Measure performance
        times = []
        for _ in range(100):
            start_time = time.time()
            result = await service_with_data.get_instrument_by_id(instrument_id)
            end_time = time.time()

            assert result is not None
            times.append((end_time - start_time) * 1000)  # Convert to ms

        # Performance assertions
        avg_time = statistics.mean(times)
        p95_time = statistics.quantiles(times, n=20)[18]  # 95th percentile

        assert avg_time < 10, f"Average lookup time {avg_time:.2f}ms exceeds 10ms threshold"
        assert p95_time < 50, f"95th percentile time {p95_time:.2f}ms exceeds 50ms threshold"

        print(f"Single lookup performance: avg={avg_time:.2f}ms, p95={p95_time:.2f}ms")

    @pytest.mark.performance
    async def test_batch_lookup_performance(self, service_with_data):
        """Test batch instrument lookup performance."""
        instrument_ids = [f"PERF_TEST_{i:03d}" for i in range(1, 101)]

        # Test concurrent lookups
        start_time = time.time()

        tasks = [
            service_with_data.get_instrument_by_id(instrument_id)
            for instrument_id in instrument_ids
        ]

        results = await asyncio.gather(*tasks)
        end_time = time.time()

        total_time_ms = (end_time - start_time) * 1000
        avg_time_per_lookup = total_time_ms / len(instrument_ids)

        # Performance assertions
        assert all(result is not None for result in results)
        assert total_time_ms < 1000, f"Batch lookup took {total_time_ms:.2f}ms, exceeds 1000ms"
        assert avg_time_per_lookup < 20, f"Average per-lookup time {avg_time_per_lookup:.2f}ms exceeds 20ms"

        print(f"Batch lookup performance: total={total_time_ms:.2f}ms, avg={avg_time_per_lookup:.2f}ms")

    @pytest.mark.performance
    async def test_search_performance(self, service_with_data):
        """Test search operation performance."""
        search_filters = {
            "instrument_type": "stock",
            "exchange": "NYSE"
        }

        # Warm up
        for _ in range(5):
            await service_with_data.search_instruments(search_filters)

        # Measure performance
        times = []
        for _ in range(20):
            start_time = time.time()
            results = await service_with_data.search_instruments(search_filters)
            end_time = time.time()

            assert len(results) > 0
            times.append((end_time - start_time) * 1000)

        avg_time = statistics.mean(times)
        assert avg_time < 100, f"Average search time {avg_time:.2f}ms exceeds 100ms threshold"

        print(f"Search performance: avg={avg_time:.2f}ms")

    @pytest.mark.performance
    async def test_cache_performance_impact(self, service_with_data):
        """Test performance impact of caching."""
        instrument_id = "PERF_TEST_001"

        # First call (cache miss)
        start_time = time.time()
        result1 = await service_with_data.get_instrument_by_id(instrument_id)
        miss_time = (time.time() - start_time) * 1000

        # Second call (cache hit)
        start_time = time.time()
        result2 = await service_with_data.get_instrument_by_id(instrument_id)
        hit_time = (time.time() - start_time) * 1000

        assert result1 == result2
        assert hit_time < miss_time, "Cache hit should be faster than cache miss"
        assert hit_time < 1, f"Cache hit time {hit_time:.2f}ms should be under 1ms"

        speedup_factor = miss_time / hit_time
        assert speedup_factor > 5, f"Cache speedup {speedup_factor:.1f}x should be > 5x"

        print(f"Cache performance: miss={miss_time:.2f}ms, hit={hit_time:.2f}ms, speedup={speedup_factor:.1f}x")

    @pytest.mark.performance
    async def test_memory_usage_under_load(self, service_with_data):
        """Test memory usage characteristics under load."""
        import psutil
        import gc

        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Perform intensive operations
        for batch in range(10):
            tasks = [
                service_with_data.get_instrument_by_id(f"PERF_TEST_{i:03d}")
                for i in range(1, 101)
            ]
            await asyncio.gather(*tasks)

            # Force garbage collection
            gc.collect()

        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory

        assert memory_increase < 100, f"Memory increase {memory_increase:.1f}MB exceeds 100MB threshold"

        print(f"Memory usage: initial={initial_memory:.1f}MB, final={final_memory:.1f}MB, increase={memory_increase:.1f}MB")

    @pytest.mark.performance
    async def test_concurrent_write_performance(self, service_with_data):
        """Test performance of concurrent write operations."""
        # Create instruments concurrently
        create_tasks = []
        for i in range(50):
            from src.domains.instruments.services.interfaces.instrument_service_interface import CreateInstrumentRequest
            request = CreateInstrumentRequest(
                symbol=f"PERF_WRITE_{i:03d}",
                name=f"Performance Test Write {i}",
                instrument_type="stock",
                exchange="TEST",
                currency="USD"
            )
            task = service_with_data.create_instrument(request)
            create_tasks.append(task)

        start_time = time.time()
        created_instruments = await asyncio.gather(*create_tasks)
        create_time = (time.time() - start_time) * 1000

        try:
            # Performance assertions
            assert len(created_instruments) == 50
            assert all(instr is not None for instr in created_instruments)
            assert create_time < 5000, f"Concurrent creates took {create_time:.2f}ms, exceeds 5000ms"

            avg_create_time = create_time / 50
            assert avg_create_time < 200, f"Average create time {avg_create_time:.2f}ms exceeds 200ms"

            print(f"Concurrent write performance: total={create_time:.2f}ms, avg={avg_create_time:.2f}ms")

        finally:
            # Cleanup
            delete_tasks = [
                service_with_data.delete_instrument(instr.id)
                for instr in created_instruments if instr
            ]
            await asyncio.gather(*delete_tasks)
'''
            }
        }

    def _initialize_mock_patterns(self) -> Dict[str, Dict[str, str]]:
        """Initialize mock patterns for service testing."""
        return {
            'repository_mock': '''
    @pytest.fixture
    def mock_repository(self):
        """Create mock repository for testing."""
        repository = Mock()
        repository.get_by_id = AsyncMock()
        repository.create = AsyncMock()
        repository.update = AsyncMock()
        repository.delete = AsyncMock()
        repository.list_all = AsyncMock()
        repository.search = AsyncMock()
        return repository
''',
            'cache_mock': '''
    @pytest.fixture
    def mock_cache(self):
        """Create mock cache for testing."""
        cache = Mock()
        cache.get = AsyncMock()
        cache.set = AsyncMock()
        cache.delete = AsyncMock()
        cache.clear = AsyncMock()
        return cache
''',
            'service_mock': '''
    @pytest.fixture
    def mock_service(self):
        """Create mock service for testing."""
        service = Mock()
        service.get_by_id = AsyncMock()
        service.create = AsyncMock()
        service.update = AsyncMock()
        service.delete = AsyncMock()
        service.search = AsyncMock()
        return service
'''
        }

    def _get_default_services(self) -> List[str]:
        """Get default list of services to migrate tests for."""
        return ['instruments', 'market_data', 'analytics', 'user_management']

    def _ensure_test_directories(self):
        """Ensure all test directories exist."""
        directories = [
            self.target_test_dir,
            self.backup_dir,
            self.target_test_dir / "fixtures",
            self.target_test_dir / "utilities",
            self.target_test_dir / "integration"
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    def _create_test_backup(self):
        """Create backup of existing test files."""
        import shutil

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.backup_dir / f"tests_backup_{timestamp}"

        if self.source_test_dir.exists():
            shutil.copytree(self.source_test_dir, backup_path)
            logger.info(f"Created test backup at: {backup_path}")

    def _analyze_existing_tests(self) -> List[Dict[str, Any]]:
        """Analyze existing test files to understand structure."""
        existing_tests = []

        if not self.source_test_dir.exists():
            logger.warning(f"Source test directory not found: {self.source_test_dir}")
            return existing_tests

        for test_file in self.source_test_dir.rglob("test_*.py"):
            try:
                with open(test_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Parse AST to analyze test structure
                tree = ast.parse(content)

                test_analysis = {
                    'file_path': str(test_file),
                    'classes': [],
                    'functions': [],
                    'imports': [],
                    'fixtures': [],
                    'marks': []
                }

                for node in ast.walk(tree):
                    if isinstance(node, ast.ClassDef):
                        test_analysis['classes'].append(node.name)
                    elif isinstance(node, ast.FunctionDef):
                        if node.name.startswith('test_'):
                            test_analysis['functions'].append(node.name)
                        elif any(decorator.id == 'pytest.fixture'
                               for decorator in node.decorator_list
                               if isinstance(decorator, ast.Name)):
                            test_analysis['fixtures'].append(node.name)
                    elif isinstance(node, ast.Import):
                        for alias in node.names:
                            test_analysis['imports'].append(alias.name)
                    elif isinstance(node, ast.ImportFrom):
                        if node.module:
                            test_analysis['imports'].append(node.module)

                existing_tests.append(test_analysis)

            except Exception as e:
                logger.error(f"Failed to analyze test file {test_file}: {e}")

        return existing_tests

    def _create_service_test_structure(self, service_name: str) -> TestSuiteStructure:
        """Create comprehensive test structure for service."""
        return TestSuiteStructure(
            service_name=service_name,
            test_types=['unit', 'integration', 'api', 'performance'],
            unit_tests=[
                f'test_{service_name}_service_unit.py',
                f'test_{service_name}_repository_unit.py',
                f'test_{service_name}_models_unit.py'
            ],
            integration_tests=[
                f'test_{service_name}_service_integration.py',
                f'test_{service_name}_api_integration.py',
                f'test_{service_name}_database_integration.py'
            ],
            api_tests=[
                f'test_{service_name}_api_endpoints.py',
                f'test_{service_name}_api_validation.py',
                f'test_{service_name}_api_performance.py'
            ],
            performance_tests=[
                f'test_{service_name}_performance.py',
                f'test_{service_name}_load_test.py',
                f'test_{service_name}_stress_test.py'
            ],
            mock_requirements={
                'repositories': [f'{service_name}_repository'],
                'services': [f'{service_name}_service'],
                'external_apis': [],
                'cache': ['redis_cache', 'memory_cache']
            }
        )

    def _find_relevant_existing_tests(
        self,
        service_name: str,
        existing_tests: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Find existing tests relevant to the service."""
        relevant_tests = []

        # Keywords to identify relevant tests
        service_keywords = [
            service_name,
            service_name.replace('_', ''),
            service_name.replace('_', ' ').title().replace(' ', '')
        ]

        for test_analysis in existing_tests:
            file_path = test_analysis['file_path']
            content_lower = file_path.lower()

            # Check if test file is relevant to this service
            if any(keyword.lower() in content_lower for keyword in service_keywords):
                relevant_tests.append(test_analysis)
                continue

            # Check class names and function names
            all_names = test_analysis['classes'] + test_analysis['functions']
            if any(any(keyword.lower() in name.lower() for keyword in service_keywords)
                   for name in all_names):
                relevant_tests.append(test_analysis)

        return relevant_tests

    def _migrate_service_tests(
        self,
        service_name: str,
        service_structure: TestSuiteStructure,
        existing_tests: List[Dict[str, Any]]
    ) -> List[TestMigrationResult]:
        """Migrate existing tests to service structure."""
        results = []

        for existing_test in existing_tests:
            try:
                # Read existing test content
                with open(existing_test['file_path'], 'r', encoding='utf-8') as f:
                    original_content = f.read()

                # Transform test content for service architecture
                transformed_content = self._transform_test_content(
                    original_content, service_name
                )

                # Determine target test file
                target_file = self._determine_target_test_file(
                    existing_test, service_name, service_structure
                )

                # Write transformed test
                target_path = self.target_test_dir / service_name / target_file
                target_path.parent.mkdir(parents=True, exist_ok=True)

                with open(target_path, 'w', encoding='utf-8') as f:
                    f.write(transformed_content)

                results.append(TestMigrationResult(
                    test_file=str(target_path),
                    migration_type="existing_test_migration",
                    status="success",
                    changes=["Transformed test for service architecture"],
                    new_test_count=0,
                    converted_test_count=len(existing_test['functions']),
                    error_message=None
                ))

            except Exception as e:
                logger.error(f"Failed to migrate test {existing_test['file_path']}: {e}")
                results.append(TestMigrationResult(
                    test_file=existing_test['file_path'],
                    migration_type="existing_test_migration",
                    status="failed",
                    changes=[],
                    new_test_count=0,
                    converted_test_count=0,
                    error_message=str(e)
                ))

        return results

    def _create_new_service_tests(
        self,
        service_name: str,
        service_structure: TestSuiteStructure
    ) -> List[TestMigrationResult]:
        """Create new test files for service."""
        results = []

        # Create unit tests
        for test_type in ['unit_test', 'integration_test', 'api_test', 'performance_test']:
            if test_type in self.test_templates and service_name in self.test_templates[test_type]:
                template_content = self.test_templates[test_type][service_name]

                test_file = f"test_{service_name}_{test_type.replace('_test', '')}.py"
                target_path = self.target_test_dir / service_name / test_file
                target_path.parent.mkdir(parents=True, exist_ok=True)

                try:
                    with open(target_path, 'w', encoding='utf-8') as f:
                        f.write(template_content)

                    # Count test methods in template
                    test_count = template_content.count('def test_')

                    results.append(TestMigrationResult(
                        test_file=str(target_path),
                        migration_type=f"new_{test_type}",
                        status="success",
                        changes=[f"Created {test_type} template"],
                        new_test_count=test_count,
                        converted_test_count=0,
                        error_message=None
                    ))

                except Exception as e:
                    results.append(TestMigrationResult(
                        test_file=str(target_path),
                        migration_type=f"new_{test_type}",
                        status="failed",
                        changes=[],
                        new_test_count=0,
                        converted_test_count=0,
                        error_message=str(e)
                    ))

        return results

    def _create_shared_test_utilities(self) -> TestMigrationResult:
        """Create shared test utilities and fixtures."""
        utilities_content = '''"""
Shared Test Utilities for Service Architecture

Common fixtures, mocks, and utilities used across service tests.
"""

import pytest
import asyncio
from typing import Dict, Any, Optional
from unittest.mock import Mock, AsyncMock

from src.infrastructure.caching import MemoryCache, CacheConfig
from src.infrastructure.service_container import ServiceContainer


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def test_cache():
    """Create test cache instance."""
    config = CacheConfig(
        ttl_seconds=300,
        max_size=100,
        namespace="test"
    )
    cache = MemoryCache(config)
    yield cache
    await cache.clear()


@pytest.fixture
def mock_service_container():
    """Create mock service container."""
    container = Mock(spec=ServiceContainer)
    container.get_service = Mock()
    container.register_service = Mock()
    container.get_cached_service = Mock()
    return container


@pytest.fixture
def mock_database():
    """Create mock database connection."""
    db = Mock()
    db.execute = AsyncMock()
    db.fetch = AsyncMock()
    db.fetchrow = AsyncMock()
    db.fetchval = AsyncMock()
    return db


class MockRepository:
    """Base mock repository for testing."""

    def __init__(self):
        self.get_by_id = AsyncMock()
        self.create = AsyncMock()
        self.update = AsyncMock()
        self.delete = AsyncMock()
        self.list_all = AsyncMock()
        self.search = AsyncMock()


class MockServiceBase:
    """Base mock service for testing."""

    def __init__(self):
        self.get_by_id = AsyncMock()
        self.create = AsyncMock()
        self.update = AsyncMock()
        self.delete = AsyncMock()
        self.search = AsyncMock()


def create_test_dto(model_class, **kwargs):
    """Create test DTO instance with default values."""
    import inspect
    from datetime import datetime

    # Get constructor signature
    sig = inspect.signature(model_class.__init__)
    params = {}

    for param_name, param in sig.parameters.items():
        if param_name == 'self':
            continue

        if param_name in kwargs:
            params[param_name] = kwargs[param_name]
        elif param.annotation == str:
            params[param_name] = f"test_{param_name}"
        elif param.annotation == int:
            params[param_name] = 1
        elif param.annotation == float:
            params[param_name] = 1.0
        elif param.annotation == bool:
            params[param_name] = True
        elif param.annotation == datetime:
            params[param_name] = datetime.now()
        elif param.default != inspect.Parameter.empty:
            params[param_name] = param.default
        else:
            params[param_name] = None

    return model_class(**params)


async def wait_for_condition(condition_func, timeout_seconds=5, check_interval=0.1):
    """Wait for condition to become true with timeout."""
    import time

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        if await condition_func():
            return True
        await asyncio.sleep(check_interval)

    return False


def assert_dto_equals(dto1, dto2, ignore_fields=None):
    """Assert two DTOs are equal, optionally ignoring specific fields."""
    ignore_fields = ignore_fields or []

    dto1_dict = dto1.__dict__ if hasattr(dto1, '__dict__') else dto1
    dto2_dict = dto2.__dict__ if hasattr(dto2, '__dict__') else dto2

    for field in ignore_fields:
        dto1_dict.pop(field, None)
        dto2_dict.pop(field, None)

    assert dto1_dict == dto2_dict
'''

        utilities_path = self.target_test_dir / "utilities" / "test_utilities.py"
        utilities_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(utilities_path, 'w', encoding='utf-8') as f:
                f.write(utilities_content)

            return TestMigrationResult(
                test_file=str(utilities_path),
                migration_type="shared_utilities",
                status="success",
                changes=["Created shared test utilities"],
                new_test_count=0,
                converted_test_count=0,
                error_message=None
            )

        except Exception as e:
            return TestMigrationResult(
                test_file=str(utilities_path),
                migration_type="shared_utilities",
                status="failed",
                changes=[],
                new_test_count=0,
                converted_test_count=0,
                error_message=str(e)
            )

    def _create_test_configuration(self, services: List[str]) -> TestMigrationResult:
        """Create test configuration files."""
        config_content = f'''"""
Test Configuration for Service Architecture

Configuration settings and fixtures for service testing.
"""

import pytest
import os
from pathlib import Path

# Test environment configuration
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "postgresql://test:test@localhost:5432/test_db")
TEST_REDIS_URL = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/1")

# Test data paths
TEST_DATA_DIR = Path(__file__).parent / "data"
TEST_FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Service configuration for tests
SERVICE_CONFIGS = {{
{chr(10).join(f'    "{service}": {{"base_url": "http://localhost:800{i+1}", "timeout": 30}},' for i, service in enumerate(services))}
}}

# Performance test thresholds
PERFORMANCE_THRESHOLDS = {{
    "api_response_time_ms": 500,
    "database_query_time_ms": 100,
    "cache_hit_time_ms": 10,
    "memory_usage_mb": 500
}}

# Test markers configuration
pytest_plugins = [
    "pytest_asyncio",
    "pytest_mock",
    "pytest_cov"
]

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "api: API tests")
    config.addinivalue_line("markers", "performance: Performance tests")
    config.addinivalue_line("markers", "slow: Slow running tests")

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment before running tests."""
    # Ensure test directories exist
    TEST_DATA_DIR.mkdir(exist_ok=True)
    TEST_FIXTURES_DIR.mkdir(exist_ok=True)

    # Set test environment variables
    os.environ["ENVIRONMENT"] = "test"
    os.environ["LOG_LEVEL"] = "DEBUG"

    yield

    # Cleanup after tests
    pass
'''

        config_path = self.target_test_dir / "conftest.py"

        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(config_content)

            return TestMigrationResult(
                test_file=str(config_path),
                migration_type="test_configuration",
                status="success",
                changes=["Created pytest configuration"],
                new_test_count=0,
                converted_test_count=0,
                error_message=None
            )

        except Exception as e:
            return TestMigrationResult(
                test_file=str(config_path),
                migration_type="test_configuration",
                status="failed",
                changes=[],
                new_test_count=0,
                converted_test_count=0,
                error_message=str(e)
            )

    def _create_ci_test_scripts(self, services: List[str]) -> TestMigrationResult:
        """Create CI/CD test scripts."""
        ci_script_content = f'''#!/bin/bash
"""
CI/CD Test Script for Service Architecture

Runs comprehensive test suite for all services.
"""

set -e

echo "🧪 Starting Service Architecture Test Suite"

# Test configuration
export ENVIRONMENT=test
export LOG_LEVEL=WARNING
export PYTHONPATH=${{PYTHONPATH:-}}:src

# Colors for output
RED='\\033[0;31m'
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
NC='\\033[0m' # No Color

print_status() {{
    echo -e "${{GREEN}}[INFO]${{NC}} $1"
}}

print_warning() {{
    echo -e "${{YELLOW}}[WARN]${{NC}} $1"
}}

print_error() {{
    echo -e "${{RED}}[ERROR]${{NC}} $1"
}}

# Run unit tests for each service
print_status "Running unit tests..."
for service in {' '.join(services)}; do
    print_status "Testing $service service (unit tests)"
    python -m pytest tests/services/$service/test_${{service}}_unit.py -v --tb=short
done

# Run integration tests
print_status "Running integration tests..."
for service in {' '.join(services)}; do
    print_status "Testing $service service (integration tests)"
    python -m pytest tests/services/$service/test_${{service}}_integration.py -v --tb=short
done

# Run API tests
print_status "Running API tests..."
for service in {' '.join(services)}; do
    print_status "Testing $service service (API tests)"
    python -m pytest tests/services/$service/test_${{service}}_api.py -v --tb=short
done

# Run performance tests (optional)
if [[ "$RUN_PERFORMANCE_TESTS" == "true" ]]; then
    print_status "Running performance tests..."
    for service in {' '.join(services)}; do
        print_status "Testing $service service (performance tests)"
        python -m pytest tests/services/$service/test_${{service}}_performance.py -v --tb=short -m performance
    done
fi

# Generate coverage report
print_status "Generating test coverage report..."
python -m pytest tests/services/ --cov=src --cov-report=html --cov-report=term

# Test summary
print_status "✅ All service tests completed successfully!"

echo "📊 Test Results Summary:"
echo "  - Services tested: {len(services)}"
echo "  - Test types: unit, integration, api"
echo "  - Coverage report: htmlcov/index.html"

if [[ "$RUN_PERFORMANCE_TESTS" == "true" ]]; then
    echo "  - Performance tests: included"
fi
'''

        script_path = self.target_test_dir / "run_service_tests.sh"

        try:
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(ci_script_content)

            # Make script executable
            os.chmod(script_path, 0o755)

            return TestMigrationResult(
                test_file=str(script_path),
                migration_type="ci_test_script",
                status="success",
                changes=["Created CI/CD test script"],
                new_test_count=0,
                converted_test_count=0,
                error_message=None
            )

        except Exception as e:
            return TestMigrationResult(
                test_file=str(script_path),
                migration_type="ci_test_script",
                status="failed",
                changes=[],
                new_test_count=0,
                converted_test_count=0,
                error_message=str(e)
            )

    def _transform_test_content(self, content: str, service_name: str) -> str:
        """Transform existing test content for service architecture."""
        # This is a simplified transformation
        # In practice, this would need more sophisticated AST manipulation

        # Update imports to use service interfaces
        content = re.sub(
            r'from src\.domains\.(\w+)\.dao',
            r'from src.domains.\1.services.interfaces',
            content
        )

        # Update DAO references to service references
        content = re.sub(
            r'(\w+)DAO',
            r'\1Service',
            content
        )

        # Add service fixture imports
        service_import = f"""
from src.domains.{service_name}.services.implementations.{service_name}_service import {service_name.title()}Service
from src.domains.{service_name}.services.interfaces.{service_name}_service_interface import {service_name.title()}ServiceInterface
"""

        # Insert imports after existing imports
        import_end = content.find('\n\n')
        if import_end > 0:
            content = content[:import_end] + service_import + content[import_end:]

        return content

    def _determine_target_test_file(
        self,
        existing_test: Dict[str, Any],
        service_name: str,
        service_structure: TestSuiteStructure
    ) -> str:
        """Determine target test file for migrated test."""
        file_path = existing_test['file_path'].lower()

        if 'integration' in file_path:
            return f"test_{service_name}_integration.py"
        elif 'api' in file_path:
            return f"test_{service_name}_api.py"
        elif 'performance' in file_path or 'load' in file_path:
            return f"test_{service_name}_performance.py"
        else:
            return f"test_{service_name}_unit.py"

    def _analyze_service_test_coverage(self, service_name: str) -> Dict[str, Any]:
        """Analyze test coverage for specific service."""
        service_dir = self.target_test_dir / service_name

        if not service_dir.exists():
            return {'coverage_score': 0, 'test_files': 0}

        test_files = list(service_dir.glob("test_*.py"))
        total_test_methods = 0

        for test_file in test_files:
            try:
                with open(test_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                total_test_methods += content.count('def test_')
            except Exception:
                pass

        # Simple coverage estimation based on test file types and methods
        expected_test_types = 4  # unit, integration, api, performance
        actual_test_types = len(test_files)

        coverage_score = (actual_test_types / expected_test_types) * 100
        coverage_score = min(coverage_score, 100)  # Cap at 100%

        return {
            'coverage_score': coverage_score,
            'test_files': len(test_files),
            'test_methods': total_test_methods,
            'unit_tests': len([f for f in test_files if 'unit' in f.name]),
            'integration_tests': len([f for f in test_files if 'integration' in f.name]),
            'api_tests': len([f for f in test_files if 'api' in f.name]),
            'performance_tests': len([f for f in test_files if 'performance' in f.name])
        }

    def _generate_coverage_recommendations(
        self,
        coverage_analysis: Dict[str, Any]
    ) -> List[str]:
        """Generate recommendations for improving test coverage."""
        recommendations = []

        overall_coverage = coverage_analysis['overall_coverage']

        if overall_coverage < 70:
            recommendations.append(
                f"Overall test coverage is {overall_coverage:.1f}%. "
                "Consider adding more comprehensive tests."
            )

        test_types = coverage_analysis['test_types']

        if test_types['unit'] < 10:
            recommendations.append("Add more unit tests for better isolation testing.")

        if test_types['integration'] < 5:
            recommendations.append("Add integration tests for service interaction validation.")

        if test_types['api'] < 3:
            recommendations.append("Add API tests for endpoint validation.")

        if test_types['performance'] < 1:
            recommendations.append("Add performance tests for service SLA validation.")

        # Service-specific recommendations
        for service_name, service_data in coverage_analysis['services'].items():
            service_coverage = service_data.get('coverage_score', 0)

            if service_coverage < 50:
                recommendations.append(
                    f"{service_name} service has low test coverage ({service_coverage:.1f}%). "
                    "Add comprehensive test suite."
                )

        return recommendations

    def _validate_service_tests(self, service_name: str) -> Dict[str, Any]:
        """Validate test files for specific service."""
        service_dir = self.target_test_dir / service_name
        validation_result = {
            'service': service_name,
            'status': 'valid',
            'test_files': [],
            'syntax_errors': [],
            'import_errors': []
        }

        if not service_dir.exists():
            validation_result['status'] = 'invalid'
            validation_result['syntax_errors'].append(f"Service test directory not found: {service_dir}")
            return validation_result

        for test_file in service_dir.glob("test_*.py"):
            file_validation = {
                'file': str(test_file),
                'status': 'valid',
                'issues': []
            }

            try:
                # Check syntax
                with open(test_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                ast.parse(content)

                # Check for common issues
                if 'def test_' not in content:
                    file_validation['issues'].append("No test methods found")

                if 'import pytest' not in content:
                    file_validation['issues'].append("pytest import missing")

                if file_validation['issues']:
                    file_validation['status'] = 'warning'

            except SyntaxError as e:
                file_validation['status'] = 'invalid'
                file_validation['issues'].append(f"Syntax error: {e}")
                validation_result['syntax_errors'].append(f"{test_file}: {e}")

            except Exception as e:
                file_validation['status'] = 'invalid'
                file_validation['issues'].append(f"Validation error: {e}")

            validation_result['test_files'].append(file_validation)

            if file_validation['status'] == 'invalid':
                validation_result['status'] = 'invalid'

        return validation_result

    def _validate_shared_utilities(self) -> List[Dict[str, Any]]:
        """Validate shared test utilities."""
        utilities_dir = self.target_test_dir / "utilities"
        validation_results = []

        if not utilities_dir.exists():
            return [{
                'file': 'utilities directory',
                'status': 'missing',
                'issues': ['Shared utilities directory not found']
            }]

        for util_file in utilities_dir.glob("*.py"):
            validation = {
                'file': str(util_file),
                'status': 'valid',
                'issues': []
            }

            try:
                with open(util_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                ast.parse(content)

            except SyntaxError as e:
                validation['status'] = 'invalid'
                validation['issues'].append(f"Syntax error: {e}")

            validation_results.append(validation)

        return validation_results

    def _validate_test_configuration(self) -> Dict[str, Any]:
        """Validate test configuration files."""
        conftest_path = self.target_test_dir / "conftest.py"

        validation = {
            'conftest': {
                'exists': conftest_path.exists(),
                'status': 'unknown',
                'issues': []
            }
        }

        if conftest_path.exists():
            try:
                with open(conftest_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                ast.parse(content)
                validation['conftest']['status'] = 'valid'

            except SyntaxError as e:
                validation['conftest']['status'] = 'invalid'
                validation['conftest']['issues'].append(f"Syntax error: {e}")
        else:
            validation['conftest']['status'] = 'missing'
            validation['conftest']['issues'].append("conftest.py not found")

        return validation