"""
Integration tests for Market Data Service architecture.

Tests end-to-end functionality including:
- Market data service layer integration
- Daily prices and fundamentals management
- Price analytics and validation
- Service container functionality
"""

import pytest
import asyncio
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import List
from unittest.mock import Mock, AsyncMock

from domains.market_data.services.interfaces.market_data_service_interface import (
    MarketDataServiceInterface,
    DailyPriceDTO,
    FundamentalDTO,
    MarketDataSearchCriteria,
    PriceAnalysisResult,
    MarketDataOperationResult
)
from domains.market_data.services.config.market_data_service_container import get_market_data_service
from core.platform.config.environment import Environment, EnvironmentType


class TestMarketDataServiceArchitecture:
    """Test market data service architecture components work correctly"""
    
    @pytest.fixture
    def test_environment(self):
        """Create test environment"""
        return Environment(None, EnvironmentType.DEV)
    
    @pytest.fixture
    def sample_daily_price(self):
        """Sample daily price for testing"""
        return DailyPriceDTO(
            symbol="AAPL",
            date=date.today(),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.00"),
            close=Decimal("153.50"),
            volume=1000000,
            instrument_id=1
        )
    
    @pytest.fixture
    def sample_fundamental(self):
        """Sample fundamental data for testing"""
        return FundamentalDTO(
            instrument_id=1,
            date=date.today(),
            market_cap=2500000000000,  # $2.5T
            pe_ratio=Decimal("25.5"),
            eps=Decimal("6.15"),
            revenue=365000000000,  # $365B
            profit=95000000000     # $95B
        )
    
    def test_service_interface_compliance(self):
        """Test service implements interface correctly"""
        from domains.market_data.services.impl.market_data_service_impl import MarketDataServiceImpl
        
        # Test implementation implements interface
        assert issubclass(MarketDataServiceImpl, MarketDataServiceInterface)
        
        # Test all interface methods exist
        interface_methods = [
            'create_daily_price',
            'get_daily_price_by_id',
            'get_daily_price',
            'list_daily_prices',
            'update_daily_price',
            'create_daily_prices_batch',
            'get_price_history',
            'create_fundamental',
            'get_fundamental_by_id',
            'get_fundamental',
            'list_fundamentals',
            'create_fundamentals_batch',
            'calculate_returns',
            'calculate_volatility',
            'analyze_price_performance',
            'get_correlation_matrix',
            'validate_price_data',
            'detect_price_anomalies',
            'get_data_coverage_report',
            'consolidate_vendor_data',
            'get_vendor_comparison',
            'sync_vendor_data',
            'get_market_summary',
            'get_top_performers',
            'get_market_breadth',
            'export_price_data',
            'get_ohlc_data'
        ]
        
        for method_name in interface_methods:
            assert hasattr(MarketDataServiceImpl, method_name)
            assert callable(getattr(MarketDataServiceImpl, method_name))
    
    def test_dto_models_work(self):
        """Test DTO models work correctly"""
        
        # Test DailyPriceDTO
        price = DailyPriceDTO(
            symbol="TSLA",
            date=date.today(),
            open=Decimal("800.00"),
            high=Decimal("820.00"),
            low=Decimal("795.00"),
            close=Decimal("815.50"),
            volume=500000
        )
        assert price.symbol == "TSLA"
        assert price.open == Decimal("800.00")
        assert price.volume == 500000
        
        # Test FundamentalDTO
        fundamental = FundamentalDTO(
            instrument_id=2,
            date=date.today(),
            market_cap=800000000000,
            pe_ratio=Decimal("50.2"),
            eps=Decimal("8.75")
        )
        assert fundamental.instrument_id == 2
        assert fundamental.pe_ratio == Decimal("50.2")
        
        # Test MarketDataSearchCriteria
        criteria = MarketDataSearchCriteria(
            symbols=["AAPL", "TSLA"],
            start_date=date.today() - timedelta(days=30),
            end_date=date.today(),
            limit=100
        )
        assert criteria.symbols == ["AAPL", "TSLA"]
        assert criteria.limit == 100
        
        # Test PriceAnalysisResult
        analysis = PriceAnalysisResult(
            symbol="GOOGL",
            start_date=date.today() - timedelta(days=30),
            end_date=date.today(),
            total_return=Decimal("0.15"),
            volatility=Decimal("0.25")
        )
        assert analysis.symbol == "GOOGL"
        assert analysis.total_return == Decimal("0.15")
        
        # Test MarketDataOperationResult
        result = MarketDataOperationResult(
            success=True,
            created_count=5,
            skipped_count=1
        )
        assert result.success is True
        assert result.created_count == 5
    
    def test_service_container_integration(self):
        """Test service container integrates correctly"""
        from domains.market_data.services.config.market_data_service_container import (
            MarketDataServiceContainer,
            get_market_data_service_container,
            get_market_data_service
        )
        from unittest.mock import Mock
        
        # Create mock environment to avoid database dependency issues
        mock_env = Mock()
        mock_env.env_type = EnvironmentType.DEV
        mock_env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test"
        mock_env.get_table_name.side_effect = lambda table: f"test_{table}"
        
        container = MarketDataServiceContainer(mock_env)
        
        assert container is not None
        assert container.environment == mock_env
        assert not container._initialized
        
        # Test health status
        health = container.get_health_status()
        assert "initialized" in health
        assert "environment" in health
        assert health["status"] == "not_initialized"
    
    def test_import_paths_work(self):
        """Test all service imports work correctly"""
        
        # Test service interface import
        from domains.market_data.services.interfaces.market_data_service_interface import MarketDataServiceInterface
        assert MarketDataServiceInterface is not None
        
        # Test service implementation import  
        from domains.market_data.services.impl.market_data_service_impl import MarketDataServiceImpl
        assert MarketDataServiceImpl is not None
        
        # Test service container import
        from domains.market_data.services.config.market_data_service_container import get_market_data_service
        assert get_market_data_service is not None


class TestMarketDataServiceLogic:
    """Test market data service business logic"""
    
    @pytest.fixture
    def test_environment(self):
        """Create test environment"""
        return Environment(None, EnvironmentType.DEV)
    
    def test_price_validation_logic(self):
        """Test price validation business logic"""
        from domains.market_data.services.impl.market_data_service_impl import MarketDataServiceImpl
        from unittest.mock import Mock
        
        # Create service with mock DAOs
        daily_prices_dao = Mock()
        fundamentals_dao = Mock()
        service = MarketDataServiceImpl(daily_prices_dao, fundamentals_dao)
        
        # Test valid price validation
        valid_price = DailyPriceDTO(
            symbol="AAPL",
            date=date.today(),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.00"),
            close=Decimal("153.50"),
            volume=1000000,
            instrument_id=1
        )
        
        validation_result = asyncio.run(service.validate_price_data(valid_price))
        assert validation_result["valid"] is True
        assert len(validation_result["issues"]) == 0
        assert validation_result["data_quality_score"] == 1.0
        
        # Test invalid price validation
        invalid_price = DailyPriceDTO(
            symbol="",  # Missing symbol
            date=None,  # Missing date
            open=Decimal("-10.00"),  # Negative price
            high=Decimal("100.00"),
            low=Decimal("150.00"),  # Low > High (invalid)
            close=Decimal("120.00"),
            volume=-500,  # Negative volume
            instrument_id=None  # Missing instrument_id
        )
        
        validation_result = asyncio.run(service.validate_price_data(invalid_price))
        assert validation_result["valid"] is False
        assert len(validation_result["issues"]) > 0
        assert validation_result["data_quality_score"] < 1.0
    
    def test_dto_conversion_logic(self):
        """Test DAO to DTO conversion logic"""
        from domains.market_data.services.impl.market_data_service_impl import MarketDataServiceImpl
        from unittest.mock import Mock
        
        # Create service with mock DAOs
        daily_prices_dao = Mock()
        fundamentals_dao = Mock()
        service = MarketDataServiceImpl(daily_prices_dao, fundamentals_dao)
        
        # Test daily price DAO to DTO conversion
        dao_price = {
            'id': 1,
            'symbol': 'MSFT',
            'date': date.today(),
            'open': Decimal('300.00'),
            'high': Decimal('305.00'),
            'low': Decimal('298.00'),
            'close': Decimal('302.50'),
            'volume': 2000000,
            'instrument_id': 3,
            'vendor': 'polygon'
        }
        
        price_dto = service._dao_to_daily_price_dto(dao_price)
        assert isinstance(price_dto, DailyPriceDTO)
        assert price_dto.id == 1
        assert price_dto.symbol == 'MSFT'
        assert price_dto.open == Decimal('300.00')
        assert price_dto.volume == 2000000
        assert price_dto.vendor == 'polygon'
        
        # Test fundamental DAO to DTO conversion
        dao_fundamental = {
            'id': 2,
            'instrument_id': 3,
            'date': date.today(),
            'market_cap': 2000000000000,
            'pe_ratio': Decimal('30.5'),
            'eps': Decimal('10.25'),
            'revenue': 200000000000,
            'profit': 60000000000
        }
        
        fundamental_dto = service._dao_to_fundamental_dto(dao_fundamental)
        assert isinstance(fundamental_dto, FundamentalDTO)
        assert fundamental_dto.id == 2
        assert fundamental_dto.instrument_id == 3
        assert fundamental_dto.market_cap == 2000000000000
        assert fundamental_dto.pe_ratio == Decimal('30.5')
        assert fundamental_dto.eps == Decimal('10.25')
    
    def test_business_logic_error_handling(self):
        """Test business logic handles errors gracefully"""
        from domains.market_data.services.impl.market_data_service_impl import MarketDataServiceImpl
        from unittest.mock import Mock, AsyncMock
        
        # Create service with mock DAOs that raise exceptions
        daily_prices_dao = Mock()
        daily_prices_dao.insert_price = AsyncMock(side_effect=Exception("Database error"))
        
        fundamentals_dao = Mock()
        fundamentals_dao.insert_fundamental = AsyncMock(side_effect=Exception("Database error"))
        
        service = MarketDataServiceImpl(daily_prices_dao, fundamentals_dao)
        
        # Test price creation error handling
        test_price = DailyPriceDTO(
            symbol="TEST",
            date=date.today(),
            open=Decimal("100.00"),
            high=Decimal("105.00"),
            low=Decimal("99.00"),
            close=Decimal("103.00"),
            volume=10000,
            instrument_id=1
        )
        
        result = asyncio.run(service.create_daily_price(test_price))
        assert result.success is False
        assert result.error_message is not None
        assert "Database error" in result.error_message
        
        # Test fundamental creation error handling
        test_fundamental = FundamentalDTO(
            instrument_id=1,
            date=date.today(),
            market_cap=1000000000
        )
        
        result = asyncio.run(service.create_fundamental(test_fundamental))
        assert result.success is False
        assert result.error_message is not None
    
    def test_price_analytics_logic(self):
        """Test price analytics calculations"""
        from domains.market_data.services.impl.market_data_service_impl import MarketDataServiceImpl
        from unittest.mock import Mock, AsyncMock
        
        # Create service with mock DAOs
        daily_prices_dao = Mock()
        fundamentals_dao = Mock()
        service = MarketDataServiceImpl(daily_prices_dao, fundamentals_dao)
        
        # Test anomaly detection logic directly with sample data (need at least 5 prices)
        sample_prices = [
            DailyPriceDTO(symbol="TEST", date=date.today() - timedelta(days=6), 
                         open=Decimal("100"), high=Decimal("105"), low=Decimal("99"), close=Decimal("102"), volume=1000),
            DailyPriceDTO(symbol="TEST", date=date.today() - timedelta(days=5), 
                         open=Decimal("101"), high=Decimal("106"), low=Decimal("100"), close=Decimal("103"), volume=1200),
            DailyPriceDTO(symbol="TEST", date=date.today() - timedelta(days=4), 
                         open=Decimal("102"), high=Decimal("107"), low=Decimal("101"), close=Decimal("104"), volume=800),
            DailyPriceDTO(symbol="TEST", date=date.today() - timedelta(days=3), 
                         open=Decimal("103"), high=Decimal("108"), low=Decimal("102"), close=Decimal("105"), volume=900),
            DailyPriceDTO(symbol="TEST", date=date.today() - timedelta(days=2), 
                         open=Decimal("104"), high=Decimal("109"), low=Decimal("103"), close=Decimal("106"), volume=1100),
            DailyPriceDTO(symbol="TEST", date=date.today() - timedelta(days=1), 
                         open=Decimal("130"), high=Decimal("135"), low=Decimal("128"), close=Decimal("132"), volume=0),  # Gap + zero volume
            DailyPriceDTO(symbol="TEST", date=date.today(), 
                         open=Decimal("131"), high=Decimal("134"), low=Decimal("130"), close=Decimal("133"), volume=500)
        ]
        
        # Mock get_price_history as an AsyncMock
        service.get_price_history = AsyncMock(return_value=sample_prices)
        
        # Test anomaly detection
        anomalies = asyncio.run(service.detect_price_anomalies("TEST", date.today() - timedelta(days=6), date.today()))
        
        # Should detect price gap and zero volume
        anomaly_types = [a['type'] for a in anomalies]
        assert 'price_gap' in anomaly_types  # Large gap from 103 to 130
        assert 'zero_volume' in anomaly_types  # Zero volume on middle day


class TestMarketDataServiceMigrationValidation:
    """Validate the market data service migration is working correctly"""
    
    def test_market_data_migration_completeness(self):
        """Test migration touched all necessary components"""
        
        # Test files exist
        import os
        
        service_files = [
            'src/domains/market_data/services/interfaces/market_data_service_interface.py',
            'src/domains/market_data/services/impl/market_data_service_impl.py',
            'src/domains/market_data/services/config/market_data_service_container.py'
        ]
        
        for file_path in service_files:
            full_path = os.path.join('/home/jianjun/ats-genai-data', file_path)
            assert os.path.exists(full_path), f"Market data service file missing: {file_path}"
    
    def test_market_data_service_patterns(self):
        """Test market data service follows proper architectural patterns"""
        from domains.market_data.services.impl.market_data_service_impl import MarketDataServiceImpl
        from unittest.mock import Mock
        
        # Create service with mock DAOs
        daily_prices_dao = Mock()
        fundamentals_dao = Mock()
        service = MarketDataServiceImpl(daily_prices_dao, fundamentals_dao)
        
        # Test service coordinates DAOs
        assert hasattr(service, 'daily_prices_dao')
        assert hasattr(service, 'fundamentals_dao')
        
        # Test service provides proper methods
        assert hasattr(service, 'create_daily_price')
        assert hasattr(service, 'create_fundamental')
        assert hasattr(service, 'validate_price_data')
        
        # Test service has conversion methods
        assert hasattr(service, '_dao_to_daily_price_dto')
        assert hasattr(service, '_dao_to_fundamental_dto')
    
    def test_market_data_service_business_logic_patterns(self):
        """Test market data service implements business logic patterns correctly"""
        from domains.market_data.services.impl.market_data_service_impl import MarketDataServiceImpl
        from unittest.mock import Mock, AsyncMock
        
        # Create service with mock DAOs
        daily_prices_dao = Mock()
        daily_prices_dao.insert_price = AsyncMock(return_value=None)
        
        fundamentals_dao = Mock()
        fundamentals_dao.insert_fundamental = AsyncMock(return_value=None)
        
        service = MarketDataServiceImpl(daily_prices_dao, fundamentals_dao)
        
        # Test business validation patterns
        invalid_price = DailyPriceDTO(
            date=None,  # Missing required field
            instrument_id=None  # Missing required field
        )
        result = asyncio.run(service.create_daily_price(invalid_price))
        
        assert result.success is False
        assert "Invalid price data" in result.error_message
        
        # Test successful business operation patterns
        valid_price = DailyPriceDTO(
            symbol="AAPL",
            date=date.today(),
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.00"),
            close=Decimal("153.50"),
            volume=1000000,
            instrument_id=1
        )
        result = asyncio.run(service.create_daily_price(valid_price))
        
        # Should succeed with mock DAO
        assert result.success is True
        assert result.created_count == 1
    
    def test_market_data_analytics_integration(self):
        """Test market data analytics functionality is properly integrated"""
        from domains.market_data.services.impl.market_data_service_impl import MarketDataServiceImpl
        from unittest.mock import Mock
        
        # Create service with mock DAOs
        daily_prices_dao = Mock()
        fundamentals_dao = Mock()
        service = MarketDataServiceImpl(daily_prices_dao, fundamentals_dao)
        
        # Test analytics methods exist and have proper signatures
        analytics_methods = [
            'calculate_returns',
            'calculate_volatility', 
            'analyze_price_performance',
            'get_correlation_matrix',
            'detect_price_anomalies'
        ]
        
        for method_name in analytics_methods:
            assert hasattr(service, method_name)
            method = getattr(service, method_name)
            assert callable(method)
            # Check it's an async method
            assert asyncio.iscoroutinefunction(method)
    
    def test_market_data_export_capabilities(self):
        """Test market data export functionality"""
        from domains.market_data.services.impl.market_data_service_impl import MarketDataServiceImpl
        from unittest.mock import Mock
        import pandas as pd
        
        # Create service with mock DAOs
        daily_prices_dao = Mock()
        fundamentals_dao = Mock()
        service = MarketDataServiceImpl(daily_prices_dao, fundamentals_dao)
        
        # Mock list_daily_prices to return sample data
        sample_prices = [
            DailyPriceDTO(symbol="AAPL", date=date.today(), 
                         open=Decimal("150"), high=Decimal("155"), low=Decimal("149"), close=Decimal("153"), volume=1000),
            DailyPriceDTO(symbol="TSLA", date=date.today(),
                         open=Decimal("800"), high=Decimal("820"), low=Decimal("795"), close=Decimal("815"), volume=500)
        ]
        
        async def mock_list_daily_prices(criteria):
            return sample_prices
        
        service.list_daily_prices = mock_list_daily_prices
        
        # Test export to DataFrame
        criteria = MarketDataSearchCriteria(symbols=["AAPL", "TSLA"], limit=10)
        df = asyncio.run(service.export_price_data(criteria, "dataframe"))
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'symbol' in df.columns
        assert 'open' in df.columns
        assert 'close' in df.columns
        
        # Test export to CSV
        csv_data = asyncio.run(service.export_price_data(criteria, "csv"))
        assert isinstance(csv_data, str)
        assert 'symbol' in csv_data
        assert 'AAPL' in csv_data
        assert 'TSLA' in csv_data


if __name__ == "__main__":
    # Run with: pytest tests/integration/test_market_data_service_integration.py -v
    pytest.main([__file__, "-v", "--tb=short"])