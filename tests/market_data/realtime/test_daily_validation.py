#!/usr/bin/env python3
"""
Comprehensive tests for the Daily Validation Engine

Tests cover:
- Daily validation workflow
- Real-time vs batch data comparison
- Vendor API integration
- Validation result storage
- Quality scoring algorithms
- Error handling and edge cases
"""

import pytest
import asyncio
import asyncpg
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta, date, timezone
import json
import os
import aiohttp

# Import the module under test
import sys
sys.path.append('src')

from market_data.realtime.daily_validation import (
    DailyValidationEngine,
    ValidationResult
)

class TestValidationResult:
    """Test the ValidationResult data structure"""
    
    def test_validation_result_creation(self):
        """Test creating ValidationResult with all fields"""
        validation_date = date.today()
        result = ValidationResult(
            symbol='AAPL',
            vendor='polygon',
            validation_date=validation_date,
            realtime_bars_count=390,
            batch_bars_count=390,
            missing_realtime_bars=0,
            discrepant_prices=2,
            avg_price_difference=0.001,
            max_price_difference=0.005,
            avg_data_latency_minutes=1.5,
            max_data_latency_minutes=3.0,
            late_bars_count=5,
            realtime_quality_score=0.95,
            batch_quality_score=1.0,
            overall_accuracy_score=0.99,
            validation_status='passed',
            validation_notes='All checks passed'
        )
        
        assert result.symbol == 'AAPL'
        assert result.vendor == 'polygon'
        assert result.validation_date == validation_date
        assert result.realtime_bars_count == 390
        assert result.batch_bars_count == 390
        assert result.missing_realtime_bars == 0
        assert result.discrepant_prices == 2
        assert result.avg_price_difference == 0.001
        assert result.max_price_difference == 0.005
        assert result.avg_data_latency_minutes == 1.5
        assert result.max_data_latency_minutes == 3.0
        assert result.late_bars_count == 5
        assert result.realtime_quality_score == 0.95
        assert result.batch_quality_score == 1.0
        assert result.overall_accuracy_score == 0.99
        assert result.validation_status == 'passed'
        assert result.validation_notes == 'All checks passed'

class TestDailyValidationEngine:
    """Test the main DailyValidationEngine class"""
    
    @pytest.fixture
    def mock_env(self):
        """Mock environment configuration"""
        with patch('market_data.realtime.daily_validation.Environment') as mock_env_class:
            mock_env = Mock()
            mock_env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test"
            mock_env_class.return_value = mock_env
            yield mock_env
    
    @pytest.fixture
    def validation_engine(self, mock_env):
        """Create a validation engine instance with mocked dependencies"""
        with patch.dict(os.environ, {
            'VALIDATION_DATE': '2025-01-15',
            'MAX_VALIDATION_SYMBOLS': '50',
            'PRICE_TOLERANCE_PERCENT': '0.01',
            'ENABLE_SLACK_ALERTS': 'true',
            'POLYGON_API_KEY': 'test_polygon_key',
            'TIINGO_API_KEY': 'test_tiingo_key',
            'FMP_API_KEY': 'test_fmp_key'
        }):
            with patch('market_data.realtime.daily_validation.get_previous_trading_day', 
                      return_value=date(2025, 1, 15)):
                engine = DailyValidationEngine()
                return engine
    
    def test_engine_initialization(self, validation_engine):
        """Test validation engine initialization"""
        assert validation_engine.validation_date == date(2025, 1, 15)
        assert validation_engine.max_symbols == 50
        assert validation_engine.price_tolerance == 0.01
        assert validation_engine.enable_alerts is True
        assert validation_engine.polygon_api_key == 'test_polygon_key'
        assert validation_engine.tiingo_api_key == 'test_tiingo_key'
        assert validation_engine.fmp_api_key == 'test_fmp_key'
    
    def test_get_validation_date_yesterday(self):
        """Test getting validation date when set to 'yesterday'"""
        with patch.dict(os.environ, {'VALIDATION_DATE': 'yesterday'}):
            with patch('market_data.realtime.daily_validation.get_previous_trading_day', 
                      return_value=date(2025, 1, 14)):
                with patch('market_data.realtime.daily_validation.Environment'):
                    engine = DailyValidationEngine()
                    assert engine.validation_date == date(2025, 1, 14)
    
    def test_get_validation_date_specific(self):
        """Test getting validation date when set to specific date"""
        with patch.dict(os.environ, {'VALIDATION_DATE': '2025-01-10'}):
            with patch('market_data.realtime.daily_validation.Environment'):
                engine = DailyValidationEngine()
                assert engine.validation_date == date(2025, 1, 10)
    
    def test_has_api_key(self, validation_engine):
        """Test API key availability checking"""
        assert validation_engine._has_api_key('polygon') is True
        assert validation_engine._has_api_key('tiingo') is True
        assert validation_engine._has_api_key('fmp') is True
        assert validation_engine._has_api_key('unknown') is False
    
    @pytest.mark.asyncio
    async def test_initialize_database_connection(self, validation_engine, mock_env):
        """Test database initialization"""
        mock_pool = AsyncMock()
        
        with patch('market_data.realtime.daily_validation.asyncpg.create_pool', return_value=mock_pool):
            await validation_engine.initialize()
            assert validation_engine.pool == mock_pool
            mock_env.get_database_url.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_active_symbols(self, validation_engine):
        """Test getting active symbols for validation"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        validation_engine.pool = mock_pool
        
        # Mock database response
        mock_conn.fetch.return_value = [
            {'symbol': 'AAPL'},
            {'symbol': 'MSFT'},
            {'symbol': 'GOOGL'}
        ]
        
        symbols = await validation_engine._get_active_symbols()
        
        assert symbols == ['AAPL', 'MSFT', 'GOOGL']
        mock_conn.fetch.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_realtime_data(self, validation_engine):
        """Test getting real-time data from database"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        validation_engine.pool = mock_pool
        
        # Mock database response
        timestamp = datetime.now(timezone.utc)
        mock_conn.fetch.return_value = [
            {
                'timestamp': timestamp,
                'open_price': 150.0,
                'high_price': 152.0,
                'low_price': 149.0,
                'close_price': 151.0,
                'volume': 1000000,
                'data_latency_ms': 30000,
                'quality_score': 0.95
            },
            {
                'timestamp': timestamp + timedelta(minutes=1),
                'open_price': 151.0,
                'high_price': 153.0,
                'low_price': 150.0,
                'close_price': 152.0,
                'volume': 1100000,
                'data_latency_ms': 25000,
                'quality_score': 0.97
            }
        ]
        
        data = await validation_engine._get_realtime_data('polygon', 'AAPL')
        
        assert len(data) == 2
        assert data[0]['open_price'] == 150.0
        assert data[1]['close_price'] == 152.0
        mock_conn.fetch.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_polygon_batch_data(self, validation_engine):
        """Test getting batch data from Polygon API"""
        validation_date = date(2025, 1, 15)
        validation_engine.validation_date = validation_date
        
        # Mock aiohttp response
        mock_response_data = {
            'results': [
                {
                    't': int(datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc).timestamp() * 1000),
                    'o': 150.0,
                    'h': 152.0,
                    'l': 149.0,
                    'c': 151.0,
                    'v': 1000000
                },
                {
                    't': int(datetime(2025, 1, 15, 14, 31, 0, tzinfo=timezone.utc).timestamp() * 1000),
                    'o': 151.0,
                    'h': 153.0,
                    'l': 150.0,
                    'c': 152.0,
                    'v': 1100000
                }
            ]
        }
        
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data
        
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            data = await validation_engine._get_polygon_batch_data('AAPL')
            
            assert len(data) == 2
            assert data[0]['open_price'] == 150.0
            assert data[1]['close_price'] == 152.0
            assert isinstance(data[0]['timestamp'], datetime)
    
    @pytest.mark.asyncio
    async def test_get_tiingo_batch_data(self, validation_engine):
        """Test getting batch data from Tiingo API"""
        validation_date = date(2025, 1, 15)
        validation_engine.validation_date = validation_date
        
        # Mock aiohttp response
        mock_response_data = [
            {
                'date': '2025-01-15T14:30:00Z',
                'open': 150.0,
                'high': 152.0,
                'low': 149.0,
                'close': 151.0,
                'volume': 1000000
            },
            {
                'date': '2025-01-15T14:31:00Z',
                'open': 151.0,
                'high': 153.0,
                'low': 150.0,
                'close': 152.0,
                'volume': 1100000
            }
        ]
        
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data
        
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            data = await validation_engine._get_tiingo_batch_data('AAPL')
            
            assert len(data) == 2
            assert data[0]['open_price'] == 150.0
            assert data[1]['close_price'] == 152.0
            assert isinstance(data[0]['timestamp'], datetime)
    
    @pytest.mark.asyncio
    async def test_get_fmp_batch_data(self, validation_engine):
        """Test getting batch data from FMP API"""
        validation_date = date(2025, 1, 15)
        validation_engine.validation_date = validation_date
        
        # Mock aiohttp response
        mock_response_data = [
            {
                'date': '2025-01-15T14:30:00Z',
                'open': 150.0,
                'high': 152.0,
                'low': 149.0,
                'close': 151.0,
                'volume': 1000000
            },
            {
                'date': '2025-01-15T14:31:00Z',
                'open': 151.0,
                'high': 153.0,
                'low': 150.0,
                'close': 152.0,
                'volume': 1100000
            }
        ]
        
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data
        
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            data = await validation_engine._get_fmp_batch_data('AAPL')
            
            assert len(data) == 2
            assert data[0]['open_price'] == 150.0
            assert data[1]['close_price'] == 152.0
            assert isinstance(data[0]['timestamp'], datetime)
    
    def test_compare_data_perfect_match(self, validation_engine):
        """Test data comparison with perfect match"""
        timestamp = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        
        realtime_data = [
            {
                'timestamp': timestamp,
                'open_price': 150.0,
                'high_price': 152.0,
                'low_price': 149.0,
                'close_price': 151.0,
                'volume': 1000000,
                'data_latency_ms': 30000,
                'quality_score': 0.95
            }
        ]
        
        batch_data = [
            {
                'timestamp': timestamp,
                'open_price': 150.0,
                'high_price': 152.0,
                'low_price': 149.0,
                'close_price': 151.0,
                'volume': 1000000
            }
        ]
        
        result = validation_engine._compare_data('polygon', 'AAPL', realtime_data, batch_data)
        
        assert result.symbol == 'AAPL'
        assert result.vendor == 'polygon'
        assert result.realtime_bars_count == 1
        assert result.batch_bars_count == 1
        assert result.missing_realtime_bars == 0
        assert result.discrepant_prices == 0
        assert result.avg_price_difference == 0.0
        assert result.max_price_difference == 0.0
        assert result.overall_accuracy_score == 1.0
        assert result.validation_status == 'passed'
    
    def test_compare_data_with_discrepancies(self, validation_engine):
        """Test data comparison with price discrepancies"""
        timestamp = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        
        realtime_data = [
            {
                'timestamp': timestamp,
                'close_price': 151.0,
                'data_latency_ms': 30000,
                'quality_score': 0.95
            },
            {
                'timestamp': timestamp + timedelta(minutes=1),
                'close_price': 152.5,  # Discrepant price
                'data_latency_ms': 25000,
                'quality_score': 0.97
            }
        ]
        
        batch_data = [
            {
                'timestamp': timestamp,
                'close_price': 151.0
            },
            {
                'timestamp': timestamp + timedelta(minutes=1),
                'close_price': 152.0  # Different from realtime
            }
        ]
        
        # Set price tolerance to 1%
        validation_engine.price_tolerance = 0.01
        
        result = validation_engine._compare_data('polygon', 'AAPL', realtime_data, batch_data)
        
        assert result.discrepant_prices == 1
        assert result.avg_price_difference > 0
        assert result.max_price_difference > 0
        assert result.overall_accuracy_score == 0.5  # 1 out of 2 bars discrepant
        assert result.validation_status == 'warning'  # Below 99% but above 95%
    
    def test_compare_data_high_latency(self, validation_engine):
        """Test data comparison with high latency bars"""
        timestamp = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        
        realtime_data = [
            {
                'timestamp': timestamp,
                'close_price': 151.0,
                'data_latency_ms': 300000,  # 5 minutes latency
                'quality_score': 0.95
            },
            {
                'timestamp': timestamp + timedelta(minutes=1),
                'close_price': 152.0,
                'data_latency_ms': 600000,  # 10 minutes latency
                'quality_score': 0.97
            }
        ]
        
        batch_data = [
            {
                'timestamp': timestamp,
                'close_price': 151.0
            },
            {
                'timestamp': timestamp + timedelta(minutes=1),
                'close_price': 152.0
            }
        ]
        
        result = validation_engine._compare_data('polygon', 'AAPL', realtime_data, batch_data)
        
        assert result.late_bars_count == 2  # Both bars are late (>5 minutes)
        assert result.avg_data_latency_minutes == 7.5  # Average of 5 and 10 minutes
        assert result.max_data_latency_minutes == 10.0
    
    def test_compare_data_missing_realtime_bars(self, validation_engine):
        """Test data comparison with missing real-time bars"""
        timestamp = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        
        realtime_data = [
            {
                'timestamp': timestamp,
                'close_price': 151.0,
                'data_latency_ms': 30000,
                'quality_score': 0.95
            }
        ]
        
        batch_data = [
            {
                'timestamp': timestamp,
                'close_price': 151.0
            },
            {
                'timestamp': timestamp + timedelta(minutes=1),
                'close_price': 152.0
            },
            {
                'timestamp': timestamp + timedelta(minutes=2),
                'close_price': 153.0
            }
        ]
        
        result = validation_engine._compare_data('polygon', 'AAPL', realtime_data, batch_data)
        
        assert result.realtime_bars_count == 1
        assert result.batch_bars_count == 3
        assert result.missing_realtime_bars == 2
    
    def test_compare_data_failed_validation(self, validation_engine):
        """Test data comparison that fails validation"""
        timestamp = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        
        realtime_data = [
            {
                'timestamp': timestamp,
                'close_price': 160.0,  # Very different price
                'data_latency_ms': 30000,
                'quality_score': 0.95
            }
        ]
        
        batch_data = [
            {
                'timestamp': timestamp,
                'close_price': 150.0
            }
        ]
        
        # Set price tolerance to 1%
        validation_engine.price_tolerance = 0.01
        
        result = validation_engine._compare_data('polygon', 'AAPL', realtime_data, batch_data)
        
        assert result.discrepant_prices == 1
        assert result.overall_accuracy_score == 0.0  # 100% discrepant
        assert result.validation_status == 'failed'  # Below 95%
    
    @pytest.mark.asyncio
    async def test_validate_vendor(self, validation_engine):
        """Test validating a specific vendor"""
        # Mock all the required methods
        validation_engine._get_realtime_data = AsyncMock(return_value=[
            {
                'timestamp': datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc),
                'close_price': 151.0,
                'data_latency_ms': 30000,
                'quality_score': 0.95
            }
        ])
        
        validation_engine._get_batch_data = AsyncMock(return_value=[
            {
                'timestamp': datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc),
                'close_price': 151.0
            }
        ])
        
        symbols = ['AAPL', 'MSFT']
        await validation_engine._validate_vendor('polygon', symbols)
        
        # Should have created validation results
        assert len(validation_engine.validation_results) == 2
        assert validation_engine.validation_results[0].vendor == 'polygon'
        assert validation_engine.validation_results[0].symbol == 'AAPL'
        assert validation_engine.validation_results[1].symbol == 'MSFT'
    
    @pytest.mark.asyncio
    async def test_store_validation_results(self, validation_engine):
        """Test storing validation results in database"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        validation_engine.pool = mock_pool
        
        # Add some validation results
        validation_engine.validation_results = [
            ValidationResult(
                symbol='AAPL',
                vendor='polygon',
                validation_date=date(2025, 1, 15),
                realtime_bars_count=390,
                batch_bars_count=390,
                missing_realtime_bars=0,
                discrepant_prices=0,
                avg_price_difference=0.0,
                max_price_difference=0.0,
                avg_data_latency_minutes=1.0,
                max_data_latency_minutes=2.0,
                late_bars_count=0,
                realtime_quality_score=0.95,
                batch_quality_score=1.0,
                overall_accuracy_score=1.0,
                validation_status='passed',
                validation_notes='Perfect match'
            )
        ]
        
        await validation_engine._store_validation_results()
        
        # Verify database insert was called
        mock_conn.execute.assert_called_once()
        
        # Check that all fields are in the SQL
        sql_call = mock_conn.execute.call_args[0][0]
        assert 'symbol' in sql_call
        assert 'validation_date' in sql_call
        assert 'vendor' in sql_call
        assert 'overall_accuracy_score' in sql_call
    
    @pytest.mark.asyncio
    async def test_generate_validation_summary(self, validation_engine):
        """Test generating validation summary"""
        # Add test validation results
        validation_engine.validation_results = [
            ValidationResult(
                symbol='AAPL', vendor='polygon', validation_date=date(2025, 1, 15),
                realtime_bars_count=390, batch_bars_count=390, missing_realtime_bars=0,
                discrepant_prices=0, avg_price_difference=0.0, max_price_difference=0.0,
                avg_data_latency_minutes=1.0, max_data_latency_minutes=2.0, late_bars_count=0,
                realtime_quality_score=0.95, batch_quality_score=1.0, overall_accuracy_score=1.0,
                validation_status='passed', validation_notes='Perfect'
            ),
            ValidationResult(
                symbol='MSFT', vendor='polygon', validation_date=date(2025, 1, 15),
                realtime_bars_count=390, batch_bars_count=390, missing_realtime_bars=0,
                discrepant_prices=5, avg_price_difference=0.001, max_price_difference=0.005,
                avg_data_latency_minutes=1.5, max_data_latency_minutes=3.0, late_bars_count=2,
                realtime_quality_score=0.93, batch_quality_score=1.0, overall_accuracy_score=0.98,
                validation_status='warning', validation_notes='Minor issues'
            ),
            ValidationResult(
                symbol='GOOGL', vendor='polygon', validation_date=date(2025, 1, 15),
                realtime_bars_count=350, batch_bars_count=390, missing_realtime_bars=40,
                discrepant_prices=20, avg_price_difference=0.01, max_price_difference=0.05,
                avg_data_latency_minutes=5.0, max_data_latency_minutes=10.0, late_bars_count=50,
                realtime_quality_score=0.80, batch_quality_score=1.0, overall_accuracy_score=0.90,
                validation_status='failed', validation_notes='Significant issues'
            )
        ]
        
        await validation_engine._generate_validation_summary()
        
        summary = validation_engine.validation_summary
        assert summary['total_validations'] == 3
        assert summary['passed_validations'] == 1
        assert summary['failed_validations'] == 1
        assert summary['warning_validations'] == 1
        assert summary['success_rate'] == 1/3
        assert len(summary['critical_issues']) == 1
        assert summary['critical_issues'][0].symbol == 'GOOGL'
    
    @pytest.mark.asyncio
    async def test_send_validation_alerts(self, validation_engine):
        """Test sending validation alerts"""
        # Set up critical issues in summary
        validation_engine.validation_summary = {
            'critical_issues': [
                ValidationResult(
                    symbol='GOOGL', vendor='polygon', validation_date=date(2025, 1, 15),
                    realtime_bars_count=350, batch_bars_count=390, missing_realtime_bars=40,
                    discrepant_prices=20, avg_price_difference=0.01, max_price_difference=0.05,
                    avg_data_latency_minutes=5.0, max_data_latency_minutes=10.0, late_bars_count=50,
                    realtime_quality_score=0.80, batch_quality_score=1.0, overall_accuracy_score=0.85,
                    validation_status='failed', validation_notes='Significant issues'
                )
            ]
        }
        
        # This should not raise an exception (just logs warnings)
        await validation_engine._send_validation_alerts()
        
        # In a real implementation, this would test actual alert sending
        # For now, we just verify it doesn't crash
    
    @pytest.mark.asyncio
    async def test_run_daily_validation_complete_flow(self, validation_engine):
        """Test the complete daily validation flow"""
        # Mock all dependencies
        mock_pool = AsyncMock()
        validation_engine.pool = mock_pool
        
        validation_engine._get_active_symbols = AsyncMock(return_value=['AAPL', 'MSFT'])
        validation_engine._validate_vendor = AsyncMock()
        validation_engine._store_validation_results = AsyncMock()
        validation_engine._generate_validation_summary = AsyncMock()
        validation_engine._send_validation_alerts = AsyncMock()
        
        await validation_engine.run_daily_validation()
        
        # Verify all steps were called
        validation_engine._get_active_symbols.assert_called_once()
        validation_engine._validate_vendor.assert_called()  # Called for each vendor
        validation_engine._store_validation_results.assert_called_once()
        validation_engine._generate_validation_summary.assert_called_once()
        validation_engine._send_validation_alerts.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_shutdown(self, validation_engine):
        """Test graceful shutdown"""
        mock_pool = AsyncMock()
        validation_engine.pool = mock_pool
        
        await validation_engine.shutdown()
        mock_pool.close.assert_called_once()

class TestAPIErrorHandling:
    """Test API error handling scenarios"""
    
    @pytest.fixture
    def validation_engine(self):
        with patch('market_data.realtime.daily_validation.Environment'):
            with patch.dict(os.environ, {
                'POLYGON_API_KEY': 'test_key',
                'TIINGO_API_KEY': 'test_key',
                'FMP_API_KEY': 'test_key'
            }):
                engine = DailyValidationEngine()
                return engine
    
    @pytest.mark.asyncio
    async def test_polygon_api_error(self, validation_engine):
        """Test handling Polygon API errors"""
        mock_response = AsyncMock()
        mock_response.status = 429  # Rate limit
        
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            data = await validation_engine._get_polygon_batch_data('AAPL')
            assert data == []
    
    @pytest.mark.asyncio
    async def test_tiingo_api_error(self, validation_engine):
        """Test handling Tiingo API errors"""
        mock_response = AsyncMock()
        mock_response.status = 500  # Server error
        
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            data = await validation_engine._get_tiingo_batch_data('AAPL')
            assert data == []
    
    @pytest.mark.asyncio
    async def test_fmp_api_error(self, validation_engine):
        """Test handling FMP API errors"""
        mock_response = AsyncMock()
        mock_response.status = 403  # Forbidden
        
        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            data = await validation_engine._get_fmp_batch_data('AAPL')
            assert data == []
    
    @pytest.mark.asyncio
    async def test_network_timeout(self, validation_engine):
        """Test handling network timeouts"""
        mock_session = AsyncMock()
        mock_session.get.side_effect = asyncio.TimeoutError("Request timeout")
        
        with patch('aiohttp.ClientSession', return_value=mock_session):
            data = await validation_engine._get_polygon_batch_data('AAPL')
            assert data == []

class TestEdgeCases:
    """Test edge cases and boundary conditions"""
    
    @pytest.fixture
    def validation_engine(self):
        with patch('market_data.realtime.daily_validation.Environment'):
            return DailyValidationEngine()
    
    def test_compare_data_empty_datasets(self, validation_engine):
        """Test comparison with empty datasets"""
        result = validation_engine._compare_data('polygon', 'AAPL', [], [])
        
        assert result.realtime_bars_count == 0
        assert result.batch_bars_count == 0
        assert result.overall_accuracy_score == 0
        assert result.validation_status == 'failed'
    
    def test_compare_data_time_offset_tolerance(self, validation_engine):
        """Test that time offset tolerance works correctly"""
        base_time = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        
        realtime_data = [
            {
                'timestamp': base_time,
                'close_price': 151.0,
                'data_latency_ms': 30000,
                'quality_score': 0.95
            }
        ]
        
        batch_data = [
            {
                'timestamp': base_time + timedelta(seconds=30),  # 30 second offset
                'close_price': 151.0
            }
        ]
        
        result = validation_engine._compare_data('polygon', 'AAPL', realtime_data, batch_data)
        
        # Should still match within tolerance
        assert result.discrepant_prices == 0
        assert result.overall_accuracy_score == 1.0
    
    def test_compare_data_no_batch_match(self, validation_engine):
        """Test when no batch data matches real-time data"""
        base_time = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        
        realtime_data = [
            {
                'timestamp': base_time,
                'close_price': 151.0,
                'data_latency_ms': 30000,
                'quality_score': 0.95
            }
        ]
        
        batch_data = [
            {
                'timestamp': base_time + timedelta(hours=1),  # Way off
                'close_price': 151.0
            }
        ]
        
        result = validation_engine._compare_data('polygon', 'AAPL', realtime_data, batch_data)
        
        # No matches found, so no price differences calculated
        assert result.avg_price_difference == 0
        assert result.max_price_difference == 0

if __name__ == '__main__':
    pytest.main([__file__, '-v'])