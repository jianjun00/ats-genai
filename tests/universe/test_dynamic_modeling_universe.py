"""
Tests for Dynamic Modeling Universe

Tests all aspects of the dynamic universe system:
- Stock qualification based on market cap and volume
- Entry, warning, and removal workflows  
- Grace period and re-entry restriction logic
- Daily update processing
- Database tracking and reporting
"""

import pytest
import asyncio
from datetime import date, timedelta
from unittest.mock import Mock, AsyncMock, patch
import asyncpg

from domains.trading.services.dynamic_modeling_universe import (
    DynamicModelingUniverse,
    UniverseStock,
    QualificationMetrics
)
from shared.utils.environment import Environment


@pytest.fixture
async def mock_env():
    """Mock environment for testing"""
    env = Mock(spec=Environment)
    env.get_database_connection_params.return_value = {
        'host': 'localhost',
        'port': 5432,
        'user': 'test',
        'password': 'test',
        'database': 'test'
    }
    env.get_table_name = lambda name: f"test_{name}"
    return env


@pytest.fixture  
async def universe_system(mock_env):
    """Create universe system for testing"""
    universe = DynamicModelingUniverse(mock_env)
    
    # Mock the database pool
    universe.db_pool = AsyncMock()
    universe.db_pool.acquire.return_value.__aenter__ = AsyncMock()
    universe.db_pool.acquire.return_value.__aexit__ = AsyncMock()
    
    return universe


class TestQualificationMetrics:
    """Test qualification metrics and criteria"""
    
    def test_metrics_creation(self):
        """Test qualification metrics creation"""
        metrics = QualificationMetrics(
            symbol="AAPL",
            instrument_id=123,
            avg_market_cap_millions=500.0,
            avg_dollar_volume_millions=150.0,
            trading_days_count=45,
            meets_market_cap=True,
            meets_volume=True,
            qualifies=True
        )
        
        assert metrics.symbol == "AAPL"
        assert metrics.avg_market_cap_millions == 500.0
        assert metrics.avg_dollar_volume_millions == 150.0
        assert metrics.qualifies is True
    
    def test_qualification_logic(self):
        """Test qualification logic combinations"""
        # Both criteria met
        metrics1 = QualificationMetrics(
            symbol="MSFT", instrument_id=124,
            avg_market_cap_millions=600.0, avg_dollar_volume_millions=200.0,
            trading_days_count=50, meets_market_cap=True, meets_volume=True, qualifies=True
        )
        assert metrics1.qualifies is True
        
        # Only market cap met
        metrics2 = QualificationMetrics(
            symbol="GOOGL", instrument_id=125,
            avg_market_cap_millions=800.0, avg_dollar_volume_millions=50.0,
            trading_days_count=45, meets_market_cap=True, meets_volume=False, qualifies=False
        )
        assert metrics2.qualifies is False
        
        # Only volume met
        metrics3 = QualificationMetrics(
            symbol="TSLA", instrument_id=126,
            avg_market_cap_millions=200.0, avg_dollar_volume_millions=300.0,
            trading_days_count=42, meets_market_cap=False, meets_volume=True, qualifies=False
        )
        assert metrics3.qualifies is False
        
        # Neither criteria met
        metrics4 = QualificationMetrics(
            symbol="SMALL", instrument_id=127,
            avg_market_cap_millions=100.0, avg_dollar_volume_millions=20.0,
            trading_days_count=30, meets_market_cap=False, meets_volume=False, qualifies=False
        )
        assert metrics4.qualifies is False


class TestUniverseStock:
    """Test universe stock tracking"""
    
    def test_stock_creation(self):
        """Test universe stock creation"""
        stock = UniverseStock(
            instrument_id=123,
            symbol="AAPL",
            entry_date=date(2024, 1, 15),
            last_qualifying_date=date(2024, 2, 1)
        )
        
        assert stock.instrument_id == 123
        assert stock.symbol == "AAPL"
        assert stock.entry_date == date(2024, 1, 15)
        assert stock.warning_date is None
        assert stock.removal_date is None
    
    def test_stock_lifecycle_states(self):
        """Test stock lifecycle states"""
        # Active stock
        active_stock = UniverseStock(
            instrument_id=123, symbol="AAPL",
            entry_date=date(2024, 1, 1),
            last_qualifying_date=date(2024, 2, 1)
        )
        assert active_stock.removal_date is None
        
        # Warned stock
        warned_stock = UniverseStock(
            instrument_id=124, symbol="WARN",
            entry_date=date(2024, 1, 1),
            last_qualifying_date=date(2024, 1, 20),
            warning_date=date(2024, 2, 1)
        )
        assert warned_stock.warning_date is not None
        assert warned_stock.removal_date is None
        
        # Removed stock
        removed_stock = UniverseStock(
            instrument_id=125, symbol="GONE",
            entry_date=date(2024, 1, 1),
            last_qualifying_date=date(2024, 1, 15),
            warning_date=date(2024, 2, 1),
            removal_date=date(2024, 2, 8),
            removal_reason="Failed volume criteria"
        )
        assert removed_stock.removal_date is not None
        assert removed_stock.removal_reason is not None


class TestDynamicModelingUniverse:
    """Test main dynamic universe functionality"""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_universe_initialization(self, universe_system):
        """Test universe system initialization"""
        # Mock the ensure universe exists method
        universe_system._ensure_universe_exists = AsyncMock()
        
        await universe_system.initialize()
        
        assert universe_system.universe_name == "dynamic_modeling_400m_100m"
        assert universe_system.min_market_cap_millions == 400
        assert universe_system.min_dollar_volume_millions == 100
        assert universe_system.lookback_days == 52
        assert universe_system.grace_period_days == 7
        assert universe_system.reentry_restriction_days == 365
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_qualifying_stocks_query(self, universe_system):
        """Test qualifying stocks database query"""
        # Mock database response
        mock_rows = [
            {
                'instrument_id': 123,
                'symbol': 'AAPL',
                'trading_days': 50,
                'avg_market_cap_millions': 2500.0,
                'avg_dollar_volume_millions': 8000.0,
                'last_price': 185.50,
                'last_date': date(2024, 2, 1),
                'meets_market_cap': True,
                'meets_volume': True,
                'qualifies': True
            },
            {
                'instrument_id': 124,
                'symbol': 'SMALL',
                'trading_days': 45,
                'avg_market_cap_millions': 200.0,
                'avg_dollar_volume_millions': 50.0,
                'last_price': 25.30,
                'last_date': date(2024, 2, 1),
                'meets_market_cap': False,
                'meets_volume': False,
                'qualifies': False
            }
        ]
        
        # Mock the database connection and query
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_rows
        universe_system.db_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Test the method
        metrics = await universe_system._get_qualifying_stocks(date(2024, 2, 1))
        
        assert len(metrics) == 2
        assert metrics[0].symbol == 'AAPL'
        assert metrics[0].qualifies is True
        assert metrics[1].symbol == 'SMALL'
        assert metrics[1].qualifies is False
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_reentry_eligibility_check(self, universe_system):
        """Test re-entry eligibility checking"""
        # Mock database responses for different scenarios
        
        # Case 1: Never removed (eligible)
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = None
        universe_system.db_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        eligible = await universe_system._check_reentry_eligibility(123, date(2024, 2, 1))
        assert eligible is True
        
        # Case 2: Removed recently (not eligible)
        mock_conn.fetchrow.return_value = {'last_removal_date': date(2024, 1, 1)}
        eligible = await universe_system._check_reentry_eligibility(123, date(2024, 2, 1))
        assert eligible is False  # Only 31 days, need 365
        
        # Case 3: Removed over a year ago (eligible)
        mock_conn.fetchrow.return_value = {'last_removal_date': date(2023, 1, 1)}
        eligible = await universe_system._check_reentry_eligibility(123, date(2024, 2, 1))
        assert eligible is True  # Over 365 days
    
    def test_failure_reason_generation(self, universe_system):
        """Test failure reason text generation"""
        # No metrics
        reason = universe_system._get_failure_reason(None)
        assert "No recent data" in reason
        
        # Market cap failure
        metrics = QualificationMetrics(
            symbol="TEST", instrument_id=123,
            avg_market_cap_millions=200.0, avg_dollar_volume_millions=150.0,
            trading_days_count=45, meets_market_cap=False, meets_volume=True, qualifies=False
        )
        reason = universe_system._get_failure_reason(metrics)
        assert "Market cap" in reason and "200M < 400M" in reason
        
        # Volume failure
        metrics = QualificationMetrics(
            symbol="TEST", instrument_id=123,
            avg_market_cap_millions=500.0, avg_dollar_volume_millions=50.0,
            trading_days_count=45, meets_market_cap=True, meets_volume=False, qualifies=False
        )
        reason = universe_system._get_failure_reason(metrics)
        assert "Volume" in reason and "50M < 100M" in reason
        
        # Both failures
        metrics = QualificationMetrics(
            symbol="TEST", instrument_id=123,
            avg_market_cap_millions=200.0, avg_dollar_volume_millions=50.0,
            trading_days_count=45, meets_market_cap=False, meets_volume=False, qualifies=False
        )
        reason = universe_system._get_failure_reason(metrics)
        assert "Market cap" in reason and "Volume" in reason
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_stock_addition_workflow(self, universe_system):
        """Test stock addition to universe"""
        metrics = QualificationMetrics(
            symbol="AAPL", instrument_id=123,
            avg_market_cap_millions=2500.0, avg_dollar_volume_millions=8000.0,
            trading_days_count=50, meets_market_cap=True, meets_volume=True, qualifies=True
        )
        
        # Mock database operations
        mock_conn = AsyncMock()
        universe_system.db_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        await universe_system._add_stock_to_universe(metrics, date(2024, 2, 1))
        
        # Verify database calls were made
        assert mock_conn.execute.call_count == 2  # Tracking + membership inserts
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_stock_removal_workflow(self, universe_system):
        """Test stock removal from universe"""
        stock = UniverseStock(
            instrument_id=123, symbol="AAPL",
            entry_date=date(2024, 1, 1),
            last_qualifying_date=date(2024, 1, 20),
            warning_date=date(2024, 2, 1)
        )
        
        # Mock database operations
        mock_conn = AsyncMock()
        universe_system.db_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        await universe_system._remove_stock_from_universe(
            stock, date(2024, 2, 8), "Failed volume criteria"
        )
        
        # Verify database calls were made
        assert mock_conn.execute.call_count == 2  # Tracking update + membership delete
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_warning_workflow(self, universe_system):
        """Test warning date setting and clearing"""
        stock = UniverseStock(
            instrument_id=123, symbol="AAPL",
            entry_date=date(2024, 1, 1),
            last_qualifying_date=date(2024, 2, 1)
        )
        
        # Mock database operations
        mock_conn = AsyncMock()
        universe_system.db_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Test setting warning
        await universe_system._set_warning_date(stock, date(2024, 2, 5))
        mock_conn.execute.assert_called()
        
        # Test clearing warning
        await universe_system._clear_warning_date(stock, date(2024, 2, 10))
        assert mock_conn.execute.call_count >= 2
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_daily_update_logic(self, universe_system):
        """Test daily update processing logic"""
        # Mock current stocks
        current_stocks = [
            UniverseStock(
                instrument_id=123, symbol="AAPL",
                entry_date=date(2024, 1, 1),
                last_qualifying_date=date(2024, 2, 1)
            ),
            UniverseStock(
                instrument_id=124, symbol="WARN",
                entry_date=date(2024, 1, 1),
                last_qualifying_date=date(2024, 1, 20),
                warning_date=date(2024, 2, 1)  # 7 days ago
            )
        ]
        
        # Mock qualifying stocks
        qualifying_metrics = [
            QualificationMetrics(
                symbol="AAPL", instrument_id=123,
                avg_market_cap_millions=2500.0, avg_dollar_volume_millions=8000.0,
                trading_days_count=50, meets_market_cap=True, meets_volume=True, qualifies=True
            ),
            QualificationMetrics(
                symbol="NEWSTOCK", instrument_id=125,
                avg_market_cap_millions=600.0, avg_dollar_volume_millions=200.0,
                trading_days_count=45, meets_market_cap=True, meets_volume=True, qualifies=True
            )
        ]
        
        # Mock database methods
        universe_system._get_current_universe_stocks = AsyncMock(return_value=current_stocks)
        universe_system._get_qualifying_stocks = AsyncMock(return_value=qualifying_metrics)
        universe_system._check_reentry_eligibility = AsyncMock(return_value=True)
        universe_system._add_stock_to_universe = AsyncMock()
        universe_system._remove_stock_from_universe = AsyncMock()
        universe_system._update_stock_metrics_in_db = AsyncMock()
        
        # Run daily update
        summary = await universe_system.run_daily_update(date(2024, 2, 8))
        
        # Verify results
        assert summary['current_count'] == 2
        assert summary['qualifying_count'] == 2
        assert len(summary['added']) == 1  # NEWSTOCK should be added
        assert len(summary['removed']) == 1  # WARN should be removed (grace period expired)
        assert summary['added'][0]['symbol'] == 'NEWSTOCK'
        assert summary['removed'][0]['symbol'] == 'WARN'


class TestGracePeriodLogic:
    """Test grace period and removal timing"""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_grace_period_scenarios(self, universe_system):
        """Test different grace period scenarios"""
        update_date = date(2024, 2, 8)
        
        # Stock warned 3 days ago (still in grace period)
        stock_grace_active = UniverseStock(
            instrument_id=123, symbol="GRACE",
            entry_date=date(2024, 1, 1),
            last_qualifying_date=date(2024, 1, 25),
            warning_date=date(2024, 2, 5)  # 3 days ago
        )
        
        days_warned = (update_date - stock_grace_active.warning_date).days
        assert days_warned == 3
        assert days_warned < universe_system.grace_period_days  # Still in grace period
        
        # Stock warned 7 days ago (grace period expired)
        stock_grace_expired = UniverseStock(
            instrument_id=124, symbol="EXPIRE",
            entry_date=date(2024, 1, 1),
            last_qualifying_date=date(2024, 1, 20),
            warning_date=date(2024, 2, 1)  # 7 days ago
        )
        
        days_warned = (update_date - stock_grace_expired.warning_date).days
        assert days_warned == 7
        assert days_warned >= universe_system.grace_period_days  # Grace period expired
        
        # Stock warned 10 days ago (well past grace period)
        stock_overdue = UniverseStock(
            instrument_id=125, symbol="OVERDUE",
            entry_date=date(2024, 1, 1),
            last_qualifying_date=date(2024, 1, 15),
            warning_date=date(2024, 1, 29)  # 10 days ago
        )
        
        days_warned = (update_date - stock_overdue.warning_date).days
        assert days_warned == 10
        assert days_warned >= universe_system.grace_period_days  # Well past grace period


class TestReentryRestrictions:
    """Test re-entry restriction logic"""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_reentry_timing(self, universe_system):
        """Test re-entry restriction timing"""
        current_date = date(2024, 8, 15)
        
        # Mock database responses for different removal dates
        mock_conn = AsyncMock()
        universe_system.db_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Removed 6 months ago (not eligible)
        mock_conn.fetchrow.return_value = {'last_removal_date': date(2024, 2, 15)}
        eligible = await universe_system._check_reentry_eligibility(123, current_date)
        days_since = (current_date - date(2024, 2, 15)).days
        assert days_since == 182  # About 6 months
        assert eligible is False
        
        # Removed exactly 1 year ago (eligible)
        mock_conn.fetchrow.return_value = {'last_removal_date': date(2023, 8, 15)}
        eligible = await universe_system._check_reentry_eligibility(123, current_date)
        days_since = (current_date - date(2023, 8, 15)).days
        assert days_since == 365
        assert eligible is True
        
        # Removed 2 years ago (definitely eligible)
        mock_conn.fetchrow.return_value = {'last_removal_date': date(2022, 8, 15)}
        eligible = await universe_system._check_reentry_eligibility(123, current_date)
        days_since = (current_date - date(2022, 8, 15)).days
        assert days_since == 730
        assert eligible is True


class TestReportGeneration:
    """Test universe reporting functionality"""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_universe_report_generation(self, universe_system):
        """Test universe report generation"""
        # Mock current stocks
        current_stocks = [
            UniverseStock(
                instrument_id=123, symbol="AAPL",
                entry_date=date(2024, 1, 15),
                last_qualifying_date=date(2024, 2, 1),
                avg_market_cap=2500.0,
                avg_dollar_volume=8000.0
            ),
            UniverseStock(
                instrument_id=124, symbol="MSFT",
                entry_date=date(2024, 1, 20),
                last_qualifying_date=date(2024, 2, 1),
                warning_date=date(2024, 2, 5),
                avg_market_cap=2800.0,
                avg_dollar_volume=6000.0
            )
        ]
        
        # Mock qualifying metrics
        qualifying_metrics = [
            QualificationMetrics(
                symbol="AAPL", instrument_id=123,
                avg_market_cap_millions=2500.0, avg_dollar_volume_millions=8000.0,
                trading_days_count=50, meets_market_cap=True, meets_volume=True, qualifies=True
            ),
            QualificationMetrics(
                symbol="MSFT", instrument_id=124,
                avg_market_cap_millions=2800.0, avg_dollar_volume_millions=50.0,  # Failed volume
                trading_days_count=48, meets_market_cap=True, meets_volume=False, qualifies=False
            )
        ]
        
        universe_system._get_current_universe_stocks = AsyncMock(return_value=current_stocks)
        universe_system._get_qualifying_stocks = AsyncMock(return_value=qualifying_metrics)
        
        # Generate report
        report = await universe_system.get_current_universe_report()
        
        # Verify report content
        assert "Dynamic Modeling Universe Report" in report
        assert "Current Universe (2 stocks)" in report
        assert "AAPL" in report
        assert "MSFT" in report
        assert "✅ Qualifying" in report
        assert "⚠️ Failing" in report
        assert "Total Market Cap" in report


if __name__ == "__main__":
    pytest.main([__file__, "-v"])