import pytest
import asyncio
import asyncpg
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import date, datetime, timedelta
from typing import List, Dict, Set, Optional

from universe.data_complete_universe_creator import (
    DataCompleteUniverseCreator, 
    DataCompleteness
)
from config.environment import Environment


class TestDataCompleteUniverseCreator:
    """Comprehensive test coverage for DataCompleteUniverseCreator."""
    
    def test_init_default_environment(self):
        """Test initialization with default environment."""
        with patch('universe.data_complete_universe_creator.Environment') as mock_env:
            mock_env_instance = MagicMock()
            mock_env.return_value = mock_env_instance
            
            creator = DataCompleteUniverseCreator()
            
            assert creator.env == mock_env_instance
            assert creator.min_years == 5
            assert creator.min_daily_completeness == 0.95
            assert creator.min_minute_completeness == 0.85
            assert creator.min_overall_quality == 0.80
            assert creator.logger is not None

    def test_init_custom_environment(self):
        """Test initialization with custom environment."""
        custom_env = MagicMock()
        
        creator = DataCompleteUniverseCreator(env=custom_env)
        
        assert creator.env == custom_env

    @pytest.mark.asyncio
    async def test_analyze_data_completeness_success(self):
        """Test successful data completeness analysis."""
        mock_env = MagicMock()
        mock_env.get_database_url.return_value = "postgresql://test"
        
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        # Mock database pool
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value = mock_conn  # Direct assignment, not context manager
        
        # Mock completeness data
        test_symbols = ["AAPL", "MSFT", "GOOGL"]
        test_completeness = [
            DataCompleteness(
                symbol="AAPL",
                instrument_id=1,
                daily_start_date=date(2019, 1, 1),
                daily_end_date=date(2024, 1, 1),
                daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0),
                minute_count=489600,
                minute_trading_days=1260,
                expected_daily_count=1300,
                expected_minute_count=507000,
                daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.97,
                overall_quality_score=0.97
            ),
            DataCompleteness(
                symbol="MSFT",
                instrument_id=2,
                daily_start_date=date(2019, 1, 1),
                daily_end_date=date(2024, 1, 1),
                daily_count=1250,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0),
                minute_count=485250,
                minute_trading_days=1250,
                expected_daily_count=1300,
                expected_minute_count=507000,
                daily_completeness_ratio=0.96,
                minute_completeness_ratio=0.96,
                overall_quality_score=0.96
            )
        ]
        
        with patch('universe.data_complete_universe_creator.asyncpg.create_pool', return_value=mock_pool):
            with patch.object(creator, '_get_symbols_with_both_datasets', new_callable=AsyncMock, return_value=set(test_symbols)):
                with patch.object(creator, '_analyze_symbol_completeness', new_callable=AsyncMock, side_effect=test_completeness + [None]):  # None for GOOGL
                    
                    result = await creator.analyze_data_completeness()
                    
                    assert len(result) == 2  # Only AAPL and MSFT (GOOGL returned None)
                    assert result[0].symbol == "AAPL"
                    assert result[1].symbol == "MSFT"
                    assert result[0].overall_quality_score == 0.97
                    assert result[1].overall_quality_score == 0.96

    @pytest.mark.asyncio
    async def test_analyze_data_completeness_database_error(self):
        """Test data completeness analysis with database connection error."""
        mock_env = MagicMock()
        mock_env.get_database_url.return_value = "postgresql://invalid"
        
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        with patch('universe.data_complete_universe_creator.asyncpg.create_pool', new_callable=AsyncMock) as mock_create_pool:
            mock_create_pool.side_effect = asyncpg.PostgresConnectionError("Connection failed")
            
            with pytest.raises(asyncpg.PostgresConnectionError):
                await creator.analyze_data_completeness()

    @pytest.mark.asyncio
    async def test_get_symbols_with_both_datasets_success(self):
        """Test getting symbols present in both daily and minute data."""
        mock_env = MagicMock()
        mock_env.get_table_name.return_value = "dev_minute_bars"
        
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        mock_conn = AsyncMock()
        
        # Mock daily symbols query result
        daily_symbols = [{"symbol": "AAPL"}, {"symbol": "MSFT"}, {"symbol": "GOOGL"}]
        # Mock minute symbols query result
        minute_symbols = [{"symbol": "AAPL"}, {"symbol": "MSFT"}, {"symbol": "TSLA"}]
        
        mock_conn.fetch.side_effect = [daily_symbols, minute_symbols]
        
        result = await creator._get_symbols_with_both_datasets(mock_conn)
        
        # Should return intersection: AAPL and MSFT
        assert result == {"AAPL", "MSFT"}
        assert len(result) == 2
        assert "GOOGL" not in result  # Only in daily
        assert "TSLA" not in result   # Only in minute

    @pytest.mark.asyncio
    async def test_analyze_symbol_completeness_complete_data(self):
        """Test symbol completeness analysis for high-quality data."""
        mock_env = MagicMock()
        mock_env.get_table_name.side_effect = lambda x: f"dev_{x}"
        
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        mock_conn = AsyncMock()
        
        # Mock daily completeness analysis
        creator._analyze_daily_completeness = AsyncMock(return_value={
            'start_date': date(2019, 1, 1),
            'end_date': date(2024, 1, 1),
            'count': 1260
        })
        
        # Mock minute completeness analysis
        creator._analyze_minute_completeness = AsyncMock(return_value={
            'start_datetime': datetime(2019, 1, 1, 9, 30),
            'end_datetime': datetime(2024, 1, 1, 16, 0),
            'count': 489600,
            'trading_days': 1260
        })
        
        # Mock instrument ID lookup
        creator._get_instrument_id = AsyncMock(return_value=1)
        
        # Mock calculation methods 
        creator._calculate_expected_trading_days = MagicMock(return_value=1300)
        creator._calculate_expected_minute_bars = MagicMock(return_value=507000)
        creator._calculate_quality_score = MagicMock(return_value=0.97)
        
        result = await creator._analyze_symbol_completeness(mock_conn, "AAPL")
        
        assert result is not None
        assert result.symbol == "AAPL"
        assert result.instrument_id == 1
        assert result.daily_count == 1260
        assert result.minute_count == 489600
        assert result.overall_quality_score == 0.97

    @pytest.mark.asyncio
    async def test_analyze_symbol_completeness_insufficient_data(self):
        """Test symbol completeness analysis for insufficient data."""
        mock_env = MagicMock()
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        mock_conn = AsyncMock()
        
        # Mock daily completeness with insufficient data
        creator._analyze_daily_completeness = AsyncMock(return_value=None)
        creator._analyze_minute_completeness = AsyncMock(return_value=None)
        
        result = await creator._analyze_symbol_completeness(mock_conn, "BADSTOCK")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_analyze_daily_completeness_success(self):
        """Test daily data completeness analysis."""
        mock_env = MagicMock()
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        mock_conn = AsyncMock()
        
        # Mock database query result
        mock_result = {
            "start_date": date(2019, 1, 1),
            "end_date": date(2024, 1, 1),
            "count": 1260
        }
        mock_conn.fetchrow.return_value = mock_result
        
        result = await creator._analyze_daily_completeness(mock_conn, "AAPL")
        
        assert result is not None
        assert result['start_date'] == date(2019, 1, 1)
        assert result['end_date'] == date(2024, 1, 1)
        assert result['count'] == 1260

    @pytest.mark.asyncio
    async def test_analyze_daily_completeness_no_data(self):
        """Test daily completeness analysis with no data."""
        mock_env = MagicMock()
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = None
        
        result = await creator._analyze_daily_completeness(mock_conn, "NODATA")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_analyze_minute_completeness_success(self):
        """Test minute data completeness analysis."""
        mock_env = MagicMock()
        mock_env.get_table_name.return_value = "dev_minute_bars"
        
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        mock_conn = AsyncMock()
        
        # Mock database query result
        mock_result = {
            "start_datetime": datetime(2019, 1, 1, 9, 30),
            "end_datetime": datetime(2024, 1, 1, 16, 0),
            "count": 489600,
            "trading_days": 1260
        }
        mock_conn.fetchrow.return_value = mock_result
        
        result = await creator._analyze_minute_completeness(mock_conn, "AAPL")
        
        assert result is not None
        assert result['start_datetime'] == datetime(2019, 1, 1, 9, 30)
        assert result['end_datetime'] == datetime(2024, 1, 1, 16, 0)
        assert result['count'] == 489600
        assert result['trading_days'] == 1260

    def test_calculate_expected_trading_days_full_years(self):
        """Test expected trading days calculation for full years."""
        creator = DataCompleteUniverseCreator()
        
        # 5 years from 2019 to 2024
        start_date = date(2019, 1, 1)
        end_date = date(2024, 1, 1)
        
        result = creator._calculate_expected_trading_days(start_date, end_date)
        
        # Approximately 252 trading days per year * 5 years
        assert 1250 <= result <= 1310  # Allow for holidays/weekends variance

    def test_calculate_expected_trading_days_partial_year(self):
        """Test expected trading days calculation for partial year."""
        creator = DataCompleteUniverseCreator()
        
        # 6 months
        start_date = date(2023, 1, 1)
        end_date = date(2023, 7, 1)
        
        result = creator._calculate_expected_trading_days(start_date, end_date)
        
        # Approximately 126 trading days for 6 months
        assert 120 <= result <= 135

    def test_calculate_expected_minute_bars_full_trading_days(self):
        """Test expected minute bars calculation."""
        creator = DataCompleteUniverseCreator()
        
        trading_days = 1260
        
        # 390 minute bars per trading day (9:30 AM to 4:00 PM = 6.5 hours * 60 minutes)
        result = creator._calculate_expected_minute_bars(trading_days)
        
        assert result == 491400  # 1260 * 390

    def test_calculate_quality_score_high_quality(self):
        """Test quality score calculation for high-quality data."""
        creator = DataCompleteUniverseCreator()
        
        # High completeness ratios and high data counts
        daily_ratio = 0.98
        minute_ratio = 0.96
        daily_count = 1260
        minute_count = 550000
        
        score = creator._calculate_quality_score(daily_ratio, minute_ratio, daily_count, minute_count)
        
        # Should be weighted average (minute data is weighted more heavily) plus bonuses
        expected_base = (daily_ratio * 0.3) + (minute_ratio * 0.7)
        expected_bonus = 0.1  # Both data volume bonuses
        expected = expected_base + expected_bonus
        assert score == pytest.approx(expected, abs=0.001)
        assert score > 0.95

    def test_calculate_quality_score_low_quality(self):
        """Test quality score calculation for low-quality data."""
        creator = DataCompleteUniverseCreator()
        
        # Low completeness ratios and low data counts
        daily_ratio = 0.70
        minute_ratio = 0.60
        daily_count = 500
        minute_count = 100000
        
        score = creator._calculate_quality_score(daily_ratio, minute_ratio, daily_count, minute_count)
        
        # Should be weighted average with no bonuses
        expected = (daily_ratio * 0.3) + (minute_ratio * 0.7)
        assert score == pytest.approx(expected, abs=0.001)
        assert score < 0.70

    @pytest.mark.asyncio
    async def test_get_instrument_id_success(self):
        """Test instrument ID lookup."""
        mock_env = MagicMock()
        mock_env.get_table_name.return_value = "dev_instruments"
        
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {"instrument_id": 123}
        
        result = await creator._get_instrument_id(mock_conn, "AAPL")
        
        assert result == 123

    @pytest.mark.asyncio
    async def test_get_instrument_id_not_found(self):
        """Test instrument ID lookup when not found."""
        mock_env = MagicMock()
        mock_env.get_table_name.return_value = "dev_instruments"
        
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = None
        
        result = await creator._get_instrument_id(mock_conn, "UNKNOWN")
        
        assert result is None

    @pytest.mark.asyncio
    async def test_create_data_complete_universe_success(self):
        """Test creating a data complete universe."""
        mock_env = MagicMock()
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        # Mock qualified instruments
        qualified_instruments = [
            DataCompleteness(
                symbol="AAPL", instrument_id=1, daily_start_date=date(2019, 1, 1),
                daily_end_date=date(2024, 1, 1), daily_count=1260, 
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.97, overall_quality_score=0.97
            )
        ]
        
        # Mock methods
        creator.analyze_data_completeness = AsyncMock(return_value=qualified_instruments)
        creator._filter_qualified_instruments = MagicMock(return_value=qualified_instruments)
        creator._create_universe_with_members = AsyncMock(return_value=100)  # universe_id
        
        universe_id = await creator.create_data_complete_universe("test_universe")
        
        assert universe_id == 100
        creator.analyze_data_completeness.assert_called_once()
        creator._filter_qualified_instruments.assert_called_once()
        creator._create_universe_with_members.assert_called_once()

    def test_filter_qualified_instruments_all_pass(self):
        """Test filtering instruments where all pass quality thresholds."""
        creator = DataCompleteUniverseCreator()
        
        instruments = [
            DataCompleteness(
                symbol="AAPL", instrument_id=1, daily_start_date=date(2019, 1, 1),
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.96, overall_quality_score=0.97
            ),
            DataCompleteness(
                symbol="MSFT", instrument_id=2, daily_start_date=date(2019, 1, 1),
                daily_end_date=date(2024, 1, 1), daily_count=1250,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=485250,
                minute_trading_days=1250, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.96,
                minute_completeness_ratio=0.96, overall_quality_score=0.96
            )
        ]
        
        result = creator._filter_qualified_instruments(instruments)
        
        assert len(result) == 2
        assert result[0].symbol == "AAPL"
        assert result[1].symbol == "MSFT"

    def test_filter_qualified_instruments_some_fail(self):
        """Test filtering instruments where some fail quality thresholds."""
        creator = DataCompleteUniverseCreator()
        
        instruments = [
            # Good quality instrument
            DataCompleteness(
                symbol="AAPL", instrument_id=1, daily_start_date=date(2019, 1, 1),
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.96, overall_quality_score=0.97
            ),
            # Poor quality instrument (low daily completeness)
            DataCompleteness(
                symbol="BADSTOCK", instrument_id=3, daily_start_date=date(2019, 1, 1),
                daily_end_date=date(2024, 1, 1), daily_count=800,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=300000,
                minute_trading_days=800, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.62,  # Below 0.95 threshold
                minute_completeness_ratio=0.59, overall_quality_score=0.60  # Below 0.80 threshold
            )
        ]
        
        result = creator._filter_qualified_instruments(instruments)
        
        assert len(result) == 1  # Only AAPL should pass
        assert result[0].symbol == "AAPL"

    def test_filter_qualified_instruments_empty_list(self):
        """Test filtering with empty instruments list."""
        creator = DataCompleteUniverseCreator()
        
        result = creator._filter_qualified_instruments([])
        
        assert result == []

    @pytest.mark.asyncio
    async def test_create_universe_with_members_success(self):
        """Test creating universe with members."""
        mock_env = MagicMock()
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        qualified_instruments = [
            DataCompleteness(
                symbol="AAPL", instrument_id=1, daily_start_date=date(2019, 1, 1),
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.96, overall_quality_score=0.97
            )
        ]
        
        # Mock universe creation
        creator._create_universe = AsyncMock(return_value=100)
        creator._add_universe_members = AsyncMock()
        
        universe_id = await creator._create_universe_with_members(
            "test_universe", 
            qualified_instruments
        )
        
        assert universe_id == 100
        creator._create_universe.assert_called_once()
        creator._add_universe_members.assert_called_once()

    @pytest.mark.asyncio
    async def test_generate_quality_report_comprehensive(self):
        """Test generating comprehensive quality report."""
        creator = DataCompleteUniverseCreator()
        
        completeness_data = [
            DataCompleteness(
                symbol="AAPL", instrument_id=1, daily_start_date=date(2019, 1, 1),
                daily_end_date=date(2024, 1, 1), daily_count=1260,
                minute_start_date=datetime(2019, 1, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
                minute_trading_days=1260, expected_daily_count=1300,
                expected_minute_count=507000, daily_completeness_ratio=0.97,
                minute_completeness_ratio=0.96, overall_quality_score=0.97
            ),
            DataCompleteness(
                symbol="MSFT", instrument_id=2, daily_start_date=date(2019, 6, 1),
                daily_end_date=date(2024, 1, 1), daily_count=1150,
                minute_start_date=datetime(2019, 6, 1, 9, 30),
                minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=448500,
                minute_trading_days=1150, expected_daily_count=1200,
                expected_minute_count=468000, daily_completeness_ratio=0.96,
                minute_completeness_ratio=0.96, overall_quality_score=0.96
            )
        ]
        
        # Mock the internal methods
        creator.analyze_data_completeness = AsyncMock(return_value=completeness_data)
        creator._filter_qualified_instruments = MagicMock(return_value=completeness_data)
        
        report = await creator.generate_quality_report()
        
        assert "Data Completeness Analysis Report" in report
        assert "Total symbols analyzed: 2" in report
        assert "AAPL" in report
        assert "MSFT" in report
        assert "0.97" in report  # AAPL quality score
        assert "0.96" in report  # MSFT quality score


class TestDataCompletenessDataclass:
    """Test DataCompleteness dataclass functionality."""
    
    def test_dataclass_creation(self):
        """Test DataCompleteness dataclass creation."""
        completeness = DataCompleteness(
            symbol="AAPL",
            instrument_id=1,
            daily_start_date=date(2019, 1, 1),
            daily_end_date=date(2024, 1, 1),
            daily_count=1260,
            minute_start_date=datetime(2019, 1, 1, 9, 30),
            minute_end_date=datetime(2024, 1, 1, 16, 0),
            minute_count=489600,
            minute_trading_days=1260,
            expected_daily_count=1300,
            expected_minute_count=507000,
            daily_completeness_ratio=0.97,
            minute_completeness_ratio=0.96,
            overall_quality_score=0.97
        )
        
        assert completeness.symbol == "AAPL"
        assert completeness.instrument_id == 1
        assert completeness.daily_count == 1260
        assert completeness.minute_count == 489600
        assert completeness.overall_quality_score == 0.97

    def test_dataclass_with_none_values(self):
        """Test DataCompleteness with None values."""
        completeness = DataCompleteness(
            symbol="UNKNOWN",
            instrument_id=None,
            daily_start_date=None,
            daily_end_date=None,
            daily_count=0,
            minute_start_date=None,
            minute_end_date=None,
            minute_count=0,
            minute_trading_days=0,
            expected_daily_count=0,
            expected_minute_count=0,
            daily_completeness_ratio=0.0,
            minute_completeness_ratio=0.0,
            overall_quality_score=0.0
        )
        
        assert completeness.symbol == "UNKNOWN"
        assert completeness.instrument_id is None
        assert completeness.daily_start_date is None
        assert completeness.minute_start_date is None
        assert completeness.overall_quality_score == 0.0


class TestDataCompleteUniverseCreatorIntegration:
    """Integration tests for DataCompleteUniverseCreator."""
    
    @pytest.mark.asyncio
    async def test_end_to_end_universe_creation_workflow(self):
        """Test complete end-to-end universe creation workflow."""
        mock_env = MagicMock()
        mock_env.get_database_url.return_value = "postgresql://test"
        mock_env.get_table_name.side_effect = lambda x: f"dev_{x}"
        
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        # Mock all async database operations
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        # Create comprehensive mock data
        test_completeness = DataCompleteness(
            symbol="AAPL", instrument_id=1, daily_start_date=date(2019, 1, 1),
            daily_end_date=date(2024, 1, 1), daily_count=1260,
            minute_start_date=datetime(2019, 1, 1, 9, 30),
            minute_end_date=datetime(2024, 1, 1, 16, 0), minute_count=489600,
            minute_trading_days=1260, expected_daily_count=1300,
            expected_minute_count=507000, daily_completeness_ratio=0.97,
            minute_completeness_ratio=0.96, overall_quality_score=0.97
        )
        
        with patch('universe.data_complete_universe_creator.asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            # Mock all the workflow steps
            creator._get_symbols_with_both_datasets = AsyncMock(return_value={"AAPL"})
            creator._analyze_symbol_completeness = AsyncMock(return_value=test_completeness)
            creator._create_universe = AsyncMock(return_value=100)
            creator._add_universe_members = AsyncMock()
            
            # Execute the workflow
            universe_id = await creator.create_data_complete_universe("integration_test_universe")
            
            # Verify the workflow completed successfully
            assert universe_id == 100
            creator._get_symbols_with_both_datasets.assert_called_once()
            creator._analyze_symbol_completeness.assert_called_once_with(mock_conn, "AAPL")
            creator._create_universe.assert_called_once()
            creator._add_universe_members.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling_during_analysis(self):
        """Test error handling during completeness analysis."""
        mock_env = MagicMock()
        mock_env.get_database_url.return_value = "postgresql://test"
        
        creator = DataCompleteUniverseCreator(env=mock_env)
        
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        with patch('universe.data_complete_universe_creator.asyncpg.create_pool', return_value=mock_pool):
            with patch.object(creator, '_get_symbols_with_both_datasets', new_callable=AsyncMock, return_value={"AAPL"}):
                with patch.object(creator, '_analyze_symbol_completeness', new_callable=AsyncMock, side_effect=asyncpg.PostgresError("Database error")):
                    
                    # Should handle the error gracefully and not crash
                    with pytest.raises(asyncpg.PostgresError):
                        await creator.analyze_data_completeness()

    def test_quality_thresholds_configuration(self):
        """Test that quality thresholds can be configured."""
        creator = DataCompleteUniverseCreator()
        
        # Test default thresholds
        assert creator.min_daily_completeness == 0.95
        assert creator.min_minute_completeness == 0.85
        assert creator.min_overall_quality == 0.80
        
        # Test threshold modification
        creator.min_daily_completeness = 0.90
        creator.min_minute_completeness = 0.80
        creator.min_overall_quality = 0.75
        
        assert creator.min_daily_completeness == 0.90
        assert creator.min_minute_completeness == 0.80
        assert creator.min_overall_quality == 0.75

    def test_gin_configurable_decorator(self):
        """Test that the class can be configured with gin."""
        # The class is marked as gin.configurable in the source
        # This test verifies it can be instantiated (basic gin functionality)
        creator = DataCompleteUniverseCreator()
        assert creator.min_years == 5