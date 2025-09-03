"""
Test suite for Data Validation Reporter

Tests data quality validation, anomaly detection, and Slack reporting.
"""

import pytest
import asyncio
import asyncpg
from datetime import date, datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
import json

from src.monitoring.data_validation_reporter import (
    DataValidationReporter,
    ValidationIssue,
    ValidationReport,
    StockInfo,
    run_daily_validation_report
)


class TestDataValidationReporter:
    """Test data validation reporter functionality."""
    
    @pytest.fixture
    def mock_pool(self):
        """Mock database pool."""
        pool = Mock()
        conn = AsyncMock()
        
        # Create async context manager mock
        async_context = AsyncMock()
        async_context.__aenter__.return_value = conn
        async_context.__aexit__.return_value = None
        pool.acquire.return_value = async_context
        
        return pool, conn
    
    @pytest.fixture
    def reporter(self, mock_pool):
        """Create test reporter instance."""
        pool, _ = mock_pool
        return DataValidationReporter(
            pool=pool,
            slack_webhook_url="https://hooks.slack.com/test",
            slack_channel="#ats-dev"
        )
    
    def test_is_trading_day(self, reporter):
        """Test trading day detection."""
        # Weekday (Monday)
        assert reporter.is_trading_day(date(2024, 8, 19)) == True
        
        # Weekend (Saturday)
        assert reporter.is_trading_day(date(2024, 8, 17)) == False
        
        # Weekend (Sunday)
        assert reporter.is_trading_day(date(2024, 8, 18)) == False
        
        # Independence Day (July 4th - US holiday)
        assert reporter.is_trading_day(date(2024, 7, 4)) == False
    
    def test_get_expected_trading_days(self, reporter):
        """Test expected trading days calculation."""
        # Week with Labor Day (Sept 2, 2024)
        start_date = date(2024, 8, 30)  # Friday
        end_date = date(2024, 9, 6)     # Friday
        
        trading_days = reporter.get_expected_trading_days(start_date, end_date)
        
        # Should include: Aug 30, Sep 3, 4, 5, 6 (skip weekend + Labor Day)
        expected_days = [
            date(2024, 8, 30),  # Friday
            date(2024, 9, 3),   # Tuesday (skip Labor Day Monday)
            date(2024, 9, 4),   # Wednesday
            date(2024, 9, 5),   # Thursday
            date(2024, 9, 6)    # Friday
        ]
        
        assert trading_days == expected_days
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_stock_info(self, reporter, mock_pool):
        """Test stock information retrieval."""
        pool, conn = mock_pool
        
        # Mock database response
        conn.fetchrow.return_value = {
            'symbol': 'AAPL',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'listing_date': date(1980, 12, 12),
            'delisting_date': None,
            'sector': 'Technology',
            'is_active': True
        }
        
        stock_info = await reporter.get_stock_info('AAPL')
        
        assert stock_info.symbol == 'AAPL'
        assert stock_info.name == 'Apple Inc.'
        assert stock_info.exchange == 'NASDAQ'
        assert stock_info.sector == 'Technology'
        assert stock_info.is_active == True
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_data_coverage(self, reporter, mock_pool):
        """Test data coverage calculation."""
        pool, conn = mock_pool
        
        # Mock database response
        conn.fetch.return_value = [
            {'trade_date': date(2024, 8, 19), 'bar_count': 390},
            {'trade_date': date(2024, 8, 20), 'bar_count': 350},
            {'trade_date': date(2024, 8, 21), 'bar_count': 390}
        ]
        
        coverage = await reporter.get_data_coverage(
            'AAPL', date(2024, 8, 19), date(2024, 8, 21)
        )
        
        assert coverage[date(2024, 8, 19)] == 390
        assert coverage[date(2024, 8, 20)] == 350
        assert coverage[date(2024, 8, 21)] == 390
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_price_anomalies(self, reporter, mock_pool):
        """Test price anomaly detection."""
        pool, conn = mock_pool
        
        # Mock database response with price spike
        conn.fetch.return_value = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 8, 19, 10, 30),
                'close': 220.00,
                'prev_close': 200.00,
                'change_percent': 10.0  # 10% increase
            },
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 8, 19, 10, 31),
                'close': 180.00,
                'prev_close': 220.00,
                'change_percent': 18.18  # Major drop
            }
        ]
        
        issues = await reporter.get_price_anomalies(
            'AAPL', date(2024, 8, 19), date(2024, 8, 19)
        )
        
        assert len(issues) == 2
        assert issues[0].issue_type == "price_anomaly"
        assert issues[0].severity == "warning"  # 10% change
        assert issues[1].severity == "warning"  # < 20% threshold
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_ohlc_validation_issues(self, reporter, mock_pool):
        """Test OHLC validation."""
        pool, conn = mock_pool
        
        # Mock database response with OHLC violations
        conn.fetch.return_value = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 8, 19, 10, 30),
                'open': 200.00,
                'high': 190.00,  # High < open (violation)
                'low': 195.00,   # Low > high (violation)
                'close': 198.00
            }
        ]
        
        issues = await reporter.get_ohlc_validation_issues(
            'AAPL', date(2024, 8, 19), date(2024, 8, 19)
        )
        
        assert len(issues) == 1
        assert issues[0].issue_type == "ohlc_violation"
        assert issues[0].severity == "critical"
        assert "high (190.0) < low (195.0)" in issues[0].description
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_validate_symbol_data(self, reporter, mock_pool):
        """Test comprehensive symbol validation."""
        pool, conn = mock_pool
        
        # Mock responses for different validation checks
        conn.fetchrow.return_value = {
            'symbol': 'AAPL',
            'name': 'Apple Inc.',
            'exchange': 'NASDAQ',
            'listing_date': date(1980, 1, 1),
            'delisting_date': None,
            'sector': 'Technology',
            'is_active': True
        }
        
        # Mock data coverage (missing one day)
        conn.fetch.side_effect = [
            # Data coverage
            [
                {'trade_date': date(2024, 8, 19), 'bar_count': 390},
                # Missing 2024-8-20
                {'trade_date': date(2024, 8, 21), 'bar_count': 200}  # Incomplete
            ],
            # Quality scores
            [
                {'trade_date': date(2024, 8, 19), 'avg_quality': 0.95},
                {'trade_date': date(2024, 8, 21), 'avg_quality': 0.4}  # Low quality
            ],
            # Price anomalies
            [],
            # OHLC violations
            []
        ]
        
        issues = await reporter.validate_symbol_data(
            'AAPL', date(2024, 8, 19), date(2024, 8, 21)
        )
        
        # Should find: 1 missing day + 1 incomplete day + 1 low quality
        issue_types = [issue.issue_type for issue in issues]
        assert "missing_data" in issue_types
        assert "incomplete_data" in issue_types
        assert "low_quality" in issue_types
    
    def test_format_slack_report(self, reporter):
        """Test Slack report formatting."""
        # Create test report
        report = ValidationReport(
            report_date=date(2024, 8, 19),
            total_issues=5,
            critical_issues=2,
            warning_issues=3,
            issues_by_date={date(2024, 8, 19): 3, date(2024, 8, 20): 2},
            issues_by_stock={'AAPL': 3, 'MSFT': 2},
            stock_coverage={'AAPL': 95.0, 'MSFT': 98.0},
            data_quality_scores={'AAPL': 0.85, 'MSFT': 0.92},
            detailed_issues=[
                ValidationIssue(
                    symbol='AAPL',
                    date=date(2024, 8, 19),
                    issue_type='missing_data',
                    severity='critical',
                    description='No data available'
                )
            ],
            summary_stats={
                'total_symbols': 2,
                'avg_coverage': 96.5,
                'avg_quality': 0.885
            }
        )
        
        slack_payload = reporter.format_slack_report(report)
        
        assert "🔴" in slack_payload['text']  # Critical issues indicator
        assert "CRITICAL ISSUES DETECTED" in slack_payload['text']
        assert slack_payload['channel'] == "#ats-dev"
        assert len(slack_payload['blocks']) > 0
        
        # Check summary block
        summary_text = slack_payload['blocks'][0]['text']['text']
        assert "Total Issues: 5" in summary_text
        assert "Critical: 2" in summary_text
        assert "Average Coverage: 96.5%" in summary_text
    
    @patch('requests.post')
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_post_to_slack_success(self, mock_post, reporter):
        """Test successful Slack posting."""
        mock_post.return_value.status_code = 200
        
        report = ValidationReport(
            report_date=date(2024, 8, 19),
            total_issues=0,
            critical_issues=0,
            warning_issues=0,
            issues_by_date={},
            issues_by_stock={},
            stock_coverage={},
            data_quality_scores={},
            detailed_issues=[],
            summary_stats={'total_symbols': 0, 'avg_coverage': 100, 'avg_quality': 1.0}
        )
        
        result = await reporter.post_to_slack(report)
        
        assert result == True
        mock_post.assert_called_once()
        
        # Verify payload
        call_args = mock_post.call_args
        payload = call_args[1]['json']
        assert payload['channel'] == "#ats-dev"
        assert "🟢" in payload['text']  # Success indicator
    
    @patch('requests.post')
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_post_to_slack_failure(self, mock_post, reporter):
        """Test Slack posting failure."""
        mock_post.return_value.status_code = 500
        mock_post.return_value.text = "Internal Server Error"
        
        report = ValidationReport(
            report_date=date(2024, 8, 19),
            total_issues=0,
            critical_issues=0,
            warning_issues=0,
            issues_by_date={},
            issues_by_stock={},
            stock_coverage={},
            data_quality_scores={},
            detailed_issues=[],
            summary_stats={'total_symbols': 0, 'avg_coverage': 100, 'avg_quality': 1.0}
        )
        
        result = await reporter.post_to_slack(report)
        
        assert result == False
    
    def test_save_report(self, reporter, tmp_path):
        """Test report saving to file."""
        report = ValidationReport(
            report_date=date(2024, 8, 19),
            total_issues=1,
            critical_issues=1,
            warning_issues=0,
            issues_by_date={date(2024, 8, 19): 1},
            issues_by_stock={'AAPL': 1},
            stock_coverage={'AAPL': 95.0},
            data_quality_scores={'AAPL': 0.85},
            detailed_issues=[
                ValidationIssue(
                    symbol='AAPL',
                    date=date(2024, 8, 19),
                    issue_type='missing_data',
                    severity='critical',
                    description='Test issue',
                    expected_bars=390,
                    actual_bars=0,
                    missing_bars=390
                )
            ],
            summary_stats={'total_symbols': 1, 'avg_coverage': 95.0, 'avg_quality': 0.85}
        )
        
        file_path = reporter.save_report(report, str(tmp_path))
        
        # Verify file was created
        assert tmp_path / "data_validation_report_2024-08-19.json" == file_path
        
        # Verify content
        with open(file_path) as f:
            saved_data = json.load(f)
        
        assert saved_data['report_date'] == '2024-08-19'
        assert saved_data['total_issues'] == 1
        assert saved_data['critical_issues'] == 1
        assert len(saved_data['detailed_issues']) == 1


@pytest.mark.asyncio
class TestValidationIntegration:
    """Integration tests for validation system."""
    
    @patch('src.monitoring.data_validation_reporter.asyncpg.create_pool')
    @pytest.mark.asyncio
    async def test_run_daily_validation_report(self, mock_create_pool):
        """Test complete validation report workflow."""
        # Mock database pool
        mock_pool = AsyncMock()
        mock_create_pool.return_value = mock_pool
        
        # Mock database responses
        mock_conn = AsyncMock()
        async_context = AsyncMock()
        async_context.__aenter__.return_value = mock_conn
        async_context.__aexit__.return_value = None
        mock_pool.acquire.return_value = async_context
        
        # Active symbols
        mock_conn.fetch.return_value = [
            {'symbol': 'AAPL'},
            {'symbol': 'MSFT'}
        ]
        
        # Stock info
        mock_conn.fetchrow.side_effect = [
            {
                'symbol': 'AAPL',
                'name': 'Apple Inc.',
                'exchange': 'NASDAQ',
                'listing_date': date(1980, 1, 1),
                'delisting_date': None,
                'sector': 'Technology',
                'is_active': True
            },
            {
                'symbol': 'MSFT',
                'name': 'Microsoft Corp.',
                'exchange': 'NASDAQ',
                'listing_date': date(1986, 3, 13),
                'delisting_date': None,
                'sector': 'Technology',
                'is_active': True
            }
        ]
        
        with patch('src.monitoring.data_validation_reporter.DataValidationReporter.post_to_slack', return_value=True):
            report = await run_daily_validation_report(
                db_url="postgresql://test",
                symbols=['AAPL', 'MSFT'],
                slack_webhook_url="https://hooks.slack.com/test",
                post_to_slack=False,
                save_to_file=False
            )
        
        assert report.report_date == date.today()
        assert isinstance(report.total_issues, int)
        assert isinstance(report.summary_stats, dict)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])