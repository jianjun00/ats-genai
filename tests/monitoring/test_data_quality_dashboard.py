"""
Tests for data quality monitoring dashboard.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock
import asyncpg

# Mock plotly and streamlit before importing the module
with patch.dict('sys.modules', {
    'plotly': Mock(),
    'plotly.graph_objects': Mock(),
    'plotly.express': Mock(),
    'plotly.subplots': Mock(),
    'streamlit': Mock()
}):
    from infrastructure.monitoring.legacy.data_quality_dashboard import (
        DataQualityLevel,
        DataQualityMetric,
        DataQualityReport,
        DataQualityMonitor,
        DataQualityDashboard,
        run_data_quality_dashboard
    )

@pytest.fixture
def mock_connection_pool():
    """Mock database connection pool."""
    pool = Mock(spec=asyncpg.Pool)
    conn = Mock(spec=asyncpg.Connection)

    # Create async context manager mock
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = conn
    context_manager.__aexit__.return_value = None

    pool.acquire.return_value = context_manager
    return pool, conn

@pytest.fixture
def mock_env():
    """Mock environment configuration."""
    env = Mock()
    env.get_table_name.side_effect = lambda x: f"test_{x}"
    return env

@pytest.fixture
def sample_quality_metric():
    """Sample data quality metric."""
    return DataQualityMetric(
        metric_name="completeness_check",
        table_name="test_daily_price_polygon",
        column_name="close",
        metric_value=0.95,
        threshold_warning=0.90,
        threshold_critical=0.80,
        quality_level=DataQualityLevel.GOOD,
        message="Column completeness is good",
        timestamp=datetime(2024, 1, 15, 10, 30, 0),
        metadata={"total_records": 10000, "null_count": 500}
    )

@pytest.fixture
def sample_quality_report(sample_quality_metric):
    """Sample data quality report."""
    return DataQualityReport(
        report_timestamp=datetime(2024, 1, 15, 10, 30, 0),
        overall_score=0.92,
        overall_level=DataQualityLevel.GOOD,
        metrics=[sample_quality_metric],
        summary_stats={
            "total_tables": 3,
            "total_metrics": 15,
            "avg_score": 0.92,
            "high_quality_tables": 2,
            "medium_quality_tables": 1,
            "low_quality_tables": 0
        },
        recommendations=[
            "Monitor null values in test_daily_price_polygon.volume",
            "Consider adding data validation for price ranges"
        ]
    )

class TestDataQualityLevel:
    """Test DataQualityLevel enum."""

    def test_quality_level_values(self):
        """Test DataQualityLevel enum values."""
        assert DataQualityLevel.EXCELLENT.value == "excellent"
        assert DataQualityLevel.GOOD.value == "good"
        assert DataQualityLevel.WARNING.value == "warning"
        assert DataQualityLevel.CRITICAL.value == "critical"
        assert DataQualityLevel.FAILURE.value == "failure"

class TestDataQualityMetric:
    """Test DataQualityMetric dataclass."""

    def test_data_quality_metric_creation(self, sample_quality_metric):
        """Test DataQualityMetric creation."""
        metric = sample_quality_metric

        assert metric.metric_name == "completeness_check"
        assert metric.table_name == "test_daily_price_polygon"
        assert metric.column_name == "close"
        assert metric.metric_value == 0.95
        assert metric.threshold_warning == 0.90
        assert metric.threshold_critical == 0.80
        assert metric.quality_level == DataQualityLevel.GOOD
        assert "completeness" in metric.message.lower()
        assert isinstance(metric.timestamp, datetime)
        assert isinstance(metric.metadata, dict)

    def test_data_quality_metric_with_none_column(self):
        """Test DataQualityMetric with None column (table-level metric)."""
        metric = DataQualityMetric(
            metric_name="row_count",
            table_name="test_table",
            column_name=None,
            metric_value=10000,
            threshold_warning=1000,
            threshold_critical=100,
            quality_level=DataQualityLevel.GOOD,
            message="Table has adequate row count",
            timestamp=datetime.now(),
            metadata={}
        )

        assert metric.column_name is None
        assert metric.metric_value == 10000

class TestDataQualityReport:
    """Test DataQualityReport dataclass."""

    def test_data_quality_report_creation(self, sample_quality_report):
        """Test DataQualityReport creation."""
        report = sample_quality_report

        assert isinstance(report.report_timestamp, datetime)
        assert report.overall_score == 0.92
        assert report.overall_level == DataQualityLevel.GOOD
        assert len(report.metrics) == 1
        assert isinstance(report.summary_stats, dict)
        assert len(report.recommendations) == 2

        # Check summary stats
        assert report.summary_stats["total_tables"] == 3
        assert report.summary_stats["avg_score"] == 0.92

    def test_data_quality_report_empty_metrics(self):
        """Test DataQualityReport with empty metrics."""
        report = DataQualityReport(
            report_timestamp=datetime.now(),
            overall_score=0.0,
            overall_level=DataQualityLevel.CRITICAL,
            metrics=[],
            summary_stats={},
            recommendations=[]
        )

        assert len(report.metrics) == 0
        assert len(report.recommendations) == 0
        assert report.overall_score == 0.0

class TestDataQualityMonitor:
    """Test DataQualityMonitor functionality."""

    def test_monitor_initialization(self, mock_connection_pool, mock_env):
        """Test monitor initialization."""
        pool, conn = mock_connection_pool

        monitor = DataQualityMonitor(pool, mock_env)

        assert monitor.pool == pool
        assert monitor.env == mock_env
        assert isinstance(monitor.quality_thresholds, dict)
        assert 'completeness' in monitor.quality_thresholds
        assert 'freshness_hours' in monitor.quality_thresholds

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_check_table_completeness(self, mock_connection_pool, mock_env):
        """Test table completeness check."""
        pool, conn = mock_connection_pool

        # Mock query results
        conn.fetchrow.side_effect = [
            {'total_records': 10000},  # Total count
            {'null_count': 500}  # Null count for column
        ]

        monitor = DataQualityMonitor(pool, mock_env)

        metric = await monitor._check_table_completeness("daily_price_polygon", "close")

        assert isinstance(metric, DataQualityMetric)
        assert metric.metric_name == "completeness"
        assert metric.table_name == "test_daily_price_polygon"
        assert metric.column_name == "close"
        assert metric.metric_value == 0.95  # (10000 - 500) / 10000
        assert metric.quality_level in [DataQualityLevel.GOOD, DataQualityLevel.WARNING, DataQualityLevel.WARNING]

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_check_table_completeness_no_data(self, mock_connection_pool, mock_env):
        """Test table completeness check with no data."""
        pool, conn = mock_connection_pool

        # Mock empty table
        conn.fetchrow.side_effect = [
            {'total_records': 0},  # No records
            {'null_count': 0}
        ]

        monitor = DataQualityMonitor(pool, mock_env)

        metric = await monitor._check_table_completeness("empty_table", "close")

        assert metric.metric_value == 0.0
        assert metric.quality_level == DataQualityLevel.CRITICAL

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_check_data_freshness(self, mock_connection_pool, mock_env):
        """Test data freshness check."""
        pool, conn = mock_connection_pool

        # Mock recent data (2 hours old)
        conn.fetchrow.return_value = {'hours_since_update': 2.0}

        monitor = DataQualityMonitor(pool, mock_env)

        metric = await monitor._check_data_freshness("daily_price_polygon")

        assert isinstance(metric, DataQualityMetric)
        assert metric.metric_name == "freshness"
        assert metric.metric_value == 2.0
        assert metric.quality_level in [DataQualityLevel.GOOD, DataQualityLevel.WARNING]

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_check_data_freshness_stale(self, mock_connection_pool, mock_env):
        """Test data freshness check with stale data."""
        pool, conn = mock_connection_pool

        # Mock stale data (25 hours old)
        conn.fetchrow.return_value = {'hours_since_update': 25.0}

        monitor = DataQualityMonitor(pool, mock_env)

        metric = await monitor._check_data_freshness("daily_price_polygon")

        assert metric.metric_value == 25.0
        assert metric.quality_level in [DataQualityLevel.WARNING, DataQualityLevel.CRITICAL]

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_check_duplicate_records(self, mock_connection_pool, mock_env):
        """Test duplicate records check."""
        pool, conn = mock_connection_pool

        # Mock some duplicates
        conn.fetchrow.side_effect = [
            {'total_records': 10000},
            {'duplicate_count': 50}
        ]

        monitor = DataQualityMonitor(pool, mock_env)

        metric = await monitor._check_duplicate_records("daily_price_polygon")

        assert isinstance(metric, DataQualityMetric)
        assert metric.metric_name == "duplicates"
        assert metric.metric_value == 0.005  # 50 / 10000
        assert metric.quality_level in [DataQualityLevel.GOOD, DataQualityLevel.WARNING]

    def test_determine_quality_level(self, mock_connection_pool, mock_env):
        """Test quality level determination."""
        monitor = DataQualityMonitor(None, None)

        # Test high quality
        level = monitor._determine_quality_level(0.95, 0.90, 0.80)
        assert level == DataQualityLevel.GOOD

        # Test medium quality
        level = monitor._determine_quality_level(0.85, 0.90, 0.80)
        assert level == DataQualityLevel.WARNING

        # Test low quality
        level = monitor._determine_quality_level(0.75, 0.90, 0.80)
        assert level == DataQualityLevel.WARNING

        # Test critical quality
        level = monitor._determine_quality_level(0.70, 0.90, 0.80)
        assert level == DataQualityLevel.CRITICAL

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_quality_report(self, mock_connection_pool, mock_env):
        """Test quality report generation."""
        pool, conn = mock_connection_pool

        # Mock table list
        conn.fetch.return_value = [
            {'table_name': 'test_daily_price_polygon'},
            {'table_name': 'test_instruments'}
        ]

        monitor = DataQualityMonitor(pool, mock_env)

        # Mock individual metric checks
        sample_metric = DataQualityMetric(
            "test_metric", "test_table", "test_col", 0.95, 0.90, 0.80,
            DataQualityLevel.GOOD, "Test message", datetime.now(), {}
        )

        with patch.object(monitor, '_check_table_completeness', new_callable=AsyncMock) as mock_completeness, \
             patch.object(monitor, '_check_data_freshness', new_callable=AsyncMock) as mock_freshness, \
             patch.object(monitor, '_check_duplicate_records', new_callable=AsyncMock) as mock_duplicates:

            mock_completeness.return_value = sample_metric
            mock_freshness.return_value = sample_metric
            mock_duplicates.return_value = sample_metric

            report = await monitor.generate_quality_report()

            assert isinstance(report, DataQualityReport)
            assert len(report.metrics) > 0
            assert 0 <= report.overall_score <= 1
            assert isinstance(report.summary_stats, dict)
            assert isinstance(report.recommendations, list)

class TestDataQualityDashboard:
    """Test DataQualityDashboard functionality."""

    def test_dashboard_initialization(self, mock_connection_pool, mock_env):
        """Test dashboard initialization."""
        monitor = DataQualityMonitor(None, None)
        dashboard = DataQualityDashboard(monitor)

        assert dashboard.monitor == monitor

    def test_render_dashboard(self, mock_connection_pool, mock_env, sample_quality_report):
        """Test dashboard rendering."""
        monitor = DataQualityMonitor(None, None)
        dashboard = DataQualityDashboard(monitor)

        # Mock Streamlit
        with patch('monitoring.data_quality_dashboard.st') as mock_st:
            mock_st.title = Mock()
            mock_st.columns = Mock(return_value=[Mock(), Mock(), Mock()])
            mock_st.metric = Mock()
            mock_st.dataframe = Mock()
            mock_st.plotly_chart = Mock()
            mock_st.success = Mock()
            mock_st.warning = Mock()
            mock_st.error = Mock()
            mock_st.subheader = Mock()
            mock_st.button = Mock(return_value=False)
            mock_st.download_button = Mock()

            # Should not raise an exception
            dashboard.render_dashboard(sample_quality_report)

            # Verify basic Streamlit calls were made
            mock_st.title.assert_called()
            mock_st.columns.assert_called()

class TestConvenienceFunction:
    """Test convenience function."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_run_data_quality_dashboard(self, mock_connection_pool, mock_env):
        """Test convenience function."""
        pool, conn = mock_connection_pool

        # Mock Streamlit
        with patch('monitoring.data_quality_dashboard.st') as mock_st:
            mock_st.title = Mock()
            mock_st.columns = Mock(return_value=[Mock(), Mock(), Mock()])
            mock_st.metric = Mock()
            mock_st.dataframe = Mock()
            mock_st.plotly_chart = Mock()
            mock_st.success = Mock()
            mock_st.subheader = Mock()
            mock_st.button = Mock(return_value=False)

            # Mock the monitor's generate_quality_report method
            with patch.object(DataQualityMonitor, 'generate_quality_report', new_callable=AsyncMock) as mock_generate:
                sample_metric = DataQualityMetric(
                    "test_metric", "test_table", "test_col", 0.95, 0.90, 0.80,
                    DataQualityLevel.GOOD, "Test message", datetime.now(), {}
                )
                mock_report = DataQualityReport(
                    datetime.now(), 0.95, DataQualityLevel.GOOD, [sample_metric], {}, []
                )
                mock_generate.return_value = mock_report

                result = await run_data_quality_dashboard(pool, mock_env)

                assert isinstance(result, DataQualityReport)
                assert result.overall_score == 0.95

class TestErrorHandlingAndEdgeCases:
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_connection_error(self, mock_connection_pool, mock_env):
        """Test handling of database connection errors."""
        pool, conn = mock_connection_pool

        # Mock connection error
        conn.fetchrow.side_effect = asyncpg.ConnectionDoesNotExistError("Connection lost")

        monitor = DataQualityMonitor(pool, mock_env)

        # Should handle error gracefully
        with pytest.raises(asyncpg.ConnectionDoesNotExistError):
            await monitor._check_table_completeness("test_table", "test_col")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_invalid_table_name(self, mock_connection_pool, mock_env):
        """Test handling of invalid table names."""
        pool, conn = mock_connection_pool

        # Mock table not found
        conn.fetchrow.side_effect = asyncpg.UndefinedTableError("Table does not exist")

        monitor = DataQualityMonitor(pool, mock_env)

        with pytest.raises(asyncpg.UndefinedTableError):
            await monitor._check_table_completeness("nonexistent_table", "test_col")

    def test_quality_level_edge_cases(self, mock_connection_pool, mock_env):
        """Test quality level determination edge cases."""
        monitor = DataQualityMonitor(None, None)

        # Test with exactly threshold values
        level = monitor._determine_quality_level(0.90, 0.90, 0.80)
        assert level == DataQualityLevel.GOOD

        level = monitor._determine_quality_level(0.80, 0.90, 0.80)
        assert level == DataQualityLevel.WARNING

        # Test with extreme values
        level = monitor._determine_quality_level(0.0, 0.90, 0.80)
        assert level == DataQualityLevel.CRITICAL

        level = monitor._determine_quality_level(1.0, 0.90, 0.80)
        assert level == DataQualityLevel.GOOD

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_empty_database(self, mock_connection_pool, mock_env):
        """Test monitoring with empty database."""
        pool, conn = mock_connection_pool

        # Mock empty database (no tables)
        conn.fetch.return_value = []

        monitor = DataQualityMonitor(pool, mock_env)

        report = await monitor.generate_quality_report()

        assert isinstance(report, DataQualityReport)
        assert len(report.metrics) == 0
        assert report.overall_score == 0.0
        assert report.overall_level == DataQualityLevel.CRITICAL

    def test_dashboard_with_no_metrics(self, mock_connection_pool, mock_env):
        """Test dashboard with no metrics."""
        monitor = DataQualityMonitor(None, None)
        dashboard = DataQualityDashboard(monitor)

        empty_report = DataQualityReport(
            datetime.now(), 0.0, DataQualityLevel.CRITICAL, [], {}, []
        )

        with patch('monitoring.data_quality_dashboard.st') as mock_st:
            mock_st.title = Mock()
            mock_st.warning = Mock()
            mock_st.columns = Mock(return_value=[Mock(), Mock(), Mock()])
            mock_st.metric = Mock()
            mock_st.subheader = Mock()
            mock_st.button = Mock(return_value=False)

            # Should handle empty report gracefully
            dashboard.render_dashboard(empty_report)

            # Should show warning for no data
            mock_st.warning.assert_called()

@pytest.mark.integration
class TestDataQualityIntegration:
    """Test integration scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_monitoring_workflow(self, mock_connection_pool, mock_env):
        """Test complete data quality monitoring workflow."""
        pool, conn = mock_connection_pool

        # Setup comprehensive mock data
        conn.fetch.return_value = [
            {'table_name': 'test_daily_price_polygon'},
            {'table_name': 'test_instruments'}
        ]

        # Mock different quality scenarios for each table
        mock_responses = [
            # daily_price_polygon - good quality
            {'total_records': 100000}, {'null_count': 500},  # completeness
            {'hours_since_update': 2.0},  # freshness
            {'total_records': 100000}, {'duplicate_count': 10},  # duplicates

            # instruments - medium quality
            {'total_records': 5000}, {'null_count': 250},  # completeness
            {'hours_since_update': 8.0},  # freshness
            {'total_records': 5000}, {'duplicate_count': 50},  # duplicates
        ]

        conn.fetchrow.side_effect = mock_responses

        monitor = DataQualityMonitor(pool, mock_env)

        # Generate complete report
        report = await monitor.generate_quality_report()

        # Validate results
        assert isinstance(report, DataQualityReport)
        assert len(report.metrics) > 0
        assert 0 <= report.overall_score <= 1
        assert isinstance(report.summary_stats, dict)
        assert isinstance(report.recommendations, list)

        # Check that we have metrics for both tables
        table_names = {m.table_name for m in report.metrics}
        assert 'test_daily_price_polygon' in table_names
        assert 'test_instruments' in table_names

        # Check metric types
        metric_names = {m.metric_name for m in report.metrics}
        assert 'completeness' in metric_names
        assert 'freshness' in metric_names
        assert 'duplicates' in metric_names

if __name__ == "__main__":
    pytest.main([__file__])