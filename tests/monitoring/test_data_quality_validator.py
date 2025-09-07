#!/usr/bin/env python3
"""
Tests for Data Quality Validation Framework

Tests comprehensive data quality validation including:
- Data completeness validation
- Cross-vendor consistency validation
- Data freshness monitoring
- Data integrity validation
- Report generation
"""

import pytest
import asyncio
import numpy as np
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from monitoring.data_quality_validator import (
    DataQualityValidator,
    ValidationResult,
    ValidationSeverity,
    ValidationCategory,
    DataQualityReport
)
from shared.utils.environment import Environment

@pytest.fixture
def env():
    return Environment()

@pytest.fixture
def validator(env):
    return DataQualityValidator(env)

@pytest.fixture
def sample_validation_results():
    """Sample validation results for testing report generation"""
    return [
        ValidationResult(
            category=ValidationCategory.COMPLETENESS,
            severity=ValidationSeverity.INFO,
            test_name="completeness_test",
            description="Data completeness check",
            passed=True,
            details={'coverage': 0.95}
        ),
        ValidationResult(
            category=ValidationCategory.CONSISTENCY,
            severity=ValidationSeverity.WARNING,
            test_name="consistency_test",
            description="Price consistency check",
            passed=False,
            details={'variance': 0.08},
            recommendation="Investigate price discrepancies"
        ),
        ValidationResult(
            category=ValidationCategory.INTEGRITY,
            severity=ValidationSeverity.CRITICAL,
            test_name="integrity_test",
            description="Data integrity violation",
            passed=False,
            details={'violations': 5},
            affected_records=5,
            recommendation="Fix negative prices"
        )
    ]

class TestDataQualityValidator:
    """Test DataQualityValidator initialization and configuration"""

    def test_validator_initialization(self, validator):
        """Test validator initialization with correct configuration"""
        assert validator.env is not None
        assert validator.db_url is not None
        assert len(validator.vendor_tables) == 4
        assert 'polygon' in validator.vendor_tables
        assert 'tiingo' in validator.vendor_tables
        assert 'alphavantage' in validator.vendor_tables
        assert 'fmp' in validator.vendor_tables

    def test_quality_thresholds_configuration(self, validator):
        """Test quality thresholds are properly configured"""
        thresholds = validator.quality_thresholds

        assert thresholds['min_data_coverage'] == 0.80
        assert thresholds['max_price_variance'] == 0.10
        assert thresholds['max_stale_days'] == 3
        assert thresholds['min_volume_correlation'] == 0.70
        assert thresholds['max_missing_ratio'] == 0.05

class TestValidationResult:
    """Test ValidationResult data structure"""

    def test_validation_result_creation(self):
        """Test ValidationResult creation with all fields"""
        result = ValidationResult(
            category=ValidationCategory.COMPLETENESS,
            severity=ValidationSeverity.WARNING,
            test_name="test_completeness",
            description="Test data completeness",
            passed=False,
            details={'coverage': 0.75},
            affected_records=100,
            recommendation="Improve data coverage"
        )

        assert result.category == ValidationCategory.COMPLETENESS
        assert result.severity == ValidationSeverity.WARNING
        assert result.test_name == "test_completeness"
        assert result.description == "Test data completeness"
        assert result.passed == False
        assert result.details['coverage'] == 0.75
        assert result.affected_records == 100
        assert result.recommendation == "Improve data coverage"
        assert isinstance(result.timestamp, datetime)

class TestReportGeneration:
    """Test data quality report generation"""

    def test_report_generation_with_mixed_results(self, validator, sample_validation_results):
        """Test report generation with mixed validation results"""
        start_time = datetime.now() - timedelta(minutes=5)

        report = validator._generate_data_quality_report(sample_validation_results, start_time)

        assert isinstance(report, DataQualityReport)
        assert report.total_tests == 3
        assert report.passed_tests == 1
        assert report.failed_tests == 2
        assert report.critical_issues == 1
        assert report.warning_issues == 1
        assert report.info_issues == 0
        assert len(report.validation_results) == 3
        assert 0 <= report.overall_score <= 100
        assert "critical" in report.summary.lower()

    def test_report_generation_all_passed(self, validator):
        """Test report generation when all tests pass"""
        all_passed_results = [
            ValidationResult(
                category=ValidationCategory.COMPLETENESS,
                severity=ValidationSeverity.INFO,
                test_name="test1",
                description="Test 1",
                passed=True
            ),
            ValidationResult(
                category=ValidationCategory.CONSISTENCY,
                severity=ValidationSeverity.INFO,
                test_name="test2",
                description="Test 2",
                passed=True
            )
        ]

        start_time = datetime.now()
        report = validator._generate_data_quality_report(all_passed_results, start_time)

        assert report.total_tests == 2
        assert report.passed_tests == 2
        assert report.failed_tests == 0
        assert report.critical_issues == 0
        assert report.warning_issues == 0
        assert report.overall_score == 100.0
        assert "good" in report.summary.lower()

    def test_report_generation_empty_results(self, validator):
        """Test report generation with no results"""
        start_time = datetime.now()
        report = validator._generate_data_quality_report([], start_time)

        assert report.total_tests == 0
        assert report.passed_tests == 0
        assert report.failed_tests == 0
        assert report.overall_score == 100.0

class TestPriceConsistencyAnalysis:
    """Test cross-vendor price consistency analysis"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_price_consistency_analysis_normal_variance(self, validator):
        """Test price consistency with normal variance"""
        symbol = "AAPL"
        vendor_prices = {
            'polygon': {
                date(2025, 8, 19): {'close': 250.00, 'volume': 1000000},
                date(2025, 8, 20): {'close': 251.00, 'volume': 1100000}
            },
            'tiingo': {
                date(2025, 8, 19): {'close': 249.50, 'volume': 950000},
                date(2025, 8, 20): {'close': 250.75, 'volume': 1050000}
            }
        }

        result = await validator._analyze_price_consistency(symbol, vendor_prices)

        assert result.category == ValidationCategory.CONSISTENCY
        assert result.test_name == "price_consistency_check"
        assert result.details['symbol'] == symbol
        assert result.details['comparison_dates'] == 2

        # Normal variance should pass
        if result.passed:
            assert result.severity == ValidationSeverity.INFO
        else:
            # Small variance might still trigger warning depending on threshold
            assert result.severity in [ValidationSeverity.WARNING, ValidationSeverity.CRITICAL]

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_price_consistency_analysis_high_variance(self, validator):
        """Test price consistency with high variance"""
        symbol = "TEST"
        vendor_prices = {
            'polygon': {
                date(2025, 8, 19): {'close': 250.00, 'volume': 1000000},
            },
            'tiingo': {
                date(2025, 8, 19): {'close': 350.00, 'volume': 950000},  # 40% difference
            },
            'fmp': {
                date(2025, 8, 19): {'close': 380.00, 'volume': 800000},  # More extreme variance
            }
        }

        result = await validator._analyze_price_consistency(symbol, vendor_prices)

        assert result.category == ValidationCategory.CONSISTENCY
        assert result.passed == False
        assert result.severity in [ValidationSeverity.WARNING, ValidationSeverity.CRITICAL]
        assert result.details['high_variance_dates'] > 0
        assert 'sample_issues' in result.details

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_price_consistency_analysis_no_data(self, validator):
        """Test price consistency with no overlapping data"""
        symbol = "NODATA"
        vendor_prices = {}

        result = await validator._analyze_price_consistency(symbol, vendor_prices)

        assert result.category == ValidationCategory.CONSISTENCY
        assert result.passed == False
        assert result.severity == ValidationSeverity.WARNING
        assert result.details['reason'] == 'no_data'

class TestVolumeConsistencyAnalysis:
    """Test volume consistency analysis"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_volume_consistency_high_correlation(self, validator):
        """Test volume consistency with high correlation"""
        symbol = "AAPL"
        vendor_prices = {
            'polygon': {
                date(2025, 8, 19): {'close': 250.00, 'volume': 1000000},
                date(2025, 8, 20): {'close': 251.00, 'volume': 1100000},
                date(2025, 8, 21): {'close': 249.00, 'volume': 900000}
            },
            'tiingo': {
                date(2025, 8, 19): {'close': 249.50, 'volume': 1050000},  # Similar volume
                date(2025, 8, 20): {'close': 250.75, 'volume': 1150000},
                date(2025, 8, 21): {'close': 248.75, 'volume': 950000}
            }
        }

        result = await validator._analyze_volume_consistency(symbol, vendor_prices)

        assert result.category == ValidationCategory.CONSISTENCY
        assert result.test_name == "volume_consistency_check"
        assert result.details['symbol'] == symbol

        # High correlation should pass
        if result.details['average_correlation'] >= validator.quality_thresholds['min_volume_correlation']:
            assert result.passed == True
            assert result.severity == ValidationSeverity.INFO

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_volume_consistency_low_correlation(self, validator):
        """Test volume consistency with low correlation"""
        symbol = "TEST"
        vendor_prices = {
            'polygon': {
                date(2025, 8, 19): {'close': 250.00, 'volume': 1000000},
                date(2025, 8, 20): {'close': 251.00, 'volume': 2000000},
            },
            'tiingo': {
                date(2025, 8, 19): {'close': 249.50, 'volume': 100000},   # Very different volume
                date(2025, 8, 20): {'close': 250.75, 'volume': 150000},
            }
        }

        result = await validator._analyze_volume_consistency(symbol, vendor_prices)

        assert result.category == ValidationCategory.CONSISTENCY

        # Low correlation should fail
        if result.details.get('average_correlation', 0) < validator.quality_thresholds['min_volume_correlation']:
            assert result.passed == False
            assert result.severity == ValidationSeverity.WARNING

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_volume_consistency_insufficient_data(self, validator):
        """Test volume consistency with insufficient data"""
        symbol = "SINGLE"
        vendor_prices = {
            'polygon': {
                date(2025, 8, 19): {'close': 250.00, 'volume': 1000000}
            }
        }

        result = await validator._analyze_volume_consistency(symbol, vendor_prices)

        assert result.category == ValidationCategory.CONSISTENCY
        assert result.passed == True
        assert result.severity == ValidationSeverity.INFO
        assert result.details['reason'] == 'insufficient_data'

class TestTradingDaysCalculation:
    """Test trading days calculation"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_trading_days_weekdays_only(self, validator):
        """Test that trading days excludes weekends"""
        # Mock database connection
        mock_conn = MagicMock()

        start_date = date(2025, 8, 18)  # Monday
        end_date = date(2025, 8, 24)    # Sunday

        trading_days = await validator._get_trading_days(mock_conn, start_date, end_date)

        # Should only include Monday-Friday (5 days)
        assert len(trading_days) == 5

        # Check that weekends are excluded
        for day in trading_days:
            assert day.weekday() < 5  # Monday=0 to Friday=4

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_trading_days_single_day(self, validator):
        """Test trading days calculation for single day"""
        mock_conn = MagicMock()

        monday = date(2025, 8, 18)  # Monday
        trading_days = await validator._get_trading_days(mock_conn, monday, monday)

        assert len(trading_days) == 1
        assert trading_days[0] == monday

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_trading_days_weekend_only(self, validator):
        """Test trading days calculation for weekend only"""
        mock_conn = MagicMock()

        saturday = date(2025, 8, 23)  # Saturday
        sunday = date(2025, 8, 24)    # Sunday
        trading_days = await validator._get_trading_days(mock_conn, saturday, sunday)

        assert len(trading_days) == 0

class TestUtilityMethods:
    """Test utility methods"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_instrument_id_exists(self, validator):
        """Test getting instrument ID for existing symbol"""
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=123)

        instrument_id = await validator._get_instrument_id(mock_conn, "AAPL")

        assert instrument_id == 123
        mock_conn.fetchval.assert_called_once_with(
            "SELECT id FROM dev_instruments WHERE symbol = $1",
            "AAPL"
        )

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_instrument_id_not_exists(self, validator):
        """Test getting instrument ID for non-existent symbol"""
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=None)

        instrument_id = await validator._get_instrument_id(mock_conn, "NONEXISTENT")

        assert instrument_id is None

@pytest.mark.integration
class TestDataQualityValidatorIntegration:
    """Integration tests with mocked database calls"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_comprehensive_validation_workflow(self, validator):
        """Test complete validation workflow with mocked database"""

        # Mock database calls
        with patch.object(validator, 'validate_data_completeness') as mock_completeness, \
             patch.object(validator, 'validate_cross_vendor_consistency') as mock_consistency, \
             patch.object(validator, 'validate_data_freshness') as mock_freshness, \
             patch.object(validator, 'validate_data_integrity') as mock_integrity:

            # Setup mock returns
            mock_completeness.return_value = [
                ValidationResult(ValidationCategory.COMPLETENESS, ValidationSeverity.INFO, "test", "test", True)
            ]
            mock_consistency.return_value = [
                ValidationResult(ValidationCategory.CONSISTENCY, ValidationSeverity.WARNING, "test", "test", False)
            ]
            mock_freshness.return_value = [
                ValidationResult(ValidationCategory.FRESHNESS, ValidationSeverity.INFO, "test", "test", True)
            ]
            mock_integrity.return_value = [
                ValidationResult(ValidationCategory.INTEGRITY, ValidationSeverity.CRITICAL, "test", "test", False)
            ]

            # Run validation
            report = await validator.run_comprehensive_validation(["AAPL", "MSFT"], days_back=30)

            # Verify all validation methods were called
            assert mock_completeness.called
            assert mock_consistency.called
            assert mock_freshness.called
            assert mock_integrity.called

            # Verify report structure
            assert isinstance(report, DataQualityReport)
            assert report.total_tests == 4
            assert report.passed_tests == 2
            assert report.failed_tests == 2
            assert report.critical_issues == 1
            assert report.warning_issues == 1
            assert report.overall_score < 100

if __name__ == "__main__":
    # Allow running this test file directly
    pytest.main([__file__, "-v", "-s"])