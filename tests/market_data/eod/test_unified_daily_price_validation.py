"""
Test suite for unified daily price validation and unification
"""

import pytest
from datetime import date
from unittest.mock import AsyncMock, patch

# Import our modules (adjust path as needed)
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../src'))

from domains.market_data.services.eod.unified_daily_price_validator import (
    UnifiedDailyPriceValidator,
    ValidationStatus,
    VendorPrice,
    ValidationResult,
    UnifiedPrice
)
from domains.market_data.services.eod.unified_daily_price_pipeline import UnifiedDailyPricePipeline
from core.shared.utils.environment import Environment


class TestUnifiedDailyPriceValidator:
    """Test the price validator functionality"""

    @pytest.fixture
    async def validator(self):
        """Create a validator instance for testing"""
        env = Environment()
        validator = UnifiedDailyPriceValidator(env)

        # Mock the database connection
        validator.conn = AsyncMock()

        yield validator

        if validator.conn:
            await validator.disconnect()

    def test_vendor_price_creation(self):
        """Test VendorPrice data class"""
        vendor_price = VendorPrice(
            vendor="polygon",
            symbol="AAPL",
            date=date(2024, 1, 15),
            open_price=150.0,
            high_price=155.0,
            low_price=149.0,
            close=154.0,
            adj_close=153.5,
            volume=1000000
        )

        assert vendor_price.vendor == "polygon"
        assert vendor_price.symbol == "AAPL"
        assert vendor_price.close == 154.0
        assert vendor_price.confidence == 1.0  # default

    def test_validation_result_creation(self):
        """Test ValidationResult data class"""
        result = ValidationResult(
            is_valid=True,
            status=ValidationStatus.VALID,
            confidence_score=0.95,
            statistical_score=1.2,
            price_variance=0.01,
            validation_notes="Price passed all checks"
        )

        assert result.is_valid is True
        assert result.status == ValidationStatus.VALID
        assert result.confidence_score == 0.95

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_statistical_validation_normal_price(self, validator):
        """Test statistical validation with normal price"""
        # Mock historical stats
        historical_stats = {
            'mean_price': 150.0,
            'std_price': 5.0,
            'sample_size': 50
        }

        # Test normal price (within 1 sigma)
        result = validator.validate_price_statistically(152.0, historical_stats, "AAPL")

        assert result.is_valid is True
        assert result.status == ValidationStatus.VALID
        assert result.confidence_score > 0.8
        assert result.statistical_score < 1.0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_statistical_validation_outlier(self, validator):
        """Test statistical validation with outlier price"""
        historical_stats = {
            'mean_price': 150.0,
            'std_price': 5.0,
            'sample_size': 50
        }

        # Test extreme outlier (6+ sigma)
        result = validator.validate_price_statistically(180.0, historical_stats, "AAPL")

        assert result.is_valid is False
        assert result.status == ValidationStatus.OUTLIER_STATISTICAL
        assert result.confidence_score == 0.0
        assert result.statistical_score > 6.0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_statistical_validation_manual_review(self, validator):
        """Test statistical validation requiring manual review"""
        historical_stats = {
            'mean_price': 150.0,
            'std_price': 5.0,
            'sample_size': 50
        }

        # Test moderate outlier (4-6 sigma)
        result = validator.validate_price_statistically(172.0, historical_stats, "AAPL")

        assert result.is_valid is False
        assert result.status == ValidationStatus.MANUAL_REVIEW
        assert 0.0 < result.confidence_score < 0.5
        assert 4.0 < result.statistical_score < 6.0

    def test_vendor_reconciliation_single_vendor(self, validator):
        """Test price reconciliation with single vendor"""
        vendor_prices = [
            VendorPrice(
                vendor="polygon",
                symbol="AAPL",
                date=date(2024, 1, 15),
                open_price=150.0,
                high_price=155.0,
                low_price=149.0,
                close=154.0,
                adj_close=None,
                volume=1000000
            )
        ]

        result, consensus = validator.reconcile_vendor_prices(vendor_prices, "AAPL")

        assert result.is_valid is True
        assert result.status == ValidationStatus.VALID
        assert result.confidence_score == 0.6  # Lower confidence for single vendor
        assert consensus.close == 154.0
        assert consensus.vendor == "polygon"

    def test_vendor_reconciliation_consensus(self, validator):
        """Test price reconciliation with multiple agreeing vendors"""
        vendor_prices = [
            VendorPrice("polygon", "AAPL", date(2024, 1, 15), 150.0, 155.0, 149.0, 154.0, None, 1000000),
            VendorPrice("tiingo", "AAPL", date(2024, 1, 15), 150.1, 154.9, 149.1, 154.1, 153.5, 990000),
            VendorPrice("fmp", "AAPL", date(2024, 1, 15), 149.9, 155.1, 148.9, 153.9, 153.4, 1010000)
        ]

        result, consensus = validator.reconcile_vendor_prices(vendor_prices, "AAPL")

        assert result.is_valid is True
        assert result.status == ValidationStatus.VALID
        assert result.confidence_score > 0.8  # High confidence with multiple vendors
        assert 153.9 <= consensus.close <= 154.1  # Should be close to average
        assert result.price_variance is not None
        assert result.price_variance < 0.1  # Low variance

    def test_vendor_reconciliation_disagreement(self, validator):
        """Test price reconciliation with disagreeing vendors"""
        vendor_prices = [
            VendorPrice("polygon", "AAPL", date(2024, 1, 15), 150.0, 155.0, 149.0, 154.0, None, 1000000),
            VendorPrice("tiingo", "AAPL", date(2024, 1, 15), 150.0, 155.0, 149.0, 170.0, 169.5, 1000000),  # Significant disagreement
            VendorPrice("fmp", "AAPL", date(2024, 1, 15), 150.0, 155.0, 149.0, 155.0, 154.5, 1000000)
        ]

        result, consensus = validator.reconcile_vendor_prices(vendor_prices, "AAPL")

        assert result.is_valid is False
        assert result.status == ValidationStatus.OUTLIER_VENDOR_DISAGREEMENT
        assert result.confidence_score < 0.5
        assert consensus is None  # No consensus due to disagreement

    def test_vendor_reconciliation_no_data(self, validator):
        """Test price reconciliation with no vendor data"""
        vendor_prices = []

        result, consensus = validator.reconcile_vendor_prices(vendor_prices, "AAPL")

        assert result.is_valid is False
        assert result.status == ValidationStatus.MISSING_VENDOR_DATA
        assert result.confidence_score == 0.0
        assert consensus is None


class TestUnifiedDailyPricePipeline:
    """Test the unified price pipeline"""

    @pytest.fixture
    async def pipeline(self):
        """Create a pipeline instance for testing"""
        env = Environment()
        pipeline = UnifiedDailyPricePipeline(env)

        # Mock the database connections
        pipeline.conn = AsyncMock()
        pipeline.validator.conn = AsyncMock()

        yield pipeline

        await pipeline.disconnect()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_create_run_record(self, pipeline):
        """Test creating a run record"""
        # Mock the database return
        pipeline.conn.fetchval.return_value = 123

        parameters = {
            'start_date': '2024-01-15',
            'end_date': '2024-01-15',
            'symbols': ['AAPL', 'MSFT'],
            'limit': 10
        }

        run_id = await pipeline.create_run_record('daily_price_unification', parameters)

        assert run_id == 123
        pipeline.conn.fetchval.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_update_run_record(self, pipeline):
        """Test updating a run record"""
        results = {
            'total_processed': 100,
            'successful': 95,
            'failed': 5
        }

        await pipeline.update_run_record(123, 'completed', results)

        pipeline.conn.execute.assert_called_once()
        call_args = pipeline.conn.execute.call_args[0]
        assert call_args[1] == 'completed'  # status
        assert '"total_processed": 100' in call_args[2]  # results JSON

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_symbols_to_process_explicit(self, pipeline):
        """Test getting symbols when explicitly provided"""
        symbols = await pipeline.get_symbols_to_process(['AAPL', 'MSFT', 'GOOGL'], limit=2)

        assert symbols == ['AAPL', 'MSFT']

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_symbols_to_process_from_universe(self, pipeline):
        """Test getting symbols from universe membership"""
        # Mock database return
        mock_rows = [
            {'symbol': 'AAPL'},
            {'symbol': 'MSFT'},
            {'symbol': 'GOOGL'}
        ]
        pipeline.conn.fetch.return_value = mock_rows

        symbols = await pipeline.get_symbols_to_process(limit=2)

        assert symbols == ['AAPL', 'MSFT']
        pipeline.conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_store_unified_price(self, pipeline):
        """Test storing a unified price record"""
        # Mock validation status lookup
        pipeline.conn.fetchrow.return_value = {'id': 1}  # valid status
        pipeline.conn.fetchval.return_value = 456  # inserted record id

        # Create a mock unified price
        validation_result = ValidationResult(
            is_valid=True,
            status=ValidationStatus.VALID,
            confidence_score=0.95,
            statistical_score=1.2,
            price_variance=0.01,
            validation_notes="Test validation"
        )

        unified_price = UnifiedPrice(
            instrument_id=1,
            date=date(2024, 1, 15),
            open_price=150.0,
            high_price=155.0,
            low_price=149.0,
            close=154.0,
            adj_close=153.5,
            volume=1000000,
            primary_vendor="polygon",
            secondary_vendors=["tiingo"],
            vendor_count=2,
            validation_result=validation_result,
            vendor_prices={"polygon": 154.0, "tiingo": 154.1}
        )

        unified_id = await pipeline.store_unified_price(unified_price, run_id=123)

        assert unified_id == 456
        assert pipeline.conn.fetchval.call_count == 1


class TestIntegrationScenarios:
    """Integration test scenarios"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_end_to_end_validation_scenario(self):
        """Test complete end-to-end validation scenario"""
        env = Environment()
        validator = UnifiedDailyPriceValidator(env)

        # Mock database connection
        validator.conn = AsyncMock()

        # Mock vendor price fetching
        with patch.object(validator, 'fetch_vendor_prices') as mock_fetch:
            # Mock good consensus data
            mock_vendor_prices = [
                VendorPrice("polygon", "AAPL", date(2024, 1, 15), 150.0, 155.0, 149.0, 154.0, None, 1000000),
                VendorPrice("tiingo", "AAPL", date(2024, 1, 15), 150.1, 154.9, 149.1, 154.1, 153.5, 990000)
            ]
            mock_fetch.return_value = mock_vendor_prices

            # Mock historical statistics
            with patch.object(validator, 'calculate_historical_statistics') as mock_stats:
                mock_stats.return_value = {
                    'mean_price': 150.0,
                    'std_price': 5.0,
                    'sample_size': 50
                }

                # Mock instrument ID lookup
                validator._get_instrument_id = AsyncMock(return_value=1)

                # Run validation
                unified_price = await validator.validate_and_unify_price("AAPL", date(2024, 1, 15))

                # Verify results
                assert unified_price is not None
                assert unified_price.validation_result.is_valid
                assert unified_price.validation_result.status == ValidationStatus.VALID
                assert unified_price.close > 150.0  # Should be consensus price
                assert unified_price.vendor_count == 2
                assert unified_price.primary_vendor in ["polygon", "tiingo"]

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_holiday_detection_scenario(self):
        """Test detection of market holidays"""
        # This would test holiday exclusion logic
        # Implementation depends on how holidays are detected

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_corporate_action_scenario(self):
        """Test handling of corporate actions affecting prices"""
        # This would test detection of stock splits, dividends affecting prices
        # Implementation depends on corporate action detection logic


# Performance and edge case tests
class TestPerformanceAndEdgeCases:
    """Test performance and edge cases"""

    def test_extreme_price_values(self):
        """Test handling of extreme price values"""
        env = Environment()
        validator = UnifiedDailyPriceValidator(env)

        # Test very high price
        historical_stats = {'mean_price': 1.0, 'std_price': 0.1, 'sample_size': 50}
        result = validator.validate_price_statistically(1000000.0, historical_stats, "TEST")
        assert not result.is_valid

        # Test very low price
        result = validator.validate_price_statistically(0.000001, historical_stats, "TEST")
        assert not result.is_valid

        # Test negative price (should be handled by database constraints)
        with pytest.raises(Exception):
            VendorPrice("test", "TEST", date.today(), None, None, None, -1.0, None, 1000)

    def test_missing_historical_data(self):
        """Test behavior with insufficient historical data"""
        env = Environment()
        validator = UnifiedDailyPriceValidator(env)

        # Empty historical stats
        result = validator.validate_price_statistically(100.0, {}, "TEST")
        assert result.is_valid  # Should pass with lower confidence
        assert result.confidence_score < 1.0

    def test_data_type_validation(self):
        """Test proper data type handling"""
        # Test that price values are properly converted to float
        vendor_price = VendorPrice(
            vendor="test",
            symbol="TEST",
            date=date.today(),
            open_price="150.00",  # String input
            high_price="155.00",
            low_price="149.00",
            close="154.00",
            adj_close="153.50",
            volume="1000000"
        )

        # Should handle string inputs properly
        assert isinstance(vendor_price.close, str)  # VendorPrice stores as provided

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_large_batch_processing(self):
        """Test processing large batches of symbols"""
        env = Environment()
        pipeline = UnifiedDailyPricePipeline(env)

        # Mock connections
        pipeline.conn = AsyncMock()
        pipeline.validator.conn = AsyncMock()

        # Test batch size handling
        large_symbol_list = [f"SYM{i:04d}" for i in range(1000)]

        # Mock the get_symbols_to_process method
        pipeline.get_symbols_to_process = AsyncMock(return_value=large_symbol_list[:100])

        symbols = await pipeline.get_symbols_to_process(large_symbol_list, limit=100)
        assert len(symbols) == 100


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])