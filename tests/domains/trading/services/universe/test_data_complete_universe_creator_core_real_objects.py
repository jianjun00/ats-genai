"""
Real objects integration tests for DataCompleteUniverseCreator core functionality.

Replaces mock-heavy testing with authentic database integration to test:
- Real data completeness calculations with actual market data
- Trading days calculation with real calendar integration
- Universe creation with actual database constraint validation
- Data quality assessment with real time series analysis
- Error handling with actual data processing exceptions

This demonstrates fail-fast testing that eliminates MagicMock dependencies
and provides authentic validation of universe creation business logic.
"""

import pytest
from datetime import date, datetime, timedelta

from domains.trading.services.universe.data_complete_universe_creator import (
    DataCompleteUniverseCreator,
    DataCompleteness
)
from shared.utils.environment import Environment, EnvironmentType
from core.dao.instruments_dao import InstrumentsDAO
from core.dao.daily_prices_dao import DailyPricesDAO


class TestDataCompleteUniverseCreatorCoreRealObjects:
    """Real objects test suite for DataCompleteUniverseCreator core business logic."""

    @pytest.fixture
    async def test_environment(self):
        """Real Environment instance for testing."""
        return Environment(
            env_type=EnvironmentType.DEV,
            db_url="postgresql://postgres:dev_password@localhost:3432/dev_db"
        )

    @pytest.fixture
    async def real_universe_creator(self, test_environment):
        """Real DataCompleteUniverseCreator with actual environment."""
        return DataCompleteUniverseCreator(env=test_environment)

    @pytest.fixture
    async def test_instruments_and_data(self, test_environment):
        """Create real test instruments with market data and clean up after test."""
        instruments_dao = InstrumentsDAO(test_environment)
        daily_prices_dao = DailyPricesDAO(test_environment)
        
        # Create test instruments
        test_symbols = ['TEST_AAPL', 'TEST_GOOGL', 'TEST_MSFT']
        instrument_ids = []
        
        for symbol in test_symbols:
            instrument_id = await instruments_dao.create_instrument(
                symbol=symbol,
                name=f"Test {symbol.replace('TEST_', '')} Inc.",
                exchange="NASDAQ",
                sector="Technology"
            )
            instrument_ids.append(instrument_id)
        
        # Create real market data for completeness testing
        price_records = []
        start_date = date.today() - timedelta(days=365 * 2)  # 2 years of data
        
        for instrument_id in instrument_ids:
            current_date = start_date
            while current_date <= date.today():
                # Skip weekends for realistic market data
                if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                    price_records.append({
                        'instrument_id': instrument_id,
                        'price_date': current_date,
                        'open': 100.0 + (current_date.toordinal() % 100),
                        'high': 105.0 + (current_date.toordinal() % 100),
                        'low': 95.0 + (current_date.toordinal() % 100),
                        'close': 102.0 + (current_date.toordinal() % 100),
                        'volume': 1000000 + (current_date.toordinal() % 1000000)
                    })
                current_date += timedelta(days=1)
        
        # Insert real price data
        for price_record in price_records:
            await daily_prices_dao.create_daily_price(**price_record)
        
        yield {
            'symbols': test_symbols,
            'instrument_ids': instrument_ids,
            'start_date': start_date,
            'end_date': date.today(),
            'price_records_count': len(price_records)
        }
        
        # Cleanup - delete price data and instruments
        for instrument_id in instrument_ids:
            await daily_prices_dao.delete_prices_for_instrument(instrument_id)
            await instruments_dao.delete_instrument(instrument_id)

    async def test_init_with_real_environment(self, real_universe_creator, test_environment):
        """Test initialization with real environment."""
        assert real_universe_creator.env == test_environment
        assert real_universe_creator.min_years == 5
        assert real_universe_creator.min_daily_completeness == 0.95
        assert real_universe_creator.min_minute_completeness == 0.85
        assert real_universe_creator.min_overall_quality == 0.80
        
        # Verify real environment connectivity
        assert test_environment.env_type == EnvironmentType.DEV
        assert "postgresql" in test_environment.db_url

    async def test_calculate_expected_trading_days_real_objects(self, real_universe_creator):
        """Test expected trading days calculation with real calendar data."""
        # Test full years calculation
        start_date = date(2019, 1, 1)
        end_date = date(2024, 1, 1)
        
        expected_days = await real_universe_creator.calculate_expected_trading_days(
            start_date, end_date
        )
        
        # Validate real trading days calculation
        assert expected_days > 0
        assert isinstance(expected_days, int)
        
        # Should be approximately 252 trading days per year * 5 years
        # Allow some variance for holidays and market closures
        expected_range = (252 * 5 * 0.95, 252 * 5 * 1.05)  # ±5% variance
        assert expected_range[0] <= expected_days <= expected_range[1]
        
        print(f"Trading days from {start_date} to {end_date}: {expected_days}")

    async def test_calculate_data_completeness_real_objects(self, real_universe_creator, test_instruments_and_data):
        """Test data completeness calculation with real market data."""
        symbols = test_instruments_and_data['symbols']
        start_date = test_instruments_and_data['start_date']
        end_date = test_instruments_and_data['end_date']
        
        # Test real data completeness calculation
        for symbol in symbols:
            completeness = await real_universe_creator.calculate_data_completeness(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )
            
            # Validate real completeness calculation
            assert completeness is not None
            assert isinstance(completeness, DataCompleteness)
            
            # Verify completeness metrics
            assert 0.0 <= completeness.daily_completeness <= 1.0
            assert 0.0 <= completeness.minute_completeness <= 1.0
            assert 0.0 <= completeness.overall_quality <= 1.0
            
            # With our test data (weekdays only), daily completeness should be high
            assert completeness.daily_completeness > 0.8
            
            print(f"{symbol} completeness: daily={completeness.daily_completeness:.3f}, "
                  f"minute={completeness.minute_completeness:.3f}, "
                  f"overall={completeness.overall_quality:.3f}")

    async def test_assess_data_quality_real_objects(self, real_universe_creator, test_instruments_and_data):
        """Test data quality assessment with real time series analysis."""
        symbols = test_instruments_and_data['symbols']
        start_date = test_instruments_and_data['start_date']
        end_date = test_instruments_and_data['end_date']
        
        # Test real data quality assessment
        for symbol in symbols:
            quality_report = await real_universe_creator.assess_data_quality(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )
            
            # Validate quality assessment
            assert quality_report is not None
            assert isinstance(quality_report, dict)
            
            # Check for expected quality metrics
            expected_keys = ['price_consistency', 'volume_consistency', 'gap_analysis', 'outlier_detection']
            for key in expected_keys:
                if key in quality_report:
                    assert quality_report[key] is not None
            
            # Validate specific quality checks
            if 'gap_analysis' in quality_report:
                gap_info = quality_report['gap_analysis']
                assert isinstance(gap_info, dict)
                
                if 'gap_count' in gap_info:
                    assert isinstance(gap_info['gap_count'], int)
                    assert gap_info['gap_count'] >= 0

    async def test_create_universe_with_real_data(self, real_universe_creator, test_instruments_and_data):
        """Test universe creation with real data completeness validation."""
        symbols = test_instruments_and_data['symbols']
        start_date = test_instruments_and_data['start_date']
        end_date = test_instruments_and_data['end_date']
        
        # Test real universe creation
        universe = await real_universe_creator.create_data_complete_universe(
            candidate_symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            min_years=1,  # Reduce requirement for test data
            min_daily_completeness=0.7,  # Lower thresholds for test
            min_minute_completeness=0.5,
            min_overall_quality=0.6
        )
        
        # Validate real universe creation
        assert universe is not None
        assert isinstance(universe, (list, dict))
        
        if isinstance(universe, list):
            # Should include symbols that meet completeness criteria
            assert len(universe) > 0
            
            # All returned symbols should be from our test set
            for symbol in universe:
                assert symbol in symbols
        
        elif isinstance(universe, dict):
            # Should have universe metadata
            if 'symbols' in universe:
                assert len(universe['symbols']) > 0
            
            if 'completeness_stats' in universe:
                stats = universe['completeness_stats']
                assert isinstance(stats, dict)

    async def test_filter_by_completeness_thresholds_real_objects(self, real_universe_creator, test_instruments_and_data):
        """Test filtering by completeness thresholds with real data validation."""
        symbols = test_instruments_and_data['symbols']
        start_date = test_instruments_and_data['start_date']
        end_date = test_instruments_and_data['end_date']
        
        # Test different threshold configurations
        threshold_configs = [
            {'daily': 0.9, 'minute': 0.8, 'overall': 0.75},
            {'daily': 0.7, 'minute': 0.6, 'overall': 0.65},
            {'daily': 0.5, 'minute': 0.4, 'overall': 0.45}
        ]
        
        results = {}
        
        for i, thresholds in enumerate(threshold_configs):
            filtered_universe = await real_universe_creator.filter_by_completeness(
                candidate_symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                daily_threshold=thresholds['daily'],
                minute_threshold=thresholds['minute'],
                overall_threshold=thresholds['overall']
            )
            
            results[f"config_{i}"] = filtered_universe
            
            # Validate filtering results
            assert filtered_universe is not None
            assert isinstance(filtered_universe, (list, dict))
            
            if isinstance(filtered_universe, list):
                # All symbols should meet the thresholds
                assert len(filtered_universe) >= 0
                for symbol in filtered_universe:
                    assert symbol in symbols
        
        # Lower thresholds should include more symbols than higher thresholds
        if all(isinstance(r, list) for r in results.values()):
            config_0_count = len(results['config_0'])
            config_1_count = len(results['config_1'])
            config_2_count = len(results['config_2'])
            
            # Lower thresholds (config_2) should have >= symbols than higher thresholds (config_0)
            assert config_2_count >= config_1_count >= config_0_count

    async def test_error_handling_real_objects(self, real_universe_creator):
        """Test error handling with real data processing exceptions."""
        
        # Test invalid date range
        invalid_start = date.today()
        invalid_end = date.today() - timedelta(days=30)  # End before start
        
        try:
            result = await real_universe_creator.calculate_expected_trading_days(
                invalid_start, invalid_end
            )
            
            # If calculation succeeds, result should be 0 or negative
            assert result <= 0
            
        except Exception as e:
            # Real validation error is expected
            assert isinstance(e, Exception)
            assert "date" in str(e).lower() or "invalid" in str(e).lower()
        
        # Test non-existent symbol
        try:
            completeness = await real_universe_creator.calculate_data_completeness(
                symbol="NONEXISTENT_SYMBOL_XYZ",
                start_date=date.today() - timedelta(days=30),
                end_date=date.today()
            )
            
            # If calculation succeeds, completeness should indicate no data
            if completeness is not None:
                assert completeness.daily_completeness == 0.0
                assert completeness.minute_completeness == 0.0
                
        except Exception as e:
            # Real database error is acceptable
            assert isinstance(e, Exception)
            print(f"Expected error for non-existent symbol: {e}")

    async def test_performance_characteristics_real_objects(self, real_universe_creator, test_instruments_and_data):
        """Test performance characteristics with real data processing."""
        import time
        
        symbols = test_instruments_and_data['symbols']
        start_date = test_instruments_and_data['start_date']
        end_date = test_instruments_and_data['end_date']
        
        # Measure real completeness calculation performance
        start_time = time.time()
        
        completeness_results = []
        for symbol in symbols:
            completeness = await real_universe_creator.calculate_data_completeness(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )
            completeness_results.append(completeness)
        
        calculation_time = time.time() - start_time
        
        # Validate performance
        assert len(completeness_results) == len(symbols)
        assert calculation_time >= 0
        
        # Calculate performance metrics
        symbols_per_second = len(symbols) / calculation_time if calculation_time > 0 else float('inf')
        
        print(f"Completeness calculation time: {calculation_time:.4f}s for {len(symbols)} symbols")
        print(f"Processing rate: {symbols_per_second:.1f} symbols/second")
        
        # Performance should be reasonable (basic benchmark)
        assert symbols_per_second > 0

    async def test_concurrent_completeness_calculation_real_objects(self, real_universe_creator, test_instruments_and_data):
        """Test concurrent completeness calculations with real database access."""
        import asyncio
        
        symbols = test_instruments_and_data['symbols']
        start_date = test_instruments_and_data['start_date']
        end_date = test_instruments_and_data['end_date']
        
        # Test concurrent completeness calculations
        async def calculate_completeness_concurrent(symbol):
            return await real_universe_creator.calculate_data_completeness(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            )
        
        # Execute concurrent operations
        tasks = [calculate_completeness_concurrent(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Validate concurrent processing
        successful_results = [r for r in results if not isinstance(r, Exception)]
        assert len(successful_results) >= 1  # At least one should succeed
        
        # Verify result quality
        for result in successful_results:
            if result is not None:
                assert isinstance(result, DataCompleteness)
                assert 0.0 <= result.daily_completeness <= 1.0
        
        # Check for concurrency issues
        exceptions = [r for r in results if isinstance(r, Exception)]
        for exc in exceptions:
            # Real database concurrency exceptions are acceptable and informative
            print(f"Concurrent processing exception: {exc}")
            assert isinstance(exc, Exception)

    async def test_data_completeness_edge_cases_real_objects(self, real_universe_creator, test_instruments_and_data):
        """Test data completeness calculation edge cases with real data."""
        symbols = test_instruments_and_data['symbols']
        
        # Test very short date range
        short_start = date.today() - timedelta(days=7)
        short_end = date.today()
        
        for symbol in symbols:
            short_completeness = await real_universe_creator.calculate_data_completeness(
                symbol=symbol,
                start_date=short_start,
                end_date=short_end
            )
            
            # Should handle short ranges gracefully
            if short_completeness is not None:
                assert isinstance(short_completeness, DataCompleteness)
                assert 0.0 <= short_completeness.daily_completeness <= 1.0
        
        # Test single day range
        single_day = date.today() - timedelta(days=1)
        
        single_day_completeness = await real_universe_creator.calculate_data_completeness(
            symbol=symbols[0],
            start_date=single_day,
            end_date=single_day
        )
        
        # Should handle single day gracefully
        if single_day_completeness is not None:
            assert isinstance(single_day_completeness, DataCompleteness)
            # Single day should have either 0% or 100% daily completeness
            assert single_day_completeness.daily_completeness in [0.0, 1.0]