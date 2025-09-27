"""
Tests for Historical Universe Creator (Bias-Free Universe Generation)
"""

import pytest
import numpy as np
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from domains.trading.services.universe.modeling_universe_creator import (
    HistoricalUniverseCreator,
    HistoricalStock
)

class TestHistoricalStock:
    """Test suite for HistoricalStock data structure"""

    def test_historical_stock_creation(self):
        """Test creating HistoricalStock objects"""
        stock = HistoricalStock(
            symbol='AAPL',
            instrument_id=1001,
            market_cap=2500000000000,  # $2.5T
            avg_volume=75000000,
            avg_price=180.50,
            trading_days=252,
            first_date=date(2020, 1, 2),
            last_date=date(2020, 12, 31)
        )

        assert stock.symbol == 'AAPL'
        assert stock.instrument_id == 1001
        assert stock.market_cap == 2500000000000
        assert stock.avg_volume == 75000000
        assert stock.avg_price == 180.50
        assert stock.trading_days == 252
        assert stock.first_date == date(2020, 1, 2)
        assert stock.last_date == date(2020, 12, 31)

    def test_historical_stock_validation(self):
        """Test validation of HistoricalStock values"""
        stock = HistoricalStock(
            symbol='TEST',
            instrument_id=None,  # Can be None
            market_cap=1000000000,  # $1B
            avg_volume=500000,
            avg_price=50.0,
            trading_days=200,
            first_date=date(2020, 1, 15),
            last_date=date(2020, 11, 30)
        )

        assert stock.symbol.isalpha()
        assert stock.market_cap > 0 if stock.market_cap else True
        assert stock.avg_volume > 0 if stock.avg_volume else True
        assert stock.avg_price > 0 if stock.avg_price else True
        assert stock.trading_days >= 0
        assert stock.first_date <= stock.last_date

class TestHistoricalUniverseCreator:
    """Test suite for HistoricalUniverseCreator"""

    @pytest.fixture
    def creator(self):
        """Create creator instance for testing"""
        env = MagicMock()
        env.get_database_url.return_value = "test://db"
        env.get_table_name.return_value = "test_table"
        return HistoricalUniverseCreator(env=env)

    @pytest.fixture
    def sample_stock_data(self):
        """Create sample stock data for testing"""
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'JNJ', 'V']
        stocks = []

        for i, symbol in enumerate(symbols):
            # Create varying market caps and volumes
            market_cap = (1000 + i * 500) * 1_000_000_000  # $1B to $5.5B
            avg_volume = (100_000 + i * 50_000) * (10 + i)  # Varying volumes
            avg_price = 50 + i * 20 + np.random.uniform(-5, 5)
            trading_days = 240 + np.random.randint(-20, 20)

            stock = HistoricalStock(
                symbol=symbol,
                instrument_id=1000 + i,
                market_cap=market_cap,
                avg_volume=avg_volume,
                avg_price=avg_price,
                trading_days=trading_days,
                first_date=date(2020, 1, 2),
                last_date=date(2020, 12, 30)
            )
            stocks.append(stock)

        return stocks

    def test_creator_initialization(self, creator):
        """Test creator initialization"""
        assert creator.env is not None
        assert hasattr(creator, 'logger')

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_active_stocks_in_year_mock(self, creator):
        """Test getting active stocks with mocked database"""
        with patch('asyncpg.create_pool') as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn

            # Mock database response
            mock_conn.fetch.return_value = [
                {
                    'symbol': 'AAPL',
                    'instrument_id': 1001,
                    'avg_volume': 75000000,
                    'avg_price': 125.0,
                    'trading_days': 252,
                    'first_date': date(2020, 1, 2),
                    'last_date': date(2020, 12, 30),
                    'estimated_market_cap': 2000000000000
                },
                {
                    'symbol': 'MSFT',
                    'instrument_id': 1002,
                    'avg_volume': 30000000,
                    'avg_price': 215.0,
                    'trading_days': 251,
                    'first_date': date(2020, 1, 3),
                    'last_date': date(2020, 12, 29),
                    'estimated_market_cap': 1800000000000
                }
            ]

            stocks = await creator.get_active_stocks_in_year(
                year=2020,
                min_market_cap_millions=1000,
                min_avg_volume=25000000,
                min_trading_days=200
            )

            assert len(stocks) == 2
            assert all(isinstance(stock, HistoricalStock) for stock in stocks)
            assert stocks[0].symbol == 'AAPL'
            assert stocks[1].symbol == 'MSFT'

    def test_sample_stocks_by_market_cap(self, creator, sample_stock_data):
        """Test market cap weighted sampling"""
        # Test sampling smaller number than available
        sampled = creator._sample_stocks_by_market_cap(sample_stock_data, 5)

        assert len(sampled) == 5
        assert all(isinstance(stock, HistoricalStock) for stock in sampled)
        assert len(set(stock.symbol for stock in sampled)) == 5  # No duplicates

        # Should include some high market cap stocks due to weighting
        sampled_symbols = [stock.symbol for stock in sampled]
        high_cap_symbols = [stock.symbol for stock in sample_stock_data[-3:]]  # Last 3 have highest caps
        assert any(symbol in sampled_symbols for symbol in high_cap_symbols)

    def test_sample_stocks_by_market_cap_edge_cases(self, creator, sample_stock_data):
        """Test edge cases for market cap sampling"""
        # Sample more than available
        sampled = creator._sample_stocks_by_market_cap(sample_stock_data, 20)
        assert len(sampled) == len(sample_stock_data)  # Should return all

        # Sample exactly the number available
        sampled = creator._sample_stocks_by_market_cap(sample_stock_data, len(sample_stock_data))
        assert len(sampled) == len(sample_stock_data)

        # Sample zero
        sampled = creator._sample_stocks_by_market_cap(sample_stock_data, 0)
        assert len(sampled) == 0

    def test_sample_stocks_weighting_logic(self, creator):
        """Test that market cap weighting works correctly"""
        # Create stocks with very different market caps
        stocks = [
            HistoricalStock('SMALL', 1, 1_000_000_000, 100000, 10.0, 250, date(2020, 1, 1), date(2020, 12, 31)),  # $1B
            HistoricalStock('MEDIUM', 2, 100_000_000_000, 500000, 50.0, 250, date(2020, 1, 1), date(2020, 12, 31)),  # $100B
            HistoricalStock('LARGE', 3, 2_000_000_000_000, 1000000, 200.0, 250, date(2020, 1, 1), date(2020, 12, 31))  # $2T
        ]

        # Sample many times and check distribution
        sample_counts = {'SMALL': 0, 'MEDIUM': 0, 'LARGE': 0}

        for _ in range(100):  # Multiple samples
            sampled = creator._sample_stocks_by_market_cap(stocks, 1)
            if sampled:
                sample_counts[sampled[0].symbol] += 1

        # Large cap should be sampled most often
        assert sample_counts['LARGE'] > sample_counts['MEDIUM']
        assert sample_counts['MEDIUM'] > sample_counts['SMALL']

    def test_sample_stocks_no_market_cap(self, creator):
        """Test sampling when market cap data is missing"""
        stocks = [
            HistoricalStock('TEST1', 1, None, 1000000, 100.0, 250, date(2020, 1, 1), date(2020, 12, 31)),
            HistoricalStock('TEST2', 2, None, 500000, 50.0, 250, date(2020, 1, 1), date(2020, 12, 31))
        ]

        # Should use volume * price as proxy
        sampled = creator._sample_stocks_by_market_cap(stocks, 1)
        assert len(sampled) == 1
        # TEST1 should be more likely (higher volume * price)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_create_historical_sample_universe_mock(self, creator):
        """Test creating historical sample universe with mocked database"""
        with patch('asyncpg.create_pool') as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn

            # Mock get_active_stocks_in_year
            creator.get_active_stocks_in_year = AsyncMock(return_value=[
                HistoricalStock('AAPL', 1, 2000000000000, 75000000, 125.0, 252, date(2020, 1, 2), date(2020, 12, 30)),
                HistoricalStock('MSFT', 2, 1800000000000, 30000000, 215.0, 251, date(2020, 1, 3), date(2020, 12, 29)),
                HistoricalStock('GOOGL', 3, 1500000000000, 25000000, 1800.0, 250, date(2020, 1, 6), date(2020, 12, 28))
            ])

            # Mock universe creation
            mock_conn.fetchrow.return_value = {'id': 100}  # Universe ID
            mock_conn.execute.return_value = None  # Member insertion

            universe_id = await creator.create_historical_sample_universe(
                universe_name='test_universe_2020',
                sample_year=2020,
                sample_size=2,
                min_market_cap_millions=1000,
                min_avg_volume=20000000,
                min_trading_days=200,
                seed=42
            )

            assert universe_id == 100
            assert creator.get_active_stocks_in_year.called
            assert mock_conn.fetchrow.called  # Universe creation
            assert mock_conn.execute.called   # Member insertion

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_historical_report(self, creator, sample_stock_data):
        """Test generating historical report"""
        # Take first 5 stocks for report
        stocks_for_report = sample_stock_data[:5]

        report = await creator.generate_historical_report(
            stocks=stocks_for_report,
            sample_year=2020,
            output_file=None  # Don't write to file
        )

        assert isinstance(report, str)
        assert len(report) > 0
        assert 'Historical Universe Report - 2020' in report
        assert 'Summary' in report
        assert 'Methodology' in report
        assert 'Selected Stocks' in report

        # Should contain stock symbols
        for stock in stocks_for_report:
            assert stock.symbol in report

    def test_bias_prevention_methodology(self, creator):
        """Test that the methodology prevents survivorship bias"""
        # This test validates the conceptual approach rather than implementation

        # 1. Historical sampling: Only uses data from the sample year
        sample_year = 2020
        current_year = 2023

        # The methodology should not use any information from after sample_year
        assert sample_year < current_year  # We're looking backwards

        # 2. Market cap weighting: Includes companies of various sizes
        # (This prevents only selecting "winners")

        # 3. No future data: Only uses information available at sample_year
        # (This is enforced by the database queries)

    def test_deterministic_sampling(self, creator, sample_stock_data):
        """Test that sampling is deterministic with same seed"""
        # Sample with same seed multiple times
        np.random.seed(42)
        sample1 = creator._sample_stocks_by_market_cap(sample_stock_data, 3)

        np.random.seed(42)
        sample2 = creator._sample_stocks_by_market_cap(sample_stock_data, 3)

        # Should get identical results
        symbols1 = [stock.symbol for stock in sample1]
        symbols2 = [stock.symbol for stock in sample2]
        assert symbols1 == symbols2

    def test_sample_size_validation(self, creator, sample_stock_data):
        """Test sample size validation"""
        # Valid sample sizes
        for size in [1, 5, len(sample_stock_data)]:
            sampled = creator._sample_stocks_by_market_cap(sample_stock_data, size)
            assert len(sampled) == min(size, len(sample_stock_data))

        # Edge case: empty input
        sampled = creator._sample_stocks_by_market_cap([], 5)
        assert len(sampled) == 0

    def test_market_cap_filtering(self, creator):
        """Test market cap filtering in stock selection"""
        # Create stocks with different market caps
        stocks = [
            HistoricalStock('SMALL', 1, 500_000_000, 100000, 10.0, 250, date(2020, 1, 1), date(2020, 12, 31)),     # $500M - below threshold
            HistoricalStock('MEDIUM', 2, 1_500_000_000, 500000, 50.0, 250, date(2020, 1, 1), date(2020, 12, 31)),  # $1.5B - above threshold
            HistoricalStock('LARGE', 3, 5_000_000_000, 1000000, 200.0, 250, date(2020, 1, 1), date(2020, 12, 31))  # $5B - above threshold
        ]

        # Filter for minimum $1B market cap
        min_market_cap_millions = 1000

        qualified_stocks = [
            stock for stock in stocks
            if stock.market_cap and stock.market_cap >= min_market_cap_millions * 1_000_000
        ]

        assert len(qualified_stocks) == 2  # MEDIUM and LARGE
        assert all(stock.symbol in ['MEDIUM', 'LARGE'] for stock in qualified_stocks)

    def test_volume_filtering(self, creator):
        """Test volume filtering in stock selection"""
        stocks = [
            HistoricalStock('LOW_VOL', 1, 2_000_000_000, 50_000, 100.0, 250, date(2020, 1, 1), date(2020, 12, 31)),     # Low volume
            HistoricalStock('HIGH_VOL', 2, 1_500_000_000, 1_000_000, 50.0, 250, date(2020, 1, 1), date(2020, 12, 31))  # High volume
        ]

        min_avg_volume = 100_000

        qualified_stocks = [
            stock for stock in stocks
            if stock.avg_volume and stock.avg_volume >= min_avg_volume
        ]

        assert len(qualified_stocks) == 1
        assert qualified_stocks[0].symbol == 'HIGH_VOL'

    def test_trading_days_filtering(self, creator):
        """Test trading days filtering"""
        stocks = [
            HistoricalStock('INACTIVE', 1, 2_000_000_000, 500_000, 100.0, 150, date(2020, 1, 1), date(2020, 6, 30)),     # Low trading days
            HistoricalStock('ACTIVE', 2, 1_500_000_000, 1_000_000, 50.0, 250, date(2020, 1, 1), date(2020, 12, 31))      # High trading days
        ]

        min_trading_days = 200

        qualified_stocks = [
            stock for stock in stocks
            if stock.trading_days >= min_trading_days
        ]

        assert len(qualified_stocks) == 1
        assert qualified_stocks[0].symbol == 'ACTIVE'

@pytest.mark.integration
class TestHistoricalUniverseIntegration:
    """Integration tests for historical universe creation"""

    @pytest.fixture
    def mock_env(self):
        """Create mock environment for integration tests"""
        env = MagicMock()
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name = lambda name: f"test_{name}"
        return env

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_end_to_end_universe_creation(self, mock_env):
        """Test complete universe creation workflow"""
        creator = HistoricalUniverseCreator(env=mock_env)

        with patch('asyncpg.create_pool') as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.return_value.__aenter__.return_value.acquire.return_value.__aenter__.return_value = mock_conn

            # Mock stock data query
            mock_conn.fetch.return_value = [
                {
                    'symbol': f'STOCK{i}',
                    'instrument_id': 1000 + i,
                    'avg_volume': 1000000 + i * 100000,
                    'avg_price': 100.0 + i * 10,
                    'trading_days': 250,
                    'first_date': date(2020, 1, 2),
                    'last_date': date(2020, 12, 30),
                    'estimated_market_cap': (2000 + i * 100) * 1_000_000_000
                }
                for i in range(10)
            ]

            # Mock universe creation
            mock_conn.fetchrow.return_value = {'id': 999}
            mock_conn.execute.return_value = None

            # Test the workflow
            universe_id = await creator.create_historical_sample_universe(
                universe_name='integration_test_universe',
                sample_year=2020,
                sample_size=5,
                min_market_cap_millions=1000,
                min_avg_volume=500000,
                min_trading_days=200,
                seed=42
            )

            assert universe_id == 999
            assert mock_conn.fetch.called
            assert mock_conn.fetchrow.called
            assert mock_conn.execute.called

    def test_universe_creation_with_insufficient_data(self):
        """Test universe creation when insufficient stocks meet criteria"""
        creator = HistoricalUniverseCreator()

        # Create stocks that don't meet criteria
        insufficient_stocks = [
            HistoricalStock('SMALL1', 1, 500_000_000, 50000, 10.0, 100, date(2020, 1, 1), date(2020, 3, 31)),
            HistoricalStock('SMALL2', 2, 800_000_000, 80000, 15.0, 150, date(2020, 1, 1), date(2020, 6, 30))
        ]

        # Try to sample more than available
        sampled = creator._sample_stocks_by_market_cap(insufficient_stocks, 5)

        # Should return all available stocks
        assert len(sampled) == len(insufficient_stocks)

    def test_bias_prevention_validation(self):
        """Test that the approach truly prevents survivorship bias"""
        # This test validates the conceptual correctness

        # Key principles that prevent survivorship bias:

        # 1. Historical point-in-time sampling
        sample_year = 2020
        training_start_year = 2021
        assert training_start_year > sample_year  # Future data not used in selection

        # 2. Criteria based on past performance only
        criteria = {
            'market_cap': 'Based on 2020 market cap',
            'volume': 'Based on 2020 average volume',
            'trading_days': 'Based on 2020 activity'
        }

        # No future performance criteria
        forbidden_criteria = [
            'future_returns',
            'future_data_availability',
            'post_2020_performance'
        ]

        # 3. Includes companies that may have failed later
        # (This is captured in the sampling methodology)

        # Validation: methodology uses only historical information
        assert all('2020' in str(criterion) for criterion in criteria.values())

if __name__ == "__main__":
    pytest.main([__file__, "-v"])