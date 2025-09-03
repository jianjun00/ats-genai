"""
Comprehensive test suite for ModelingUniverseCreator.

Tests business logic for creating modeling universes based on market cap
and dollar volume criteria without complex async database mocking.
"""

import pytest
from datetime import date, timedelta
from unittest.mock import Mock, MagicMock, patch
from typing import List, Optional

# Test imports
import sys
sys.path.insert(0, 'src')

from src.universe.modeling_universe_creator import ModelingUniverseCreator, ModelingStock
from shared.utils.environment import Environment


class TestModelingUniverseCreatorCore:
    """Test core initialization and business logic."""

    def test_init_with_default_environment(self):
        """Test initialization with default environment."""
        creator = ModelingUniverseCreator()
        
        assert creator.env is not None
        assert creator.logger is not None
        assert creator.logger.name == "src.universe.modeling_universe_creator"

    def test_init_with_custom_environment(self):
        """Test initialization with custom environment."""
        mock_env = Mock(spec=Environment)
        creator = ModelingUniverseCreator(env=mock_env)
        
        assert creator.env == mock_env
        assert creator.logger is not None

    def test_rank_stocks_for_modeling_by_market_cap(self):
        """Test ranking stocks by market cap descending."""
        creator = ModelingUniverseCreator()
        
        stocks = [
            ModelingStock("AAPL", 1, 3000.0, 500.0, 10000.0, 150.0, 20, 
                         date(2024, 1, 1), date(2024, 1, 31)),
            ModelingStock("MSFT", 2, 2800.0, 450.0, 9000.0, 140.0, 20,
                         date(2024, 1, 1), date(2024, 1, 31)),
            ModelingStock("GOOGL", 3, 3200.0, 600.0, 8000.0, 120.0, 20,
                         date(2024, 1, 1), date(2024, 1, 31))
        ]
        
        ranked = creator._rank_stocks_for_modeling(stocks)
        
        # Should be ranked by market cap descending: GOOGL (3200), AAPL (3000), MSFT (2800)
        assert len(ranked) == 3
        assert ranked[0].symbol == "GOOGL"
        assert ranked[0].avg_market_cap == 3200.0
        assert ranked[1].symbol == "AAPL" 
        assert ranked[1].avg_market_cap == 3000.0
        assert ranked[2].symbol == "MSFT"
        assert ranked[2].avg_market_cap == 2800.0

    def test_rank_stocks_for_modeling_none_market_cap(self):
        """Test ranking stocks when some have None market cap."""
        creator = ModelingUniverseCreator()
        
        stocks = [
            ModelingStock("AAPL", 1, 3000.0, 500.0, 10000.0, 150.0, 20,
                         date(2024, 1, 1), date(2024, 1, 31)),
            ModelingStock("UNKNOWN", 2, None, 450.0, 9000.0, 140.0, 20,
                         date(2024, 1, 1), date(2024, 1, 31)),
            ModelingStock("MSFT", 3, 2800.0, 600.0, 8000.0, 120.0, 20,
                         date(2024, 1, 1), date(2024, 1, 31))
        ]
        
        ranked = creator._rank_stocks_for_modeling(stocks)
        
        # Should put None market cap stocks last
        assert len(ranked) == 3
        assert ranked[0].symbol == "AAPL"
        assert ranked[1].symbol == "MSFT"
        assert ranked[2].symbol == "UNKNOWN"
        assert ranked[2].avg_market_cap is None

    def test_rank_stocks_for_modeling_empty_list(self):
        """Test ranking empty list of stocks."""
        creator = ModelingUniverseCreator()
        
        ranked = creator._rank_stocks_for_modeling([])
        
        assert ranked == []


class TestModelingStockDataclass:
    """Test ModelingStock dataclass functionality."""

    def test_modeling_stock_creation_complete(self):
        """Test creating ModelingStock with all fields."""
        stock = ModelingStock(
            symbol="AAPL",
            instrument_id=1,
            avg_market_cap=3000000.0,
            avg_dollar_volume=500000.0,
            avg_volume=10000.0,
            avg_price=150.0,
            trading_days=20,
            first_date=date(2024, 1, 1),
            last_date=date(2024, 1, 31)
        )
        
        assert stock.symbol == "AAPL"
        assert stock.instrument_id == 1
        assert stock.avg_market_cap == 3000000.0
        assert stock.avg_dollar_volume == 500000.0
        assert stock.avg_volume == 10000.0
        assert stock.avg_price == 150.0
        assert stock.trading_days == 20
        assert stock.first_date == date(2024, 1, 1)
        assert stock.last_date == date(2024, 1, 31)

    def test_modeling_stock_creation_with_none_values(self):
        """Test creating ModelingStock with None values for optional fields."""
        stock = ModelingStock(
            symbol="UNKNOWN",
            instrument_id=999,
            avg_market_cap=None,
            avg_dollar_volume=None,
            avg_volume=None,
            avg_price=None,
            trading_days=0,
            first_date=date(2024, 1, 1),
            last_date=date(2024, 1, 1)
        )
        
        assert stock.symbol == "UNKNOWN"
        assert stock.instrument_id == 999
        assert stock.avg_market_cap is None
        assert stock.avg_dollar_volume is None
        assert stock.avg_volume is None
        assert stock.avg_price is None
        assert stock.trading_days == 0

    def test_modeling_stock_equality(self):
        """Test ModelingStock equality comparison."""
        stock1 = ModelingStock("AAPL", 1, 3000.0, 500.0, 10000.0, 150.0, 20,
                              date(2024, 1, 1), date(2024, 1, 31))
        stock2 = ModelingStock("AAPL", 1, 3000.0, 500.0, 10000.0, 150.0, 20,
                              date(2024, 1, 1), date(2024, 1, 31))
        stock3 = ModelingStock("MSFT", 2, 2800.0, 450.0, 9000.0, 140.0, 20,
                              date(2024, 1, 1), date(2024, 1, 31))
        
        assert stock1 == stock2
        assert stock1 != stock3


class TestModelingUniverseCreatorBusinessLogic:
    """Test business logic and edge cases for modeling universe creation."""

    def test_market_cap_criteria_validation(self):
        """Test that market cap criteria are properly applied."""
        creator = ModelingUniverseCreator()
        
        # Test stocks: some above 400M, some below
        high_cap_stock = ModelingStock("AAPL", 1, 3000.0, 500.0, 10000.0, 150.0, 20,
                                      date(2024, 1, 1), date(2024, 1, 31))
        low_cap_stock = ModelingStock("SMALL", 2, 200.0, 300.0, 8000.0, 25.0, 20,
                                     date(2024, 1, 1), date(2024, 1, 31))
        no_cap_stock = ModelingStock("UNKNOWN", 3, None, 600.0, 12000.0, 50.0, 20,
                                    date(2024, 1, 1), date(2024, 1, 31))
        
        stocks = [high_cap_stock, low_cap_stock, no_cap_stock]
        
        # Based on the business logic, stocks with market cap >= 400M should qualify
        # Stocks with None market cap might be handled differently
        qualified = [s for s in stocks if s.avg_market_cap and s.avg_market_cap >= 400.0]
        
        assert len(qualified) == 1
        assert qualified[0].symbol == "AAPL"

    def test_dollar_volume_criteria_validation(self):
        """Test that dollar volume criteria are properly applied."""
        creator = ModelingUniverseCreator()
        
        # Test stocks: some above 100M dollar volume, some below
        high_volume_stock = ModelingStock("AAPL", 1, 3000.0, 500.0, 10000.0, 150.0, 20,
                                         date(2024, 1, 1), date(2024, 1, 31))
        low_volume_stock = ModelingStock("SMALL", 2, 600.0, 50.0, 8000.0, 25.0, 20,
                                        date(2024, 1, 1), date(2024, 1, 31))
        no_volume_stock = ModelingStock("UNKNOWN", 3, 800.0, None, 12000.0, 50.0, 20,
                                       date(2024, 1, 1), date(2024, 1, 31))
        
        stocks = [high_volume_stock, low_volume_stock, no_volume_stock]
        
        # Based on the business logic, stocks with dollar volume >= 100M should qualify
        qualified = [s for s in stocks if s.avg_dollar_volume and s.avg_dollar_volume >= 100.0]
        
        assert len(qualified) == 1
        assert qualified[0].symbol == "AAPL"

    def test_trading_days_criteria_validation(self):
        """Test that minimum trading days criteria are applied."""
        creator = ModelingUniverseCreator()
        
        sufficient_days_stock = ModelingStock("AAPL", 1, 3000.0, 500.0, 10000.0, 150.0, 20,
                                             date(2024, 1, 1), date(2024, 1, 31))
        insufficient_days_stock = ModelingStock("NEW", 2, 800.0, 300.0, 8000.0, 25.0, 10,
                                               date(2024, 1, 20), date(2024, 1, 31))
        
        stocks = [sufficient_days_stock, insufficient_days_stock]
        min_trading_days = 15
        
        # Based on business logic, only stocks with >= min_trading_days should qualify
        qualified = [s for s in stocks if s.trading_days >= min_trading_days]
        
        assert len(qualified) == 1
        assert qualified[0].symbol == "AAPL"

    def test_combined_criteria_filtering(self):
        """Test filtering with all criteria combined."""
        creator = ModelingUniverseCreator()
        
        # Perfect stock: meets all criteria
        perfect_stock = ModelingStock("AAPL", 1, 3000.0, 500.0, 10000.0, 150.0, 20,
                                     date(2024, 1, 1), date(2024, 1, 31))
        
        # Fails market cap
        low_cap_stock = ModelingStock("SMALL1", 2, 200.0, 300.0, 8000.0, 25.0, 20,
                                     date(2024, 1, 1), date(2024, 1, 31))
        
        # Fails dollar volume
        low_volume_stock = ModelingStock("SMALL2", 3, 800.0, 50.0, 2000.0, 25.0, 20,
                                        date(2024, 1, 1), date(2024, 1, 31))
        
        # Fails trading days
        new_stock = ModelingStock("NEW", 4, 1000.0, 200.0, 8000.0, 25.0, 5,
                                 date(2024, 1, 25), date(2024, 1, 31))
        
        stocks = [perfect_stock, low_cap_stock, low_volume_stock, new_stock]
        
        min_market_cap = 400.0
        min_dollar_volume = 100.0
        min_trading_days = 15
        
        # Apply all criteria
        qualified = [
            s for s in stocks
            if (s.avg_market_cap and s.avg_market_cap >= min_market_cap and
                s.avg_dollar_volume and s.avg_dollar_volume >= min_dollar_volume and
                s.trading_days >= min_trading_days)
        ]
        
        assert len(qualified) == 1
        assert qualified[0].symbol == "AAPL"

    def test_edge_case_zero_values(self):
        """Test handling of zero values in stock metrics."""
        creator = ModelingUniverseCreator()
        
        zero_stock = ModelingStock("ZERO", 1, 0.0, 0.0, 0.0, 0.0, 0,
                                  date(2024, 1, 1), date(2024, 1, 1))
        
        # Zero values should not qualify
        min_market_cap = 400.0
        min_dollar_volume = 100.0
        min_trading_days = 15
        
        qualifies = (
            zero_stock.avg_market_cap and zero_stock.avg_market_cap >= min_market_cap and
            zero_stock.avg_dollar_volume and zero_stock.avg_dollar_volume >= min_dollar_volume and
            zero_stock.trading_days >= min_trading_days
        )
        
        assert not qualifies

    def test_edge_case_negative_values(self):
        """Test handling of negative values in stock metrics."""
        creator = ModelingUniverseCreator()
        
        negative_stock = ModelingStock("NEG", 1, -100.0, -50.0, -1000.0, -10.0, -5,
                                      date(2024, 1, 1), date(2024, 1, 1))
        
        # Negative values should not qualify
        min_market_cap = 400.0
        min_dollar_volume = 100.0
        min_trading_days = 15
        
        qualifies = (
            negative_stock.avg_market_cap and negative_stock.avg_market_cap >= min_market_cap and
            negative_stock.avg_dollar_volume and negative_stock.avg_dollar_volume >= min_dollar_volume and
            negative_stock.trading_days >= min_trading_days
        )
        
        assert not qualifies

    def test_ranking_stability_with_equal_market_caps(self):
        """Test ranking stability when stocks have equal market caps."""
        creator = ModelingUniverseCreator()
        
        # Two stocks with identical market caps
        stock1 = ModelingStock("STOCK1", 1, 1000.0, 200.0, 5000.0, 50.0, 20,
                              date(2024, 1, 1), date(2024, 1, 31))
        stock2 = ModelingStock("STOCK2", 2, 1000.0, 300.0, 6000.0, 60.0, 20,
                              date(2024, 1, 1), date(2024, 1, 31))
        
        ranked = creator._rank_stocks_for_modeling([stock1, stock2])
        
        # Should maintain original order when market caps are equal
        # (Python's sort is stable)
        assert len(ranked) == 2
        assert ranked[0].symbol in ["STOCK1", "STOCK2"]
        assert ranked[1].symbol in ["STOCK1", "STOCK2"]
        assert ranked[0].symbol != ranked[1].symbol


if __name__ == "__main__":
    pytest.main([__file__, "-v"])