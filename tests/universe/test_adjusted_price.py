import pytest
from datetime import date
from src.universe.calculate_adjusted_prices import compute_adjusted_prices

def test_split_adjustment():
    # 2-for-1 split on 2023-01-03
    prices = [
        {'date': date(2023, 1, 1), 'close': 100},
        {'date': date(2023, 1, 2), 'close': 110},
        {'date': date(2023, 1, 3), 'close': 120},
        {'date': date(2023, 1, 4), 'close': 130},
    ]
    splits = [
        {'split_date': date(2023, 1, 3), 'numerator': 2, 'denominator': 1}
    ]
    dividends = []
    adj = compute_adjusted_prices(prices, splits, dividends)
    # All prices before split should be halved
    assert abs(adj[date(2023, 1, 1)] - 50) < 1e-6
    assert abs(adj[date(2023, 1, 2)] - 55) < 1e-6
    assert abs(adj[date(2023, 1, 3)] - 60) < 1e-6
    assert abs(adj[date(2023, 1, 4)] - 130) < 1e-6

def test_dividend_adjustment():
    # $5 dividend on 2023-01-03
    prices = [
        {'date': date(2023, 1, 1), 'close': 100},
        {'date': date(2023, 1, 2), 'close': 110},
        {'date': date(2023, 1, 3), 'close': 120},
        {'date': date(2023, 1, 4), 'close': 130},
    ]
    splits = []
    dividends = [
        {'ex_date': date(2023, 1, 3), 'amount': 5}
    ]
    adj = compute_adjusted_prices(prices, splits, dividends)
    # All prices before dividend should be reduced
    assert abs(adj[date(2023, 1, 1)] - 95) < 1e-6
    assert abs(adj[date(2023, 1, 2)] - 104.5) < 1e-6
    assert abs(adj[date(2023, 1, 3)] - 115) < 1e-6
    assert abs(adj[date(2023, 1, 4)] - 130) < 1e-6

def test_multiple_events():
    # $5 dividend on 2023-01-3, 2-for-1 split on 2023-01-2
    prices = [
        {'date': date(2023, 1, 1), 'close': 100},
        {'date': date(2023, 1, 2), 'close': 110},
        {'date': date(2023, 1, 3), 'close': 120},
        {'date': date(2023, 1, 4), 'close': 130},
    ]
    splits = [
        {'split_date': date(2023, 1, 2), 'numerator': 2, 'denominator': 1}
    ]
    dividends = [
        {'ex_date': date(2023, 1, 3), 'amount': 5}
    ]
    adj = compute_adjusted_prices(prices, splits, dividends)
    # Apply split first (backward), then dividend
    assert abs(adj[date(2023, 1, 1)] - 47.5) < 1e-6
    assert abs(adj[date(2023, 1, 2)] - 52.5) < 1e-6
    assert abs(adj[date(2023, 1, 3)] - 57.5) < 1e-6
    assert abs(adj[date(2023, 1, 4)] - 130) < 1e-6

def test_no_events():
    prices = [
        {'date': date(2023, 1, 1), 'close': 100},
        {'date': date(2023, 1, 2), 'close': 110},
    ]
    splits = []
    dividends = []
    adj = compute_adjusted_prices(prices, splits, dividends)
    assert abs(adj[date(2023, 1, 1)] - 100) < 1e-6
    assert abs(adj[date(2023, 1, 2)] - 110) < 1e-6
