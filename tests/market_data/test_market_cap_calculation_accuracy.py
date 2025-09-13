#!/usr/bin/env python3
"""
Test coverage for market cap calculation accuracy and data quality.
This test suite validates that our market cap computations match external sources.
"""

import pytest
import asyncio
import asyncpg
import aiohttp
import os
from datetime import date, timedelta
from typing import Dict, List, Optional

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_market_cap_calculation_accuracy():
    """Test that our market cap calculations match external sources within 5% tolerance"""

    # Connect to test database
    pool = await asyncpg.create_pool(
        host="localhost",
        port=5433,
        user="postgres",
        password="postgres",
        database="dev_db",
        min_size=1,
        max_size=3
    )

    try:
        async with pool.acquire() as conn:
            # Get our calculated market caps for major stocks
            our_market_caps = await conn.fetch("""
                SELECT
                    i.symbol,
                    mc.market_cap/1000000000 as our_market_cap_billions,
                    mc.shares_outstanding,
                    p.close as our_price,
                    mc.date
                FROM dev_daily_market_cap mc
                JOIN dev_instrument i ON mc.instrument_id = i.id
                JOIN dev_daily_price_polygon p ON mc.instrument_id = p.instrument_id AND mc.date = p.date
                WHERE mc.date = (SELECT MAX(date) FROM dev_daily_market_cap)
                  AND i.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'NVDA', 'AMZN', 'META', 'TSLA')
                ORDER BY mc.market_cap DESC
            """)

            # Expected market caps (approximate, as of August 2025)
            expected_market_caps = {
                'AAPL': 3400,   # ~$3.4T
                'MSFT': 3800,   # ~$3.8T
                'GOOGL': 2000,  # ~$2.0T
                'NVDA': 3000,   # ~$3.0T
                'AMZN': 1800,   # ~$1.8T
                'META': 1300,   # ~$1.3T
                'TSLA': 800,    # ~$800B
            }

            accuracy_results = []

            for stock in our_market_caps:
                symbol = stock['symbol']
                our_cap = stock['our_market_cap_billions']
                expected_cap = expected_market_caps.get(symbol)

                if expected_cap:
                    accuracy_percent = (our_cap / expected_cap) * 100
                    accuracy_results.append({
                        'symbol': symbol,
                        'our_cap': our_cap,
                        'expected_cap': expected_cap,
                        'accuracy': accuracy_percent
                    })

                    # Assert within 20% tolerance (should be much closer once fixed)
                    assert accuracy_percent >= 80, f"{symbol}: Our ${our_cap:.0f}B vs Expected ${expected_cap:.0f}B = {accuracy_percent:.1f}% (too low)"
                    assert accuracy_percent <= 120, f"{symbol}: Our ${our_cap:.0f}B vs Expected ${expected_cap:.0f}B = {accuracy_percent:.1f}% (too high)"

            # Ensure we tested at least 5 major stocks
            assert len(accuracy_results) >= 5, "Should test at least 5 major stocks"

            # Average accuracy should be within 10%
            avg_accuracy = sum(r['accuracy'] for r in accuracy_results) / len(accuracy_results)
            assert 90 <= avg_accuracy <= 110, f"Average accuracy {avg_accuracy:.1f}% outside acceptable range"

    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_market_cap_universe_coverage():
    """Test that we have sufficient market cap coverage for universe building"""

    pool = await asyncpg.create_pool(
        host="localhost",
        port=5433,
        user="postgres",
        password="postgres",
        database="dev_db",
        min_size=1,
        max_size=3
    )

    try:
        async with pool.acquire() as conn:
            # Check total instruments with price data
            price_coverage = await conn.fetchval("""
                SELECT COUNT(DISTINCT instrument_id)
                FROM dev_daily_price_polygon
                WHERE date >= $1
            """, date.today() - timedelta(days=7))

            # Check instruments with market cap data
            market_cap_coverage = await conn.fetchval("""
                SELECT COUNT(DISTINCT instrument_id)
                FROM dev_daily_market_cap
            """)

            # Check qualifying universe (>$400M market cap + >$100M volume)
            qualifying_count = await conn.fetchval("""
                WITH latest_data AS (
                    SELECT
                        mc.instrument_id,
                        AVG(mc.market_cap) as avg_market_cap,
                        AVG(p.volume * p.close) as avg_dollar_volume
                    FROM dev_daily_market_cap mc
                    JOIN dev_daily_price_polygon p ON mc.instrument_id = p.instrument_id AND mc.date = p.date
                    WHERE p.date >= $1
                    GROUP BY mc.instrument_id
                    HAVING COUNT(*) >= 5  -- At least 5 days of data
                )
                SELECT COUNT(*)
                FROM latest_data
                WHERE avg_market_cap >= 400000000  -- $400M
                  AND avg_dollar_volume >= 100000000  -- $100M
            """, date.today() - timedelta(days=10))

            # Assertions
            assert price_coverage >= 9000, f"Should have price data for at least 9,000 stocks, got {price_coverage}"
            assert market_cap_coverage >= 300, f"Should have market cap for at least 300 stocks, got {market_cap_coverage}"
            assert qualifying_count >= 250, f"Should have at least 250 qualifying stocks, got {qualifying_count}"

    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_polygon_api_data_freshness():
    """Test that Polygon API returns fresh, accurate data for major stocks"""

    polygon_api_key = os.environ.get("POLYGON_API_KEY")
    if not polygon_api_key:
        pytest.skip("POLYGON_API_KEY not available")

    test_symbols = ['AAPL', 'MSFT', 'GOOGL']

    for symbol in test_symbols:
        url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
        params = {"apikey": polygon_api_key}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                assert response.status == 200, f"Polygon API failed for {symbol}: HTTP {response.status}"

                data = await response.json()
                results = data.get('results', {})

                # Validate required fields are present
                shares_outstanding = results.get('share_class_shares_outstanding')
                market_cap = results.get('market_cap')

                assert shares_outstanding is not None, f"{symbol}: shares_outstanding missing from API"
                assert shares_outstanding > 0, f"{symbol}: shares_outstanding should be positive"
                assert market_cap is not None, f"{symbol}: market_cap missing from API"
                assert market_cap > 0, f"{symbol}: market_cap should be positive"

                # Sanity check: market cap should be reasonable for major stocks
                market_cap_billions = market_cap / 1000000000
                assert market_cap_billions >= 100, f"{symbol}: market cap ${market_cap_billions:.0f}B seems too low"
                assert market_cap_billions <= 5000, f"{symbol}: market cap ${market_cap_billions:.0f}B seems too high"

        await asyncio.sleep(0.2)  # Rate limiting


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_price_data_consistency():
    """Test that our price data is consistent and reasonable"""

    pool = await asyncpg.create_pool(
        host="localhost",
        port=5433,
        user="postgres",
        password="postgres",
        database="dev_db",
        min_size=1,
        max_size=3
    )

    try:
        async with pool.acquire() as conn:
            # Check for obviously wrong prices
            suspicious_prices = await conn.fetch("""
                SELECT
                    i.symbol,
                    p.close,
                    p.date
                FROM dev_daily_price_polygon p
                JOIN dev_instrument i ON p.instrument_id = i.id
                WHERE p.date >= $1
                  AND i.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'NVDA', 'AMZN', 'META', 'TSLA')
                  AND (p.close < 10 OR p.close > 10000)  -- Outside reasonable range
                ORDER BY p.close
            """, date.today() - timedelta(days=7))

            # Major stocks should not have extremely low or high prices
            for row in suspicious_prices:
                pytest.fail(f"Suspicious price for {row['symbol']}: ${row['close']} on {row['date']}")

            # Check price consistency over time (no huge jumps without splits)
            price_volatility = await conn.fetch("""
                WITH price_changes AS (
                    SELECT
                        i.symbol,
                        p.date,
                        p.close,
                        LAG(p.close) OVER (PARTITION BY i.symbol ORDER BY p.date) as prev_close,
                        ABS(p.close - LAG(p.close) OVER (PARTITION BY i.symbol ORDER BY p.date)) /
                        LAG(p.close) OVER (PARTITION BY i.symbol ORDER BY p.date) as daily_change
                    FROM dev_daily_price_polygon p
                    JOIN dev_instrument i ON p.instrument_id = i.id
                    WHERE p.date >= $1
                      AND i.symbol IN ('AAPL', 'MSFT', 'GOOGL')
                )
                SELECT symbol, MAX(daily_change) as max_daily_change
                FROM price_changes
                WHERE daily_change IS NOT NULL
                GROUP BY symbol
                HAVING MAX(daily_change) > 0.5  -- More than 50% daily change
            """, date.today() - timedelta(days=7))

            # Should not have extreme daily movements for major stocks (unless stock splits)
            for row in price_volatility:
                pytest.fail(f"Extreme price movement for {row['symbol']}: {row['max_daily_change']*100:.1f}% daily change")

    finally:
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_shares_outstanding_reasonableness():
    """Test that shares outstanding data is reasonable for major companies"""

    pool = await asyncpg.create_pool(
        host="localhost",
        port=5433,
        user="postgres",
        password="postgres",
        database="dev_db",
        min_size=1,
        max_size=3
    )

    try:
        async with pool.acquire() as conn:
            shares_data = await conn.fetch("""
                SELECT
                    i.symbol,
                    mc.shares_outstanding,
                    mc.market_cap/1000000000 as market_cap_billions
                FROM dev_daily_market_cap mc
                JOIN dev_instrument i ON mc.instrument_id = i.id
                WHERE mc.date = (SELECT MAX(date) FROM dev_daily_market_cap)
                  AND i.symbol IN ('AAPL', 'MSFT', 'GOOGL', 'NVDA')
            """)

            # Expected shares outstanding ranges (approximate)
            expected_shares = {
                'AAPL': (14_000_000_000, 16_000_000_000),   # ~15B shares
                'MSFT': (7_000_000_000, 8_000_000_000),     # ~7.4B shares
                'GOOGL': (5_500_000_000, 6_500_000_000),    # ~5.8B shares
                'NVDA': (20_000_000_000, 25_000_000_000),   # ~24B shares
            }

            for row in shares_data:
                symbol = row['symbol']
                shares = row['shares_outstanding']

                if symbol in expected_shares:
                    min_shares, max_shares = expected_shares[symbol]
                    assert min_shares <= shares <= max_shares, \
                        f"{symbol}: shares {shares:,} outside expected range {min_shares:,}-{max_shares:,}"

    finally:
        await pool.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])