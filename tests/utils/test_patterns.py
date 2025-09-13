"""
Common test patterns and utilities for ATS platform tests.

This module consolidates frequently used test patterns across the 300+ test files
to reduce duplication and improve maintainability.
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Union
from unittest.mock import Mock, patch, AsyncMock
from contextlib import asynccontextmanager
import pytest


class DatabaseTestHelper:
    """Helper class for database-related tests."""

    @staticmethod
    def mock_connection_with_data(query_results: Dict[str, List[Dict]]):
        """Create a mock database connection that returns specific data for queries."""
        def side_effect(query, params=None):
            # Match query patterns to return appropriate data
            query_lower = query.lower()
            if 'select count' in query_lower:
                return [{'count': len(query_results.get('default', []))}]
            elif 'instruments' in query_lower:
                return query_results.get('instruments', [])
            elif 'daily_prices' in query_lower:
                return query_results.get('prices', [])
            else:
                return query_results.get('default', [])

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.side_effect = side_effect
        mock_cursor.fetchone.side_effect = lambda: side_effect(mock_cursor.fetchall.call_args[0][0])[0] if side_effect(mock_cursor.fetchall.call_args[0][0]) else None

        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)

        return mock_conn

    @staticmethod
    def create_mock_db_manager(connection_success: bool = True, query_results: Optional[Dict] = None):
        """Create a mock database manager for testing."""
        mock_manager = Mock()
        mock_manager.check_connection.return_value = connection_success

        if query_results:
            mock_manager.execute_query.side_effect = lambda q, p=None: query_results.get(
                q.split()[1].lower() if len(q.split()) > 1 else 'default', []
            )

        return mock_manager


class APITestHelper:
    """Helper class for API-related tests."""

    @staticmethod
    def mock_http_response(status_code: int = 200, json_data: Optional[Dict] = None, text_data: str = ""):
        """Create a mock HTTP response."""
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.json.return_value = json_data or {}
        mock_response.text = text_data
        mock_response.raise_for_status.return_value = None

        if status_code >= 400:
            from requests.exceptions import HTTPError
            mock_response.raise_for_status.side_effect = HTTPError(f"HTTP {status_code}")

        return mock_response

    @staticmethod
    @asynccontextmanager
    async def mock_aiohttp_session(responses: Dict[str, Dict]):
        """Mock aiohttp session for async API tests."""
        mock_session = AsyncMock()

        async def mock_get(url, **kwargs):
            response_data = responses.get(url, {'status': 200, 'json': {}})
            mock_response = AsyncMock()
            mock_response.status = response_data.get('status', 200)
            mock_response.json = AsyncMock(return_value=response_data.get('json', {}))
            mock_response.text = AsyncMock(return_value=response_data.get('text', ''))
            return mock_response

        mock_session.get = mock_get

        with patch('aiohttp.ClientSession', return_value=mock_session):
            yield mock_session


class TimeSeriesTestHelper:
    """Helper class for time series data tests."""

    @staticmethod
    def generate_price_sequence(
        symbol: str = "AAPL",
        start_price: float = 100.0,
        days: int = 30,
        volatility: float = 0.02,
        trend: float = 0.001
    ) -> List[Dict]:
        """Generate a realistic price sequence for testing."""
        import random

        data = []
        current_price = start_price
        base_date = datetime(2024, 1, 1)

        for i in range(days):
            # Skip weekends
            current_date = base_date + timedelta(days=i)
            if current_date.weekday() >= 5:  # Saturday or Sunday
                continue

            # Generate price movement
            daily_change = random.gauss(trend, volatility)
            current_price *= (1 + daily_change)

            # Generate OHLC from close price
            daily_vol = volatility * 0.5
            high = current_price * (1 + random.uniform(0, daily_vol))
            low = current_price * (1 - random.uniform(0, daily_vol))
            open_price = current_price + random.gauss(0, current_price * daily_vol * 0.3)

            # Ensure OHLC relationships
            high = max(high, open_price, current_price)
            low = min(low, open_price, current_price)

            # Generate volume
            volume = int(random.lognormvariate(13, 0.5))  # Around 1M average

            data.append({
                'symbol': symbol,
                'date': current_date.date(),
                'open': round(open_price, 2),
                'high': round(high, 2),
                'low': round(low, 2),
                'close': round(current_price, 2),
                'volume': volume,
                'timestamp': current_date
            })

        return data

    @staticmethod
    def create_market_data_batch(symbols: List[str], days: int = 30) -> Dict[str, List[Dict]]:
        """Create market data for multiple symbols."""
        return {
            symbol: TimeSeriesTestHelper.generate_price_sequence(
                symbol=symbol,
                start_price=100.0 + hash(symbol) % 100,  # Different starting prices
                days=days
            )
            for symbol in symbols
        }


class AsyncTestHelper:
    """Helper class for async tests."""

    @staticmethod
    async def run_with_timeout(coro, timeout: float = 5.0):
        """Run an async coroutine with timeout."""
        return await asyncio.wait_for(coro, timeout=timeout)

    @staticmethod
    def create_mock_async_pool():
        """Create a mock async database pool."""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        return mock_pool, mock_conn


class FileTestHelper:
    """Helper class for file-based tests."""

    @staticmethod
    def create_temp_config_file(config_data: Dict, file_path: str):
        """Create a temporary configuration file."""
        import os
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if file_path.endswith('.json'):
            with open(file_path, 'w') as f:
                json.dump(config_data, f, indent=2)
        elif file_path.endswith('.gin'):
            with open(file_path, 'w') as f:
                for key, value in config_data.items():
                    if isinstance(value, str):
                        f.write(f"{key} = '{value}'\n")
                    else:
                        f.write(f"{key} = {value}\n")

    @staticmethod
    def create_temp_data_file(data: Union[List, Dict], file_path: str):
        """Create a temporary data file."""
        import os
        import pandas as pd

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if file_path.endswith('.json'):
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        elif file_path.endswith('.parquet'):
            df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
            df.to_parquet(file_path, index=False)
        elif file_path.endswith('.csv'):
            df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
            df.to_csv(file_path, index=False)


class ValidationTestHelper:
    """Helper class for validation tests."""

    @staticmethod
    def assert_price_data_valid(price_data: List[Dict]):
        """Assert that price data is valid."""
        assert price_data, "Price data should not be empty"

        for record in price_data:
            assert 'open' in record, "Price record should have 'open' field"
            assert 'high' in record, "Price record should have 'high' field"
            assert 'low' in record, "Price record should have 'low' field"
            assert 'close' in record, "Price record should have 'close' field"
            assert 'volume' in record, "Price record should have 'volume' field"

            # Validate OHLC relationships
            assert record['high'] >= record['open'], f"High should be >= open: {record}"
            assert record['high'] >= record['close'], f"High should be >= close: {record}"
            assert record['low'] <= record['open'], f"Low should be <= open: {record}"
            assert record['low'] <= record['close'], f"Low should be <= close: {record}"

            # Validate positive values
            assert record['volume'] >= 0, f"Volume should be non-negative: {record}"
            assert all(record[field] > 0 for field in ['open', 'high', 'low', 'close']), \
                f"OHLC prices should be positive: {record}"

    @staticmethod
    def assert_api_response_structure(response: Dict, required_fields: List[str]):
        """Assert that API response has required structure."""
        assert isinstance(response, dict), "Response should be a dictionary"

        for field in required_fields:
            assert field in response, f"Response should contain field '{field}'"

        # Common API response validations
        if 'status' in response:
            assert response['status'] in ['OK', 'SUCCESS', 'ok', 'success'], \
                f"Invalid status: {response['status']}"

        if 'results' in response:
            assert isinstance(response['results'], list), "Results should be a list"


class PerformanceTestHelper:
    """Helper class for performance tests."""

    @staticmethod
    def measure_execution_time(func: Callable, *args, **kwargs) -> tuple:
        """Measure execution time of a function."""
        import time
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        return result, execution_time

    @staticmethod
    async def measure_async_execution_time(coro) -> tuple:
        """Measure execution time of an async coroutine."""
        import time
        start_time = time.perf_counter()
        result = await coro
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        return result, execution_time

    @staticmethod
    def assert_performance_threshold(execution_time: float, threshold: float, operation_name: str):
        """Assert that execution time is within threshold."""
        assert execution_time <= threshold, \
            f"{operation_name} took {execution_time:.3f}s, expected <= {threshold}s"


# Convenience functions for common test patterns
def skip_if_no_database():
    """Skip test if no database connection available."""
    return pytest.mark.skipif(
        not hasattr(pytest, '_database_available'),
        reason="Database connection not available"
    )


def skip_if_no_api_key(vendor: str):
    """Skip test if API key not available."""
    import os
    api_key_env = f"{vendor.upper()}_API_KEY"
    return pytest.mark.skipif(
        not os.getenv(api_key_env),
        reason=f"{vendor} API key not available"
    )


def requires_internet():
    """Mark test as requiring internet connection."""
    return pytest.mark.skipif(
        not _check_internet_connection(),
        reason="Internet connection required"
    )


def _check_internet_connection() -> bool:
    """Check if internet connection is available."""
    import socket
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        return True
    except OSError:
        return False