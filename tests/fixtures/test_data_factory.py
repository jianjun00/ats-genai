"""
Test data factory for generating financial market data.

This module replaces static JSON test files with parameterized data generators,
reducing storage overhead and improving test maintainability.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import random


@dataclass
class PriceDataPoint:
    """A single price data point for financial instruments."""
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    symbol: str = "AAPL"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return asdict(self)


@dataclass
class APIResponse:
    """Generic API response structure."""
    status: str
    request_id: str
    count: int
    results: List[Dict[str, Any]]
    next_url: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return asdict(self)


class TestDataFactory:
    """Factory for generating realistic test data for financial markets."""

    def __init__(self, seed: int = 42):
        """
        Initialize the factory with a random seed for reproducible results.

        Args:
            seed: Random seed for reproducible data generation
        """
        random.seed(seed)
        self.base_prices = {
            'AAPL': 150.0,
            'TSLA': 800.0,
            'MSFT': 300.0,
            'GOOGL': 2500.0,
            'AMZN': 3000.0
        }

    def generate_price_data(self, symbol: str = "AAPL",
                          start_date: datetime = None,
                          end_date: datetime = None,
                          frequency: str = "1D") -> List[PriceDataPoint]:
        """
        Generate realistic price data for a given symbol and date range.

        Args:
            symbol: Stock symbol
            start_date: Start date for data generation
            end_date: End date for data generation
            frequency: Data frequency ('1D', '1H', '1M')

        Returns:
            List of PriceDataPoint objects
        """
        if start_date is None:
            start_date = datetime(2024, 1, 1)
        if end_date is None:
            end_date = datetime(2024, 1, 31)

        base_price = self.base_prices.get(symbol, 100.0)
        current_price = base_price
        data_points = []

        # Determine time delta based on frequency
        if frequency == "1D":
            delta = timedelta(days=1)
        elif frequency == "1H":
            delta = timedelta(hours=1)
        elif frequency == "1M":
            delta = timedelta(minutes=1)
        else:
            raise ValueError(f"Unsupported frequency: {frequency}")

        current_date = start_date
        while current_date <= end_date:
            # Skip weekends for daily data
            if frequency == "1D" and current_date.weekday() >= 5:
                current_date += delta
                continue

            # Generate realistic price movement
            volatility = 0.02  # 2% daily volatility
            if frequency == "1H":
                volatility = 0.005  # Lower for hourly
            elif frequency == "1M":
                volatility = 0.001  # Lower for minute data

            # Random walk with mean reversion
            change = random.gauss(0, volatility)
            mean_reversion = (base_price - current_price) * 0.001
            current_price *= (1 + change + mean_reversion)

            # Generate OHLC from current price
            daily_vol = volatility * 0.5
            high = current_price * (1 + random.uniform(0, daily_vol))
            low = current_price * (1 - random.uniform(0, daily_vol))
            open_price = current_price + random.gauss(0, current_price * daily_vol * 0.3)

            # Ensure OHLC relationships are valid
            high = max(high, open_price, current_price)
            low = min(low, open_price, current_price)

            # Generate volume (log-normal distribution)
            base_volume = 1000000 if frequency == "1D" else 10000
            volume = int(random.lognormvariate(
                random.log(base_volume), 0.5
            ))

            data_point = PriceDataPoint(
                timestamp=current_date.isoformat(),
                open=round(open_price, 2),
                high=round(high, 2),
                low=round(low, 2),
                close=round(current_price, 2),
                volume=volume,
                symbol=symbol
            )

            data_points.append(data_point)
            current_date += delta

        return data_points

    def generate_polygon_response(self, symbol: str = "AAPL",
                                start_date: str = "2024-01-01",
                                end_date: str = "2024-01-02") -> Dict[str, Any]:
        """
        Generate a Polygon API response format.

        Args:
            symbol: Stock symbol
            start_date: Start date string
            end_date: End date string

        Returns:
            Dictionary representing Polygon API response
        """
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)

        price_data = self.generate_price_data(symbol, start_dt, end_dt)

        results = []
        for data_point in price_data:
            # Convert to Polygon format
            result = {
                "T": symbol,
                "t": int(datetime.fromisoformat(data_point.timestamp).timestamp() * 1000),
                "o": data_point.open,
                "h": data_point.high,
                "l": data_point.low,
                "c": data_point.close,
                "v": data_point.volume,
                "vw": (data_point.high + data_point.low + data_point.close) / 3,  # VWAP approximation
                "n": random.randint(1000, 10000)  # Number of transactions
            }
            results.append(result)

        response = APIResponse(
            status="OK",
            request_id=f"req_{random.randint(100000, 999999)}",
            count=len(results),
            results=results
        )

        return response.to_dict()

    def generate_tiingo_response(self, symbol: str = "AAPL",
                               start_date: str = "2024-01-01",
                               end_date: str = "2024-01-02") -> List[Dict[str, Any]]:
        """
        Generate a Tiingo API response format.

        Args:
            symbol: Stock symbol
            start_date: Start date string
            end_date: End date string

        Returns:
            List representing Tiingo API response
        """
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)

        price_data = self.generate_price_data(symbol, start_dt, end_dt)

        results = []
        for data_point in price_data:
            # Convert to Tiingo format
            result = {
                "date": data_point.timestamp.split('T')[0],
                "open": data_point.open,
                "high": data_point.high,
                "low": data_point.low,
                "close": data_point.close,
                "volume": data_point.volume,
                "adjOpen": data_point.open,
                "adjHigh": data_point.high,
                "adjLow": data_point.low,
                "adjClose": data_point.close,
                "adjVolume": data_point.volume,
                "divCash": 0.0,
                "splitFactor": 1.0
            }
            results.append(result)

        return results

    def generate_test_datasets(self) -> Dict[str, Any]:
        """
        Generate a comprehensive set of test datasets for common test scenarios.

        Returns:
            Dictionary containing various test datasets
        """
        datasets = {}

        # Common symbols and date ranges for testing
        symbols = ['AAPL', 'TSLA', 'MSFT']
        date_ranges = [
            ('2024-01-01', '2024-01-02'),
            ('2024-08-15', '2024-08-16'),
            ('2024-12-30', '2024-12-31')
        ]

        # Generate Polygon responses
        datasets['polygon'] = {}
        for symbol in symbols:
            datasets['polygon'][symbol] = {}
            for start_date, end_date in date_ranges:
                key = f"{start_date}_{end_date}"
                datasets['polygon'][symbol][key] = {
                    'request': {
                        'symbol': symbol,
                        'from': start_date,
                        'to': end_date,
                        'adjusted': True,
                        'sort': 'asc'
                    },
                    'response': self.generate_polygon_response(symbol, start_date, end_date)
                }

        # Generate Tiingo responses
        datasets['tiingo'] = {}
        for symbol in symbols:
            datasets['tiingo'][symbol] = {}
            for start_date, end_date in date_ranges:
                key = f"{start_date}_{end_date}"
                datasets['tiingo'][symbol][key] = {
                    'request': {
                        'symbol': symbol,
                        'startDate': start_date,
                        'endDate': end_date,
                        'format': 'json'
                    },
                    'response': self.generate_tiingo_response(symbol, start_date, end_date)
                }

        return datasets

    def save_datasets_to_files(self, output_dir: str = "tests/fixtures/generated_data"):
        """
        Save generated datasets to JSON files (for compatibility with existing tests).

        Args:
            output_dir: Directory to save files
        """
        import os

        os.makedirs(output_dir, exist_ok=True)
        datasets = self.generate_test_datasets()

        for vendor in datasets:
            vendor_dir = os.path.join(output_dir, vendor)
            os.makedirs(vendor_dir, exist_ok=True)

            for symbol in datasets[vendor]:
                for date_range in datasets[vendor][symbol]:
                    data = datasets[vendor][symbol][date_range]

                    # Save request and response separately
                    req_filename = f"{vendor}_{symbol.lower()}_{date_range}_request.json"
                    resp_filename = f"{vendor}_{symbol.lower()}_{date_range}_response.json"

                    with open(os.path.join(vendor_dir, req_filename), 'w') as f:
                        json.dump(data['request'], f, indent=2)

                    with open(os.path.join(vendor_dir, resp_filename), 'w') as f:
                        json.dump(data['response'], f, indent=2)


# Utility functions for test fixtures
def get_sample_price_data(symbol: str = "AAPL",
                         days: int = 30,
                         start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """
    Get sample price data for testing.

    Args:
        symbol: Stock symbol
        days: Number of days of data
        start_date: Start date (defaults to 30 days ago)

    Returns:
        List of price data dictionaries
    """
    if start_date is None:
        start_date = datetime.now() - timedelta(days=days)

    end_date = start_date + timedelta(days=days)

    factory = TestDataFactory()
    price_data = factory.generate_price_data(symbol, start_date, end_date)

    return [data_point.to_dict() for data_point in price_data]


def get_polygon_test_data(symbol: str = "AAPL",
                         start_date: str = "2024-01-01",
                         end_date: str = "2024-01-02") -> Dict[str, Any]:
    """
    Get Polygon-formatted test data.

    Args:
        symbol: Stock symbol
        start_date: Start date string
        end_date: End date string

    Returns:
        Polygon API response dictionary
    """
    factory = TestDataFactory()
    return factory.generate_polygon_response(symbol, start_date, end_date)


def get_tiingo_test_data(symbol: str = "AAPL",
                        start_date: str = "2024-01-01",
                        end_date: str = "2024-01-02") -> List[Dict[str, Any]]:
    """
    Get Tiingo-formatted test data.

    Args:
        symbol: Stock symbol
        start_date: Start date string
        end_date: End date string

    Returns:
        Tiingo API response list
    """
    factory = TestDataFactory()
    return factory.generate_tiingo_response(symbol, start_date, end_date)


# Pytest fixtures for common use cases
import pytest

@pytest.fixture
def test_data_factory():
    """Pytest fixture providing a TestDataFactory instance."""
    return TestDataFactory()

@pytest.fixture
def sample_aapl_data():
    """Pytest fixture providing sample AAPL price data."""
    return get_sample_price_data("AAPL", days=5)

@pytest.fixture
def polygon_aapl_response():
    """Pytest fixture providing Polygon AAPL response."""
    return get_polygon_test_data("AAPL", "2024-01-01", "2024-01-02")

@pytest.fixture
def tiingo_aapl_response():
    """Pytest fixture providing Tiingo AAPL response."""
    return get_tiingo_test_data("AAPL", "2024-01-01", "2024-01-02")