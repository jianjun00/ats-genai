import os
import requests
import time
from typing import List, Optional
from dataclasses import dataclass
from .base_adapter import VendorAdapter
from .models import InstrumentMetadata, EODPrice
import logging
import sys
import gin

@gin.configurable
@dataclass
class TiingoAdapterConfig:
    """Configuration for Tiingo adapter with tracking"""
    base_url: str = "https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    request_timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    rate_limit_delay: float = 1.0
    batch_size: int = 100

    # Response tracking settings
    track_response_sizes: bool = True
    track_latency: bool = True
    log_api_errors: bool = True

    # Data validation settings
    validate_prices: bool = True
    min_price: float = 0.01
    max_price: float = 100000.0

# Add scripts to path for API tracker
sys.path.append('/workspace/scripts')
from api_status_tracker import get_global_tracker

logger = logging.getLogger(__name__)

class TiingoAdapterWithTracking(VendorAdapter):
    """
    Enhanced Tiingo adapter with API status code tracking.

    Tracks all API calls with status codes, latency, and response sizes
    for monitoring and dashboard visualization.
    """
    vendor_name = "tiingo"

    def __init__(self, api_key: Optional[str] = None, config: TiingoAdapterConfig = None):
        self.config = config or TiingoAdapterConfig()
        self.api_key = api_key or os.getenv("TIINGO_API_KEY")
        if not self.api_key:
            raise Exception("Please set your TIINGO_API_KEY environment variable or pass api_key explicitly.")

        # Build base URL from config
        self.BASE_URL = f"{self.config.base_url}?startDate={{start}}&endDate={{end}}&token={{api_key}}"

        # Get global API tracker
        self.api_tracker = get_global_tracker()

    def _make_tracked_request(self, url: str, endpoint: str, symbol: Optional[str] = None) -> requests.Response:
        """
        Make HTTP request with full API tracking.

        Args:
            url: Full request URL
            endpoint: API endpoint type (daily_price_polygon, instruments, etc.)
            symbol: Symbol being requested (for context)

        Returns:
            requests.Response object
        """
        start_time = time.time()
        response = None
        error_message = None

        try:
            response = requests.get(url, timeout=30)
            latency_ms = (time.time() - start_time) * 1000

            # Get response size
            response_size = len(response.content) if response.content else 0

            # Track the request
            self.api_tracker.track_request(
                vendor=self.vendor_name,
                api_endpoint=endpoint,
                status_code=response.status_code,
                latency_ms=latency_ms,
                response_size_bytes=response_size,
                symbol=symbol,
                request_url=url
            )

            # Log based on status code
            if response.status_code == 200:
                logger.debug(f"✅ {self.vendor_name} {endpoint} {symbol}: {response.status_code} ({latency_ms:.1f}ms, {response_size} bytes)")
            elif response.status_code == 429:
                logger.warning(f"⚠️ {self.vendor_name} {endpoint} {symbol}: Rate limited ({response.status_code})")
            else:
                logger.error(f"❌ {self.vendor_name} {endpoint} {symbol}: Error {response.status_code}")

            return response

        except requests.exceptions.Timeout as e:
            latency_ms = (time.time() - start_time) * 1000
            error_message = f"Request timeout after 30s"

            # Track timeout as 408 Request Timeout
            self.api_tracker.track_request(
                vendor=self.vendor_name,
                api_endpoint=endpoint,
                status_code=408,
                latency_ms=latency_ms,
                error_message=error_message,
                symbol=symbol,
                request_url=url
            )

            logger.error(f"🕐 {self.vendor_name} {endpoint} {symbol}: Timeout ({latency_ms:.1f}ms)")
            raise

        except requests.exceptions.ConnectionError as e:
            latency_ms = (time.time() - start_time) * 1000
            error_message = f"Connection error: {str(e)}"

            # Track connection error as 503 Service Unavailable
            self.api_tracker.track_request(
                vendor=self.vendor_name,
                api_endpoint=endpoint,
                status_code=503,
                latency_ms=latency_ms,
                error_message=error_message,
                symbol=symbol,
                request_url=url
            )

            logger.error(f"🔌 {self.vendor_name} {endpoint} {symbol}: Connection error ({latency_ms:.1f}ms)")
            raise

        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            error_message = f"Request failed: {str(e)}"

            # Track general error as 500 Internal Server Error
            self.api_tracker.track_request(
                vendor=self.vendor_name,
                api_endpoint=endpoint,
                status_code=500,
                latency_ms=latency_ms,
                error_message=error_message,
                symbol=symbol,
                request_url=url
            )

            logger.error(f"💥 {self.vendor_name} {endpoint} {symbol}: Request failed ({latency_ms:.1f}ms) - {e}")
            raise

    def fetch_instruments(self) -> List[InstrumentMetadata]:
        """Fetch instrument metadata from Tiingo supported tickers API with tracking."""
        url = f"https://api.tiingo.com/tiingo/supported-tickers?token={self.api_key}"

        try:
            resp = self._make_tracked_request(url, "instruments")
            resp.raise_for_status()

            data = resp.json()
            instruments = []

            for row in data:
                instruments.append(InstrumentMetadata(
                    instrument_id=row.get("ticker"),
                    symbol=row.get("ticker"),
                    name=row.get("name"),
                    exchange=row.get("exchange"),
                    sector=None,  # Tiingo doesn't provide sector directly
                    list_date=None,  # Tiingo doesn't provide list date directly
                    delist_date=None,  # Tiingo doesn't provide delist date directly
                    vendor=self.vendor_name,
                    extra=row
                ))

            logger.info(f"✅ {self.vendor_name} instruments: Retrieved {len(instruments)} instruments")
            return instruments

        except Exception as e:
            logger.error(f"❌ {self.vendor_name} instruments: Failed to fetch - {e}")
            return []

    def fetch_eod(self, symbols: List[str], start_date, end_date) -> List[EODPrice]:
        """Fetch EOD prices with full API tracking."""
        eod_prices = []

        for ticker in symbols:
            try:
                url = self.BASE_URL.format(
                    ticker=ticker,
                    start=start_date,
                    end=end_date,
                    api_key=self.api_key
                )

                resp = self._make_tracked_request(url, "daily_price_polygon", symbol=ticker)

                # Handle rate limiting (429 errors)
                if resp.status_code == 429:
                    logger.warning(f"⚠️ Rate limited for {ticker}, skipping")
                    continue

                # Handle other HTTP errors
                if resp.status_code >= 400:
                    logger.warning(f"⚠️ HTTP {resp.status_code} for {ticker}, skipping")
                    continue

                # Process successful response
                data = resp.json()

                # Log request/response for AAPL/TSLA in date range (existing logging)
                from datetime import datetime
                import json
                log_tickers = {"AAPL", "TSLA"}
                log_start = datetime(2020, 1, 10)
                log_end = datetime(2024, 12, 31)

                def in_log_range(s, e):
                    try:
                        sdt = datetime.strptime(str(s), "%Y-%m-%d")
                        edt = datetime.strptime(str(e), "%Y-%m-%d")
                        return not (edt < log_start or sdt > log_end)
                    except Exception:
                        return False

                if ticker.upper() in log_tickers and in_log_range(start_date, end_date):
                    os.makedirs("tests/data", exist_ok=True)
                    req_path = f"tests/data/tiingo_{ticker.lower()}_{start_date}_{end_date}_request.json"
                    resp_path = f"tests/data/tiingo_{ticker.lower()}_{start_date}_{end_date}_response.json"

                    with open(req_path, "w") as f:
                        json.dump({"url": url}, f, indent=2)
                    try:
                        with open(resp_path, "w") as f:
                            json.dump(data, f, indent=2)
                    except Exception as e:
                        with open(resp_path, "w") as f:
                            json.dump({"error": str(e), "raw_response": str(resp.content)}, f, indent=2)

                # Convert response to EODPrice objects
                for price_data in data:
                    try:
                        eod_prices.append(EODPrice(
                            symbol=ticker,
                            date=price_data.get("date"),
                            open=price_data.get("open"),
                            high=price_data.get("high"),
                            low=price_data.get("low"),
                            close=price_data.get("close"),
                            adjusted_close=price_data.get("adjClose"),
                            volume=price_data.get("volume"),
                            vendor=self.vendor_name
                        ))
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to parse price data for {ticker}: {e}")
                        continue

            except Exception as e:
                logger.error(f"❌ Failed to fetch EOD for {ticker}: {e}")
                continue

        logger.info(f"✅ {self.vendor_name} daily_price_polygon: Retrieved {len(eod_prices)} price records for {len(symbols)} symbols")
        return eod_prices

    def fetch_fundamentals(self, symbols: List[str]) -> List[dict]:
        """Fetch fundamentals data with API tracking."""
        fundamentals = []

        for ticker in symbols:
            try:
                url = f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/daily?token={self.api_key}"

                resp = self._make_tracked_request(url, "fundamentals", symbol=ticker)

                if resp.status_code == 429:
                    logger.warning(f"⚠️ Rate limited for {ticker} fundamentals, skipping")
                    continue

                if resp.status_code >= 400:
                    logger.warning(f"⚠️ HTTP {resp.status_code} for {ticker} fundamentals, skipping")
                    continue

                data = resp.json()
                fundamentals.append({
                    'symbol': ticker,
                    'vendor': self.vendor_name,
                    'data': data
                })

            except Exception as e:
                logger.error(f"❌ Failed to fetch fundamentals for {ticker}: {e}")
                continue

        logger.info(f"✅ {self.vendor_name} fundamentals: Retrieved data for {len(fundamentals)} symbols")
        return fundamentals

    def fetch_news(self, symbols: List[str], limit: int = 100) -> List[dict]:
        """Fetch news data with API tracking."""
        news_articles = []

        for ticker in symbols:
            try:
                url = f"https://api.tiingo.com/tiingo/news?tickers={ticker}&token={self.api_key}&limit={limit}"

                resp = self._make_tracked_request(url, "news", symbol=ticker)

                if resp.status_code == 429:
                    logger.warning(f"⚠️ Rate limited for {ticker} news, skipping")
                    continue

                if resp.status_code >= 400:
                    logger.warning(f"⚠️ HTTP {resp.status_code} for {ticker} news, skipping")
                    continue

                data = resp.json()
                news_articles.extend([
                    {
                        'symbol': ticker,
                        'vendor': self.vendor_name,
                        'article': article
                    }
                    for article in data
                ])

            except Exception as e:
                logger.error(f"❌ Failed to fetch news for {ticker}: {e}")
                continue

        logger.info(f"✅ {self.vendor_name} news: Retrieved {len(news_articles)} articles for {len(symbols)} symbols")
        return news_articles