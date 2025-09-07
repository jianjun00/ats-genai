import os
import requests
from typing import List, Optional
from .base_adapter import VendorAdapter
from .models import InstrumentMetadata, EODPrice
from shared.utils.vendor_api_keys import get_tiingo_api_key
from shared.utils.backfill_framework import VendorRateLimiters, BackfillStats

class TiingoAdapter(VendorAdapter):
    vendor_name = "tiingo"
    BASE_URL = "https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={start}&endDate={end}&token={api_key}"

    def __init__(self, api_key: Optional[str] = None):
        # Use shared utilities for robust API key management
        self.api_key = api_key or get_tiingo_api_key()
        
        # Initialize shared utilities for monitoring and rate limiting
        self.stats = BackfillStats()
        self.rate_limiter = VendorRateLimiters.tiingo()

    def fetch_instruments(self) -> List[InstrumentMetadata]:
        # Example: fetch instrument metadata from Tiingo supported tickers API
        url = f"https://api.tiingo.com/tiingo/supported-tickers?token={self.api_key}"
        
        # Use shared rate limiting
        import asyncio
        asyncio.run(self.rate_limiter.wait_if_needed())
        
        # Track API call statistics
        self.stats.api_calls_made += 1
        
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self.stats.api_errors += 1
            raise
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
            
        # Track records fetched
        self.stats.records_fetched += len(instruments)
        return instruments

    def fetch_eod(self, symbols: List[str], start_date, end_date) -> List[EODPrice]:
        eod_prices = []
        for ticker in symbols:
            # Use shared rate limiting to prevent 429 errors
            import asyncio
            asyncio.run(self.rate_limiter.wait_if_needed())
            
            url = self.BASE_URL.format(
                ticker=ticker,
                start=start_date,
                end=end_date,
                api_key=self.api_key
            )
            
            # Track API call statistics
            self.stats.api_calls_made += 1
            
            try:
                resp = requests.get(url)
                
                # Handle rate limiting with shared framework
                if resp.status_code == 429:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Rate limited for {ticker} despite shared rate limiter")
                    self.stats.api_errors += 1
                    continue
                    
                resp.raise_for_status()
            except Exception as e:
                self.stats.api_errors += 1
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error fetching {ticker}: {e}")
                continue
            
            # Log request/response for AAPL/TSLA in date range
            from datetime import datetime
            import json, os
            log_tickers = {"AAPL", "TSLA"}
            log_start = datetime(2020, 1, 10)
            log_end = datetime(2024, 12, 31)
            # Dates as strings, but comparison is safe if format is YYYY-MM-DD
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
                        json.dump(resp.json(), f, indent=2)
                except Exception as e:
                    with open(resp_path, "w") as f:
                        f.write(f"[ERROR] Could not serialize response: {e}\n")
            
            if resp.status_code != 200:
                continue
            
            data = resp.json()
            for row in data:
                date_val = datetime.strptime(row["date"].split("T")[0], "%Y-%m-%d").date()
                eod_prices.append(EODPrice(
                    instrument_id=ticker,
                    date=date_val,
                    open=row.get("open"),
                    high=row.get("high"),
                    low=row.get("low"),
                    close=row.get("close"),
                    adj_close=row.get("adjClose"),
                    volume=row.get("volume"),
                    vendor=self.vendor_name,
                    quality_score=None,
                    provenance={"tiingo_row": row}
                ))
                
        # Track records fetched in EOD operation
        self.stats.records_fetched += len(eod_prices) 
        return eod_prices

    def fetch_ticks(self, symbol: str, start_dt, end_dt):
        raise NotImplementedError("TiingoAdapter.fetch_ticks is not implemented yet.")

    def fetch_interval(self, symbol: str, interval: str, start_dt, end_dt):
        raise NotImplementedError("TiingoAdapter.fetch_interval is not implemented yet.")
        
    def get_statistics_summary(self) -> dict:
        """Get comprehensive statistics summary using shared framework"""
        return {
            "vendor": self.vendor_name,
            "api_calls_made": self.stats.api_calls_made,
            "api_errors": self.stats.api_errors,
            "records_fetched": self.stats.records_fetched,
            "success_rate": self.stats.success_rate,
            "rate_limiter_status": self.rate_limiter.get_status() if hasattr(self.rate_limiter, 'get_status') else "active"
        }
        
    def log_final_summary(self, logger):
        """Log comprehensive operation summary using shared framework"""
        self.stats.log_final_summary(logger)
