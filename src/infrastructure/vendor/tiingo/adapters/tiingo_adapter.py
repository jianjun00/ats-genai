import os
import requests
from typing import List, Optional
from .base_adapter import VendorAdapter
from .models import InstrumentMetadata, EODPrice

class TiingoAdapter(VendorAdapter):
    vendor_name = "tiingo"
    BASE_URL = "https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={start}&endDate={end}&token={api_key}"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("TIINGO_API_KEY")
        if not self.api_key:
            raise Exception("Please set your TIINGO_API_KEY environment variable or pass api_key explicitly.")

    def fetch_instruments(self) -> List[InstrumentMetadata]:
        # Example: fetch instrument metadata from Tiingo supported tickers API
        url = f"https://api.tiingo.com/tiingo/supported-tickers?token={self.api_key}"
        resp = requests.get(url)
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
        return instruments

    def fetch_eod(self, symbols: List[str], start_date, end_date) -> List[EODPrice]:
        eod_prices = []
        for ticker in symbols:
            url = self.BASE_URL.format(
                ticker=ticker,
                start=start_date,
                end=end_date,
                api_key=self.api_key
            )
            resp = requests.get(url)

            # Handle rate limiting (429 errors)
            if resp.status_code == 429:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Rate limited for {ticker}, skipping")
                # Don't retry immediately, just skip this symbol
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
        return eod_prices

    def fetch_ticks(self, symbol: str, start_dt, end_dt):
        raise NotImplementedError("TiingoAdapter.fetch_ticks is not implemented yet.")

    def fetch_interval(self, symbol: str, interval: str, start_dt, end_dt):
        raise NotImplementedError("TiingoAdapter.fetch_interval is not implemented yet.")
