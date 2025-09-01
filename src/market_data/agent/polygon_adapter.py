import os
import requests
from typing import List, Optional
from .base_adapter import VendorAdapter
from .models import InstrumentMetadata, EODPrice

class PolygonAdapter(VendorAdapter):
    vendor_name = "polygon"
    BASE_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise Exception("Please set your POLYGON_API_KEY environment variable or pass api_key explicitly.")

    def fetch_instruments(self) -> List[InstrumentMetadata]:
        # Example: fetch instrument metadata from Polygon reference API
        url = f"https://api.polygon.io/v3/reference/tickers?active=true&apiKey={self.api_key}"
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        instruments = []
        for row in results:
            instruments.append(InstrumentMetadata(
                instrument_id=row.get("cik", row.get("ticker")),
                symbol=row.get("ticker"),
                name=row.get("name"),
                exchange=row.get("primary_exchange"),
                sector=row.get("sic_description"),
                list_date=row.get("list_date"),
                delist_date=row.get("delisted_utc"),
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
                req_path = f"tests/data/polygon_{ticker.lower()}_{start_date}_{end_date}_request.json"
                resp_path = f"tests/data/polygon_{ticker.lower()}_{start_date}_{end_date}_response.json"
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
            for row in data.get("results", []):
                # Fix timezone handling - use timezone-aware datetime
                from zoneinfo import ZoneInfo
                utc_dt = datetime.fromtimestamp(row["t"] / 1000, tz=ZoneInfo("UTC"))
                date_val = utc_dt.date()
                eod_prices.append(EODPrice(
                    instrument_id=ticker,
                    date=date_val,
                    open=row.get("o"),
                    high=row.get("h"),
                    low=row.get("l"),
                    close=row.get("c"),
                    adj_close=None,  # Polygon does not provide adjusted close directly
                    volume=row.get("v"),
                    vendor=self.vendor_name,
                    quality_score=None,
                    provenance={"polygon_row": row}
                ))
        return eod_prices

    def fetch_ticks(self, symbol: str, start_dt, end_dt):
        raise NotImplementedError("PolygonAdapter.fetch_ticks is not implemented yet.")

    def fetch_interval(self, symbol: str, interval: str, start_dt, end_dt):
        raise NotImplementedError("PolygonAdapter.fetch_interval is not implemented yet.")
