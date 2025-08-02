import os
import requests
from datetime import datetime
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
            if resp.status_code != 200:
                continue
            data = resp.json()
            for row in data.get("results", []):
                date_val = datetime.utcfromtimestamp(row["t"] / 1000).date()
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
