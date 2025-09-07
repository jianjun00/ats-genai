from abc import ABC, abstractmethod
from typing import List
from .models import InstrumentMetadata, EODPrice, TickData, IntervalData

class VendorAdapter(ABC):
    """
    Abstract base class for all vendor data adapters.
    Each method returns canonical schema objects.
    """
    vendor_name: str

    @abstractmethod
    def fetch_instruments(self) -> List[InstrumentMetadata]:
        pass

    @abstractmethod
    def fetch_eod(self, symbols: List[str], start_date, end_date) -> List[EODPrice]:
        pass

    @abstractmethod
    def fetch_ticks(self, symbol: str, start_dt, end_dt) -> List[TickData]:
        pass

    @abstractmethod
    def fetch_interval(self, symbol: str, interval: str, start_dt, end_dt) -> List[IntervalData]:
        pass
