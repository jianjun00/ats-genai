from typing import Optional, List, Dict, Any
from datetime import date, datetime
from pydantic import BaseModel, Field

# Instrument Metadata
class InstrumentMetadata(BaseModel):
    instrument_id: str
    symbol: str
    name: Optional[str]
    exchange: Optional[str]
    sector: Optional[str]
    list_date: Optional[date]
    delist_date: Optional[date]
    vendor: Optional[str]
    extra: Optional[Dict[str, Any]] = None

# EOD Price
class EODPrice(BaseModel):
    instrument_id: str
    date: date
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    adj_close: Optional[float]
    volume: Optional[float]
    vendor: Optional[str]
    quality_score: Optional[float]
    provenance: Optional[Dict[str, Any]] = None

# Tick Data
class TickData(BaseModel):
    instrument_id: str
    timestamp: datetime
    price: float
    size: Optional[float]
    exchange: Optional[str]
    vendor: Optional[str]
    quality_score: Optional[float]
    provenance: Optional[Dict[str, Any]] = None

# Interval Data
class IntervalData(BaseModel):
    instrument_id: str
    interval: str  # e.g. '1m', '5m', '1h'
    start_time: datetime
    end_time: datetime
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[float]
    vendor: Optional[str]
    quality_score: Optional[float]
    provenance: Optional[Dict[str, Any]] = None

# Reconciliation result with audit
class ReconciledRecord(BaseModel):
    data_type: str  # 'instrument', 'eod', 'tick', 'interval'
    instrument_id: str
    as_of: datetime
    value: Dict[str, Any]
    quality_score: float
    sources: List[str]
    rationale: Optional[str] = None
    provenance: Optional[Dict[str, Any]] = None
