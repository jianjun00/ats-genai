from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime

class EventIn(BaseModel):
    event_type: str
    symbol: Optional[str]
    event_time: datetime
    reported_time: Optional[datetime] = None
    source: Optional[str] = None
    data: dict
    sources: Optional[List[str]] = None  # List of contributing sources
    raw: Optional[Dict[str, Any]] = None # Raw data from each source

class EventOut(EventIn):
    id: int
    created_at: datetime
