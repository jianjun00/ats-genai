from fastapi import APIRouter
from .schemas import EventIn
from .db import get_events, insert_event
from typing import Optional
from datetime import datetime

router = APIRouter()

@router.get("/events")
async def query_events(
    symbol: Optional[str] = None,
    event_type: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None
):
    return await get_events(symbol, event_type, start, end)

@router.post("/events")
async def add_event(event: EventIn):
    return await insert_event(event)
