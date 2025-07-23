from config.environment import get_environment, Environment
from db.dao.events_dao import EventsDAO
from .schemas import EventIn
from typing import Optional

async def get_events(symbol: Optional[str]=None, event_type: Optional[str]=None, start: Optional[str]=None, end: Optional[str]=None):
    env = get_environment()
    dao = EventsDAO(env)
    return await dao.get_events(symbol=symbol, event_type=event_type, start=start, end=end)

async def insert_event(event: EventIn):
    env = get_environment()
    dao = EventsDAO(env)
    return await dao.insert_event(
        event.event_type, event.symbol, event.event_time, event.reported_time, event.source, event.data
    )
