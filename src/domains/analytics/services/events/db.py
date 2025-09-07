from core.config.environment import Environment
from core.dao.events_dao import EventsDAO
from core.dao.instruments_dao import InstrumentsDAO
from .schemas import EventIn
from typing import Optional

async def get_events(symbol: Optional[str]=None, event_type: Optional[str]=None, start: Optional[str]=None, end: Optional[str]=None):
    env = Environment()
    events_dao = EventsDAO(env)
    
    # If symbol is provided, look up the instrument_id
    instrument_id = None
    if symbol:
        instruments_dao = InstrumentsDAO(env)
        instrument = await instruments_dao.get_instrument_by_symbol(symbol)
        if instrument:
            instrument_id = instrument['id']
    
    return await events_dao.get_events(instrument_id=instrument_id, event_type=event_type, start=start, end=end)

async def insert_event(event: EventIn):
    env = Environment()
    dao = EventsDAO(env)
    return await dao.insert_event(
        event.event_type, event.symbol, event.event_time, event.reported_time, event.source, event.data
    )
