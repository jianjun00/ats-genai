from core.platform.config.environment import Environment
from core.dao.events_dao import EventsDAO
from domains.instruments.services.config.service_container import get_instrument_service
from .schemas import EventIn
from typing import Optional

async def get_events(symbol: Optional[str]=None, event_type: Optional[str]=None, start: Optional[str]=None, end: Optional[str]=None):
    env = Environment()
    events_dao = EventsDAO(env)

    # If symbol is provided, look up the instrument_id using InstrumentService
    instrument_id = None
    if symbol:
        instrument_service = await get_instrument_service(env)
        instrument_dto = await instrument_service.get_instrument_by_symbol(symbol)
        if instrument_dto and instrument_dto.id:
            instrument_id = instrument_dto.id

    return await events_dao.get_events(instrument_id=instrument_id, event_type=event_type, start=start, end=end)

async def insert_event(event: EventIn):
    env = Environment()
    dao = EventsDAO(env)
    return await dao.insert_event(
        event.event_type, event.symbol, event.event_time, event.reported_time, event.source, event.data
    )
