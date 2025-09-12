from core.platform.config.environment import Environment
from core.dao.analytics.events_dao import EventsDAO
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

async def create_analytics_event(env: Environment, event_type: str, symbol: str, data: dict):
    """Create analytics event with instrument resolution via service"""
    from shared.utils.database import Database
    import json
    from datetime import datetime
    
    # Resolve instrument_id using InstrumentService
    instrument_id = None
    try:
        instrument_service = await get_instrument_service(env)
        instrument_dto = await instrument_service.get_instrument_by_symbol(symbol)
        if instrument_dto and instrument_dto.id:
            instrument_id = instrument_dto.id
    except Exception:
        # Graceful fallback - log event without instrument_id
        instrument_id = None
    
    # Insert event into database
    pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)
    async with pool.acquire() as conn:
        await conn.execute(f"""
            INSERT INTO {env.get_table_name('analytics_events')} 
            (instrument_id, event_type, symbol, event_time, data) 
            VALUES ($1, $2, $3, $4, $5)
        """, instrument_id, event_type, symbol, datetime.now(), json.dumps(data))
    
    await pool.close()
