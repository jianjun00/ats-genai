from .instrument_interval_dao import InstrumentIntervalDAO
from .instrument_indicator_interval_dao import InstrumentIndicatorIntervalDAO
from .factor_interval_dao import FactorIntervalDAO
import asyncpg
from typing import Optional, Dict, TYPE_CHECKING
from core.platform.config.environment import Environment
from datetime import datetime

# Use TYPE_CHECKING to avoid circular import
if TYPE_CHECKING:
    from domains.trading.services.state.universe_state import UniverseStateInterval

class UniverseStateIntervalDAO:
    # ... existing methods ...

    async def async_load_row_to_interval(self, row: dict) -> "UniverseStateInterval":
        """
        Given a DB row for universe_state_interval, load all nested instrument_intervals,
        instrument_indicator_intervals, and factor_intervals from the DB and construct a
        fully populated UniverseStateInterval.
        """
        from core.calendars.time_duration import TimeDuration
        from domains.trading.services.state.universe_state import UniverseStateInterval
        from domains.trading.services.state.instrument_interval import InstrumentInterval
        from domains.trading.services.state.indicator_interval import IndicatorInterval
        from domains.trading.services.state.factor_interval import FactorInterval
        duration = TimeDuration(row['duration'])
        start_date_time = row['start_date_time']
        end_date_time = row['end_date_time']
        if isinstance(start_date_time, str):
            start_date_time = datetime.fromisoformat(start_date_time)
        if isinstance(end_date_time, str):
            end_date_time = datetime.fromisoformat(end_date_time)
        # --- Load instrument intervals ---
        instrument_interval_dao = InstrumentIntervalDAO(self.env)
        interval_pk = row.get('id', row.get('interval_id'))
        instrument_rows = await instrument_interval_dao.list(universe_state_interval_id=interval_pk)
        instrument_intervals = {}
        for irow in instrument_rows:
            ii = InstrumentInterval(
                instrument_id=irow['instrument_id'],
                start_date_time=start_date_time,
                end_date_time=end_date_time,
                open=irow['open'],
                high=irow['high'],
                low=irow['low'],
                close=irow['close'],
                traded_volume=irow['traded_volume'],
                traded_dollar=irow['traded_dollar'],
                status=irow.get('status'),
                market_cap=irow.get('market_cap')
            )
            instrument_intervals[irow['instrument_id']] = ii
        # --- Load instrument indicator intervals ---
        indicator_interval_dao = InstrumentIndicatorIntervalDAO(self.env)
        instrument_indicator_intervals: Dict[str, Dict[int, IndicatorInterval]] = {}
        for irow in instrument_rows:
            indicator_rows = await indicator_interval_dao.list(instrument_interval_id=irow['id'])
            for ind_row in indicator_rows:
                ind_type = ind_row['indicator_name']
                if ind_type not in instrument_indicator_intervals:
                    instrument_indicator_intervals[ind_type] = {}
                indicators = {ind_row['indicator_name']: {
                    'value': ind_row['indicator_value'],
                    'status': ind_row.get('indicator_status', 'ok'),
                    'update_at': None
                }}
                indicator_interval = IndicatorInterval(
                    instrument_id=irow['instrument_id'],
                    start_date_time=start_date_time,
                    end_date_time=end_date_time,
                    indicators=indicators
                )
                instrument_indicator_intervals[ind_type][irow['instrument_id']] = indicator_interval
        # --- Load factor intervals ---
        factor_interval_dao = FactorIntervalDAO(self.env)
        factor_rows = await factor_interval_dao.list(universe_state_interval_id=interval_pk)
        factor_intervals = []
        for frow in factor_rows:
            factor_intervals.append(FactorInterval(
                start_date_time=start_date_time,
                end_date_time=end_date_time,
                instrument_intervals={}  # Extend if schema supports nested instrument intervals
            ))
        # Import at runtime to avoid circular dependency
        from domains.trading.services.state.universe_state import UniverseStateInterval

        return UniverseStateInterval(
            universe_id=row.get('universe_id'),
            duration=duration,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            factor_intervals=factor_intervals,
            instrument_intervals=instrument_intervals,
            instrument_indicator_intervals=instrument_indicator_intervals
        )
    def _row_to_interval(self, row: dict):
        from core.calendars.time_duration import TimeDuration
        from domains.trading.services.state.universe_state import UniverseStateInterval
        from datetime import datetime
        # Parse duration
        duration = TimeDuration(row['duration'])
        # Parse datetimes
        start_date_time = row['start_date_time']
        end_date_time = row['end_date_time']
        if isinstance(start_date_time, str):
            start_date_time = datetime.fromisoformat(start_date_time)
        if isinstance(end_date_time, str):
            end_date_time = datetime.fromisoformat(end_date_time)
        # TODO: Load factor_intervals, instrument_intervals, instrument_indicator_intervals from related tables
        # For now, use empty defaults so the object can be constructed and code can proceed
        return UniverseStateInterval(
            universe_id=row.get('universe_id'),
            duration=duration,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            factor_intervals=[],
            instrument_intervals={},
            instrument_indicator_intervals={}
        )

    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()

    async def create(self, universe_id: int, duration: str, start_date_time, end_date_time, run_id: str) -> int:
        """Insert a new UniverseStateInterval record. Returns the new id (or interval_id)."""
        conn = await asyncpg.connect(self.db_url)
        try:
            try:
                row = await conn.fetchrow(
                    f"""
                    INSERT INTO {self.env.get_table_name('universe_state_interval')} (
                        universe_id, duration, start_date_time, end_date_time, run_id
                    ) VALUES ($1, $2, $3, $4, $5)
                    RETURNING id
                    """,
                    universe_id, duration, start_date_time, end_date_time, run_id
                )
                return row['id']
            except asyncpg.UndefinedColumnError:
                row = await conn.fetchrow(
                    f"""
                    INSERT INTO {self.env.get_table_name('universe_state_interval')} (
                        universe_id, duration, start_date_time, end_date_time, run_id
                    ) VALUES ($1, $2, $3, $4, $5)
                    RETURNING interval_id
                    """,
                    universe_id, duration, start_date_time, end_date_time, run_id
                )
                return row['interval_id']
        finally:
            await conn.close()

    async def get(self, id: int) -> Optional[dict]:
        """Fetch a UniverseStateInterval by id, supporting schemas with 'id' or 'interval_id'."""
        conn = await asyncpg.connect(self.db_url)
        try:
            try:
                row = await conn.fetchrow(
                    f"SELECT * FROM {self.env.get_table_name('universe_state_interval')} WHERE id = $1",
                    id
                )
            except asyncpg.UndefinedColumnError:
                row = await conn.fetchrow(
                    f"SELECT * FROM {self.env.get_table_name('universe_state_interval')} WHERE interval_id = $1",
                    id
                )
            if not row:
                return None
            d = dict(row)
            if 'id' not in d and 'interval_id' in d:
                d['id'] = d['interval_id']
            return d
        finally:
            await conn.close()

    async def list(self, universe_id: int = None, start_date_time: str = None, end_date_time: str = None) -> list:
        """List UniverseStateIntervals, optionally filter by universe_id, start_date_time, and end_date_time, fully populating nested intervals."""
        import asyncio
        conn = await asyncpg.connect(self.db_url)
        try:
            query = f"SELECT * FROM {self.env.get_table_name('universe_state_interval')}"
            filters = []
            params = []
            if universe_id is not None:
                filters.append("universe_id = $%d" % (len(params) + 1))
                params.append(universe_id)
            if start_date_time is not None:
                filters.append("start_date_time >= $%d" % (len(params) + 1))
                params.append(start_date_time)
            if end_date_time is not None:
                filters.append("end_date_time <= $%d" % (len(params) + 1))
                params.append(end_date_time)
            if filters:
                query += " WHERE " + " AND ".join(filters)
            rows = await conn.fetch(query, *params)
            # Normalize PK key for downstream usage
            norm_rows = []
            for row in rows:
                d = dict(row)
                if 'id' not in d and 'interval_id' in d:
                    d['id'] = d['interval_id']
                norm_rows.append(d)
            # Use async_load_row_to_interval for each row
            tasks = [self.async_load_row_to_interval(d) for d in norm_rows]
            return await asyncio.gather(*tasks)
        finally:
            await conn.close()

    async def delete(self, id: int) -> bool:
        """Delete a UniverseStateInterval by id, supporting 'id' or 'interval_id'."""
        conn = await asyncpg.connect(self.db_url)
        try:
            try:
                result = await conn.execute(
                    f"DELETE FROM {self.env.get_table_name('universe_state_interval')} WHERE id = $1",
                    id
                )
            except asyncpg.UndefinedColumnError:
                result = await conn.execute(
                    f"DELETE FROM {self.env.get_table_name('universe_state_interval')} WHERE interval_id = $1",
                    id
                )
            return result.startswith("DELETE")
        finally:
            await conn.close()
