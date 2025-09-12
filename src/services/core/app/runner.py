from datetime import datetime, timedelta, time
from typing import List, Optional
import pytz


from core.platform.config.environment import Environment, EnvironmentType
from domains.market_data.services.core.market_data_manager import MarketDataManager
from domains.market_data.services.vendor_adapters.eod.daily_price_market_data_manager import DailyPriceMarketDataManager
from domains.instruments.services.secmaster.security_master import SecurityMaster
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from core.business.calendars.time_duration import TimeDuration
from domains.trading.services.universe.universe_manager import UniverseManager
from core.shared.run_context import RunContext, create_run_context
from core.shared.run_aware_logging import setup_run_aware_logging, get_run_aware_logger

from domains.trading.services.state.runner_callback import RunnerCallback

import gin

import logging
@gin.configurable
class Runner:
    def __init__(self, start_date: str, end_date: str, environment: Environment,
    universe_id: int, callbacks: List[str], base_duration: str,
    security_master=None, universe_state_manager=None, universe_manager=None, market_data_manager=None,
    run_context: Optional[RunContext] = None, enable_run_isolation: bool = True,
    # Trading hours configuration (gin configurable)
    trading_start_hour: int = 9, trading_start_minute: int = 35,   # 9:35 AM Eastern Time
    trading_end_hour: int = 16, trading_end_minute: int = 0,       # 4:00 PM Eastern Time
    timezone: str = 'America/New_York', enable_trading_hours_filter: bool = True):
        self.env = environment
        self.universe_id = universe_id
        self.enable_run_isolation = enable_run_isolation

        # Trading hours configuration
        self.trading_start_hour = trading_start_hour
        self.trading_start_minute = trading_start_minute
        self.trading_end_hour = trading_end_hour
        self.trading_end_minute = trading_end_minute
        self.timezone = timezone
        self.enable_trading_hours_filter = enable_trading_hours_filter

        from datetime import datetime, date
        print(f"[DEBUG] Runner.__init__ received start_date: {start_date!r} (type: {type(start_date)})")
        print(f"[DEBUG] Runner.__init__ received end_date: {end_date!r} (type: {type(end_date)})")
        if isinstance(start_date, str):
            self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        elif isinstance(start_date, (datetime, date)):
            self.start_date = start_date if isinstance(start_date, datetime) else datetime.combine(start_date, datetime.min.time())
        else:
            raise TypeError(f"start_date must be str or date/datetime, got {type(start_date)}")
        if isinstance(end_date, str):
            self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        elif isinstance(end_date, (datetime, date)):
            self.end_date = end_date if isinstance(end_date, datetime) else datetime.combine(end_date, datetime.min.time())
        else:
            raise TypeError(f"end_date must be str or date/datetime, got {type(end_date)}")
        print(f"[DEBUG] Runner.__init__ final self.start_date: {self.start_date!r} (type: {type(self.start_date)})")
        print(f"[DEBUG] Runner.__init__ final self.end_date: {self.end_date!r} (type: {type(self.end_date)})")

        # Initialize or create run context
        if run_context is None and enable_run_isolation:
            # Create new run context with metadata about this run
            run_metadata = {
                'start_date': self.start_date.isoformat(),
                'end_date': self.end_date.isoformat(),
                'universe_id': universe_id,
                'base_duration': base_duration,
                'environment': environment.env_type.value if environment else 'unknown',
                'callbacks': [str(cb) for cb in callbacks] if callbacks else []
            }
            self.run_context = create_run_context(metadata=run_metadata)
            logging.getLogger(__name__).info(f"Created run context: {self.run_context.run_id}")
        else:
            self.run_context = run_context

        # Set up run-aware logging if we have a run context
        if self.run_context and enable_run_isolation:
            setup_run_aware_logging(run_context=self.run_context)
            self.logger = get_run_aware_logger(__name__, self.run_context)
            self.logger.info(f"Initialized Runner with run-aware logging: {self.run_context.run_id}")
        else:
            self.logger = logging.getLogger(__name__)

        self.duration = TimeDuration(base_duration)  # expects TimeDuration
        self.callbacks: List[RunnerCallback] = self._init_callbacks(callbacks)
        self.security_master = security_master if security_master is not None else SecurityMaster(self.env)

        # Disable metadata generation during tests
        is_test_env = self.env and self.env.env_type == EnvironmentType.TEST

        # Initialize universe state manager with run context support
        if universe_state_manager is not None:
            self.universe_state_manager = universe_state_manager
        else:
            self.universe_state_manager = UniverseStateManager(
                self.env,
                write_metadata=not is_test_env,
                run_context=self.run_context if enable_run_isolation else None
            )
            if enable_run_isolation and self.run_context:
                self.logger.info(f"Using run-aware universe state manager with run_id: {self.run_context.run_id}")
            else:
                self.logger.info("Using legacy universe state manager")

        self.universe_manager = universe_manager if universe_manager is not None else UniverseManager(self.env, self.universe_id)
        self.market_data_manager = market_data_manager if market_data_manager is not None else DailyPriceMarketDataManager(self.env)

    def _init_callbacks(self, callbacks: List[str]) -> List[RunnerCallback]:
        # Expect config to contain a list of callback classes/instances
        import gin
        callback_classes = callbacks or []
        callbacks = []
        print("_init_callbacks CONFIGURABLE REGISTERED:", gin.config._CONFIG.keys())
        for cb_class in callback_classes:
            if isinstance(cb_class, RunnerCallback):
                callbacks.append(cb_class)  # Already an instance, do not call
            elif isinstance(cb_class, type) and issubclass(cb_class, RunnerCallback):
                callbacks.append(cb_class())
            elif isinstance(cb_class, str):
                # Optionally support import by string
                mod_name, class_name = cb_class.rsplit('.', 1)
                mod = __import__(mod_name, fromlist=[class_name])
                callbacks.append(getattr(mod, class_name)(self.env))
        return callbacks

    def get_environment(self) -> Environment:
        return self.env

    def get_security_master(self) -> SecurityMaster:
        return self.security_master

    def get_universe_state_manager(self) -> UniverseStateManager:
        return self.universe_state_manager

    def get_market_data_manager(self) -> MarketDataManager:
        return self.market_data_manager

    def get_universe_manager(self) -> UniverseManager:
        return self.universe_manager

    def iter_events(self):
        """
        Yields (datetime, type) tuples for each simulation event.
        Only for exchange trading days.
        'start' at the first second of the start date,
        'interval' for each interval step,
        'sod' at the first second of each trading day,
        'eod' at the last second of each trading day,
        'end' at the last second of the end date.
        """
        from core.business.calendars.exchange_calendar import ExchangeCalendar
        exchange = getattr(self.market_data_manager, 'exchange', 'NYSE')
        cal = ExchangeCalendar(exchange)
        trading_days = list(cal.all_trading_days(self.start_date.date(), self.end_date.date()))
        if not trading_days:
            logging.warning(f"[Runner.iter_events] No trading days found for {exchange} between {self.start_date} and {self.end_date}")
            return
        # START event
        start_time = self.start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        yield (start_time, "start")
        last_eod_date = None
        last_sod_date = None
        for day in trading_days:
            sod_time = datetime.combine(day, datetime.min.time())
            logging.debug(f"[Runner.iter_events] Yielding sod: {sod_time}")
            if last_sod_date != day:
                yield (sod_time, "sod")
                last_sod_date = day
            # Yield multiple interval events throughout the day based on base_duration
            # Apply trading hours filter if enabled
            current_interval_time = sod_time
            next_day = sod_time + timedelta(days=1)

            while current_interval_time < next_day:
                # Check if within trading hours before yielding interval
                if self._is_within_trading_hours(current_interval_time):
                    logging.debug(f"[Runner.iter_events] Yielding interval: {current_interval_time}")
                    print(f"[PRINT][Runner.iter_events] Yielding interval: {current_interval_time}")
                    yield (current_interval_time, "interval")
                else:
                    logging.debug(f"[Runner.iter_events] Skipping interval outside trading hours: {current_interval_time}")

                current_interval_time = self._advance_time(current_interval_time)
            # EOD event
            eod_time = sod_time.replace(hour=23, minute=59, second=59, microsecond=0)
            logging.debug(f"[Runner.iter_events] Yielding eod: {eod_time}")
            if last_eod_date != day:
                yield (eod_time, "eod")
                last_eod_date = day
        # END event
        end_time = self.end_date.replace(hour=23, minute=59, second=59, microsecond=0)
        logging.debug(f"[Runner.iter_events] Yielding end: {end_time}")
        yield (end_time, "end")
        end_time = self.end_date.replace(hour=23, minute=59, second=59, microsecond=0)
        logging.debug(f"[Runner.iter_events] Yielding end: {end_time}")
        yield (end_time, "end")

    async def run(self):
        for event_time, event_type in self.iter_events():
            print(f"[PRINT][Runner.run] Event: {event_type} at {event_time}")
            if event_type == "start":
                logging.debug(f"[Runner.run] Handling start event at {event_time}")
                for cb in self.callbacks:
                    if hasattr(cb, 'handleStart'):
                        cb.handleStart(self, event_time)
            elif event_type == "sod":
                update = self.update_for_sod(event_time)
                if hasattr(update, '__await__'):
                    await update
                for cb in self.callbacks:
                    if hasattr(cb, 'handleStartOfDay'):
                        cb.handleStartOfDay(self, event_time)
            elif event_type == "interval":
                print(f"[PRINT][Runner.run] interval event: callbacks={[(type(cb), id(cb)) for cb in self.callbacks]}")
                for cb in self.callbacks:
                    print(f"[PRINT][Runner.run] Checking callback {cb} (type={type(cb)}, id={id(cb)}) for handleInterval")
                    if hasattr(cb, 'handleInterval'):
                        print(f"[PRINT][Runner.run] Calling handleInterval on {cb} at {event_time}")
                        result = cb.handleInterval(self, event_time)
                        if hasattr(result, '__await__'):
                            await result
                    else:
                        print(f"[PRINT][Runner.run] Callback {cb} has no handleInterval")
            elif event_type == "eod":
                update = self.update_for_eod(event_time)
                if hasattr(update, '__await__'):
                    await update
                for cb in self.callbacks:
                    cb.handleEndOfDay(self, event_time)
            elif event_type == "end":
                end_update = self.update_for_eod(event_time)
                if hasattr(end_update, '__await__'):
                    await end_update
                for cb in self.callbacks:
                    if hasattr(cb, 'handleEnd'):
                        handle_end_result = cb.handleEnd(self, event_time)
                        if hasattr(handle_end_result, '__await__'):
                            await handle_end_result

    async def update_for_sod(self, current_time: datetime):
        """
        Call update_for_sod on universe_state_manager, universe_manager, and security_master if available.
        """
        import logging
        logger = logging.getLogger(__name__)
        # UniverseManager SOD
        if hasattr(self.universe_manager, 'update_for_sod'):
            logger.info(f"Runner.update_forun: Calling universe_manager.update_for_sod at {current_time}, universe_id: {self.universe_id}, date: {current_time.date()}")
            await self.universe_manager.update_for_sod(self, current_time)

        if hasattr(self.security_master, 'update_for_sod'):
            # If security_master.update_for_sod is async, await it; otherwise, call directly
            sm_update = self.security_master.update_for_sod(self, current_time)
            if hasattr(sm_update, '__await__'):
                await sm_update

        if hasattr(self.market_data_manager, 'update_for_sod'):
            logger.info(f"Runner.update_forun: Calling market_data_manager.update_for_sod at {current_time}, universe_id: {self.universe_id}, date: {current_time.date()}")
            mdm_update = self.market_data_manager.update_for_sod(self, current_time)
            if hasattr(mdm_update, '__await__'):
                await mdm_update

        logger.info(f"Runner.update_forun: Calling universe_state_manager.update_for_sod at {current_time}, universe_id: {self.universe_id}, date: {current_time.date()}")
        if hasattr(self.universe_state_manager, 'update_for_sod'):
            usm_update = self.universe_state_manager.update_for_sod(self, current_time)
            if hasattr(usm_update, '__await__'):
                await usm_update


    async def update_for_eod(self, current_time: datetime):
        """
        Call update_for_eod on universe_state_manager, universe_manager, and security_master if available.
        """
        import logging
        logger = logging.getLogger(__name__)
        import gin
        try:
            saved_dir = gin.query_parameter('runner/saved_dir')
        except ValueError:
            saved_dir = None
        logger.info(f"saved_dir:{saved_dir}")
        if hasattr(self.universe_state_manager, 'handleEnd'):
            logger.info(f"Runner.run: Calling universe_state_manager.handleEnd at {current_time}, saved_dir: {saved_dir}")
            usm_update = self.universe_state_manager.update_for_eod(self, current_time)
            if hasattr(usm_update, '__await__'):
                await usm_update
        # UniverseManager EOD
        if hasattr(self.universe_manager, 'update_for_eod'):
            logger.info(f"Runner.run: Calling universe_manager.update_for_eod at {current_time}, universe_id: {self.universe_id}, date: {current_time.date()}")
            um_update = self.universe_manager.update_for_eod(self, current_time)
            if hasattr(um_update, '__await__'):
                await um_update
        if hasattr(self.security_master, 'update_for_eod'):
            sm_update = self.security_master.update_for_eod(self, current_time)
            if hasattr(sm_update, '__await__'):
                await sm_update
        if hasattr(self.market_data_manager, 'update_for_eod'):
            logger.info(f"Runner.run: Calling market_data_manager.update_for_eod at {current_time}, universe_id: {self.universe_id}, date: {current_time.date()}")
            mdm_update = self.market_data_manager.update_for_eod(self, current_time)
            if hasattr(mdm_update, '__await__'):
                await mdm_update


    def _advance_time(self, current_time: datetime) -> datetime:
        # Use TimeDuration to advance
        minutes = self.duration.get_duration_minutes()
        if minutes:
            return current_time + timedelta(minutes=minutes)
        else:
            # Daily/weekly/monthly
            if self.duration.duration_type.name == 'DAILY':
                return current_time + timedelta(days=1)
            elif self.duration.duration_type.name == 'WEEKLY':
                return current_time + timedelta(weeks=1)
            # Add more as needed
            else:
                raise NotImplementedError(f"Unsupported duration type: {self.duration.duration_type}")

    def _is_within_trading_hours(self, dt: datetime) -> bool:
        """
        Check if a datetime is within configured trading hours.
        Handles timezone conversion to ensure accurate market hours checking.

        Args:
            dt: Datetime to check (assumed to be UTC)

        Returns:
            bool: True if within trading hours, False otherwise
        """
        if not self.enable_trading_hours_filter:
            return True

        # Convert UTC time to market timezone
        try:
            market_tz = pytz.timezone(self.timezone)
            utc_dt = dt.replace(tzinfo=pytz.UTC) if dt.tzinfo is None else dt
            local_dt = utc_dt.astimezone(market_tz)

            # Create trading start and end times for the same date
            trading_start = local_dt.replace(
                hour=self.trading_start_hour,
                minute=self.trading_start_minute,
                second=0,
                microsecond=0
            )
            trading_end = local_dt.replace(
                hour=self.trading_end_hour,
                minute=self.trading_end_minute,
                second=0,
                microsecond=0
            )

            # Check if time is within trading hours
            is_within = trading_start <= local_dt <= trading_end

            logging.debug(f"Trading hours check: {dt} UTC -> {local_dt} {self.timezone} | "
                         f"Market: {trading_start.time()}-{trading_end.time()} | Within: {is_within}")

            return is_within

        except Exception as e:
            logging.warning(f"Error checking trading hours for {dt}: {e}. Defaulting to True.")
            return True
