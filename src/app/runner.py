import pandas as pd
from datetime import datetime, timedelta
from typing import List, Callable, Optional, Any

from config.environment import Environment
from market_data.market_data_manager import MarketDataManager
from market_data.daily_price_market_data_manager import DailyPriceMarketDataManager
from secmaster.security_master import SecurityMaster
from state.universe_state_manager import UniverseStateManager
from calendars.time_duration import TimeDuration
from universe.universe_manager import UniverseManager

class RunnerCallback:
    """
    Base class for runner callbacks. Users should subclass and implement desired hooks.
    """
    def handleStart(self, runner, current_time: datetime):
        pass
    def handleStartOfDay(self, runner, current_time: datetime):
        pass
    def handleEndOfDay(self, runner, current_time: datetime):
        pass
    def handleInterval(self, runner, current_time: datetime):
        pass
    def handleEnd(self, runner, current_time: datetime):
        pass

class Runner:
    def __init__(self, start_date: str, end_date: str, environment: Environment, universe_id: int):
        self.env = environment
        self.universe_id = universe_id
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.duration = self.env.get_base_duration()  # expects TimeDuration
        self.callbacks: List[RunnerCallback] = self._init_callbacks()
        self.security_master = SecurityMaster(self.env)
        self.universe_state_manager = UniverseStateManager(self.env)
        self.universe_manager = UniverseManager(self.env)
        self.market_data_manager = DailyPriceMarketDataManager(self.env)

    def _init_callbacks(self) -> List[RunnerCallback]:
        # Expect config to contain a list of callback classes/instances
        callback_classes = self.env.get('runner', 'callbacks', [])
        callbacks = []
        for cb_class in callback_classes:
            if isinstance(cb_class, RunnerCallback):
                callbacks.append(cb_class)
            elif isinstance(cb_class, type) and issubclass(cb_class, RunnerCallback):
                callbacks.append(cb_class())
            elif isinstance(cb_class, str):
                # Optionally support import by string
                mod_name, class_name = cb_class.rsplit('.', 1)
                mod = __import__(mod_name, fromlist=[class_name])
                callbacks.append(getattr(mod, class_name)())
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
        'start' at the first second of the start date,
        'interval' for each interval step,
        'sod' at the first second of each day,
        'eod' at the last second of each day,
        'end' at the last second of the end date.
        """
        # START event
        start_time = self.start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        yield (start_time, "start")
        current_time = self.start_date
        last_eod_date = None
        last_sod_date = None
        while current_time <= self.end_date:
            # SOD event at first second of each date
            sod_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
            if last_sod_date != current_time.date():
                yield (sod_time, "sod")
                last_sod_date = current_time.date()
            # Yield interval event
            yield (current_time, "interval")
            # Check if EOD event should be yielded
            next_time = self._advance_time(current_time)
            # If next_time is a new day or past end_date, yield EOD at last second of current day
            if (next_time.date() != current_time.date()) or next_time > self.end_date:
                eod_time = current_time.replace(hour=23, minute=59, second=59, microsecond=0)
                if last_eod_date != current_time.date():
                    yield (eod_time, "eod")
                    last_eod_date = current_time.date()
            current_time = next_time
        # END event
        end_time = self.end_date.replace(hour=23, minute=59, second=59, microsecond=0)
        yield (end_time, "end")

    async def run(self):
        for event_time, event_type in self.iter_events():
            if event_type == "start":
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
                for cb in self.callbacks:
                    cb.handleInterval(self, event_time)
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
                        cb.handleEnd(self, event_time)

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
        saved_dir = None
        if hasattr(self.env, 'get'):
            saved_dir = self.env.get('runner', 'saved_dir', None)
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
