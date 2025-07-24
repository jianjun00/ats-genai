import pandas as pd
from datetime import datetime, timedelta
from typing import List, Callable, Optional, Any

from config.environment import Environment
from secmaster.security_master import SecurityMaster
from state.universe_state_manager import UniverseStateManager
from trading.time_duration import TimeDuration

class RunnerCallback:
    """
    Base class for runner callbacks. Users should subclass and implement desired hooks.
    """
    def handleStartOfDay(self, runner, current_time: datetime):
        pass
    def handleEndOfDay(self, runner, current_time: datetime):
        pass
    def handleInterval(self, runner, current_time: datetime):
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

    def iter_events(self):
        """
        Yields (datetime, type) tuples for each simulation event.
        'interval' for each interval step, 'eod' once per day at the last second.
        """
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

    def run(self):
        for event_time, event_type in self.iter_events():
            if event_type == "sod":
                self.update_for_sod(event_time)
                for cb in self.callbacks:
                    if hasattr(cb, 'handleStartOfDay'):
                        cb.handleStartOfDay(self, event_time)
            elif event_type == "interval":
                for cb in self.callbacks:
                    cb.handleInterval(self, event_time)
            elif event_type == "eod":
                self.update_for_eod(event_time)
                for cb in self.callbacks:
                    cb.handleEndOfDay(self, event_time)

    def update_for_sod(self, current_time: datetime):
        """
        Call update_for_sod on universe_state_manager, universe_manager, and security_manager if available.
        """
        # UniverseStateManager SOD (e.g., initialize state)
        if hasattr(self.universe_state_manager, 'update_for_sod'):
            self.universe_state_manager.update_for_sod(current_time)
        # UniverseManager SOD
        if hasattr(self, 'universe_manager') and self.universe_manager:
            import asyncio
            if hasattr(self.universe_manager, 'update_for_sod'):
                try:
                    asyncio.run(self.universe_manager.update_for_sod(self.universe_id, current_time.date()))
                except Exception as e:
                    print(f"UniverseManager.update_for_sod failed: {e}")
        # SecurityManager SOD (if implemented)
        if hasattr(self, 'security_manager') and self.security_manager:
            if hasattr(self.security_manager, 'update_for_sod'):
                self.security_manager.update_for_sod(current_time)

    def update_for_eod(self, current_time: datetime):
        """
        Call update_for_eod on universe_state_manager, universe_manager, and security_manager if available.
        """
        # UniverseStateManager EOD (e.g., flush cache or finalize state)
        if hasattr(self.universe_state_manager, 'update_for_eod'):
            self.universe_state_manager.update_for_eod(current_time)
        # UniverseManager EOD
        if hasattr(self, 'universe_manager') and self.universe_manager:
            import asyncio
            if hasattr(self.universe_manager, 'update_for_eod'):
                try:
                    asyncio.run(self.universe_manager.update_for_eod(self.universe_id, current_time.date()))
                except Exception as e:
                    print(f"UniverseManager.update_for_eod failed: {e}")
        # SecurityManager EOD (if implemented)
        if hasattr(self, 'security_manager') and self.security_manager:
            if hasattr(self.security_manager, 'update_for_eod'):
                self.security_manager.update_for_eod(current_time)

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
