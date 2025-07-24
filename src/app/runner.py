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

    def run(self):
        current_time = self.start_date
        while current_time <= self.end_date:
            # Start of day
            if self.duration.is_daily_or_longer() or current_time.time() == datetime.min.time():
                for cb in self.callbacks:
                    cb.handleStartOfDay(self, current_time)
            # Interval
            for cb in self.callbacks:
                cb.handleInterval(self, current_time)
            # End of day
            next_time = self._advance_time(current_time)
            if (next_time.date() != current_time.date()) or next_time > self.end_date:
                for cb in self.callbacks:
                    cb.handleEndOfDay(self, current_time)
            current_time = next_time

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
