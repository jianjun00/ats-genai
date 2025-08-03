from datetime import datetime
from typing import Any

class RunnerCallback:
    """
    Base class for runner callbacks. Users should subclass and implement desired hooks.
    """
    def handleStart(self, runner: Any, current_time: datetime):
        pass
    def handleStartOfDay(self, runner: Any, current_time: datetime):
        pass
    def handleEndOfDay(self, runner: Any, current_time: datetime):
        pass
    def handleInterval(self, runner: Any, current_time: datetime):
        pass
    def handleEnd(self, runner: Any, current_time: datetime):
        pass
