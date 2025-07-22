from typing import List, Dict
from trading.universe_interval import UniverseInterval
from trading.indicator import UniverseState

class UniverseStateBuilder:
    """
    Manages the construction of UniverseState from a sequence of UniverseInterval objects.
    """
    def __init__(self):
        self.intervals: List[UniverseInterval] = []

    def add_interval(self, interval: UniverseInterval):
        self.intervals.append(interval)

    def build(self) -> UniverseState:
        return UniverseState(intervals=self.intervals.copy())

    def reset(self):
        self.intervals.clear()
