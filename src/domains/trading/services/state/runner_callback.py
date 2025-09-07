from datetime import datetime
from typing import Any

class RunnerCallback:
    """
    Event-Driven Processing Framework - Base class for time-driven and event-driven processing.

    ARCHITECTURE ROLE: Provides event-driven hooks for processing universe state changes.

    RESPONSIBILITIES:
    - Define standard lifecycle events for time-series data processing
    - Provide extensible hook pattern for pluggable processing components
    - Enable time-driven execution (intervals, SOD, EOD)
    - Support event-driven architecture with standardized callback interfaces
    - Allow multiple callbacks to process the same events independently

    CALLBACK EVENTS:
    - handleStart: System initialization and setup
    - handleStartOfDay: Daily initialization, file setup, resource preparation
    - handleInterval: Core processing at each time interval (main data flow)
    - handleEndOfDay: Daily cleanup, file closure, reporting, validation
    - handleEnd: System shutdown and cleanup

    DOES NOT:
    - Execute business logic (specific callback implementations responsibility)
    - Manage state persistence (specific callbacks use UniverseStateManager)
    - Fetch or transform data (callbacks use MarketDataManager and UniverseStateBuilder)

    INTERACTIONS:
    - Extended BY: UniverseStateBuilder, TrainingDataCallback, ForecastCallback, etc.
    - Coordinated BY: Runner framework for time-based execution
    - Provides: Event hooks for pluggable processing modules
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
