"""
UniverseStateBuilder - Business logic for building and transforming universe state.

This module handles the business logic layer for universe state construction,
including data validation, transformation rules, corporate actions, and
integration with multiple data sources.
"""

import pandas as pd
import asyncpg
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime, date
from dataclasses import dataclass
from enum import Enum
import numpy as np
from config.environment import Environment, get_environment
from trading.time_duration import TimeDuration
from state.instrument_interval import InstrumentInterval
from state.universe_interval import UniverseInterval
from app.runner import RunnerCallback

class UniverseStateBuilder(RunnerCallback):
    def handleStartOfDay(self, runner, current_time):
        self.logger.info(f"UniverseStateBuilder.handleStartOfDay called at {current_time}")
        pass

    def handleEndOfDay(self, runner, current_time):
        self.logger.info(f"UniverseStateBuilder.handleEndOfDay called at {current_time}")
        pass

    def handleInterval(self, runner, current_time):
        """
        Build multi-duration intervals for the current time using runner's market_data_manager, security_master, and universe_state_manager.
        After building, add the intervals to the universe_state_manager.
        """
        self.logger.info(f"UniverseStateBuilder.handleInterval called at {current_time}")
        # Use runner's managers
        # Build intervals
        intervals = self.build_multi_duration_intervals(current_time, runner)
        self.logger.info(f"Built intervals for {len(intervals)} durations at {current_time}")
        # Add to universe_state_manager
        runner.universe_state_manager.addIntervals(intervals, current_time)


    """
    Builds universe state from multiple data sources with business logic,
    validation, and transformation rules.
    
    Handles data collection, validation, corporate actions, and derived calculations.
    """
    
    def __init__(self, 
                 env: Optional['Environment'] = None):
        """
        Initialize UniverseStateBuilder.
        Args:
            env: Environment instance (uses global if None)

        """
        self.env = env or get_environment()
        self.logger = logging.getLogger(__name__)


    def build_multi_duration_intervals(self, start_time: 'datetime', runner: 'Runner') -> dict:
        """
        Build intervals for all target durations for the current universe at start_time.
        Returns a dict mapping duration string to UniverseInterval.
        """
        intervals = {}
        for duration in self.env.get_target_durations():
            end_time = duration.get_end_time(start_time)
            instrument_intervals = {}
            ohlc_batch = runner.market_data_manager.get_ohlc_batch(runner.universe_manager.universe.instrument_ids, start_time, end_time)
            for inst_id in runner.universe.instrument_ids:
                ohlc = ohlc_batch.get(inst_id)
                if ohlc:
                    instrument_intervals[inst_id] = InstrumentInterval(
                        instrument_id=inst_id,
                        start_date_time=start_time,
                        end_date_time=end_time,
                        open=ohlc.get('open', 0.0),
                        high=ohlc.get('high', 0.0),
                        low=ohlc.get('low', 0.0),
                        close=ohlc.get('close', 0.0),
                        traded_volume=ohlc.get('volume', 0.0),
                        traded_dollar=ohlc.get('close', 0.0) * ohlc.get('volume', 0.0),
                        status='ok'
                    )
            self.logger.info('Built interval for %s at %s, instrument_ids: %s', duration.get_duration_string(), start_time, self.universe.instrument_ids)
            intervals[duration.get_duration_string()] = UniverseInterval(
                start_date_time=start_time,
                end_date_time=end_time,
                instrument_intervals=instrument_intervals
            )
        return intervals

