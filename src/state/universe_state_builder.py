"""
UniverseStateBuilder - Business logic for building and transforming universe state.

This module handles the business logic layer for universe state construction,
including data validation, transformation rules, corporate actions, and
integration with multiple data sources.
"""

import pandas as pd
import gin
from state.runner_callback import RunnerCallback
import asyncpg
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime, date
from dataclasses import dataclass
from enum import Enum
import numpy as np
from config.environment import Environment
from calendars.time_duration import TimeDuration
from state.instrument_interval import InstrumentInterval
from state.universe_interval import UniverseInterval
from dao.daily_market_cap_dao import DailyMarketCapDAO

from signals.indicator_builder import IndicatorBuilder
from signals.indicator_config import IndicatorConfig

@gin.configurable
class UniverseStateBuilder(RunnerCallback):
    def handleStartOfDay(self, runner, current_time):
        self.logger.info(f"UniverseStateBuilder.handleStartOfDay called at {current_time}")
        pass

    def handleEndOfDay(self, runner, current_time):
        self.logger.info(f"UniverseStateBuilder.handleEndOfDay called at {current_time}")
        pass

    async def handleInterval(self, runner, current_time):
        print(f"[DEBUG][handleInterval] handleInterval CALLED at {current_time}")
        """
        Build UniverseState for base_duration and each target duration, passing each to UniverseStateManager.
        Maintains rolling window cache of InstrumentIntervals and builds indicators via IndicatorBuilder.
        """
        from state.universe_state import UniverseState
        self.logger.info(f"UniverseStateBuilder.handleInterval called at {current_time}")
        durations = self.target_durations
        if not durations:
            self.logger.error("No target durations configured.")
            return
        instrument_ids = runner.universe_manager.instrument_ids
        # --- 1. Always build and update rolling cache for base_duration (assume durations[0] is base) ---
        base_duration = self.base_duration
        base_end_time = base_duration.get_end_time(current_time)
        print(f"[DEBUG][handleInterval] Calling get_ohlc_batch with instrument_ids: {instrument_ids}, current_time: {current_time}, base_end_time: {base_end_time}")
        ohlc_batch = runner.market_data_manager.get_ohlc_batch(instrument_ids, current_time, base_end_time)
        print(f"[DEBUG][handleInterval] ohlc_batch result: {ohlc_batch}")
        # Fetch market_cap for all instruments for current_time
        rows = await self.market_cap_dao.list_market_caps_for_date(current_time.date())
        market_caps = {row['instrument_id']: row['market_cap'] for row in rows}
        for inst_id in instrument_ids:
            print(f"[DEBUG][handleInterval] Checking ohlc for inst_id: {inst_id}")
            ohlc = ohlc_batch.get(inst_id)
            print(f"[DEBUG][handleInterval] ohlc for inst_id {inst_id}: {ohlc}")
            if ohlc:
                interval = InstrumentInterval(
                    instrument_id=inst_id,
                    start_date_time=current_time,
                    end_date_time=base_end_time,
                    open=ohlc.get('open', 0.0),
                    high=ohlc.get('high', 0.0),
                    low=ohlc.get('low', 0.0),
                    close=ohlc.get('close', 0.0),
                    traded_volume=ohlc.get('volume', 0.0),
                    traded_dollar=ohlc.get('close', 0.0) * ohlc.get('volume', 0.0),
                    status='ok',
                    market_cap=market_caps.get(inst_id)
                )
                if inst_id not in self.instrument_history:
                    self.instrument_history[inst_id] = []
                self.instrument_history[inst_id].append(interval)
                if len(self.instrument_history[inst_id]) > self.rolling_window:
                    self.instrument_history[inst_id] = self.instrument_history[inst_id][-self.rolling_window:]
            else:
                self.logger.warning(f"No ohlc data for instrument_id: {inst_id}")
        # --- 2. For each duration, build UniverseInterval, indicator_intervals, UniverseState, emit ---
        duration_to_state = {}
        for duration in self.target_durations:
            d_end_time = duration.get_end_time(current_time)
            interval_map = {}
            from calendars.time_duration import TimeDuration
            base_duration = self.base_duration
            for inst_id in instrument_ids:
                history = self.instrument_history.get(inst_id, [])
                if duration == base_duration:
                    interval = history[-1] if history else None
                else:
                    n = None
                    base_minutes = base_duration.get_duration_minutes()
                    target_minutes = duration.get_duration_minutes()
                    if base_minutes and target_minutes and target_minutes % base_minutes == 0:
                        n = target_minutes // base_minutes
                    if n is not None and len(history) >= n:
                        to_agg = history[-n:]
                        interval = duration.aggregate_intervals(to_agg)
                    else:
                        interval = history[-1] if history else None
                if interval:
                    interval_map[inst_id] = interval
                else:
                    self.logger.warning(f"No interval data for instrument_id: {inst_id}")
            universe_interval = UniverseInterval(
                start_date_time=current_time,
                end_date_time=d_end_time,
                instrument_intervals=interval_map
            )
            indicator_intervals = {}
            indicator_intervals['default'] = self.indicator_builder.build_indicator_intervals(
                {inst_id: self.instrument_history.get(inst_id, []) for inst_id in instrument_ids},
                start_date_time=current_time,
                end_date_time=d_end_time
            )
            universe_state = UniverseState(
                universe_interval=universe_interval,
                instrument_intervals=interval_map,
                indicator_intervals=indicator_intervals
            )
            duration_to_state[duration] = universe_state
        if hasattr(runner, 'universe_state_manager'):
            print(f"[BUILDER] id(runner.universe_state_manager): {id(runner.universe_state_manager)}")
            runner.universe_state_manager.addUniverseState(duration_to_state, current_time)
        else:
            self.logger.fatal("runner.universe_state_manager not available; skipping addUniverseState.")

    """
    Builds universe state from multiple data sources with business logic,
    validation, and transformation rules.
    
    Handles data collection, validation, corporate actions, and derived calculations.
    """

    def __init__(self, env: Environment, base_duration: str, target_durations: str):
        # Inject DailyMarketCapDAO for market_cap sourcing
        self.market_cap_dao = DailyMarketCapDAO(env)
        """
        Initialize UniverseStateBuilder.
        Args:
            env: Environment instance (uses global if None)
            base_duration: str (e.g. '5m'), overrides Gin config if provided
            target_durations: comma-separated str (e.g. '5m,15m,60m'), overrides Gin config if provided
        """
        self.env = env
        self.logger = logging.getLogger(__name__)
        # Rolling cache: instrument_id -> list of InstrumentInterval
        self.instrument_history: Dict[int, List[InstrumentInterval]] = {}
        # Load indicator config from env
        indicator_config = getattr(self.env, 'get_indicator_config', lambda: IndicatorConfig.empty_config())()
        self.indicator_builder = IndicatorBuilder(indicator_config)
        # Rolling window size (max history to keep, can be set by env or default)
        self.rolling_window = getattr(self.env, 'indicator_rolling_window', 20)

        # Durations
        from calendars.time_duration import TimeDuration
        base_duration_str = base_duration
        self.base_duration = TimeDuration(base_duration_str)
        target_durations_str = target_durations
        self.target_durations = [TimeDuration(d.strip()) for d in target_durations_str.split(',')]

        # Default business logic parameters (from test expectations)
        self.min_market_cap = 100_000_000
        self.min_avg_volume = 100_000
        self.max_universe_size = 3000
        self.data_source_priorities = {
            'polygon': 1,
            'tiingo': 2,
            'quandl': 3
        }

    def validate_universe_state(self, df):
        required_cols = {'symbol', 'market_cap', 'avg_volume', 'sector', 'exchange', 'is_active', 'as_of_date'}
        if not isinstance(df, pd.DataFrame) or df.empty:
            return False
        if not required_cols.issubset(df.columns):
            return False
        # Check for duplicate symbols
        if df['symbol'].duplicated().any():
            return False
        return True


    def build_multi_duration_intervals(self, start_time: 'datetime', runner: 'Runner') -> dict:
        """
        Build intervals for all target durations for the current universe at start_time.
        Returns a dict mapping duration string to UniverseInterval.
        """
        intervals = {}
        self.logger.info(f"Building intervals for {len(self.target_durations)} durations at {start_time}")
        for duration in self.target_durations:
            end_time = duration.get_end_time(start_time)
            instrument_intervals = {}
            instrument_ids = runner.universe_manager.instrument_ids
            ohlc_batch = runner.market_data_manager.get_ohlc_batch(instrument_ids, start_time, end_time)
            self.logger.info(f"Built ohlc_batch for {ohlc_batch} instruments at {start_time}, instrument_ids: {instrument_ids}")
            for inst_id in instrument_ids:
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
            self.logger.info('Built interval for %s at %s, instrument_ids: %s', duration.get_duration_string(), start_time, instrument_ids)
            intervals[duration.get_duration_string()] = UniverseInterval(
                start_date_time=start_time,
                end_date_time=end_time,
                instrument_intervals=instrument_intervals
            )
        return intervals
