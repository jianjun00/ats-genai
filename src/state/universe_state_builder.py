"""
UniverseStateIntervalBuilder - Business logic for building and transforming universe state.

This module handles the business logic layer for universe state construction,
including data validation, transformation rules, corporate actions, and
integration with multiple data sources.
"""

import pandas as pd
import gin
try:
    from state.runner_callback import RunnerCallback
except Exception:
    # Fallback minimal base when runner_callback is not available in test context
    class RunnerCallback:
        pass
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
from .factor_interval import FactorInterval
from dao.daily_market_cap_dao import DailyMarketCapDAO

from signals.indicator_builder import IndicatorBuilder
from signals.indicator_config import IndicatorConfig

@gin.configurable
class UniverseStateIntervalBuilder(RunnerCallback):
    def handleStartOfDay(self, runner, current_time):
        self.logger.debug(f"UniverseStateIntervalBuilder.handleStartOfDay called at {current_time}")
        pass

    def handleEndOfDay(self, runner, current_time):
        self.logger.debug(f"UniverseStateIntervalBuilder.handleEndOfDay called at {current_time}")
        pass

    async def handleInterval(self, runner, current_time):
        self.logger.debug(f"[handleInterval] handleInterval CALLED at {current_time}")
        """
        Build UniverseStateInterval for base_duration and each target duration, passing each to UniverseStateIntervalManager.
        Maintains rolling window cache of InstrumentIntervals and builds indicators via IndicatorBuilder.
        """
        from state.universe_state import UniverseStateInterval
        self.logger.debug(f"UniverseStateIntervalBuilder.handleInterval called at {current_time}")
        durations = self.target_durations
        if not durations:
            self.logger.error("No target durations configured.")
            return
        instrument_ids = runner.universe_manager.instrument_ids
        # --- 1. Always build and update rolling cache for base_duration (assume durations[0] is base) ---
        base_duration = self.base_duration
        base_end_time = base_duration.get_end_time(current_time)
        print(f"[DEBUG][handleInterval] Calling get_ohlc_batch with instrument_ids: {instrument_ids}, current_time: {current_time}, base_end_time: {base_end_time}")
        # Await async market data fetch
        ohlc_batch = await runner.market_data_manager.get_ohlc_batch(instrument_ids, current_time, base_end_time)
        print(f"[DEBUG][handleInterval] ohlc_batch keys: {list(ohlc_batch.keys()) if hasattr(ohlc_batch, 'keys') else type(ohlc_batch)}")
        # Fetch market_cap for all instruments for current_time
        rows = await self.market_cap_dao.list_market_caps_for_date(current_time.date())
        market_caps = {row['instrument_id']: row['market_cap'] for row in rows}
        for inst_id in instrument_ids:
            print(f"[DEBUG][handleInterval] Checking ohlc for inst_id: {inst_id}")
            ohlc = ohlc_batch.get(inst_id)
            print(f"[DEBUG][handleInterval] ohlc for inst_id {inst_id}: {ohlc}")
            if ohlc:
                # Use None for missing OHLC fields; mark interval as 'missing' if all are None
                open_ = ohlc.get('open') if ohlc.get('open') is not None else None
                high_ = ohlc.get('high') if ohlc.get('high') is not None else None
                low_ = ohlc.get('low') if ohlc.get('low') is not None else None
                close_ = ohlc.get('close') if ohlc.get('close') is not None else None
                volume_ = ohlc.get('volume') if ohlc.get('volume') is not None else None
                all_none = all(x is None for x in [open_, high_, low_, close_, volume_])
                status = 'missing' if all_none else 'ok'
                traded_dollar = (close_ * volume_) if (close_ is not None and volume_ is not None) else None
                interval = InstrumentInterval(
                    instrument_id=inst_id,
                    start_date_time=current_time,
                    end_date_time=base_end_time,
                    open=open_,
                    high=high_,
                    low=low_,
                    close=close_,
                    traded_volume=volume_,
                    traded_dollar=traded_dollar,
                    status=status,
                    market_cap=market_caps.get(inst_id)
                )
                import math
                ohlc_fields = ['open', 'high', 'low', 'close']
                ohlc_vals = [getattr(interval, f) for f in ohlc_fields]
                nan_fields = [f for f, v in zip(ohlc_fields, ohlc_vals) if v is None or (isinstance(v, float) and math.isnan(v))]
                self.logger.debug(f"[INTERVAL CONSTRUCTED] instrument_id={inst_id}, start={interval.start_date_time}, end={interval.end_date_time}, open={interval.open}, high={interval.high}, low={interval.low}, close={interval.close}, traded_volume={interval.traded_volume}, traded_dollar={interval.traded_dollar}, status={interval.status}, market_cap={interval.market_cap}")
                if nan_fields:
                    self.logger.warning(f"[INTERVAL NAN/None] instrument_id={inst_id}, fields_with_nan_or_none={nan_fields}, values={[getattr(interval, f) for f in nan_fields]}, interval={interval}")
                if inst_id not in self.instrument_history:
                    self.instrument_history[inst_id] = []
                self.instrument_history[inst_id].append(interval)
                if len(self.instrument_history[inst_id]) > self.rolling_window:
                    self.instrument_history[inst_id] = self.instrument_history[inst_id][-self.rolling_window:]
            else:
                self.logger.warning(f"No ohlc data for instrument_id: {inst_id}")
        # --- 2. For each duration, build FactorInterval, instrument_indicator_intervals, UniverseStateInterval, emit ---
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
            universe_intervals = FactorInterval(
                start_date_time=current_time,
                end_date_time=d_end_time,
                instrument_intervals=interval_map
            )
            instrument_indicator_intervals = {}
            instrument_indicator_intervals['default'] = self.indicator_builder.build_indicator_intervals(
                {inst_id: self.instrument_history.get(inst_id, []) for inst_id in instrument_ids},
                start_date_time=current_time,
                end_date_time=d_end_time
            )
            universe_state = UniverseStateInterval(
                universe_id=runner.universe_id,
                duration=duration,
                start_date_time=current_time,
                end_date_time=d_end_time,
                factor_intervals=[universe_intervals],
                instrument_intervals=interval_map,
                instrument_indicator_intervals=instrument_indicator_intervals
            )
            # Optional: augment with forecasts via forecast callback
            if hasattr(self, 'forecast_callback') and self.forecast_callback is not None:
                try:
                    self.forecast_callback.augment_universe_state(
                        universe_state=universe_state,
                        instrument_ids=instrument_ids,
                        instrument_history=self.instrument_history,
                        current_time=current_time
                    )
                except Exception as e:
                    self.logger.error(f"Forecast augmentation failed: {e}")
            duration_to_state[duration] = universe_state
        # --- Debug: Check for empty intervals before saving ---
        all_empty = True
        for dur, state in duration_to_state.items():
            self.logger.debug(f"[handleInterval] duration={dur}, state type={type(state)}")
            assert hasattr(state, 'to_dataframe'), (
                f"[handleInterval] duration={dur} value type={type(state)} does not have .to_dataframe(). Value: {state}")
            df = state.to_dataframe()
            self.logger.debug(f"[DEBUG] UniverseStateIntervalBuilder: duration={dur}, DataFrame shape={df.shape}")
            if not df.empty:
                all_empty = False
            else:
                self.logger.warning(f"[DEBUG] duration={dur} produced empty DataFrame at {current_time}")
        if all_empty:
            self.logger.warning(f"[DEBUG] All intervals produced empty DataFrames at {current_time}. Universe state will not be saved.")
        if hasattr(runner, 'universe_state_manager'):
            print(f"[BUILDER] id(runner.universe_state_manager): {id(runner.universe_state_manager)}")
            await runner.universe_state_manager.addUniverseState(duration_to_state, current_time)
        else:
            self.logger.fatal("runner.universe_state_manager not available; skipping addUniverseStateInterval.")

    def handleIntervalSync(self, runner, current_time):
        """Synchronous wrapper to run handleInterval for tests that call without await."""
        import asyncio
        return asyncio.run(self.handleInterval(runner, current_time))

    """
    Builds universe state from multiple data sources with business logic,
    validation, and transformation rules.
    
    Handles data collection, validation, corporate actions, and derived calculations.
    """

    def __init__(self, env: Environment, base_duration: str, target_durations: str, forecast_callback=None):
        # Inject DailyMarketCapDAO for market_cap sourcing
        self.market_cap_dao = DailyMarketCapDAO(env)
        """
        Initialize UniverseStateIntervalBuilder.
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
        # Optional modeling callback for forecasts
        self.forecast_callback = forecast_callback

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
        Returns a dict mapping duration string to FactorInterval.
        """
        intervals = {}
        self.logger.debug(f"Building intervals for {len(self.target_durations)} durations at {start_time}")
        for duration in self.target_durations:
            end_time = duration.get_end_time(start_time)
            instrument_intervals = {}
            instrument_ids = runner.universe_manager.instrument_ids
            ohlc_batch = runner.market_data_manager.get_ohlc_batch(instrument_ids, start_time, end_time)
            self.logger.debug(f"Built ohlc_batch for {ohlc_batch} instruments at {start_time}, instrument_ids: {instrument_ids}")
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
            self.logger.debug('Built interval for %s at %s, instrument_ids: %s', duration.get_duration_string(), start_time, instrument_ids)
            intervals[duration.get_duration_string()] = FactorInterval(
                start_date_time=start_time,
                end_date_time=end_time,
                instrument_intervals=instrument_intervals
            )
        return intervals
