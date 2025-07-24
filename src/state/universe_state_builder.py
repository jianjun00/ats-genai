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



class UniverseAction(Enum):
    """Types of universe membership actions."""
    ADD = "add"
    REMOVE = "remove"
    UPDATE = "update"


class CorporateActionType(Enum):
    """Types of corporate actions."""
    SPLIT = "split"
    DIVIDEND = "dividend"
    MERGER = "merger"
    SPINOFF = "spinoff"
    DELISTING = "delisting"


@dataclass
class UniverseMembershipChange:
    """Represents a change in universe membership."""
    symbol: str
    action: UniverseAction
    effective_date: str
    reason: str
    metadata: Dict[str, Any] = None


@dataclass
class CorporateAction:
    """Represents a corporate action affecting universe state."""
    symbol: str
    action_type: CorporateActionType
    effective_date: str
    ratio: Optional[float] = None
    amount: Optional[float] = None
    new_symbol: Optional[str] = None
    metadata: Dict[str, Any] = None


class UniverseStateBuilder:
    """
    Builds universe state from multiple data sources with business logic,
    validation, and transformation rules.
    
    Handles data collection, validation, corporate actions, and derived calculations.
    """
    
    def __init__(self, 
                 universe: 'Universe',
                 state_manager: 'UniverseStateManager' = None,
                 env: Optional['Environment'] = None,
                 base_duration: Optional['TimeDuration'] = None,
                 target_durations: Optional[List['TimeDuration']] = None,
                 market_data_manager: Optional['MarketDataManager'] = None,
                 indicator_config: Optional[Any] = None):
        """
        Initialize UniverseStateBuilder with multi-duration support.
        Args:
            universe: Universe object
            state_manager: UniverseStateManager instance for persistence
            env: Environment instance (uses global if None)
            base_duration: Base TimeDuration (default 5m)
            target_durations: List of TimeDuration (default [5m])
            market_data_manager: MarketDataManager instance
            indicator_config: Optional indicator config
        """
        self.universe = universe
        self.state_manager = state_manager
        self.env = env or get_environment()
        self.logger = logging.getLogger(__name__)
        self.market_data_manager = market_data_manager
        self.indicator_config = indicator_config
        self.universe_state = None

        # Multi-duration logic
        self.base_duration = base_duration or TimeDuration.create_5_minutes()
        self.target_durations = target_durations or [self.base_duration]
        self._validate_duration_compatibility()

        # Business rules configuration
        self.min_market_cap = 100_000_000  # $100M minimum market cap
        self.min_avg_volume = 100_000      # 100K shares minimum daily volume
        self.max_universe_size = 3000      # Maximum number of securities
        self.data_source_priorities = {
            'polygon': 3,
            'tiingo': 2,
            'manual': 1
        }

    def get_base_duration(self) -> 'TimeDuration':
        return self.base_duration

    def get_target_durations(self) -> 'List[TimeDuration]':
        return self.target_durations

    def set_target_durations(self, durations: 'List[TimeDuration]'):
        self.target_durations = durations
        self._validate_duration_compatibility()

    def _validate_duration_compatibility(self):
        # Only validate if all durations are minute-based
        base = self.base_duration
        for dur in self.target_durations:
            if dur.is_intraday() and base.is_intraday():
                base_min = base.get_duration_minutes()
                dur_min = dur.get_duration_minutes()
                if dur_min % base_min != 0:
                    raise ValueError(f"Target duration {dur} is not a multiple of base duration {base}")

    def build_multi_duration_intervals(self, start_time: 'datetime') -> dict:
        """
        Build intervals for all target durations for the current universe at start_time.
        Returns a dict mapping duration string to UniverseInterval.
        """
        intervals = {}
        for duration in self.target_durations:
            end_time = duration.get_end_time(start_time)
            instrument_intervals = {}
            ohlc_batch = self.market_data_manager.get_ohlc_batch(self.universe.instrument_ids, start_time, end_time)
            for inst_id in self.universe.instrument_ids:
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
            intervals[duration.get_duration_string()] = UniverseInterval(
                start_date_time=start_time,
                end_date_time=end_time,
                instrument_intervals=instrument_intervals
            )
        return intervals

    def add_multi_duration_intervals(self, start_time: 'datetime', new_date=None, new_instrument_ids=None):
        """
        Add intervals for all target durations, optionally advancing universe date/instruments.
        """
        if new_date is not None:
            self.universe.current_date = new_date
        if new_instrument_ids is not None:
            self.universe.instrument_ids = new_instrument_ids
        intervals = self.build_multi_duration_intervals(start_time)
        if self.universe_state is None:
            from trading.indicator import UniverseState
            self.universe_state = UniverseState()
        for interval in intervals.values():
            self.universe_state.intervals.append(interval)
        return intervals

    def build_aggregated_interval(self, intervals: 'List[UniverseInterval]', target_duration: 'TimeDuration') -> 'UniverseInterval':
        """
        Aggregate a list of UniverseInterval into a single UniverseInterval for the target duration.
        Assumes all intervals contain the same instrument_id.
        """
        if not intervals:
            raise ValueError("Cannot aggregate empty list of intervals")
        instrument_ids = set()
        for interval in intervals:
            instrument_ids.update(interval.instrument_intervals.keys())
        if len(instrument_ids) != 1:
            raise ValueError("All intervals must contain exactly one common instrument_id")
        instrument_id = instrument_ids.pop()
        inst_intervals = [interval.instrument_intervals[instrument_id] for interval in intervals]
        start_date_time = inst_intervals[0].start_date_time
        end_date_time = inst_intervals[-1].end_date_time
        open_ = inst_intervals[0].open
        close_ = inst_intervals[-1].close
        high_ = max(i.high for i in inst_intervals)
        low_ = min(i.low for i in inst_intervals)
        traded_volume = sum(i.traded_volume for i in inst_intervals)
        traded_dollar = sum(i.traded_dollar for i in inst_intervals)
        status = 'ok' if all(i.status == 'ok' for i in inst_intervals) else 'invalid'
        aggregated_inst = InstrumentInterval(
            instrument_id=instrument_id,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            open=open_,
            high=high_,
            low=low_,
            close=close_,
            traded_volume=traded_volume,
            traded_dollar=traded_dollar,
            status=status
        )
        return UniverseInterval(
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            instrument_intervals={instrument_id: aggregated_inst}
        )

    def build(self):
        """
        Return the built universe state (for test compatibility). If indicator_config is provided, compute indicators for the latest interval.
        """
        if self.indicator_config and self.universe_state and hasattr(self.universe_state, "intervals") and self.universe_state.intervals:
            # Compute indicators for latest interval (simulate as in test)
            latest_interval = self.universe_state.intervals[-1]
            from state.indicator_interval import IndicatorInterval
            self.universe_state.indicator_intervals = {}
            indicator_instances = self.indicator_config.create_indicator_instances()
            # For each instrument in the interval
            for inst_id, inst_interval in latest_interval.instrument_intervals.items():
                # Gather history for this instrument
                history = []
                if hasattr(self.universe_state, "instrument_history") and inst_id in self.universe_state.instrument_history:
                    history = self.universe_state.instrument_history[inst_id]
                else:
                    history = [inst_interval]
                indicator_interval = IndicatorInterval(
                    instrument_id=inst_id,
                    start_date_time=inst_interval.start_date_time,
                    end_date_time=inst_interval.end_date_time,
                )
                for name, indicator in indicator_instances.items():
                    indicator.update(history)
                    indicator_interval.add_indicator(
                        name,
                        indicator.get_value(),
                        indicator.status,
                        getattr(indicator, 'update_at', None)
                    )
                self.universe_state.indicator_intervals[inst_id] = indicator_interval
        return self.universe_state

    
    async def build_universe_state(self, as_of_date: str) -> pd.DataFrame:
        """
        Build complete universe state for a specific date.
        
        Orchestrates data collection from multiple sources and applies
        business logic to create a comprehensive universe state.
        
        Args:
            as_of_date: Date string in YYYY-MM-DD format
            
        Returns:
            DataFrame with complete universe state
            
        Raises:
            ValueError: If date format is invalid
            RuntimeError: If universe building fails
        """
        try:
            self.logger.info(f"Building universe state for {as_of_date}")
            
            # Validate date format
            datetime.strptime(as_of_date, "%Y-%m-%d")
            
            # Step 1: Get base universe from database
            base_universe = await self._get_base_universe(as_of_date)
            
            # Step 2: Apply membership changes
            universe_with_changes = await self._apply_membership_changes(
                base_universe, as_of_date
            )
            
            # Step 3: Apply corporate actions
            universe_with_actions = await self._apply_corporate_actions(
                universe_with_changes, as_of_date
            )
            
            # Step 4: Calculate derived fields
            universe_with_derived = self._calculate_derived_fields(universe_with_actions)
            
            # Step 5: Apply business rules and filters
            filtered_universe = self._apply_business_rules(universe_with_derived)
            
            # Step 6: Final validation
            if not self.validate_universe_state(filtered_universe):
                raise RuntimeError("Universe state validation failed")
            
            self.logger.info(f"Universe state built: {len(filtered_universe)} securities")
            return filtered_universe
            
        except Exception as e:
            self.logger.error(f"Failed to build universe state for {as_of_date}: {e}")
            raise RuntimeError(f"Universe building failed: {e}")
    
    async def update_universe_membership(self, 
                                       membership_changes: List[UniverseMembershipChange]) -> None:
        """
        Apply membership changes to the universe.
        
        Args:
            membership_changes: List of membership changes to apply
        """
        if not membership_changes:
            return
        
        self.logger.info(f"Applying {len(membership_changes)} membership changes")
        
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                for change in membership_changes:
                    await self._apply_single_membership_change(conn, change)
                    
        finally:
            await pool.close()
    
    def validate_universe_state(self, universe_data: pd.DataFrame) -> bool:
        """
        Validate universe state meets business rules and constraints.
        
        Args:
            universe_data: DataFrame to validate
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check required columns
            required_columns = [
                'market_cap', 'avg_volume', 
                'exchange', 'is_active', 'as_of_date'
            ]
            
            missing_columns = set(required_columns) - set(universe_data.columns)
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return False
            
            # Check for empty universe
            if universe_data.empty:
                self.logger.error("Universe state is empty")
                return False
            
            # Check for duplicate symbols
            if universe_data['symbol'].duplicated().any():
                self.logger.error("Duplicate symbols found in universe")
                return False
            
            # Check market cap constraints
            if (universe_data['market_cap'] < 0).any():
                self.logger.error("Negative market cap values found")
                return False
            
            # Check volume constraints
            if (universe_data['avg_volume'] < 0).any():
                self.logger.error("Negative volume values found")
                return False
            
            # Check universe size
            if len(universe_data) > self.max_universe_size:
                self.logger.warning(f"Universe size ({len(universe_data)}) exceeds maximum ({self.max_universe_size})")
            
            # Check data completeness
            critical_columns = ['market_cap']
            for col in critical_columns:
                null_count = universe_data[col].isnull().sum()
                if null_count > 0:
                    self.logger.warning(f"Found {null_count} null values in {col}")
            
            self.logger.info("Universe state validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Universe validation failed: {e}")
            return False
    
    async def rebuild_from_scratch(self, as_of_date: str) -> pd.DataFrame:
        """
        Rebuild entire universe state from source data.
        
        This method performs a complete rebuild without using any cached
        or incremental data.
        
        Args:
            as_of_date: Date string in YYYY-MM-DD format
            
        Returns:
            DataFrame with rebuilt universe state
        """
        self.logger.info(f"Rebuilding universe from scratch for {as_of_date}")
        
        # Clear any cached data
        self.state_manager.clear_cache()
        
        # Rebuild from source data
        return await self.build_universe_state(as_of_date)
    
    def apply_corporate_actions(self, 
                              universe_data: pd.DataFrame,
                              corporate_actions: List[CorporateAction]) -> pd.DataFrame:
        """
        Apply corporate actions to universe state.
        
        Args:
            universe_data: Current universe state
            corporate_actions: List of corporate actions to apply
            
        Returns:
            DataFrame with corporate actions applied
        """
        if not corporate_actions:
            return universe_data
        
        self.logger.info(f"Applying {len(corporate_actions)} corporate actions")
        
        result_data = universe_data.copy()
        
        for action in corporate_actions:
            result_data = self._apply_single_corporate_action(result_data, action)
        
        return result_data
    
    def calculate_derived_fields(self, universe_data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate derived fields for universe state.
        
        Args:
            universe_data: Base universe data
            
        Returns:
            DataFrame with derived fields added
        """
        return self._calculate_derived_fields(universe_data)
    
    def calculate_changes(self, 
                         old_state: pd.DataFrame, 
                         new_state: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate changes between two universe states.
        
        Args:
            old_state: Previous universe state
            new_state: Current universe state
            
        Returns:
            DataFrame with changes (additions, removals, updates)
        """
        # Find additions (in new but not in old)
        additions = new_state[~new_state['symbol'].isin(old_state['symbol'])].copy()
        additions['change_type'] = 'addition'
        
        # Find removals (in old but not in new)
        removals = old_state[~old_state['symbol'].isin(new_state['symbol'])].copy()
        removals['change_type'] = 'removal'
        
        # Find updates (in both but with different values)
        common_symbols = set(old_state['symbol']) & set(new_state['symbol'])
        updates = []
        
        for symbol in common_symbols:
            old_row = old_state[old_state['symbol'] == symbol].iloc[0]
            new_row = new_state[new_state['symbol'] == symbol].iloc[0]
            
            # Check for changes in key fields
            key_fields = ['market_cap', 'exchange', 'is_active']
            has_changes = False
            
            for field in key_fields:
                if field in old_row and field in new_row:
                    if old_row[field] != new_row[field]:
                        has_changes = True
                        break
            
            if has_changes:
                update_row = new_row.copy()
                update_row['change_type'] = 'update'
                updates.append(update_row)
        
        # Combine all changes
        if updates:
            updates_df = pd.DataFrame(updates)
            changes = pd.concat([additions, removals, updates_df], ignore_index=True)
        else:
            changes = pd.concat([additions, removals], ignore_index=True)
        
        return changes.sort_values('symbol').reset_index(drop=True)
    
    # Private helper methods
    
    async def _get_base_universe(self, as_of_date: str) -> pd.DataFrame:
        """Get base universe from database."""
        # Ensure as_of_date is a datetime.date
        if isinstance(as_of_date, str):
            as_of_date_dt = datetime.strptime(as_of_date, "%Y-%m-%d").date()
        else:
            as_of_date_dt = as_of_date
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                # Get active instruments with basic data
                query = f"""
                SELECT DISTINCT
                    i.symbol,
                    i.name,
                    
                    i.exchange,
                    dp.close,
                    dp.volume,
                    dp.market_cap,
                    dp.date as as_of_date,
                    true as is_active
                FROM {self.env.get_table_name('instruments')} i
                JOIN {self.env.get_table_name('daily_prices')} dp ON i.symbol = dp.symbol
                WHERE dp.date = $1
                    AND dp.close > 0
                    AND dp.volume > 0
                    AND i.active = true
                ORDER BY i.symbol
                """
                
                rows = await conn.fetch(query, as_of_date_dt)
                
                if not rows:
                    self.logger.warning(f"No base universe data found for {as_of_date}")
                    return pd.DataFrame()
                
                return pd.DataFrame([dict(row) for row in rows])
                
        finally:
            await pool.close()

    async def _apply_membership_changes(self, 
                                      universe_data: pd.DataFrame, 
                                      as_of_date: str) -> pd.DataFrame:
        """Apply membership changes up to the specified date."""
        # Ensure as_of_date is a datetime.date
        if isinstance(as_of_date, str):
            as_of_date_dt = datetime.strptime(as_of_date, "%Y-%m-%d").date()
        else:
            as_of_date_dt = as_of_date
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                # Get membership changes up to the date
                query = f"""
                SELECT symbol, action, effective_date, reason
                FROM {self.env.get_table_name('universe_membership_changes')}
                WHERE effective_date <= $1
                ORDER BY effective_date, symbol
                """
                
                rows = await conn.fetch(query, as_of_date_dt)
                
                result_data = universe_data.copy()
                
                for row in rows:
                    if row['action'] == 'add':
                        # Add symbol if not already present
                        if row['symbol'] not in result_data['symbol'].values:
                            # Get symbol data and add to universe
                            symbol_data = await self._get_symbol_data(conn, row['symbol'], as_of_date)
                            if not symbol_data.empty:
                                result_data = pd.concat([result_data, symbol_data], ignore_index=True)
                    
                    elif row['action'] == 'remove':
                        # Remove symbol from universe
                        result_data = result_data[result_data['symbol'] != row['symbol']]
                
                return result_data
                
        finally:
            await pool.close()
    
    async def _apply_corporate_actions(self, 
                                     universe_data: pd.DataFrame, 
                                     as_of_date: str) -> pd.DataFrame:
        """Apply corporate actions up to the specified date."""
        # Ensure as_of_date is a datetime.date
        if isinstance(as_of_date, str):
            as_of_date_dt = datetime.strptime(as_of_date, "%Y-%m-%d").date()
        else:
            as_of_date_dt = as_of_date
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                # Get corporate actions up to the date
                query = f"""
                SELECT symbol, action_type, effective_date, ratio, amount, new_symbol
                FROM {self.env.get_table_name('corporate_actions')}
                WHERE effective_date <= $1
                ORDER BY effective_date, symbol
                """
                
                rows = await conn.fetch(query, as_of_date_dt)
                
                result_data = universe_data.copy()
                
                for row in rows:
                    action = CorporateAction(
                        symbol=row['symbol'],
                        action_type=CorporateActionType(row['action_type']),
                        effective_date=row['effective_date'],
                        ratio=row['ratio'],
                        amount=row['amount'],
                        new_symbol=row['new_symbol']
                    )
                    result_data = self._apply_single_corporate_action(result_data, action)
                
                return result_data
                
        finally:
            await pool.close()
    
    def _calculate_derived_fields(self, universe_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate derived fields like market cap tiers, liquidity metrics, etc."""
        if universe_data.empty:
            return universe_data
        
        result_data = universe_data.copy()
        
        # Market cap tiers
        result_data['market_cap_tier'] = pd.cut(
            result_data['market_cap'],
            bins=[0, 300_000_000, 2_000_000_000, 10_000_000_000, float('inf')],
            labels=['Small', 'Mid', 'Large', 'Mega'],
            include_lowest=True
        )
        
        # Volume tiers (liquidity)
        result_data['liquidity_tier'] = pd.cut(
            result_data['avg_volume'] if 'avg_volume' in result_data.columns else result_data['volume'],
            bins=[0, 100_000, 500_000, 2_000_000, float('inf')],
            labels=['Low', 'Medium', 'High', 'Very High'],
            include_lowest=True
        )
        
        # Price tiers
        if 'close_price' in result_data.columns:
            result_data['price_tier'] = pd.cut(
                result_data['close_price'],
                bins=[0, 5, 20, 100, float('inf')],
                labels=['Penny', 'Low', 'Medium', 'High'],
                include_lowest=True
            )
        
        # Calculate average volume if not present
        if 'avg_volume' not in result_data.columns and 'volume' in result_data.columns:
            result_data['avg_volume'] = result_data['volume']  # Simplified for now
        
        # Add universe rank by market cap
        result_data['market_cap_rank'] = result_data['market_cap'].rank(method='dense', ascending=False)
        
        # Add  weights (percentage of universe)
        _counts = result_data[''].value_counts()
        result_data['_weight'] = result_data[''].map(
            _counts / len(result_data)
        )
        
        return result_data
    
    def _apply_business_rules(self, universe_data: pd.DataFrame) -> pd.DataFrame:
        """Apply business rules and filters to universe data."""
        if universe_data.empty:
            return universe_data
        
        result_data = universe_data.copy()
        
        # Apply minimum market cap filter
        result_data = result_data[result_data['market_cap'] >= self.min_market_cap]
        
        # Apply minimum volume filter
        volume_col = 'avg_volume' if 'avg_volume' in result_data.columns else 'volume'
        result_data = result_data[result_data[volume_col] >= self.min_avg_volume]
        
        # Filter out inactive securities
        if 'is_active' in result_data.columns:
            result_data = result_data[result_data['is_active'] == True]
        
        # Limit universe size (keep top by market cap)
        if len(result_data) > self.max_universe_size:
            result_data = result_data.nlargest(self.max_universe_size, 'market_cap')
        
        return result_data.reset_index(drop=True)
    
    def _apply_single_corporate_action(self, 
                                     universe_data: pd.DataFrame, 
                                     action: CorporateAction) -> pd.DataFrame:
        """Apply a single corporate action to universe data."""
        result_data = universe_data.copy()
        
        if action.action_type == CorporateActionType.SPLIT:
            # Adjust prices and shares for stock split
            mask = result_data['symbol'] == action.symbol
            if mask.any():
                if 'close_price' in result_data.columns:
                    result_data.loc[mask, 'close_price'] /= action.ratio
                if 'volume' in result_data.columns:
                    result_data.loc[mask, 'volume'] *= action.ratio
        
        elif action.action_type == CorporateActionType.DELISTING:
            # Remove delisted security
            result_data = result_data[result_data['symbol'] != action.symbol]
        
        elif action.action_type == CorporateActionType.MERGER:
            # Handle merger (replace old symbol with new)
            if action.new_symbol:
                mask = result_data['symbol'] == action.symbol
                result_data.loc[mask, 'symbol'] = action.new_symbol
        
        return result_data
    
    async def _apply_single_membership_change(self, 
                                            conn: asyncpg.Connection, 
                                            change: UniverseMembershipChange) -> None:
        """Apply a single membership change to the database."""
        table_name = self.env.get_table_name('universe_membership_changes')
        
        query = f"""
        INSERT INTO {table_name} (symbol, action, effective_date, reason, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (symbol, effective_date) DO UPDATE SET
            action = EXCLUDED.action,
            reason = EXCLUDED.reason,
            updated_at = NOW()
        """
        
        await conn.execute(
            query,
            change.symbol,
            change.action.value,
            change.effective_date,
            change.reason,
            datetime.now()
        )
    
    async def _get_symbol_data(self, 
                             conn: asyncpg.Connection, 
                             symbol: str, 
                             as_of_date: str) -> pd.DataFrame:
        # Ensure as_of_date is a datetime.date
        if isinstance(as_of_date, str):
            as_of_date_dt = datetime.strptime(as_of_date, "%Y-%m-%d").date()
        else:
            as_of_date_dt = as_of_date
        
        query = f"""
        SELECT 
            i.symbol,
            i.name,
            
            i.exchange,
            dp.close,
            dp.volume,
            dp.market_cap,
            $2 as as_of_date,
            true as is_active
        FROM {self.env.get_table_name('instruments')} i
        JOIN {self.env.get_table_name('daily_prices')} dp ON i.symbol = dp.symbol
        WHERE i.symbol = $1 AND dp.date = $2
        """
        
        rows = await conn.fetch(query, symbol, as_of_date_dt)
        
        if rows:
            return pd.DataFrame([dict(row) for row in rows])
        else:
            return pd.DataFrame()
