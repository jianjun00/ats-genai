"""
UniverseService - High-level service orchestrating universe operations.

This module provides a unified interface for universe state management,
combining the persistence layer (UniverseStateManager) with the business
logic layer (UniverseStateBuilder).
"""

import pandas as pd
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta
from config.environment import Environment, get_environment
from state.universe_state_manager import UniverseStateManager
from state.universe_state_builder import (
    UniverseStateBuilder, 
    UniverseMembershipChange, 
    CorporateAction
)


class UniverseService:
    """
    High-level service orchestrating universe operations.
    
    Provides a unified interface for all universe-related operations,
    combining fast persistence with business logic processing.
    """
    
    def __init__(self, 
                 base_path: Optional[str] = None,
                 env: Optional[Environment] = None):
        """
        Initialize UniverseService.
        
        Args:
            base_path: Base directory for universe state files
            env: Environment instance (uses global if None)
        """
        self.env = env or get_environment()
        self.state_manager = UniverseStateManager(base_path)
        self.state_builder = UniverseStateBuilder(self.state_manager, self.env)
        self.logger = logging.getLogger(__name__)
    
    async def get_current_universe(self, 
                                 filters: Optional[List] = None,
                                 columns: Optional[List[str]] = None,
                                 use_cache: bool = True) -> pd.DataFrame:
        """
        Get current universe state with optional filtering.
        
        Args:
            filters: PyArrow filters for fast data filtering
            columns: Specific columns to load (all if None)
            use_cache: Whether to use in-memory cache
            
        Returns:
            DataFrame containing current universe state
            
        Raises:
            FileNotFoundError: If no universe states exist
        """
        try:
            return self.state_manager.load_universe_state(
                timestamp=None,  # Latest
                filters=filters,
                columns=columns,
                use_cache=use_cache
            )
        except FileNotFoundError:
            self.logger.warning("No current universe state found")
            raise
    
    async def get_universe_at_date(self, 
                                 as_of_date: str,
                                 filters: Optional[List] = None,
                                 columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Get universe state for a specific date.
        
        Args:
            as_of_date: Date string in YYYY-MM-DD format
            filters: PyArrow filters for fast data filtering
            columns: Specific columns to load (all if None)
            
        Returns:
            DataFrame containing universe state for the date
        """
        # Convert date to timestamp format
        date_obj = datetime.strptime(as_of_date, "%Y-%m-%d")
        timestamp = date_obj.strftime("%Y%m%d_000000")
        
        # Find closest available timestamp
        available_states = self.state_manager.list_available_states()
        closest_timestamp = None
        
        for state_timestamp in available_states:
            if state_timestamp <= timestamp:
                closest_timestamp = state_timestamp
                break
        
        if not closest_timestamp:
            raise FileNotFoundError(f"No universe state found for date {as_of_date}")
        
        return self.state_manager.load_universe_state(
            timestamp=closest_timestamp,
            filters=filters,
            columns=columns
        )
    
    async def update_universe(self, 
                            as_of_date: str,
                            force_rebuild: bool = False) -> str:
        """
        Build new universe state and persist it.
        
        Args:
            as_of_date: Date string in YYYY-MM-DD format
            force_rebuild: Whether to force a complete rebuild
            
        Returns:
            File path of saved universe state
        """
        try:
            self.logger.info(f"Updating universe for {as_of_date}")
            
            # Build universe state using business logic
            if force_rebuild:
                universe_data = await self.state_builder.rebuild_from_scratch(as_of_date)
            else:
                universe_data = await self.state_builder.build_universe_state(as_of_date)
            
            # Generate timestamp for persistence
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Prepare metadata
            metadata = {
                'as_of_date': as_of_date,
                'data_sources': ['database', 'corporate_actions', 'membership_changes'],
                'universe_type': 'equity',
                'build_method': 'rebuild' if force_rebuild else 'incremental'
            }
            
            # Persist using state manager
            file_path = self.state_manager.save_universe_state(
                universe_data, 
                timestamp, 
                metadata
            )
            
            self.logger.info(f"Universe updated successfully: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Failed to update universe: {e}")
            raise
    
    async def apply_membership_changes(self, 
                                     membership_changes: List[UniverseMembershipChange]) -> None:
        """
        Apply membership changes to the universe.
        
        Args:
            membership_changes: List of membership changes to apply
        """
        await self.state_builder.update_universe_membership(membership_changes)
        self.logger.info(f"Applied {len(membership_changes)} membership changes")
    
    async def apply_corporate_actions(self, 
                                    corporate_actions: List[CorporateAction]) -> None:
        """
        Apply corporate actions and update universe state.
        
        Args:
            corporate_actions: List of corporate actions to apply
        """
        # Get current universe
        current_universe = await self.get_current_universe()
        
        # Apply corporate actions
        updated_universe = self.state_builder.apply_corporate_actions(
            current_universe, corporate_actions
        )
        
        # Save updated state
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        metadata = {
            'data_sources': ['corporate_actions'],
            'universe_type': 'equity',
            'build_method': 'corporate_actions_update'
        }
        
        self.state_manager.save_universe_state(updated_universe, timestamp, metadata)
        self.logger.info(f"Applied {len(corporate_actions)} corporate actions")
    
    async def get_universe_changes(self, 
                                 from_date: str, 
                                 to_date: str) -> pd.DataFrame:
        """
        Compare two universe states to see what changed.
        
        Args:
            from_date: Start date in YYYY-MM-DD format
            to_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with changes (additions, removals, updates)
        """
        old_state = await self.get_universe_at_date(from_date)
        new_state = await self.get_universe_at_date(to_date)
        
        return self.state_builder.calculate_changes(old_state, new_state)
    
    def get_universe_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about universe states.
        
        Returns:
            Dictionary with universe statistics
        """
        # Get storage statistics
        storage_stats = self.state_manager.get_storage_stats()
        
        # Add business logic statistics
        try:
            current_universe = self.state_manager.load_universe_state()
            
            # Calculate business metrics
            sector_distribution = current_universe['sector'].value_counts().to_dict()
            market_cap_stats = {
                'mean': float(current_universe['market_cap'].mean()),
                'median': float(current_universe['market_cap'].median()),
                'min': float(current_universe['market_cap'].min()),
                'max': float(current_universe['market_cap'].max())
            }
            
            # Market cap tier distribution
            if 'market_cap_tier' in current_universe.columns:
                tier_distribution = current_universe['market_cap_tier'].value_counts().to_dict()
            else:
                tier_distribution = {}
            
            business_stats = {
                'sector_distribution': sector_distribution,
                'market_cap_stats': market_cap_stats,
                'tier_distribution': tier_distribution,
                'active_securities': int(current_universe['is_active'].sum()) if 'is_active' in current_universe.columns else len(current_universe)
            }
            
        except Exception as e:
            self.logger.warning(f"Could not calculate business statistics: {e}")
            business_stats = {}
        
        return {
            **storage_stats,
            **business_stats
        }
    
    def list_available_dates(self, limit: Optional[int] = None) -> List[str]:
        """
        List available universe state dates.
        
        Args:
            limit: Maximum number of dates to return
            
        Returns:
            List of date strings in YYYY-MM-DD format
        """
        timestamps = self.state_manager.list_available_states(limit)
        
        # Convert timestamps to dates
        dates = []
        for timestamp in timestamps:
            try:
                date_obj = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
                dates.append(date_obj.strftime("%Y-%m-%d"))
            except ValueError:
                continue
        
        return dates
    
    async def validate_current_universe(self) -> bool:
        """
        Validate the current universe state.
        
        Returns:
            True if validation passes, False otherwise
        """
        try:
            current_universe = await self.get_current_universe()
            return self.state_builder.validate_universe_state(current_universe)
        except Exception as e:
            self.logger.error(f"Universe validation failed: {e}")
            return False
    
    def cleanup_old_states(self, keep_days: int = 30) -> int:
        """
        Remove old universe states to manage disk space.
        
        Args:
            keep_days: Number of days of states to keep
            
        Returns:
            Number of files removed
        """
        return self.state_manager.cleanup_old_states(keep_days)
    
    def clear_cache(self) -> None:
        """Clear in-memory cache."""
        self.state_manager.clear_cache()
    
    async def get_universe_metadata(self, timestamp: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metadata for a universe state.
        
        Args:
            timestamp: Specific timestamp (latest if None)
            
        Returns:
            Dictionary with metadata information
        """
        if timestamp is None:
            timestamp = self.state_manager.get_latest_timestamp()
        
        if not timestamp:
            raise FileNotFoundError("No universe states found")
        
        metadata = self.state_manager.get_state_metadata(timestamp)
        return {
            'timestamp': metadata.timestamp,
            'record_count': metadata.record_count,
            'file_size_mb': round(metadata.file_size_bytes / (1024 * 1024), 2),
            'created_at': metadata.created_at,
            'columns': metadata.columns,
            'data_sources': metadata.data_sources,
            'universe_type': metadata.universe_type,
            'version': metadata.version,
            'checksum': metadata.checksum
        }
