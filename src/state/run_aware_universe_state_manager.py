"""
Run-Aware Universe State Manager

Enhanced UniverseStateManager that integrates with the run_id system for
better artifact organization and state isolation between runs.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import logging
import json
import hashlib
from datetime import datetime, timedelta
import shutil
import os
from dataclasses import dataclass, asdict

from .universe_state_manager import UniverseStateManager, UniverseStateMetadata
from core.run_context import RunContext, get_current_run_context
from state.universe_state import UniverseStateInterval
from state.instrument_interval import InstrumentInterval
from state.indicator_interval import IndicatorInterval

import gin
from dao.universe_state_interval_dao import UniverseStateIntervalDAO
from dao.instrument_interval_dao import InstrumentIntervalDAO
from dao.instrument_indicator_interval_dao import InstrumentIndicatorIntervalDAO
from dao.factor_interval_dao import FactorIntervalDAO

logger = logging.getLogger(__name__)


@gin.configurable
class RunAwareUniverseStateManager(UniverseStateManager):
    """
    Enhanced UniverseStateManager with run_id integration.
    
    Features:
    - Automatic run_id-based directory organization
    - State isolation between runs
    - Enhanced metadata with run information
    - Artifact traceability
    """
    
    def __init__(self, env=None, base_path: Optional[str] = None, write_metadata: bool = True, 
                 run_context: Optional[RunContext] = None):
        """
        Initialize RunAwareUniverseStateManager.
        
        Args:
            env: Environment instance (optional)
            base_path: Base directory for universe state files. If None, uses environment config.
            write_metadata: Whether to write metadata files (can be disabled for tests)
            run_context: Optional run context for organizing files by run_id
        """
        # Get run context if not provided
        if run_context is None:
            run_context = get_current_run_context()
        
        self.env = env
        self.run_context = run_context
        self.write_metadata = write_metadata
        
        # Determine base path based on run context or fallback
        if run_context:
            self.base_path = run_context.universe_state_dir
            self.run_id = run_context.run_id
            logger.info(f"Using run-specific universe state directory: {self.base_path} (run_id: {self.run_id})")
        else:
            self.base_path = Path(base_path) if base_path else Path("data/universe_state")
            self.run_id = None
            logger.warning("No run_context provided - using legacy directory structure")
            
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for organization
        self.states_dir = self.base_path / "states"
        self.metadata_dir = self.base_path / "metadata"
        self.cache_dir = self.base_path / "cache"
        
        # Add run-specific subdirectories if we have a run context
        if self.run_context:
            self.run_artifacts_dir = self.run_context.artifacts_dir / "universe_state"
            self.run_artifacts_dir.mkdir(parents=True, exist_ok=True)
        
        for dir_path in [self.states_dir, self.metadata_dir, self.cache_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # In-memory cache for frequently accessed data
        self._cache: Dict[str, pd.DataFrame] = {}
        self._cache_expiry: Dict[str, datetime] = {}
        self._cache_ttl = timedelta(minutes=30)  # Cache TTL
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize DAOs with environment
        if env:
            self.dao = UniverseStateIntervalDAO(env)
            self.instrument_interval_dao = InstrumentIntervalDAO(env)
            self.instrument_indicator_interval_dao = InstrumentIndicatorIntervalDAO(env)
            self.factor_interval_dao = FactorIntervalDAO(env)
        else:
            self.dao = None
            self.instrument_interval_dao = None
            self.instrument_indicator_interval_dao = None
            self.factor_interval_dao = None
    
    def _get_enhanced_metadata(self, df: pd.DataFrame, timestamp: str) -> UniverseStateMetadata:
        """Create enhanced metadata with run information."""
        # Calculate basic metadata
        record_count = len(df)
        file_size_bytes = int(df.memory_usage(deep=True).sum())  # Convert to int for JSON serialization
        
        # Create checksum
        checksum = hashlib.md5(df.to_string().encode()).hexdigest()
        
        # Enhanced metadata with run information
        metadata = UniverseStateMetadata(
            timestamp=timestamp,
            record_count=record_count,
            file_size_bytes=file_size_bytes,
            checksum=checksum,
            created_at=datetime.now().isoformat(),
            columns=list(df.columns),
            data_sources=self._extract_data_sources(df),
            universe_type="run_aware",
            version="1.1"
        )
        
        return metadata
    
    def _extract_data_sources(self, df: pd.DataFrame) -> List[str]:
        """Extract data sources from DataFrame columns."""
        sources = set()
        
        # Look for vendor/source indicators in columns
        for col in df.columns:
            col_lower = col.lower()
            if 'polygon' in col_lower:
                sources.add('polygon')
            elif 'tiingo' in col_lower:
                sources.add('tiingo')
            elif 'finnhub' in col_lower:
                sources.add('finnhub')
            elif 'yahoo' in col_lower:
                sources.add('yahoo')
        
        return list(sources) if sources else ['unknown']
    
    def save_universe_state(self, df: pd.DataFrame, timestamp: str, 
                           persist_to_db: bool = True) -> Optional[str]:
        """
        Save universe state with run_id organization.
        
        Args:
            df: Universe state DataFrame
            timestamp: Timestamp string for the data
            persist_to_db: Whether to persist to database
            
        Returns:
            Path to saved file or None if failed
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for timestamp {timestamp}")
            return None
        
        try:
            # Generate filename with run_id prefix if available
            if self.run_id:
                filename = f"{self.run_id}_universe_state_{timestamp}.parquet"
            else:
                filename = f"universe_state_{timestamp}.parquet"
            
            file_path = self.states_dir / filename
            
            # Save parquet file
            df.to_parquet(file_path, engine='pyarrow')
            logger.info(f"Saved universe state to {file_path} ({len(df)} records)")
            
            # Save enhanced metadata
            if self.write_metadata:
                try:
                    metadata = self._get_enhanced_metadata(df, timestamp)
                    
                    # Add run context to metadata
                    if self.run_context:
                        metadata.universe_type = f"run_aware_{self.run_id}"
                    
                    metadata_file = self.metadata_dir / f"{filename}.metadata.json"
                    with open(metadata_file, 'w') as f:
                        json.dump(asdict(metadata), f, indent=2)
                    
                    logger.debug(f"Saved metadata to {metadata_file}")
                except Exception as e:
                    logger.warning(f"Failed to save metadata: {e}")
            
            # Copy to run artifacts directory for easy access
            if self.run_context:
                try:
                    artifact_path = self.run_artifacts_dir / filename
                    shutil.copy2(file_path, artifact_path)
                    logger.debug(f"Copied universe state to run artifacts: {artifact_path}")
                except Exception as e:
                    logger.warning(f"Failed to copy to artifacts: {e}")
            
            # Persist to database if requested
            if persist_to_db and self.dao:
                try:
                    self._persist_to_database(df, timestamp)
                except Exception as e:
                    logger.error(f"Failed to persist to database: {e}")
            
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Failed to save universe state for {timestamp}: {e}")
            return None
    
    def load_universe_state(self, timestamp: str, from_run_id: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Load universe state for a specific timestamp, optionally from a specific run.
        
        Args:
            timestamp: Timestamp string
            from_run_id: Optional run_id to load from (defaults to current run)
            
        Returns:
            DataFrame or None if not found
        """
        # Determine which run_id to use
        target_run_id = from_run_id or self.run_id
        
        # Try run-specific file first
        if target_run_id:
            filename = f"{target_run_id}_universe_state_{timestamp}.parquet"
        else:
            filename = f"universe_state_{timestamp}.parquet"
        
        file_path = self.states_dir / filename
        
        if file_path.exists():
            try:
                df = pd.read_parquet(file_path)
                logger.info(f"Loaded universe state from {file_path} ({len(df)} records)")
                return df
            except Exception as e:
                logger.error(f"Failed to load universe state from {file_path}: {e}")
        
        # Fallback: try without run_id prefix
        if target_run_id:
            fallback_filename = f"universe_state_{timestamp}.parquet"
            fallback_path = self.states_dir / fallback_filename
            
            if fallback_path.exists():
                try:
                    df = pd.read_parquet(fallback_path)
                    logger.info(f"Loaded universe state from fallback {fallback_path} ({len(df)} records)")
                    return df
                except Exception as e:
                    logger.error(f"Failed to load universe state from fallback {fallback_path}: {e}")
        
        logger.warning(f"No universe state found for timestamp {timestamp}")
        return None
    
    def list_available_timestamps(self, run_id: Optional[str] = None) -> List[str]:
        """
        List available timestamps for universe state files.
        
        Args:
            run_id: Optional run_id to filter by
            
        Returns:
            List of timestamp strings
        """
        timestamps = set()
        
        # Search for files in states directory
        for file_path in self.states_dir.glob("*.parquet"):
            filename = file_path.name
            
            # Extract timestamp from filename
            if run_id:
                # Look for run-specific files
                prefix = f"{run_id}_universe_state_"
                if filename.startswith(prefix):
                    timestamp = filename[len(prefix):-8]  # Remove .parquet
                    timestamps.add(timestamp)
            else:
                # Look for any universe state files
                if "universe_state_" in filename:
                    # Extract timestamp (handles both run-aware and legacy formats)
                    parts = filename.replace('.parquet', '').split('_')
                    if len(parts) >= 3:  # universe_state_TIMESTAMP or RUN_universe_state_TIMESTAMP
                        timestamp = '_'.join(parts[-1:])  # Get last part as timestamp
                        timestamps.add(timestamp)
        
        return sorted(list(timestamps))
    
    def get_run_summary(self) -> Dict[str, Any]:
        """Get summary information for the current run."""
        if not self.run_context:
            return {'error': 'No run context available'}
        
        timestamps = self.list_available_timestamps(self.run_id)
        
        # Calculate total file sizes
        total_size = 0
        file_count = 0
        for file_path in self.states_dir.glob(f"{self.run_id}_*.parquet"):
            total_size += file_path.stat().st_size
            file_count += 1
        
        return {
            'run_id': self.run_id,
            'start_time': self.run_context.start_time.isoformat(),
            'universe_state_dir': str(self.run_context.universe_state_dir),
            'artifacts_dir': str(self.run_context.artifacts_dir),
            'available_timestamps': timestamps,
            'file_count': file_count,
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }
    
    def cleanup_current_run(self):
        """Clean up files for the current run."""
        if not self.run_id:
            logger.warning("No run_id available for cleanup")
            return
        
        removed_count = 0
        total_size = 0
        
        # Remove run-specific files
        for file_path in self.states_dir.glob(f"{self.run_id}_*"):
            try:
                size = file_path.stat().st_size
                file_path.unlink()
                removed_count += 1
                total_size += size
                logger.debug(f"Removed {file_path}")
            except Exception as e:
                logger.warning(f"Failed to remove {file_path}: {e}")
        
        # Remove metadata files
        for metadata_path in self.metadata_dir.glob(f"{self.run_id}_*.metadata.json"):
            try:
                metadata_path.unlink()
                logger.debug(f"Removed metadata {metadata_path}")
            except Exception as e:
                logger.warning(f"Failed to remove metadata {metadata_path}: {e}")
        
        logger.info(f"Cleanup complete: removed {removed_count} files ({total_size} bytes)")


# Convenience function to create run-aware manager
def create_run_aware_universe_state_manager(env=None, run_context: Optional[RunContext] = None) -> RunAwareUniverseStateManager:
    """Create a run-aware universe state manager with proper context."""
    return RunAwareUniverseStateManager(env=env, run_context=run_context)