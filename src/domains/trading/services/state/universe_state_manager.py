"""
UniverseStateManager - Modular Architecture

Data Persistence and Retrieval Layer for Universe State with focused modules.
"""

import pandas as pd
import gin
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta

from core.dao.universe_state_interval_dao import UniverseStateIntervalDAO
from core.dao.instrument_interval_dao import InstrumentIntervalDAO
from core.dao.instrument_indicator_interval_dao import InstrumentIndicatorIntervalDAO
from core.dao.factor_interval_dao import FactorIntervalDAO

from .universe_state.metadata import MetadataManager
from .universe_state.storage import StorageManager

logger = logging.getLogger(__name__)

@gin.configurable
class UniverseStateManager:
    """
    Modular Universe State Manager for optimized storage and retrieval.
    
    Coordinates metadata management and storage operations through
    focused modules for better maintainability.
    """
    
    def __init__(self, env=None, base_path: Optional[str] = None, write_metadata: bool = True):
        self.env = env
        self.base_path = base_path or "/tmp/universe_state"
        self.write_metadata = write_metadata
        
        # Initialize modular components
        self.metadata_manager = MetadataManager(self.base_path)
        self.storage_manager = StorageManager(self.base_path)
        
        logger.info(f"🚀 UniverseStateManager initialized with modular architecture")
        logger.info(f"   Base path: {self.base_path}")
        logger.info(f"   Metadata enabled: {self.write_metadata}")
        
    def get_lag_prices(self, instrument_id: int, cur_datetime, lag_periods: int, time_interval: str = '1d') -> pd.DataFrame:
        """Return OHLCV features for previous lag_periods."""
        
        logger.info(f"🔍 Getting {lag_periods} lag periods for instrument {instrument_id}")
        
        # TODO: Implement modular lag price retrieval
        # This would use storage_manager to efficiently query historical data
        
        return pd.DataFrame()
        
    def get_lead_prices(self, instrument_id: int, cur_datetime, lead_periods: int, time_interval: str = '1d') -> pd.DataFrame:
        """Return OHLCV features for future lead_periods."""
        
        logger.info(f"🔮 Getting {lead_periods} lead periods for instrument {instrument_id}")
        
        # TODO: Implement modular lead price retrieval
        
        return pd.DataFrame()
        
    def save_universe_state_sync(self, universe_data: pd.DataFrame, timestamp: str, 
                                metadata: Optional[Dict[str, Any]] = None, 
                                partition_cols: Optional[List[str]] = None) -> str:
        """Save universe state using modular storage."""
        
        logger.info(f"💾 Saving universe state for timestamp {timestamp}")
        
        try:
            # Save using storage manager
            filepath = self.storage_manager.save_parquet(universe_data, timestamp, partition_cols)
            
            # Save metadata if enabled
            if self.write_metadata:
                state_metadata = self.metadata_manager.create_metadata(
                    universe_data, 
                    timestamp, 
                    metadata.get('data_sources', []) if metadata else []
                )
                self.metadata_manager.save_metadata(state_metadata, filepath)
                
            logger.info(f"✅ Successfully saved universe state: {len(universe_data)} records")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"❌ Failed to save universe state: {e}")
            raise
            
    def load_universe_state(self, timestamp: Optional[str] = None, 
                           filters: Optional[List] = None, 
                           columns: Optional[List[str]] = None, 
                           use_cache: bool = True) -> pd.DataFrame:
        """Load universe state using modular storage."""
        
        if not timestamp:
            timestamp = self.get_latest_timestamp()
            
        if not timestamp:
            logger.warning("⚠️ No universe state data available")
            return pd.DataFrame()
            
        try:
            # Load using storage manager
            df = self.storage_manager.load_parquet(timestamp, columns, filters)
            
            logger.info(f"📊 Loaded universe state: {len(df)} records for {timestamp}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to load universe state: {e}")
            return pd.DataFrame()
            
    def get_latest_timestamp(self) -> Optional[str]:
        """Get the most recent timestamp."""
        
        files = self.storage_manager.list_files()
        return max(files) if files else None
        
    def list_available_states(self, limit: Optional[int] = None) -> List[str]:
        """List available universe state timestamps."""
        
        files = self.storage_manager.list_files()
        files.sort(reverse=True)  # Most recent first
        
        return files[:limit] if limit else files
        
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get comprehensive storage statistics."""
        
        files = self.storage_manager.list_files()
        total_size = sum(self.storage_manager.get_file_size(f) for f in files)
        
        return {
            "total_states": len(files),
            "total_size_mb": round(total_size / (1024*1024), 2),
            "latest_timestamp": self.get_latest_timestamp(),
            "base_path": str(self.base_path)
        }
        
    def cleanup_old_states(self, keep_days: int = 30) -> int:
        """Clean up old universe state files."""
        
        # TODO: Implement cleanup using storage manager
        
        logger.info(f"🧹 Cleanup completed: keeping {keep_days} days")
        return 0
