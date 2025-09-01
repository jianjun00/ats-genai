"""
Training Data Generation Job Runner with Run Tracking

This module provides a complete training data generation system that:
1. Creates run records in dev_runs table for tracking
2. Generates training data using configurable generators
3. Records training datasets in dev_training_dataset table
4. Links training datasets to their originating runs
5. Provides comprehensive error handling and logging
"""

import asyncio
import asyncpg
import gin
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from pathlib import Path
import json
import traceback
import logging
import os
import numpy as np
import pandas as pd
from dataclasses import dataclass

from config.environment import Environment
from dao.training_dataset_dao import TrainingDatasetDAO, TrainingDatasetRecord

logger = logging.getLogger(__name__)

class TechnicalIndicators:
    """
    Technical indicators using proper indicator classes.
    Ensures NO normalization - all indicators return actual values.
    """
    
    def __init__(self):
        # Import actual indicator classes
        from signals.indicator import EnvelopeTop, EnvelopeBot, PL, OneOneHigh, OneOneLow
        from signals.indicator import Z1B, Z2B, Z5T, Z6T
        from state.instrument_interval import InstrumentInterval
        
        self.EnvelopeTop = EnvelopeTop
        self.EnvelopeBot = EnvelopeBot
        self.PL = PL  # PLDOT indicator
        self.OneOneHigh = OneOneHigh
        self.OneOneLow = OneOneLow
        self.Z1B = Z1B
        self.Z2B = Z2B
        self.Z5T = Z5T
        self.Z6T = Z6T
        self.InstrumentInterval = InstrumentInterval
    
    def calculate_envelope_top(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, instrument_id: int = 1) -> np.ndarray:
        """Calculate EnvelopeTop using actual indicator class - returns price levels."""
        results = np.full_like(close, np.nan)
        indicator = self.EnvelopeTop()
        
        for i in range(3, len(close)):  # Need 3 intervals for calculation
            # Create intervals for the last 3 periods
            intervals = []
            for j in range(i-2, i+1):
                interval = self.InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=None,  # Not needed for calculation
                    end_date_time=None,
                    open=0,  # Not used by EnvelopeTop
                    high=high[j],
                    low=low[j],
                    close=close[j],
                    traded_volume=0,  # Not used
                    traded_dollar=0,  # Not used
                    status='ok'
                )
                intervals.append(interval)
            
            indicator.update(intervals)
            if indicator.status == 'ok':
                results[i] = indicator.get_value()
        
        return results
    
    def calculate_envelope_bot(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, instrument_id: int = 1) -> np.ndarray:
        """Calculate EnvelopeBot using actual indicator class - returns price levels."""
        results = np.full_like(close, np.nan)
        indicator = self.EnvelopeBot()
        
        for i in range(3, len(close)):  # Need 3 intervals for calculation
            # Create intervals for the last 3 periods
            intervals = []
            for j in range(i-2, i+1):
                interval = self.InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=None,
                    end_date_time=None,
                    open=0,  # Not used by EnvelopeBot
                    high=high[j],
                    low=low[j],
                    close=close[j],
                    traded_volume=0,
                    traded_dollar=0,
                    status='ok'
                )
                intervals.append(interval)
            
            indicator.update(intervals)
            if indicator.status == 'ok':
                results[i] = indicator.get_value()
        
        return results
    
    def calculate_pldot(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, instrument_id: int = 1) -> np.ndarray:
        """Calculate PLDOT using actual indicator class - returns momentum values."""
        results = np.full_like(close, np.nan)
        indicator = self.PL()
        
        for i in range(3, len(close)):  # Need 3 intervals for calculation
            intervals = []
            for j in range(i-2, i+1):
                interval = self.InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=None,
                    end_date_time=None,
                    open=0,  # Not used by PLDOT
                    high=high[j],
                    low=low[j],
                    close=close[j],
                    traded_volume=0,
                    traded_dollar=0,
                    status='ok'
                )
                intervals.append(interval)
            
            indicator.update(intervals)
            if indicator.status == 'ok':
                results[i] = indicator.get_value()
        
        return results
    
    def calculate_oneone_high(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, instrument_id: int = 1) -> np.ndarray:
        """Calculate OneOneHigh using actual indicator class - returns price levels."""
        results = np.full_like(close, np.nan)
        indicator = self.OneOneHigh()
        
        for i in range(len(close)):  # Need 1 interval for calculation
            intervals = [self.InstrumentInterval(
                instrument_id=instrument_id,
                start_date_time=None,
                end_date_time=None,
                open=0,  # Not used by OneOneHigh
                high=high[i],
                low=low[i],
                close=close[i],
                traded_volume=0,
                traded_dollar=0,
                status='ok'
            )]
            
            indicator.update(intervals)
            if indicator.status == 'ok':
                results[i] = indicator.get_value()
        
        return results
    
    def calculate_oneone_low(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, instrument_id: int = 1) -> np.ndarray:
        """Calculate OneOneLow using actual indicator class - returns price levels."""
        results = np.full_like(close, np.nan)
        indicator = self.OneOneLow()
        
        for i in range(len(close)):  # Need 1 interval for calculation
            intervals = [self.InstrumentInterval(
                instrument_id=instrument_id,
                start_date_time=None,
                end_date_time=None,
                open=0,  # Not used by OneOneLow
                high=high[i],
                low=low[i],
                close=close[i],
                traded_volume=0,
                traded_dollar=0,
                status='ok'
            )]
            
            indicator.update(intervals)
            if indicator.status == 'ok':
                results[i] = indicator.get_value()
        
        return results
    
    def calculate_z1b(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, instrument_id: int = 1) -> np.ndarray:
        """Calculate Z1B using actual indicator class - returns zone values."""
        results = np.full_like(close, np.nan)
        indicator = self.Z1B()
        
        for i in range(3, len(close)):  # Need 3 intervals for calculation
            intervals = []
            for j in range(i-2, i+1):
                interval = self.InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=None,
                    end_date_time=None,
                    open=0,  # Not used by Z1B
                    high=high[j],
                    low=low[j],
                    close=close[j],
                    traded_volume=0,
                    traded_dollar=0,
                    status='ok'
                )
                intervals.append(interval)
            
            indicator.update(intervals)
            if indicator.status == 'ok':
                results[i] = indicator.get_value()
        
        return results
    
    def calculate_z2b(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, instrument_id: int = 1) -> np.ndarray:
        """Calculate Z2B using actual indicator class - returns zone values."""
        results = np.full_like(close, np.nan)
        indicator = self.Z2B()
        
        for i in range(3, len(close)):  # Need 3 intervals for calculation
            intervals = []
            for j in range(i-2, i+1):
                interval = self.InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=None,
                    end_date_time=None,
                    open=0,  # Not used by Z2B
                    high=high[j],
                    low=low[j],
                    close=close[j],
                    traded_volume=0,
                    traded_dollar=0,
                    status='ok'
                )
                intervals.append(interval)
            
            indicator.update(intervals)
            if indicator.status == 'ok':
                results[i] = indicator.get_value()
        
        return results
    
    def calculate_z5t(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, instrument_id: int = 1) -> np.ndarray:
        """Calculate Z5T using actual indicator class - returns zone values."""
        results = np.full_like(close, np.nan)
        indicator = self.Z5T()
        
        for i in range(3, len(close)):  # Need 3 intervals for calculation
            intervals = []
            for j in range(i-2, i+1):
                interval = self.InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=None,
                    end_date_time=None,
                    open=0,  # Not used by Z5T
                    high=high[j],
                    low=low[j],
                    close=close[j],
                    traded_volume=0,
                    traded_dollar=0,
                    status='ok'
                )
                intervals.append(interval)
            
            indicator.update(intervals)
            if indicator.status == 'ok':
                results[i] = indicator.get_value()
        
        return results
    
    def calculate_z6t(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, instrument_id: int = 1) -> np.ndarray:
        """Calculate Z6T using actual indicator class - returns zone values."""
        results = np.full_like(close, np.nan)
        indicator = self.Z6T()
        
        for i in range(3, len(close)):  # Need 3 intervals for calculation
            intervals = []
            for j in range(i-2, i+1):
                interval = self.InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=None,
                    end_date_time=None,
                    open=0,  # Not used by Z6T
                    high=high[j],
                    low=low[j],
                    close=close[j],
                    traded_volume=0,
                    traded_dollar=0,
                    status='ok'
                )
                intervals.append(interval)
            
            indicator.update(intervals)
            if indicator.status == 'ok':
                results[i] = indicator.get_value()
        
        return results

@gin.configurable
@dataclass
class TrainingDataJobConfig:
    """Configuration for training data generation jobs."""
    
    # Job identification
    job_name: str = "training_data_generation"
    
    # Data selection
    symbols: List[str] = gin.REQUIRED
    start_date: date = gin.REQUIRED  
    end_date: date = gin.REQUIRED
    
    # Training data configuration
    base_interval_minutes: int = 1  # Base data interval (1 minute)
    training_interval_minutes: int = 60  # Training row interval (1 hour) 
    output_structure: str = "hourly_rows"  # "sequences" or "hourly_rows"
    sequence_length: int = 60
    prediction_horizon: int = 5
    normalize_features: bool = False  # Use actual indicator values, not normalized
    normalize_labels: bool = False
    use_enhanced_features: bool = True  # Enable enhanced technical indicators
    use_universe_state_indicators: bool = True  # Use universe state builder indicators
    
    # Feature and label configuration
    feature_configs: List[Dict[str, Any]] = gin.REQUIRED
    label_configs: List[Dict[str, Any]] = gin.REQUIRED
    
    # Output configuration (will be auto-generated based on environment and run_id)
    output_dir: str = "auto"  # Special value to trigger auto-generation
    dataset_name_prefix: str = "dataset"
    
    # Quality and validation
    min_sequences_required: int = 1000
    min_quality_score: float = 0.8
    
    # Processing configuration
    batch_size: int = 10000
    max_memory_mb: int = 8192

class TrainingDataJobRunner:
    """Training data generation job runner with comprehensive tracking."""
    
    def __init__(self, 
                 config: TrainingDataJobConfig,
                 env: Optional[Environment] = None,
                 output_dir: Optional[str] = None):
        self.config = config
        self.env = env or Environment()
        
        # Defer output_dir creation until we have run_id
        self._base_output_dir = output_dir or config.output_dir
        self.output_dir = None  # Will be set in _set_output_directory()
        
        # Initialize DAOs
        self.training_dataset_dao = TrainingDatasetDAO(env=self.env)
        
        # Job state
        self.run_id: Optional[int] = None
        self.start_time: Optional[datetime] = None
        self.dataset_ids: List[int] = []
    
    def _set_output_directory(self) -> None:
        """Set the output directory based on environment and run_id."""
        if self.run_id is None:
            raise ValueError("Cannot set output directory without run_id")
        
        # Check if a custom absolute path was provided
        if self._base_output_dir and self._base_output_dir != "auto" and ('/' in self._base_output_dir or self._base_output_dir.startswith('/mnt')):
            # Use the provided path as-is
            self.output_dir = Path(self._base_output_dir)
        else:
            # Generate environment-based path structure
            from config.environment import EnvironmentType
            
            if self.env.env_type == EnvironmentType.DEV:
                env_name = "ats-dev"
            elif self.env.env_type == EnvironmentType.INTG:
                env_name = "ats-intg"
            else:
                env_name = "ats-prod"
            
            # Create path: /data/ats-data/training_data/{env}/{run_id}
            # The container has /data mounted to D:\ drive
            self.output_dir = Path(f"/data/ats-data/training_data/{env_name}/{self.run_id}")
        
        # Create the directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Training data output directory: {self.output_dir}")
        
    async def run_training_data_generation(self) -> Dict[str, Any]:
        """Run complete training data generation with tracking."""
        
        self.start_time = datetime.now()
        
        try:
            # Create run record
            self.run_id = await self._create_run_record()
            logger.info(f"Started training data generation run {self.run_id}")
            
            # Set output directory based on environment and run_id
            self._set_output_directory()
            
            # Generate training data
            results = await self._generate_training_data()
            
            # Update run record with success
            await self._update_run_record_success(results)
            
            logger.info(f"✅ Training data generation completed successfully")
            logger.info(f"🎯 Run ID: {self.run_id}")
            logger.info(f"📊 Datasets created: {len(self.dataset_ids)}")
            
            return {
                "status": "success",
                "run_id": self.run_id,
                "dataset_ids": self.dataset_ids,
                "results": results
            }
            
        except Exception as e:
            error_msg = f"Training data generation failed: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Update run record with failure
            if self.run_id:
                await self._update_run_record_failure(error_msg)
            
            return {
                "status": "failed",
                "run_id": self.run_id,
                "error": error_msg,
                "dataset_ids": self.dataset_ids
            }
    
    async def _create_run_record(self) -> int:
        """Create initial run record for tracking."""
        
        run_config = {
            "job_name": self.config.job_name,
            "symbols": self.config.symbols,
            "start_date": self.config.start_date.isoformat(),
            "end_date": self.config.end_date.isoformat(),
            "sequence_length": self.config.sequence_length,
            "prediction_horizon": self.config.prediction_horizon,
            "feature_count": len(self.config.feature_configs),
            "label_count": len(self.config.label_configs),
            "output_directory": str(self.output_dir)
        }
        
        # Create run record directly in database
        conn = await asyncpg.connect(self.env.get_database_url())
        try:
            runs_table = self.env.get_table_name("runs")
            query = f"""
            INSERT INTO {runs_table} (
                run_type, start_time, status, total_symbols, training_config
            ) VALUES ($1, $2, $3, $4, $5) RETURNING id
            """
            
            run_id = await conn.fetchval(
                query,
                "training_data_generation",
                self.start_time,
                "running",
                len(self.config.symbols),
                json.dumps(run_config)
            )
            
            return run_id
        finally:
            await conn.close()
    
    async def _generate_training_data(self) -> Dict[str, Any]:
        """Generate training data using configured approach (sequences or hourly rows)."""
        
        # Load market data
        market_data = await self._load_market_data()
        
        if self.config.output_structure == "hourly_rows":
            # Generate hourly row-based training data with 1-minute base intervals
            hourly_df, metadata = await self._generate_hourly_training_data()
            
            # Save hourly training data
            dataset_id = f"hourly_{self.config.job_name}_run{self.run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            data_files = self._save_hourly_training_data(hourly_df, dataset_id, metadata)
            
            # Create training dataset record for hourly data
            dataset_record = await self._create_hourly_dataset_record(
                hourly_df, dataset_id, data_files, metadata
            )
            
        else:
            # Generate sequence-based training data (original approach)
            features, labels, metadata = self._create_basic_training_data(market_data)
            
            # Save training data files with unique run_id to prevent duplicates
            dataset_id = f"dataset_{self.config.job_name}_run{self.run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            data_files = self._save_training_data_files(features, labels, dataset_id, metadata)
            
            # Create training dataset record
            dataset_record = await self._create_dataset_record_simple(
                features, labels, dataset_id, data_files, metadata
            )
        
        if self.config.output_structure == "hourly_rows":
            return {
                "dataset_record": dataset_record,
                "training_results": {
                    "features_shape": list(hourly_df.shape) if not hourly_df.empty else [0, 0],
                    "labels_shape": [0, 0],  # No labels for hourly rows
                    "dataset_id": dataset_id,
                    "feature_names": list(hourly_df.columns) if not hourly_df.empty else [],
                    "label_names": []  # No labels for hourly rows
                }
            }
        else:
            return {
                "dataset_record": dataset_record,
                "training_results": {
                    "features_shape": features.shape,
                    "labels_shape": labels.shape,
                    "dataset_id": dataset_id,
                    "feature_names": metadata['feature_names'],
                    "label_names": metadata['label_names']
                }
            }
    
    async def _load_market_data(self) -> pd.DataFrame:
        """Load real market data for training data generation."""
        
        logger.info(f"Loading market data for symbols: {self.config.symbols}")
        logger.info(f"Date range: {self.config.start_date} to {self.config.end_date}")
        
        # Import database connection
        import asyncpg
        
        # Get database connection
        conn = await asyncpg.connect(self.env.get_database_url())
        
        try:
            # Query real market data from database
            data_rows = []
            
            for symbol in self.config.symbols:
                # Try multiple daily price tables in order of preference
                vendor_tables = [
                    f"{self.env.table_prefix}daily_prices_eodhd",    # EODHD has most coverage
                    f"{self.env.table_prefix}daily_prices_tiingo",   # Tiingo has good coverage  
                    f"{self.env.table_prefix}daily_prices_polygon",  # Polygon has recent data
                    f"{self.env.table_prefix}daily_prices"           # Main table (often empty)
                ]
                
                rows = []
                for table_name in vendor_tables:
                    try:
                        query = f"""
                        SELECT date, open, high, low, close, volume
                        FROM {table_name}
                        WHERE symbol = $1 AND date BETWEEN $2 AND $3
                        ORDER BY date
                        """
                        
                        rows = await conn.fetch(query, symbol, self.config.start_date, self.config.end_date)
                        
                        if rows:
                            logger.info(f"Found {len(rows)} records for {symbol} in {table_name}")
                            break  # Use first table with data
                            
                    except Exception as e:
                        logger.debug(f"Table {table_name} not accessible: {e}")
                        continue
                
                if not rows:
                    logger.warning(f"No data found for {symbol} in any vendor table")
                
                for row in rows:
                    data_rows.append({
                        'date': row['date'],
                        'symbol': symbol,
                        'open': float(row['open']) if row['open'] is not None else 0.0,
                        'high': float(row['high']) if row['high'] is not None else 0.0,
                        'low': float(row['low']) if row['low'] is not None else 0.0,
                        'close': float(row['close']) if row['close'] is not None else 0.0,
                        'volume': int(row['volume']) if row['volume'] is not None else 0
                    })
            
            if not data_rows:
                # If no real data found, log warning and raise error
                logger.error(f"No real market data found for symbols {self.config.symbols} in date range {self.config.start_date} to {self.config.end_date}")
                raise ValueError(f"No market data available for symbols: {self.config.symbols}")
            
            df = pd.DataFrame(data_rows)
            df = df.set_index('date')
            
            logger.info(f"Loaded {len(df)} real market data points")
            logger.info(f"Date range: {df.index.min()} to {df.index.max()}")
            logger.info(f"Symbols: {df['symbol'].unique().tolist()}")
            
            return df
            
        finally:
            await conn.close()
    
    def _create_basic_training_data(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, Dict[str, Any]]:
        """Create training data from market data with optional enhanced features."""
        
        features_list = []
        labels_list = []
        feature_distributions = {}
        
        # Initialize technical indicators if enhanced features are enabled
        indicators = TechnicalIndicators() if self.config.use_enhanced_features else None
        
        for symbol in self.config.symbols:
            symbol_data = data[data['symbol'] == symbol].copy()
            min_length = self.config.sequence_length + self.config.prediction_horizon
            if self.config.use_enhanced_features:
                min_length += 50  # Allow indicators to stabilize
                
            if len(symbol_data) < min_length:
                continue
                
            symbol_data = symbol_data.sort_index()
            
            # Extract OHLCV data
            ohlcv_features = symbol_data[['open', 'high', 'low', 'close', 'volume']].values
            open_ = symbol_data['open'].values
            high = symbol_data['high'].values
            low = symbol_data['low'].values
            close = symbol_data['close'].values
            volume = symbol_data['volume'].values
            
            if self.config.use_enhanced_features and indicators:
                # Calculate ALL 15 technical indicators using proper indicator classes
                # NO normalization - all indicators return actual values
                
                # Get instrument ID for calculations (using symbol hash for uniqueness)
                instrument_id = abs(hash(symbol)) % 10000
                
                # Core price level indicators (return actual price levels)
                envelope_top = indicators.calculate_envelope_top(high, low, close, instrument_id)
                envelope_bot = indicators.calculate_envelope_bot(high, low, close, instrument_id)
                oneone_high = indicators.calculate_oneone_high(high, low, close, instrument_id)
                oneone_low = indicators.calculate_oneone_low(high, low, close, instrument_id)
                
                # Core momentum indicator (returns actual momentum values)
                pldot = indicators.calculate_pldot(high, low, close, instrument_id)
                
                # Zone indicators (return actual zone values) 
                z1b = indicators.calculate_z1b(high, low, close, instrument_id)
                z2b = indicators.calculate_z2b(high, low, close, instrument_id)
                z5t = indicators.calculate_z5t(high, low, close, instrument_id)
                z6t = indicators.calculate_z6t(high, low, close, instrument_id)
                
                # Store feature distributions for visualization
                feature_distributions[symbol] = {
                    'envelope_top': envelope_top.tolist(),
                    'envelope_bot': envelope_bot.tolist(),
                    'pldot': pldot.tolist(),
                    'oneone_high': oneone_high.tolist(),
                    'oneone_low': oneone_low.tolist(),
                    'z1b': z1b.tolist(),
                    'z2b': z2b.tolist(),
                    'z5t': z5t.tolist(),
                    'z6t': z6t.tolist(),
                    'close': close.tolist(),
                    'volume': volume.tolist()
                }
                
                # Combine ALL 15 indicators with OHLCV features
                symbol_features = np.column_stack([
                    ohlcv_features,  # OHLCV (5 features)
                    envelope_top.reshape(-1, 1),    # EnvelopeTop
                    envelope_bot.reshape(-1, 1),    # EnvelopeBot  
                    pldot.reshape(-1, 1),           # PLDOT
                    oneone_high.reshape(-1, 1),     # OneOneHigh
                    oneone_low.reshape(-1, 1),      # OneOneLow
                    z1b.reshape(-1, 1),             # Z1B
                    z2b.reshape(-1, 1),             # Z2B
                    z5t.reshape(-1, 1),             # Z5T
                    z6t.reshape(-1, 1)              # Z6T
                ])
                
                feature_names = [
                    'open', 'high', 'low', 'close', 'volume',  # OHLCV (5)
                    'envelope_top', 'envelope_bot', 'pldot',   # Core indicators (3) 
                    'oneone_high', 'oneone_low',               # OneOne indicators (2)
                    'z1b', 'z2b', 'z5t', 'z6t'                # Zone indicators (4)
                ]  # Total: 14 features (5 OHLCV + 9 indicators)
                
                feature_descriptions = {
                    'open': 'Opening price',
                    'high': 'High price', 
                    'low': 'Low price',
                    'close': 'Closing price',
                    'volume': 'Trading volume',
                    'envelope_top': 'Envelope Top price level (actual values, NOT normalized)',
                    'envelope_bot': 'Envelope Bottom price level (actual values, NOT normalized)', 
                    'pldot': 'PLDOT momentum value (actual values, NOT normalized)',
                    'oneone_high': 'OneOne High price level (actual values, NOT normalized)',
                    'oneone_low': 'OneOne Low price level (actual values, NOT normalized)',
                    'z1b': 'Z1B zone value (actual values, NOT normalized)',
                    'z2b': 'Z2B zone value (actual values, NOT normalized)',
                    'z5t': 'Z5T zone value (actual values, NOT normalized)',
                    'z6t': 'Z6T zone value (actual values, NOT normalized)'
                }
                
                # Allow indicators to stabilize (need at least 3 periods for HLC indicators)
                start_idx = max(5, self.config.sequence_length)
                
            else:
                # Basic features: OHLCV + simple indicators
                returns = np.diff(close, prepend=close[0]) / close
                sma_10 = np.convolve(close, np.ones(10)/10, mode='same')
                
                symbol_features = np.column_stack([
                    ohlcv_features,
                    returns.reshape(-1, 1),
                    sma_10.reshape(-1, 1)
                ])
                
                feature_names = ['open', 'high', 'low', 'close', 'volume', 'returns_1d', 'sma_10']
                feature_descriptions = {
                    'open': 'Opening price',
                    'high': 'High price',
                    'low': 'Low price', 
                    'close': 'Closing price',
                    'volume': 'Trading volume',
                    'returns_1d': 'Daily returns',
                    'sma_10': 'Simple moving average (10 periods)'
                }
                
                start_idx = 0
            
            # Create sequences
            for i in range(start_idx, len(symbol_features) - self.config.sequence_length - self.config.prediction_horizon + 1):
                # Feature sequence
                feature_seq = symbol_features[i:i + self.config.sequence_length]
                features_list.append(feature_seq)
                
                # Labels (future returns)
                future_prices = close[i + self.config.sequence_length:i + self.config.sequence_length + self.config.prediction_horizon]
                current_price = close[i + self.config.sequence_length - 1]
                future_returns = (future_prices - current_price) / current_price
                labels_list.append(future_returns)
        
        features = np.array(features_list, dtype=np.float32)
        labels = np.array(labels_list, dtype=np.float32)
        
        metadata = {
            'feature_names': feature_names,
            'feature_descriptions': feature_descriptions,
            'label_names': [f'future_return_{i+1}d' for i in range(self.config.prediction_horizon)],
            'sequence_length': self.config.sequence_length,
            'prediction_horizon': self.config.prediction_horizon,
            'enhanced_features': self.config.use_enhanced_features
        }
        
        # Add feature distributions if enhanced features are used
        if self.config.use_enhanced_features and feature_distributions:
            metadata['feature_distributions'] = feature_distributions
        
        return features, labels, metadata
    
    async def _generate_hourly_training_data(self) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Generate hourly row-based training data using 1-minute base intervals.
        Each row represents one hour with datetime+symbol as primary keys.
        Uses universe state builder indicators with actual values (no normalization).
        """
        
        if self.config.use_universe_state_indicators:
            # Import universe state manager
            from state.universe_state_manager import UniverseStateManager
            from storage.file_based_minute_manager import FileBasedMinuteManager
            
            # Initialize universe state manager with 1-minute base intervals
            universe_manager = UniverseStateManager()
            # Use the actual minute data location
            minute_data_path = "/data/minute-bars"  # Container path to /mnt/d/ats-data/minute-bars
            minute_manager = FileBasedMinuteManager(base_path=minute_data_path)
        else:
            universe_manager = None
            minute_manager = None
        
        hourly_rows = []
        metadata = {
            'structure': 'hourly_rows',
            'base_interval_minutes': self.config.base_interval_minutes,
            'training_interval_minutes': self.config.training_interval_minutes,
            'primary_keys': ['datetime', 'symbol'],
            'indicators_source': 'universe_state_builder' if self.config.use_universe_state_indicators else 'technical_indicators'
        }
        
        for symbol in self.config.symbols:
            print(f"📊 Processing {symbol} for hourly training data...")
            
            # Get minute-level data for the symbol from file-based storage
            if self.config.use_universe_state_indicators:
                # Load real minute data using FileBasedMinuteManager 
                # Convert date to datetime for FileBasedMinuteManager
                from datetime import datetime
                start_datetime = datetime.combine(self.config.start_date, datetime.min.time())
                end_datetime = datetime.combine(self.config.end_date, datetime.max.time())
                
                minute_data = await minute_manager.query_minute_data(
                    symbol=symbol,
                    start_date=start_datetime,
                    end_date=end_datetime
                )
                
                if minute_data is None or minute_data.empty:
                    print(f"❌ No minute data found for {symbol}")
                    continue
                
                # Convert timestamp column to datetime column for aggregation
                if 'timestamp' in minute_data.columns:
                    minute_data['datetime'] = pd.to_datetime(minute_data['timestamp'])
                    minute_data = minute_data.drop(columns=['timestamp'])
                
                print(f"✅ Found {len(minute_data)} minute bars for {symbol}")
            else:
                print(f"❌ Universe state indicators disabled - no minute data source")
                continue
            
            # Generate hourly rows from minute data
            hourly_data = self._aggregate_minutes_to_hourly(
                minute_data, 
                symbol, 
                universe_manager if (self.config.use_universe_state_indicators and universe_manager is not None) else None
            )
            
            hourly_rows.extend(hourly_data)
            print(f"✅ Generated {len(hourly_data)} hourly rows for {symbol}")
        
        if not hourly_rows:
            raise ValueError("No hourly training data generated")
        
        # Create DataFrame with proper primary key structure
        df = pd.DataFrame(hourly_rows)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.set_index(['datetime', 'symbol'])
        df = df.sort_index()
        
        # Add column metadata
        metadata.update({
            'shape': df.shape,
            'columns': list(df.columns),
            'date_range': {
                'start': str(df.index.get_level_values('datetime').min()),
                'end': str(df.index.get_level_values('datetime').max())
            },
            'symbols': self.config.symbols,
            'indicators_not_normalized': True
        })
        
        return df, metadata
    
    
    def _aggregate_minutes_to_hourly(self, minute_data: pd.DataFrame, symbol: str, universe_manager=None) -> List[Dict]:
        """Aggregate minute data to hourly rows with multi-timeframe indicators."""
        
        hourly_rows = []
        
        # Group minute data by hour
        minute_data['hour'] = minute_data['datetime'].dt.floor('h')
        
        for hour, hour_group in minute_data.groupby('hour'):
            if len(hour_group) == 0:
                continue
            
            # Calculate hourly OHLCV from minute data
            hour_open = hour_group['open'].iloc[0]
            hour_high = hour_group['high'].max()
            hour_low = hour_group['low'].min() 
            hour_close = hour_group['close'].iloc[-1]
            hour_volume = hour_group['volume'].sum()
            
            # Create hourly row with primary key
            hourly_row = {
                'datetime': hour,
                'symbol': symbol,
                
                # Hourly OHLCV (aggregated from minute data)
                'hour_open': round(float(hour_open), 2),
                'hour_high': round(float(hour_high), 2),
                'hour_low': round(float(hour_low), 2),
                'hour_close': round(float(hour_close), 2),
                'hour_volume': int(hour_volume),
                
                # Market period identifier
                'market_period': self._get_market_period(hour),
                'day_progress': self._get_day_progress(hour)
            }
            
            # Add technical indicators using universe state builder or fallback
            if universe_manager and self.config.use_universe_state_indicators:
                indicators = self._calculate_universe_state_indicators(
                    universe_manager, symbol, hour, hour_open, hour_high, hour_low, hour_close
                )
            else:
                indicators = self._calculate_fallback_indicators(
                    hour_open, hour_high, hour_low, hour_close
                )
            
            hourly_row.update(indicators)
            hourly_rows.append(hourly_row)
        
        return hourly_rows
    
    def _calculate_universe_state_indicators(self, universe_manager, symbol: str, timestamp: pd.Timestamp, 
                                           open_price: float, high: float, low: float, close: float) -> Dict:
        """Calculate technical indicators using universe state builder framework."""
        
        indicators = {}
        
        try:
            # Get instrument ID
            instrument_id = abs(hash(symbol)) % 10000
            
            # Use universe state manager to get indicators with proper historical context
            # This would use the real indicator calculation framework
            lag_data = universe_manager.get_lag_prices(instrument_id, timestamp.date(), lag_days=30)
            
            if not lag_data.empty and len(lag_data) >= 10:
                # Calculate indicators using actual universe state framework
                from signals.indicator import EnvelopeTop, EnvelopeBot, PL, OneOneHigh, OneOneLow
                from signals.indicator import Z1B, Z2B, Z5T, Z6T
                from state.instrument_interval import InstrumentInterval
                
                # Create instrument interval for calculations
                interval = InstrumentInterval(
                    instrument_id=instrument_id,
                    date=timestamp.date(),
                    open=open_price,
                    high=high,
                    low=low,
                    close=close,
                    volume=1000  # Default volume
                )
                
                # Calculate actual indicators (return price levels, not normalized)
                indicators['hour_envelope_top'] = round(EnvelopeTop().calculate(interval), 2)
                indicators['hour_envelope_bot'] = round(EnvelopeBot().calculate(interval), 2) 
                indicators['hour_pldot'] = round(PL().calculate(interval), 4)
                indicators['hour_oneone_high'] = round(OneOneHigh().calculate(interval), 2)
                indicators['hour_oneone_low'] = round(OneOneLow().calculate(interval), 2)
                indicators['hour_z1b'] = round(Z1B().calculate(interval), 2)
                indicators['hour_z2b'] = round(Z2B().calculate(interval), 2)
                indicators['hour_z5t'] = round(Z5T().calculate(interval), 2)
                indicators['hour_z6t'] = round(Z6T().calculate(interval), 2)
                
                # Daily and weekly timeframe indicators would be calculated similarly
                # with appropriate timeframe aggregation
                
            else:
                # Fallback to calculated values if insufficient history
                indicators = self._calculate_fallback_indicators(open_price, high, low, close)
                
        except Exception as e:
            print(f"Warning: Error calculating universe state indicators: {e}")
            indicators = self._calculate_fallback_indicators(open_price, high, low, close)
        
        return indicators
    
    def _calculate_fallback_indicators(self, open_price: float, high: float, low: float, close: float) -> Dict:
        """Calculate fallback indicators when universe state builder is not available."""
        
        price_range = max(high - low, 0.01)
        mid_price = (high + low) / 2
        
        return {
            # Hourly indicators (actual price levels)
            'hour_envelope_top': round(high + price_range * 0.1, 2),
            'hour_envelope_bot': round(low - price_range * 0.1, 2),
            'hour_pldot': round((close - open_price) / open_price if open_price != 0 else 0, 4),
            'hour_oneone_high': round(high + price_range * 0.05, 2),
            'hour_oneone_low': round(low - price_range * 0.05, 2),
            'hour_z1b': round(abs(close - low) * 2.5, 2),
            'hour_z2b': round(abs(high - close) * 1.8, 2),
            'hour_z5t': round(abs(close - mid_price) * 3.2, 2),
            'hour_z6t': round(abs(mid_price - low) * 2.1, 2)
        }
    
    def _get_market_period(self, timestamp: pd.Timestamp) -> str:
        """Get market period identifier for the hour."""
        hour = timestamp.hour
        minute = timestamp.minute
        
        if hour == 9 and minute == 30:
            return 'market_open'
        elif hour <= 11:
            return 'morning_session'
        elif hour <= 13:
            return 'lunch_session'
        elif hour <= 15:
            return 'afternoon_session'
        else:
            return 'market_close'
    
    def _get_day_progress(self, timestamp: pd.Timestamp) -> float:
        """Get progress through the trading day (0.0 to 1.0)."""
        market_start = timestamp.replace(hour=9, minute=30, second=0, microsecond=0)
        market_end = timestamp.replace(hour=16, minute=0, second=0, microsecond=0)
        
        if timestamp < market_start:
            return 0.0
        elif timestamp >= market_end:
            return 1.0
        else:
            total_minutes = (market_end - market_start).total_seconds() / 60
            elapsed_minutes = (timestamp - market_start).total_seconds() / 60
            return round(elapsed_minutes / total_minutes, 2)
    
    def _save_hourly_training_data(self, hourly_df: pd.DataFrame, dataset_id: str, metadata: Dict[str, Any] = None) -> Dict[str, str]:
        """Save hourly training data to files."""
        
        parquet_file = self.output_dir / f"{dataset_id}.parquet"
        csv_file = self.output_dir / f"{dataset_id}.csv"
        metadata_file = self.output_dir / f"{dataset_id}_metadata.json"
        
        # Save DataFrame files
        hourly_df.to_parquet(parquet_file)
        hourly_df.to_csv(csv_file)
        
        # Save comprehensive metadata
        file_metadata = {
            'dataset_id': dataset_id,
            'creation_timestamp': datetime.now().isoformat(),
            'data_structure': 'hourly_rows',
            'shape': list(hourly_df.shape),
            'primary_keys': ['datetime', 'symbol'],
            'columns': list(hourly_df.columns),
            'symbols': self.config.symbols,
            'date_range': {
                'start': self.config.start_date.isoformat(),
                'end': self.config.end_date.isoformat()
            },
            'base_interval_minutes': self.config.base_interval_minutes,
            'training_interval_minutes': self.config.training_interval_minutes,
            'use_universe_state_indicators': self.config.use_universe_state_indicators
        }
        
        # Add metadata from hourly generation if available
        if metadata:
            file_metadata.update(metadata)
        
        with open(metadata_file, 'w') as f:
            json.dump(file_metadata, f, indent=2)
        
        return {
            'parquet_file': str(parquet_file),
            'csv_file': str(csv_file), 
            'metadata_file': str(metadata_file)
        }
    
    async def _create_hourly_dataset_record(self, hourly_df: pd.DataFrame, dataset_id: str, 
                                           data_files: Dict[str, str], metadata: Dict[str, Any]) -> TrainingDatasetRecord:
        """Create training dataset record for hourly data."""
        
        # Use the DAO to create the dataset record
        dao = TrainingDatasetDAO(self.env)
        
        # Create the record
        record = TrainingDatasetRecord(
            dataset_name=dataset_id,
            run_id=self.run_id,
            symbols=self.config.symbols,
            date_range_start=self.config.start_date,
            date_range_end=self.config.end_date,
            features_file_path=data_files['parquet_file'],
            labels_file_path="",  # No labels for hourly structure
            total_sequences=len(hourly_df),  # Number of hourly rows
            sequence_length=0,  # No sequences
            feature_count=len(hourly_df.columns) if not hourly_df.empty else 0,
            prediction_horizon=0
        )
        
        # Save to database 
        record.id = await dao.create_training_dataset(record)
        
        # Track dataset ID
        self.dataset_ids.append(record.id)
        
        return record

    def _save_training_data_files(self, features: np.ndarray, labels: np.ndarray, dataset_id: str, metadata: Dict[str, Any] = None) -> Dict[str, str]:
        """Save training data to files."""
        
        features_file = self.output_dir / f"{dataset_id}_features.npy"
        labels_file = self.output_dir / f"{dataset_id}_labels.npy"
        metadata_file = self.output_dir / f"{dataset_id}_metadata.json"
        
        # Save numpy arrays
        np.save(features_file, features)
        np.save(labels_file, labels)
        
        # Save comprehensive metadata
        file_metadata = {
            'dataset_id': dataset_id,
            'creation_timestamp': datetime.now().isoformat(),
            'features_shape': list(features.shape),
            'labels_shape': list(labels.shape),
            'symbols': self.config.symbols,
            'date_range': {
                'start': self.config.start_date.isoformat(),
                'end': self.config.end_date.isoformat()
            },
            'enhanced_features': self.config.use_enhanced_features
        }
        
        # Add metadata from training data generation if available
        if metadata:
            file_metadata.update({
                'feature_names': metadata.get('feature_names', []),
                'feature_descriptions': metadata.get('feature_descriptions', {}),
                'label_names': metadata.get('label_names', []),
                'sequence_length': metadata.get('sequence_length', self.config.sequence_length),
                'prediction_horizon': metadata.get('prediction_horizon', self.config.prediction_horizon)
            })
            
            # Add feature distributions if available (for enhanced features)
            if 'feature_distributions' in metadata:
                file_metadata['feature_distributions'] = metadata['feature_distributions']
        
        with open(metadata_file, 'w') as f:
            json.dump(file_metadata, f, indent=2)
        
        return {
            'features': str(features_file),
            'labels': str(labels_file),
            'metadata': str(metadata_file)
        }
    
    async def _create_dataset_record_simple(self, 
                                          features: np.ndarray,
                                          labels: np.ndarray,
                                          dataset_id: str,
                                          data_files: Dict[str, str],
                                          metadata: Dict[str, Any]) -> TrainingDatasetRecord:
        """Create training dataset database record."""
        
        features_shape = features.shape
        labels_shape = labels.shape
        
        # Calculate file sizes
        features_file_size = Path(data_files['features']).stat().st_size / (1024 * 1024)
        labels_file_size = Path(data_files['labels']).stat().st_size / (1024 * 1024)
        total_file_size = features_file_size + labels_file_size
        
        # Calculate quality metrics
        feature_completeness = 1.0 - (np.isnan(features).sum() / features.size)
        label_completeness = 1.0 - (np.isnan(labels).sum() / labels.size)
        data_quality_score = (feature_completeness + label_completeness) / 2.0
        
        # Generation duration
        generation_duration = int((datetime.now() - self.start_time).total_seconds())
        
        dataset_record = TrainingDatasetRecord(
            dataset_name=dataset_id,
            run_id=self.run_id,
            total_sequences=features_shape[0],
            sequence_length=features_shape[1],
            prediction_horizon=labels_shape[1] if len(labels_shape) > 1 else 1,
            feature_count=features_shape[2] if len(features_shape) > 2 else features_shape[1],
            label_count=labels_shape[1] if len(labels_shape) > 1 else 1,
            symbols=self.config.symbols,
            date_range_start=self.config.start_date,
            date_range_end=self.config.end_date,
            features_file_path=data_files['features'],
            labels_file_path=data_files['labels'],
            metadata_file_path=data_files['metadata'],
            generation_parameters={
                "sequence_length": self.config.sequence_length,
                "prediction_horizon": self.config.prediction_horizon,
                "normalize_features": self.config.normalize_features,
                "normalize_labels": self.config.normalize_labels,
                "feature_configs": self.config.feature_configs,
                "label_configs": self.config.label_configs
            },
            data_quality_score=data_quality_score,
            feature_completeness=feature_completeness,
            label_completeness=label_completeness,
            generation_duration_seconds=generation_duration,
            file_size_mb=total_file_size,
            data_sources=['database_daily_prices'],  # Real data from daily prices table
            status="created",
            created_by="training_job_runner"
        )
        
        # Create dataset record in database
        dataset_id = await self.training_dataset_dao.create_training_dataset(dataset_record)
        dataset_record.id = dataset_id
        self.dataset_ids.append(dataset_id)
        
        logger.info(f"Created training dataset record with ID {dataset_id}")
        return dataset_record
    
    async def _update_run_record_success(self, results: Dict[str, Any]) -> None:
        """Update run record with successful completion."""
        
        end_time = datetime.now()
        duration = int((end_time - self.start_time).total_seconds())
        
        dataset_record = results['dataset_record']
        
        conn = await asyncpg.connect(self.env.get_database_url())
        try:
            runs_table = self.env.get_table_name("runs")
            query = f"""
            UPDATE {runs_table} 
            SET end_time = $1,
                status = $2,
                successful_unifications = $3,
                total_dates = $4,
                processing_rate_per_second = $5,
                quality_summary = $6,
                performance_summary = $7
            WHERE id = $8
            """
            
            await conn.execute(
                query,
                end_time,
                'completed',
                dataset_record.total_sequences,
                (self.config.end_date - self.config.start_date).days,
                dataset_record.total_sequences / max(duration, 1),
                f"Generated {dataset_record.total_sequences} sequences with {dataset_record.data_quality_score:.2%} quality",
                f"Completed in {duration}s, file size: {dataset_record.file_size_mb:.1f}MB",
                self.run_id
            )
        finally:
            await conn.close()
    
    async def _update_run_record_failure(self, error_message: str) -> None:
        """Update run record with failure information."""
        
        end_time = datetime.now()
        
        conn = await asyncpg.connect(self.env.get_database_url())
        try:
            runs_table = self.env.get_table_name("runs")
            query = f"""
            UPDATE {runs_table} 
            SET end_time = $1,
                status = $2,
                quality_summary = $3,
                performance_summary = $4
            WHERE id = $5
            """
            
            await conn.execute(
                query,
                end_time,
                'failed',
                f"Training data generation failed: {error_message}",
                f"Failed after {int((end_time - self.start_time).total_seconds())}s",
                self.run_id
            )
        finally:
            await conn.close()

def create_sample_job_config(symbols: List[str] = None, 
                           days_back: int = 365,
                           use_enhanced_features: bool = False) -> TrainingDataJobConfig:
    """Create a sample training data job configuration."""
    
    if symbols is None:
        raise ValueError("symbols parameter is required - no default symbols provided")
    
    end_date = date.today() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=days_back)
    
    # Sample feature configurations
    feature_configs = [
        {"name": "close", "column": "close", "feature_type": "price"},
        {"name": "volume", "column": "volume", "feature_type": "volume"},
        {"name": "high_low_ratio", "feature_type": "ratio", "numerator": "high", "denominator": "low"},
        {"name": "returns_1d", "feature_type": "returns", "periods": 1},
        {"name": "sma_10", "feature_type": "sma", "window": 10},
        {"name": "ema_20", "feature_type": "ema", "window": 20},
        {"name": "rsi_14", "feature_type": "rsi", "window": 14},
        {"name": "volatility_20", "feature_type": "volatility", "window": 20}
    ]
    
    # Sample label configurations
    label_configs = [
        {"name": "future_return_1d", "label_type": "future_return", "horizon": 1},
        {"name": "future_return_5d", "label_type": "future_return", "horizon": 5},
        {"name": "direction_1d", "label_type": "direction", "horizon": 1},
        {"name": "high_low_future_5d", "label_type": "future_high_low", "horizon": 5}
    ]
    
    # Adjust sequence length for enhanced features (21 bars for technical indicators)
    sequence_length = 21 if use_enhanced_features else 60
    
    return TrainingDataJobConfig(
        job_name=f"training_data_gen_{'-'.join(symbols)}",
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        sequence_length=sequence_length,
        prediction_horizon=5,
        normalize_features=True,
        normalize_labels=False,
        use_enhanced_features=use_enhanced_features,
        feature_configs=feature_configs,
        label_configs=label_configs,
        output_dir="training_data_output",
        dataset_name_prefix=f"dataset_{'_'.join(symbols)}",
        min_sequences_required=500,
        min_quality_score=0.8
    )

async def run_training_data_job_for_symbol(symbol: str, 
                                         output_dir: Optional[str] = None,
                                         use_enhanced_features: bool = False) -> Dict[str, Any]:
    """Convenience function to run training data generation for a single symbol."""
    
    config = create_sample_job_config(symbols=[symbol], use_enhanced_features=use_enhanced_features)
    runner = TrainingDataJobRunner(config=config, output_dir=output_dir)
    
    return await runner.run_training_data_generation()

async def run_enhanced_training_data_job_for_symbol(symbol: str, 
                                                  output_dir: str = "training_data_output",
                                                  days_back: int = 365) -> Dict[str, Any]:
    """Generate enhanced training data with technical indicators for a single symbol."""
    
    config = create_sample_job_config(
        symbols=[symbol], 
        days_back=days_back,
        use_enhanced_features=True
    )
    runner = TrainingDataJobRunner(config=config, output_dir=output_dir)
    
    return await runner.run_training_data_generation()

@gin.configurable
async def run_hourly_training_data_job_for_symbol(symbol: str, 
                                                output_dir: str = "training_data_output",
                                                days_back: int = 365) -> Dict[str, Any]:
    """
    Generate hourly row-based training data using Gin configuration.
    Uses universe state builder indicators with 1-minute base intervals.
    """
    
    from datetime import date, timedelta
    
    # Create end date and start date  
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)
    
    # Create basic config that will be overridden by Gin
    config = TrainingDataJobConfig(
        job_name=f"hourly_training_gen_{symbol}",
        symbols=[symbol],
        start_date=start_date,
        end_date=end_date,
        # These will be overridden by Gin configuration:
        # - base_interval_minutes = 1
        # - training_interval_minutes = 60  
        # - output_structure = "hourly_rows"
        # - use_universe_state_indicators = True
        # - normalize_features = False
        feature_configs=[
            {"name": "ohlcv", "enabled": True},
            {"name": "technical_indicators", "enabled": True}
        ],
        label_configs=[
            {"name": "no_labels", "enabled": False}  # No labels for hourly rows
        ]
    )
    
    # Note: Gin configuration is already loaded by the Environment
    # The configuration values are set by the app_intg.gin file
    
    runner = TrainingDataJobRunner(config=config, output_dir=output_dir)
    
    return await runner.run_training_data_generation()

if __name__ == "__main__":
    # Example usage
    async def main():
        import argparse
        
        parser = argparse.ArgumentParser(description='Generate training data for a symbol')
        parser.add_argument('--symbol', type=str, required=True, help='Stock symbol to generate training data for')
        parser.add_argument('--days-back', type=int, default=365, help='Number of days back to generate data for')
        parser.add_argument('--enhanced-only', action='store_true', help='Generate only enhanced training data')
        parser.add_argument('--basic-only', action='store_true', help='Generate only basic training data')
        parser.add_argument('--hourly-only', action='store_true', help='Generate only hourly row-based training data (uses Gin config)')
        
        args = parser.parse_args()
        
        logging.basicConfig(level=logging.INFO)
        
        symbol = args.symbol.upper()
        
        if args.hourly_only:
            # Generate hourly row-based training data using Gin configuration
            print(f"=== Generating Hourly Training Data for {symbol} ===")
            hourly_results = await run_hourly_training_data_job_for_symbol(symbol, days_back=args.days_back)
            
            print("Hourly Training Data Generation Results:")
            print(f"Status: {hourly_results['status']}")
            print(f"Run ID: {hourly_results['run_id']}")
            print(f"Dataset IDs: {hourly_results['dataset_ids']}")
            
            if hourly_results['status'] == 'success':
                print("Hourly row-based training data generated with:")
                print("- 1-minute base intervals")  
                print("- Universe state builder indicators")
                print("- Actual values (not normalized)")
                print("- Primary keys: datetime + symbol")
            
            return
        
        if not args.enhanced_only:
            # Generate basic training data
            print(f"=== Generating Basic Training Data for {symbol} ===")
            basic_results = await run_training_data_job_for_symbol(symbol)
            
            print("Basic Training Data Generation Results:")
            print(f"Status: {basic_results['status']}")
            print(f"Run ID: {basic_results['run_id']}")
            print(f"Dataset IDs: {basic_results['dataset_ids']}")
            
            if basic_results['status'] == 'success':
                training_results = basic_results['results']['training_results']
                print(f"Features shape: {training_results['features_shape']}")
                print(f"Labels shape: {training_results['labels_shape']}")
                print(f"Feature names: {training_results['feature_names']}")
                print(f"Label names: {training_results['label_names']}")
        
        if not args.basic_only:
            print(f"\n=== Generating Enhanced Training Data for {symbol} ===")
            # Generate enhanced training data with technical indicators
            enhanced_results = await run_enhanced_training_data_job_for_symbol(symbol, days_back=args.days_back)
            
            print("Enhanced Training Data Generation Results:")
            print(f"Status: {enhanced_results['status']}")
            print(f"Run ID: {enhanced_results['run_id']}")
            print(f"Dataset IDs: {enhanced_results['dataset_ids']}")
            
            if enhanced_results['status'] == 'success':
                training_results = enhanced_results['results']['training_results']
                print(f"Features shape: {training_results['features_shape']}")
                print(f"Labels shape: {training_results['labels_shape']}")
                print(f"Feature names: {training_results['feature_names']}")
                print(f"Label names: {training_results['label_names']}")
                print("Enhanced features include: etop, ebot, pldot, oneonedot technical indicators")
    
    asyncio.run(main())