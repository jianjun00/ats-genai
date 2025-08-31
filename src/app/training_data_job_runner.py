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
    """Technical indicators for enhanced training data generation."""
    
    @staticmethod
    def calculate_elliott_top(high: np.ndarray, low: np.ndarray, close: np.ndarray, window: int = 21) -> np.ndarray:
        """Calculate Envelope Top indicator - identifies potential reversal tops."""
        etop = np.zeros_like(close)
        
        for i in range(window, len(close)):
            # Look for local highs within window
            window_high = high[i-window:i+1]
            window_idx = np.argmax(window_high)
            
            # Check if current bar or recent bar is a significant high
            if window_idx >= window - 5:  # Recent high
                strength = (window_high[window_idx] - np.mean(window_high)) / np.std(window_high)
                etop[i] = max(0, strength)
            
        return etop
    
    @staticmethod
    def calculate_elliott_bottom(high: np.ndarray, low: np.ndarray, close: np.ndarray, window: int = 21) -> np.ndarray:
        """Calculate Envelope Bottom indicator - identifies potential reversal bottoms."""
        ebot = np.zeros_like(close)
        
        for i in range(window, len(close)):
            # Look for local lows within window
            window_low = low[i-window:i+1]
            window_idx = np.argmin(window_low)
            
            # Check if current bar or recent bar is a significant low
            if window_idx >= window - 5:  # Recent low
                strength = (np.mean(window_low) - window_low[window_idx]) / np.std(window_low)
                ebot[i] = max(0, strength)
            
        return ebot
    
    @staticmethod
    def calculate_pivot_line_dot(high: np.ndarray, low: np.ndarray, close: np.ndarray, window: int = 21) -> np.ndarray:
        """Calculate Pivot Line Dot indicator - pivot point momentum."""
        pldot = np.zeros_like(close)
        
        for i in range(window, len(close)):
            # Calculate pivot point as (H + L + C) / 3
            pivot = (high[i-1] + low[i-1] + close[i-1]) / 3
            
            # Calculate momentum relative to pivot
            current_price = close[i]
            pivot_momentum = (current_price - pivot) / pivot
            
            # Smooth over window
            window_momentum = []
            for j in range(max(0, i-window), i):
                p = (high[j-1] + low[j-1] + close[j-1]) / 3 if j > 0 else pivot
                m = (close[j] - p) / p if p != 0 else 0
                window_momentum.append(m)
            
            pldot[i] = np.mean(window_momentum) if window_momentum else 0
            
        return pldot
    
    @staticmethod
    def calculate_oneonedot(open_: np.ndarray, high: np.ndarray, low: np.ndarray, close: np.ndarray, window: int = 21) -> np.ndarray:
        """Calculate One-One-Dot indicator - custom momentum oscillator."""
        oneonedot = np.zeros_like(close)
        
        for i in range(window, len(close)):
            # Calculate various momentum metrics
            window_data = close[i-window:i+1]
            
            # Rate of change
            roc = (close[i] - close[i-window]) / close[i-window] if close[i-window] != 0 else 0
            
            # Relative position within recent range
            recent_high = np.max(high[i-window:i+1])
            recent_low = np.min(low[i-window:i+1])
            position = (close[i] - recent_low) / (recent_high - recent_low) if recent_high != recent_low else 0.5
            
            # Trend strength - ensure arrays have same length
            if len(window_data) == window:
                slope = np.polyfit(range(window), window_data, 1)[0]
                trend_strength = slope / np.mean(window_data) if np.mean(window_data) != 0 else 0
            else:
                trend_strength = 0
            
            # Combine metrics
            oneonedot[i] = (roc + (position - 0.5) * 2 + trend_strength) / 3
            
        return oneonedot

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
    sequence_length: int = 60
    prediction_horizon: int = 5
    normalize_features: bool = True
    normalize_labels: bool = False
    use_enhanced_features: bool = True  # Enable enhanced technical indicators
    
    # Multi-scale configuration (NEW)
    enable_multi_scale: bool = False
    scales: List[str] = None  # ['minute', 'hourly', 'daily', 'weekly']
    sequence_length_minute: int = 1440  # 1 day of minutes
    sequence_length_hourly: int = 168   # 1 week of hours
    sequence_length_daily: int = 60     # ~3 months of days
    sequence_length_weekly: int = 12    # ~3 months of weeks
    
    # Event integration (NEW)
    enable_events: bool = False
    enable_llm_events: bool = False  # Feature-gated
    max_events_per_sequence: int = 50
    
    # Agent features (NEW) 
    enable_agent_features: bool = False  # Feature-gated
    
    # Prediction horizons (NEW)
    prediction_horizons: Dict[str, int] = None  # Multiple prediction targets
    
    # Feature and label configuration
    feature_configs: List[Dict[str, Any]] = gin.REQUIRED
    label_configs: List[Dict[str, Any]] = gin.REQUIRED
    
    # Output configuration
    output_dir: str = "training_data_output"
    dataset_name_prefix: str = "dataset"
    output_format: str = "basic"  # "basic", "multi_scale"
    
    # Quality and validation
    min_sequences_required: int = 1000
    min_quality_score: float = 0.8
    
    # Processing configuration
    batch_size: int = 10000
    max_memory_mb: int = 8192
    
    def __post_init__(self):
        """Initialize default values and validate configuration."""
        # Set default scales if multi-scale is enabled
        if self.enable_multi_scale and self.scales is None:
            self.scales = ['hourly', 'daily']
        
        # Set default prediction horizons
        if self.prediction_horizons is None:
            if self.enable_multi_scale:
                self.prediction_horizons = {
                    "short_term": 60,    # 1 hour ahead (minutes)
                    "medium_term": 1440, # 1 day ahead (minutes)  
                    "long_term": 10080   # 1 week ahead (minutes)
                }
            else:
                self.prediction_horizons = {"default": self.prediction_horizon}
        
        # Check feature flag compatibility
        from config.feature_flags import is_enabled
        if self.enable_llm_events and not is_enabled("enable_llm_events"):
            logger.warning("LLM events requested but feature flag disabled - will be ignored")
        if self.enable_agent_features and not is_enabled("enable_agent_networks"):
            logger.warning("Agent features requested but feature flag disabled - will be ignored")

class TrainingDataJobRunner:
    """Training data generation job runner with comprehensive tracking."""
    
    def __init__(self, 
                 config: TrainingDataJobConfig,
                 env: Optional[Environment] = None,
                 output_dir: Optional[str] = None):
        self.config = config
        self.env = env or Environment()
        self.output_dir = Path(output_dir or config.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize DAOs
        self.training_dataset_dao = TrainingDatasetDAO(env=self.env)
        
        # Job state
        self.run_id: Optional[int] = None
        self.start_time: Optional[datetime] = None
        self.dataset_ids: List[int] = []
        
    async def run_training_data_generation(self) -> Dict[str, Any]:
        """Run complete training data generation with tracking."""
        
        self.start_time = datetime.now()
        
        try:
            # Create run record
            self.run_id = await self._create_run_record()
            logger.info(f"Started training data generation run {self.run_id}")
            
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
            
            # Use the actual schema with parameters field for configuration
            query = f"""
            INSERT INTO {runs_table} (
                run_type, start_time, status, parameters
            ) VALUES ($1, $2, $3, $4) RETURNING id
            """
            
            run_id = await conn.fetchval(
                query,
                "training_data_generation",
                self.start_time,
                "running",
                json.dumps(run_config)
            )
            
            return run_id
        finally:
            await conn.close()
    
    async def _generate_training_data(self) -> Dict[str, Any]:
        """Generate training data using basic or multi-scale approach."""
        
        if self.config.enable_multi_scale:
            return await self._generate_multi_scale_training_data()
        else:
            return await self._generate_basic_training_data()
    
    async def _generate_basic_training_data(self) -> Dict[str, Any]:
        """Generate training data using original simplified approach."""
        
        # Load market data
        market_data = await self._load_market_data()
        
        # For simplified implementation, create basic features and labels
        features, labels, metadata = self._create_basic_training_data(market_data)
        
        # Save training data files
        dataset_id = f"dataset_{self.config.job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        data_files = self._save_training_data_files(features, labels, dataset_id, metadata)
        
        # Create training dataset record
        dataset_record = await self._create_dataset_record_simple(
            features, labels, dataset_id, data_files, metadata
        )
        
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
    
    async def _generate_multi_scale_training_data(self) -> Dict[str, Any]:
        """Generate multi-scale training data with events and agent features."""
        
        logger.info("Generating multi-scale training data")
        
        # Convert string scales to TimeScale objects
        from storage.multi_scale_sequence import TimeScale
        scale_map = {
            'minute': TimeScale.MINUTE,
            'hourly': TimeScale.HOURLY,
            'daily': TimeScale.DAILY, 
            'weekly': TimeScale.WEEKLY
        }
        scales = [scale_map[s] for s in self.config.scales if s in scale_map]
        
        all_sequences = []
        
        for symbol in self.config.symbols:
            logger.info(f"Processing {symbol} for multi-scale generation")
            
            # Generate multi-scale sequence for this symbol
            sequence = await self._generate_multi_scale_sequence(symbol, scales)
            
            # Create training sequences from multi-scale data
            training_sequences = self._create_training_sequences_from_multi_scale(sequence)
            all_sequences.extend(training_sequences)
            
            logger.info(f"Generated {len(training_sequences)} sequences for {symbol}")
        
        if not all_sequences:
            raise ValueError("No training sequences generated")
        
        # Convert to arrays
        features_list = [seq['features'] for seq in all_sequences]
        features = np.stack(features_list)
        
        # Handle multiple prediction horizons
        label_names = list(self.config.prediction_horizons.keys())
        labels = np.array([
            [seq['labels'].get(label_name, 0.0) for label_name in label_names] 
            for seq in all_sequences
        ])
        
        # Create metadata
        metadata = {
            'feature_names': self._get_multi_scale_feature_names(),
            'label_names': label_names,
            'scales': self.config.scales,
            'prediction_horizons': self.config.prediction_horizons,
            'symbols': self.config.symbols,
            'events_enabled': self.config.enable_events,
            'agent_features_enabled': self.config.enable_agent_features,
            'llm_events_enabled': self.config.enable_llm_events,
            'total_sequences': len(all_sequences)
        }
        
        # Save training data files
        dataset_id = f"multi_scale_{self.config.job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        data_files = self._save_training_data_files(features, labels, dataset_id, metadata)
        
        # Create training dataset record
        dataset_record = await self._create_dataset_record_simple(
            features, labels, dataset_id, data_files, metadata
        )
        
        return {
            "dataset_record": dataset_record,
            "training_results": {
                "features_shape": features.shape,
                "labels_shape": labels.shape,
                "dataset_id": dataset_id,
                "feature_names": metadata['feature_names'],
                "label_names": metadata['label_names'],
                "multi_scale_metadata": metadata
            }
        }
    
    async def _load_market_data(self) -> pd.DataFrame:
        """Load market data for training data generation."""
        
        # For this implementation, we'll create synthetic market data
        # In real implementation, this would query the dev_daily_prices table
        
        logger.info(f"Loading market data for symbols: {self.config.symbols}")
        logger.info(f"Date range: {self.config.start_date} to {self.config.end_date}")
        
        # Generate synthetic OHLCV data
        data_rows = []
        
        for symbol in self.config.symbols:
            # Generate date range
            current_date = self.config.start_date
            base_price = 100.0 + np.random.uniform(-20, 20)  # Base price around $100
            
            while current_date <= self.config.end_date:
                # Skip weekends
                if current_date.weekday() < 5:  # Monday = 0, Sunday = 6
                    # Generate realistic OHLCV data
                    daily_return = np.random.normal(0.001, 0.02)  # ~0.1% daily return, 2% volatility
                    base_price *= (1 + daily_return)
                    
                    # Create intraday high/low
                    daily_range = base_price * np.random.uniform(0.005, 0.03)  # 0.5-3% daily range
                    high = base_price + daily_range / 2
                    low = base_price - daily_range / 2
                    
                    # Open and close within range
                    open_price = np.random.uniform(low, high)
                    close_price = base_price  # Use base_price as close
                    
                    # Volume
                    volume = int(np.random.lognormal(15, 1))  # Log-normal distribution for volume
                    
                    data_rows.append({
                        'date': current_date,
                        'symbol': symbol,
                        'open': round(open_price, 2),
                        'high': round(high, 2),
                        'low': round(low, 2),
                        'close': round(close_price, 2),
                        'volume': volume
                    })
                
                current_date += timedelta(days=1)
        
        df = pd.DataFrame(data_rows)
        df = df.set_index('date')
        
        logger.info(f"Generated {len(df)} market data points")
        logger.info(f"Date range: {df.index.min()} to {df.index.max()}")
        logger.info(f"Symbols: {df['symbol'].unique()}")
        
        return df
    
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
                # Calculate enhanced technical indicators
                envelope_top = indicators.calculate_elliott_top(high, low, close, 21)
                envelope_bot = indicators.calculate_elliott_bottom(high, low, close, 21)
                pldot = indicators.calculate_pivot_line_dot(high, low, close, 21)
                oneonedot = indicators.calculate_oneonedot(open_, high, low, close, 21)
                
                # Store feature distributions for visualization
                feature_distributions[symbol] = {
                    'envelope_top': envelope_top.tolist(),
                    'envelope_bot': envelope_bot.tolist(),
                    'pldot': pldot.tolist(),
                    'oneonedot': oneonedot.tolist(),
                    'close': close.tolist(),
                    'volume': volume.tolist()
                }
                
                # Combine all features with enhanced indicators
                symbol_features = np.column_stack([
                    ohlcv_features,  # OHLCV
                    envelope_top.reshape(-1, 1),
                    envelope_bot.reshape(-1, 1),
                    pldot.reshape(-1, 1),
                    oneonedot.reshape(-1, 1)
                ])
                
                feature_names = ['open', 'high', 'low', 'close', 'volume', 'envelope_top', 'envelope_bot', 'pldot', 'oneonedot']
                feature_descriptions = {
                    'open': 'Opening price',
                    'high': 'High price', 
                    'low': 'Low price',
                    'close': 'Closing price',
                    'volume': 'Trading volume',
                    'envelope_top': 'Envelope Top reversal indicator (21 periods)',
                    'envelope_bot': 'Envelope Bottom reversal indicator (21 periods)',
                    'pldot': 'Pivot Line Dot momentum indicator (21 periods)',
                    'oneonedot': 'One-One-Dot custom momentum oscillator (21 periods)'
                }
                
                # Allow indicators to stabilize
                start_idx = max(50, self.config.sequence_length)
                
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
            data_sources=['synthetic'],  # In real implementation, would track actual sources
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
            # Create summary for parameters field
            summary = {
                'end_time': end_time.isoformat(),
                'status': 'completed',
                'total_sequences': getattr(dataset_record, 'total_sequences', 0),
                'duration_seconds': duration,
                'file_size_mb': getattr(dataset_record, 'file_size_mb', 0),
                'data_quality_score': getattr(dataset_record, 'data_quality_score', 1.0),
                'processing_rate': getattr(dataset_record, 'total_sequences', 0) / max(duration, 1)
            }
            
            query = f"""
            UPDATE {runs_table} 
            SET end_time = $1,
                status = $2,
                parameters = $3
            WHERE id = $4
            """
            
            await conn.execute(
                query,
                end_time,
                'completed',
                json.dumps(summary),
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
            
            # Create failure summary for parameters field
            failure_summary = {
                'end_time': end_time.isoformat(),
                'status': 'failed',
                'error_message': error_message,
                'duration_seconds': (end_time - self.start_time).total_seconds()
            }
            
            query = f"""
            UPDATE {runs_table} 
            SET end_time = $1,
                status = $2,
                error_message = $3,
                parameters = $4
            WHERE id = $5
            """
            
            await conn.execute(
                query,
                end_time,
                'failed',
                error_message,
                json.dumps(failure_summary),
                self.run_id
            )
        finally:
            await conn.close()

    async def _generate_multi_scale_sequence(self, symbol: str, scales: List):
        """Generate multi-scale sequence for a single symbol."""
        
        from storage.multi_scale_sequence import MultiScaleSequence, ScaleFeatures, TimeScale, MarketEvent, EventSequence
        from config.feature_flags import is_enabled
        
        # Generate base minute data (if minute scale is requested)
        minute_data = None
        if TimeScale.MINUTE in scales:
            minute_data = self._generate_synthetic_minute_data(symbol)
        else:
            # Generate daily data as base
            market_data = await self._load_market_data_for_symbol(symbol)
            minute_data = self._convert_daily_to_minute_data(market_data[market_data['symbol'] == symbol])
        
        # Aggregate to all requested timeframes
        all_timeframes = self._aggregate_to_higher_timeframes(minute_data, scales)
        
        # Create ScaleFeatures for each timeframe
        scale_features = {}
        for scale, data in all_timeframes.items():
            if len(data) > 0:
                enhanced_data = self._calculate_technical_indicators(data, scale)
                
                # Add agent features if enabled
                if self.config.enable_agent_features and is_enabled("enable_agent_networks"):
                    agent_features = self._generate_agent_features(symbol, enhanced_data)
                    if agent_features:
                        # Add agent features to the data
                        for agent_feature_name, values in agent_features.items():
                            enhanced_data[agent_feature_name] = values
                
                scale_features[scale] = self._create_scale_features(enhanced_data, scale)
        
        # Generate events if enabled
        events = []
        if self.config.enable_events:
            timeframe = (datetime.combine(self.config.start_date, datetime.min.time()), 
                        datetime.combine(self.config.end_date, datetime.min.time()))
            events = await self._generate_synthetic_events(symbol, timeframe)
            
            # Enhance with LLM if enabled
            if self.config.enable_llm_events and is_enabled("enable_llm_events"):
                events = await self._enhance_events_with_llm(events)
        
        # Create event sequence
        event_sequence = EventSequence(
            events=events,
            time_range=(datetime.combine(self.config.start_date, datetime.min.time()),
                       datetime.combine(self.config.end_date, datetime.min.time()))
        ) if events else None
        
        # Create multi-scale sequence
        sequence_kwargs = {
            'symbol': symbol,
            'time_range': (datetime.combine(self.config.start_date, datetime.min.time()),
                          datetime.combine(self.config.end_date, datetime.min.time())),
            'event_sequence': event_sequence
        }
        
        # Add scale-specific features
        for scale, features in scale_features.items():
            if scale == TimeScale.MINUTE:
                sequence_kwargs['minute_features'] = features
            elif scale == TimeScale.HOURLY:
                sequence_kwargs['hourly_features'] = features
            elif scale == TimeScale.DAILY:
                sequence_kwargs['daily_features'] = features
            elif scale == TimeScale.WEEKLY:
                sequence_kwargs['weekly_features'] = features
        
        return MultiScaleSequence(**sequence_kwargs)
    
    def _generate_synthetic_minute_data(self, symbol: str) -> pd.DataFrame:
        """Generate synthetic minute-level data."""
        
        # Simplified minute data generation for integration
        days = (self.config.end_date - self.config.start_date).days
        base_price = 100.0 + np.random.uniform(-20, 20)
        
        timestamps = []
        data_rows = []
        
        current_date = datetime.combine(self.config.start_date, datetime.min.time())
        end_date = datetime.combine(self.config.end_date, datetime.min.time())
        
        while current_date <= end_date:
            if current_date.weekday() < 5:  # Weekdays only
                # Generate market hours (9:30 AM to 4:00 PM)
                for hour in range(10, 16):  # Simplified to 10 AM - 4 PM
                    for minute in range(0, 60, 5):  # Every 5 minutes
                        timestamp = current_date.replace(hour=hour, minute=minute)
                        
                        # Generate price movement
                        price_change = np.random.normal(0, 0.01)
                        base_price *= (1 + price_change)
                        
                        # Generate OHLC
                        minute_range = base_price * 0.005
                        high = base_price + minute_range * np.random.uniform(0, 1)
                        low = base_price - minute_range * np.random.uniform(0, 1)
                        open_price = np.random.uniform(low, high)
                        volume = int(np.random.lognormal(10, 1))
                        
                        data_rows.append({
                            'timestamp': timestamp,
                            'symbol': symbol,
                            'open': round(open_price, 2),
                            'high': round(high, 2),
                            'low': round(low, 2),
                            'close': round(base_price, 2),
                            'volume': volume
                        })
                        
            current_date += timedelta(days=1)
        
        df = pd.DataFrame(data_rows)
        if len(df) > 0:
            df = df.set_index('timestamp')
        
        logger.info(f"Generated {len(df)} minute bars for {symbol}")
        return df
    
    async def _load_market_data_for_symbol(self, symbol: str) -> pd.DataFrame:
        """Load market data for a specific symbol."""
        market_data = await self._load_market_data()
        return market_data[market_data['symbol'] == symbol] if len(market_data) > 0 else pd.DataFrame()
    
    def _convert_daily_to_minute_data(self, daily_data: pd.DataFrame) -> pd.DataFrame:
        """Convert daily data to synthetic minute data."""
        if len(daily_data) == 0:
            return pd.DataFrame()
        
        minute_rows = []
        for date_idx, row in daily_data.iterrows():
            # Get the date - could be from index or 'date' column
            if hasattr(date_idx, 'date'):  # It's already a datetime/timestamp
                date = date_idx
            elif 'date' in row:
                date = row['date']
                if isinstance(date, str):
                    date = pd.to_datetime(date)
            else:
                # Use index as date
                date = pd.to_datetime(date_idx)
            
            # Ensure we have a proper datetime object
            from datetime import datetime
            if hasattr(date, 'date') and hasattr(date, 'time'):
                # It's already a datetime
                base_date = date.date()
            else:
                # It might be just a date
                base_date = date if hasattr(date, 'year') else pd.to_datetime(date).date()
            
            # Create hourly bars from daily data
            for hour in range(10, 16):
                timestamp = datetime.combine(base_date, datetime.min.time()).replace(hour=hour, minute=0)
                minute_rows.append({
                    'timestamp': timestamp,
                    'symbol': row['symbol'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume'] // 6  # Divide daily volume by 6 hours
                })
        
        df = pd.DataFrame(minute_rows)
        if len(df) > 0:
            df = df.set_index('timestamp')
        return df
    
    def _aggregate_to_higher_timeframes(self, minute_data: pd.DataFrame, scales: List) -> Dict:
        """Aggregate minute data to higher timeframes."""
        
        from storage.multi_scale_sequence import TimeScale
        
        if len(minute_data) == 0:
            return {}
        
        aggregated = {}
        
        if TimeScale.MINUTE in scales:
            aggregated[TimeScale.MINUTE] = minute_data
        
        if TimeScale.HOURLY in scales:
            hourly = minute_data.resample('1H').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'symbol': 'first'
            }).dropna()
            if len(hourly) > 0:
                aggregated[TimeScale.HOURLY] = hourly
        
        if TimeScale.DAILY in scales:
            daily = minute_data.resample('1D').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'symbol': 'first'
            }).dropna()
            if len(daily) > 0:
                aggregated[TimeScale.DAILY] = daily
        
        if TimeScale.WEEKLY in scales:
            weekly = minute_data.resample('1W').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'symbol': 'first'
            }).dropna()
            if len(weekly) > 0:
                aggregated[TimeScale.WEEKLY] = weekly
        
        return aggregated
    
    def _calculate_technical_indicators(self, data: pd.DataFrame, scale) -> pd.DataFrame:
        """Calculate technical indicators for given timeframe."""
        if len(data) == 0:
            return data
        
        df = data.copy()
        
        # Use existing enhanced technical indicators
        if self.config.use_enhanced_features:
            if len(df) >= 21:  # Minimum length for indicators
                try:
                    etop = TechnicalIndicators.calculate_elliott_top(
                        df['high'].values, df['low'].values, df['close'].values
                    )
                    df['etop'] = etop
                    
                    ebot = TechnicalIndicators.calculate_elliott_bottom(
                        df['high'].values, df['low'].values, df['close'].values
                    )
                    df['ebot'] = ebot
                    
                    pldot = TechnicalIndicators.calculate_pldot(
                        df['high'].values, df['low'].values, df['close'].values, df['volume'].values
                    )
                    df['pldot'] = pldot
                    
                    oneonedot = TechnicalIndicators.calculate_oneonedot(
                        df['open'].values, df['high'].values, df['low'].values, df['close'].values
                    )
                    df['oneonedot'] = oneonedot
                    
                except Exception as e:
                    logger.warning(f"Failed to calculate enhanced technical indicators: {e}")
        
        # Basic technical indicators
        if len(df) >= 14:
            # RSI
            delta = df['close'].diff()
            gain = delta.where(delta > 0, 0).rolling(window=14).mean()
            loss = (-delta).where(delta < 0, 0).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
        
        # Fill NaN values
        df = df.fillna(method='ffill').fillna(0)
        
        return df
    
    def _generate_agent_features(self, symbol: str, data: pd.DataFrame) -> Dict[str, np.ndarray]:
        """Generate agent-based features."""
        
        if len(data) < 20:  # Need enough data
            return {}
        
        agent_features = {}
        
        try:
            # Trend agent
            sma_20 = data['close'].rolling(20).mean()
            sma_50 = data['close'].rolling(50).mean() if len(data) >= 50 else sma_20
            trend_signal = np.where(sma_20 > sma_50, 1, -1)
            agent_features['trend_agent'] = trend_signal
            
            # Volatility agent
            returns = data['close'].pct_change()
            volatility = returns.rolling(20).std()
            vol_signal = np.where(volatility > volatility.quantile(0.8), -1,
                                 np.where(volatility < volatility.quantile(0.2), 1, 0))
            agent_features['vol_agent'] = vol_signal
            
            logger.debug(f"Generated agent features for {symbol}")
            
        except Exception as e:
            logger.warning(f"Failed to generate agent features for {symbol}: {e}")
        
        return agent_features
    
    async def _generate_synthetic_events(self, symbol: str, timeframe: Tuple) -> List:
        """Generate synthetic market events."""
        
        from storage.multi_scale_sequence import MarketEvent
        
        start_time, end_time = timeframe
        events = []
        
        # Generate 1-2 events per month on average
        total_days = (end_time - start_time).days
        num_events = max(1, int(total_days / 30 * np.random.uniform(1, 2)))
        
        event_types = ['news', 'earnings', 'upgrade', 'economic']
        
        for i in range(num_events):
            random_days = np.random.uniform(0, total_days)
            event_time = start_time + timedelta(days=random_days)
            
            event_type = np.random.choice(event_types)
            content = f"{symbol} {event_type} event - synthetic data for training"
            
            sentiment_score = np.random.uniform(-0.5, 0.5)
            importance_score = np.random.uniform(0.3, 0.8)
            
            event = MarketEvent(
                event_id=f"synthetic_{symbol}_{i}",
                symbol=symbol,
                timestamp=event_time,
                event_type=event_type,
                content=content,
                sentiment_score=sentiment_score,
                importance_score=importance_score
            )
            
            events.append(event)
        
        return events
    
    async def _enhance_events_with_llm(self, events: List) -> List:
        """Enhance events with LLM analysis if available."""
        
        try:
            from llm import quick_event_analysis
            
            enhanced_events = []
            for event in events:
                try:
                    result = await quick_event_analysis(event.content, event.symbol, event.event_type)
                    if result:
                        event.sentiment_score = result.sentiment_score
                        event.importance_score = result.importance_score
                        event.metadata = {
                            'llm_enhanced': True,
                            'impact_category': result.impact_category,
                            'confidence': result.confidence_score
                        }
                except Exception as e:
                    logger.debug(f"LLM enhancement failed for event {event.event_id}: {e}")
                
                enhanced_events.append(event)
            
            return enhanced_events
            
        except Exception as e:
            logger.warning(f"LLM event enhancement not available: {e}")
            return events
    
    def _create_scale_features(self, data: pd.DataFrame, scale):
        """Create ScaleFeatures object from processed data."""
        
        from storage.multi_scale_sequence import ScaleFeatures
        
        if len(data) == 0:
            return None
        
        # Extract OHLCV
        ohlcv_columns = ['open', 'high', 'low', 'close', 'volume']
        ohlcv_data = data[ohlcv_columns].values
        
        # Extract other features (technical indicators, agent features)
        other_columns = [col for col in data.columns if col not in ohlcv_columns + ['symbol']]
        technical_data = data[other_columns].values if other_columns else np.zeros((len(data), 0))
        
        return ScaleFeatures(
            timestamps=data.index,
            ohlcv=ohlcv_data,
            technical=technical_data
        )
    
    def _create_training_sequences_from_multi_scale(self, sequence) -> List[Dict]:
        """Create training sequences from multi-scale sequence."""
        
        training_sequences = []
        
        # Use daily as primary scale for sequence creation
        from storage.multi_scale_sequence import TimeScale
        primary_scale = TimeScale.DAILY if TimeScale.DAILY in sequence.scales else list(sequence.scales.keys())[0]
        
        primary_features = sequence.get_features(primary_scale, 'all')
        if primary_features is None or len(primary_features) == 0:
            return []
        
        sequence_length = getattr(self.config, f'sequence_length_{primary_scale.value.lower()}', 60)
        
        # Create sequences
        for i in range(sequence_length, len(primary_features)):
            feature_sequence = primary_features[i-sequence_length:i]
            
            # Create labels for multiple horizons
            labels = {}
            for horizon_name, horizon_steps in self.config.prediction_horizons.items():
                if i + horizon_steps < len(primary_features):
                    current_price = primary_features[i, 3]  # Close price
                    future_price = primary_features[i + horizon_steps, 3]
                    if current_price > 0:
                        returns = (future_price - current_price) / current_price
                        labels[horizon_name] = returns
            
            if labels:
                sequence_data = {
                    'features': feature_sequence,
                    'labels': labels,
                    'symbol': sequence.symbol
                }
                training_sequences.append(sequence_data)
        
        return training_sequences
    
    def _get_multi_scale_feature_names(self) -> List[str]:
        """Get feature names for multi-scale data."""
        
        feature_names = ['open', 'high', 'low', 'close', 'volume']
        
        # Add enhanced technical indicators
        if self.config.use_enhanced_features:
            feature_names.extend(['etop', 'ebot', 'pldot', 'oneonedot'])
        
        feature_names.extend(['rsi'])
        
        # Add agent features if enabled
        if self.config.enable_agent_features:
            feature_names.extend(['trend_agent', 'vol_agent'])
        
        return feature_names


def create_sample_job_config(symbols: List[str] = None, 
                           days_back: int = 365,
                           use_enhanced_features: bool = False) -> TrainingDataJobConfig:
    """Create a sample training data job configuration."""
    
    if symbols is None:
        symbols = ['AAPL', 'MSFT', 'GOOGL']
    
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

async def run_multi_scale_training_data_job(
    symbols: List[str], 
    scales: List[str] = None,
    days_back: int = 90,
    enable_events: bool = True,
    enable_agent_features: bool = False,
    enable_llm_events: bool = False,
    output_dir: str = "multi_scale_training_data"
) -> Dict[str, Any]:
    """Generate multi-scale training data with advanced features."""
    
    if scales is None:
        scales = ['hourly', 'daily']
    
    # Create config
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)
    
    config = TrainingDataJobConfig(
        job_name="multi_scale_training",
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        
        # Multi-scale settings
        enable_multi_scale=True,
        scales=scales,
        output_format="multi_scale",
        
        # Advanced features
        enable_events=enable_events,
        enable_agent_features=enable_agent_features,
        enable_llm_events=enable_llm_events,
        
        # Basic settings
        use_enhanced_features=True,
        feature_configs=[],  # Will be generated
        label_configs=[]     # Will be generated
    )
    
    runner = TrainingDataJobRunner(config=config, output_dir=output_dir)
    return await runner.run_training_data_generation()

async def run_multi_scale_training_data_job_for_symbol(
    symbol: str,
    scales: List[str] = None,
    days_back: int = 90,
    output_dir: str = "multi_scale_training_data",
    enable_all_features: bool = False
) -> Dict[str, Any]:
    """Convenience function for single symbol multi-scale training data generation."""
    
    # Enable advanced features if requested
    enable_agents = enable_all_features
    enable_llm = enable_all_features
    
    return await run_multi_scale_training_data_job(
        symbols=[symbol],
        scales=scales,
        days_back=days_back,
        enable_events=True,
        enable_agent_features=enable_agents,
        enable_llm_events=enable_llm,
        output_dir=output_dir
    )


if __name__ == "__main__":
    # Example usage
    async def main():
        logging.basicConfig(level=logging.INFO)
        
        # Generate basic training data for AAPL
        print("=== Generating Basic Training Data ===")
        basic_results = await run_training_data_job_for_symbol('AAPL')
        
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
        
        print("\n=== Generating Enhanced Training Data ===")
        # Generate enhanced training data with technical indicators
        enhanced_results = await run_enhanced_training_data_job_for_symbol('AAPL', days_back=180)
        
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
        
        print("\n=== Generating Multi-Scale Training Data ===")
        # Generate multi-scale training data
        multi_scale_results = await run_multi_scale_training_data_job_for_symbol(
            'AAPL', 
            scales=['hourly', 'daily'],
            days_back=60,
            enable_all_features=False  # Set to True to enable all advanced features
        )
        
        print("Multi-Scale Training Data Generation Results:")
        print(f"Status: {multi_scale_results['status']}")
        print(f"Run ID: {multi_scale_results['run_id']}")
        print(f"Dataset IDs: {multi_scale_results['dataset_ids']}")
        
        if multi_scale_results['status'] == 'success':
            training_results = multi_scale_results['results']['training_results']
            print(f"Features shape: {training_results['features_shape']}")
            print(f"Labels shape: {training_results['labels_shape']}")
            print(f"Feature names: {training_results['feature_names']}")
            print(f"Label names: {training_results['label_names']}")
            if 'multi_scale_metadata' in training_results:
                metadata = training_results['multi_scale_metadata']
                print(f"Scales used: {metadata['scales']}")
                print(f"Events enabled: {metadata['events_enabled']}")
                print(f"Agent features: {metadata['agent_features_enabled']}")
                print(f"LLM events: {metadata['llm_events_enabled']}")
    
    asyncio.run(main())