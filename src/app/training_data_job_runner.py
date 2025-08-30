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
    
    # Feature and label configuration
    feature_configs: List[Dict[str, Any]] = gin.REQUIRED
    label_configs: List[Dict[str, Any]] = gin.REQUIRED
    
    # Output configuration
    output_dir: str = "training_data_output"
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
        """Generate training data using simplified approach."""
        
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
                etop = indicators.calculate_elliott_top(high, low, close, 21)
                ebot = indicators.calculate_elliott_bottom(high, low, close, 21)
                pldot = indicators.calculate_pivot_line_dot(high, low, close, 21)
                oneonedot = indicators.calculate_oneonedot(open_, high, low, close, 21)
                
                # Store feature distributions for visualization
                feature_distributions[symbol] = {
                    'etop': etop.tolist(),
                    'ebot': ebot.tolist(),
                    'pldot': pldot.tolist(),
                    'oneonedot': oneonedot.tolist(),
                    'close': close.tolist(),
                    'volume': volume.tolist()
                }
                
                # Combine all features with enhanced indicators
                symbol_features = np.column_stack([
                    ohlcv_features,  # OHLCV
                    etop.reshape(-1, 1),
                    ebot.reshape(-1, 1),
                    pldot.reshape(-1, 1),
                    oneonedot.reshape(-1, 1)
                ])
                
                feature_names = ['open', 'high', 'low', 'close', 'volume', 'etop', 'ebot', 'pldot', 'oneonedot']
                feature_descriptions = {
                    'open': 'Opening price',
                    'high': 'High price', 
                    'low': 'Low price',
                    'close': 'Closing price',
                    'volume': 'Trading volume',
                    'etop': 'Envelope Top reversal indicator (21 periods)',
                    'ebot': 'Envelope Bottom reversal indicator (21 periods)',
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
    
    asyncio.run(main())