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
        data_files = self._save_training_data_files(features, labels, dataset_id)
        
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
        """Create basic training data from market data."""
        
        # Create simple features: OHLCV + basic indicators
        features_list = []
        labels_list = []
        
        for symbol in self.config.symbols:
            symbol_data = data[data['symbol'] == symbol].copy()
            if len(symbol_data) < self.config.sequence_length + self.config.prediction_horizon:
                continue
                
            symbol_data = symbol_data.sort_index()
            
            # Create features (OHLCV + simple indicators)
            ohlcv_features = symbol_data[['open', 'high', 'low', 'close', 'volume']].values
            
            # Add simple technical indicators
            close_prices = symbol_data['close'].values
            returns = np.diff(close_prices, prepend=close_prices[0]) / close_prices
            
            # Simple moving average
            sma_10 = np.convolve(close_prices, np.ones(10)/10, mode='same')
            
            # Combine features
            symbol_features = np.column_stack([
                ohlcv_features,
                returns.reshape(-1, 1),
                sma_10.reshape(-1, 1)
            ])
            
            # Create sequences
            for i in range(len(symbol_features) - self.config.sequence_length - self.config.prediction_horizon + 1):
                # Feature sequence
                feature_seq = symbol_features[i:i + self.config.sequence_length]
                features_list.append(feature_seq)
                
                # Label (future returns)
                future_prices = close_prices[i + self.config.sequence_length:i + self.config.sequence_length + self.config.prediction_horizon]
                current_price = close_prices[i + self.config.sequence_length - 1]
                future_returns = (future_prices - current_price) / current_price
                labels_list.append(future_returns)
        
        features = np.array(features_list, dtype=np.float32)
        labels = np.array(labels_list, dtype=np.float32)
        
        metadata = {
            'feature_names': ['open', 'high', 'low', 'close', 'volume', 'returns_1d', 'sma_10'],
            'label_names': [f'future_return_{i+1}d' for i in range(self.config.prediction_horizon)],
            'sequence_length': self.config.sequence_length,
            'prediction_horizon': self.config.prediction_horizon
        }
        
        return features, labels, metadata
    
    def _save_training_data_files(self, features: np.ndarray, labels: np.ndarray, dataset_id: str) -> Dict[str, str]:
        """Save training data to files."""
        
        features_file = self.output_dir / f"{dataset_id}_features.npy"
        labels_file = self.output_dir / f"{dataset_id}_labels.npy"
        metadata_file = self.output_dir / f"{dataset_id}_metadata.json"
        
        # Save numpy arrays
        np.save(features_file, features)
        np.save(labels_file, labels)
        
        # Save metadata
        metadata = {
            'dataset_id': dataset_id,
            'creation_timestamp': datetime.now().isoformat(),
            'features_shape': features.shape,
            'labels_shape': labels.shape,
            'symbols': self.config.symbols,
            'date_range': {
                'start': self.config.start_date.isoformat(),
                'end': self.config.end_date.isoformat()
            }
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
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
                           days_back: int = 365) -> TrainingDataJobConfig:
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
    
    return TrainingDataJobConfig(
        job_name=f"training_data_gen_{'-'.join(symbols)}",
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        sequence_length=60,
        prediction_horizon=5,
        normalize_features=True,
        normalize_labels=False,
        feature_configs=feature_configs,
        label_configs=label_configs,
        output_dir="training_data_output",
        dataset_name_prefix=f"dataset_{'_'.join(symbols)}",
        min_sequences_required=500,
        min_quality_score=0.8
    )

async def run_training_data_job_for_symbol(symbol: str, 
                                         output_dir: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to run training data generation for a single symbol."""
    
    config = create_sample_job_config(symbols=[symbol])
    runner = TrainingDataJobRunner(config=config, output_dir=output_dir)
    
    return await runner.run_training_data_generation()

if __name__ == "__main__":
    # Example usage
    async def main():
        logging.basicConfig(level=logging.INFO)
        
        # Generate training data for AAPL
        results = await run_training_data_job_for_symbol('AAPL')
        
        print("Training Data Generation Results:")
        print(f"Status: {results['status']}")
        print(f"Run ID: {results['run_id']}")
        print(f"Dataset IDs: {results['dataset_ids']}")
        
        if results['status'] == 'success':
            training_results = results['results']['training_results']
            print(f"Features shape: {training_results['features_shape']}")
            print(f"Labels shape: {training_results['labels_shape']}")
            print(f"Feature names: {training_results['feature_names']}")
            print(f"Label names: {training_results['label_names']}")
    
    asyncio.run(main())