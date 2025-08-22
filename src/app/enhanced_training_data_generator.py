#!/usr/bin/env python3
"""
Enhanced Training Data Generator with Advanced Technical Indicators

This module generates training data with comprehensive technical indicators:
- etop, ebot, pldot, oneonedot for past 21 bars
- OHLC sequences for past 21 bars
- Feature distributions for visualization
- Structured data for web app filtering and charting
"""

import asyncio
import asyncpg
import logging
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import json

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

class EnhancedTrainingDataJobConfig:
    """Enhanced configuration for training data generation."""
    
    def __init__(self,
                 symbols: List[str],
                 start_date: date,
                 end_date: date,
                 sequence_length: int = 21,  # Using 21 for past 21 bars
                 prediction_horizon: int = 5,
                 include_technical_indicators: bool = True,
                 indicator_windows: List[int] = None):
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.sequence_length = sequence_length
        self.prediction_horizon = prediction_horizon
        self.include_technical_indicators = include_technical_indicators
        self.indicator_windows = indicator_windows or [21]
        
        # Enhanced feature configuration
        self.feature_config = self._create_enhanced_feature_config()
    
    def _create_enhanced_feature_config(self) -> Dict[str, Any]:
        """Create enhanced feature configuration."""
        return {
            'price_features': {
                'ohlc_sequence': {
                    'columns': ['open', 'high', 'low', 'close'],
                    'sequence_length': self.sequence_length,
                    'description': f'OHLC prices for past {self.sequence_length} bars'
                },
                'volume_sequence': {
                    'columns': ['volume'],
                    'sequence_length': self.sequence_length,
                    'description': f'Volume for past {self.sequence_length} bars'
                }
            },
            'technical_indicators': {
                'etop': {
                    'function': 'envelope_top',
                    'window': 21,
                    'description': 'Envelope Top reversal indicator (21 periods)'
                },
                'ebot': {
                    'function': 'envelope_bottom',
                    'window': 21,
                    'description': 'Envelope Bottom reversal indicator (21 periods)'
                },
                'pldot': {
                    'function': 'pivot_line_dot',
                    'window': 21,
                    'description': 'Pivot Line Dot momentum indicator (21 periods)'
                },
                'oneonedot': {
                    'function': 'oneonedot',
                    'window': 21,
                    'description': 'One-One-Dot custom momentum oscillator (21 periods)'
                }
            }
        }

class EnhancedTrainingDataJobRunner:
    """Enhanced training data job runner with advanced features."""
    
    def __init__(self, config: EnhancedTrainingDataJobConfig, output_dir: str = "training_data_output", env: Environment = None):
        self.config = config
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.env = env or Environment()
        
        self.run_id = None
        self.dataset_ids = []
        self.start_time = datetime.now()
        
        # Initialize DAO
        self.training_dao = TrainingDatasetDAO(env=self.env)
    
    async def run_training_data_generation(self) -> Dict[str, Any]:
        """Run enhanced training data generation with comprehensive features."""
        
        try:
            logger.info("🚀 Starting Enhanced Training Data Generation")
            logger.info(f"Symbols: {self.config.symbols}")
            logger.info(f"Date range: {self.config.start_date} to {self.config.end_date}")
            logger.info(f"Sequence length: {self.config.sequence_length}")
            logger.info(f"Technical indicators: {list(self.config.feature_config['technical_indicators'].keys())}")
            
            # Create run record
            self.run_id = await self._create_run_record()
            
            # Load and prepare market data
            market_data = await self._load_market_data()
            
            # Generate enhanced features and labels
            features, labels, metadata = await self._create_enhanced_training_data(market_data)
            
            # Create dataset ID
            dataset_id = f"enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.config.symbols[0].lower()}"
            
            # Save data files
            data_files = self._save_training_data_files(features, labels, dataset_id, metadata)
            
            # Create database record
            dataset_record = await self._create_enhanced_dataset_record(features, labels, dataset_id, data_files, metadata)
            
            # Update run record with success
            await self._update_run_record_success({'dataset_record': dataset_record})
            
            logger.info("✅ Enhanced Training Data Generation Completed Successfully!")
            
            return {
                'status': 'success',
                'run_id': self.run_id,
                'dataset_ids': self.dataset_ids,
                'features_shape': features.shape,
                'labels_shape': labels.shape,
                'metadata': metadata
            }
            
        except Exception as e:
            logger.error(f"❌ Enhanced training data generation failed: {e}")
            if self.run_id:
                await self._update_run_record_failure(str(e))
            
            return {
                'status': 'error',
                'error': str(e),
                'run_id': self.run_id
            }
    
    async def _create_run_record(self) -> int:
        """Create run record in database."""
        
        conn = await asyncpg.connect(self.env.get_database_url())
        try:
            runs_table = self.env.get_table_name("runs")
            
            run_id = await conn.fetchval(f"""
                INSERT INTO {runs_table} (
                    run_type, start_time, status, total_symbols
                ) VALUES ($1, $2, $3, $4) RETURNING id
            """, 
                "enhanced_training_data_generation", 
                self.start_time, 
                "running", 
                len(self.config.symbols)
            )
            
            logger.info(f"Created run record with ID: {run_id}")
            return run_id
            
        finally:
            await conn.close()
    
    async def _load_market_data(self) -> pd.DataFrame:
        """Load market data for training data generation."""
        
        # For now, generate synthetic data with proper technical indicators
        logger.info("Generating enhanced synthetic market data...")
        
        data_rows = []
        
        for symbol in self.config.symbols:
            current_date = self.config.start_date
            base_price = 100.0 + np.random.uniform(-20, 20)
            
            while current_date <= self.config.end_date:
                if current_date.weekday() < 5:  # Trading days only
                    # Generate more realistic price action
                    daily_return = np.random.normal(0.001, 0.02)
                    base_price *= (1 + daily_return)
                    
                    # Create realistic intraday patterns
                    daily_range = base_price * np.random.uniform(0.01, 0.04)
                    
                    # Generate OHLC with proper relationships
                    open_price = base_price * (1 + np.random.normal(0, 0.005))
                    high = max(open_price, base_price) + daily_range / 2
                    low = min(open_price, base_price) - daily_range / 2
                    close_price = np.random.uniform(low, high)
                    
                    volume = int(np.random.lognormal(15, 1))
                    
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
        
        logger.info(f"Generated {len(df)} enhanced market data points")
        return df
    
    async def _create_enhanced_training_data(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, Dict[str, Any]]:
        """Create enhanced training data with technical indicators."""
        
        features_list = []
        labels_list = []
        feature_distributions = {}
        
        indicators = TechnicalIndicators()
        
        for symbol in self.config.symbols:
            symbol_data = data[data['symbol'] == symbol].copy()
            if len(symbol_data) < self.config.sequence_length + self.config.prediction_horizon + 50:
                continue
                
            symbol_data = symbol_data.sort_index()
            
            # Extract OHLCV
            ohlcv = symbol_data[['open', 'high', 'low', 'close', 'volume']].values
            open_ = symbol_data['open'].values
            high = symbol_data['high'].values
            low = symbol_data['low'].values
            close = symbol_data['close'].values
            volume = symbol_data['volume'].values
            
            # Calculate technical indicators
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
            
            # Combine all features
            all_features = np.column_stack([
                ohlcv,  # OHLCV for past 21 bars
                etop.reshape(-1, 1),
                ebot.reshape(-1, 1),
                pldot.reshape(-1, 1),
                oneonedot.reshape(-1, 1)
            ])
            
            # Create sequences
            start_idx = max(50, self.config.sequence_length)  # Allow indicators to stabilize
            
            for i in range(start_idx, len(all_features) - self.config.prediction_horizon + 1):
                # Feature sequence (past 21 bars)
                feature_seq = all_features[i-self.config.sequence_length:i]
                features_list.append(feature_seq)
                
                # Labels (future returns)
                future_prices = close[i:i + self.config.prediction_horizon]
                current_price = close[i-1]
                future_returns = (future_prices - current_price) / current_price
                labels_list.append(future_returns)
        
        features = np.array(features_list, dtype=np.float32)
        labels = np.array(labels_list, dtype=np.float32)
        
        metadata = {
            'feature_names': ['open', 'high', 'low', 'close', 'volume', 'etop', 'ebot', 'pldot', 'oneonedot'],
            'feature_descriptions': {
                'open': 'Opening price',
                'high': 'High price', 
                'low': 'Low price',
                'close': 'Closing price',
                'volume': 'Trading volume',
                'etop': 'Envelope Top reversal indicator (21 periods)',
                'ebot': 'Envelope Bottom reversal indicator (21 periods)',
                'pldot': 'Pivot Line Dot momentum indicator (21 periods)',
                'oneonedot': 'One-One-Dot custom momentum oscillator (21 periods)'
            },
            'label_names': [f'future_return_{i+1}d' for i in range(self.config.prediction_horizon)],
            'sequence_length': self.config.sequence_length,
            'prediction_horizon': self.config.prediction_horizon,
            'feature_distributions': feature_distributions,
            'technical_indicators': self.config.feature_config['technical_indicators']
        }
        
        logger.info(f"Created enhanced training data: {features.shape} features, {labels.shape} labels")
        logger.info(f"Features: {metadata['feature_names']}")
        
        return features, labels, metadata
    
    def _save_training_data_files(self, features: np.ndarray, labels: np.ndarray, dataset_id: str, metadata: Dict[str, Any]) -> Dict[str, str]:
        """Save enhanced training data to files."""
        
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
            'feature_names': metadata['feature_names'],
            'feature_descriptions': metadata['feature_descriptions'],
            'technical_indicators': metadata['technical_indicators'],
            'feature_distributions': metadata['feature_distributions']
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(file_metadata, f, indent=2)
        
        logger.info(f"Saved training data files: {features_file}, {labels_file}, {metadata_file}")
        
        return {
            'features': str(features_file),
            'labels': str(labels_file), 
            'metadata': str(metadata_file)
        }
    
    async def _create_enhanced_dataset_record(self, 
                                            features: np.ndarray,
                                            labels: np.ndarray, 
                                            dataset_id: str,
                                            data_files: Dict[str, str],
                                            metadata: Dict[str, Any]) -> TrainingDatasetRecord:
        """Create enhanced dataset record in database."""
        
        features_shape = features.shape
        labels_shape = labels.shape
        
        # Calculate data quality metrics
        data_quality_score = self._calculate_data_quality(features, labels)
        
        # Calculate file sizes
        features_file_size = Path(data_files['features']).stat().st_size / (1024 * 1024)
        labels_file_size = Path(data_files['labels']).stat().st_size / (1024 * 1024)
        total_file_size = features_file_size + labels_file_size
        
        dataset_record = TrainingDatasetRecord(
            dataset_name=dataset_id,
            run_id=self.run_id,
            total_sequences=features_shape[0],
            sequence_length=features_shape[1],
            feature_count=features_shape[2],
            label_count=labels_shape[1],
            symbols=self.config.symbols,
            date_range_start=self.config.start_date,
            date_range_end=self.config.end_date,
            data_quality_score=data_quality_score,
            feature_completeness=1.0,  # All features generated
            label_completeness=1.0,   # All labels generated
            generation_duration_seconds=int((datetime.now() - self.start_time).total_seconds()),
            file_size_mb=total_file_size,
            data_sources=['synthetic_enhanced'],
            status='created',
            features_file_path=data_files['features'],
            labels_file_path=data_files['labels'],
            metadata_file_path=data_files['metadata'],
            feature_metadata=json.dumps(metadata['feature_descriptions']),
            technical_indicators=json.dumps(metadata['technical_indicators'])
        )
        
        # Save to database
        dataset_id_db = await self.training_dao.create_training_dataset(dataset_record)
        dataset_record.id = dataset_id_db
        
        self.dataset_ids.append(dataset_id_db)
        
        logger.info(f"Created enhanced training dataset record with ID {dataset_id_db}")
        return dataset_record
    
    def _calculate_data_quality(self, features: np.ndarray, labels: np.ndarray) -> float:
        """Calculate data quality score for enhanced training data."""
        
        # Check for NaN or infinite values
        feature_quality = 1.0 - (np.isnan(features).sum() + np.isinf(features).sum()) / features.size
        label_quality = 1.0 - (np.isnan(labels).sum() + np.isinf(labels).sum()) / labels.size
        
        # Check for reasonable value ranges
        feature_range_quality = 1.0
        if np.std(features) == 0:
            feature_range_quality = 0.5  # No variation is concerning
        
        # Overall quality score
        return (feature_quality + label_quality + feature_range_quality) / 3
    
    async def _update_run_record_success(self, results: Dict[str, Any]) -> None:
        """Update run record with success information."""
        
        end_time = datetime.now()
        duration = int((end_time - self.start_time).total_seconds())
        dataset_record = results['dataset_record']
        
        conn = await asyncpg.connect(self.env.get_database_url())
        try:
            runs_table = self.env.get_table_name("runs")
            await conn.execute(f"""
                UPDATE {runs_table} 
                SET end_time = $1,
                    status = $2,
                    successful_unifications = $3,
                    total_dates = $4,
                    processing_rate_per_second = $5,
                    quality_summary = $6,
                    performance_summary = $7
                WHERE id = $8
            """,
                end_time,
                'completed',
                dataset_record.total_sequences,
                (self.config.end_date - self.config.start_date).days,
                dataset_record.total_sequences / max(duration, 1),
                f"Enhanced training data: {dataset_record.total_sequences} sequences with {dataset_record.data_quality_score:.2%} quality",
                f"Features: OHLC + etop/ebot/pldot/oneonedot, Duration: {duration}s, Size: {dataset_record.file_size_mb:.1f}MB",
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
            await conn.execute(f"""
                UPDATE {runs_table} 
                SET end_time = $1,
                    status = $2,
                    quality_summary = $3,
                    performance_summary = $4
                WHERE id = $5
            """,
                end_time,
                'failed',
                f"Enhanced training data generation failed: {error_message}",
                f"Failed after {int((end_time - self.start_time).total_seconds())}s",
                self.run_id
            )
        finally:
            await conn.close()

# Convenience functions for easy usage
async def run_enhanced_training_data_job_for_symbol(symbol: str, 
                                                  output_dir: str = "training_data_output",
                                                  days_back: int = 365) -> Dict[str, Any]:
    """Generate enhanced training data for a single symbol."""
    
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back)
    
    config = EnhancedTrainingDataJobConfig(
        symbols=[symbol],
        start_date=start_date,
        end_date=end_date,
        sequence_length=21,  # Past 21 bars as requested
        prediction_horizon=5
    )
    
    runner = EnhancedTrainingDataJobRunner(config, output_dir)
    return await runner.run_training_data_generation()

if __name__ == "__main__":
    # Test the enhanced training data generation
    async def test_enhanced_generation():
        results = await run_enhanced_training_data_job_for_symbol('AAPL', days_back=180)
        print(f"Enhanced training data generation results: {results}")
    
    asyncio.run(test_enhanced_generation())