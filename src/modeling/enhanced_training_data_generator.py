"""
Enhanced Training Data Generator

Generates single comprehensive training datasets with multi-timeframe typed features
using real market data from the ATS database.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
import h5py
import json
import os
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path
import logging

try:
    from .enhanced_feature_types import (
        FeatureSpecification, FeatureType, TimeframeSpec, 
        TechnicalIndicator, EnhancedFeatureRegistry
    )
    from .multi_timeframe_data_collector import (
        MultiTimeframeDataCollector, DataCollectionConfig
    )
    from .cross_timeframe_aligner import CrossTimeframeAligner
except ImportError:
    from enhanced_feature_types import (
        FeatureSpecification, FeatureType, TimeframeSpec, 
        TechnicalIndicator, EnhancedFeatureRegistry
    )
    from multi_timeframe_data_collector import (
        MultiTimeframeDataCollector, DataCollectionConfig
    )
    from cross_timeframe_aligner import CrossTimeframeAligner

logger = logging.getLogger(__name__)


@dataclass
class EnhancedTrainingConfig:
    """Configuration for enhanced training data generation."""
    
    # Data selection
    symbols: List[str]
    start_date: str
    end_date: str
    
    # Feature configuration
    feature_specs: List[FeatureSpecification]
    include_cross_timeframe: bool = True
    
    # Data processing
    sequence_length: int = 64  # Number of time steps per sample
    prediction_horizon: int = 1  # Days ahead to predict
    min_samples_per_symbol: int = 100
    
    # Quality control
    max_missing_ratio: float = 0.1  # Max 10% missing data
    outlier_std_threshold: float = 4.0  # Remove outliers beyond 4 std
    
    # Output configuration
    output_dir: str = "training_data_output"
    dataset_name: str = "enhanced_training_dataset"
    compression_level: int = 6
    
    # Metadata
    description: str = ""
    created_by: str = "enhanced_training_data_generator"
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass  
class TrainingDatasetMetadata:
    """Comprehensive metadata for training dataset."""
    
    # Basic info
    dataset_name: str
    creation_timestamp: datetime
    config: EnhancedTrainingConfig
    
    # Data statistics
    total_samples: int
    symbols_count: int
    date_range: Tuple[str, str]
    
    # Feature information
    feature_registry: Dict[str, Any]
    feature_shapes: Dict[str, Tuple[int, ...]]
    feature_types: Dict[str, str]
    
    # Quality metrics
    data_quality_score: float
    missing_data_ratio: float
    outliers_removed: int
    
    # File information
    file_paths: Dict[str, str]  # 'features', 'labels', 'metadata'
    file_sizes_mb: Dict[str, float]
    compression_info: Dict[str, Any]
    
    # Generation performance
    generation_duration_seconds: float
    processing_stages: List[Dict[str, Any]]


class EnhancedTrainingDataGenerator:
    """Enhanced training data generator with real database integration."""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.db_pool: Optional[asyncpg.Pool] = None
        
        # Components
        self.feature_registry = EnhancedFeatureRegistry()
        self.data_collector: Optional[MultiTimeframeDataCollector] = None
        self.cross_aligner = CrossTimeframeAligner()
        
        logger.info("Initialized EnhancedTrainingDataGenerator")
    
    async def initialize(self):
        """Initialize database connection and components."""
        logger.info("Initializing database connection...")
        
        self.db_pool = await asyncpg.create_pool(
            self.database_url,
            min_size=2,
            max_size=10,
            command_timeout=300
        )
        
        self.data_collector = MultiTimeframeDataCollector(
            self.db_pool, self.feature_registry
        )
        
        logger.info("Database connection and components initialized")
    
    async def close(self):
        """Close database connections."""
        if self.db_pool:
            await self.db_pool.close()
            logger.info("Database connections closed")
    
    async def generate_training_dataset(self, config: EnhancedTrainingConfig) -> TrainingDatasetMetadata:
        """Generate complete training dataset with typed features."""
        
        start_time = datetime.now()
        logger.info(f"Starting enhanced training data generation: {config.dataset_name}")
        
        processing_stages = []
        
        try:
            # Stage 1: Validate configuration
            stage_start = datetime.now()
            await self._validate_config(config)
            processing_stages.append({
                "stage": "configuration_validation",
                "duration_seconds": (datetime.now() - stage_start).total_seconds(),
                "status": "completed"
            })
            
            # Stage 2: Collect multi-timeframe data
            stage_start = datetime.now()
            feature_data = await self._collect_feature_data(config)
            processing_stages.append({
                "stage": "data_collection", 
                "duration_seconds": (datetime.now() - stage_start).total_seconds(),
                "features_collected": len(feature_data),
                "status": "completed"
            })
            
            # Stage 3: Generate labels
            stage_start = datetime.now()
            labels = await self._generate_labels(config)
            processing_stages.append({
                "stage": "label_generation",
                "duration_seconds": (datetime.now() - stage_start).total_seconds(),
                "labels_generated": len(labels) if labels is not None else 0,
                "status": "completed"
            })
            
            # Stage 4: Quality validation and cleaning
            stage_start = datetime.now()
            cleaned_features, cleaned_labels, quality_metrics = self._clean_and_validate_data(
                feature_data, labels, config
            )
            processing_stages.append({
                "stage": "data_cleaning",
                "duration_seconds": (datetime.now() - stage_start).total_seconds(),
                "outliers_removed": quality_metrics.get("outliers_removed", 0),
                "status": "completed"
            })
            
            # Stage 5: Create sequences
            stage_start = datetime.now()
            sequences, sequence_labels = self._create_sequences(
                cleaned_features, cleaned_labels, config
            )
            processing_stages.append({
                "stage": "sequence_creation",
                "duration_seconds": (datetime.now() - stage_start).total_seconds(),
                "sequences_created": len(sequences) if sequences else 0,
                "status": "completed"
            })
            
            # Stage 6: Save dataset
            stage_start = datetime.now()
            file_paths, file_sizes = await self._save_dataset(
                sequences, sequence_labels, config
            )
            processing_stages.append({
                "stage": "dataset_saving",
                "duration_seconds": (datetime.now() - stage_start).total_seconds(),
                "files_created": len(file_paths),
                "total_size_mb": sum(file_sizes.values()),
                "status": "completed"
            })
            
            # Stage 7: Create metadata
            total_duration = (datetime.now() - start_time).total_seconds()
            metadata = self._create_metadata(
                config, sequences, sequence_labels, feature_data,
                quality_metrics, file_paths, file_sizes, 
                total_duration, processing_stages
            )
            
            # Save metadata
            metadata_path = os.path.join(config.output_dir, f"{config.dataset_name}_metadata.json")
            with open(metadata_path, 'w') as f:
                json.dump(asdict(metadata), f, indent=2, default=str)
            
            logger.info(f"Enhanced training dataset generated successfully: {config.dataset_name}")
            logger.info(f"Total samples: {metadata.total_samples}")
            logger.info(f"Total duration: {total_duration:.2f} seconds")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error generating training dataset: {e}")
            processing_stages.append({
                "stage": "error",
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
                "error": str(e),
                "status": "failed"
            })
            raise
    
    async def _validate_config(self, config: EnhancedTrainingConfig):
        """Validate configuration and check data availability."""
        
        logger.info("Validating configuration...")
        
        # Check symbols exist in database
        async with self.db_pool.acquire() as conn:
            query = """
            SELECT symbol FROM dev_instruments 
            WHERE symbol = ANY($1)
            """
            found_symbols = await conn.fetch(query, config.symbols)
            found_symbol_list = [row['symbol'] for row in found_symbols]
        
        missing_symbols = set(config.symbols) - set(found_symbol_list)
        if missing_symbols:
            logger.warning(f"Symbols not found in database: {missing_symbols}")
            # Remove missing symbols from config
            config.symbols = [s for s in config.symbols if s in found_symbol_list]
            
        if not config.symbols:
            raise ValueError("No valid symbols found in database")
        
        logger.info(f"Validated {len(config.symbols)} symbols")
        
        # Check date range has data
        async with self.db_pool.acquire() as conn:
            query = """
            SELECT COUNT(*) as count, MIN(date) as min_date, MAX(date) as max_date
            FROM dev_daily_prices dp
            JOIN dev_instruments i ON dp.instrument_id = i.id
            WHERE i.symbol = ANY($1) AND dp.date BETWEEN $2 AND $3
            """
            result = await conn.fetchrow(query, config.symbols, config.start_date, config.end_date)
        
        if result['count'] == 0:
            raise ValueError(f"No data found for date range {config.start_date} to {config.end_date}")
        
        logger.info(f"Found {result['count']} records in date range {result['min_date']} to {result['max_date']}")
    
    async def _collect_feature_data(self, config: EnhancedTrainingConfig) -> Dict[str, np.ndarray]:
        """Collect multi-timeframe feature data."""
        
        logger.info(f"Collecting feature data for {len(config.feature_specs)} features...")
        
        # Create data collection configuration
        data_config = DataCollectionConfig(
            symbols=config.symbols,
            start_date=config.start_date,
            end_date=config.end_date,
            feature_specs=config.feature_specs,
            batch_size=1000,
            validate_data=True
        )
        
        # Collect base feature data
        feature_data = await self.data_collector.collect_training_data(data_config)
        
        # Add cross-timeframe features if enabled
        if config.include_cross_timeframe:
            cross_features = [f for f in config.feature_specs 
                            if f.feature_type == FeatureType.CROSS_TIMEFRAME_INDICATORS]
            
            if cross_features:
                logger.info(f"Processing {len(cross_features)} cross-timeframe features...")
                cross_data = await self.cross_aligner.align_cross_timeframe_features(
                    feature_data, cross_features, config.symbols, 
                    config.start_date, config.end_date
                )
                feature_data.update(cross_data)
        
        logger.info(f"Collected {len(feature_data)} feature matrices")
        
        # Log feature shapes for debugging
        for name, data in feature_data.items():
            logger.info(f"Feature '{name}': shape {data.shape}")
        
        return feature_data
    
    async def _generate_labels(self, config: EnhancedTrainingConfig) -> Optional[np.ndarray]:
        """Generate prediction labels."""
        
        logger.info(f"Generating labels with {config.prediction_horizon} day horizon...")
        
        async with self.db_pool.acquire() as conn:
            # Get price data for label generation
            query = """
            SELECT 
                i.symbol,
                dp.date,
                dp.close as current_close,
                LEAD(dp.close, $4) OVER (PARTITION BY i.symbol ORDER BY dp.date) as future_close
            FROM dev_daily_prices dp
            JOIN dev_instruments i ON dp.instrument_id = i.id  
            WHERE i.symbol = ANY($1)
            AND dp.date BETWEEN $2 AND $3
            ORDER BY i.symbol, dp.date
            """
            
            rows = await conn.fetch(
                query, config.symbols, config.start_date, 
                config.end_date, config.prediction_horizon
            )
        
        if not rows:
            logger.warning("No data found for label generation")
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        
        # Calculate returns (labels)
        df['return'] = (df['future_close'] / df['current_close'] - 1).fillna(0)
        
        # Create binary classification labels (positive/negative return)
        df['label'] = (df['return'] > 0).astype(int)
        
        # Group by symbol and create sequences
        labels = []
        for symbol in config.symbols:
            symbol_data = df[df['symbol'] == symbol].copy()
            symbol_data = symbol_data.sort_values('date')
            
            # Get labels for this symbol
            symbol_labels = symbol_data['label'].values
            labels.extend(symbol_labels)
        
        result = np.array(labels)
        logger.info(f"Generated {len(result)} labels")
        
        return result
    
    def _clean_and_validate_data(self, 
                                feature_data: Dict[str, np.ndarray],
                                labels: Optional[np.ndarray],
                                config: EnhancedTrainingConfig) -> Tuple[Dict[str, np.ndarray], Optional[np.ndarray], Dict[str, Any]]:
        """Clean and validate data quality."""
        
        logger.info("Cleaning and validating data...")
        
        quality_metrics = {
            "total_samples_before": 0,
            "total_samples_after": 0,
            "outliers_removed": 0,
            "missing_data_ratio": 0.0,
            "data_quality_score": 0.0
        }
        
        if not feature_data:
            return feature_data, labels, quality_metrics
        
        # Get sample size from first feature
        first_feature_name = list(feature_data.keys())[0]
        total_samples = feature_data[first_feature_name].shape[0]
        quality_metrics["total_samples_before"] = total_samples
        
        # Find valid indices (samples with no NaN/inf values in any feature)
        valid_indices = np.ones(total_samples, dtype=bool)
        
        for name, data in feature_data.items():
            # Check for NaN or infinite values
            finite_mask = np.isfinite(data).all(axis=(1, 2)) if data.ndim == 3 else np.isfinite(data).all(axis=1)
            valid_indices &= finite_mask
            
            # Remove outliers (values beyond threshold std deviations)
            if config.outlier_std_threshold > 0:
                flattened = data.reshape(data.shape[0], -1)
                mean_vals = np.mean(flattened, axis=1)
                std_vals = np.std(flattened, axis=1)
                
                # Mark outliers
                for i in range(len(mean_vals)):
                    if std_vals[i] > 0:
                        sample_data = flattened[i]
                        outliers = np.abs(sample_data - mean_vals[i]) > (config.outlier_std_threshold * std_vals[i])
                        if outliers.sum() > len(sample_data) * 0.5:  # If >50% are outliers, remove sample
                            valid_indices[i] = False
        
        # Calculate quality metrics
        valid_count = valid_indices.sum()
        quality_metrics["total_samples_after"] = valid_count
        quality_metrics["outliers_removed"] = total_samples - valid_count
        quality_metrics["missing_data_ratio"] = 1.0 - (valid_count / total_samples)
        quality_metrics["data_quality_score"] = min(1.0, valid_count / max(1, total_samples))
        
        logger.info(f"Data cleaning: {total_samples} -> {valid_count} samples ({quality_metrics['outliers_removed']} removed)")
        
        # Filter data
        cleaned_features = {}
        for name, data in feature_data.items():
            cleaned_features[name] = data[valid_indices]
        
        cleaned_labels = labels[valid_indices] if labels is not None else None
        
        return cleaned_features, cleaned_labels, quality_metrics
    
    def _create_sequences(self, 
                         feature_data: Dict[str, np.ndarray],
                         labels: Optional[np.ndarray],
                         config: EnhancedTrainingConfig) -> Tuple[Dict[str, np.ndarray], Optional[np.ndarray]]:
        """Create sequences from cleaned data."""
        
        logger.info(f"Creating sequences with length {config.sequence_length}...")
        
        if not feature_data:
            return feature_data, labels
        
        # For this implementation, we'll assume data is already in sequence format
        # In a full implementation, you'd create sliding windows here
        
        sequences = feature_data.copy()
        sequence_labels = labels
        
        # Validate minimum samples per symbol
        if sequences and len(list(sequences.values())[0]) < config.min_samples_per_symbol * len(config.symbols):
            logger.warning(f"Fewer samples than minimum required: {len(list(sequences.values())[0])}")
        
        return sequences, sequence_labels
    
    async def _save_dataset(self, 
                           sequences: Dict[str, np.ndarray],
                           labels: Optional[np.ndarray],
                           config: EnhancedTrainingConfig) -> Tuple[Dict[str, str], Dict[str, float]]:
        """Save dataset to HDF5 files with compression."""
        
        logger.info("Saving dataset to HDF5 files...")
        
        # Create output directory
        os.makedirs(config.output_dir, exist_ok=True)
        
        file_paths = {}
        file_sizes = {}
        
        # Save features
        features_path = os.path.join(config.output_dir, f"{config.dataset_name}_features.h5")
        
        with h5py.File(features_path, 'w') as f:
            for name, data in sequences.items():
                f.create_dataset(
                    name, 
                    data=data,
                    compression='gzip',
                    compression_opts=config.compression_level,
                    shuffle=True
                )
            
            # Add metadata to file
            f.attrs['dataset_name'] = config.dataset_name
            f.attrs['creation_time'] = datetime.now().isoformat()
            f.attrs['num_features'] = len(sequences)
        
        file_paths['features'] = features_path
        file_sizes['features'] = os.path.getsize(features_path) / (1024 * 1024)  # MB
        
        # Save labels if available
        if labels is not None:
            labels_path = os.path.join(config.output_dir, f"{config.dataset_name}_labels.h5")
            
            with h5py.File(labels_path, 'w') as f:
                f.create_dataset(
                    'labels',
                    data=labels,
                    compression='gzip',
                    compression_opts=config.compression_level
                )
                f.attrs['dataset_name'] = config.dataset_name
                f.attrs['creation_time'] = datetime.now().isoformat()
                f.attrs['num_labels'] = len(labels)
            
            file_paths['labels'] = labels_path
            file_sizes['labels'] = os.path.getsize(labels_path) / (1024 * 1024)
        
        logger.info(f"Dataset saved: {sum(file_sizes.values()):.2f} MB total")
        
        return file_paths, file_sizes
    
    def _create_metadata(self,
                        config: EnhancedTrainingConfig,
                        sequences: Dict[str, np.ndarray],
                        labels: Optional[np.ndarray],
                        feature_data: Dict[str, np.ndarray],
                        quality_metrics: Dict[str, Any],
                        file_paths: Dict[str, str],
                        file_sizes: Dict[str, float],
                        duration: float,
                        processing_stages: List[Dict[str, Any]]) -> TrainingDatasetMetadata:
        """Create comprehensive metadata for the dataset."""
        
        # Feature registry export
        feature_registry_data = {
            name: spec.to_dict() 
            for name, spec in self.feature_registry.feature_specs.items()
            if name in feature_data
        }
        
        # Feature shapes and types
        feature_shapes = {name: data.shape for name, data in sequences.items()}
        feature_types = {
            name: self.feature_registry.get_feature_spec(name).feature_type.value
            for name in sequences.keys()
            if self.feature_registry.get_feature_spec(name)
        }
        
        total_samples = len(list(sequences.values())[0]) if sequences else 0
        
        metadata = TrainingDatasetMetadata(
            dataset_name=config.dataset_name,
            creation_timestamp=datetime.now(),
            config=config,
            total_samples=total_samples,
            symbols_count=len(config.symbols),
            date_range=(config.start_date, config.end_date),
            feature_registry=feature_registry_data,
            feature_shapes=feature_shapes,
            feature_types=feature_types,
            data_quality_score=quality_metrics.get("data_quality_score", 0.0),
            missing_data_ratio=quality_metrics.get("missing_data_ratio", 0.0),
            outliers_removed=quality_metrics.get("outliers_removed", 0),
            file_paths=file_paths,
            file_sizes_mb=file_sizes,
            compression_info={
                "compression_type": "gzip",
                "compression_level": config.compression_level,
                "shuffle": True
            },
            generation_duration_seconds=duration,
            processing_stages=processing_stages
        )
        
        return metadata


# Demo and testing functions
async def demo_with_real_database():
    """Demonstrate enhanced training data generation with real database."""
    
    # Database configuration (update with your actual credentials)
    DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/dev_db"
    
    generator = EnhancedTrainingDataGenerator(DATABASE_URL)
    
    try:
        await generator.initialize()
        
        # Get available symbols from database
        async with generator.db_pool.acquire() as conn:
            symbols_result = await conn.fetch("""
                SELECT DISTINCT i.symbol 
                FROM dev_instruments i 
                JOIN dev_daily_prices dp ON i.id = dp.instrument_id 
                WHERE dp.date >= '2024-01-01'
                ORDER BY i.symbol 
                LIMIT 5
            """)
            available_symbols = [row['symbol'] for row in symbols_result]
        
        if not available_symbols:
            print("No symbols found in database. Using demo configuration...")
            available_symbols = ['AAPL', 'TSLA']  # Fallback
        
        print(f"Using symbols: {available_symbols}")
        
        # Create feature specifications 
        feature_specs = [
            # OHLC features
            generator.feature_registry.get_feature_spec("ohlc_5min_8"),
            generator.feature_registry.get_feature_spec("ohlc_daily_16"),
            
            # Technical indicators
            generator.feature_registry.get_feature_spec("etop_5min_8"),
            generator.feature_registry.get_feature_spec("ebot_daily_16"),
            
            # Cross-timeframe
            generator.feature_registry.get_feature_spec("etop_1hour_on_5min")
        ]
        
        # Filter out None values
        feature_specs = [spec for spec in feature_specs if spec is not None]
        
        if not feature_specs:
            print("No valid feature specs found. Check feature registry.")
            return
        
        print(f"Using {len(feature_specs)} features:")
        for spec in feature_specs:
            print(f"  - {spec.name}: {spec.dimensions}")
        
        # Create configuration
        config = EnhancedTrainingConfig(
            symbols=available_symbols,
            start_date="2024-01-01",
            end_date="2024-01-31",
            feature_specs=feature_specs,
            sequence_length=32,
            prediction_horizon=1,
            output_dir="demo_training_data",
            dataset_name="demo_enhanced_dataset",
            description="Demo enhanced training dataset with real market data"
        )
        
        print(f"\n=== Generating Enhanced Training Dataset ===")
        print(f"Dataset: {config.dataset_name}")
        print(f"Symbols: {config.symbols}")
        print(f"Date range: {config.start_date} to {config.end_date}")
        print(f"Features: {len(config.feature_specs)}")
        
        # Generate dataset
        metadata = await generator.generate_training_dataset(config)
        
        print(f"\n=== Generation Complete ===")
        print(f"Total samples: {metadata.total_samples}")
        print(f"Data quality score: {metadata.data_quality_score:.3f}")
        print(f"Generation time: {metadata.generation_duration_seconds:.2f} seconds")
        print(f"Total file size: {sum(metadata.file_sizes_mb.values()):.2f} MB")
        
        print(f"\nFeature shapes:")
        for name, shape in metadata.feature_shapes.items():
            print(f"  - {name}: {shape}")
        
        print(f"\nFile paths:")
        for file_type, path in metadata.file_paths.items():
            print(f"  - {file_type}: {path}")
        
        return metadata
        
    except Exception as e:
        print(f"Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        await generator.close()


if __name__ == "__main__":
    # Run demo
    import logging
    logging.basicConfig(level=logging.INFO)
    
    print("=== Enhanced Training Data Generator Demo ===")
    print("This demo will connect to the real ATS database and generate training data.")
    print("Make sure the database is accessible at the configured URL.")
    
    result = asyncio.run(demo_with_real_database())
    
    if result:
        print(f"\n✅ Demo completed successfully!")
        print(f"Check the output directory for generated files.")
    else:
        print(f"\n❌ Demo failed. Check logs for details.")