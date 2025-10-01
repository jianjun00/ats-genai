#!/usr/bin/env python3
"""
Dataset Client - High-level interface for training and EDA to interact with dataset service
Provides simplified methods for common dataset operations without exposing service internals.
"""

import logging
from typing import Dict, List, Optional, Any, Iterator, Tuple
import numpy as np
import pandas as pd

from domains.ml.services.training_data.dao.training_dataset_dao import TrainingDatasetDAO
from domains.ml.services.training_data.apis.training_dataset_api import TrainingDatasetAPI

# Legacy compatibility - migrate to domains-based services
class DatasetMetadata:
    """Legacy compatibility class - should migrate to modern domains-based DTOs."""
    pass

class DatasetService:
    """Legacy compatibility class - should migrate to TrainingDatasetDAO."""
    def __init__(self, db_config=None):
        self.training_dataset_dao = TrainingDatasetDAO()
        self.training_dataset_api = TrainingDatasetAPI()

logger = logging.getLogger(__name__)

class DatasetClient:
    """High-level client for dataset operations used by training and EDA."""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """Initialize dataset client."""
        self.service = DatasetService(db_config)
        self._current_dataset = None
        logger.info("✅ Dataset Client initialized")

    def find_dataset(self,
                    name: Optional[str] = None,
                    symbols: Optional[List[str]] = None,
                    min_sequences: Optional[int] = None,
                    min_quality: Optional[float] = None) -> Optional[DatasetMetadata]:
        """Find best matching dataset based on criteria."""

        try:
            if name:
                # Direct name lookup
                dataset = self.service.get_dataset_by_name(name)
                if dataset:
                    logger.info(f"✅ Found dataset by name: {dataset.dataset_name}")
                    return dataset
                else:
                    logger.warning(f"⚠️ No dataset found with name: {name}")

            # Search by symbols
            try:
                candidates = self.service.list_datasets(symbols=symbols, limit=50)
            except Exception as e:
                logger.error(f"❌ Failed to list datasets: {e}")
                return None

            if not candidates:
                logger.warning(f"⚠️ No datasets found for symbols: {symbols}")
                return None

            # Apply filters
            filtered_candidates = []
            for candidate in candidates:
                try:
                    if min_sequences and candidate.total_sequences < min_sequences:
                        continue
                    if min_quality and candidate.data_quality_score < min_quality:
                        continue
                    filtered_candidates.append(candidate)
                except AttributeError as e:
                    logger.warning(f"⚠️ Skipping invalid dataset metadata: {e}")
                    continue

            if not filtered_candidates:
                logger.warning(f"⚠️ No datasets found matching criteria: symbols={symbols}, min_sequences={min_sequences}, min_quality={min_quality}")
                return None

            # Return the most recent high-quality dataset
            try:
                best_candidate = max(filtered_candidates,
                               key=lambda d: (d.data_quality_score, d.total_sequences, d.creation_timestamp))

                logger.info(f"✅ Selected best dataset: {best_candidate.dataset_name} (quality={best_candidate.data_quality_score:.3f})")
                return best_candidate
            except (ValueError, AttributeError) as e:
                logger.error(f"❌ Failed to select best dataset: {e}")
                return None

        except Exception as e:
            logger.error(f"❌ Unexpected error in find_dataset: {e}")
            return None

    def get_training_data_config(self,
                               symbols: List[str],
                               min_sequences: int = 1000,
                               preferred_timeframes: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """Get configuration for training data loading."""

        try:
            if not symbols:
                logger.error("❌ No symbols provided for training data config")
                return None

            dataset = self.find_dataset(
                symbols=symbols,
                min_sequences=min_sequences,
                min_quality=0.7
            )

            if not dataset:
                logger.error(f"❌ No suitable dataset found for symbols {symbols}")
                return None

            # Get file iterators with error handling
            try:
                iterators = self.service.get_file_iterators(dataset.dataset_id)
            except Exception as e:
                logger.error(f"❌ Failed to get file iterators for dataset {dataset.dataset_id}: {e}")
                return None

            if not iterators:
                logger.error(f"❌ No accessible files found for dataset {dataset.dataset_id}")
                return None

            # Validate files are accessible with error handling
            try:
                validation = self.service.validate_dataset_availability(dataset.dataset_id)
            except Exception as e:
                logger.error(f"❌ Failed to validate dataset {dataset.dataset_id}: {e}")
                return None

            if not validation['valid']:
                logger.error(f"❌ Dataset validation failed: {validation}")
                missing_files = validation.get('missing_files', [])
                if missing_files:
                    logger.error(f"   Missing files: {missing_files}")
                return None

            # Build configuration with error handling for each field
            try:
                config = {
                    'dataset_id': dataset.dataset_id,
                    'dataset_name': dataset.dataset_name,
                    'symbols': dataset.symbols if dataset.symbols else [],
                    'file_paths': [it.file_path for it in iterators if hasattr(it, 'file_path')],
                    'total_sequences': max(dataset.total_sequences, 0) if dataset.total_sequences else 0,
                    'total_records': max(dataset.total_records, 0) if dataset.total_records else 0,
                    'feature_count': max(dataset.feature_count, 1) if dataset.feature_count else 1,
                    'sequence_length': max(dataset.sequence_length, 1) if dataset.sequence_length else 100,
                    'batch_size_recommendation': max([it.batch_size_recommendation for it in iterators if hasattr(it, 'batch_size_recommendation')], default=32),
                    'estimated_memory_mb': sum([it.estimated_memory_mb for it in iterators if hasattr(it, 'estimated_memory_mb')]),
                    'file_format': dataset.file_format if dataset.file_format else 'unknown',
                    'data_quality_score': max(min(dataset.data_quality_score, 1.0), 0.0) if dataset.data_quality_score is not None else 0.0,
                    'technical_indicators': dataset.technical_indicators if dataset.technical_indicators else [],
                    'timeframes': dataset.timeframes if dataset.timeframes else ['1h'],
                    'date_range': {
                        'start': dataset.date_range_start if dataset.date_range_start else '1970-01-01',
                        'end': dataset.date_range_end if dataset.date_range_end else '2025-12-31'
                    },
                    'iterator_configs': []
                }

                # Safely get iterator configs
                for iterator in iterators:
                    try:
                        iterator_config = iterator.get_iterator_config()
                        config['iterator_configs'].append(iterator_config)
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to get config for iterator {iterator.file_path}: {e}")
                        continue

                self._current_dataset = dataset
                logger.info(f"✅ Training data config ready: {dataset.dataset_name}")
                logger.info(f"   📊 {config['total_sequences']} sequences, {config['feature_count']} features")
                logger.info(f"   💾 {config['estimated_memory_mb']:.1f} MB estimated memory")
                logger.info(f"   🎯 Quality score: {config['data_quality_score']:.3f}")

                return config

            except Exception as e:
                logger.error(f"❌ Failed to build training data config: {e}")
                return None

        except Exception as e:
            logger.error(f"❌ Unexpected error in get_training_data_config: {e}")
            return None


    def create_data_loader(self, config: Dict[str, Any]) -> Optional['DatasetLoader']:
        """Create a data loader from dataset configuration."""
        try:
            if not config:
                logger.error("❌ No configuration provided for data loader")
                return None

            required_keys = ['dataset_id', 'dataset_name', 'iterator_configs']
            missing_keys = [key for key in required_keys if key not in config]

            if missing_keys:
                logger.error(f"❌ Missing required config keys: {missing_keys}")
                return None

            return DatasetLoader(config, self.service)
        except Exception as e:
            logger.error(f"❌ Failed to create data loader: {e}")
            return None

    def get_dataset_summary(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """Get human-readable summary of dataset."""

        try:
            if not isinstance(dataset_id, int) or dataset_id <= 0:
                logger.error(f"❌ Invalid dataset_id: {dataset_id}")
                return None

            stats = self.service.get_dataset_statistics(dataset_id)
            if not stats:
                logger.warning(f"⚠️ No statistics found for dataset {dataset_id}")
                return None

            # Safely extract summary with defaults
            summary = {}

            try:
                summary['name'] = stats.get('dataset_info', {}).get('name', f'dataset_{dataset_id}')
                summary['symbols'] = ', '.join(stats.get('dataset_info', {}).get('symbols', []))

                creation_date = stats.get('dataset_info', {}).get('creation_date', '')
                summary['created'] = creation_date[:10] if len(creation_date) >= 10 else 'Unknown'

                sequences = stats.get('data_volume', {}).get('total_sequences', 0)
                summary['size'] = f"{sequences:,} sequences" if sequences > 0 else "No sequences"

                quality = stats.get('data_quality', {}).get('quality_score', 0.0)
                summary['quality'] = f"{quality:.1%}" if isinstance(quality, (int, float)) else "Unknown"

                memory_mb = stats.get('data_volume', {}).get('estimated_memory_mb', 0.0)
                summary['memory'] = f"{memory_mb:.0f} MB" if memory_mb > 0 else "Unknown"

                file_count = stats.get('data_volume', {}).get('file_count', 0)
                summary['files'] = f"{file_count} files" if file_count > 0 else "No files"

                timeframes = stats.get('data_characteristics', {}).get('timeframes', [])
                summary['timeframes'] = ', '.join(timeframes) if timeframes else "Unknown"

                indicators = stats.get('data_characteristics', {}).get('technical_indicators', [])
                summary['indicators'] = len(indicators) if indicators else 0

                summary['date_range'] = stats.get('processing_info', {}).get('date_range', 'Unknown')

            except KeyError as e:
                logger.warning(f"⚠️ Missing expected field in dataset statistics: {e}")
                # Return partial summary
                summary.setdefault('name', f'dataset_{dataset_id}')
                summary.setdefault('symbols', 'Unknown')
                summary.setdefault('created', 'Unknown')
                summary.setdefault('size', 'Unknown')
                summary.setdefault('quality', 'Unknown')
                summary.setdefault('memory', 'Unknown')
                summary.setdefault('files', 'Unknown')
                summary.setdefault('timeframes', 'Unknown')
                summary.setdefault('indicators', 0)
                summary.setdefault('date_range', 'Unknown')

            return summary

        except Exception as e:
            logger.error(f"❌ Failed to get dataset summary for {dataset_id}: {e}")
            return None

    def list_available_datasets(self, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """List available datasets with summaries."""

        try:
            datasets = self.service.list_datasets(symbols=symbols, limit=20)

            if not datasets:
                logger.info(f"📋 No datasets found for symbols: {symbols}")
                return []

            summaries = []

            for dataset in datasets:
                try:
                    summary = self.get_dataset_summary(dataset.dataset_id)
                    if summary:
                        summary['id'] = dataset.dataset_id
                        summaries.append(summary)
                    else:
                        logger.warning(f"⚠️ Could not get summary for dataset {dataset.dataset_id}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to get summary for dataset {dataset.dataset_id}: {e}")
                    continue

            logger.info(f"📋 Found {len(summaries)} datasets for symbols: {symbols}")
            return summaries

        except Exception as e:
            logger.error(f"❌ Failed to list available datasets: {e}")
            return []

    def validate_dataset_for_training(self, dataset_id: int,
                                    required_features: int,
                                    min_sequences: int) -> Dict[str, Any]:
        """Validate dataset meets training requirements."""

        metadata = self.service.get_dataset_metadata(dataset_id)
        if not metadata:
            return {'valid': False, 'error': 'Dataset not found'}

        validation = self.service.validate_dataset_availability(dataset_id)

        checks = {
            'files_accessible': validation['valid'],
            'sufficient_sequences': metadata.total_sequences >= min_sequences,
            'sufficient_features': metadata.feature_count >= required_features,
            'good_quality': metadata.data_quality_score >= 0.7,
            'has_labels': metadata.label_count > 0
        }

        result = {
            'valid': all(checks.values()),
            'dataset_name': metadata.dataset_name,
            'checks': checks,
            'details': {
                'sequences': f"{metadata.total_sequences} (required: {min_sequences})",
                'features': f"{metadata.feature_count} (required: {required_features})",
                'quality': f"{metadata.data_quality_score:.3f} (required: 0.7)",
                'files': f"{validation['accessible_files']}/{validation['total_files']} accessible"
            }
        }

        if result['valid']:
            logger.info(f"✅ Dataset {dataset_id} validated for training")
        else:
            failed_checks = [check for check, passed in checks.items() if not passed]
            logger.warning(f"⚠️ Dataset {dataset_id} validation failed: {failed_checks}")

        return result

class DatasetLoader:
    """Data loader that uses dataset service metadata."""

    def __init__(self, config: Dict[str, Any], service: DatasetService):
        self.config = config
        self.service = service
        self._current_file_idx = 0
        logger.info(f"✅ DatasetLoader initialized for {config['dataset_name']}")

    def get_batch_iterator(self, batch_size: Optional[int] = None) -> Iterator[Tuple[np.ndarray, np.ndarray]]:
        """Create batch iterator over dataset files."""

        effective_batch_size = batch_size or self.config['batch_size_recommendation']

        for file_config in self.config['iterator_configs']:
            file_path = file_config['file_path']

            try:
                if file_path.endswith('.npy'):
                    data = np.load(file_path)
                    logger.info(f"📂 Loaded {file_path}: {data.shape}")

                    # Yield batches
                    for i in range(0, len(data), effective_batch_size):
                        batch = data[i:i + effective_batch_size]
                        if len(batch.shape) > 2:
                            # Assume last column is target
                            X = batch[:, :, :-1]
                            y = batch[:, -1, -1] if batch.shape[2] > 1 else batch[:, 0, -1]
                        else:
                            X = batch[:, :-1]
                            y = batch[:, -1]
                        yield X, y

                elif file_path.endswith('.parquet'):
                    df = pd.read_parquet(file_path)
                    logger.info(f"📂 Loaded {file_path}: {df.shape}")

                    # Convert to numpy and yield batches
                    data = df.values
                    for i in range(0, len(data), effective_batch_size):
                        batch = data[i:i + effective_batch_size]
                        X = batch[:, :-1]  # All columns except last
                        y = batch[:, -1]   # Last column as target
                        yield X, y

                else:
                    logger.warning(f"⚠️ Unsupported file format: {file_path}")
                    continue

            except Exception as e:
                logger.error(f"❌ Failed to load {file_path}: {e}")
                continue

    def get_full_dataset(self) -> Tuple[np.ndarray, np.ndarray]:
        """Load entire dataset into memory (use cautiously)."""

        if self.config['estimated_memory_mb'] > 2000:  # 2GB limit
            logger.warning(f"⚠️ Large dataset ({self.config['estimated_memory_mb']:.0f} MB), consider using batch iterator")

        X_list, y_list = [], []

        for X_batch, y_batch in self.get_batch_iterator():
            X_list.append(X_batch)
            y_list.append(y_batch)

        if X_list:
            X_full = np.vstack(X_list)
            y_full = np.concatenate(y_list)
            logger.info(f"✅ Loaded full dataset: X{X_full.shape}, y{y_full.shape}")
            return X_full, y_full
        else:
            logger.error("❌ No data loaded")
            return np.array([]), np.array([])

    def get_sample(self, sample_size: int = 1000) -> Tuple[np.ndarray, np.ndarray]:
        """Get a random sample from the dataset."""

        # Collect samples from batch iterator
        collected_X, collected_y = [], []
        collected_count = 0

        for X_batch, y_batch in self.get_batch_iterator():
            if collected_count >= sample_size:
                break

            remaining = sample_size - collected_count
            if len(X_batch) > remaining:
                # Randomly sample from this batch
                indices = np.random.choice(len(X_batch), remaining, replace=False)
                X_batch = X_batch[indices]
                y_batch = y_batch[indices]

            collected_X.append(X_batch)
            collected_y.append(y_batch)
            collected_count += len(X_batch)

        if collected_X:
            X_sample = np.vstack(collected_X)
            y_sample = np.concatenate(collected_y)
            logger.info(f"✅ Created sample: {len(X_sample)} records")
            return X_sample, y_sample
        else:
            return np.array([]), np.array([])

    def get_metadata(self) -> Dict[str, Any]:
        """Get dataset metadata."""
        return self.config.copy()