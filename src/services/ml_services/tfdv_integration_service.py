#!/usr/bin/env python3
"""
TensorFlow Data Validation (TFDV) Integration Service

Provides TFDV statistics generation and anomaly detection for training datasets.
Integrates with the EDA dashboard for comprehensive data quality analysis.
"""

import os
import json
import logging
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor

# TFDV imports with fallback
try:
    import tensorflow_data_validation as tfdv
    from tensorflow_metadata.proto.v0 import statistics_pb2
    from tensorflow_metadata.proto.v0 import schema_pb2
    TFDV_AVAILABLE = True
except ImportError:
    TFDV_AVAILABLE = False
    logging.warning("TensorFlow Data Validation not available. Using mock implementation.")

from ml.training_data.dao.training_dataset_dao import TrainingDatasetDAO, TrainingDatasetRecord

logger = logging.getLogger(__name__)

class TFDVIntegrationService:
    """Service for integrating TFDV statistics with training datasets."""
    
    def __init__(self, output_dir: str = "/tmp/tfdv_stats"):
        """Initialize TFDV service."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        if not TFDV_AVAILABLE:
            logger.warning("TFDV not available - using mock statistics")
    
    async def compute_dataset_statistics(self, 
                                       features: np.ndarray, 
                                       labels: np.ndarray,
                                       feature_names: List[str],
                                       label_names: List[str],
                                       dataset_name: str) -> Dict[str, Any]:
        """Compute comprehensive TFDV statistics for a dataset."""
        try:
            if TFDV_AVAILABLE:
                return await self._compute_real_statistics(
                    features, labels, feature_names, label_names, dataset_name
                )
            else:
                return await self._compute_mock_statistics(
                    features, labels, feature_names, label_names, dataset_name
                )
                
        except Exception as e:
            logger.error(f"Error computing dataset statistics: {e}")
            return self._get_error_statistics(str(e))
    
    async def _compute_real_statistics(self, 
                                     features: np.ndarray,
                                     labels: np.ndarray, 
                                     feature_names: List[str],
                                     label_names: List[str],
                                     dataset_name: str) -> Dict[str, Any]:
        """Compute real TFDV statistics."""
        loop = asyncio.get_event_loop()
        
        return await loop.run_in_executor(
            self.executor, 
            self._compute_tfdv_stats_sync,
            features, labels, feature_names, label_names, dataset_name
        )
    
    def _compute_tfdv_stats_sync(self, 
                               features: np.ndarray,
                               labels: np.ndarray,
                               feature_names: List[str], 
                               label_names: List[str],
                               dataset_name: str) -> Dict[str, Any]:
        """Synchronous TFDV computation."""
        try:
            # Flatten features for TFDV (samples, features)
            if len(features.shape) == 3:  # (samples, timesteps, features)
                # Take the last timestep for each sequence
                features_flat = features[:, -1, :]
            else:
                features_flat = features
            
            # Create DataFrame for TFDV
            feature_df = pd.DataFrame(features_flat, columns=feature_names)
            label_df = pd.DataFrame(labels, columns=label_names)
            combined_df = pd.concat([feature_df, label_df], axis=1)
            
            # Generate statistics
            stats = tfdv.generate_statistics_from_dataframe(combined_df)
            
            # Convert to dictionary format
            stats_dict = self._proto_to_dict(stats)
            
            # Generate schema
            schema = tfdv.infer_schema(statistics=stats)
            schema_dict = self._schema_to_dict(schema)
            
            # Detect anomalies (if we have a baseline)
            anomalies = {}  # Would compare with baseline in real implementation
            
            # Generate histograms and save to files
            histogram_path = self._generate_histograms(combined_df, dataset_name)
            
            # Compute feature distributions
            feature_distributions = self._compute_feature_distributions(feature_df)
            label_distributions = self._compute_feature_distributions(label_df)
            
            return {
                "statistics": stats_dict,
                "schema": schema_dict,
                "anomalies": anomalies,
                "histogram_path": str(histogram_path),
                "feature_distributions": feature_distributions,
                "label_distributions": label_distributions,
                "computation_timestamp": pd.Timestamp.now().isoformat(),
                "tfdv_version": tfdv.__version__ if hasattr(tfdv, '__version__') else "unknown"
            }
            
        except Exception as e:
            logger.error(f"Error in TFDV computation: {e}")
            return self._get_error_statistics(str(e))
    
    async def _compute_mock_statistics(self,
                                     features: np.ndarray,
                                     labels: np.ndarray,
                                     feature_names: List[str], 
                                     label_names: List[str],
                                     dataset_name: str) -> Dict[str, Any]:
        """Compute mock statistics when TFDV is not available."""
        loop = asyncio.get_event_loop()
        
        return await loop.run_in_executor(
            self.executor,
            self._compute_mock_stats_sync,
            features, labels, feature_names, label_names, dataset_name
        )
    
    def _compute_mock_stats_sync(self,
                               features: np.ndarray,
                               labels: np.ndarray, 
                               feature_names: List[str],
                               label_names: List[str],
                               dataset_name: str) -> Dict[str, Any]:
        """Synchronous mock statistics computation."""
        try:
            # Flatten features if needed
            if len(features.shape) == 3:
                features_flat = features[:, -1, :]
            else:
                features_flat = features
                
            # Create basic statistics
            feature_stats = {}
            for i, name in enumerate(feature_names):
                if i < features_flat.shape[1]:
                    col_data = features_flat[:, i]
                    feature_stats[name] = {
                        "count": len(col_data),
                        "mean": float(np.mean(col_data)),
                        "std": float(np.std(col_data)),
                        "min": float(np.min(col_data)),
                        "max": float(np.max(col_data)),
                        "median": float(np.median(col_data)),
                        "q25": float(np.percentile(col_data, 25)),
                        "q75": float(np.percentile(col_data, 75)),
                        "missing_count": int(np.sum(np.isnan(col_data))),
                        "unique_count": len(np.unique(col_data[~np.isnan(col_data)]))
                    }
            
            label_stats = {}
            for i, name in enumerate(label_names):
                if i < labels.shape[1]:
                    col_data = labels[:, i]
                    label_stats[name] = {
                        "count": len(col_data),
                        "mean": float(np.mean(col_data)),
                        "std": float(np.std(col_data)),
                        "min": float(np.min(col_data)),
                        "max": float(np.max(col_data)),
                        "median": float(np.median(col_data)),
                        "q25": float(np.percentile(col_data, 25)),
                        "q75": float(np.percentile(col_data, 75)),
                        "missing_count": int(np.sum(np.isnan(col_data))),
                        "unique_count": len(np.unique(col_data[~np.isnan(col_data)]))
                    }
            
            # Generate mock histogram path
            histogram_path = self.output_dir / f"{dataset_name}_mock_histograms.json"
            with open(histogram_path, 'w') as f:
                json.dump({
                    "features": feature_stats,
                    "labels": label_stats,
                    "note": "Mock histograms - TFDV not available"
                }, f, indent=2)
            
            return {
                "statistics": {
                    "features": feature_stats,
                    "labels": label_stats,
                    "dataset_level": {
                        "num_examples": features.shape[0],
                        "num_features": len(feature_names),
                        "num_labels": len(label_names)
                    }
                },
                "schema": {
                    "feature": [{"name": name, "type": "FLOAT"} for name in feature_names],
                    "label": [{"name": name, "type": "FLOAT"} for name in label_names]
                },
                "anomalies": {},
                "histogram_path": str(histogram_path),
                "feature_distributions": feature_stats,
                "label_distributions": label_stats,
                "computation_timestamp": pd.Timestamp.now().isoformat(),
                "tfdv_version": "mock_implementation"
            }
            
        except Exception as e:
            logger.error(f"Error in mock computation: {e}")
            return self._get_error_statistics(str(e))
    
    def _proto_to_dict(self, proto_stats) -> Dict[str, Any]:
        """Convert TFDV proto statistics to dictionary."""
        # This would convert the protobuf to a JSON-serializable dict
        # Simplified implementation
        return {
            "datasets": [],  # Would extract actual dataset stats
            "conversion_note": "Proto conversion not fully implemented"
        }
    
    def _schema_to_dict(self, schema) -> Dict[str, Any]:
        """Convert TFDV schema to dictionary."""
        # This would convert the schema protobuf to dict
        return {
            "feature": [],  # Would extract actual schema
            "conversion_note": "Schema conversion not fully implemented"
        }
    
    def _generate_histograms(self, df: pd.DataFrame, dataset_name: str) -> Path:
        """Generate histogram visualizations."""
        histogram_path = self.output_dir / f"{dataset_name}_histograms.json"
        
        # Generate histogram data for each column
        histogram_data = {}
        for col in df.columns:
            if df[col].dtype in ['float64', 'int64']:
                hist, bins = np.histogram(df[col].dropna(), bins=20)
                histogram_data[col] = {
                    "histogram": hist.tolist(),
                    "bins": bins.tolist(),
                    "type": "numeric"
                }
            else:
                value_counts = df[col].value_counts().head(20)
                histogram_data[col] = {
                    "categories": value_counts.index.tolist(),
                    "counts": value_counts.values.tolist(),
                    "type": "categorical"
                }
        
        with open(histogram_path, 'w') as f:
            json.dump(histogram_data, f, indent=2)
        
        return histogram_path
    
    def _compute_feature_distributions(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Compute distribution summaries for features."""
        distributions = {}
        
        for col in df.columns:
            if df[col].dtype in ['float64', 'int64']:
                distributions[col] = {
                    "type": "numeric",
                    "mean": df[col].mean(),
                    "std": df[col].std(),
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "percentiles": {
                        "5": df[col].quantile(0.05),
                        "25": df[col].quantile(0.25),
                        "50": df[col].quantile(0.50),
                        "75": df[col].quantile(0.75),
                        "95": df[col].quantile(0.95)
                    }
                }
            else:
                value_counts = df[col].value_counts()
                distributions[col] = {
                    "type": "categorical",
                    "unique_count": len(value_counts),
                    "top_values": dict(value_counts.head(10))
                }
        
        return distributions
    
    def _get_error_statistics(self, error_message: str) -> Dict[str, Any]:
        """Return error statistics when computation fails."""
        return {
            "statistics": {},
            "schema": {},
            "anomalies": {},
            "histogram_path": "",
            "feature_distributions": {},
            "label_distributions": {},
            "error": error_message,
            "computation_timestamp": pd.Timestamp.now().isoformat()
        }
    
    async def process_training_dataset_file(self, 
                                          features_file: str,
                                          labels_file: str, 
                                          metadata_file: str,
                                          dataset_name: str) -> Dict[str, Any]:
        """Process training dataset files and compute TFDV statistics."""
        try:
            # Load the training data
            if features_file.endswith('.npy'):
                features = np.load(features_file)
                labels = np.load(labels_file)
            else:
                raise ValueError(f"Unsupported file format: {features_file}")
            
            # Load metadata to get feature/label names
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            feature_names = metadata.get('feature_names', [f'feature_{i}' for i in range(features.shape[-1])])
            label_names = metadata.get('label_names', [f'label_{i}' for i in range(labels.shape[-1])])
            
            # Compute statistics
            stats = await self.compute_dataset_statistics(
                features, labels, feature_names, label_names, dataset_name
            )
            
            return stats
            
        except Exception as e:
            logger.error(f"Error processing training dataset files: {e}")
            return self._get_error_statistics(str(e))
    
    async def update_dataset_tfdv_stats(self, 
                                      dao: TrainingDatasetDAO,
                                      dataset_id: int,
                                      features_file: str, 
                                      labels_file: str,
                                      metadata_file: str) -> bool:
        """Update TFDV statistics for a dataset in the database."""
        try:
            # Get dataset record
            dataset = await dao.get_training_dataset(dataset_id)
            if not dataset:
                logger.error(f"Dataset {dataset_id} not found")
                return False
            
            # Compute TFDV statistics
            stats = await self.process_training_dataset_file(
                features_file, labels_file, metadata_file, dataset.dataset_name
            )
            
            # Update database with new statistics
            success = await dao.update_tfdv_stats(
                dataset_id=dataset_id,
                tfdv_stats=stats.get("statistics", {}),
                histogram_path=stats.get("histogram_path", ""),
                anomalies=stats.get("anomalies", {}),
                feature_distributions=stats.get("feature_distributions", {}),
                label_distributions=stats.get("label_distributions", {})
            )
            
            if success:
                logger.info(f"Updated TFDV statistics for dataset {dataset_id}")
            else:
                logger.error(f"Failed to update TFDV statistics for dataset {dataset_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error updating dataset TFDV stats: {e}")
            return False