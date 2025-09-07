#!/usr/bin/env python3
"""
Training Dataset API for EDA Integration

Provides endpoints for managing training datasets with TFDV statistics
and integration with the EDA dashboard.
"""

import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, date
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from domains.ml.services.training_data.dao.training_dataset_dao import TrainingDatasetDAO, TrainingDatasetRecord
from shared.utils.environment import Environment

logger = logging.getLogger(__name__)

# Pydantic models for API requests/responses
class TrainingDatasetResponse(BaseModel):
    """Response model for training dataset."""
    id: int
    dataset_name: str
    run_id: Optional[int]
    total_sequences: int
    sequence_length: int
    feature_count: int
    label_count: int
    symbols: List[str]
    date_range_start: Optional[date]
    date_range_end: Optional[date]
    data_quality_score: float
    feature_completeness: float
    label_completeness: float
    generation_duration_seconds: int
    file_size_mb: float
    data_sources: List[str]
    status: str
    features_file_path: str
    labels_file_path: str
    metadata_file_path: str
    feature_metadata: str
    technical_indicators: str
    prediction_horizon: int
    created_by: str
    generation_parameters: Dict[str, Any]

    # TFDV stats
    tfdv_statistics: Dict[str, Any]
    tfdv_histogram_path: str
    tfdv_anomalies: Dict[str, Any]
    tfdv_schema_path: str
    feature_distributions: Dict[str, Any]
    label_distributions: Dict[str, Any]

    created_at: Optional[datetime]
    updated_at: Optional[datetime]

class TrainingDatasetListResponse(BaseModel):
    """Response model for training dataset list."""
    datasets: List[TrainingDatasetResponse]
    total_count: int
    page: int
    page_size: int

class TFDVStatsUpdate(BaseModel):
    """Request model for updating TFDV statistics."""
    tfdv_statistics: Dict[str, Any]
    histogram_path: Optional[str] = ""
    anomalies: Optional[Dict[str, Any]] = None
    feature_distributions: Optional[Dict[str, Any]] = None
    label_distributions: Optional[Dict[str, Any]] = None

def get_training_dataset_dao() -> TrainingDatasetDAO:
    """Dependency to get TrainingDatasetDAO instance."""
    env = Environment()
    return TrainingDatasetDAO(env)

def create_training_dataset_router():
    """Create router with training dataset endpoints."""
    router = APIRouter(prefix="/api/v1/training-datasets", tags=["training-datasets"])

    @router.get("/", response_model=TrainingDatasetListResponse)
    async def list_training_datasets(
        page: int = Query(1, ge=1, description="Page number"),
        page_size: int = Query(50, ge=1, le=100, description="Items per page"),
        status: Optional[str] = Query(None, description="Filter by status"),
        dao: TrainingDatasetDAO = Depends(get_training_dataset_dao)
    ):
        """List training datasets with pagination and optional filtering."""
        try:
            offset = (page - 1) * page_size

            # Get datasets
            datasets = await dao.list_training_datasets(limit=page_size, offset=offset)

            # Filter by status if provided
            if status:
                datasets = [d for d in datasets if d.status == status]

            # Convert to response models
            dataset_responses = [
                TrainingDatasetResponse(
                    id=d.id,
                    dataset_name=d.dataset_name,
                    run_id=d.run_id,
                    total_sequences=d.total_sequences,
                    sequence_length=d.sequence_length,
                    feature_count=d.feature_count,
                    label_count=d.label_count,
                    symbols=d.symbols,
                    date_range_start=d.date_range_start,
                    date_range_end=d.date_range_end,
                    data_quality_score=d.data_quality_score,
                    feature_completeness=d.feature_completeness,
                    label_completeness=d.label_completeness,
                    generation_duration_seconds=d.generation_duration_seconds,
                    file_size_mb=d.file_size_mb,
                    data_sources=d.data_sources,
                    status=d.status,
                    features_file_path=d.features_file_path,
                    labels_file_path=d.labels_file_path,
                    metadata_file_path=d.metadata_file_path,
                    feature_metadata=d.feature_metadata,
                    technical_indicators=d.technical_indicators,
                    prediction_horizon=d.prediction_horizon,
                    created_by=d.created_by,
                    generation_parameters=d.generation_parameters,
                    tfdv_statistics=d.tfdv_statistics,
                    tfdv_histogram_path=d.tfdv_histogram_path,
                    tfdv_anomalies=d.tfdv_anomalies,
                    tfdv_schema_path=d.tfdv_schema_path,
                    feature_distributions=d.feature_distributions,
                    label_distributions=d.label_distributions,
                    created_at=d.created_at,
                    updated_at=d.updated_at
                )
                for d in datasets
            ]

            return TrainingDatasetListResponse(
                datasets=dataset_responses,
                total_count=len(dataset_responses),  # In real implementation, get actual count
                page=page,
                page_size=page_size
            )

        except Exception as e:
            logger.error(f"Error listing training datasets: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to list training datasets: {str(e)}")

    @router.get("/{dataset_id}", response_model=TrainingDatasetResponse)
    async def get_training_dataset(
        dataset_id: int,
        dao: TrainingDatasetDAO = Depends(get_training_dataset_dao)
    ):
        """Get a specific training dataset by ID."""
        try:
            dataset = await dao.get_training_dataset(dataset_id)

            if not dataset:
                raise HTTPException(status_code=404, detail="Training dataset not found")

            return TrainingDatasetResponse(
                id=dataset.id,
                dataset_name=dataset.dataset_name,
                run_id=dataset.run_id,
                total_sequences=dataset.total_sequences,
                sequence_length=dataset.sequence_length,
                feature_count=dataset.feature_count,
                label_count=dataset.label_count,
                symbols=dataset.symbols,
                date_range_start=dataset.date_range_start,
                date_range_end=dataset.date_range_end,
                data_quality_score=dataset.data_quality_score,
                feature_completeness=dataset.feature_completeness,
                label_completeness=dataset.label_completeness,
                generation_duration_seconds=dataset.generation_duration_seconds,
                file_size_mb=dataset.file_size_mb,
                data_sources=dataset.data_sources,
                status=dataset.status,
                features_file_path=dataset.features_file_path,
                labels_file_path=dataset.labels_file_path,
                metadata_file_path=dataset.metadata_file_path,
                feature_metadata=dataset.feature_metadata,
                technical_indicators=dataset.technical_indicators,
                prediction_horizon=dataset.prediction_horizon,
                created_by=dataset.created_by,
                generation_parameters=dataset.generation_parameters,
                tfdv_statistics=dataset.tfdv_statistics,
                tfdv_histogram_path=dataset.tfdv_histogram_path,
                tfdv_anomalies=dataset.tfdv_anomalies,
                tfdv_schema_path=dataset.tfdv_schema_path,
                feature_distributions=dataset.feature_distributions,
                label_distributions=dataset.label_distributions,
                created_at=dataset.created_at,
                updated_at=dataset.updated_at
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting training dataset {dataset_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to get training dataset: {str(e)}")

    @router.post("/{dataset_id}/tfdv-stats")
    async def update_tfdv_statistics(
        dataset_id: int,
        stats_update: TFDVStatsUpdate,
        dao: TrainingDatasetDAO = Depends(get_training_dataset_dao)
    ):
        """Update TFDV statistics for a training dataset."""
        try:
            success = await dao.update_tfdv_stats(
                dataset_id=dataset_id,
                tfdv_stats=stats_update.tfdv_statistics,
                histogram_path=stats_update.histogram_path,
                anomalies=stats_update.anomalies,
                feature_distributions=stats_update.feature_distributions,
                label_distributions=stats_update.label_distributions
            )

            if not success:
                raise HTTPException(status_code=404, detail="Training dataset not found")

            return {
                "status": "success",
                "message": f"TFDV statistics updated for dataset {dataset_id}",
                "dataset_id": dataset_id
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating TFDV stats for dataset {dataset_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to update TFDV statistics: {str(e)}")

    @router.get("/{dataset_id}/distributions")
    async def get_dataset_distributions(
        dataset_id: int,
        dao: TrainingDatasetDAO = Depends(get_training_dataset_dao)
    ):
        """Get feature and label distributions for a dataset (for EDA visualization)."""
        try:
            dataset = await dao.get_training_dataset(dataset_id)

            if not dataset:
                raise HTTPException(status_code=404, detail="Training dataset not found")

            return {
                "dataset_id": dataset_id,
                "dataset_name": dataset.dataset_name,
                "feature_distributions": dataset.feature_distributions,
                "label_distributions": dataset.label_distributions,
                "tfdv_statistics": dataset.tfdv_statistics,
                "data_quality_score": dataset.data_quality_score,
                "feature_completeness": dataset.feature_completeness,
                "label_completeness": dataset.label_completeness
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting distributions for dataset {dataset_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to get dataset distributions: {str(e)}")

    @router.get("/{dataset_id}/histogram")
    async def get_dataset_histogram(
        dataset_id: int,
        feature_name: Optional[str] = Query(None, description="Specific feature name"),
        dao: TrainingDatasetDAO = Depends(get_training_dataset_dao)
    ):
        """Get histogram data for dataset features (for EDA visualization)."""
        try:
            dataset = await dao.get_training_dataset(dataset_id)

            if not dataset:
                raise HTTPException(status_code=404, detail="Training dataset not found")

            # Return histogram path and relevant statistics
            histogram_data = {
                "dataset_id": dataset_id,
                "histogram_path": dataset.tfdv_histogram_path,
                "tfdv_statistics": dataset.tfdv_statistics
            }

            # If specific feature requested, filter statistics
            if feature_name and dataset.tfdv_statistics:
                feature_stats = dataset.tfdv_statistics.get("features", {}).get(feature_name, {})
                histogram_data["feature_statistics"] = feature_stats
                histogram_data["feature_name"] = feature_name

            return histogram_data

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting histogram for dataset {dataset_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to get dataset histogram: {str(e)}")

    @router.get("/{dataset_id}/visualization-data")
    async def get_dataset_visualization_data(
        dataset_id: int,
        start_idx: int = Query(0, description="Starting row index"),
        count: int = Query(21, description="Number of rows to return (for 10 before + 1 selected + 10 after)"),
        dao: TrainingDatasetDAO = Depends(get_training_dataset_dao)
    ):
        """Get training dataset data rows for interactive OHLC visualization (with technical indicators)."""
        try:
            dataset = await dao.get_training_dataset(dataset_id)

            if not dataset:
                raise HTTPException(status_code=404, detail="Training dataset not found")

            # Load the features data from the numpy file
            import numpy as np
            import os

            if not os.path.exists(dataset.features_file_path):
                raise HTTPException(status_code=404, detail="Features file not found")

            # Load features array [sequences, time_steps, features]
            features_data = np.load(dataset.features_file_path)

            # Calculate the selected sequence index and time step within sequence
            sequence_idx = start_idx // dataset.sequence_length
            time_step_in_sequence = start_idx % dataset.sequence_length

            # Ensure we don't go out of bounds
            if sequence_idx >= features_data.shape[0]:
                raise HTTPException(status_code=400, detail="Start index out of bounds")

            # Extract data around the selected point (10 before, current, 10 after)
            half_window = count // 2
            start_time_step = max(0, time_step_in_sequence - half_window)
            end_time_step = min(dataset.sequence_length, time_step_in_sequence + half_window + 1)

            # Get the data slice
            data_slice = features_data[sequence_idx, start_time_step:end_time_step, :]

            # Parse feature names from metadata to understand column mapping
            feature_metadata = json.loads(dataset.feature_metadata) if dataset.feature_metadata else {}
            feature_names = feature_metadata.get('feature_names', [
                'open', 'high', 'low', 'close', 'volume', 'etop', 'ebot', 'pldot', 'z1b', 'z2b', 'z5t', 'z6t'
            ])

            # Create visualization data structure
            visualization_data = []
            for i, row in enumerate(data_slice):
                row_data = {
                    'index': start_time_step + i,
                    'is_selected': (start_time_step + i) == time_step_in_sequence
                }

                # Map features to their names
                for j, feature_name in enumerate(feature_names):
                    if j < len(row):
                        row_data[feature_name] = float(row[j])

                visualization_data.append(row_data)

            return {
                'data': visualization_data,
                'sequence_idx': sequence_idx,
                'selected_time_step': time_step_in_sequence,
                'total_sequences': features_data.shape[0],
                'sequence_length': dataset.sequence_length,
                'feature_names': feature_names
            }

        except Exception as e:
            logger.error(f"Error getting dataset data rows: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to get dataset data rows: {str(e)}")

    return router