#!/usr/bin/env python3
"""
Simplified Training Dataset API for EDA Integration
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import asyncpg
from datetime import datetime, date
import logging
import json
from src.core.shared.utils.environment import Environment

logger = logging.getLogger(__name__)

router = APIRouter()

# Response Models
class TrainingDatasetInfo(BaseModel):
    id: int
    dataset_name: str
    total_sequences: int = 0
    feature_count: int = 0
    label_count: int = 0
    data_quality_score: float = 0.0
    feature_completeness: float = 0.0
    label_completeness: float = 0.0
    file_size_mb: float = 0.0
    technical_indicators: Optional[str] = ""
    symbols: List[str] = []
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None
    created_at: Optional[datetime] = None

class TrainingDatasetListResponse(BaseModel):
    datasets: List[TrainingDatasetInfo]
    total_count: int

class TrainingDatasetDistributions(BaseModel):
    data_quality_score: float
    feature_completeness: float
    label_completeness: float
    feature_distributions: Dict[str, Any] = {}
    label_distributions: Dict[str, Any] = {}
    tfdv_statistics: Dict[str, Any] = {}
    tfdv_anomalies: Dict[str, Any] = {}

async def get_db_connection():
    """Get database connection using environment configuration"""
    try:
        env = Environment()
        return await asyncpg.connect(env.get_database_url())
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

@router.get("/", response_model=TrainingDatasetListResponse)
async def list_training_datasets():
    """List all training datasets"""
    try:
        conn = await get_db_connection()

        # Use environment-aware table name
        env = Environment()
        table_name = env.get_table_name("training_datasets")
        query = f"""
        SELECT id, dataset_name, total_sequences, feature_count, label_count,
               data_quality_score, feature_completeness, label_completeness,
               file_size_mb, technical_indicators, symbols,
               date_range_start, date_range_end, created_at
        FROM {table_name}
        ORDER BY created_at DESC
        """

        rows = await conn.fetch(query)

        datasets = []
        for row in rows:
            datasets.append(TrainingDatasetInfo(
                id=row['id'],
                dataset_name=row['dataset_name'],
                total_sequences=row['total_sequences'] or 0,
                feature_count=row['feature_count'] or 0,
                label_count=row['label_count'] or 0,
                data_quality_score=row['data_quality_score'] or 0.0,
                feature_completeness=row['feature_completeness'] or 0.0,
                label_completeness=row['label_completeness'] or 0.0,
                file_size_mb=row['file_size_mb'] or 0.0,
                technical_indicators=row['technical_indicators'] or "",
                symbols=row['symbols'] or [],
                date_range_start=row['date_range_start'],
                date_range_end=row['date_range_end'],
                created_at=row['created_at']
            ))

        await conn.close()

        return TrainingDatasetListResponse(
            datasets=datasets,
            total_count=len(datasets)
        )

    except Exception as e:
        logger.error(f"Error listing training datasets: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list training datasets: {str(e)}")

@router.get("/{dataset_id}/distributions", response_model=TrainingDatasetDistributions)
async def get_training_dataset_distributions(dataset_id: int):
    """Get TFDV distributions for a training dataset"""
    try:
        conn = await get_db_connection()

        # Use environment-aware table name
        env = Environment()
        table_name = env.get_table_name("training_datasets")
        query = f"""
        SELECT data_quality_score, feature_completeness, label_completeness,
               tfdv_statistics, feature_distributions, label_distributions, tfdv_anomalies
        FROM {table_name}
        WHERE id = $1
        """

        row = await conn.fetchrow(query, dataset_id)
        if not row:
            raise HTTPException(status_code=404, detail=f"Training dataset {dataset_id} not found")

        await conn.close()

        # Parse JSONB fields
        tfdv_stats = json.loads(row['tfdv_statistics']) if row['tfdv_statistics'] else {}
        feature_dists = json.loads(row['feature_distributions']) if row['feature_distributions'] else {}
        label_dists = json.loads(row['label_distributions']) if row['label_distributions'] else {}
        anomalies = json.loads(row['tfdv_anomalies']) if row['tfdv_anomalies'] else {}

        return TrainingDatasetDistributions(
            data_quality_score=row['data_quality_score'] or 0.0,
            feature_completeness=row['feature_completeness'] or 0.0,
            label_completeness=row['label_completeness'] or 0.0,
            feature_distributions=feature_dists,
            label_distributions=label_dists,
            tfdv_statistics=tfdv_stats,
            tfdv_anomalies=anomalies
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting training dataset distributions: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get distributions: {str(e)}")

@router.get("/{dataset_id}/histogram")
async def get_training_dataset_histogram(dataset_id: int, feature_name: Optional[str] = None):
    """Get histogram data for training dataset"""
    try:
        conn = await get_db_connection()

        # Get TFDV histogram path
        query = """
        SELECT tfdv_histogram_path, feature_distributions, label_distributions
        FROM dev_training_datasets
        WHERE id = $1
        """

        row = await conn.fetchrow(query, dataset_id)
        if not row:
            raise HTTPException(status_code=404, detail=f"Training dataset {dataset_id} not found")

        await conn.close()

        # For now, return mock histogram data based on distributions
        feature_dists = json.loads(row['feature_distributions']) if row['feature_distributions'] else {}
        label_dists = json.loads(row['label_distributions']) if row['label_distributions'] else {}

        histogram_data = {
            "tfdv_statistics": {
                "features": feature_dists,
                "labels": label_dists
            },
            "histogram_path": row['tfdv_histogram_path'] or ""
        }

        if feature_name and feature_name in feature_dists:
            # Return specific feature histogram
            histogram_data["selected_feature"] = {
                feature_name: feature_dists[feature_name]
            }

        return histogram_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting histogram data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get histogram: {str(e)}")