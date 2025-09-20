"""
Training Dataset DAO

Full implementation with database integration and TFDV stats support
"""

from typing import List, Optional, Dict, Any
from datetime import date, datetime
from dataclasses import dataclass
import asyncpg
import json


@dataclass
class TrainingDatasetRecord:
    """Training dataset record structure."""

    dataset_name: str
    run_id: Optional[int] = None
    total_sequences: int = 0
    sequence_length: int = 0
    feature_count: int = 0
    label_count: int = 0
    symbols: List[str] = None
    date_range_start: date = None
    date_range_end: date = None
    data_quality_score: float = 0.0
    feature_completeness: float = 0.0
    label_completeness: float = 0.0
    generation_duration_seconds: int = 0
    file_size_mb: float = 0.0
    data_sources: List[str] = None
    status: str = "created"
    features_file_path: str = ""
    labels_file_path: str = ""
    metadata_file_path: str = ""
    feature_metadata: str = ""
    technical_indicators: str = ""
    prediction_horizon: int = 0
    created_by: str = ""
    generation_parameters: Dict[str, Any] = None

    # TFDV (TensorFlow Data Validation) stats
    tfdv_statistics: Dict[str, Any] = None
    tfdv_histogram_path: str = ""
    tfdv_anomalies: Dict[str, Any] = None
    tfdv_schema_path: str = ""
    feature_distributions: Dict[str, Any] = None
    label_distributions: Dict[str, Any] = None

    # Metadata
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    id: Optional[int] = None

    def __post_init__(self):
        """Initialize default values."""
        if self.symbols is None:
            self.symbols = []
        if self.data_sources is None:
            self.data_sources = []
        if self.generation_parameters is None:
            self.generation_parameters = {}
        if self.tfdv_statistics is None:
            self.tfdv_statistics = {}
        if self.tfdv_anomalies is None:
            self.tfdv_anomalies = {}
        if self.feature_distributions is None:
            self.feature_distributions = {}
        if self.label_distributions is None:
            self.label_distributions = {}


class TrainingDatasetDAO:
    """Training Dataset Data Access Object with full database integration."""

    def __init__(self, env=None):
        """Initialize DAO."""
        self.env = env

    async def create_training_dataset(self, record: TrainingDatasetRecord) -> int:
        """Create training dataset record in database."""
        if not self.env:
            raise ValueError("Environment is required for database operations")

        conn = await asyncpg.connect(self.env.get_database_url())
        try:
            table_name = self.env.get_table_name("training_dataset")

            query = f"""
            INSERT INTO {table_name} (
                dataset_name, run_id, total_sequences, sequence_length, feature_count, label_count,
                symbols, date_range_start, date_range_end, data_quality_score, feature_completeness,
                label_completeness, generation_duration_seconds, file_size_mb, data_sources, status,
                features_file_path, labels_file_path, metadata_file_path, feature_metadata,
                technical_indicators, prediction_horizon, created_by, generation_parameters,
                tfdv_statistics, tfdv_histogram_path, tfdv_anomalies, tfdv_schema_path,
                feature_distributions, label_distributions
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30
            ) RETURNING id
            """

            dataset_id = await conn.fetchval(
                query,
                record.dataset_name, record.run_id, record.total_sequences, record.sequence_length,
                record.feature_count, record.label_count, record.symbols, record.date_range_start,
                record.date_range_end, record.data_quality_score, record.feature_completeness,
                record.label_completeness, record.generation_duration_seconds, record.file_size_mb,
                record.data_sources, record.status, record.features_file_path, record.labels_file_path,
                record.metadata_file_path, record.feature_metadata, record.technical_indicators,
                record.prediction_horizon, record.created_by, json.dumps(record.generation_parameters),
                json.dumps(record.tfdv_statistics), record.tfdv_histogram_path,
                json.dumps(record.tfdv_anomalies), record.tfdv_schema_path,
                json.dumps(record.feature_distributions), json.dumps(record.label_distributions)
            )

            record.id = dataset_id
            return dataset_id

        finally:
            await conn.close()

    async def get_training_dataset(self, dataset_id: int) -> Optional[TrainingDatasetRecord]:
        """Get training dataset by ID."""
        if not self.env:
            raise ValueError("Environment is required for database operations")

        conn = await asyncpg.connect(self.env.get_database_url())
        try:
            table_name = self.env.get_table_name("training_dataset")

            query = f"SELECT * FROM {table_name} WHERE id = $1"
            row = await conn.fetchrow(query, dataset_id)

            if not row:
                return None

            return self._row_to_record(row)

        finally:
            await conn.close()

    async def list_training_datasets(self, limit: int = 100, offset: int = 0) -> List[TrainingDatasetRecord]:
        """List training datasets with pagination."""
        if not self.env:
            raise ValueError("Environment is required for database operations")

        conn = await asyncpg.connect(self.env.get_database_url())
        try:
            table_name = self.env.get_table_name("training_dataset")

            query = f"""
            SELECT * FROM {table_name}
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            """
            rows = await conn.fetch(query, limit, offset)

            return [self._row_to_record(row) for row in rows]

        finally:
            await conn.close()

    async def update_tfdv_stats(self, dataset_id: int, tfdv_stats: Dict[str, Any],
                               histogram_path: str = "", anomalies: Dict[str, Any] = None,
                               feature_distributions: Dict[str, Any] = None,
                               label_distributions: Dict[str, Any] = None) -> bool:
        """Update TFDV statistics for a dataset."""
        if not self.env:
            raise ValueError("Environment is required for database operations")

        conn = await asyncpg.connect(self.env.get_database_url())
        try:
            table_name = self.env.get_table_name("training_dataset")

            query = f"""
            UPDATE {table_name}
            SET tfdv_statistics = $1, tfdv_histogram_path = $2, tfdv_anomalies = $3,
                feature_distributions = $4, label_distributions = $5, updated_at = NOW()
            WHERE id = $6
            """

            result = await conn.execute(
                query,
                json.dumps(tfdv_stats or {}),
                histogram_path,
                json.dumps(anomalies or {}),
                json.dumps(feature_distributions or {}),
                json.dumps(label_distributions or {}),
                dataset_id
            )

            return "UPDATE 1" in result

        finally:
            await conn.close()

    def _row_to_record(self, row) -> TrainingDatasetRecord:
        """Convert database row to TrainingDatasetRecord."""
        return TrainingDatasetRecord(
            id=row['id'],
            dataset_name=row['dataset_name'],
            run_id=row['run_id'],
            total_sequences=row['total_sequences'],
            sequence_length=row['sequence_length'],
            feature_count=row['feature_count'],
            label_count=row['label_count'],
            symbols=list(row['symbols']) if row['symbols'] else [],
            date_range_start=row['date_range_start'],
            date_range_end=row['date_range_end'],
            data_quality_score=row['data_quality_score'],
            feature_completeness=row['feature_completeness'],
            label_completeness=row['label_completeness'],
            generation_duration_seconds=row['generation_duration_seconds'],
            file_size_mb=row['file_size_mb'],
            data_sources=list(row['data_sources']) if row['data_sources'] else [],
            status=row['status'],
            features_file_path=row['features_file_path'],
            labels_file_path=row['labels_file_path'],
            metadata_file_path=row['metadata_file_path'],
            feature_metadata=row['feature_metadata'],
            technical_indicators=row['technical_indicators'],
            prediction_horizon=row['prediction_horizon'],
            created_by=row['created_by'],
            generation_parameters=json.loads(row['generation_parameters']) if row['generation_parameters'] else {},
            tfdv_statistics=json.loads(row['tfdv_statistics']) if row['tfdv_statistics'] else {},
            tfdv_histogram_path=row['tfdv_histogram_path'],
            tfdv_anomalies=json.loads(row['tfdv_anomalies']) if row['tfdv_anomalies'] else {},
            tfdv_schema_path=row['tfdv_schema_path'],
            feature_distributions=json.loads(row['feature_distributions']) if row['feature_distributions'] else {},
            label_distributions=json.loads(row['label_distributions']) if row['label_distributions'] else {},
            created_at=row['created_at'],
            updated_at=row['updated_at']
        )