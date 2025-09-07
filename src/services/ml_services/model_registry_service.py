#!/usr/bin/env python3
"""
Model Registry Service - Centralized tracking of all training models
Provides model artifact management, input signature tracking, and comprehensive metadata.
"""

import os
import json
import logging
import hashlib
import shutil
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import psycopg2
import psycopg2.extras
import numpy as np
import torch
import pickle

logger = logging.getLogger(__name__)

@dataclass
class ModelInputSignature:
    """Model input signature specification."""

    # Input Shape Information
    input_shape: List[int]  # [batch_size, sequence_length, features]
    feature_count: int
    sequence_length: int

    # Feature Metadata
    feature_names: List[str]
    feature_types: List[str]  # FeatureType enum values
    feature_descriptions: List[str]

    # Data Requirements
    expected_data_types: List[str]  # numpy/torch data types
    normalization_requirements: Dict[str, Any]
    preprocessing_steps: List[str]

    # Validation Constraints
    min_values: List[float]
    max_values: List[float]
    required_technical_indicators: List[str]
    supported_timeframes: List[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelInputSignature':
        """Create from dictionary."""
        return cls(**data)

    def validate_input(self, X: np.ndarray) -> Tuple[bool, List[str]]:
        """Validate input data against signature."""
        errors = []

        # Shape validation
        if len(X.shape) != len(self.input_shape):
            errors.append(f"Expected {len(self.input_shape)} dimensions, got {len(X.shape)}")
        elif X.shape[1:] != tuple(self.input_shape[1:]):  # Skip batch dimension
            errors.append(f"Expected shape {self.input_shape[1:]}, got {X.shape[1:]}")

        # Feature count validation
        if len(X.shape) >= 2 and X.shape[-1] != self.feature_count:
            errors.append(f"Expected {self.feature_count} features, got {X.shape[-1]}")

        # Data type validation
        expected_dtype = self.expected_data_types[0] if self.expected_data_types else 'float32'
        if str(X.dtype) != expected_dtype:
            errors.append(f"Expected dtype {expected_dtype}, got {X.dtype}")

        # Value range validation (sample check)
        if len(self.min_values) == X.shape[-1] and len(self.max_values) == X.shape[-1]:
            for i, (min_val, max_val) in enumerate(zip(self.min_values, self.max_values)):
                feature_min, feature_max = X[:, :, i].min(), X[:, :, i].max()
                if feature_min < min_val * 0.9 or feature_max > max_val * 1.1:  # 10% tolerance
                    errors.append(f"Feature {i} ({self.feature_names[i] if i < len(self.feature_names) else 'unknown'}) "
                                f"values [{feature_min:.3f}, {feature_max:.3f}] outside expected range [{min_val:.3f}, {max_val:.3f}]")

        return len(errors) == 0, errors

@dataclass
class ModelMetadata:
    """Comprehensive model metadata."""

    # Core Identification
    model_id: int
    model_name: str
    model_version: str
    model_type: str  # 'transformer', 'lstm', 'cnn', etc.

    # Training Information
    training_run_id: int
    dataset_id: Optional[int]
    training_duration_seconds: float
    training_start_time: datetime
    training_end_time: datetime

    # Model Architecture
    architecture_config: Dict[str, Any]
    parameter_count: int
    model_size_mb: float

    # Performance Metrics
    final_loss: float
    validation_metrics: Dict[str, float]
    training_metrics: Dict[str, float]

    # Input/Output Specifications
    input_signature: ModelInputSignature
    output_shape: List[int]
    output_type: str  # 'regression', 'classification', 'sequence'

    # File Information
    model_artifact_path: str
    checkpoint_path: Optional[str]
    onnx_path: Optional[str]

    # Metadata
    tags: List[str]
    description: str
    created_by: str
    framework: str  # 'pytorch', 'tensorflow', 'sklearn'
    framework_version: str
    python_version: str

    # Deployment Information
    deployment_status: str  # 'registered', 'staging', 'production', 'retired'
    deployment_config: Optional[Dict[str, Any]]

    # Audit Information
    creation_timestamp: datetime
    last_updated: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        # Convert datetime objects to ISO strings
        for field in ['training_start_time', 'training_end_time', 'creation_timestamp', 'last_updated']:
            if isinstance(result[field], datetime):
                result[field] = result[field].isoformat()
        return result

class ModelRegistryService:
    """Centralized model registry for tracking all training models."""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None,
                 model_storage_path: str = "/tmp/ats-models"):
        """Initialize model registry service."""

        self.db_config = db_config or self._get_default_db_config()
        self.model_storage_path = Path(model_storage_path)
        self.model_storage_path.mkdir(parents=True, exist_ok=True)

        # Initialize database connection
        self.connection = None
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self._ensure_model_registry_tables()
            logger.info("✅ Model Registry Service initialized with database")
        except Exception as e:
            logger.warning(f"⚠️ Model Registry database unavailable: {e}")
            logger.info("💾 Continuing with local file storage only")

    def _get_default_db_config(self) -> Dict[str, Any]:
        """Get default database configuration."""
        return {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'database': os.getenv('POSTGRES_DB', 'dev_db'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'dev_password')
        }

    def _ensure_model_registry_tables(self):
        """Create model registry tables if they don't exist."""

        create_models_table = """
        CREATE TABLE IF NOT EXISTS dev_model_registry (
            id SERIAL PRIMARY KEY,
            model_name VARCHAR(255) NOT NULL,
            model_version VARCHAR(100) NOT NULL,
            model_type VARCHAR(100) NOT NULL,

            -- Training Information
            training_run_id INTEGER,
            dataset_id INTEGER,
            training_duration_seconds FLOAT,
            training_start_time TIMESTAMP,
            training_end_time TIMESTAMP,

            -- Model Architecture
            architecture_config JSONB,
            parameter_count BIGINT,
            model_size_mb FLOAT,

            -- Performance Metrics
            final_loss FLOAT,
            validation_metrics JSONB,
            training_metrics JSONB,

            -- Input/Output Specifications
            input_signature JSONB NOT NULL,
            output_shape INTEGER[],
            output_type VARCHAR(100),

            -- File Information
            model_artifact_path TEXT NOT NULL,
            checkpoint_path TEXT,
            onnx_path TEXT,

            -- Metadata
            tags TEXT[],
            description TEXT,
            created_by VARCHAR(255),
            framework VARCHAR(100),
            framework_version VARCHAR(100),
            python_version VARCHAR(100),

            -- Deployment Information
            deployment_status VARCHAR(100) DEFAULT 'registered',
            deployment_config JSONB,

            -- Audit Information
            creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- Constraints
            UNIQUE(model_name, model_version)
        );

        -- Indexes for efficient queries
        CREATE INDEX IF NOT EXISTS idx_model_registry_name ON dev_model_registry(model_name);
        CREATE INDEX IF NOT EXISTS idx_model_registry_type ON dev_model_registry(model_type);
        CREATE INDEX IF NOT EXISTS idx_model_registry_training_run ON dev_model_registry(training_run_id);
        CREATE INDEX IF NOT EXISTS idx_model_registry_dataset ON dev_model_registry(dataset_id);
        CREATE INDEX IF NOT EXISTS idx_model_registry_tags ON dev_model_registry USING GIN(tags);
        CREATE INDEX IF NOT EXISTS idx_model_registry_deployment_status ON dev_model_registry(deployment_status);
        CREATE INDEX IF NOT EXISTS idx_model_registry_creation_time ON dev_model_registry(creation_timestamp);
        """

        create_model_artifacts_table = """
        CREATE TABLE IF NOT EXISTS dev_model_artifacts (
            id SERIAL PRIMARY KEY,
            model_id INTEGER REFERENCES dev_model_registry(id) ON DELETE CASCADE,
            artifact_type VARCHAR(100) NOT NULL, -- 'model', 'checkpoint', 'onnx', 'metadata'
            artifact_path TEXT NOT NULL,
            artifact_size_mb FLOAT,
            checksum_md5 VARCHAR(32),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_model_artifacts_model_id ON dev_model_artifacts(model_id);
        CREATE INDEX IF NOT EXISTS idx_model_artifacts_type ON dev_model_artifacts(artifact_type);
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_models_table)
                cursor.execute(create_model_artifacts_table)
                self.connection.commit()
                logger.info("✅ Model registry tables ensured")
        except Exception as e:
            logger.error(f"❌ Failed to create model registry tables: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    def register_model(self, model_metadata: ModelMetadata,
                      model_artifact: Any,
                      additional_artifacts: Optional[Dict[str, str]] = None) -> int:
        """Register a new model in the registry."""

        try:
            # Save model artifact to storage
            artifact_path = self._save_model_artifact(
                model_artifact,
                model_metadata.model_name,
                model_metadata.model_version
            )

            # Update metadata with actual artifact path
            model_metadata.model_artifact_path = str(artifact_path)

            # Calculate model size
            model_metadata.model_size_mb = self._calculate_file_size_mb(artifact_path)

            # Register in database
            model_id = self._insert_model_metadata(model_metadata)

            # Register additional artifacts
            if additional_artifacts:
                for artifact_type, artifact_path in additional_artifacts.items():
                    self._register_artifact(model_id, artifact_type, artifact_path)

            logger.info(f"✅ Model registered: {model_metadata.model_name} v{model_metadata.model_version} (ID: {model_id})")
            return model_id

        except Exception as e:
            logger.error(f"❌ Failed to register model {model_metadata.model_name}: {e}")
            raise

    def _save_model_artifact(self, model_artifact: Any, model_name: str, version: str) -> Path:
        """Save model artifact to storage."""

        # Create model directory
        model_dir = self.model_storage_path / model_name / version
        model_dir.mkdir(parents=True, exist_ok=True)

        # Determine artifact path based on type
        if hasattr(model_artifact, 'state_dict'):  # PyTorch model
            artifact_path = model_dir / f"{model_name}_v{version}.pth"
            torch.save(model_artifact.state_dict(), artifact_path)
        elif isinstance(model_artifact, dict) and 'model_state_dict' in model_artifact:  # PyTorch state dict
            artifact_path = model_dir / f"{model_name}_v{version}.pth"
            torch.save(model_artifact, artifact_path)
        elif isinstance(model_artifact, str) and os.path.exists(model_artifact):  # File path
            artifact_path = model_dir / f"{model_name}_v{version}{Path(model_artifact).suffix}"
            shutil.copy2(model_artifact, artifact_path)
        else:  # Generic pickle
            artifact_path = model_dir / f"{model_name}_v{version}.pkl"
            with open(artifact_path, 'wb') as f:
                pickle.dump(model_artifact, f)

        logger.info(f"💾 Model artifact saved: {artifact_path}")
        return artifact_path

    def _calculate_file_size_mb(self, file_path: Path) -> float:
        """Calculate file size in MB."""
        try:
            size_bytes = file_path.stat().st_size
            return size_bytes / (1024 * 1024)
        except Exception:
            return 0.0

    def _insert_model_metadata(self, metadata: ModelMetadata) -> int:
        """Insert model metadata into database."""

        if not self.connection:
            logger.warning("⚠️ No database connection - model metadata not persisted")
            return 0

        insert_query = """
        INSERT INTO dev_model_registry (
            model_name, model_version, model_type,
            training_run_id, dataset_id, training_duration_seconds,
            training_start_time, training_end_time,
            architecture_config, parameter_count, model_size_mb,
            final_loss, validation_metrics, training_metrics,
            input_signature, output_shape, output_type,
            model_artifact_path, checkpoint_path, onnx_path,
            tags, description, created_by, framework, framework_version, python_version,
            deployment_status, deployment_config
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id;
        """

        values = (
            metadata.model_name, metadata.model_version, metadata.model_type,
            metadata.training_run_id, metadata.dataset_id, metadata.training_duration_seconds,
            metadata.training_start_time, metadata.training_end_time,
            json.dumps(metadata.architecture_config) if metadata.architecture_config else None,
            metadata.parameter_count, metadata.model_size_mb,
            metadata.final_loss,
            json.dumps(metadata.validation_metrics) if metadata.validation_metrics else None,
            json.dumps(metadata.training_metrics) if metadata.training_metrics else None,
            json.dumps(metadata.input_signature.to_dict()),
            metadata.output_shape, metadata.output_type,
            metadata.model_artifact_path, metadata.checkpoint_path, metadata.onnx_path,
            metadata.tags, metadata.description, metadata.created_by,
            metadata.framework, metadata.framework_version, metadata.python_version,
            metadata.deployment_status,
            json.dumps(metadata.deployment_config) if metadata.deployment_config else None
        )

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, values)
                model_id = cursor.fetchone()[0]
                self.connection.commit()
                return model_id
        except Exception as e:
            logger.error(f"❌ Failed to insert model metadata: {e}")
            self.connection.rollback()
            raise

    def _register_artifact(self, model_id: int, artifact_type: str, artifact_path: str):
        """Register additional artifact for model."""

        if not self.connection:
            return

        # Calculate size and checksum
        try:
            size_mb = Path(artifact_path).stat().st_size / (1024 * 1024)
            checksum = self._calculate_md5_checksum(artifact_path)
        except Exception:
            size_mb = 0.0
            checksum = None

        insert_query = """
        INSERT INTO dev_model_artifacts (
            model_id, artifact_type, artifact_path, artifact_size_mb, checksum_md5
        ) VALUES (%s, %s, %s, %s, %s);
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_query, (model_id, artifact_type, artifact_path, size_mb, checksum))
                self.connection.commit()
        except Exception as e:
            logger.error(f"❌ Failed to register artifact {artifact_type}: {e}")
            self.connection.rollback()

    def _calculate_md5_checksum(self, file_path: str) -> str:
        """Calculate MD5 checksum of file."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception:
            return ""

    def get_model(self, model_id: int) -> Optional[ModelMetadata]:
        """Retrieve model metadata by ID."""

        if not self.connection:
            logger.warning("⚠️ No database connection - cannot retrieve model")
            return None

        query = """
        SELECT * FROM dev_model_registry WHERE id = %s;
        """

        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, (model_id,))
                row = cursor.fetchone()

                if not row:
                    return None

                return self._row_to_model_metadata(row)

        except Exception as e:
            logger.error(f"❌ Failed to retrieve model {model_id}: {e}")
            return None

    def list_models(self, model_type: Optional[str] = None,
                   tags: Optional[List[str]] = None,
                   deployment_status: Optional[str] = None,
                   limit: int = 50) -> List[ModelMetadata]:
        """List models with optional filtering."""

        if not self.connection:
            logger.warning("⚠️ No database connection - cannot list models")
            return []

        query = "SELECT * FROM dev_model_registry WHERE 1=1"
        params = []

        if model_type:
            query += " AND model_type = %s"
            params.append(model_type)

        if tags:
            query += " AND tags && %s"  # Array overlap
            params.append(tags)

        if deployment_status:
            query += " AND deployment_status = %s"
            params.append(deployment_status)

        query += " ORDER BY creation_timestamp DESC LIMIT %s"
        params.append(limit)

        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()

                return [self._row_to_model_metadata(row) for row in rows]

        except Exception as e:
            logger.error(f"❌ Failed to list models: {e}")
            return []

    def _row_to_model_metadata(self, row: Dict[str, Any]) -> ModelMetadata:
        """Convert database row to ModelMetadata object."""

        # Parse JSON fields
        architecture_config = json.loads(row['architecture_config']) if row['architecture_config'] else {}
        validation_metrics = json.loads(row['validation_metrics']) if row['validation_metrics'] else {}
        training_metrics = json.loads(row['training_metrics']) if row['training_metrics'] else {}
        input_signature_data = json.loads(row['input_signature'])
        deployment_config = json.loads(row['deployment_config']) if row['deployment_config'] else None

        return ModelMetadata(
            model_id=row['id'],
            model_name=row['model_name'],
            model_version=row['model_version'],
            model_type=row['model_type'],
            training_run_id=row['training_run_id'],
            dataset_id=row['dataset_id'],
            training_duration_seconds=row['training_duration_seconds'] or 0.0,
            training_start_time=row['training_start_time'],
            training_end_time=row['training_end_time'],
            architecture_config=architecture_config,
            parameter_count=row['parameter_count'] or 0,
            model_size_mb=row['model_size_mb'] or 0.0,
            final_loss=row['final_loss'] or 0.0,
            validation_metrics=validation_metrics,
            training_metrics=training_metrics,
            input_signature=ModelInputSignature.from_dict(input_signature_data),
            output_shape=row['output_shape'] or [],
            output_type=row['output_type'] or 'regression',
            model_artifact_path=row['model_artifact_path'],
            checkpoint_path=row['checkpoint_path'],
            onnx_path=row['onnx_path'],
            tags=row['tags'] or [],
            description=row['description'] or '',
            created_by=row['created_by'] or 'unknown',
            framework=row['framework'] or 'pytorch',
            framework_version=row['framework_version'] or '',
            python_version=row['python_version'] or '',
            deployment_status=row['deployment_status'],
            deployment_config=deployment_config,
            creation_timestamp=row['creation_timestamp'],
            last_updated=row['last_updated']
        )

    def update_deployment_status(self, model_id: int, status: str,
                                config: Optional[Dict[str, Any]] = None) -> bool:
        """Update model deployment status."""

        if not self.connection:
            return False

        query = """
        UPDATE dev_model_registry
        SET deployment_status = %s, deployment_config = %s, last_updated = CURRENT_TIMESTAMP
        WHERE id = %s;
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (status, json.dumps(config) if config else None, model_id))
                self.connection.commit()

                logger.info(f"✅ Updated model {model_id} deployment status to {status}")
                return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"❌ Failed to update deployment status for model {model_id}: {e}")
            self.connection.rollback()
            return False

    def search_models_by_input_signature(self, required_features: List[str],
                                       sequence_length: Optional[int] = None) -> List[ModelMetadata]:
        """Search for models compatible with specific input requirements."""

        if not self.connection:
            return []

        query = """
        SELECT * FROM dev_model_registry
        WHERE input_signature->'feature_names' ?& %s
        """
        params = [required_features]

        if sequence_length:
            query += " AND (input_signature->'sequence_length')::int = %s"
            params.append(sequence_length)

        query += " ORDER BY final_loss ASC"

        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()

                return [self._row_to_model_metadata(row) for row in rows]

        except Exception as e:
            logger.error(f"❌ Failed to search models by input signature: {e}")
            return []

    def get_model_statistics(self) -> Dict[str, Any]:
        """Get comprehensive model registry statistics."""

        if not self.connection:
            return {'error': 'No database connection'}

        stats_query = """
        SELECT
            COUNT(*) as total_models,
            COUNT(DISTINCT model_type) as unique_model_types,
            COUNT(DISTINCT training_run_id) as unique_training_runs,
            AVG(model_size_mb) as avg_model_size_mb,
            AVG(parameter_count) as avg_parameter_count,
            AVG(final_loss) as avg_final_loss,
            MIN(creation_timestamp) as earliest_model,
            MAX(creation_timestamp) as latest_model
        FROM dev_model_registry;

        SELECT model_type, COUNT(*) as count
        FROM dev_model_registry
        GROUP BY model_type
        ORDER BY count DESC;

        SELECT deployment_status, COUNT(*) as count
        FROM dev_model_registry
        GROUP BY deployment_status;
        """

        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Get overall statistics
                cursor.execute(stats_query.split(';')[0])
                overall_stats = cursor.fetchone()

                # Get model type distribution
                cursor.execute(stats_query.split(';')[1])
                model_type_distribution = {row['model_type']: row['count'] for row in cursor.fetchall()}

                # Get deployment status distribution
                cursor.execute(stats_query.split(';')[2])
                deployment_status_distribution = {row['deployment_status']: row['count'] for row in cursor.fetchall()}

                return {
                    'overview': dict(overall_stats),
                    'model_type_distribution': model_type_distribution,
                    'deployment_status_distribution': deployment_status_distribution
                }

        except Exception as e:
            logger.error(f"❌ Failed to get model statistics: {e}")
            return {'error': str(e)}

def create_input_signature_from_dataset_config(config: Dict[str, Any],
                                             feature_metadata: Optional[Dict[str, Any]] = None) -> ModelInputSignature:
    """Create ModelInputSignature from dataset configuration."""

    # Extract basic information
    feature_count = config.get('feature_count', 10)
    sequence_length = config.get('sequence_length', 100)

    # Create default feature information
    feature_names = [f"feature_{i}" for i in range(feature_count)]
    feature_types = ['FLOAT'] * feature_count
    feature_descriptions = [f"Feature {i} - auto-generated" for i in range(feature_count)]

    # Use feature metadata if available
    if feature_metadata and 'features' in feature_metadata:
        features = feature_metadata['features'][:feature_count]  # Take only what we need
        feature_names = [f.get('name', f"feature_{i}") for i, f in enumerate(features)]
        feature_types = [f.get('feature_type', 'FLOAT') for f in features]
        feature_descriptions = [f.get('description', f"Feature {i}") for i, f in enumerate(features)]

    # Technical indicators from config
    technical_indicators = config.get('technical_indicators', [])
    timeframes = config.get('timeframes', ['1h'])

    return ModelInputSignature(
        input_shape=[-1, sequence_length, feature_count],  # -1 for batch dimension
        feature_count=feature_count,
        sequence_length=sequence_length,
        feature_names=feature_names,
        feature_types=feature_types,
        feature_descriptions=feature_descriptions,
        expected_data_types=['float32'],
        normalization_requirements={'method': 'z-score', 'per_feature': True},
        preprocessing_steps=['normalization', 'sequence_padding'],
        min_values=[-10.0] * feature_count,  # Default ranges
        max_values=[10.0] * feature_count,
        required_technical_indicators=technical_indicators,
        supported_timeframes=timeframes
    )