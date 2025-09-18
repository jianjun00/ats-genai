#!/usr/bin/env python3
"""
Model Registry API Endpoints - REST API for model registry operations
Provides endpoints for model discovery, metadata retrieval, and deployment management.
"""

import logging
from typing import Dict, Optional, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Path
from pydantic import BaseModel

from services.model_registry_service import ModelRegistryService

logger = logging.getLogger(__name__)

# Create APIRouter for model registry endpoints
model_registry_bp = APIRouter(prefix='/api/models')

# Initialize service
registry_service = ModelRegistryService()

# Pydantic models for request/response
class DeploymentUpdateRequest(BaseModel):
    status: str
    config: Optional[Dict[str, Any]] = None

@model_registry_bp.get('/health')
async def health_check():
    """Health check endpoint for model registry service."""

    try:
        # Check if service is operational
        stats = registry_service.get_model_statistics()

        if 'error' in stats:
            raise HTTPException(
                status_code=503,
                detail={
                    'status': 'degraded',
                    'message': 'Model registry database unavailable',
                    'details': stats['error']
                }
            )

        return {
            'status': 'healthy',
            'message': 'Model registry service operational',
            'timestamp': datetime.now().isoformat(),
            'total_models': stats['overview'].get('total_models', 0)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                'status': 'unhealthy',
                'message': 'Model registry service error',
                'error': str(e)
            }
        )

@model_registry_bp.get('/list')
async def list_models(
    model_type: Optional[str] = Query(None),
    deployment_status: str = Query('registered'),
    tags: Optional[str] = Query(None),
    limit: int = Query(50)
):
    """List models with optional filtering."""

    try:
        # Parse tags
        tag_list = None
        if tags:
            tag_list = [tag.strip() for tag in tags.split(',')]

        # Get models
        models = registry_service.list_models(
            model_type=model_type,
            tags=tag_list,
            deployment_status=deployment_status,
            limit=limit
        )

        # Convert to API response format
        model_summaries = []
        for model in models:
            summary = {
                'model_id': model.model_id,
                'model_name': model.model_name,
                'model_version': model.model_version,
                'model_type': model.model_type,
                'training_run_id': model.training_run_id,
                'parameter_count': model.parameter_count,
                'model_size_mb': model.model_size_mb,
                'final_loss': model.final_loss,
                'deployment_status': model.deployment_status,
                'tags': model.tags,
                'framework': model.framework,
                'created_by': model.created_by,
                'creation_timestamp': model.creation_timestamp.isoformat() if model.creation_timestamp else None,
                'description': model.description
            }
            model_summaries.append(summary)

        return {
            'models': model_summaries,
            'total_count': len(model_summaries),
            'filters': {
                'model_type': model_type,
                'deployment_status': deployment_status,
                'tags': tag_list
            }
        }

    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={
                'error': 'Invalid parameter format',
                'message': str(e)
            }
        )
    except Exception as e:
        logger.error(f"❌ Error listing models: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                'error': 'Internal server error',
                'message': str(e)
            }
        )

@model_registry_bp.get('/statistics')
async def get_statistics():
    """Get comprehensive model registry statistics."""

    try:
        stats = registry_service.get_model_statistics()

        if 'error' in stats:
            raise HTTPException(
                status_code=503,
                detail={
                    'error': 'Statistics unavailable',
                    'message': stats['error']
                }
            )

        # Format statistics for API response
        formatted_stats = {
            'overview': {
                'total_models': stats['overview'].get('total_models', 0),
                'unique_model_types': stats['overview'].get('unique_model_types', 0),
                'unique_training_runs': stats['overview'].get('unique_training_runs', 0),
                'avg_model_size_mb': round(stats['overview'].get('avg_model_size_mb', 0.0), 2),
                'avg_parameter_count': int(stats['overview'].get('avg_parameter_count', 0)),
                'avg_final_loss': round(stats['overview'].get('avg_final_loss', 0.0), 6),
                'earliest_model': stats['overview'].get('earliest_model'),
                'latest_model': stats['overview'].get('latest_model')
            },
            'distributions': {
                'model_types': stats.get('model_type_distribution', {}),
                'deployment_status': stats.get('deployment_status_distribution', {})
            },
            'generated_at': datetime.now().isoformat()
        }

        return formatted_stats

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error retrieving statistics: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                'error': 'Internal server error',
                'message': str(e)
            }
        )

@model_registry_bp.get('/search')
async def search_models(
    features: str = Query(..., description="Comma-separated list of required features"),
    sequence_length: Optional[int] = Query(None)
):
    """Search models by input signature compatibility."""

    try:
        # Parse feature list
        feature_list = [f.strip() for f in features.split(',')]

        # Search compatible models
        models = registry_service.search_models_by_input_signature(
            required_features=feature_list,
            sequence_length=sequence_length
        )

        # Format response
        compatible_models = []
        for model in models:
            compatibility_info = {
                'model_id': model.model_id,
                'model_name': model.model_name,
                'model_version': model.model_version,
                'model_type': model.model_type,
                'final_loss': model.final_loss,
                'deployment_status': model.deployment_status,

                # Input compatibility info
                'input_compatibility': {
                    'sequence_length': model.input_signature.sequence_length,
                    'feature_count': model.input_signature.feature_count,
                    'matched_features': [f for f in feature_list if f in model.input_signature.feature_names],
                    'required_technical_indicators': model.input_signature.required_technical_indicators
                },

                'creation_timestamp': model.creation_timestamp.isoformat() if model.creation_timestamp else None
            }
            compatible_models.append(compatibility_info)

        return {
            'compatible_models': compatible_models,
            'search_criteria': {
                'required_features': feature_list,
                'sequence_length': sequence_length
            },
            'total_found': len(compatible_models)
        }

    except Exception as e:
        logger.error(f"❌ Error searching models: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                'error': 'Internal server error',
                'message': str(e)
            }
        )

@model_registry_bp.get('/types')
async def get_model_types():
    """Get list of available model types."""

    try:
        models = registry_service.list_models(limit=1000)  # Get all models

        model_types = set()
        type_counts = {}

        for model in models:
            model_types.add(model.model_type)
            type_counts[model.model_type] = type_counts.get(model.model_type, 0) + 1

        return {
            'model_types': sorted(list(model_types)),
            'type_distribution': type_counts,
            'total_types': len(model_types)
        }

    except Exception as e:
        logger.error(f"❌ Error retrieving model types: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                'error': 'Internal server error',
                'message': str(e)
            }
        )

@model_registry_bp.get('/{model_id}')
async def get_model(model_id: int = Path(...)):
    """Get detailed model information by ID."""

    try:
        model = registry_service.get_model(model_id)

        if not model:
            raise HTTPException(
                status_code=404,
                detail={
                    'error': 'Model not found',
                    'message': f'No model with ID {model_id}'
                }
            )

        # Convert to detailed response format
        model_detail = {
            'model_id': model.model_id,
            'model_name': model.model_name,
            'model_version': model.model_version,
            'model_type': model.model_type,

            # Training Information
            'training_info': {
                'training_run_id': model.training_run_id,
                'dataset_id': model.dataset_id,
                'training_duration_seconds': model.training_duration_seconds,
                'training_start_time': model.training_start_time.isoformat() if model.training_start_time else None,
                'training_end_time': model.training_end_time.isoformat() if model.training_end_time else None
            },

            # Architecture Information
            'architecture': {
                'parameter_count': model.parameter_count,
                'model_size_mb': model.model_size_mb,
                'architecture_config': model.architecture_config,
                'framework': model.framework,
                'framework_version': model.framework_version
            },

            # Performance Metrics
            'performance': {
                'final_loss': model.final_loss,
                'validation_metrics': model.validation_metrics,
                'training_metrics': model.training_metrics
            },

            # Input/Output Specifications
            'io_specification': {
                'input_signature': model.input_signature.to_dict(),
                'output_shape': model.output_shape,
                'output_type': model.output_type
            },

            # File Locations
            'artifacts': {
                'model_artifact_path': model.model_artifact_path,
                'checkpoint_path': model.checkpoint_path,
                'onnx_path': model.onnx_path
            },

            # Metadata
            'metadata': {
                'tags': model.tags,
                'description': model.description,
                'created_by': model.created_by,
                'python_version': model.python_version,
                'deployment_status': model.deployment_status,
                'deployment_config': model.deployment_config,
                'creation_timestamp': model.creation_timestamp.isoformat() if model.creation_timestamp else None,
                'last_updated': model.last_updated.isoformat() if model.last_updated else None
            }
        }

        return model_detail

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error retrieving model {model_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                'error': 'Internal server error',
                'message': str(e)
            }
        )

# Removed duplicate search function - moved to earlier position before /{model_id} route

@model_registry_bp.put('/{model_id}/deployment')
async def update_deployment_status(
    model_id: int = Path(...),
    deployment_data: DeploymentUpdateRequest = ...
):
    """Update model deployment status."""

    try:
        new_status = deployment_data.status
        deployment_config = deployment_data.config

        # Validate status
        valid_statuses = ['registered', 'staging', 'production', 'retired']
        if new_status not in valid_statuses:
            raise HTTPException(
                status_code=400,
                detail={
                    'error': 'Invalid status',
                    'message': f'Status must be one of: {valid_statuses}'
                }
            )

        # Update deployment status
        success = registry_service.update_deployment_status(
            model_id, new_status, deployment_config
        )

        if not success:
            raise HTTPException(
                status_code=404,
                detail={
                    'error': 'Update failed',
                    'message': f'Could not update deployment status for model {model_id}'
                }
            )

        return {
            'message': 'Deployment status updated successfully',
            'model_id': model_id,
            'new_status': new_status,
            'timestamp': datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error updating deployment status for model {model_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                'error': 'Internal server error',
                'message': str(e)
            }
        )

# Removed duplicate statistics function - moved to earlier position before /{model_id} route

@model_registry_bp.get('/{model_id}/validate')
async def validate_model_input(model_id: int = Path(...)):
    """Get model input validation schema."""

    try:
        model = registry_service.get_model(model_id)

        if not model:
            raise HTTPException(
                status_code=404,
                detail={
                    'error': 'Model not found',
                    'message': f'No model with ID {model_id}'
                }
            )

        # Return validation schema
        validation_schema = {
            'model_id': model_id,
            'input_requirements': {
                'expected_shape': model.input_signature.input_shape,
                'feature_count': model.input_signature.feature_count,
                'sequence_length': model.input_signature.sequence_length,
                'data_types': model.input_signature.expected_data_types,
                'feature_names': model.input_signature.feature_names,
                'value_ranges': {
                    'min_values': model.input_signature.min_values,
                    'max_values': model.input_signature.max_values
                }
            },
            'preprocessing_requirements': {
                'normalization': model.input_signature.normalization_requirements,
                'preprocessing_steps': model.input_signature.preprocessing_steps
            },
            'technical_requirements': {
                'required_indicators': model.input_signature.required_technical_indicators,
                'supported_timeframes': model.input_signature.supported_timeframes
            }
        }

        return validation_schema

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error validating input for model {model_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                'error': 'Internal server error',
                'message': str(e)
            }
        )

@model_registry_bp.get('/types')
async def get_model_types():
    """Get list of available model types."""

    try:
        models = registry_service.list_models(limit=1000)  # Get all models

        model_types = set()
        type_counts = {}

        for model in models:
            model_types.add(model.model_type)
            type_counts[model.model_type] = type_counts.get(model.model_type, 0) + 1

        return {
            'model_types': sorted(list(model_types)),
            'type_distribution': type_counts,
            'total_types': len(model_types)
        }

    except Exception as e:
        logger.error(f"❌ Error retrieving model types: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                'error': 'Internal server error',
                'message': str(e)
            }
        )