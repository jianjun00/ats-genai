"""
Training Schema Database Access Object (DAO)

Comprehensive database access layer for training dataset schema management in the ATS platform.
Provides CRUD operations, version control, and analytics for schema registry and dataset 
schema metadata with PostgreSQL JSONB optimization.

Key Features:
- Schema registry with version control and semantic versioning
- JSONB-optimized storage for efficient querying and indexing
- Schema compatibility tracking and migration support
- Usage analytics and popularity tracking
- Tag-based schema discovery and categorization
- Environment-aware table naming (dev/intg prefixes)

Classes:
    TrainingSchemaDAO: Main database access object for schema operations

Functions:
    create_schema_dao: Factory function for DAO creation
    register_standard_schemas: Registers common schema templates

Database Tables:
    - {env}_training_datasets: Enhanced with schema columns
    - {env}_training_schema_registry: Central schema registry
    - {env}_schema_usage_analytics: View for usage analytics

Example:
    # Create DAO instance
    dao = TrainingSchemaDAO(environment)
    
    # Register new schema
    schema_hash = await dao.register_schema(
        schema, created_by="ML Team", 
        tags=["AAPL", "daily", "technical"], 
        description="AAPL daily with technical indicators"
    )
    
    # Find compatible schemas
    compatible = await dao.find_compatible_schemas(
        feature_count=25, sequence_length=60, symbol="AAPL"
    )

Environment Support:
    - DEV: Uses dev_* table prefixes
    - INTG: Uses intg_* table prefixes
    - Automatic environment detection from configuration
"""

import asyncio
import asyncpg
import json
from typing import Dict, List, Optional, Any

from core.platform.config.environment import Environment
from schema.training_schema import TrainingDatasetSchema, ValidationResult


class TrainingSchemaDAO:
    """Database Access Object for training schema management."""
    
    def __init__(self, environment: Environment):
        self.environment = environment
        self.conn = None
        
        # Determine table prefixes based on environment
        if environment.env_type.value == 'intg':
            self.datasets_table = 'intg_training_datasets'
            self.registry_table = 'intg_training_schema_registry'
            self.analytics_view = 'intg_schema_usage_analytics'
        else:
            self.datasets_table = 'dev_training_datasets'
            self.registry_table = 'dev_training_schema_registry'
            self.analytics_view = 'dev_schema_usage_analytics'
    
    async def get_connection(self) -> asyncpg.Connection:
        """Get database connection."""
        if self.conn is None or self.conn.is_closed():
            self.conn = await self.environment.get_db_connection()
        return self.conn
    
    async def close_connection(self):
        """Close database connection."""
        if self.conn and not self.conn.is_closed():
            await self.conn.close()
    
    # Schema Registry Operations
    
    async def register_schema(
        self, 
        schema: TrainingDatasetSchema,
        created_by: str = "ATS Training System",
        tags: Optional[List[str]] = None,
        description: str = ""
    ) -> str:
        """Register a new schema in the registry."""
        
        conn = await self.get_connection()
        
        # Calculate schema hash
        schema_hash = schema.get_schema_hash()
        schema_json = schema.to_dict()
        
        # Default tags from schema metadata
        if tags is None:
            tags = [schema.metadata.symbol, schema.metadata.base_timeframe]
            if schema.metadata.additional_symbols:
                tags.extend(schema.metadata.additional_symbols)
        
        try:
            # Insert or update schema registry entry
            registry_id = await conn.fetchval("""
                INSERT INTO {registry_table} (
                    schema_name, schema_version, schema_hash, schema_json,
                    created_by, tags, description, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'active')
                ON CONFLICT (schema_name, schema_version) 
                DO UPDATE SET 
                    schema_hash = EXCLUDED.schema_hash,
                    schema_json = EXCLUDED.schema_json,
                    updated_at = NOW(),
                    tags = EXCLUDED.tags,
                    description = EXCLUDED.description
                RETURNING id
            """.format(registry_table=self.registry_table),
                schema.dataset_name,
                schema.schema_version,
                schema_hash,
                json.dumps(schema_json),
                created_by,
                tags,
                description
            )
            
            return schema_hash
            
        except Exception as e:
            raise Exception(f"Failed to register schema: {e}")
    
    async def get_schema_by_hash(self, schema_hash: str) -> Optional[TrainingDatasetSchema]:
        """Retrieve schema by hash."""
        
        conn = await self.get_connection()
        
        try:
            row = await conn.fetchrow("""
                SELECT schema_json FROM {registry_table}
                WHERE schema_hash = $1 AND status = 'active'
            """.format(registry_table=self.registry_table), schema_hash)
            
            if row:
                schema_dict = row['schema_json']
                return TrainingDatasetSchema.from_dict(schema_dict)
            
            return None
            
        except Exception as e:
            raise Exception(f"Failed to retrieve schema: {e}")
    
    async def get_schema_by_name_version(
        self, 
        schema_name: str, 
        version: str = "latest"
    ) -> Optional[TrainingDatasetSchema]:
        """Retrieve schema by name and version."""
        
        conn = await self.get_connection()
        
        try:
            if version == "latest":
                # Get latest version
                row = await conn.fetchrow("""
                    SELECT schema_json FROM {registry_table}
                    WHERE schema_name = $1 AND status = 'active'
                    ORDER BY created_at DESC
                    LIMIT 1
                """.format(registry_table=self.registry_table), schema_name)
            else:
                row = await conn.fetchrow("""
                    SELECT schema_json FROM {registry_table}
                    WHERE schema_name = $1 AND schema_version = $2 AND status = 'active'
                """.format(registry_table=self.registry_table), schema_name, version)
            
            if row:
                schema_dict = row['schema_json']
                return TrainingDatasetSchema.from_dict(schema_dict)
            
            return None
            
        except Exception as e:
            raise Exception(f"Failed to retrieve schema: {e}")
    
    async def list_schemas(
        self, 
        tags: Optional[List[str]] = None,
        status: str = "active",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """List schemas with optional filtering."""
        
        conn = await self.get_connection()
        
        try:
            where_conditions = ["status = $1"]
            params = [status]
            
            if tags:
                where_conditions.append("tags && $2")
                params.append(tags)
            
            where_clause = " AND ".join(where_conditions)
            
            rows = await conn.fetch(f"""
                SELECT schema_name, schema_version, schema_hash, description,
                       created_at, created_by, tags, usage_count, last_used_at
                FROM {self.registry_table}
                WHERE {where_clause}
                ORDER BY usage_count DESC, created_at DESC
                LIMIT ${len(params) + 1}
            """, *params, limit)
            
            return [dict(row) for row in rows]
            
        except Exception as e:
            raise Exception(f"Failed to list schemas: {e}")
    
    async def deprecate_schema(self, schema_hash: str) -> bool:
        """Mark a schema as deprecated."""
        
        conn = await self.get_connection()
        
        try:
            result = await conn.execute("""
                UPDATE {registry_table} 
                SET status = 'deprecated', updated_at = NOW()
                WHERE schema_hash = $1
            """.format(registry_table=self.registry_table), schema_hash)
            
            return result == "UPDATE 1"
            
        except Exception as e:
            raise Exception(f"Failed to deprecate schema: {e}")
    
    # Training Dataset Schema Operations
    
    async def save_dataset_schema(
        self, 
        dataset_id: int,
        schema: TrainingDatasetSchema,
        validation_result: Optional[ValidationResult] = None
    ) -> bool:
        """Save schema information for a specific dataset."""
        
        conn = await self.get_connection()
        
        try:
            schema_hash = schema.get_schema_hash()
            schema_json = schema.to_dict()
            
            # Separate feature and label schemas
            feature_schema = {
                'features': [f.__dict__ for f in schema.features],
                'metadata': schema.metadata.__dict__ if schema.metadata else {}
            }
            
            label_schema = {
                'labels': [l.__dict__ for l in schema.labels] if schema.labels else [],
                'prediction_metadata': {}
            }
            
            validation_json = {}
            if validation_result:
                validation_json = {
                    'is_valid': validation_result.is_valid,
                    'errors': [e.__dict__ for e in validation_result.errors],
                    'warnings': [w.__dict__ for w in validation_result.warnings],
                    'confidence_score': validation_result.confidence_score,
                    'validation_timestamp': validation_result.validation_timestamp
                }
            
            # Update dataset with schema information
            result = await conn.execute("""
                UPDATE {datasets_table} 
                SET schema_hash = $2,
                    schema_version = $3,
                    schema_json = $4,
                    feature_schema = $5,
                    label_schema = $6,
                    validation_results = $7,
                    updated_at = NOW()
                WHERE id = $1
            """.format(datasets_table=self.datasets_table),
                dataset_id,
                schema_hash,
                schema.schema_version,
                json.dumps(schema_json),
                json.dumps(feature_schema),
                json.dumps(label_schema),
                json.dumps(validation_json)
            )
            
            return result == "UPDATE 1"
            
        except Exception as e:
            raise Exception(f"Failed to save dataset schema: {e}")
    
    async def get_dataset_schema(self, dataset_id: int) -> Optional[TrainingDatasetSchema]:
        """Retrieve schema for a specific dataset."""
        
        conn = await self.get_connection()
        
        try:
            row = await conn.fetchrow("""
                SELECT schema_json, schema_hash FROM {datasets_table}
                WHERE id = $1
            """.format(datasets_table=self.datasets_table), dataset_id)
            
            if row and row['schema_json']:
                schema_dict = row['schema_json']
                return TrainingDatasetSchema.from_dict(schema_dict)
            
            return None
            
        except Exception as e:
            raise Exception(f"Failed to retrieve dataset schema: {e}")
    
    async def get_validation_results(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """Get validation results for a dataset."""
        
        conn = await self.get_connection()
        
        try:
            row = await conn.fetchrow("""
                SELECT validation_results FROM {datasets_table}
                WHERE id = $1
            """.format(datasets_table=self.datasets_table), dataset_id)
            
            if row and row['validation_results']:
                return row['validation_results']
            
            return None
            
        except Exception as e:
            raise Exception(f"Failed to retrieve validation results: {e}")
    
    # Analytics and Reporting
    
    async def get_schema_usage_analytics(self) -> List[Dict[str, Any]]:
        """Get schema usage analytics."""
        
        conn = await self.get_connection()
        
        try:
            rows = await conn.fetch(f"""
                SELECT * FROM {self.analytics_view}
                ORDER BY usage_count DESC, last_used_at DESC
            """)
            
            return [dict(row) for row in rows]
            
        except Exception as e:
            raise Exception(f"Failed to get schema analytics: {e}")
    
    async def get_dataset_schema_summary(self) -> Dict[str, Any]:
        """Get summary statistics for dataset schemas."""
        
        conn = await self.get_connection()
        
        try:
            summary = await conn.fetchrow(f"""
                SELECT 
                    COUNT(*) as total_datasets,
                    COUNT(DISTINCT schema_hash) as unique_schemas,
                    COUNT(*) FILTER (WHERE validation_results->>'is_valid' = 'true') as valid_datasets,
                    COUNT(*) FILTER (WHERE schema_hash IS NOT NULL) as datasets_with_schema,
                    AVG((validation_results->>'confidence_score')::float) as avg_confidence_score
                FROM {self.datasets_table}
                WHERE created_at >= NOW() - INTERVAL '30 days'
            """)
            
            return dict(summary) if summary else {}
            
        except Exception as e:
            raise Exception(f"Failed to get dataset summary: {e}")
    
    async def find_compatible_schemas(
        self, 
        feature_count: int,
        sequence_length: int,
        symbol: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Find schemas compatible with given parameters."""
        
        conn = await self.get_connection()
        
        try:
            where_conditions = ["status = 'active'"]
            params = []
            
            # Add feature count filter
            where_conditions.append("(schema_json->'metadata'->>'total_features')::int = $1")
            params.append(feature_count)
            
            # Add sequence length filter  
            where_conditions.append("(schema_json->'metadata'->>'sequence_length')::int = $2")
            params.append(sequence_length)
            
            # Add symbol filter if provided
            if symbol:
                where_conditions.append("(schema_json->'metadata'->>'symbol' = $3 OR $3 = ANY(tags))")
                params.append(symbol)
            
            where_clause = " AND ".join(where_conditions)
            
            rows = await conn.fetch(f"""
                SELECT schema_name, schema_version, schema_hash, description,
                       schema_json->'metadata' as metadata,
                       usage_count, last_used_at
                FROM {self.registry_table}
                WHERE {where_clause}
                ORDER BY usage_count DESC
                LIMIT 10
            """, *params)
            
            return [dict(row) for row in rows]
            
        except Exception as e:
            raise Exception(f"Failed to find compatible schemas: {e}")


# Convenience functions

async def create_schema_dao(environment_type: str = 'dev') -> TrainingSchemaDAO:
    """Create a TrainingSchemaDAO instance."""
    from ..config.environment import Environment, EnvironmentType
    
    env_type = EnvironmentType.DEV if environment_type == 'dev' else EnvironmentType.INTG
    env = Environment(env_type=env_type)
    
    return TrainingSchemaDAO(env)


async def register_standard_schemas():
    """Register standard schema templates in the registry."""
    from ..schema.training_schema import create_ohlcv_schema, create_multi_horizon_schema
    
    dao = await create_schema_dao()
    
    try:
        # Standard OHLCV schema
        ohlcv_schema = create_ohlcv_schema(
            dataset_name="standard_ohlcv_60d",
            symbol="TEMPLATE",
            sequence_length=60,
            include_volume=True,
            technical_indicators=["sma_10", "sma_20", "rsi_14", "macd"]
        )
        
        await dao.register_schema(
            schema=ohlcv_schema,
            created_by="ATS System",
            tags=["standard", "ohlcv", "template"],
            description="Standard OHLCV schema with common technical indicators"
        )
        
        # Multi-horizon schema
        multi_horizon_schema = create_multi_horizon_schema(
            dataset_name="multi_horizon_prediction",
            symbol="TEMPLATE",
            horizons=[1, 3, 5, 10],
            sequence_length=60
        )
        
        await dao.register_schema(
            schema=multi_horizon_schema,
            created_by="ATS System", 
            tags=["standard", "multi_horizon", "template"],
            description="Multi-horizon prediction schema with 1, 3, 5, and 10-day targets"
        )
        
        print("✅ Standard schemas registered successfully")
        
    except Exception as e:
        print(f"❌ Failed to register standard schemas: {e}")
    finally:
        await dao.close_connection()


if __name__ == "__main__":
    # Register standard schemas when run directly
    asyncio.run(register_standard_schemas())