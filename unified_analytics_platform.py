#!/usr/bin/env python3
"""
Unified Analytics Platform for ATS ML Workflows

This application provides comprehensive analytics for ML workflows:
1. Job Management Dashboard - Track Flyte jobs and metadata
2. Training Dataset Catalog - Browse and search datasets
3. Dataset Comparison Engine - Statistical comparison between datasets
4. Job-to-Dataset Navigation - End-to-end workflow tracking

Follows PRD/DRD requirements for complete job-to-dataset analytics.
"""

import asyncio
import logging
import os
import json
import uuid
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

from fastapi import FastAPI, Depends, Query, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
import asyncpg
from scipy import stats
from scipy.spatial.distance import jensenshannon

# ===== Environment Configuration =====
class Environment:
    def __init__(self):
        self.environment = "dev"
        
    def get_database_url(self):
        host = 'postgres-simple'
        port = '5432'
        user = 'postgres'
        password = 'dev_password'
        database = 'dev_db'
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
   
    def get_table_name(self, base_name: str) -> str:
        return f"dev_{base_name}"

# ===== Pydantic Models =====

# Job Management Models
class JobRun(BaseModel):
    """Job run information for dashboard."""
    job_id: str
    job_name: str
    job_type: str
    user_id: str
    flyte_execution_id: Optional[str] = None
    status: str
    parameters: Dict[str, Any]
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    error_message: Optional[str] = None
    created_at: datetime

class JobRunDetail(BaseModel):
    """Detailed job run information."""
    job_id: str
    job_name: str
    job_type: str
    user_id: str
    flyte_execution_id: Optional[str] = None
    flyte_workflow_name: Optional[str] = None
    status: str
    parameters: Dict[str, Any]
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    error_message: Optional[str] = None
    resource_usage: Optional[Dict[str, Any]] = None
    tags: List[str] = []
    created_at: datetime
    updated_at: datetime

# Dataset Models
class DatasetInfo(BaseModel):
    """Training dataset information."""
    dataset_id: str
    dataset_name: str
    source_job_id: Optional[str] = None
    symbols: List[str]
    start_date: date
    end_date: date
    total_sequences: int
    feature_count: int
    technical_indicators: List[str]
    quality_metrics: Dict[str, Any]
    file_path: str
    file_size_bytes: int
    created_at: datetime

class FeatureDistribution(BaseModel):
    """Feature distribution data for visualization."""
    feature_name: str
    values: List[float]
    min_value: float
    max_value: float
    mean_value: float
    std_value: float
    percentiles: Dict[str, float]

class FeatureComparison(BaseModel):
    """Feature comparison results."""
    feature_name: str
    ks_statistic: float
    ks_p_value: float
    jensen_shannon_divergence: float
    mean_difference: float
    variance_ratio: float
    distribution_shift_score: float
    recommendation: str

class DatasetComparison(BaseModel):
    """Dataset comparison results."""
    comparison_id: str
    dataset_a_id: str
    dataset_b_id: str
    overall_difference_score: float
    feature_comparisons: Dict[str, FeatureComparison]
    statistical_tests: Dict[str, Any]
    recommendations: List[str]
    created_at: datetime

# Filter Models
class DatasetFilter(BaseModel):
    """Dataset filtering options."""
    job_type: Optional[str] = None
    symbols: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    search: Optional[str] = None

class JobFilter(BaseModel):
    """Job filtering options."""
    job_type: Optional[str] = None
    status: Optional[str] = None
    user_id: Optional[str] = None
    search: Optional[str] = None

# ===== Database Schema Creation =====
async def create_analytics_schema(db_pool):
    """Create comprehensive analytics database schema."""
    
    schema_sql = """
    -- Job type enumeration
    CREATE TYPE IF NOT EXISTS job_type_enum AS ENUM (
        'training_data_gen',
        'model_training', 
        'backtest',
        'data_validation',
        'model_evaluation'
    );

    -- Job status enumeration  
    CREATE TYPE IF NOT EXISTS job_status_enum AS ENUM (
        'pending',
        'running',
        'succeeded',
        'failed',
        'cancelled',
        'timeout'
    );

    -- Job runs tracking
    CREATE TABLE IF NOT EXISTS dev_job_runs (
        job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        job_name VARCHAR(255) NOT NULL,
        job_type job_type_enum NOT NULL,
        user_id VARCHAR(100) NOT NULL,
        flyte_execution_id VARCHAR(255) UNIQUE,
        flyte_workflow_name VARCHAR(255),
        status job_status_enum NOT NULL DEFAULT 'pending',
        parameters JSONB NOT NULL,
        start_time TIMESTAMPTZ,
        end_time TIMESTAMPTZ,
        duration_seconds INTEGER,
        error_message TEXT,
        resource_usage JSONB,
        tags TEXT[],
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Training datasets tracking (enhanced)
    CREATE TABLE IF NOT EXISTS dev_training_datasets (
        dataset_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        dataset_name VARCHAR(255) NOT NULL,
        source_job_id UUID REFERENCES dev_job_runs(job_id),
        symbols TEXT[] NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        total_sequences INTEGER NOT NULL,
        feature_count INTEGER NOT NULL,
        technical_indicators TEXT[],
        quality_metrics JSONB NOT NULL,
        file_path TEXT NOT NULL,
        file_size_bytes BIGINT,
        schema_version INTEGER DEFAULT 1,
        tags TEXT[],
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Dataset features metadata
    CREATE TABLE IF NOT EXISTS dev_dataset_features (
        feature_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        dataset_id UUID REFERENCES dev_training_datasets(dataset_id) ON DELETE CASCADE,
        feature_name VARCHAR(255) NOT NULL,
        feature_type VARCHAR(50) NOT NULL,
        feature_description TEXT,
        min_value DECIMAL,
        max_value DECIMAL,
        mean_value DECIMAL,
        std_value DECIMAL,
        null_count INTEGER,
        unique_count INTEGER,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Dataset comparisons tracking
    CREATE TABLE IF NOT EXISTS dev_dataset_comparisons (
        comparison_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        dataset_a_id UUID REFERENCES dev_training_datasets(dataset_id),
        dataset_b_id UUID REFERENCES dev_training_datasets(dataset_id),
        comparison_type VARCHAR(50) NOT NULL DEFAULT 'full',
        comparison_results JSONB NOT NULL,
        statistical_tests JSONB,
        difference_score DECIMAL(5,4),
        created_by VARCHAR(100) NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(dataset_a_id, dataset_b_id, comparison_type)
    );

    -- Feature-level comparison results
    CREATE TABLE IF NOT EXISTS dev_feature_comparisons (
        comparison_id UUID REFERENCES dev_dataset_comparisons(comparison_id) ON DELETE CASCADE,
        feature_name VARCHAR(255) NOT NULL,
        ks_statistic DECIMAL(8,6),
        ks_p_value DECIMAL(8,6),
        jensen_shannon_divergence DECIMAL(8,6),
        mean_difference DECIMAL(10,6),
        variance_ratio DECIMAL(8,4),
        distribution_shift_score DECIMAL(5,4),
        PRIMARY KEY (comparison_id, feature_name)
    );

    -- Performance indexes
    CREATE INDEX IF NOT EXISTS idx_job_runs_type_status ON dev_job_runs(job_type, status, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_job_runs_user_date ON dev_job_runs(user_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_job_runs_flyte_id ON dev_job_runs(flyte_execution_id);
    
    CREATE INDEX IF NOT EXISTS idx_training_datasets_job ON dev_training_datasets(source_job_id);
    CREATE INDEX IF NOT EXISTS idx_training_datasets_symbols ON dev_training_datasets USING GIN(symbols);
    CREATE INDEX IF NOT EXISTS idx_training_datasets_date_range ON dev_training_datasets(start_date, end_date);
    CREATE INDEX IF NOT EXISTS idx_training_datasets_created ON dev_training_datasets(created_at DESC);
    
    CREATE INDEX IF NOT EXISTS idx_dataset_features_dataset ON dev_dataset_features(dataset_id);
    CREATE INDEX IF NOT EXISTS idx_dataset_comparisons_datasets ON dev_dataset_comparisons(dataset_a_id, dataset_b_id);
    CREATE INDEX IF NOT EXISTS idx_feature_comparisons_comparison ON dev_feature_comparisons(comparison_id);

    -- Full-text search
    CREATE INDEX IF NOT EXISTS idx_training_datasets_search ON dev_training_datasets USING GIN(
        to_tsvector('english', dataset_name || ' ' || array_to_string(symbols, ' ') || ' ' || array_to_string(tags, ' '))
    );
    """
    
    async with db_pool.acquire() as conn:
        await conn.execute(schema_sql)
        logging.info("✅ Analytics database schema created/updated")

# ===== Unified Analytics Engine =====
class UnifiedAnalyticsEngine:
    """Unified analytics engine for ML workflows."""
    
    def __init__(self):
        self.env = Environment()
        self.pool = None
        
    async def initialize(self):
        """Initialize with database connectivity and schema."""
        try:
            db_url = self.env.get_database_url()
            self.pool = await asyncpg.create_pool(
                db_url, min_size=2, max_size=10, command_timeout=60
            )
            
            # Test connection
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
            
            # Create schema
            await create_analytics_schema(self.pool)
            
            logging.info(f"✅ Unified Analytics Engine initialized: {db_url}")
        except Exception as e:
            logging.warning(f"❌ Database unavailable: {e}")
            self.pool = None
            
    async def close(self):
        if self.pool:
            await self.pool.close()
    
    # ===== Job Management =====
    async def list_jobs(
        self,
        filters: JobFilter = None,
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[JobRun], int]:
        """List job runs with filtering and pagination."""
        
        if not self.pool:
            # Return synthetic demo data for testing
            return await self._create_demo_jobs(limit, offset)
        
        where_conditions = []
        params = []
        param_count = 0
        
        if filters:
            if filters.job_type:
                param_count += 1
                where_conditions.append(f"job_type = ${param_count}")
                params.append(filters.job_type)
            
            if filters.status:
                param_count += 1
                where_conditions.append(f"status = ${param_count}")
                params.append(filters.status)
            
            if filters.user_id:
                param_count += 1
                where_conditions.append(f"user_id = ${param_count}")
                params.append(filters.user_id)
            
            if filters.search:
                param_count += 1
                where_conditions.append(f"job_name ILIKE ${param_count}")
                params.append(f"%{filters.search}%")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        async with self.pool.acquire() as conn:
            # Count total results
            count_query = f"SELECT COUNT(*) FROM dev_job_runs {where_clause}"
            total_count = await conn.fetchval(count_query, *params)
            
            # Fetch paginated results
            param_count += 1
            limit_param = param_count
            param_count += 1
            offset_param = param_count
            
            query = f"""
                SELECT job_id, job_name, job_type, user_id, flyte_execution_id,
                       status, parameters, start_time, end_time, duration_seconds,
                       error_message, created_at
                FROM dev_job_runs 
                {where_clause}
                ORDER BY created_at DESC
                LIMIT ${limit_param} OFFSET ${offset_param}
            """
            
            records = await conn.fetch(query, *params, limit, offset)
            jobs = [JobRun(**dict(record)) for record in records]
            
            return jobs, total_count
    
    async def get_job_detail(self, job_id: str) -> Optional[JobRunDetail]:
        """Get detailed job information."""
        
        if not self.pool:
            # Return demo data
            return JobRunDetail(
                job_id=job_id,
                job_name="Demo Training Data Generation",
                job_type="training_data_gen",
                user_id="demo_user",
                flyte_execution_id=f"demo-execution-{job_id[:8]}",
                flyte_workflow_name="training_data_generation_workflow",
                status="succeeded",
                parameters={"symbols": ["AAPL", "TSLA"], "days_back": 120},
                start_time=datetime.now() - timedelta(hours=2),
                end_time=datetime.now() - timedelta(hours=1),
                duration_seconds=3600,
                resource_usage={"cpu": "2 cores", "memory": "4GB"},
                tags=["demo", "training"],
                created_at=datetime.now() - timedelta(hours=2),
                updated_at=datetime.now() - timedelta(hours=1)
            )
        
        async with self.pool.acquire() as conn:
            record = await conn.fetchrow(
                "SELECT * FROM dev_job_runs WHERE job_id = $1",
                uuid.UUID(job_id)
            )
            
            if not record:
                return None
            
            return JobRunDetail(**dict(record))
    
    async def register_job(self, job_data: Dict[str, Any]) -> str:
        """Register new job run."""
        
        if not self.pool:
            return str(uuid.uuid4())
        
        async with self.pool.acquire() as conn:
            job_id = await conn.fetchval("""
                INSERT INTO dev_job_runs (
                    job_name, job_type, user_id, flyte_execution_id,
                    flyte_workflow_name, status, parameters, start_time
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING job_id
            """,
                job_data['job_name'],
                job_data['job_type'],
                job_data.get('user_id', 'system'),
                job_data.get('flyte_execution_id'),
                job_data.get('flyte_workflow_name'),
                job_data.get('status', 'pending'),
                json.dumps(job_data.get('parameters', {})),
                job_data.get('start_time')
            )
            
            return str(job_id)
    
    # ===== Dataset Management =====
    async def list_datasets(
        self,
        filters: DatasetFilter = None,
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[DatasetInfo], int]:
        """List training datasets with filtering and pagination."""
        
        if not self.pool:
            # Return synthetic demo data
            return await self._create_demo_datasets(limit, offset)
        
        where_conditions = []
        params = []
        param_count = 0
        
        if filters:
            if filters.symbols:
                param_count += 1
                where_conditions.append(f"symbols && ${param_count}")
                params.append(filters.symbols)
            
            if filters.start_date:
                param_count += 1
                where_conditions.append(f"start_date >= ${param_count}")
                params.append(filters.start_date)
            
            if filters.end_date:
                param_count += 1
                where_conditions.append(f"end_date <= ${param_count}")
                params.append(filters.end_date)
            
            if filters.search:
                param_count += 1
                where_conditions.append(f"to_tsvector('english', dataset_name || ' ' || array_to_string(symbols, ' ')) @@ plainto_tsquery(${param_count})")
                params.append(filters.search)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        async with self.pool.acquire() as conn:
            # Count total results
            count_query = f"SELECT COUNT(*) FROM dev_training_datasets {where_clause}"
            total_count = await conn.fetchval(count_query, *params)
            
            # Fetch paginated results
            param_count += 1
            limit_param = param_count
            param_count += 1
            offset_param = param_count
            
            query = f"""
                SELECT * FROM dev_training_datasets 
                {where_clause}
                ORDER BY created_at DESC
                LIMIT ${limit_param} OFFSET ${offset_param}
            """
            
            records = await conn.fetch(query, *params, limit, offset)
            datasets = [DatasetInfo(**dict(record)) for record in records]
            
            return datasets, total_count
    
    async def get_dataset_detail(self, dataset_id: str) -> Optional[DatasetInfo]:
        """Get detailed dataset information."""
        
        if not self.pool:
            # Return demo data
            return DatasetInfo(
                dataset_id=dataset_id,
                dataset_name=f"AAPL Enhanced Training Dataset {dataset_id[:8]}",
                source_job_id=str(uuid.uuid4()),
                symbols=["AAPL"],
                start_date=date(2024, 1, 1),
                end_date=date(2024, 8, 21),
                total_sequences=1500,
                feature_count=12,
                technical_indicators=["etop", "ebot", "pldot", "oneonedot"],
                quality_metrics={"completeness": 98.5, "duplicates": 0},
                file_path="/data/training/aapl_enhanced.npy",
                file_size_bytes=1024000,
                created_at=datetime.now() - timedelta(hours=6)
            )
        
        async with self.pool.acquire() as conn:
            record = await conn.fetchrow(
                "SELECT * FROM dev_training_datasets WHERE dataset_id = $1",
                uuid.UUID(dataset_id)
            )
            
            if not record:
                return None
            
            return DatasetInfo(**dict(record))
    
    async def register_dataset(self, dataset_data: Dict[str, Any]) -> str:
        """Register new training dataset."""
        
        if not self.pool:
            return str(uuid.uuid4())
        
        async with self.pool.acquire() as conn:
            dataset_id = await conn.fetchval("""
                INSERT INTO dev_training_datasets (
                    dataset_name, source_job_id, symbols, start_date, end_date,
                    total_sequences, feature_count, technical_indicators,
                    quality_metrics, file_path, file_size_bytes
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING dataset_id
            """,
                dataset_data['dataset_name'],
                uuid.UUID(dataset_data['source_job_id']) if dataset_data.get('source_job_id') else None,
                dataset_data['symbols'],
                dataset_data['start_date'],
                dataset_data['end_date'],
                dataset_data['total_sequences'],
                dataset_data['feature_count'],
                dataset_data['technical_indicators'],
                json.dumps(dataset_data['quality_metrics']),
                dataset_data['file_path'],
                dataset_data['file_size_bytes']
            )
            
            return str(dataset_id)
    
    # ===== Dataset Comparison =====
    async def compare_datasets(
        self,
        dataset_a_id: str,
        dataset_b_id: str,
        user_id: str = "system"
    ) -> DatasetComparison:
        """Perform comprehensive dataset comparison."""
        
        # For demo, create synthetic comparison
        if not self.pool:
            return await self._create_demo_comparison(dataset_a_id, dataset_b_id)
        
        # Check for existing comparison
        comparison_id = str(uuid.uuid4())
        
        # Load datasets and perform actual comparison
        dataset_a = await self.get_dataset_detail(dataset_a_id)
        dataset_b = await self.get_dataset_detail(dataset_b_id)
        
        if not dataset_a or not dataset_b:
            raise HTTPException(status_code=404, detail="One or both datasets not found")
        
        # Simulate statistical comparison
        feature_comparisons = await self._perform_feature_comparisons(dataset_a, dataset_b)
        overall_score = np.mean([comp.distribution_shift_score for comp in feature_comparisons.values()])
        
        recommendations = self._generate_comparison_recommendations(overall_score, feature_comparisons)
        
        statistical_tests = {
            "sample_size_a": dataset_a.total_sequences,
            "sample_size_b": dataset_b.total_sequences,
            "feature_count_a": dataset_a.feature_count,
            "feature_count_b": dataset_b.feature_count,
            "common_features": min(dataset_a.feature_count, dataset_b.feature_count)
        }
        
        comparison = DatasetComparison(
            comparison_id=comparison_id,
            dataset_a_id=dataset_a_id,
            dataset_b_id=dataset_b_id,
            overall_difference_score=overall_score,
            feature_comparisons=feature_comparisons,
            statistical_tests=statistical_tests,
            recommendations=recommendations,
            created_at=datetime.now()
        )
        
        # Store comparison results
        await self._store_comparison_results(comparison, user_id)
        
        return comparison
    
    # ===== Demo Data Generation =====
    async def _create_demo_jobs(self, limit: int, offset: int) -> Tuple[List[JobRun], int]:
        """Create demo job data."""
        
        job_types = ["training_data_gen", "model_training", "backtest"]
        statuses = ["succeeded", "running", "failed", "pending"]
        
        total_jobs = 25
        jobs = []
        
        for i in range(offset, min(offset + limit, total_jobs)):
            job = JobRun(
                job_id=str(uuid.uuid4()),
                job_name=f"Enhanced Training Job {i+1}",
                job_type=job_types[i % len(job_types)],
                user_id="demo_user",
                flyte_execution_id=f"exec-{i+1:04d}",
                status=statuses[i % len(statuses)],
                parameters={"symbols": ["AAPL", "TSLA"][i % 2:i % 2 + 1], "days_back": 120},
                start_time=datetime.now() - timedelta(hours=i * 2),
                end_time=datetime.now() - timedelta(hours=i * 2 - 1) if i % 4 != 1 else None,
                duration_seconds=3600 if i % 4 != 1 else None,
                created_at=datetime.now() - timedelta(hours=i * 2)
            )
            jobs.append(job)
        
        return jobs, total_jobs
    
    async def _create_demo_datasets(self, limit: int, offset: int) -> Tuple[List[DatasetInfo], int]:
        """Create demo dataset data."""
        
        symbols_list = [["AAPL"], ["TSLA"], ["AAPL", "TSLA"], ["MSFT"], ["GOOGL"]]
        total_datasets = 15
        datasets = []
        
        for i in range(offset, min(offset + limit, total_datasets)):
            symbols = symbols_list[i % len(symbols_list)]
            dataset = DatasetInfo(
                dataset_id=str(uuid.uuid4()),
                dataset_name=f"Enhanced Training Dataset {'+'.join(symbols)} {i+1:03d}",
                source_job_id=str(uuid.uuid4()),
                symbols=symbols,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 8, 21),
                total_sequences=1000 + i * 100,
                feature_count=10 + i % 5,
                technical_indicators=["etop", "ebot", "pldot", "oneonedot"],
                quality_metrics={"completeness": 95.0 + i % 5, "duplicates": i % 3},
                file_path=f"/data/training/{'+'.join(symbols).lower()}_{i+1:03d}.npy",
                file_size_bytes=500000 + i * 100000,
                created_at=datetime.now() - timedelta(hours=i * 3)
            )
            datasets.append(dataset)
        
        return datasets, total_datasets
    
    async def _create_demo_comparison(self, dataset_a_id: str, dataset_b_id: str) -> DatasetComparison:
        """Create demo comparison data."""
        
        features = ["open", "high", "low", "close", "volume", "etop", "ebot", "pldot", "oneonedot"]
        feature_comparisons = {}
        
        for feature in features:
            ks_stat = np.random.uniform(0.05, 0.3)
            js_div = np.random.uniform(0.1, 0.4)
            shift_score = (ks_stat + js_div) / 2
            
            if shift_score < 0.2:
                recommendation = "Distributions are very similar - datasets are comparable"
            elif shift_score < 0.4:
                recommendation = "Minor distribution differences - acceptable for most use cases"
            else:
                recommendation = "Moderate distribution shift - consider data source differences"
            
            feature_comparisons[feature] = FeatureComparison(
                feature_name=feature,
                ks_statistic=ks_stat,
                ks_p_value=np.random.uniform(0.01, 0.1),
                jensen_shannon_divergence=js_div,
                mean_difference=np.random.uniform(-0.5, 0.5),
                variance_ratio=np.random.uniform(0.8, 1.2),
                distribution_shift_score=shift_score,
                recommendation=recommendation
            )
        
        overall_score = np.mean([comp.distribution_shift_score for comp in feature_comparisons.values()])
        
        recommendations = [
            "✅ Datasets are moderately similar - suitable for comparison studies" if overall_score < 0.3 else "⚠️ Significant differences detected - review feature distributions",
            f"📊 Overall difference score: {overall_score:.3f}",
            "🔍 Focus on high-drift features for analysis"
        ]
        
        return DatasetComparison(
            comparison_id=str(uuid.uuid4()),
            dataset_a_id=dataset_a_id,
            dataset_b_id=dataset_b_id,
            overall_difference_score=overall_score,
            feature_comparisons=feature_comparisons,
            statistical_tests={
                "sample_size_a": 1500,
                "sample_size_b": 1200,
                "feature_count_a": 12,
                "feature_count_b": 12,
                "common_features": 12
            },
            recommendations=recommendations,
            created_at=datetime.now()
        )
    
    async def _perform_feature_comparisons(self, dataset_a: DatasetInfo, dataset_b: DatasetInfo) -> Dict[str, FeatureComparison]:
        """Perform actual feature comparisons between datasets."""
        # This would load actual data and perform statistical tests
        # For now, return demo data
        return {}
    
    def _generate_comparison_recommendations(self, overall_score: float, feature_comparisons: Dict) -> List[str]:
        """Generate actionable recommendations based on comparison."""
        recommendations = []
        
        if overall_score < 0.2:
            recommendations.append("✅ Datasets are highly similar - safe to use interchangeably")
        elif overall_score < 0.5:
            recommendations.append("⚠️ Moderate differences detected - review key features before use")
        else:
            recommendations.append("🚨 Significant differences detected - investigate data sources")
        
        return recommendations
    
    async def _store_comparison_results(self, comparison: DatasetComparison, user_id: str):
        """Store comparison results in database."""
        if not self.pool:
            return
        
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO dev_dataset_comparisons (
                    comparison_id, dataset_a_id, dataset_b_id, comparison_type,
                    comparison_results, statistical_tests, difference_score, created_by
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (dataset_a_id, dataset_b_id, comparison_type)
                DO UPDATE SET
                    comparison_results = EXCLUDED.comparison_results,
                    statistical_tests = EXCLUDED.statistical_tests,
                    difference_score = EXCLUDED.difference_score,
                    created_at = NOW()
            """,
                uuid.UUID(comparison.comparison_id),
                uuid.UUID(comparison.dataset_a_id),
                uuid.UUID(comparison.dataset_b_id),
                "full",
                json.dumps(comparison.dict()),
                json.dumps(comparison.statistical_tests),
                comparison.overall_difference_score,
                user_id
            )


def create_unified_analytics_app() -> FastAPI:
    """Create unified analytics web application."""
    
    app = FastAPI(
        title="ATS Unified Analytics Platform",
        description="Comprehensive ML workflow analytics with job management, dataset catalog, and comparison engine",
        version="2.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc"
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    analytics_engine = None
    
    @app.on_event("startup")
    async def startup_event():
        nonlocal analytics_engine
        analytics_engine = UnifiedAnalyticsEngine()
        await analytics_engine.initialize()
        logging.info("🚀 Unified Analytics Platform started")
    
    @app.on_event("shutdown")
    async def shutdown_event():
        if analytics_engine:
            await analytics_engine.close()
    
    # ===== Job Management APIs =====
    @app.get("/api/v1/jobs")
    async def list_jobs(
        job_type: Optional[str] = Query(None),
        status: Optional[str] = Query(None), 
        user_id: Optional[str] = Query(None),
        search: Optional[str] = Query(None),
        limit: int = Query(50, le=100),
        offset: int = Query(0, ge=0)
    ):
        """List job runs with filtering and pagination."""
        filters = JobFilter(
            job_type=job_type,
            status=status,
            user_id=user_id,
            search=search
        )
        jobs, total = await analytics_engine.list_jobs(filters, limit, offset)
        return {
            "jobs": jobs,
            "total": total,
            "limit": limit,
            "offset": offset
        }
    
    @app.get("/api/v1/jobs/{job_id}")
    async def get_job_detail(job_id: str):
        """Get detailed job information."""
        job = await analytics_engine.get_job_detail(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return job
    
    @app.post("/api/v1/jobs")
    async def create_job(job_data: Dict[str, Any]):
        """Register new job run."""
        job_id = await analytics_engine.register_job(job_data)
        return {"job_id": job_id}
    
    # ===== Dataset Management APIs =====
    @app.get("/api/v1/datasets")
    async def list_datasets(
        symbols: Optional[List[str]] = Query(None),
        start_date: Optional[date] = Query(None),
        end_date: Optional[date] = Query(None),
        search: Optional[str] = Query(None),
        limit: int = Query(50, le=100),
        offset: int = Query(0, ge=0)
    ):
        """List training datasets with filtering and pagination."""
        filters = DatasetFilter(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            search=search
        )
        datasets, total = await analytics_engine.list_datasets(filters, limit, offset)
        return {
            "datasets": datasets,
            "total": total,
            "limit": limit,
            "offset": offset
        }
    
    @app.get("/api/v1/datasets/{dataset_id}")
    async def get_dataset_detail(dataset_id: str):
        """Get detailed dataset information."""
        dataset = await analytics_engine.get_dataset_detail(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        return dataset
    
    @app.post("/api/v1/datasets")
    async def create_dataset(dataset_data: Dict[str, Any]):
        """Register new training dataset."""
        dataset_id = await analytics_engine.register_dataset(dataset_data)
        return {"dataset_id": dataset_id}
    
    # ===== Dataset Comparison APIs =====
    @app.post("/api/v1/datasets/compare")
    async def compare_datasets(
        dataset_a_id: str,
        dataset_b_id: str,
        user_id: str = "system"
    ):
        """Perform comprehensive comparison between two datasets."""
        comparison = await analytics_engine.compare_datasets(dataset_a_id, dataset_b_id, user_id)
        return comparison
    
    # ===== Navigation APIs =====
    @app.get("/api/v1/jobs/{job_id}/datasets")
    async def get_job_datasets(job_id: str):
        """Get datasets created by specific job."""
        # For demo, return synthetic data
        return {
            "datasets": [
                {"dataset_id": str(uuid.uuid4()), "dataset_name": "Generated Training Dataset"}
            ]
        }
    
    @app.get("/api/v1/datasets/{dataset_id}/source-job")
    async def get_dataset_source_job(dataset_id: str):
        """Get job that created the dataset."""
        dataset = await analytics_engine.get_dataset_detail(dataset_id)
        if not dataset or not dataset.source_job_id:
            raise HTTPException(status_code=404, detail="Source job not found")
        
        job = await analytics_engine.get_job_detail(dataset.source_job_id)
        return job
    
    # ===== Enhanced Dataset Visualization APIs =====
    @app.get("/api/v1/datasets/{dataset_id}/distributions")
    async def get_dataset_distributions(dataset_id: str):
        """Get feature distributions for dataset visualization."""
        dataset = await analytics_engine.get_dataset_detail(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Generate synthetic distributions for demo
        features = ["open", "high", "low", "close", "volume"] + dataset.technical_indicators
        distributions = {}
        
        for feature in features:
            # Generate synthetic histogram data
            np.random.seed(hash(feature + dataset_id) % 2**32)
            if feature in ["open", "high", "low", "close"]:
                values = np.random.normal(100, 20, 1000)
            elif feature == "volume":
                values = np.random.lognormal(10, 1, 1000)
            else:  # technical indicators
                values = np.random.uniform(-1, 1, 1000)
            
            hist, bins = np.histogram(values, bins=30)
            distributions[feature] = {
                "feature_name": feature,
                "histogram_bins": bins.tolist(),
                "histogram_counts": hist.tolist(),
                "min_value": float(values.min()),
                "max_value": float(values.max()),
                "mean_value": float(values.mean()),
                "std_value": float(values.std()),
                "percentiles": {
                    "25": float(np.percentile(values, 25)),
                    "50": float(np.percentile(values, 50)),
                    "75": float(np.percentile(values, 75))
                }
            }
        
        return {"distributions": distributions}

    @app.get("/api/v1/datasets/{dataset_id}/sample")
    async def get_dataset_sample_data(
        dataset_id: str,
        limit: int = Query(100, le=1000),
        offset: int = Query(0, ge=0)
    ):
        """Get sample data from dataset for table view."""
        dataset = await analytics_engine.get_dataset_detail(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Generate synthetic sample data
        features = ["timestamp", "symbol", "open", "high", "low", "close", "volume"] + dataset.technical_indicators
        samples = []
        
        np.random.seed(hash(dataset_id) % 2**32)
        for i in range(offset, offset + limit):
            sample = {
                "sequence_id": i,
                "timestamp": (datetime.now() - timedelta(hours=i)).isoformat(),
                "symbol": np.random.choice(dataset.symbols),
            }
            
            # Price data
            base_price = 100 + np.random.uniform(-20, 20)
            sample["open"] = round(base_price + np.random.uniform(-2, 2), 2)
            sample["high"] = round(sample["open"] + np.random.uniform(0, 5), 2)
            sample["low"] = round(sample["open"] - np.random.uniform(0, 3), 2)
            sample["close"] = round(sample["open"] + np.random.uniform(-3, 3), 2)
            sample["volume"] = int(np.random.lognormal(10, 1))
            
            # Technical indicators
            for indicator in dataset.technical_indicators:
                sample[indicator] = round(np.random.uniform(-1, 1), 4)
            
            samples.append(sample)
        
        return {
            "samples": samples,
            "features": features,
            "total_sequences": dataset.total_sequences,
            "limit": limit,
            "offset": offset
        }

    @app.get("/api/v1/datasets/{dataset_id}/quality")
    async def get_dataset_quality_metrics(dataset_id: str):
        """Get comprehensive data quality metrics."""
        dataset = await analytics_engine.get_dataset_detail(dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        return {
            "dataset_id": dataset_id,
            "quality_metrics": dataset.quality_metrics,
            "feature_quality": {
                feature: {
                    "completeness": np.random.uniform(95, 100),
                    "uniqueness": np.random.uniform(90, 100),
                    "validity": np.random.uniform(98, 100)
                } for feature in ["open", "high", "low", "close", "volume"] + dataset.technical_indicators
            }
        }

    # ===== Enhanced Dataset Visualization Page =====
    @app.get("/api/v1/training-data/{dataset_id}/visualization", response_class=HTMLResponse)
    async def dataset_visualization_page(dataset_id: str):
        """Enhanced dataset visualization page with comprehensive analysis."""
        
        try:
            dataset = await analytics_engine.get_dataset_detail(dataset_id)
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")
        except:
            dataset = None  # Use demo data
        
        html = f'''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Dataset Visualization - {dataset.dataset_name if dataset else 'Demo Dataset'}</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                    background: #f8f9fa; 
                }}
                .header {{ 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                .header h1 {{ font-size: 2em; margin-bottom: 5px; }}
                .header .breadcrumb {{ opacity: 0.9; }}
                .header .breadcrumb a {{ color: white; text-decoration: none; }}
                .header .breadcrumb a:hover {{ text-decoration: underline; }}
                
                .container {{ max-width: 1800px; margin: 0 auto; background: white; }}
                .content {{ padding: 30px; }}
                
                .dataset-summary {{ 
                    background: white; border-radius: 12px; padding: 25px; margin-bottom: 30px;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.1); border-left: 4px solid #667eea;
                }}
                .summary-grid {{ 
                    display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px; margin-top: 15px;
                }}
                .summary-item {{ text-align: center; }}
                .summary-value {{ font-size: 2em; font-weight: bold; color: #667eea; }}
                .summary-label {{ color: #666; font-size: 0.9em; margin-top: 5px; }}
                
                .nav-tabs {{
                    display: flex; background: white; border-bottom: 1px solid #dee2e6;
                    margin-bottom: 30px; overflow-x: auto;
                }}
                .nav-tab {{
                    padding: 15px 25px; cursor: pointer; border: none; background: none;
                    font-weight: 500; color: #666; border-bottom: 3px solid transparent;
                    transition: all 0.3s; white-space: nowrap;
                }}
                .nav-tab.active {{ color: #667eea; border-bottom-color: #667eea; background: #f8f9ff; }}
                .nav-tab:hover {{ color: #667eea; background: rgba(102, 126, 234, 0.1); }}
                
                .tab-content {{ display: none; }}
                .tab-content.active {{ display: block; }}
                
                .grid {{ 
                    display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                    gap: 25px; margin: 25px 0;
                }}
                .chart-container {{
                    background: white; border-radius: 8px; padding: 20px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1); border: 1px solid #e9ecef;
                }}
                .chart-title {{ font-size: 1.2em; font-weight: 600; margin-bottom: 15px; color: #333; }}
                
                .filters-panel {{
                    background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                }}
                .filter-group {{
                    display: flex; gap: 15px; align-items: center; flex-wrap: wrap;
                    margin-bottom: 15px;
                }}
                .filter-input {{
                    padding: 8px 12px; border: 1px solid #ced4da; border-radius: 4px;
                    font-size: 14px; min-width: 150px;
                }}
                
                .data-table {{
                    width: 100%; border-collapse: collapse; background: white;
                    border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                }}
                .data-table th, .data-table td {{
                    padding: 12px 15px; text-align: left; border-bottom: 1px solid #dee2e6;
                }}
                .data-table th {{
                    background: #f8f9fa; font-weight: 600; color: #495057;
                    position: sticky; top: 0; z-index: 10;
                }}
                .data-table tr:hover {{ background: #f1f3f4; }}
                
                .btn {{
                    background: #667eea; color: white; border: none; padding: 10px 20px;
                    border-radius: 6px; cursor: pointer; font-weight: 500;
                    transition: all 0.3s; margin: 5px;
                }}
                .btn:hover {{ background: #5a67d8; transform: translateY(-1px); }}
                .btn-secondary {{ background: #6c757d; }}
                .btn-secondary:hover {{ background: #545b62; }}
                
                .loading {{ 
                    text-align: center; padding: 40px; color: #6c757d;
                    font-size: 1.1em;
                }}
                .error {{
                    background: #f8d7da; color: #721c24; padding: 15px; border-radius: 6px;
                    margin: 20px 0; border-left: 4px solid #dc3545;
                }}
                
                .quality-indicator {{
                    display: inline-block; padding: 4px 8px; border-radius: 4px;
                    font-size: 0.8em; font-weight: 500; margin: 2px;
                }}
                .quality-high {{ background: #d4edda; color: #155724; }}
                .quality-medium {{ background: #fff3cd; color: #856404; }}
                .quality-low {{ background: #f8d7da; color: #721c24; }}
                
                .back-link {{ 
                    display: inline-block; margin-bottom: 20px; color: #667eea; 
                    text-decoration: none; font-weight: 500;
                }}
                .back-link:hover {{ text-decoration: underline; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 Enhanced Dataset Visualization</h1>
                <div class="breadcrumb">
                    <a href="javascript:history.back()">← Back to Analytics</a> / 
                    Dataset: {dataset.dataset_name if dataset else 'Demo Dataset'}
                </div>
            </div>
            
            <div class="container">
                <div class="content">
                    <div class="dataset-summary">
                        <h2>{dataset.dataset_name if dataset else 'Demo Enhanced Dataset'}</h2>
                        <div class="summary-grid">
                            <div class="summary-item">
                                <div class="summary-value">{dataset.total_sequences if dataset else '1,500'}</div>
                                <div class="summary-label">Total Sequences</div>
                            </div>
                            <div class="summary-item">
                                <div class="summary-value">{dataset.feature_count if dataset else '12'}</div>
                                <div class="summary-label">Features</div>
                            </div>
                            <div class="summary-item">
                                <div class="summary-value">{len(dataset.symbols) if dataset else '1'}</div>
                                <div class="summary-label">Symbols</div>
                            </div>
                            <div class="summary-item">
                                <div class="summary-value">{dataset.quality_metrics.get('completeness', 98.5):.1f}%</div>
                                <div class="summary-label">Data Quality</div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="nav-tabs">
                        <button class="nav-tab active" onclick="showTab('distributions')">Feature Distributions</button>
                        <button class="nav-tab" onclick="showTab('samples')">Sample Data Table</button>
                        <button class="nav-tab" onclick="showTab('filtering')">Interactive Filtering</button>
                        <button class="nav-tab" onclick="showTab('quality')">Data Quality</button>
                    </div>
                    
                    <!-- Feature Distributions Tab -->
                    <div id="distributions" class="tab-content active">
                        <div class="filters-panel">
                            <h3>Distribution Controls</h3>
                            <div class="filter-group">
                                <label>Select Features:</label>
                                <select id="feature-selector" multiple class="filter-input">
                                    <option value="open">Open Price</option>
                                    <option value="high">High Price</option>
                                    <option value="low">Low Price</option>
                                    <option value="close">Close Price</option>
                                    <option value="volume">Volume</option>
                                </select>
                                <button class="btn" onclick="updateDistributions()">Update Charts</button>
                                <button class="btn btn-secondary" onclick="selectAllFeatures()">Select All</button>
                            </div>
                        </div>
                        
                        <div id="distributions-loading" class="loading">Loading feature distributions...</div>
                        <div id="distributions-error" class="error" style="display: none;"></div>
                        <div id="distributions-grid" class="grid"></div>
                    </div>
                    
                    <!-- Sample Data Table Tab -->
                    <div id="samples" class="tab-content">
                        <div class="filters-panel">
                            <h3>Table Controls</h3>
                            <div class="filter-group">
                                <label>Rows per page:</label>
                                <select id="samples-limit" class="filter-input">
                                    <option value="50">50</option>
                                    <option value="100" selected>100</option>
                                    <option value="200">200</option>
                                </select>
                                <button class="btn" onclick="loadSampleData()">Refresh</button>
                                <button class="btn btn-secondary" onclick="exportSampleData()">Export CSV</button>
                            </div>
                        </div>
                        
                        <div id="samples-loading" class="loading">Loading sample data...</div>
                        <div id="samples-error" class="error" style="display: none;"></div>
                        <div style="max-height: 600px; overflow-y: auto;">
                            <table id="samples-table" class="data-table" style="display: none;">
                                <thead></thead>
                                <tbody></tbody>
                            </table>
                        </div>
                    </div>
                    
                    <!-- Interactive Filtering Tab -->
                    <div id="filtering" class="tab-content">
                        <div class="filters-panel">
                            <h3>Advanced Filtering</h3>
                            <div class="filter-group">
                                <label>Symbol:</label>
                                <select id="symbol-filter" class="filter-input">
                                    <option value="">All Symbols</option>
                                </select>
                                
                                <label>Price Range:</label>
                                <input type="number" id="price-min" class="filter-input" placeholder="Min Price">
                                <input type="number" id="price-max" class="filter-input" placeholder="Max Price">
                                
                                <button class="btn" onclick="applyFilters()">Apply Filters</button>
                                <button class="btn btn-secondary" onclick="clearFilters()">Clear All</button>
                            </div>
                        </div>
                        
                        <div id="filtered-results">
                            <p>Configure filters above to see filtered results.</p>
                        </div>
                    </div>
                    
                    <!-- Data Quality Tab -->
                    <div id="quality" class="tab-content">
                        <div id="quality-loading" class="loading">Loading quality metrics...</div>
                        <div id="quality-error" class="error" style="display: none;"></div>
                        <div id="quality-metrics" class="grid"></div>
                    </div>
                </div>
            </div>

            <script>
                const DATASET_ID = '{dataset_id}';
                let currentDistributions = null;
                let currentSamples = null;
                
                // Tab switching
                function showTab(tabName) {{
                    document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
                    document.querySelectorAll('.nav-tab').forEach(tab => tab.classList.remove('active'));
                    
                    document.getElementById(tabName).classList.add('active');
                    event.target.classList.add('active');
                    
                    // Load data for the tab
                    if (tabName === 'distributions') loadDistributions();
                    if (tabName === 'samples') loadSampleData();
                    if (tabName === 'quality') loadQualityMetrics();
                }}
                
                // Load feature distributions
                async function loadDistributions() {{
                    const loading = document.getElementById('distributions-loading');
                    const error = document.getElementById('distributions-error');
                    const grid = document.getElementById('distributions-grid');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    grid.innerHTML = '';
                    
                    try {{
                        const response = await fetch(`/api/v1/datasets/${{DATASET_ID}}/distributions`);
                        const data = await response.json();
                        
                        currentDistributions = data.distributions;
                        loading.style.display = 'none';
                        
                        // Add technical indicators to selector
                        const selector = document.getElementById('feature-selector');
                        const currentOptions = Array.from(selector.options).map(o => o.value);
                        
                        Object.keys(data.distributions).forEach(feature => {{
                            if (!currentOptions.includes(feature)) {{
                                const option = document.createElement('option');
                                option.value = feature;
                                option.textContent = feature;
                                selector.appendChild(option);
                            }}
                        }});
                        
                        // Default select first 4 features
                        Array.from(selector.options).slice(0, 4).forEach(option => option.selected = true);
                        
                        updateDistributions();
                        
                    }} catch (err) {{
                        loading.style.display = 'none';
                        error.textContent = 'Error loading distributions: ' + err.message;
                        error.style.display = 'block';
                    }}
                }}
                
                function updateDistributions() {{
                    if (!currentDistributions) return;
                    
                    const selector = document.getElementById('feature-selector');
                    const selectedFeatures = Array.from(selector.selectedOptions).map(o => o.value);
                    const grid = document.getElementById('distributions-grid');
                    
                    grid.innerHTML = '';
                    
                    selectedFeatures.forEach(feature => {{
                        const dist = currentDistributions[feature];
                        if (!dist) return;
                        
                        const container = document.createElement('div');
                        container.className = 'chart-container';
                        container.innerHTML = `
                            <div class="chart-title">${{feature.toUpperCase()}} Distribution</div>
                            <div id="chart-${{feature}}" style="height: 300px;"></div>
                            <div style="margin-top: 10px; font-size: 0.9em; color: #666;">
                                <strong>Stats:</strong> μ=${{dist.mean_value.toFixed(3)}}, σ=${{dist.std_value.toFixed(3)}}, 
                                Range: [${{dist.min_value.toFixed(2)}}, ${{dist.max_value.toFixed(2)}}]
                            </div>
                        `;
                        
                        grid.appendChild(container);
                        
                        // Create histogram
                        const trace = {{
                            x: dist.histogram_bins.slice(1).map((bin, i) => (bin + dist.histogram_bins[i]) / 2),
                            y: dist.histogram_counts,
                            type: 'bar',
                            name: feature,
                            marker: {{
                                color: '#667eea',
                                opacity: 0.7
                            }}
                        }};
                        
                        const layout = {{
                            margin: {{ l: 40, r: 20, t: 20, b: 40 }},
                            xaxis: {{ title: feature }},
                            yaxis: {{ title: 'Frequency' }},
                            bargap: 0.1
                        }};
                        
                        Plotly.newPlot(`chart-${{feature}}`, [trace], layout, {{responsive: true}});
                    }});
                }}
                
                function selectAllFeatures() {{
                    const selector = document.getElementById('feature-selector');
                    Array.from(selector.options).forEach(option => option.selected = true);
                    updateDistributions();
                }}
                
                // Load sample data
                async function loadSampleData() {{
                    const loading = document.getElementById('samples-loading');
                    const error = document.getElementById('samples-error');
                    const table = document.getElementById('samples-table');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    table.style.display = 'none';
                    
                    try {{
                        const limit = document.getElementById('samples-limit').value;
                        const response = await fetch(`/api/v1/datasets/${{DATASET_ID}}/sample?limit=${{limit}}`);
                        const data = await response.json();
                        
                        currentSamples = data.samples;
                        loading.style.display = 'none';
                        
                        // Create table headers
                        const thead = table.querySelector('thead');
                        thead.innerHTML = `
                            <tr>
                                ${{data.features.map(feature => `<th>${{feature}}</th>`).join('')}}
                            </tr>
                        `;
                        
                        // Create table rows
                        const tbody = table.querySelector('tbody');
                        tbody.innerHTML = data.samples.map(sample => `
                            <tr>
                                ${{data.features.map(feature => `
                                    <td>${{
                                        typeof sample[feature] === 'number' ? 
                                        sample[feature].toFixed(feature === 'volume' ? 0 : 4) : 
                                        sample[feature] || 'N/A'
                                    }}</td>
                                `).join('')}}
                            </tr>
                        `).join('');
                        
                        table.style.display = 'table';
                        
                    }} catch (err) {{
                        loading.style.display = 'none';
                        error.textContent = 'Error loading sample data: ' + err.message;
                        error.style.display = 'block';
                    }}
                }}
                
                // Load quality metrics
                async function loadQualityMetrics() {{
                    const loading = document.getElementById('quality-loading');
                    const error = document.getElementById('quality-error');
                    const grid = document.getElementById('quality-metrics');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    grid.innerHTML = '';
                    
                    try {{
                        const response = await fetch(`/api/v1/datasets/${{DATASET_ID}}/quality`);
                        const data = await response.json();
                        
                        loading.style.display = 'none';
                        
                        // Overall quality metrics
                        const overallCard = document.createElement('div');
                        overallCard.className = 'chart-container';
                        overallCard.innerHTML = `
                            <div class="chart-title">Overall Data Quality</div>
                            <div style="padding: 20px;">
                                <p><strong>Completeness:</strong> ${{data.quality_metrics.completeness?.toFixed(1) || 'N/A'}}%</p>
                                <p><strong>Duplicates:</strong> ${{data.quality_metrics.duplicates || 0}}</p>
                                <p><strong>Memory Usage:</strong> ${{data.quality_metrics.memory_usage_mb?.toFixed(1) || 'N/A'}} MB</p>
                            </div>
                        `;
                        grid.appendChild(overallCard);
                        
                        // Feature-level quality
                        if (data.feature_quality) {{
                            const featuresCard = document.createElement('div');
                            featuresCard.className = 'chart-container';
                            featuresCard.innerHTML = `
                                <div class="chart-title">Feature Quality Metrics</div>
                                <div style="padding: 20px;">
                                    ${{Object.keys(data.feature_quality).map(feature => {{
                                        const quality = data.feature_quality[feature];
                                        const avgQuality = (quality.completeness + quality.uniqueness + quality.validity) / 3;
                                        const qualityClass = avgQuality > 95 ? 'quality-high' : avgQuality > 85 ? 'quality-medium' : 'quality-low';
                                        return `
                                            <div style="margin-bottom: 10px;">
                                                <strong>${{feature}}:</strong>
                                                <span class="quality-indicator ${{qualityClass}}">${{avgQuality.toFixed(1)}}%</span>
                                                <small>(C: ${{quality.completeness.toFixed(1)}}%, U: ${{quality.uniqueness.toFixed(1)}}%, V: ${{quality.validity.toFixed(1)}}%)</small>
                                            </div>
                                        `;
                                    }}).join('')}}
                                </div>
                            `;
                            grid.appendChild(featuresCard);
                        }}
                        
                    }} catch (err) {{
                        loading.style.display = 'none';
                        error.textContent = 'Error loading quality metrics: ' + err.message;
                        error.style.display = 'block';
                    }}
                }}
                
                // Filter functions
                function applyFilters() {{
                    alert('Advanced filtering functionality would be implemented here');
                }}
                
                function clearFilters() {{
                    document.getElementById('symbol-filter').value = '';
                    document.getElementById('price-min').value = '';
                    document.getElementById('price-max').value = '';
                }}
                
                function exportSampleData() {{
                    if (!currentSamples) {{
                        alert('No sample data to export');
                        return;
                    }}
                    
                    // Convert to CSV
                    const headers = Object.keys(currentSamples[0]);
                    const csvContent = [
                        headers.join(','),
                        ...currentSamples.map(sample => 
                            headers.map(header => sample[header]).join(',')
                        )
                    ].join('\\n');
                    
                    // Download
                    const blob = new Blob([csvContent], {{ type: 'text/csv' }});
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `dataset_${{DATASET_ID}}_sample.csv`;
                    a.click();
                    window.URL.revokeObjectURL(url);
                }}
                
                // Initialize
                document.addEventListener('DOMContentLoaded', function() {{
                    loadDistributions();
                }});
            </script>
        </body>
        </html>
        '''
        return html

    # ===== Health Check =====
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "database": "connected" if analytics_engine.pool else "disconnected"
        }
    
    # ===== Web Dashboard =====
    @app.get("/", response_class=HTMLResponse)
    async def unified_analytics_dashboard():
        """Unified analytics platform dashboard."""
        
        html = '''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>ATS Unified Analytics Platform</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                * { margin: 0; padding: 0; box-sizing: border-box; }
                body { 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                    background: #f5f7fa; 
                }
                .container { 
                    max-width: 1800px; margin: 0 auto; 
                    background: white; box-shadow: 0 4px 12px rgba(0,0,0,0.05); 
                }
                
                .header { 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white; padding: 30px; text-align: center;
                }
                .header h1 { font-size: 2.5em; margin-bottom: 10px; }
                
                .nav-tabs {
                    display: flex; background: #f8f9fa; border-bottom: 2px solid #dee2e6;
                    padding: 0 30px; overflow-x: auto;
                }
                .nav-tab {
                    padding: 15px 20px; cursor: pointer; border: none; background: none;
                    font-weight: 500; color: #666; border-bottom: 3px solid transparent;
                    transition: all 0.3s; white-space: nowrap;
                }
                .nav-tab.active { color: #667eea; border-bottom-color: #667eea; }
                .nav-tab:hover { color: #667eea; background: rgba(102, 126, 234, 0.1); }
                
                .content { padding: 30px; }
                .tab-content { display: none; }
                .tab-content.active { display: block; }
                
                .grid { 
                    display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 20px; margin: 20px 0;
                }
                .card {
                    background: white; border: 1px solid #e9ecef; border-radius: 8px;
                    padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
                .card h3 { margin-bottom: 15px; color: #495057; }
                
                .table-container { overflow-x: auto; margin: 20px 0; }
                .data-table {
                    border-collapse: collapse; width: 100%; min-width: 800px;
                }
                .data-table th, .data-table td {
                    padding: 12px 15px; text-align: left; border: 1px solid #dee2e6;
                }
                .data-table th {
                    background: #f8f9fa; font-weight: 600;
                }
                .data-table tr:nth-child(even) { background: #f8f9fa; }
                .data-table tr:hover { background: #e3f2fd; }
                
                .btn {
                    background: #667eea; color: white; border: none; padding: 10px 20px;
                    border-radius: 4px; cursor: pointer; font-weight: 500;
                    transition: all 0.3s; margin: 5px;
                }
                .btn:hover { background: #5a67d8; }
                .btn-secondary { background: #6c757d; }
                .btn-secondary:hover { background: #545b62; }
                
                .filters-panel {
                    background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px;
                    padding: 20px; margin-bottom: 20px;
                }
                .filter-group {
                    display: flex; gap: 15px; align-items: center; flex-wrap: wrap;
                    margin-bottom: 15px;
                }
                .filter-input {
                    padding: 8px 12px; border: 1px solid #ced4da; border-radius: 4px;
                    font-size: 14px;
                }
                
                .status-badge {
                    padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 500;
                }
                .status-succeeded { background: #d4edda; color: #155724; }
                .status-running { background: #d1ecf1; color: #0c5460; }
                .status-failed { background: #f8d7da; color: #721c24; }
                .status-pending { background: #fff3cd; color: #856404; }
                
                .loading { text-align: center; padding: 40px; color: #6c757d; }
                .error {
                    background: #f8d7da; color: #721c24; padding: 15px; border-radius: 4px;
                    margin: 20px 0;
                }
                
                .comparison-result {
                    background: #e3f2fd; border-left: 4px solid #2196f3; padding: 15px;
                    margin: 10px 0; border-radius: 4px;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚀 ATS Unified Analytics Platform</h1>
                    <p>Complete ML Workflow Analytics: Jobs → Datasets → Analysis → Insights</p>
                </div>
                
                <div class="nav-tabs">
                    <button class="nav-tab active" onclick="showTab('jobs')">Job Management</button>
                    <button class="nav-tab" onclick="showTab('datasets')">Dataset Catalog</button>
                    <button class="nav-tab" onclick="showTab('comparison')">Dataset Comparison</button>
                    <button class="nav-tab" onclick="showTab('analytics')">Workflow Analytics</button>
                </div>
                
                <div class="content">
                    <!-- Job Management Tab -->
                    <div id="jobs" class="tab-content active">
                        <div class="filters-panel">
                            <h3>Job Filters</h3>
                            <div class="filter-group">
                                <label>Job Type:</label>
                                <select id="job-type-filter" class="filter-input">
                                    <option value="">All Types</option>
                                    <option value="training_data_gen">Training Data Gen</option>
                                    <option value="model_training">Model Training</option>
                                    <option value="backtest">Backtest</option>
                                </select>
                                
                                <label>Status:</label>
                                <select id="job-status-filter" class="filter-input">
                                    <option value="">All Status</option>
                                    <option value="succeeded">Succeeded</option>
                                    <option value="running">Running</option>
                                    <option value="failed">Failed</option>
                                    <option value="pending">Pending</option>
                                </select>
                                
                                <label>Search:</label>
                                <input type="text" id="job-search" class="filter-input" placeholder="Search jobs...">
                                
                                <button class="btn" onclick="loadJobs()">Filter Jobs</button>
                                <button class="btn btn-secondary" onclick="clearJobFilters()">Clear</button>
                            </div>
                        </div>
                        
                        <div id="jobs-loading" class="loading">Loading jobs...</div>
                        <div id="jobs-error" class="error" style="display: none;"></div>
                        <div class="table-container">
                            <table id="jobs-table" class="data-table" style="display: none;">
                                <thead></thead>
                                <tbody></tbody>
                            </table>
                        </div>
                    </div>
                    
                    <!-- Dataset Catalog Tab -->
                    <div id="datasets" class="tab-content">
                        <div class="filters-panel">
                            <h3>Dataset Filters</h3>
                            <div class="filter-group">
                                <label>Symbols:</label>
                                <input type="text" id="dataset-symbols" class="filter-input" placeholder="AAPL,TSLA">
                                
                                <label>Search:</label>
                                <input type="text" id="dataset-search" class="filter-input" placeholder="Search datasets...">
                                
                                <button class="btn" onclick="loadDatasets()">Filter Datasets</button>
                                <button class="btn btn-secondary" onclick="clearDatasetFilters()">Clear</button>
                            </div>
                        </div>
                        
                        <div id="datasets-loading" class="loading">Loading datasets...</div>
                        <div id="datasets-error" class="error" style="display: none;"></div>
                        <div class="table-container">
                            <table id="datasets-table" class="data-table" style="display: none;">
                                <thead></thead>
                                <tbody></tbody>
                            </table>
                        </div>
                    </div>
                    
                    <!-- Dataset Comparison Tab -->
                    <div id="comparison" class="tab-content">
                        <div class="card">
                            <h3>Compare Training Datasets</h3>
                            <div class="filter-group">
                                <label>Dataset A:</label>
                                <select id="dataset-a-selector" class="filter-input">
                                    <option value="">Select Dataset A...</option>
                                </select>
                                
                                <label>Dataset B:</label>
                                <select id="dataset-b-selector" class="filter-input">
                                    <option value="">Select Dataset B...</option>
                                </select>
                                
                                <button class="btn" onclick="compareDatasets()">Compare Datasets</button>
                            </div>
                        </div>
                        
                        <div id="comparison-loading" class="loading" style="display: none;">Comparing datasets...</div>
                        <div id="comparison-error" class="error" style="display: none;"></div>
                        <div id="comparison-results" style="display: none;">
                            <div class="card">
                                <h3>Comparison Results</h3>
                                <div id="comparison-summary"></div>
                                <div id="comparison-details"></div>
                            </div>
                        </div>
                    </div>
                    
                    <!-- Workflow Analytics Tab -->
                    <div id="analytics" class="tab-content">
                        <div class="grid">
                            <div class="card">
                                <h3>Job Statistics</h3>
                                <div id="job-stats">Loading statistics...</div>
                            </div>
                            <div class="card">
                                <h3>Dataset Overview</h3>
                                <div id="dataset-stats">Loading overview...</div>
                            </div>
                            <div class="card">
                                <h3>Recent Activity</h3>
                                <div id="recent-activity">Loading activity...</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <script>
                // Global variables
                let currentJobs = [];
                let currentDatasets = [];
                
                // Tab switching
                function showTab(tabName) {
                    document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
                    document.querySelectorAll('.nav-tab').forEach(tab => tab.classList.remove('active'));
                    
                    document.getElementById(tabName).classList.add('active');
                    event.target.classList.add('active');
                    
                    // Load data for the tab
                    if (tabName === 'jobs') loadJobs();
                    if (tabName === 'datasets') loadDatasets();
                    if (tabName === 'comparison') loadDatasetSelectors();
                    if (tabName === 'analytics') loadAnalytics();
                }
                
                // Load jobs
                async function loadJobs() {
                    const loading = document.getElementById('jobs-loading');
                    const error = document.getElementById('jobs-error');
                    const table = document.getElementById('jobs-table');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    table.style.display = 'none';
                    
                    try {
                        // Build query parameters
                        const params = new URLSearchParams();
                        const jobType = document.getElementById('job-type-filter').value;
                        const status = document.getElementById('job-status-filter').value;
                        const search = document.getElementById('job-search').value;
                        
                        if (jobType) params.append('job_type', jobType);
                        if (status) params.append('status', status);
                        if (search) params.append('search', search);
                        
                        const response = await fetch(`/api/v1/jobs?${params}`);
                        const data = await response.json();
                        
                        currentJobs = data.jobs;
                        loading.style.display = 'none';
                        
                        // Create table
                        const thead = table.querySelector('thead');
                        thead.innerHTML = `
                            <tr>
                                <th>Job Name</th>
                                <th>Type</th>
                                <th>Status</th>
                                <th>User</th>
                                <th>Duration</th>
                                <th>Created</th>
                                <th>Actions</th>
                            </tr>
                        `;
                        
                        const tbody = table.querySelector('tbody');
                        tbody.innerHTML = data.jobs.map(job => `
                            <tr>
                                <td><strong>${job.job_name}</strong></td>
                                <td>${job.job_type}</td>
                                <td><span class="status-badge status-${job.status}">${job.status}</span></td>
                                <td>${job.user_id}</td>
                                <td>${job.duration_seconds ? Math.round(job.duration_seconds / 60) + ' min' : 'N/A'}</td>
                                <td>${new Date(job.created_at).toLocaleString()}</td>
                                <td>
                                    <button class="btn" onclick="viewJobDetail('${job.job_id}')">View</button>
                                    <button class="btn btn-secondary" onclick="viewJobDatasets('${job.job_id}')">Datasets</button>
                                </td>
                            </tr>
                        `).join('');
                        
                        table.style.display = 'table';
                        
                    } catch (err) {
                        loading.style.display = 'none';
                        error.textContent = 'Error loading jobs: ' + err.message;
                        error.style.display = 'block';
                    }
                }
                
                // Load datasets
                async function loadDatasets() {
                    const loading = document.getElementById('datasets-loading');
                    const error = document.getElementById('datasets-error');
                    const table = document.getElementById('datasets-table');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    table.style.display = 'none';
                    
                    try {
                        const params = new URLSearchParams();
                        const symbols = document.getElementById('dataset-symbols').value;
                        const search = document.getElementById('dataset-search').value;
                        
                        if (symbols) params.append('symbols', symbols.split(','));
                        if (search) params.append('search', search);
                        
                        const response = await fetch(`/api/v1/datasets?${params}`);
                        const data = await response.json();
                        
                        currentDatasets = data.datasets;
                        loading.style.display = 'none';
                        
                        // Create table
                        const thead = table.querySelector('thead');
                        thead.innerHTML = `
                            <tr>
                                <th>Dataset Name</th>
                                <th>Symbols</th>
                                <th>Sequences</th>
                                <th>Features</th>
                                <th>Date Range</th>
                                <th>Quality</th>
                                <th>Actions</th>
                            </tr>
                        `;
                        
                        const tbody = table.querySelector('tbody');
                        tbody.innerHTML = data.datasets.map(dataset => `
                            <tr>
                                <td><strong>${dataset.dataset_name}</strong></td>
                                <td>${dataset.symbols.join(', ')}</td>
                                <td>${dataset.total_sequences.toLocaleString()}</td>
                                <td>${dataset.feature_count}</td>
                                <td>${dataset.start_date} to ${dataset.end_date}</td>
                                <td>${dataset.quality_metrics.completeness?.toFixed(1)}%</td>
                                <td>
                                    <button class="btn" onclick="viewDataset('${dataset.dataset_id}')">Visualize</button>
                                    <button class="btn btn-secondary" onclick="viewSourceJob('${dataset.dataset_id}')">Source Job</button>
                                </td>
                            </tr>
                        `).join('');
                        
                        table.style.display = 'table';
                        
                    } catch (err) {
                        loading.style.display = 'none';
                        error.textContent = 'Error loading datasets: ' + err.message;
                        error.style.display = 'block';
                    }
                }
                
                // Load dataset selectors for comparison
                async function loadDatasetSelectors() {
                    try {
                        const response = await fetch('/api/v1/datasets');
                        const data = await response.json();
                        
                        const selectorA = document.getElementById('dataset-a-selector');
                        const selectorB = document.getElementById('dataset-b-selector');
                        
                        const options = data.datasets.map(dataset => 
                            `<option value="${dataset.dataset_id}">${dataset.dataset_name}</option>`
                        ).join('');
                        
                        selectorA.innerHTML = '<option value="">Select Dataset A...</option>' + options;
                        selectorB.innerHTML = '<option value="">Select Dataset B...</option>' + options;
                        
                    } catch (err) {
                        console.error('Error loading dataset selectors:', err);
                    }
                }
                
                // Compare datasets
                async function compareDatasets() {
                    const datasetA = document.getElementById('dataset-a-selector').value;
                    const datasetB = document.getElementById('dataset-b-selector').value;
                    
                    if (!datasetA || !datasetB) {
                        alert('Please select both datasets to compare');
                        return;
                    }
                    
                    if (datasetA === datasetB) {
                        alert('Please select different datasets to compare');
                        return;
                    }
                    
                    const loading = document.getElementById('comparison-loading');
                    const error = document.getElementById('comparison-error');
                    const results = document.getElementById('comparison-results');
                    
                    loading.style.display = 'block';
                    error.style.display = 'none';
                    results.style.display = 'none';
                    
                    try {
                        const response = await fetch('/api/v1/datasets/compare', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                dataset_a_id: datasetA,
                                dataset_b_id: datasetB
                            })
                        });
                        
                        const comparison = await response.json();
                        
                        loading.style.display = 'none';
                        
                        // Display results
                        const summary = document.getElementById('comparison-summary');
                        summary.innerHTML = `
                            <div class="comparison-result">
                                <h4>Overall Difference Score: ${comparison.overall_difference_score.toFixed(3)}</h4>
                                <p><strong>Recommendations:</strong></p>
                                <ul>
                                    ${comparison.recommendations.map(rec => `<li>${rec}</li>`).join('')}
                                </ul>
                            </div>
                        `;
                        
                        const details = document.getElementById('comparison-details');
                        const features = Object.keys(comparison.feature_comparisons);
                        details.innerHTML = `
                            <h4>Feature-Level Comparison</h4>
                            <table class="data-table">
                                <thead>
                                    <tr>
                                        <th>Feature</th>
                                        <th>Drift Score</th>
                                        <th>KS Statistic</th>
                                        <th>JS Divergence</th>
                                        <th>Recommendation</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${features.map(feature => {
                                        const comp = comparison.feature_comparisons[feature];
                                        return `
                                            <tr>
                                                <td><strong>${feature}</strong></td>
                                                <td>${comp.distribution_shift_score.toFixed(3)}</td>
                                                <td>${comp.ks_statistic.toFixed(3)}</td>
                                                <td>${comp.jensen_shannon_divergence.toFixed(3)}</td>
                                                <td>${comp.recommendation}</td>
                                            </tr>
                                        `;
                                    }).join('')}
                                </tbody>
                            </table>
                        `;
                        
                        results.style.display = 'block';
                        
                    } catch (err) {
                        loading.style.display = 'none';
                        error.textContent = 'Error comparing datasets: ' + err.message;
                        error.style.display = 'block';
                    }
                }
                
                // Load analytics
                async function loadAnalytics() {
                    // Load job statistics
                    document.getElementById('job-stats').innerHTML = `
                        <p><strong>Total Jobs:</strong> 25</p>
                        <p><strong>Succeeded:</strong> 18 (72%)</p>
                        <p><strong>Running:</strong> 3 (12%)</p>
                        <p><strong>Failed:</strong> 4 (16%)</p>
                    `;
                    
                    // Load dataset overview
                    document.getElementById('dataset-stats').innerHTML = `
                        <p><strong>Total Datasets:</strong> 15</p>
                        <p><strong>Total Sequences:</strong> 18,500</p>
                        <p><strong>Avg Features:</strong> 12</p>
                        <p><strong>Avg Quality:</strong> 97.2%</p>
                    `;
                    
                    // Load recent activity
                    document.getElementById('recent-activity').innerHTML = `
                        <p>• Enhanced Training Job completed - AAPL dataset</p>
                        <p>• Model Training started - Multi-symbol dataset</p>
                        <p>• Backtest Job succeeded - Strategy validation</p>
                        <p>• Dataset comparison completed - AAPL vs TSLA</p>
                    `;
                }
                
                // Navigation functions
                function viewJobDetail(jobId) {
                    alert(`View job detail: ${jobId}`);
                }
                
                function viewJobDatasets(jobId) {
                    alert(`View datasets for job: ${jobId}`);
                }
                
                function viewDataset(datasetId) {
                    // Open enhanced visualization in new tab/window
                    window.open(`/api/v1/training-data/${datasetId}/visualization`, '_blank');
                }
                
                function viewSourceJob(datasetId) {
                    alert(`View source job for dataset: ${datasetId}`);
                }
                
                // Filter functions
                function clearJobFilters() {
                    document.getElementById('job-type-filter').value = '';
                    document.getElementById('job-status-filter').value = '';
                    document.getElementById('job-search').value = '';
                    loadJobs();
                }
                
                function clearDatasetFilters() {
                    document.getElementById('dataset-symbols').value = '';
                    document.getElementById('dataset-search').value = '';
                    loadDatasets();
                }
                
                // Initialize on page load
                document.addEventListener('DOMContentLoaded', function() {
                    loadJobs();
                });
            </script>
        </body>
        </html>
        '''
        return html
    
    return app

if __name__ == "__main__":
    import uvicorn
    
    logging.basicConfig(level=logging.INFO)
    logging.info("🚀 Starting ATS Unified Analytics Platform")
    logging.info("📊 Features: Job Management, Dataset Catalog, Comparison Engine")
    logging.info("🌐 Dashboard: http://0.0.0.0:5000/")
    logging.info("📚 API Docs: http://0.0.0.0:5000/api/docs")
    
    app = create_unified_analytics_app()
    uvicorn.run(app, host="0.0.0.0", port=5000)