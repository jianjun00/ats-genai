# Design Requirements Document (DRD)
## ATS Analytics Platform Architecture

**Document Version:** 2.0  
**Created:** August 2025  
**Technical Lead:** AI Trading System Team  

---

## 1. Architecture Overview

### 1.1 System Design Philosophy
Build a **unified analytics platform** that seamlessly integrates ML workflow management with comprehensive analysis capabilities:
- **Job Orchestration Layer**: Flyte integration with metadata tracking and real-time monitoring
- **Data Management Layer**: Automatic dataset registration, cataloging, and comparison engine
- **Analytics Engine**: Interactive analysis for datasets, model performance, and backtests
- **Visualization Layer**: Rich dashboards with drill-down capabilities and comparison tools

### 1.2 Core Design Principles
- **Workflow-Centric**: Every analysis tied to specific jobs and data lineage
- **Real-Time Visibility**: Live updates for job status, logs, and pipeline progress  
- **Automatic Registration**: Zero-touch dataset cataloging from job completions
- **Comparative Analysis**: Built-in capabilities for side-by-side dataset/model comparison
- **Scalable Architecture**: Support for thousands of jobs and large datasets

---

## 2. System Architecture

### 2.1 High-Level Architecture
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Frontend (React + D3.js)                         │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐ │
│  │ Jobs        │ │ Training    │ │ Dataset     │ │ Backtest Analytics      │ │
│  │ Dashboard   │ │ Datasets    │ │ Comparison  │ │ & Model Performance     │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────────────────┘ │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │ WebSocket + REST API
┌─────────────────────┴───────────────────────────────────────────────────────┐
│                       API Gateway (FastAPI)                                │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐ │
│  │ Job         │ │ Dataset     │ │ Comparison  │ │ Analytics & Real-Time   │ │
│  │ Management  │ │ Service     │ │ Engine      │ │ Event Streaming         │ │
│  │ Service     │ │             │ │             │ │                         │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────────────────┘ │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────────────────────────────────┐
│                            Data & Integration Layer                         │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐ │
│  │ PostgreSQL  │ │ Redis Cache │ │ File System │ │ Flyte Integration       │ │
│  │ (Jobs &     │ │ (Computed   │ │ (Training   │ │ (Workflow Metadata,     │ │
│  │ Datasets    │ │ Metrics &   │ │ Datasets &  │ │  Status, Logs)          │ │
│  │ Metadata)   │ │ Sessions)   │ │ Artifacts)  │ │                         │ │
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow Architecture
```
Flyte Jobs → Job Tracker → Dataset Registrar → Analytics Engine → Web UI
     ↓           ↓              ↓                    ↓             ↓
[ML Workflows] [Real-time    [Auto-Registration] [Comparative   [Interactive
 Training      Status &      Dataset Metadata   Analysis &     Dashboards
 Backtests     Log Stream]   File System       Visualization] Drill-Down]
 Data Gen]                   Integration]       Caching]       Real-time]
```

---

## 3. Component Design

### 3.1 Job Management Architecture

#### 3.1.1 Job Tracking Schema
```sql
-- Core job tracking with Flyte integration
CREATE TABLE job_runs (
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

-- Job type enumeration
CREATE TYPE job_type_enum AS ENUM (
    'training_data_gen',
    'model_training', 
    'backtest',
    'data_validation',
    'model_evaluation'
);

-- Job status enumeration  
CREATE TYPE job_status_enum AS ENUM (
    'pending',
    'running',
    'succeeded',
    'failed',
    'cancelled',
    'timeout'
);

-- Job dependencies tracking
CREATE TABLE job_dependencies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID REFERENCES job_runs(job_id) ON DELETE CASCADE,
    depends_on_job_id UUID REFERENCES job_runs(job_id) ON DELETE CASCADE,
    dependency_type VARCHAR(50) NOT NULL, -- 'dataset', 'model', 'prerequisite'
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(job_id, depends_on_job_id)
);

-- Performance indexes
CREATE INDEX idx_job_runs_type_status ON job_runs(job_type, status, created_at DESC);
CREATE INDEX idx_job_runs_user_date ON job_runs(user_id, created_at DESC);
CREATE INDEX idx_job_runs_flyte_id ON job_runs(flyte_execution_id);
CREATE INDEX idx_job_dependencies_job ON job_dependencies(job_id);
```

#### 3.1.2 Flyte Integration Service
```python
import asyncio
from typing import Dict, List, Optional, AsyncGenerator
from dataclasses import dataclass
from datetime import datetime
import aiohttp
from flytekit.remote import FlyteRemote
from flytekit.models.execution import WorkflowExecution

@dataclass
class JobExecutionInfo:
    """Comprehensive job execution information from Flyte"""
    execution_id: str
    workflow_name: str
    status: str
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    duration: Optional[int]
    inputs: Dict
    outputs: Optional[Dict]
    error: Optional[str]
    resource_usage: Dict
    logs_url: str

class FlyteJobTracker:
    """Tracks Flyte job executions and syncs with analytics database"""
    
    def __init__(self, flyte_remote: FlyteRemote, db_client, redis_client):
        self.flyte = flyte_remote
        self.db = db_client
        self.redis = redis_client
        self.tracked_executions = set()
        
    async def start_tracking(self):
        """Begin continuous tracking of Flyte executions"""
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self.sync_active_executions()),
            asyncio.create_task(self.monitor_new_executions()),
            asyncio.create_task(self.stream_logs())
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def sync_active_executions(self):
        """Continuously sync status of active executions"""
        
        while True:
            try:
                # Get active jobs from database
                active_jobs = await self.db.fetch(
                    "SELECT flyte_execution_id FROM job_runs WHERE status IN ('pending', 'running')"
                )
                
                for job in active_jobs:
                    execution_id = job['flyte_execution_id']
                    if execution_id:
                        await self.sync_execution_status(execution_id)
                
                # Wait before next sync
                await asyncio.sleep(30)  # 30-second sync interval
                
            except Exception as e:
                print(f"Error syncing executions: {e}")
                await asyncio.sleep(60)  # Wait longer on error
    
    async def sync_execution_status(self, execution_id: str):
        """Sync single execution status from Flyte"""
        
        try:
            execution = await self.flyte.fetch_execution(name=execution_id)
            execution_info = self.extract_execution_info(execution)
            
            # Update database
            await self.db.execute("""
                UPDATE job_runs SET
                    status = $1,
                    start_time = $2,
                    end_time = $3,
                    duration_seconds = $4,
                    error_message = $5,
                    resource_usage = $6,
                    updated_at = NOW()
                WHERE flyte_execution_id = $7
            """, 
                execution_info.status,
                execution_info.start_time,
                execution_info.end_time, 
                execution_info.duration,
                execution_info.error,
                execution_info.resource_usage,
                execution_id
            )
            
            # Publish status update
            await self.publish_job_update(execution_id, execution_info)
            
            # Handle completion events
            if execution_info.status in ('succeeded', 'failed'):
                await self.handle_job_completion(execution_id, execution_info)
            
        except Exception as e:
            print(f"Error syncing execution {execution_id}: {e}")
    
    async def monitor_new_executions(self):
        """Monitor for new Flyte executions to track"""
        
        while True:
            try:
                # Fetch recent executions from Flyte
                recent_executions = await self.flyte.list_executions(
                    project="ats",
                    domain="development", 
                    limit=100
                )
                
                for execution in recent_executions:
                    if execution.id.name not in self.tracked_executions:
                        await self.register_new_execution(execution)
                        self.tracked_executions.add(execution.id.name)
                
                await asyncio.sleep(60)  # Check for new executions every minute
                
            except Exception as e:
                print(f"Error monitoring new executions: {e}")
                await asyncio.sleep(120)
    
    async def register_new_execution(self, execution: WorkflowExecution):
        """Register new Flyte execution in database"""
        
        execution_info = self.extract_execution_info(execution)
        
        # Determine job type from workflow name
        job_type = self.infer_job_type(execution_info.workflow_name)
        
        # Insert into database
        job_id = await self.db.fetchval("""
            INSERT INTO job_runs (
                job_name, job_type, user_id, flyte_execution_id,
                flyte_workflow_name, status, parameters,
                start_time, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            RETURNING job_id
        """,
            execution_info.workflow_name,
            job_type,
            execution.spec.auth_role.assumable_iam_role or "system",
            execution_info.execution_id,
            execution_info.workflow_name,
            execution_info.status,
            execution_info.inputs,
            execution_info.start_time
        )
        
        print(f"Registered new job: {job_id} for execution {execution_info.execution_id}")
    
    async def handle_job_completion(self, execution_id: str, execution_info: JobExecutionInfo):
        """Handle job completion events (dataset registration, etc.)"""
        
        # Get job details
        job = await self.db.fetchrow(
            "SELECT * FROM job_runs WHERE flyte_execution_id = $1",
            execution_id
        )
        
        if not job:
            return
        
        # Handle training data generation completion
        if job['job_type'] == 'training_data_gen' and execution_info.status == 'succeeded':
            await self.register_training_dataset(job, execution_info)
        
        # Handle model training completion
        elif job['job_type'] == 'model_training' and execution_info.status == 'succeeded':
            await self.register_trained_model(job, execution_info)
        
        # Handle backtest completion
        elif job['job_type'] == 'backtest' and execution_info.status == 'succeeded':
            await self.register_backtest_results(job, execution_info)
    
    async def register_training_dataset(self, job: dict, execution_info: JobExecutionInfo):
        """Automatically register training dataset from completed job"""
        
        try:
            # Extract dataset information from job outputs
            outputs = execution_info.outputs or {}
            
            # Parse dataset metadata from outputs
            dataset_path = outputs.get('dataset_path')
            dataset_metadata = outputs.get('metadata', {})
            
            if not dataset_path:
                print(f"No dataset path in outputs for job {job['job_id']}")
                return
            
            # Register dataset
            dataset_id = await self.db.fetchval("""
                INSERT INTO training_datasets (
                    dataset_name, source_job_id, symbols, start_date, end_date,
                    total_sequences, feature_count, technical_indicators,
                    quality_metrics, file_path, file_size_bytes, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
                RETURNING dataset_id
            """,
                dataset_metadata.get('dataset_name', f"dataset_{job['job_id']}"),
                job['job_id'],
                dataset_metadata.get('symbols', []),
                dataset_metadata.get('start_date'),
                dataset_metadata.get('end_date'), 
                dataset_metadata.get('total_sequences', 0),
                dataset_metadata.get('feature_count', 0),
                dataset_metadata.get('technical_indicators', []),
                dataset_metadata.get('quality_metrics', {}),
                dataset_path,
                dataset_metadata.get('file_size_bytes', 0)
            )
            
            print(f"Registered training dataset: {dataset_id} from job {job['job_id']}")
            
            # Publish dataset registration event
            await self.redis.publish('dataset_registered', {
                'dataset_id': str(dataset_id),
                'job_id': str(job['job_id']),
                'dataset_name': dataset_metadata.get('dataset_name')
            })
            
        except Exception as e:
            print(f"Error registering training dataset for job {job['job_id']}: {e}")
    
    def extract_execution_info(self, execution: WorkflowExecution) -> JobExecutionInfo:
        """Extract relevant information from Flyte execution"""
        
        return JobExecutionInfo(
            execution_id=execution.id.name,
            workflow_name=execution.spec.workflow_id.name,
            status=execution.closure.phase.lower(),
            start_time=execution.closure.started_at,
            end_time=execution.closure.finished_at,
            duration=self.calculate_duration(execution),
            inputs=self.extract_inputs(execution),
            outputs=self.extract_outputs(execution),
            error=execution.closure.error.message if execution.closure.error else None,
            resource_usage=self.extract_resource_usage(execution),
            logs_url=f"/flyte/console/projects/ats/domains/development/executions/{execution.id.name}"
        )
    
    def infer_job_type(self, workflow_name: str) -> str:
        """Infer job type from Flyte workflow name"""
        
        workflow_lower = workflow_name.lower()
        
        if 'training_data' in workflow_lower or 'data_gen' in workflow_lower:
            return 'training_data_gen'
        elif 'training' in workflow_lower and 'model' in workflow_lower:
            return 'model_training'
        elif 'backtest' in workflow_lower:
            return 'backtest'
        elif 'validation' in workflow_lower:
            return 'data_validation'
        else:
            return 'model_training'  # Default
    
    async def stream_logs(self) -> AsyncGenerator[Dict, None]:
        """Stream logs from Flyte executions"""
        
        # Implementation would stream logs from Flyte
        # This is a placeholder for the log streaming functionality
        pass
```

### 3.2 Training Dataset Management

#### 3.2.1 Dataset Schema & Indexing
```sql
-- Comprehensive training dataset tracking
CREATE TABLE training_datasets (
    dataset_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_name VARCHAR(255) NOT NULL,
    source_job_id UUID REFERENCES job_runs(job_id),
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
CREATE TABLE dataset_features (
    feature_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID REFERENCES training_datasets(dataset_id) ON DELETE CASCADE,
    feature_name VARCHAR(255) NOT NULL,
    feature_type VARCHAR(50) NOT NULL, -- 'numeric', 'categorical', 'boolean'
    feature_description TEXT,
    min_value DECIMAL,
    max_value DECIMAL,
    mean_value DECIMAL,
    std_value DECIMAL,
    null_count INTEGER,
    unique_count INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Dataset quality metrics
CREATE TABLE dataset_quality_metrics (
    metric_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID REFERENCES training_datasets(dataset_id) ON DELETE CASCADE,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL NOT NULL,
    metric_description TEXT,
    computed_at TIMESTAMPTZ DEFAULT NOW()
);

-- Performance indexes
CREATE INDEX idx_training_datasets_job ON training_datasets(source_job_id);
CREATE INDEX idx_training_datasets_symbols ON training_datasets USING GIN(symbols);
CREATE INDEX idx_training_datasets_date_range ON training_datasets(start_date, end_date);
CREATE INDEX idx_training_datasets_created ON training_datasets(created_at DESC);
CREATE INDEX idx_dataset_features_dataset ON dataset_features(dataset_id);
CREATE INDEX idx_dataset_quality_dataset ON dataset_quality_metrics(dataset_id);

-- Full-text search
CREATE INDEX idx_training_datasets_search ON training_datasets USING GIN(
    to_tsvector('english', dataset_name || ' ' || array_to_string(symbols, ' ') || ' ' || array_to_string(tags, ' '))
);
```

#### 3.2.2 Dataset Service Implementation
```python
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import json
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, date
import asyncio

@dataclass
class DatasetInfo:
    """Comprehensive dataset information"""
    dataset_id: str
    dataset_name: str
    source_job_id: str
    symbols: List[str]
    start_date: date
    end_date: date
    total_sequences: int
    feature_count: int
    technical_indicators: List[str]
    quality_metrics: Dict
    file_path: str
    file_size_bytes: int
    created_at: datetime

@dataclass
class FeatureDistribution:
    """Statistical distribution of a dataset feature"""
    feature_name: str
    feature_type: str
    min_value: float
    max_value: float
    mean_value: float
    std_value: float
    percentiles: Dict[int, float]  # 25th, 50th, 75th, etc.
    histogram_data: Tuple[List[float], List[float]]  # bins, counts
    null_count: int
    unique_count: int

class TrainingDatasetService:
    """Service for managing training datasets"""
    
    def __init__(self, db_client, file_storage_client, redis_client):
        self.db = db_client
        self.storage = file_storage_client
        self.redis = redis_client
    
    async def register_dataset(
        self,
        dataset_name: str,
        source_job_id: str,
        file_path: str,
        metadata: Dict
    ) -> str:
        """Register new training dataset with automatic quality analysis"""
        
        # Load and analyze dataset
        dataset_analysis = await self.analyze_dataset_file(file_path)
        
        # Merge with provided metadata
        combined_metadata = {**metadata, **dataset_analysis}
        
        # Insert dataset record
        dataset_id = await self.db.fetchval("""
            INSERT INTO training_datasets (
                dataset_name, source_job_id, symbols, start_date, end_date,
                total_sequences, feature_count, technical_indicators,
                quality_metrics, file_path, file_size_bytes
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING dataset_id
        """,
            dataset_name,
            source_job_id,
            combined_metadata['symbols'],
            combined_metadata['start_date'],
            combined_metadata['end_date'],
            combined_metadata['total_sequences'],
            combined_metadata['feature_count'],
            combined_metadata['technical_indicators'],
            combined_metadata['quality_metrics'],
            file_path,
            combined_metadata['file_size_bytes']
        )
        
        # Register features metadata
        await self.register_dataset_features(dataset_id, combined_metadata['features'])
        
        # Cache dataset metadata
        await self.cache_dataset_info(dataset_id)
        
        return str(dataset_id)
    
    async def analyze_dataset_file(self, file_path: str) -> Dict:
        """Perform comprehensive analysis of dataset file"""
        
        # Load dataset (supports various formats)
        if file_path.endswith('.parquet'):
            df = pd.read_parquet(file_path)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith('.npy'):
            # Handle numpy arrays with metadata
            data = np.load(file_path, allow_pickle=True)
            df = pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
        
        # Basic dataset metrics
        total_sequences = len(df)
        feature_count = len(df.columns)
        file_size = Path(file_path).stat().st_size
        
        # Analyze features
        feature_analysis = {}
        quality_metrics = {}
        
        for column in df.columns:
            feature_stats = self.analyze_feature(df[column])
            feature_analysis[column] = feature_stats
        
        # Overall quality metrics
        quality_metrics = {
            'completeness': (df.notna().sum().sum() / (len(df) * len(df.columns))) * 100,
            'duplicate_rows': df.duplicated().sum(),
            'duplicate_percentage': (df.duplicated().sum() / len(df)) * 100,
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'numeric_features': len(df.select_dtypes(include=[np.number]).columns),
            'categorical_features': len(df.select_dtypes(include=['object', 'category']).columns)
        }
        
        # Extract metadata from column names and data
        symbols = self.extract_symbols_from_data(df)
        date_range = self.extract_date_range_from_data(df)
        technical_indicators = self.identify_technical_indicators(df.columns.tolist())
        
        return {
            'symbols': symbols,
            'start_date': date_range[0] if date_range else None,
            'end_date': date_range[1] if date_range else None,
            'total_sequences': total_sequences,
            'feature_count': feature_count,
            'technical_indicators': technical_indicators,
            'quality_metrics': quality_metrics,
            'features': feature_analysis,
            'file_size_bytes': file_size
        }
    
    async def get_dataset_info(self, dataset_id: str) -> Optional[DatasetInfo]:
        """Get comprehensive dataset information"""
        
        # Check cache first
        cached = await self.redis.get(f"dataset_info:{dataset_id}")
        if cached:
            return DatasetInfo(**json.loads(cached))
        
        # Fetch from database
        dataset_record = await self.db.fetchrow(
            "SELECT * FROM training_datasets WHERE dataset_id = $1",
            dataset_id
        )
        
        if not dataset_record:
            return None
        
        dataset_info = DatasetInfo(**dict(dataset_record))
        
        # Cache result
        await self.redis.setex(
            f"dataset_info:{dataset_id}",
            3600,  # 1 hour TTL
            json.dumps(dataset_info.__dict__, default=str)
        )
        
        return dataset_info
    
    async def get_dataset_distributions(
        self,
        dataset_id: str,
        feature_names: Optional[List[str]] = None
    ) -> Dict[str, FeatureDistribution]:
        """Get feature distributions for dataset"""
        
        dataset_info = await self.get_dataset_info(dataset_id)
        if not dataset_info:
            raise ValueError(f"Dataset {dataset_id} not found")
        
        # Load dataset file
        df = self.load_dataset_file(dataset_info.file_path)
        
        # Filter features if specified
        if feature_names:
            df = df[feature_names]
        
        # Calculate distributions
        distributions = {}
        for column in df.columns:
            distributions[column] = self.calculate_feature_distribution(df[column])
        
        return distributions
    
    async def list_datasets(
        self,
        filters: Optional[Dict] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Tuple[List[DatasetInfo], int]:
        """List datasets with filtering and pagination"""
        
        where_conditions = []
        params = []
        param_count = 0
        
        # Build dynamic query based on filters
        if filters:
            if 'job_type' in filters:
                param_count += 1
                where_conditions.append(f"source_job_id IN (SELECT job_id FROM job_runs WHERE job_type = ${param_count})")
                params.append(filters['job_type'])
            
            if 'symbols' in filters:
                param_count += 1
                where_conditions.append(f"symbols && ${param_count}")
                params.append(filters['symbols'])
            
            if 'start_date' in filters:
                param_count += 1
                where_conditions.append(f"start_date >= ${param_count}")
                params.append(filters['start_date'])
            
            if 'end_date' in filters:
                param_count += 1
                where_conditions.append(f"end_date <= ${param_count}")
                params.append(filters['end_date'])
            
            if 'search' in filters:
                param_count += 1
                where_conditions.append(f"to_tsvector('english', dataset_name || ' ' || array_to_string(symbols, ' ')) @@ plainto_tsquery(${param_count})")
                params.append(filters['search'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Count total results
        count_query = f"SELECT COUNT(*) FROM training_datasets {where_clause}"
        total_count = await self.db.fetchval(count_query, *params)
        
        # Fetch paginated results
        param_count += 1
        limit_param = param_count
        param_count += 1
        offset_param = param_count
        
        query = f"""
            SELECT * FROM training_datasets 
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${limit_param} OFFSET ${offset_param}
        """
        
        records = await self.db.fetch(query, *params, limit, offset)
        datasets = [DatasetInfo(**dict(record)) for record in records]
        
        return datasets, total_count
    
    def analyze_feature(self, series: pd.Series) -> Dict:
        """Analyze individual feature statistics"""
        
        if pd.api.types.is_numeric_dtype(series):
            return {
                'type': 'numeric',
                'min_value': float(series.min()),
                'max_value': float(series.max()),
                'mean_value': float(series.mean()),
                'std_value': float(series.std()),
                'null_count': int(series.isna().sum()),
                'unique_count': int(series.nunique()),
                'percentiles': {
                    25: float(series.quantile(0.25)),
                    50: float(series.quantile(0.50)),
                    75: float(series.quantile(0.75))
                }
            }
        else:
            return {
                'type': 'categorical',
                'null_count': int(series.isna().sum()),
                'unique_count': int(series.nunique()),
                'most_common': series.value_counts().head(10).to_dict()
            }
    
    def calculate_feature_distribution(self, series: pd.Series) -> FeatureDistribution:
        """Calculate detailed feature distribution for visualization"""
        
        if pd.api.types.is_numeric_dtype(series):
            # Calculate histogram
            counts, bins = np.histogram(series.dropna(), bins=50)
            
            return FeatureDistribution(
                feature_name=series.name,
                feature_type='numeric',
                min_value=float(series.min()),
                max_value=float(series.max()),
                mean_value=float(series.mean()),
                std_value=float(series.std()),
                percentiles={
                    5: float(series.quantile(0.05)),
                    25: float(series.quantile(0.25)),
                    50: float(series.quantile(0.50)),
                    75: float(series.quantile(0.75)),
                    95: float(series.quantile(0.95))
                },
                histogram_data=(bins.tolist(), counts.tolist()),
                null_count=int(series.isna().sum()),
                unique_count=int(series.nunique())
            )
        else:
            # For categorical data
            value_counts = series.value_counts()
            
            return FeatureDistribution(
                feature_name=series.name,
                feature_type='categorical',
                min_value=0,
                max_value=0,
                mean_value=0,
                std_value=0,
                percentiles={},
                histogram_data=(value_counts.index.tolist(), value_counts.values.tolist()),
                null_count=int(series.isna().sum()),
                unique_count=int(series.nunique())
            )
```

### 3.3 Dataset Comparison Engine

#### 3.3.1 Comparison Schema & Service
```sql
-- Dataset comparisons tracking
CREATE TABLE dataset_comparisons (
    comparison_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_a_id UUID REFERENCES training_datasets(dataset_id),
    dataset_b_id UUID REFERENCES training_datasets(dataset_id),
    comparison_type VARCHAR(50) NOT NULL, -- 'distribution', 'quality', 'full'
    comparison_results JSONB NOT NULL,
    statistical_tests JSONB,
    difference_score DECIMAL(5,4),
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(dataset_a_id, dataset_b_id, comparison_type)
);

-- Feature-level comparison results
CREATE TABLE feature_comparisons (
    comparison_id UUID REFERENCES dataset_comparisons(comparison_id) ON DELETE CASCADE,
    feature_name VARCHAR(255) NOT NULL,
    ks_statistic DECIMAL(8,6),
    ks_p_value DECIMAL(8,6),
    jensen_shannon_divergence DECIMAL(8,6),
    mean_difference DECIMAL(10,6),
    variance_ratio DECIMAL(8,4),
    distribution_shift_score DECIMAL(5,4),
    PRIMARY KEY (comparison_id, feature_name)
);

-- Indexes for comparison performance
CREATE INDEX idx_dataset_comparisons_datasets ON dataset_comparisons(dataset_a_id, dataset_b_id);
CREATE INDEX idx_feature_comparisons_comparison ON feature_comparisons(comparison_id);
```

```python
import scipy.stats as stats
from scipy.spatial.distance import jensenshannon
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
from dataclasses import dataclass

@dataclass
class ComparisonResult:
    """Results of dataset comparison"""
    comparison_id: str
    dataset_a_id: str
    dataset_b_id: str
    overall_difference_score: float
    feature_comparisons: Dict[str, Dict]
    statistical_tests: Dict[str, Dict]
    recommendations: List[str]
    
@dataclass
class FeatureComparison:
    """Feature-level comparison results"""
    feature_name: str
    ks_statistic: float
    ks_p_value: float
    jensen_shannon_divergence: float
    mean_difference: float
    variance_ratio: float
    distribution_shift_score: float
    recommendation: str

class DatasetComparisonEngine:
    """Engine for comparing training datasets"""
    
    def __init__(self, db_client, dataset_service, redis_client):
        self.db = db_client
        self.dataset_service = dataset_service
        self.redis = redis_client
    
    async def compare_datasets(
        self,
        dataset_a_id: str,
        dataset_b_id: str,
        comparison_type: str = 'full',
        user_id: str = 'system'
    ) -> ComparisonResult:
        """Perform comprehensive dataset comparison"""
        
        # Check for existing comparison
        existing = await self.get_cached_comparison(dataset_a_id, dataset_b_id, comparison_type)
        if existing:
            return existing
        
        # Load both datasets
        dataset_a_info = await self.dataset_service.get_dataset_info(dataset_a_id)
        dataset_b_info = await self.dataset_service.get_dataset_info(dataset_b_id)
        
        if not dataset_a_info or not dataset_b_info:
            raise ValueError("One or both datasets not found")
        
        # Load actual data
        data_a = self.dataset_service.load_dataset_file(dataset_a_info.file_path)
        data_b = self.dataset_service.load_dataset_file(dataset_b_info.file_path)
        
        # Perform comparison
        comparison_results = await self.perform_comparison(data_a, data_b, comparison_type)
        
        # Store comparison results
        comparison_id = await self.store_comparison_results(
            dataset_a_id, dataset_b_id, comparison_type, comparison_results, user_id
        )
        
        result = ComparisonResult(
            comparison_id=comparison_id,
            dataset_a_id=dataset_a_id,
            dataset_b_id=dataset_b_id,
            overall_difference_score=comparison_results['overall_score'],
            feature_comparisons=comparison_results['feature_comparisons'],
            statistical_tests=comparison_results['statistical_tests'],
            recommendations=comparison_results['recommendations']
        )
        
        # Cache result
        await self.cache_comparison_result(result)
        
        return result
    
    async def perform_comparison(
        self,
        data_a: pd.DataFrame,
        data_b: pd.DataFrame,
        comparison_type: str
    ) -> Dict:
        """Perform statistical comparison between datasets"""
        
        # Align datasets (common features only)
        common_features = list(set(data_a.columns) & set(data_b.columns))
        
        if not common_features:
            raise ValueError("No common features between datasets")
        
        data_a_aligned = data_a[common_features]
        data_b_aligned = data_b[common_features]
        
        feature_comparisons = {}
        overall_scores = []
        
        # Compare each feature
        for feature in common_features:
            feature_comparison = self.compare_feature(
                data_a_aligned[feature], 
                data_b_aligned[feature]
            )
            feature_comparisons[feature] = feature_comparison
            overall_scores.append(feature_comparison['distribution_shift_score'])
        
        # Calculate overall difference score
        overall_score = np.mean(overall_scores)
        
        # Statistical tests
        statistical_tests = self.perform_statistical_tests(data_a_aligned, data_b_aligned)
        
        # Generate recommendations
        recommendations = self.generate_recommendations(
            feature_comparisons, statistical_tests, overall_score
        )
        
        return {
            'overall_score': overall_score,
            'feature_comparisons': feature_comparisons,
            'statistical_tests': statistical_tests,
            'recommendations': recommendations,
            'common_features_count': len(common_features),
            'missing_in_a': list(set(data_b.columns) - set(data_a.columns)),
            'missing_in_b': list(set(data_a.columns) - set(data_b.columns))
        }
    
    def compare_feature(self, series_a: pd.Series, series_b: pd.Series) -> Dict:
        """Compare two feature series statistically"""
        
        # Remove NaN values
        clean_a = series_a.dropna()
        clean_b = series_b.dropna()
        
        if len(clean_a) == 0 or len(clean_b) == 0:
            return {
                'ks_statistic': 1.0,
                'ks_p_value': 0.0,
                'jensen_shannon_divergence': 1.0,
                'mean_difference': 0.0,
                'variance_ratio': 1.0,
                'distribution_shift_score': 1.0,
                'recommendation': 'Insufficient data for comparison'
            }
        
        if pd.api.types.is_numeric_dtype(series_a):
            return self.compare_numeric_feature(clean_a, clean_b)
        else:
            return self.compare_categorical_feature(clean_a, clean_b)
    
    def compare_numeric_feature(self, series_a: pd.Series, series_b: pd.Series) -> Dict:
        """Compare numeric features with statistical tests"""
        
        # Kolmogorov-Smirnov test
        ks_stat, ks_p = stats.ks_2samp(series_a, series_b)
        
        # Jensen-Shannon divergence
        # Create histograms with same bins
        min_val = min(series_a.min(), series_b.min())
        max_val = max(series_a.max(), series_b.max())
        bins = np.linspace(min_val, max_val, 50)
        
        hist_a, _ = np.histogram(series_a, bins=bins, density=True)
        hist_b, _ = np.histogram(series_b, bins=bins, density=True)
        
        # Normalize to probabilities
        hist_a = hist_a / hist_a.sum()
        hist_b = hist_b / hist_b.sum()
        
        js_divergence = jensenshannon(hist_a, hist_b)
        
        # Basic statistics comparison
        mean_diff = abs(series_a.mean() - series_b.mean()) / max(series_a.std(), series_b.std(), 1e-10)
        variance_ratio = series_a.var() / max(series_b.var(), 1e-10)
        
        # Combined distribution shift score (0 = identical, 1 = completely different)
        shift_score = min(1.0, (ks_stat + js_divergence) / 2)
        
        # Generate recommendation
        if shift_score < 0.1:
            recommendation = "Distributions are very similar - datasets are comparable"
        elif shift_score < 0.3:
            recommendation = "Minor distribution differences - acceptable for most use cases"
        elif shift_score < 0.6:
            recommendation = "Moderate distribution shift - consider data source differences"
        else:
            recommendation = "Significant distribution shift - investigate data quality and sources"
        
        return {
            'ks_statistic': float(ks_stat),
            'ks_p_value': float(ks_p),
            'jensen_shannon_divergence': float(js_divergence),
            'mean_difference': float(mean_diff),
            'variance_ratio': float(variance_ratio),
            'distribution_shift_score': float(shift_score),
            'recommendation': recommendation
        }
    
    def compare_categorical_feature(self, series_a: pd.Series, series_b: pd.Series) -> Dict:
        """Compare categorical features"""
        
        # Get value counts
        counts_a = series_a.value_counts(normalize=True).fillna(0)
        counts_b = series_b.value_counts(normalize=True).fillna(0)
        
        # Align categories
        all_categories = list(set(counts_a.index) | set(counts_b.index))
        aligned_a = [counts_a.get(cat, 0) for cat in all_categories]
        aligned_b = [counts_b.get(cat, 0) for cat in all_categories]
        
        # Chi-square test
        try:
            chi2_stat, chi2_p = stats.chisquare(aligned_a, aligned_b)
        except:
            chi2_stat, chi2_p = 1.0, 0.0
        
        # Jensen-Shannon divergence
        js_divergence = jensenshannon(aligned_a, aligned_b)
        
        return {
            'ks_statistic': 0.0,  # N/A for categorical
            'ks_p_value': float(chi2_p),
            'jensen_shannon_divergence': float(js_divergence),
            'mean_difference': 0.0,  # N/A for categorical
            'variance_ratio': 1.0,   # N/A for categorical
            'distribution_shift_score': float(js_divergence),
            'recommendation': f"JS divergence: {js_divergence:.3f} - {'Low' if js_divergence < 0.3 else 'High'} category distribution difference"
        }
    
    def perform_statistical_tests(self, data_a: pd.DataFrame, data_b: pd.DataFrame) -> Dict:
        """Perform overall statistical tests between datasets"""
        
        return {
            'sample_size_a': len(data_a),
            'sample_size_b': len(data_b),
            'feature_count_a': len(data_a.columns),
            'feature_count_b': len(data_b.columns),
            'common_features': len(set(data_a.columns) & set(data_b.columns)),
            'data_coverage_overlap': self.calculate_coverage_overlap(data_a, data_b)
        }
    
    def generate_recommendations(
        self,
        feature_comparisons: Dict,
        statistical_tests: Dict,
        overall_score: float
    ) -> List[str]:
        """Generate actionable recommendations based on comparison"""
        
        recommendations = []
        
        if overall_score < 0.2:
            recommendations.append("✅ Datasets are highly similar - safe to use interchangeably")
        elif overall_score < 0.5:
            recommendations.append("⚠️ Moderate differences detected - review key features before use")
        else:
            recommendations.append("🚨 Significant differences detected - investigate data sources")
        
        # Feature-specific recommendations
        high_drift_features = [
            feature for feature, comp in feature_comparisons.items()
            if comp['distribution_shift_score'] > 0.6
        ]
        
        if high_drift_features:
            recommendations.append(f"🔍 High drift in features: {', '.join(high_drift_features[:5])}")
        
        # Sample size recommendations
        if abs(statistical_tests['sample_size_a'] - statistical_tests['sample_size_b']) > min(statistical_tests['sample_size_a'], statistical_tests['sample_size_b']):
            recommendations.append("📊 Significant sample size difference - consider resampling")
        
        return recommendations
    
    async def store_comparison_results(
        self,
        dataset_a_id: str,
        dataset_b_id: str,
        comparison_type: str,
        results: Dict,
        user_id: str
    ) -> str:
        """Store comparison results in database"""
        
        # Store main comparison
        comparison_id = await self.db.fetchval("""
            INSERT INTO dataset_comparisons (
                dataset_a_id, dataset_b_id, comparison_type,
                comparison_results, statistical_tests, difference_score, created_by
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (dataset_a_id, dataset_b_id, comparison_type)
            DO UPDATE SET
                comparison_results = EXCLUDED.comparison_results,
                statistical_tests = EXCLUDED.statistical_tests,
                difference_score = EXCLUDED.difference_score,
                created_at = NOW()
            RETURNING comparison_id
        """,
            dataset_a_id, dataset_b_id, comparison_type,
            results, results['statistical_tests'],
            results['overall_score'], user_id
        )
        
        # Store feature-level comparisons
        for feature_name, feature_result in results['feature_comparisons'].items():
            await self.db.execute("""
                INSERT INTO feature_comparisons (
                    comparison_id, feature_name, ks_statistic, ks_p_value,
                    jensen_shannon_divergence, mean_difference, variance_ratio,
                    distribution_shift_score
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (comparison_id, feature_name)
                DO UPDATE SET
                    ks_statistic = EXCLUDED.ks_statistic,
                    ks_p_value = EXCLUDED.ks_p_value,
                    jensen_shannon_divergence = EXCLUDED.jensen_shannon_divergence,
                    mean_difference = EXCLUDED.mean_difference,
                    variance_ratio = EXCLUDED.variance_ratio,
                    distribution_shift_score = EXCLUDED.distribution_shift_score
            """,
                comparison_id, feature_name,
                feature_result['ks_statistic'],
                feature_result['ks_p_value'],
                feature_result['jensen_shannon_divergence'],
                feature_result['mean_difference'],
                feature_result['variance_ratio'],
                feature_result['distribution_shift_score']
            )
        
        return str(comparison_id)
```

### 3.4 API Design

#### 3.4.1 Comprehensive REST API
```python
from fastapi import FastAPI, Depends, Query, Path, HTTPException, WebSocket
from typing import List, Optional, Dict
from datetime import date, datetime
import asyncio

app = FastAPI(title="ATS Analytics Platform API", version="2.0")

# Job Management Endpoints
@app.get("/api/v1/jobs", response_model=List[JobRunInfo])
async def list_jobs(
    job_type: Optional[str] = Query(None, regex="^(training_data_gen|model_training|backtest|data_validation)$"),
    status: Optional[str] = Query(None, regex="^(pending|running|succeeded|failed|cancelled)$"),
    user_id: Optional[str] = None,
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0),
    search: Optional[str] = None
):
    """List job runs with filtering and pagination"""
    pass

@app.get("/api/v1/jobs/{job_id}", response_model=JobRunDetail)
async def get_job_detail(job_id: str = Path(...)):
    """Get detailed job information including parameters and execution details"""
    pass

@app.get("/api/v1/jobs/{job_id}/logs")
async def get_job_logs(
    job_id: str = Path(...),
    lines: Optional[int] = Query(1000, le=10000),
    search: Optional[str] = None
):
    """Get job logs with optional filtering"""
    pass

@app.get("/api/v1/jobs/{job_id}/flyte-url")
async def get_flyte_url(job_id: str = Path(...)):
    """Get direct Flyte UI URL for job"""
    pass

# Training Dataset Endpoints
@app.get("/api/v1/datasets", response_model=List[DatasetInfo])
async def list_datasets(
    job_type: Optional[str] = None,
    symbols: Optional[List[str]] = Query(None),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None,
    limit: int = Query(50, le=100),
    offset: int = Query(0, ge=0)
):
    """List training datasets with filtering and search"""
    pass

@app.get("/api/v1/datasets/{dataset_id}", response_model=DatasetDetail)
async def get_dataset_detail(dataset_id: str = Path(...)):
    """Get comprehensive dataset information and metadata"""
    pass

@app.get("/api/v1/datasets/{dataset_id}/distributions")
async def get_dataset_distributions(
    dataset_id: str = Path(...),
    features: Optional[List[str]] = Query(None)
):
    """Get feature distributions for visualization"""
    pass

@app.get("/api/v1/datasets/{dataset_id}/quality")
async def get_dataset_quality_metrics(dataset_id: str = Path(...)):
    """Get data quality assessment and metrics"""
    pass

@app.get("/api/v1/datasets/{dataset_id}/sample")
async def get_dataset_sample(
    dataset_id: str = Path(...),
    sample_size: int = Query(1000, le=10000)
):
    """Get random sample of dataset for preview"""
    pass

# Dataset Comparison Endpoints
@app.post("/api/v1/datasets/compare", response_model=ComparisonResult)
async def compare_datasets(
    dataset_a_id: str,
    dataset_b_id: str,
    comparison_type: str = "full",
    user_id: str = "system"
):
    """Perform comprehensive comparison between two datasets"""
    pass

@app.get("/api/v1/comparisons/{comparison_id}", response_model=ComparisonResult)
async def get_comparison_result(comparison_id: str = Path(...)):
    """Get detailed comparison results"""
    pass

@app.get("/api/v1/comparisons")
async def list_comparisons(
    dataset_id: Optional[str] = None,
    user_id: Optional[str] = None,
    limit: int = Query(20, le=100),
    offset: int = Query(0, ge=0)
):
    """List dataset comparisons with filtering"""
    pass

# Job-to-Dataset Navigation
@app.get("/api/v1/jobs/{job_id}/datasets")
async def get_job_datasets(job_id: str = Path(...)):
    """Get datasets created by specific job"""
    pass

@app.get("/api/v1/datasets/{dataset_id}/source-job")
async def get_dataset_source_job(dataset_id: str = Path(...)):
    """Get job that created the dataset"""
    pass

@app.get("/api/v1/datasets/{dataset_id}/related-jobs")
async def get_dataset_related_jobs(dataset_id: str = Path(...)):
    """Get all jobs that used this dataset (training, backtesting)"""
    pass

# Workflow Visualization
@app.get("/api/v1/workflows/{workflow_id}/pipeline")
async def get_workflow_pipeline(workflow_id: str = Path(...)):
    """Get end-to-end pipeline visualization data"""
    pass

@app.get("/api/v1/workflows/{workflow_id}/dependencies")
async def get_workflow_dependencies(workflow_id: str = Path(...)):
    """Get dependency graph for workflow"""
    pass

# Real-Time WebSocket Endpoints
@app.websocket("/ws/jobs/{job_id}/logs")
async def job_logs_websocket(websocket: WebSocket, job_id: str):
    """Stream real-time job logs"""
    await websocket.accept()
    # Implementation for real-time log streaming
    pass

@app.websocket("/ws/jobs/status")
async def job_status_websocket(websocket: WebSocket):
    """Stream real-time job status updates"""
    await websocket.accept()
    # Implementation for real-time status updates
    pass

@app.websocket("/ws/datasets/registration")
async def dataset_registration_websocket(websocket: WebSocket):
    """Stream real-time dataset registration events"""
    await websocket.accept()
    # Implementation for real-time dataset notifications
    pass

# Analytics Integration (Backtest Results)
@app.get("/api/v1/backtests", response_model=List[BacktestSummary])
async def list_backtests():
    """List backtest results with job lineage"""
    pass

@app.get("/api/v1/backtests/{backtest_id}/training-data")
async def get_backtest_training_data(backtest_id: str = Path(...)):
    """Get training dataset used for specific backtest"""
    pass

@app.get("/api/v1/backtests/{backtest_id}/performance")
async def get_backtest_performance(backtest_id: str = Path(...)):
    """Get comprehensive backtest performance metrics"""
    pass
```

---

## 4. Implementation Strategy

### 4.1 Development Phases

#### Phase 1: Job Management Foundation (4 weeks)
**Deliverables:**
- Flyte integration service with real-time job tracking
- Jobs dashboard with filtering, search, and pagination
- Job detail views with logs integration
- Basic database schema and APIs

**Acceptance Criteria:**
- [ ] All Flyte jobs appear in dashboard within 30 seconds
- [ ] Real-time job status updates work correctly
- [ ] Job logs are viewable and searchable
- [ ] Direct navigation to Flyte UI works

#### Phase 2: Dataset Management & Registration (4 weeks)
**Deliverables:**
- Automatic dataset registration from job completions
- Dataset catalog with search and filtering
- Dataset visualization and quality metrics
- Job-to-dataset navigation links

**Acceptance Criteria:**
- [ ] Datasets automatically registered on job completion
- [ ] Dataset visualizations load within 5 seconds
- [ ] Navigation from jobs to datasets works seamlessly
- [ ] Dataset quality metrics are accurate and comprehensive

#### Phase 3: Dataset Comparison Engine (3 weeks)
**Deliverables:**
- Statistical comparison framework
- Side-by-side distribution visualization
- Comparison reporting and recommendations
- Feature-level drift analysis

**Acceptance Criteria:**
- [ ] Dataset comparisons complete within 30 seconds
- [ ] Statistical tests provide meaningful insights
- [ ] Visual comparisons clearly show differences
- [ ] Recommendations are actionable and accurate

#### Phase 4: Advanced Analytics Integration (3 weeks)
**Deliverables:**
- Backtest results integration with training data lineage
- End-to-end workflow visualization
- Advanced filtering and search capabilities
- Performance optimization and caching

**Acceptance Criteria:**
- [ ] Complete data lineage from jobs to results
- [ ] Workflow visualizations are intuitive and informative
- [ ] System performance meets requirements under load
- [ ] All user stories are fully implemented

---

*This DRD provides the comprehensive technical foundation for building a unified analytics platform that seamlessly integrates ML workflow management with deep analytical capabilities, enabling efficient job management, dataset discovery, and comparative analysis.*