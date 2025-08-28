# ATS EDA Tool - Implementation Plan

**Document Version**: 1.0  
**Date**: August 28, 2025  
**Owner**: Data Infrastructure Team  
**Status**: Ready for Implementation  

---

## 🚀 Quick Start Guide

### Prerequisites
- ATS platform development environment set up
- PostgreSQL database with vendor data tables
- Docker and Python 3.12+ available

### Immediate First Steps

1. **Create Project Structure** (Day 1)
   ```bash
   mkdir -p src/eda/{api,services,models,utils,config}
   mkdir -p src/eda/frontend/{src,public,components}
   mkdir -p tests/eda/{unit,integration}
   mkdir -p config/eda
   ```

2. **Set Up Development Environment** (Day 1-2)
   ```bash
   # Add EDA service to run_dev.py
   python scripts/run_dev.py start --service eda
   python scripts/run_dev.py start --service redis
   ```

3. **Implement Dataset Discovery** (Day 3-5)
   - Connect to existing database connection manager
   - Scan vendor tables for metadata
   - Create basic API endpoints

4. **Build Basic Frontend** (Week 1-2)
   - React app with dataset browser
   - Simple data table with filtering
   - Basic distribution charts

---

## 📁 Project Directory Structure

```
src/eda/
├── main.py                              # FastAPI application entry point
├── api/                                 # API layer
│   ├── __init__.py
│   ├── datasets.py                      # Dataset management endpoints
│   ├── analysis.py                      # Analysis and visualization endpoints  
│   ├── sessions.py                      # Session management endpoints
│   └── health.py                        # Health check endpoints
├── services/                            # Business logic layer
│   ├── __init__.py
│   ├── dataset_service.py               # Dataset discovery and metadata
│   ├── analysis_service.py              # Statistical analysis engine
│   ├── query_service.py                 # SQL query generation and execution
│   ├── cache_service.py                 # Caching and performance optimization
│   └── visualization_service.py         # Chart generation and custom viz logic
├── models/                              # Data models and schemas
│   ├── __init__.py
│   ├── dataset.py                       # Dataset model definitions
│   ├── analysis.py                      # Analysis request/response models
│   ├── visualization.py                 # Visualization configuration models
│   └── session.py                       # Session management models  
├── utils/                               # Utility functions
│   ├── __init__.py
│   ├── db_utils.py                      # Database utilities (extends ATS utils)
│   ├── stats_utils.py                   # Statistical computation helpers
│   ├── viz_utils.py                     # Visualization generation utilities
│   └── cache_utils.py                   # Caching utilities
├── config/                              # Configuration management
│   ├── __init__.py
│   ├── settings.py                      # EDA-specific settings
│   └── visualization_rules.py           # Custom visualization rule definitions
└── frontend/                            # React frontend application
    ├── package.json
    ├── vite.config.ts
    ├── public/
    │   └── index.html
    └── src/
        ├── main.tsx                     # React app entry point
        ├── App.tsx                      # Main application component
        ├── components/                  # React components
        │   ├── DatasetBrowser/
        │   ├── AnalysisWorkspace/
        │   ├── Visualizations/
        │   ├── DataTable/
        │   └── SessionManager/
        ├── services/                    # Frontend API clients
        │   ├── api.ts                   # Main API client
        │   ├── datasets.ts              # Dataset API client
        │   └── analysis.ts              # Analysis API client
        ├── store/                       # Redux store
        │   ├── index.ts
        │   ├── datasetSlice.ts
        │   ├── analysisSlice.ts
        │   └── uiSlice.ts
        ├── types/                       # TypeScript type definitions
        │   ├── dataset.ts
        │   ├── analysis.ts
        │   └── visualization.ts
        └── utils/                       # Frontend utilities
            ├── formatters.ts
            ├── validators.ts
            └── chartHelpers.ts

tests/eda/
├── unit/                                # Unit tests
│   ├── test_dataset_service.py
│   ├── test_analysis_service.py
│   ├── test_query_service.py
│   └── test_visualization_service.py
├── integration/                         # Integration tests
│   ├── test_api_endpoints.py
│   ├── test_database_integration.py
│   └── test_cache_integration.py
└── fixtures/                            # Test data and fixtures
    ├── sample_datasets.py
    └── test_data.sql

config/eda/
├── app_eda.gin                          # Gin configuration for EDA service
├── visualization_rules.json             # Custom visualization rule definitions
└── dataset_mappings.json               # Dataset discovery configuration

k8s/eda/                                 # Kubernetes deployment files
├── eda-deployment.yaml
├── eda-service.yaml
├── redis-deployment.yaml
└── ingress.yaml
```

---

## 🛠️ Implementation Phases

### Phase 1: Foundation (Week 1-4) - MVP

#### Week 1: Project Setup and Dataset Discovery

**Day 1-2: Project Infrastructure**
```bash
# 1. Create project structure
mkdir -p src/eda/{api,services,models,utils,config,frontend}

# 2. Set up basic FastAPI application
touch src/eda/main.py
touch src/eda/api/{__init__.py,datasets.py,health.py}
touch src/eda/services/{__init__.py,dataset_service.py}
touch src/eda/models/{__init__.py,dataset.py}

# 3. Add to run_dev.py services
# Update scripts/run_dev.py to include EDA service configuration
```

**Day 3-5: Dataset Discovery Implementation**
```python
# src/eda/services/dataset_service.py - Initial implementation
from typing import List, Dict, Any
from utils.db_utils import execute_query, get_table_name

class DatasetService:
    def __init__(self):
        self.supported_tables = [
            'dev_daily_prices_tiingo_30year',
            'dev_daily_prices_polygon_30year', 
            'dev_daily_prices_eodhd_30year',
            'dev_instruments_tiingo',
            'dev_instruments_polygon',
            'dev_instruments_eodhd'
        ]
    
    async def discover_datasets(self) -> List[Dict[str, Any]]:
        datasets = []
        for table_name in self.supported_tables:
            try:
                # Get table metadata
                schema_info = await self.get_table_schema(table_name)
                row_count = await self.get_row_count(table_name)
                
                datasets.append({
                    'id': self.generate_dataset_id(table_name),
                    'name': table_name,
                    'display_name': self.format_display_name(table_name),
                    'dataset_type': 'table',
                    'source_type': 'postgres',
                    'schema_info': schema_info,
                    'row_count': row_count,
                    'tags': self.extract_tags(table_name)
                })
            except Exception as e:
                logger.warning(f"Failed to discover table {table_name}: {e}")
                
        return datasets
    
    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        query = f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        columns = execute_query(query)
        return {'columns': columns}
    
    async def get_row_count(self, table_name: str) -> int:
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = execute_query(query)
        return result[0]['count'] if result else 0
```

#### Week 2: Basic API and Frontend Setup

**API Implementation**
```python
# src/eda/api/datasets.py
from fastapi import APIRouter, Depends
from services.dataset_service import DatasetService

router = APIRouter()

@router.get("/datasets")
async def list_datasets():
    service = DatasetService()
    return await service.discover_datasets()

@router.get("/datasets/{dataset_id}")
async def get_dataset(dataset_id: str):
    service = DatasetService()
    return await service.get_dataset_details(dataset_id)

@router.post("/datasets/{dataset_id}/sample")  
async def get_dataset_sample(dataset_id: str, limit: int = 1000):
    service = DatasetService()
    return await service.get_sample_data(dataset_id, limit)
```

**Frontend Setup**
```bash
# Initialize React project
cd src/eda/frontend
npm create vite@latest . -- --template react-ts
npm install @reduxjs/toolkit react-redux @types/react @types/react-dom
npm install recharts plotly.js-react-ts axios
npm install @mui/material @emotion/react @emotion/styled
```

#### Week 3-4: Basic Visualization and Data Browser

**Data Browser Component**
```typescript
// src/eda/frontend/src/components/DatasetBrowser/DatasetList.tsx
interface Dataset {
  id: string;
  name: string;
  display_name: string;
  row_count: number;
  tags: string[];
}

export const DatasetList: React.FC = () => {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState(true);
  
  useEffect(() => {
    fetchDatasets();
  }, []);
  
  const fetchDatasets = async () => {
    try {
      const response = await fetch('/api/v1/datasets');
      const data = await response.json();
      setDatasets(data.datasets);
    } catch (error) {
      console.error('Failed to fetch datasets:', error);
    } finally {
      setLoading(false);
    }
  };
  
  return (
    <div className="dataset-list">
      <h2>Available Datasets</h2>
      {loading ? <div>Loading...</div> : (
        <div className="dataset-grid">
          {datasets.map(dataset => (
            <DatasetCard key={dataset.id} dataset={dataset} />
          ))}
        </div>
      )}
    </div>
  );
};
```

### Phase 2: Advanced Analytics (Week 5-8)

#### Week 5-6: Statistical Analysis and Visualizations

**Analysis Service Implementation**
```python
# src/eda/services/analysis_service.py
import pandas as pd
import numpy as np
from typing import Dict, Any, List
import plotly.graph_objects as go
import plotly.express as px

class AnalysisService:
    def __init__(self):
        self.cache_service = CacheService()
    
    async def generate_distribution_analysis(self, dataset_id: str, column: str, filters: Dict = None):
        cache_key = f"distribution:{dataset_id}:{column}:{hash(str(filters))}"
        
        # Check cache first
        cached_result = await self.cache_service.get(cache_key)
        if cached_result:
            return cached_result
        
        # Load data with filters
        data = await self.load_filtered_data(dataset_id, filters)
        column_data = data[column].dropna()
        
        # Generate statistical summary
        stats = {
            'mean': float(column_data.mean()),
            'median': float(column_data.median()),
            'std': float(column_data.std()),
            'min': float(column_data.min()),
            'max': float(column_data.max()),
            'q25': float(column_data.quantile(0.25)),
            'q75': float(column_data.quantile(0.75)),
            'null_count': int(data[column].isnull().sum()),
            'unique_count': int(column_data.nunique())
        }
        
        # Generate histogram
        hist_data, bins = np.histogram(column_data, bins=30)
        histogram = {
            'bins': bins.tolist(),
            'counts': hist_data.tolist(),
            'density': (hist_data / hist_data.sum()).tolist()
        }
        
        result = {
            'analysis_type': 'distribution',
            'column': column,
            'statistics': stats,
            'histogram': histogram,
            'generated_at': datetime.now().isoformat()
        }
        
        # Cache result
        await self.cache_service.set(cache_key, result, ttl=3600)
        
        return result
    
    async def generate_ohlc_analysis(self, dataset_id: str, config: Dict):
        """Generate OHLC candlestick chart for financial data"""
        data = await self.load_filtered_data(dataset_id, config.get('filters'))
        
        # Ensure required columns exist
        required_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'trade_date']
        if not all(col in data.columns for col in required_cols):
            raise ValueError(f"Dataset missing required OHLC columns: {required_cols}")
        
        # Sort by date
        data = data.sort_values('trade_date')
        
        # Create OHLC chart data
        ohlc_data = {
            'dates': data['trade_date'].dt.strftime('%Y-%m-%d').tolist(),
            'open': data['open_price'].tolist(),
            'high': data['high_price'].tolist(),
            'low': data['low_price'].tolist(),
            'close': data['close_price'].tolist(),
            'volume': data.get('volume', []).tolist() if 'volume' in data.columns else None
        }
        
        return {
            'analysis_type': 'ohlc',
            'data': ohlc_data,
            'config': {
                'chart_type': 'candlestick',
                'show_volume': 'volume' in data.columns,
                'date_range': [data['trade_date'].min().isoformat(), 
                              data['trade_date'].max().isoformat()]
            }
        }
```

#### Week 7-8: Dataset Comparison Features

**Comparison Service**
```python
# src/eda/services/comparison_service.py
from scipy import stats
from typing import Tuple, Dict, Any

class ComparisonService:
    async def compare_distributions(self, dataset1_id: str, dataset2_id: str, 
                                  column: str, filters1: Dict = None, filters2: Dict = None):
        # Load data from both datasets
        data1 = await self.load_column_data(dataset1_id, column, filters1)
        data2 = await self.load_column_data(dataset2_id, column, filters2)
        
        # Statistical tests
        ks_statistic, ks_pvalue = stats.ks_2samp(data1, data2)
        t_statistic, t_pvalue = stats.ttest_ind(data1, data2)
        
        # Summary statistics comparison
        comparison = {
            'dataset1': {
                'mean': float(data1.mean()),
                'std': float(data1.std()),
                'count': len(data1)
            },
            'dataset2': {
                'mean': float(data2.mean()), 
                'std': float(data2.std()),
                'count': len(data2)
            },
            'statistical_tests': {
                'ks_test': {'statistic': ks_statistic, 'p_value': ks_pvalue},
                't_test': {'statistic': t_statistic, 'p_value': t_pvalue}
            },
            'significant_difference': ks_pvalue < 0.05
        }
        
        return comparison
```

### Phase 3: Intelligence and Polish (Week 9-16)

#### Week 9-12: Data Quality and Operational Features

**Data Quality Service**
```python
# src/eda/services/data_quality_service.py
class DataQualityService:
    def assess_data_quality(self, dataset_id: str) -> Dict[str, Any]:
        data = self.load_dataset(dataset_id)
        
        quality_metrics = {}
        overall_score = 0
        
        for column in data.columns:
            col_quality = {
                'completeness': 1 - (data[column].isnull().sum() / len(data)),
                'uniqueness': data[column].nunique() / len(data),
                'validity': self.check_data_validity(data[column]),
                'consistency': self.check_data_consistency(data[column])
            }
            
            # Calculate column quality score
            col_score = np.mean(list(col_quality.values()))
            col_quality['score'] = col_score
            quality_metrics[column] = col_quality
            overall_score += col_score
            
        return {
            'overall_score': overall_score / len(data.columns),
            'column_quality': quality_metrics,
            'recommendations': self.generate_quality_recommendations(quality_metrics)
        }
```

#### Week 13-16: Performance Optimization and UX Polish

**Caching and Performance**
```python
# src/eda/services/cache_service.py
import redis
import pickle
from typing import Any, Optional

class CacheService:
    def __init__(self):
        self.redis_client = redis.Redis(host='redis', port=6379, db=0)
        self.memory_cache = {}
        
    async def get(self, key: str) -> Optional[Any]:
        # Try memory cache first
        if key in self.memory_cache:
            return self.memory_cache[key]
            
        # Try Redis cache
        cached_data = self.redis_client.get(key)
        if cached_data:
            result = pickle.loads(cached_data)
            self.memory_cache[key] = result  # Populate memory cache
            return result
            
        return None
        
    async def set(self, key: str, value: Any, ttl: int = 3600):
        # Store in both caches
        self.memory_cache[key] = value
        self.redis_client.setex(key, ttl, pickle.dumps(value))
```

---

## 🔧 Development Environment Setup

### Integration with run_dev.py

Add to `scripts/run_dev.py`:

```python
# Add to services configuration
services = {
    # ... existing services ...
    "eda": {
        "image": "dragonflyer762/ats-genai:latest",
        "port": "3001:8080",
        "command": "python src/eda/main.py",
        "env": {
            "DB_HOST": "postgres",
            "DB_PORT": "5432", 
            "REDIS_HOST": "redis",
            "EDA_LOG_LEVEL": "DEBUG",
            "PYTHONPATH": "/workspace/src"
        }
    },
    "redis": {
        "image": "redis:7-alpine",
        "port": "6379:6379",
        "command": "redis-server --appendonly yes"
    }
}
```

### Initial Dependencies

```txt
# requirements-eda.txt
fastapi>=0.104.0
uvicorn>=0.24.0
pandas>=2.1.0
numpy>=1.25.0
scipy>=1.11.0
plotly>=5.17.0
redis>=5.0.0
sqlalchemy>=2.0.0
pydantic>=2.4.0
python-multipart>=0.0.6
```

### Database Migration

```sql
-- Create EDA metadata tables
-- src/db/migrations/050_create_eda_tables.sql

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Dataset catalog
CREATE TABLE eda_datasets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    display_name VARCHAR(255) NOT NULL,
    description TEXT,
    dataset_type VARCHAR(50) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    source_config JSONB NOT NULL,
    schema_info JSONB,
    row_count BIGINT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    tags TEXT[],
    is_active BOOLEAN DEFAULT true
);

-- Column metadata
CREATE TABLE eda_dataset_columns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id UUID REFERENCES eda_datasets(id) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    column_type VARCHAR(100) NOT NULL,
    statistical_profile JSONB,
    data_quality_score FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes
CREATE INDEX idx_eda_datasets_name ON eda_datasets(name);
CREATE INDEX idx_eda_datasets_active ON eda_datasets(is_active);
CREATE INDEX idx_eda_dataset_columns_dataset ON eda_dataset_columns(dataset_id);
```

---

## 🎯 Success Metrics and Milestones

### Week 4 Milestone: MVP Demo
- [ ] Dataset discovery working for all vendor tables
- [ ] Basic web interface showing dataset list
- [ ] Simple data browser with sample data
- [ ] Basic distribution charts (histograms)
- [ ] Integration with ATS development environment

### Week 8 Milestone: Alpha Release
- [ ] OHLC candlestick charts for price data
- [ ] Dataset comparison functionality
- [ ] Advanced filtering capabilities  
- [ ] Statistical analysis features
- [ ] Caching and performance optimization

### Week 12 Milestone: Beta Release
- [ ] Data quality scoring and monitoring
- [ ] Operational health dashboards
- [ ] Session management and sharing
- [ ] Advanced custom visualization rules
- [ ] Production deployment ready

### Week 16 Milestone: Production Release
- [ ] Full performance optimization
- [ ] Comprehensive testing and security review
- [ ] User training and documentation
- [ ] Monitoring and alerting setup
- [ ] Go-live with full user adoption

---

## 🚀 Getting Started Commands

```bash
# 1. Create project structure
mkdir -p src/eda/{api,services,models,utils,config,frontend}
mkdir -p tests/eda/{unit,integration}
mkdir -p config/eda

# 2. Start development environment
python scripts/run_dev.py start --service postgres
python scripts/run_dev.py start --service redis  
python scripts/run_dev.py start --service eda

# 3. Run database migration
python scripts/run_dev.py query --file src/db/migrations/050_create_eda_tables.sql

# 4. Test EDA service
curl http://localhost:3001/health
curl http://localhost:3001/api/v1/datasets

# 5. Start frontend development
cd src/eda/frontend
npm install
npm run dev
```

---

## 📋 Next Steps

1. **Review and approve PRD/DRD** with stakeholders
2. **Set up project structure** following the implementation plan
3. **Begin Phase 1 development** with dataset discovery
4. **Iterate based on user feedback** throughout development
5. **Plan production deployment** and user training

**The ATS EDA Tool is ready to transform how the team explores and validates their financial datasets!** 🚀

---

**This implementation plan provides a clear roadmap from concept to production, with concrete code examples and development practices that integrate seamlessly with the existing ATS platform architecture.**