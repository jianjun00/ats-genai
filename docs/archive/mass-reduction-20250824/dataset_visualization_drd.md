# Dataset Visualization Design Requirements Document (DRD)

## System Architecture

### High-Level Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Browser   │    │  FastAPI Server │    │   PostgreSQL    │
│                 │    │                 │    │                 │
│ - Dataset View  │◄──►│ - Dataset API   │◄──►│ - Training Data │
│ - Interactive   │    │ - Sample API    │    │ - Metadata      │
│   Charts        │    │ - Analytics API │    │ - Statistics    │
│ - Filter UI     │    │ - File Loader   │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌─────────────────┐              │
         └──────────────►│  File Storage   │◄─────────────┘
                         │                 │
                         │ - .npy files    │
                         │ - .pkl files    │
                         │ - Cached data   │
                         └─────────────────┘
```

### Component Design

#### 1. Frontend Components (✅ Interactive Table Implemented)
```typescript
interface DatasetVisualizationApp {
  components: {
    // ✅ IMPLEMENTED - Interactive Dataset Table
    InteractiveDatasetTable: Component  // Professional table with sorting, filtering, pagination
    DatasetTableControls: Component     // Filter inputs and pagination controls
    DatasetTableBody: Component         // Table rows with real production data
    
    // Future Implementation
    DatasetDetailDashboard: Component
    FeatureDistributions: Component  
    SampleDataTable: Component
    SampleVisualization: Component
    FilterPanel: Component
    ExportTools: Component
  }
}
```

#### 2. Backend API Design (✅ Enhanced Dataset API Implemented)
```python
class DatasetVisualizationAPI:
    endpoints: {
        # ✅ IMPLEMENTED - Enhanced with filtering and sorting
        "/api/v1/datasets": EnhancedDatasetList,  # ?symbol_filter=&sort_by=&sort_dir=&limit=&offset=
        "/api/v1/datasets/filter": DatasetFilterOptions,  # Available filter options
        
        # Future Implementation
        "/api/v1/datasets/{id}/details": DatasetDetails
        "/api/v1/datasets/{id}/features": FeatureStatistics  
        "/api/v1/datasets/{id}/samples": PaginatedSamples
        "/api/v1/datasets/{id}/distributions": FeatureDistributions
        "/api/v1/datasets/{id}/sample/{index}": SampleDetail
        "/api/v1/datasets/{id}/export": DataExport
    }
```

## Database Schema Design

### Enhanced Dataset Tables
```sql
-- Extend existing dataset metadata
ALTER TABLE dev_training_datasets ADD COLUMN IF NOT EXISTS 
    feature_statistics JSONB,
    sample_statistics JSONB,
    distribution_cache JSONB,
    last_analyzed_at TIMESTAMP;

-- New table for detailed feature information
CREATE TABLE IF NOT EXISTS dev_dataset_features (
    id SERIAL PRIMARY KEY,
    dataset_id VARCHAR(255) NOT NULL,
    feature_name VARCHAR(100) NOT NULL,
    feature_type VARCHAR(50) NOT NULL, -- 'numerical', 'categorical', 'timestamp'
    statistics JSONB NOT NULL, -- {mean, std, min, max, quartiles, etc.}
    distribution_data JSONB, -- histogram bins, density data
    created_at TIMESTAMP DEFAULT NOW(),
    
    FOREIGN KEY (dataset_id) REFERENCES dev_training_datasets(dataset_id),
    INDEX idx_dataset_features (dataset_id, feature_name)
);

-- New table for sample data caching
CREATE TABLE IF NOT EXISTS dev_dataset_samples (
    id SERIAL PRIMARY KEY,
    dataset_id VARCHAR(255) NOT NULL,
    sample_index INTEGER NOT NULL,
    sample_data JSONB NOT NULL, -- Full feature vector
    quality_score DECIMAL(5,3), -- Data quality score for this sample
    is_outlier BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    
    FOREIGN KEY (dataset_id) REFERENCES dev_training_datasets(dataset_id),
    UNIQUE KEY idx_dataset_sample (dataset_id, sample_index),
    INDEX idx_quality_score (dataset_id, quality_score)
);
```

## API Specifications

### 1. Enhanced Dataset List Endpoint (✅ IMPLEMENTED)
```http
GET /api/v1/datasets?limit=50&offset=0&symbol_filter=tsla&sort_by=dataset_name&sort_dir=asc
```

**Query Parameters** (✅ IMPLEMENTED):
- `limit`: Number of datasets per page (default: 50, max: 100)
- `offset`: Starting offset for pagination (default: 0)
- `symbol_filter`: Filter datasets by symbol or name (optional)
- `sort_by`: Sort field - `dataset_name`, `total_sequences`, `feature_count`, `file_size_mb`, `creation_timestamp`
- `sort_dir`: Sort direction - `asc` or `desc` (default: `desc`)

**Response Schema** (✅ IMPLEMENTED):
```json
{
  "datasets": [
    {
      "dataset_id": "integer",
      "dataset_name": "string",
      "symbols": ["string"] | "string",
      "total_sequences": "integer",
      "feature_count": "integer",
      "technical_indicators": "object",
      "created_at": "iso_datetime",
      "file_size_mb": "decimal"
    }
  ],
  "total": "integer"
}
```

**Real Implementation Example**:
```bash
curl "http://172.25.223.121:3000/api/v1/datasets?symbol_filter=tsla&sort_by=dataset_name"
# Returns actual production datasets with TSLA filtering
```

### 2. Dataset Filter Options Endpoint (✅ IMPLEMENTED)
```http
GET /api/v1/datasets/filter
```

**Response Schema** (✅ IMPLEMENTED):
```json
{
  "symbols": ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN"],
  "date_ranges": ["1M", "3M", "6M", "1Y", "2Y"],
  "indicators": ["ema_12", "ema_26", "rsi", "atr", "vwap"],
  "sequence_lengths": [30, 60, 90, 120, 180]
}
```

### 3. Dataset Details Endpoint (Future Implementation)
```http
GET /api/v1/datasets/{dataset_id}/details
```

**Response Schema**:
```json
{
  "dataset_id": "string",
  "metadata": {
    "name": "string",
    "symbols": ["string"],
    "date_range": {"start": "date", "end": "date"},
    "total_sequences": "integer",
    "feature_count": "integer",
    "file_size": "integer"
  },
  "statistics": {
    "numerical_features": "integer",
    "categorical_features": "integer", 
    "missing_values": "integer",
    "data_quality_score": "decimal",
    "outlier_percentage": "decimal"
  },
  "features": [
    {
      "name": "string",
      "type": "string",
      "statistics": {
        "mean": "decimal",
        "std": "decimal", 
        "min": "decimal",
        "max": "decimal",
        "quartiles": ["decimal"],
        "missing_count": "integer"
      }
    }
  ]
}
```

### 2. Feature Distributions Endpoint
```http
GET /api/v1/datasets/{dataset_id}/distributions?features=open,high,low&bins=50
```

**Response Schema**:
```json
{
  "distributions": {
    "open": {
      "histogram": {
        "bins": ["decimal"],
        "counts": ["integer"],
        "density": ["decimal"]
      },
      "statistics": {
        "mean": "decimal",
        "std": "decimal",
        "skewness": "decimal",
        "kurtosis": "decimal"
      }
    }
  },
  "correlations": {
    "matrix": [["decimal"]],
    "feature_names": ["string"]
  }
}
```

### 3. Sample Data Endpoint
```http
GET /api/v1/datasets/{dataset_id}/samples?page=1&limit=100&filter={}&sort=index
```

**Request Parameters**:
- `page`: Page number (1-based)
- `limit`: Samples per page (10-1000)
- `filter`: JSON filter criteria
- `sort`: Sort field and direction
- `search`: Text search across features

**Filter Schema**:
```json
{
  "feature_ranges": {
    "open": {"min": 100, "max": 200},
    "volume": {"min": 1000000}
  },
  "date_range": {
    "start": "2024-01-01",
    "end": "2024-06-30"
  },
  "symbols": ["AAPL", "TSLA"],
  "quality_threshold": 0.8,
  "exclude_outliers": true
}
```

**Response Schema**:
```json
{
  "samples": [
    {
      "index": "integer",
      "data": {
        "open": "decimal",
        "high": "decimal",
        "low": "decimal", 
        "close": "decimal",
        "volume": "integer",
        "technical_indicators": {}
      },
      "metadata": {
        "timestamp": "datetime",
        "symbol": "string",
        "quality_score": "decimal",
        "is_outlier": "boolean"
      }
    }
  ],
  "pagination": {
    "page": "integer",
    "limit": "integer", 
    "total": "integer",
    "pages": "integer"
  },
  "aggregations": {
    "filtered_count": "integer",
    "outlier_count": "integer",
    "quality_stats": {}
  }
}
```

### 4. Individual Sample Detail Endpoint
```http
GET /api/v1/datasets/{dataset_id}/sample/{sample_index}
```

**Response Schema**:
```json
{
  "sample": {
    "index": "integer",
    "features": {
      "raw_data": {},
      "technical_indicators": {},
      "derived_features": {}
    },
    "metadata": {
      "timestamp": "datetime",
      "symbol": "string", 
      "sequence_position": "integer"
    },
    "analysis": {
      "quality_score": "decimal",
      "anomaly_scores": {},
      "feature_importance": {},
      "nearest_neighbors": ["integer"]
    }
  },
  "context": {
    "previous_sample": "integer",
    "next_sample": "integer", 
    "sequence_info": {}
  }
}
```

## Frontend Implementation

### Technology Stack

#### Current Implementation (✅ IMPLEMENTED)
- **Framework**: FastAPI with Python 3.12+ (Backend)
- **Frontend**: Vanilla JavaScript with HTML5/CSS3
- **Tables**: Custom interactive table with JavaScript sorting/filtering
- **Styling**: Professional CSS with shared styling between job and dataset tables
- **State Management**: JavaScript variables for table state (sorting, pagination, filtering)
- **API Integration**: Fetch API with async/await pattern

#### Future Enhancement Roadmap
- **Framework**: React 18+ with TypeScript
- **Charting**: Plotly.js for interactive visualizations
- **Tables**: React Table v8 with virtual scrolling
- **Styling**: Tailwind CSS for responsive design
- **State Management**: React Query for API caching
- **Build Tool**: Vite for fast development

### Component Architecture

#### 1. Interactive Dataset Table Implementation (✅ IMPLEMENTED)

**Current JavaScript Implementation**:
```javascript
// ✅ IMPLEMENTED - Interactive table state management
let currentDatasetSort = { field: 'creation_timestamp', direction: 'desc' };
let currentDatasetPage = 0;
let currentDatasetLimit = 25;
let currentSymbolFilter = '';

// ✅ IMPLEMENTED - Sort functionality
function sortDatasets(field) {
  if (currentDatasetSort.field === field) {
    currentDatasetSort.direction = currentDatasetSort.direction === 'desc' ? 'asc' : 'desc';
  } else {
    currentDatasetSort.field = field;
    currentDatasetSort.direction = 'desc';
  }
  currentDatasetPage = 0;
  loadDatasets();
  updateDatasetSortIndicators();
}

// ✅ IMPLEMENTED - Filter functionality with debouncing
function refreshDatasets() {
  currentSymbolFilter = document.getElementById('symbol-filter').value;
  currentDatasetLimit = parseInt(document.getElementById('dataset-limit-select').value);
  currentDatasetPage = 0;
  loadDatasets();
}

// ✅ IMPLEMENTED - Load datasets with API integration
async function loadDatasets() {
  const params = new URLSearchParams({
    limit: currentDatasetLimit.toString(),
    offset: (currentDatasetPage * currentDatasetLimit).toString(),
    sort_by: currentDatasetSort.field,
    sort_dir: currentDatasetSort.direction
  });
  
  if (currentSymbolFilter) {
    params.set('symbol_filter', currentSymbolFilter);
  }
  
  const datasets = await fetch(`/api/v1/datasets?${params}`).then(r => r.json());
  // Update table with real production data
}
```

**Future React Implementation**:
```tsx
interface DatasetDetailDashboardProps {
  datasetId: string
}

export const DatasetDetailDashboard: React.FC<DatasetDetailDashboardProps> = ({
  datasetId
}) => {
  const [activeTab, setActiveTab] = useState<'overview' | 'distributions' | 'samples' | 'analysis'>('overview')
  const { data: dataset, isLoading } = useDatasetDetails(datasetId)
  
  return (
    <div className="dataset-dashboard">
      <DashboardHeader dataset={dataset} />
      <TabNavigation activeTab={activeTab} onTabChange={setActiveTab} />
      
      {activeTab === 'overview' && <OverviewTab dataset={dataset} />}
      {activeTab === 'distributions' && <DistributionsTab datasetId={datasetId} />}
      {activeTab === 'samples' && <SamplesTab datasetId={datasetId} />}
      {activeTab === 'analysis' && <AnalysisTab datasetId={datasetId} />}
    </div>
  )
}
```

#### 2. FeatureDistributions Component
```tsx
interface FeatureDistributionsProps {
  datasetId: string
  selectedFeatures?: string[]
}

export const FeatureDistributions: React.FC<FeatureDistributionsProps> = ({
  datasetId,
  selectedFeatures = []
}) => {
  const [plotConfig, setPlotConfig] = useState({
    bins: 50,
    plotType: 'histogram',
    showOutliers: true
  })
  
  const { data: distributions } = useFeatureDistributions(datasetId, {
    features: selectedFeatures,
    bins: plotConfig.bins
  })
  
  return (
    <div className="feature-distributions">
      <DistributionControls config={plotConfig} onChange={setPlotConfig} />
      <PlotGrid distributions={distributions} config={plotConfig} />
      <StatisticalSummary distributions={distributions} />
    </div>
  )
}
```

#### 3. SampleDataTable Component
```tsx
interface SampleDataTableProps {
  datasetId: string
  initialFilters?: SampleFilters
}

export const SampleDataTable: React.FC<SampleDataTableProps> = ({
  datasetId,
  initialFilters
}) => {
  const [filters, setFilters] = useState<SampleFilters>(initialFilters || {})
  const [pagination, setPagination] = useState({ page: 1, limit: 100 })
  const [selectedSamples, setSelectedSamples] = useState<number[]>([])
  
  const { data: samplesData, isLoading } = useSampleData(datasetId, {
    ...filters,
    ...pagination
  })
  
  const columns = useMemo(() => createDynamicColumns(samplesData?.samples), [samplesData])
  
  return (
    <div className="sample-data-table">
      <FilterPanel filters={filters} onFiltersChange={setFilters} />
      <VirtualizedTable 
        columns={columns}
        data={samplesData?.samples || []}
        pagination={pagination}
        onPaginationChange={setPagination}
        selectedRows={selectedSamples}
        onRowSelection={setSelectedSamples}
        onRowClick={(sample) => openSampleDetail(sample.index)}
      />
      <TableFooter 
        pagination={samplesData?.pagination}
        aggregations={samplesData?.aggregations}
      />
    </div>
  )
}
```

#### 4. SampleVisualization Component
```tsx
interface SampleVisualizationProps {
  datasetId: string
  sampleIndex: number
  comparisonSamples?: number[]
}

export const SampleVisualization: React.FC<SampleVisualizationProps> = ({
  datasetId,
  sampleIndex,
  comparisonSamples = []
}) => {
  const { data: sample } = useSampleDetail(datasetId, sampleIndex)
  const [viewMode, setViewMode] = useState<'features' | 'timeseries' | 'correlation'>('features')
  
  return (
    <div className="sample-visualization">
      <SampleHeader sample={sample} />
      <ViewModeSelector mode={viewMode} onModeChange={setViewMode} />
      
      {viewMode === 'features' && (
        <FeatureBarChart 
          features={sample?.features}
          comparison={comparisonSamples}
        />
      )}
      
      {viewMode === 'timeseries' && (
        <TimeSeriesChart 
          data={sample?.features}
          metadata={sample?.metadata}
        />
      )}
      
      {viewMode === 'correlation' && (
        <CorrelationHeatmap 
          features={sample?.features}
          analysis={sample?.analysis}
        />
      )}
      
      <SampleNavigation 
        currentIndex={sampleIndex}
        context={sample?.context}
      />
    </div>
  )
}
```

### State Management

#### API Client with React Query
```typescript
// API client configuration
const apiClient = axios.create({
  baseURL: '/api/v1',
  timeout: 30000
})

// React Query hooks
export const useDatasetDetails = (datasetId: string) => {
  return useQuery({
    queryKey: ['dataset', datasetId, 'details'],
    queryFn: () => apiClient.get(`/datasets/${datasetId}/details`).then(res => res.data),
    staleTime: 5 * 60 * 1000, // 5 minutes
    cacheTime: 10 * 60 * 1000 // 10 minutes
  })
}

export const useFeatureDistributions = (datasetId: string, options: DistributionOptions) => {
  return useQuery({
    queryKey: ['dataset', datasetId, 'distributions', options],
    queryFn: () => apiClient.get(`/datasets/${datasetId}/distributions`, { params: options }).then(res => res.data),
    enabled: !!datasetId,
    staleTime: 2 * 60 * 1000
  })
}

export const useSampleData = (datasetId: string, filters: SampleFilters & PaginationOptions) => {
  return useQuery({
    queryKey: ['dataset', datasetId, 'samples', filters],
    queryFn: () => apiClient.get(`/datasets/${datasetId}/samples`, { params: filters }).then(res => res.data),
    enabled: !!datasetId,
    keepPreviousData: true // For smooth pagination
  })
}
```

## Backend Implementation

### Data Processing Pipeline

#### 1. Dataset Analysis Service
```python
class DatasetAnalysisService:
    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.data_loader = DatasetLoader()
        self.statistics_calculator = StatisticsCalculator()
        
    async def analyze_dataset(self) -> DatasetAnalysis:
        # Load dataset from file
        data = await self.data_loader.load_dataset(self.dataset_id)
        
        # Calculate feature statistics
        feature_stats = await self.statistics_calculator.calculate_feature_statistics(data)
        
        # Generate distribution data
        distributions = await self.statistics_calculator.calculate_distributions(data)
        
        # Detect outliers
        outliers = await self.statistics_calculator.detect_outliers(data)
        
        # Cache results in database
        await self.cache_analysis_results(feature_stats, distributions, outliers)
        
        return DatasetAnalysis(
            features=feature_stats,
            distributions=distributions,
            outliers=outliers
        )
```

#### 2. Sample Data Service
```python
class SampleDataService:
    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.cache = RedisCache()
        
    async def get_samples(self, 
                         filters: SampleFilters,
                         pagination: PaginationOptions) -> SamplePage:
        
        # Check cache first
        cache_key = self._generate_cache_key(filters, pagination)
        cached_result = await self.cache.get(cache_key)
        if cached_result:
            return cached_result
        
        # Load and filter data
        data = await self._load_filtered_data(filters)
        
        # Apply pagination
        paginated_data = self._paginate_data(data, pagination)
        
        # Calculate aggregations
        aggregations = self._calculate_aggregations(data)
        
        result = SamplePage(
            samples=paginated_data,
            pagination=self._build_pagination_info(data, pagination),
            aggregations=aggregations
        )
        
        # Cache result
        await self.cache.set(cache_key, result, ttl=300)  # 5 minute cache
        
        return result
        
    async def _load_filtered_data(self, filters: SampleFilters) -> np.ndarray:
        # Efficient filtering using numpy operations
        data = await self.data_loader.load_dataset(self.dataset_id)
        
        # Apply numerical filters
        if filters.feature_ranges:
            for feature, range_filter in filters.feature_ranges.items():
                feature_idx = self._get_feature_index(feature)
                if range_filter.get('min') is not None:
                    data = data[data[:, feature_idx] >= range_filter['min']]
                if range_filter.get('max') is not None:
                    data = data[data[:, feature_idx] <= range_filter['max']]
        
        # Apply date filters
        if filters.date_range:
            data = self._filter_by_date_range(data, filters.date_range)
        
        return data
```

### Performance Optimizations

#### 1. Caching Strategy
```python
class DatasetCache:
    def __init__(self):
        self.redis_client = redis.Redis()
        self.memory_cache = {}
        
    async def get_feature_distributions(self, dataset_id: str, features: List[str]) -> Dict:
        cache_key = f"distributions:{dataset_id}:{hash(tuple(features))}"
        
        # Try memory cache first
        if cache_key in self.memory_cache:
            return self.memory_cache[cache_key]
        
        # Try Redis cache
        cached_data = await self.redis_client.get(cache_key)
        if cached_data:
            result = json.loads(cached_data)
            self.memory_cache[cache_key] = result  # Store in memory for faster access
            return result
        
        return None
    
    async def set_feature_distributions(self, dataset_id: str, features: List[str], data: Dict):
        cache_key = f"distributions:{dataset_id}:{hash(tuple(features))}"
        
        # Store in Redis with expiration
        await self.redis_client.setex(
            cache_key, 
            3600,  # 1 hour expiration
            json.dumps(data, default=str)
        )
        
        # Store in memory cache
        self.memory_cache[cache_key] = data
```

#### 2. Efficient Data Loading
```python
class OptimizedDataLoader:
    def __init__(self):
        self.file_cache = {}
        self.chunk_size = 10000
        
    async def load_dataset_chunk(self, 
                                dataset_id: str, 
                                start_idx: int = 0, 
                                end_idx: Optional[int] = None) -> np.ndarray:
        """Load only a portion of the dataset for memory efficiency"""
        
        dataset_info = await self._get_dataset_info(dataset_id)
        file_path = dataset_info['file_path']
        
        # Use memory mapping for large files
        if dataset_info['file_size'] > 100 * 1024 * 1024:  # 100MB
            data = np.load(file_path, mmap_mode='r')
            return data[start_idx:end_idx] if end_idx else data[start_idx:]
        else:
            # Load full file and cache in memory
            if file_path not in self.file_cache:
                self.file_cache[file_path] = np.load(file_path)
            
            data = self.file_cache[file_path]
            return data[start_idx:end_idx] if end_idx else data[start_idx:]
```

## Testing Strategy

### Unit Tests
```python
class TestDatasetVisualizationAPI:
    @pytest.fixture
    async def sample_dataset(self):
        # Create sample dataset for testing
        data = np.random.randn(1000, 10)  # 1000 samples, 10 features
        dataset_id = "test_dataset_001"
        
        # Save to temporary file
        file_path = f"/tmp/{dataset_id}.npy"
        np.save(file_path, data)
        
        # Register in database
        await self.register_test_dataset(dataset_id, file_path)
        
        yield dataset_id
        
        # Cleanup
        os.remove(file_path)
        await self.cleanup_test_dataset(dataset_id)
    
    async def test_dataset_details_endpoint(self, sample_dataset):
        response = await self.client.get(f"/api/v1/datasets/{sample_dataset}/details")
        assert response.status_code == 200
        
        data = response.json()
        assert "metadata" in data
        assert "statistics" in data
        assert "features" in data
        assert data["metadata"]["total_sequences"] == 1000
    
    async def test_feature_distributions_endpoint(self, sample_dataset):
        response = await self.client.get(
            f"/api/v1/datasets/{sample_dataset}/distributions",
            params={"features": "feature_0,feature_1", "bins": 20}
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "distributions" in data
        assert "feature_0" in data["distributions"]
        assert len(data["distributions"]["feature_0"]["histogram"]["bins"]) == 20
    
    async def test_sample_data_filtering(self, sample_dataset):
        # Test numerical range filtering
        filters = {
            "feature_ranges": {
                "feature_0": {"min": -1, "max": 1}
            }
        }
        
        response = await self.client.get(
            f"/api/v1/datasets/{sample_dataset}/samples",
            params={"filter": json.dumps(filters)}
        )
        assert response.status_code == 200
        
        data = response.json()
        samples = data["samples"]
        
        # Verify all samples meet filter criteria
        for sample in samples:
            assert -1 <= sample["data"]["feature_0"] <= 1
```

### Integration Tests
```python
class TestDatasetVisualizationIntegration:
    async def test_end_to_end_dataset_exploration(self):
        """Test complete workflow from dataset creation to visualization"""
        
        # 1. Create dataset
        dataset_data = await self.create_test_dataset()
        dataset_id = dataset_data["dataset_id"]
        
        # 2. Wait for analysis to complete
        await self.wait_for_analysis_completion(dataset_id)
        
        # 3. Test dataset details
        details = await self.get_dataset_details(dataset_id)
        assert details["statistics"]["data_quality_score"] > 0
        
        # 4. Test feature distributions
        distributions = await self.get_feature_distributions(dataset_id)
        assert len(distributions["distributions"]) > 0
        
        # 5. Test sample data with filtering
        samples = await self.get_filtered_samples(dataset_id, {
            "limit": 50,
            "filter": {"quality_threshold": 0.8}
        })
        assert len(samples["samples"]) <= 50
        
        # 6. Test individual sample visualization
        sample_detail = await self.get_sample_detail(dataset_id, 0)
        assert "features" in sample_detail["sample"]
        assert "analysis" in sample_detail["sample"]
```

### Frontend Tests
```typescript
describe('DatasetDetailDashboard', () => {
  it('renders dataset overview correctly', async () => {
    const mockDataset = createMockDataset()
    
    render(
      <QueryClientProvider client={queryClient}>
        <DatasetDetailDashboard datasetId="test_dataset" />
      </QueryClientProvider>
    )
    
    // Wait for data to load
    await waitFor(() => {
      expect(screen.getByText(mockDataset.metadata.name)).toBeInTheDocument()
      expect(screen.getByText(/Total Sequences:/)).toBeInTheDocument()
      expect(screen.getByText(/Data Quality:/)).toBeInTheDocument()
    })
  })
  
  it('switches between tabs correctly', async () => {
    render(<DatasetDetailDashboard datasetId="test_dataset" />)
    
    // Click distributions tab
    fireEvent.click(screen.getByText('Distributions'))
    await waitFor(() => {
      expect(screen.getByTestId('feature-distributions')).toBeInTheDocument()
    })
    
    // Click samples tab
    fireEvent.click(screen.getByText('Samples'))
    await waitFor(() => {
      expect(screen.getByTestId('sample-data-table')).toBeInTheDocument()
    })
  })
})

describe('SampleDataTable', () => {
  it('applies filters correctly', async () => {
    const mockSamples = createMockSamples()
    
    render(<SampleDataTable datasetId="test_dataset" />)
    
    // Apply numerical filter
    const filterInput = screen.getByLabelText(/Open Price Range/)
    fireEvent.change(filterInput, { target: { value: '100-200' } })
    
    // Verify filtered results
    await waitFor(() => {
      const visibleRows = screen.getAllByTestId('sample-row')
      expect(visibleRows).toHaveLength(mockSamples.filteredCount)
    })
  })
})
```

## Deployment Configuration

### Enhanced Kubernetes Configuration (✅ DEPLOYED)

**Current Production Deployment**:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: job-management-fixed-config  # ✅ DEPLOYED
  namespace: ats-dev
data:
  unified_analytics_fixed.py: |
    # ✅ IMPLEMENTED - Enhanced analytics platform with interactive dataset table
    # Real implementation with:
    # - Enhanced list_datasets() method with filtering/sorting
    # - Interactive table HTML with JavaScript functionality
    # - Professional CSS styling shared with job management
    # - Real production data integration
  
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: enhanced-analytics-webapp
  namespace: ats-dev
spec:
  replicas: 2  # Increased for better performance
  selector:
    matchLabels:
      app: enhanced-analytics-webapp
  template:
    metadata:
      labels:
        app: enhanced-analytics-webapp
    spec:
      containers:
      - name: webapp
        image: python:3.12-slim
        ports:
        - containerPort: 5000
        env:
        - name: DB_HOST
          valueFrom:
            secretKeyRef:
              name: db-credentials-dev
              key: DB_HOST
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: db-credentials-dev
              key: DB_PASSWORD
        # Resource limits for visualization workloads
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        volumeMounts:
        - name: app-code
          mountPath: /app
      volumes:
      - name: app-code
        configMap:
          name: dataset-visualization-config
```

## Performance Requirements

### Response Time Targets
- **Dataset Details**: <2 seconds
- **Feature Distributions**: <3 seconds  
- **Sample Data (100 rows)**: <1 second
- **Individual Sample**: <500ms
- **Filter Operations**: <1 second

### Scalability Targets
- **Concurrent Users**: 50+ simultaneous users
- **Dataset Size**: Up to 1GB .npy files
- **Sample Count**: Up to 1M samples with pagination
- **Feature Count**: Up to 100 features per dataset

### Resource Requirements
- **Memory**: 2GB per webapp instance
- **CPU**: 1 core per webapp instance
- **Storage**: 10GB for cache and temporary files
- **Network**: 100Mbps for data transfer

---

**Document Version**: 2.0  
**Last Updated**: August 22, 2025  
**Next Review**: September 1, 2025

---

## ✅ Implementation Status (August 22, 2025)

### Interactive Dataset Table - COMPLETED

**Technical Achievements**:
- ✅ **Enhanced API**: `/api/v1/datasets` with filtering, sorting, pagination parameters
- ✅ **Interactive Frontend**: JavaScript-based table with sortable columns and real-time filtering
- ✅ **Professional Styling**: Consistent design with job management table
- ✅ **Real Data Integration**: Connected to `dev_training_dataset` table with production data
- ✅ **Regression Protection**: Comprehensive test suite with 16 test cases
- ✅ **Production Deployment**: Live at http://172.25.223.121:3000/

**Database Integration**:
- ✅ **Real Datasets Displayed**: `enhanced_20250821_145109_tsla`, `aapl_demo_dataset`
- ✅ **Metadata Fields**: dataset_name, symbols, total_sequences, feature_count, technical_indicators, created_at, file_size_mb
- ✅ **Query Performance**: Optimized SQL with proper indexing and pagination

**User Experience**:
- ✅ **Interactive Sorting**: Click column headers to sort by any field
- ✅ **Real-time Filtering**: Type-ahead search with debounced API calls
- ✅ **Professional Pagination**: Configurable row limits with smart page controls
- ✅ **Visual Feedback**: Sort indicators, hover effects, loading states
- ✅ **Consistent Design**: Matches job management table styling

**Quality Assurance**:
- ✅ **Regression Protection**: `test_dataset_table_regression_protection.py` - 16 test cases
- ✅ **Integration Tests**: `tests/integration/test_interactive_dataset_table_functionality.py`
- ✅ **Pre-deployment Script**: `scripts/test_dataset_table_before_deploy.sh`
- ✅ **Documentation**: `docs/DATASET_TABLE_REGRESSION_PROTECTION.md`

**API Endpoints Implemented**:
```bash
# ✅ Enhanced dataset listing with all parameters
GET /api/v1/datasets?limit=50&offset=0&symbol_filter=tsla&sort_by=dataset_name&sort_dir=asc

# ✅ Filter options for UI
GET /api/v1/datasets/filter

# ✅ Existing endpoints preserved
GET /api/v1/datasets/{id}/distributions
GET /api/v1/datasets/{id}/ohlc
```

This implementation serves as the foundation for the complete dataset visualization system outlined in this DRD.