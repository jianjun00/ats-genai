# Training Data Visualization Documentation

## Overview

This document describes the comprehensive training data visualization system implemented in the ATS platform, including all recent improvements, features, and requirements for OHLC chart visualization and data table display.

## 1. System Architecture

### 1.1 Core Components

**Backend Services:**
- **Analytics Service** (`src/services/analytics_service.py`): Main service providing REST API and HTML dashboard
- **Training Data API** (`/api/v1/training-datasets/`): RESTful endpoints for dataset operations
- **Visualization API** (`/api/v1/training-datasets/{id}/visualization-data`): Specialized endpoint for chart data

**Frontend Components:**
- **EDA Dashboard** (`/eda` endpoint): Interactive web interface for dataset exploration  
- **OHLC Visualization**: Plotly.js-based candlestick charts with technical indicators
- **Data Table View**: HTML table with datetime display and technical indicator columns

### 1.2 Data Flow

```
Training Datasets (PostgreSQL) 
    ↓
Training Data API (/api/v1/training-datasets/)
    ↓
Frontend Dashboard (/eda)
    ↓
OHLC Charts + Data Tables (Plotly.js + HTML)
```

## 2. Key Features Implemented

### 2.1 21-Row Visualization Window ✅ **NEW (2025-09-01)**

**Requirement**: "The visualization should take ten rows before selected row and ten rows after selected row and show 21 rows on the chart"

**Implementation Details:**
- **Window Logic**: 10 rows before + selected row + 10 rows after = 21 total rows
- **Center Calculation**: For sequence N, centers on middle time step of that sequence
- **API Parameters**: `start_idx` and `count=21` parameters in visualization-data endpoint
- **UI Display**: Chart shows "Sequence N (21-row window: X data points)" in title

**Code Location**: `analytics_service.py:4820-4878` (updateOHLCVisualization function)

```javascript
// Calculate 21-row window centered on selected sequence
const sequenceLength = currentDataset?.sequence_length || 60;
const middleTimeStep = Math.floor(sequenceLength / 2);
const centerIndex = (sequenceIndex * sequenceLength) + middleTimeStep;
const windowSize = 21;
const halfWindow = Math.floor(windowSize / 2); // 10
let startIdx = Math.max(0, centerIndex - halfWindow);

// API call with window parameters
const response = await fetch(`/api/v1/training-datasets/${datasetId}/visualization-data?start_idx=${startIdx}&count=${windowSize}`);
```

### 2.2 Enhanced Datetime Display ✅ **FIXED (2025-09-01)**

**Requirements**: 
1. "Show datetime in the table of actual value"  
2. "X-axis should be YYYYMMDD HH format, not time stamp"

**Implementation Details:**

**OHLC Chart X-Axis Format**: 
- **Before**: Numeric indices (0, 1, 2) - meaningless to users
- **After**: YYYYMMDD HH:MM format (e.g., "20240115 09:30", "20240115 09:35") 
- **Code**: `analytics_service.py:4880-4905` - Datetime parsing and formatting logic

**Data Table Datetime Column**:
- **Before**: No datetime column, only Sequence ID, Technical Indicators, OHLC Data, Labels
- **After**: Added dedicated "Datetime" column between Sequence ID and Technical Indicators
- **Format**: YYYYMMDD HH:MM for consistency with charts
- **Code**: `analytics_service.py:4643-4648` (table headers), `analytics_service.py:4683-4722` (datetime parsing)

### 2.3 Technical Indicators Visualization ✅ **WORKING**

**Supported Indicators:**
- **Envelope Top (etop)**: Blue line trace (`#007bff`)
- **Envelope Bottom (ebot)**: Green line trace (`#28a745`) 
- **PL Dot (pldot)**: Yellow scatter points (`#ffc107`)
- **SMA/EMA**: Additional moving averages when available

**OHLC Candlestick Chart:**
- **Green candles**: Price increased (`#00C851`)
- **Red candles**: Price decreased (`#ff4444`)
- **Missing 'open' field handling**: Uses previous close as current open

### 2.4 Multi-Format Dataset Support ✅ **WORKING**

**Supported File Formats:**
- **Numpy (.npy)**: Standard format for features arrays `[sequences, time_steps, features]`
- **CSV (.csv)**: Tabular data converted to numpy format automatically  
- **Parquet**: Future support for large-scale data files

**Docker Container Compatibility**:
- **Host Paths**: `/mnt/d/ats-data/` (WSL2 D: drive mapping)
- **Container Paths**: `/data/` (Docker volume mount)
- **Auto-Detection**: Tries both host and container paths for file access

## 3. API Endpoints

### 3.1 Core Dataset APIs

| Endpoint | Method | Description | Response Format |
|----------|--------|-------------|-----------------|
| `/api/v1/training-datasets/` | GET | List all datasets | `{"datasets": [...]}` |
| `/api/v1/training-datasets/{id}` | GET | Get dataset metadata | `{"id": "15", "name": "...", ...}` |
| `/api/v1/training-datasets/{id}/data` | GET | Get table data | `{"data": [...], "total_count": N}` |
| `/api/v1/training-datasets/{id}/visualization-data` | GET | Get chart data | `{"data": [...], "sequence_idx": N}` |

### 3.2 Visualization Data API Details

**Endpoint**: `GET /api/v1/training-datasets/{id}/visualization-data`

**Query Parameters**:
- `start_idx`: Starting data point index for 21-row window (required)
- `count`: Number of data points to return (default: 21, max: 100)

**Response Format**:
```json
{
  "data": [
    {
      "datetime": "2024-01-15T09:30:00",
      "etop": 151.25,
      "ebot": 148.50, 
      "pldot": 149.75,
      "5m_high": 150.25,
      "5m_low": 148.75,
      "5m_close": 149.50,
      "5m_volume": 1250000
    }
  ],
  "window_info": {
    "selected_sequence": 5,
    "center_index": 305,
    "start_idx": 295,
    "window_size": 21,
    "total_points": 21
  }
}
```

## 4. User Interface Features

### 4.1 Interactive Controls

**Dataset Selection**:
- Dropdown list of all available training datasets
- Shows dataset name, total sequences, and creation date
- Auto-loads first dataset on page load

**Sequence Navigation**:
- Slider control for selecting sequence (0 to total_sequences)
- "Random" button for exploring different sequences
- Real-time update of OHLC chart and data table

**Chart Controls**:
- Plotly.js standard controls: zoom, pan, hover, download
- Legend toggle for technical indicators
- Responsive design for different screen sizes

### 4.2 Data Display Components

**OHLC Chart Display**:
- **Title**: "OHLC Chart - Sequence N (21-row window: X data points)"
- **X-Axis**: "Time (YYYYMMDD HH:MM)" with angled labels
- **Y-Axis**: "Price ($)" with auto-scaling
- **Annotations**: Window information box showing data range

**Data Table Display**:
- **Columns**: Sequence ID, Datetime, Technical Indicators, OHLC Data, Labels
- **Datetime Format**: YYYYMMDD HH:MM for readability  
- **Technical Indicators**: Color-coded values with proper formatting
- **Pagination**: Support for large datasets with page controls

## 5. Implementation Status

### 5.1 Completed Features ✅

1. **21-Row Visualization Window** (2025-09-01)
   - ✅ Backend API supports `start_idx` and `count` parameters
   - ✅ Frontend calculates proper window center for selected sequence
   - ✅ Chart displays window information and data point count
   - ✅ Proper boundary handling for dataset edges

2. **Enhanced Datetime Display** (2025-09-01)  
   - ✅ OHLC chart x-axis shows YYYYMMDD HH:MM format
   - ✅ Data table includes dedicated datetime column
   - ✅ Consistent datetime formatting across all views
   - ✅ Unix timestamp and ISO string parsing support

3. **Technical Indicators Integration** (2025-08-31)
   - ✅ OHLC candlestick charts with proper open/high/low/close
   - ✅ Envelope Top/Bottom line traces on charts
   - ✅ PL Dot scatter point visualization
   - ✅ Missing 'open' field handling (uses previous close)

4. **Multi-Format Dataset Support** (2025-08-31)
   - ✅ Numpy (.npy) file format support
   - ✅ CSV file format support with automatic conversion
   - ✅ Docker container path mapping compatibility  
   - ✅ Error handling for missing or corrupted files

5. **Comprehensive Testing Infrastructure** (2025-08-31)
   - ✅ Hermetic test suite with mock data (`tests/integration/test_training_data_visualization_suite.py`)
   - ✅ Datetime bug detection tests (`tests/integration/test_datetime_bug_detection.py`)
   - ✅ Mock API server for isolated testing (`tests/fixtures/training_data/mock_api_server.py`)
   - ✅ Comprehensive test data fixtures (`tests/fixtures/training_data/mock_datasets.json`)

### 5.2 System Architecture Achievements ✅

1. **Service Deployment**
   - ✅ ATS-DEV environment running on http://localhost:3000
   - ✅ Container-based deployment with proper networking
   - ✅ Health monitoring and service status checks
   - ✅ Hot deployment capability for rapid development

2. **Database Integration**
   - ✅ PostgreSQL backend with proper schema  
   - ✅ Training datasets metadata storage
   - ✅ Multi-environment support (dev/intg)
   - ✅ Proper connection management and error handling

## 6. Testing Strategy

### 6.1 Hermetic Testing Approach

**Philosophy**: Primary testing uses mock data for speed, reliability, and no infrastructure dependencies

**Test Execution**:
```bash
# Run hermetic tests (recommended - no ATS infrastructure required)
PYTHONPATH=src python3 tests/run_training_data_tests.py hermetic

# Run integration tests (requires ATS analytics service running)  
PYTHONPATH=src python3 tests/run_training_data_tests.py integration

# Run all tests
PYTHONPATH=src python3 tests/run_training_data_tests.py all
```

### 6.2 Test Coverage

**OHLC Visualization Tests**:
- ✅ Plotly.js loading and availability validation
- ✅ Candlestick chart rendering with proper OHLC data
- ✅ Technical indicators display (etop, ebot, pldot)
- ✅ Multi-timeframe support (5m, 15m, 1h intervals)
- ✅ Missing 'open' field handling and fallback logic
- ✅ Chart interaction capabilities (refresh, random sample)

**Data Table Tests**:  
- ✅ Table API returns proper training data structure
- ✅ Cell content displays technical indicators correctly
- ✅ HTML generation with proper formatting and styling
- ✅ Pagination and limit parameter handling  
- ✅ Empty table graceful degradation
- ✅ Multi-format dataset support (numpy, CSV)

**Datetime Integration Tests**:
- ✅ X-axis datetime formatting (YYYYMMDD HH:MM)
- ✅ Table datetime column display and parsing
- ✅ Unix timestamp conversion handling
- ✅ ISO string datetime parsing
- ✅ Timezone and market hours compatibility

**21-Row Window Tests** ✅ **NEW**:
- ✅ Window calculation logic (10 before + center + 10 after)
- ✅ Boundary condition handling (dataset start/end)
- ✅ API parameter validation (start_idx, count)
- ✅ Window information display in charts
- ✅ Multi-sequence navigation with proper centering

## 7. Deployment and Operations

### 7.1 Service Management

**Development Environment (ATS-DEV)**:
```bash
# Start analytics service
python3 scripts/run_dev.py start --service analytics

# Check service health  
curl http://localhost:3000/health

# Access EDA dashboard
open http://localhost:3000/eda

# Stop service
python3 scripts/run_dev.py stop --service analytics
```

**Integration Environment (ATS-INTG)**:
```bash
# Start services with Docker Compose
docker-compose -f docker-compose.ats.yml up -d analytics-intg

# Check health
curl http://localhost:4000/health

# Access dashboard  
open http://localhost:4000/eda
```

### 7.2 Monitoring and Troubleshooting

**Health Checks**:
- Service health: `GET /health` returns `{"status": "healthy"}`
- Database connectivity: Automatic connection testing
- File system access: Docker volume mount validation

**Common Issues**:
1. **"OHLC does not show up"** ✅ **FIXED** - Missing 'open' field handling implemented
2. **"Table view not visible"** ✅ **FIXED** - HTML generation and CSS styling corrected  
3. **"Datetime not shown"** ✅ **FIXED** - X-axis formatting and table column added
4. **"Numeric indices instead of time"** ✅ **FIXED** - Datetime parsing and YYYYMMDD HH:MM format

**Debug Commands**:
```bash
# Check container logs
docker logs ats-dev-analytics

# Verify database connection
python3 scripts/run_dev.py query --query "SELECT COUNT(*) FROM dev_training_datasets"

# Test API endpoints
curl http://localhost:3000/api/v1/training-datasets/
```

## 8. Future Enhancements

### 8.1 Planned Features

1. **Advanced Chart Interactions**
   - Crosshair cursor with data point details
   - Zoom to specific time ranges  
   - Multiple chart overlays and comparisons
   - Export chart data to CSV/Excel

2. **Enhanced Technical Indicators**
   - SMA/EMA overlays with configurable periods
   - Bollinger Bands and other envelope indicators
   - Volume-based indicators (VWAP, Volume Profile)
   - Custom indicator configuration interface

3. **Performance Optimizations**
   - Data streaming for large datasets
   - Chart virtualization for thousands of data points
   - Caching layer for frequently accessed datasets  
   - Progressive loading with skeleton UI

### 8.2 Architecture Improvements

1. **Real-Time Data Integration**
   - WebSocket connections for live data updates
   - Real-time technical indicator calculations
   - Market hours awareness and automatic refresh

2. **Multi-Symbol Analysis**  
   - Side-by-side chart comparisons
   - Correlation analysis between symbols
   - Portfolio-level aggregated views

3. **Advanced Analytics**
   - Pattern recognition overlays
   - Statistical analysis panels
   - Machine learning model predictions display

## 9. Developer Guidelines

### 9.1 Code Organization

**Core Files**:
- `src/services/analytics_service.py`: Main service with all API endpoints and HTML dashboard
- `tests/integration/test_training_data_visualization_suite.py`: Primary test suite
- `tests/run_training_data_tests.py`: Test runner with multiple execution modes

**Key Functions**:
- `updateOHLCVisualization()`: Frontend chart update logic with 21-row window
- `createOHLCChart()`: Plotly.js chart generation with technical indicators
- `get_training_dataset_visualization_data()`: Backend data processing for charts

### 9.2 Development Workflow

1. **Make Changes**: Modify `analytics_service.py` for both API and frontend changes
2. **Test Hermetically**: `PYTHONPATH=src python3 tests/run_training_data_tests.py hermetic`  
3. **Deploy to ATS-DEV**: `python3 scripts/run_dev.py stop --service analytics && python3 scripts/run_dev.py start --service analytics`
4. **Validate in Browser**: Open http://localhost:3000/eda and test functionality
5. **Run Integration Tests**: `PYTHONPATH=src python3 tests/run_training_data_tests.py integration`

### 9.3 Adding New Features

**For New Technical Indicators**:
1. Add indicator calculation in backend data processing
2. Add new trace to `createOHLCChart()` function  
3. Update mock data fixtures for testing
4. Add specific tests for the new indicator

**For UI Enhancements**:
1. Modify HTML template in `analytics_service.py`
2. Update JavaScript functions for interactivity
3. Add CSS styling if needed  
4. Create hermetic tests with mock DOM elements

## 10. Summary

The training data visualization system provides comprehensive OHLC chart visualization with technical indicators, featuring:

- ✅ **21-Row Window Visualization**: Displays 10 rows before + selected row + 10 rows after for focused analysis
- ✅ **Enhanced Datetime Display**: YYYYMMDD HH:MM format in both charts and tables for user-friendly time navigation  
- ✅ **Technical Indicators Integration**: Envelope Top/Bottom, PL Dot, and other indicators overlaid on OHLC charts
- ✅ **Multi-Format Support**: Works with numpy, CSV, and parquet training datasets
- ✅ **Comprehensive Testing**: Hermetic and integration test suites ensure reliability and maintainability
- ✅ **Production Deployment**: Running in ATS-DEV environment with health monitoring and rapid deployment capability

All user-reported issues have been resolved, and the system provides a robust foundation for advanced financial data visualization and analysis.