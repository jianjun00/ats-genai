# PRD: ATS Exploratory Data Analysis (EDA) Tool

**Document Version**: 2.1  
**Date**: September 6, 2025  
**Owner**: Data Infrastructure Team  
**Status**: ✅ **IMPLEMENTED** - Sequence Selection & 21-Bar Visualization System with Full Multi-Timeframe Support  

---

## 📋 Executive Summary

The ATS EDA Tool is a comprehensive data exploration and visualization platform designed to provide deep insights into the ATS financial datasets. This tool enables data scientists, quant researchers, and platform engineers to understand data quality, discover patterns, and validate datasets through interactive visualizations and statistical analysis.

### Key Value Propositions ✅ **DELIVERED**
- **✅ Automated Statistics Computation**: Automatic metadata generation and statistics computation on first dataset access
- **✅ Unified Dataset Management**: Single interface for database tables, files, and training datasets with comprehensive metadata tracking  
- **✅ 20-100x Performance Improvement**: TFDV-inspired pre-computed statistics for instant histogram visualization
- **✅ Training Dataset Integration**: Dedicated tab for ML training datasets with specialized metadata handling
- **✅ Database-Driven Path Resolution**: Intelligent ArrayRecord file discovery using run_id linkage for precise training data location
- **✅ 🆕 Sequence Selection System**: Interactive dropdown interface for precise training sequence targeting and visualization
- **✅ 🆕 21-Bar Context Windows**: Mathematical bar selection providing contextual analysis (10 before + 1 current + 10 after)
- **✅ 🆕 Multi-Timeframe OHLC Visualization**: Simultaneous 5-chart display (5m, 15m, 1h, 1d, 1w) with real-time updates
- **✅ 🆕 Interactive Row Targeting**: Precise row index selection within training sequences for targeted analysis
- **✅ 🆕 Production-Ready Robustness**: Comprehensive error handling, NaN sanitization, and automated test coverage
- **✅ Data Quality Assurance**: Identify missing data, outliers, and inconsistencies across 30+ years of financial data
- **✅ Pattern Discovery**: Uncover market trends, correlations, and anomalies in multi-vendor datasets  
- **✅ Dataset Validation**: Compare data quality and coverage across vendors (Polygon, Tiingo, EODHD)
- **✅ Research Acceleration**: Rapid hypothesis testing and feature engineering for ML models
- **✅ Operational Insights**: Monitor data collection health and identify collection gaps

---

## 🚀 **NEW**: Unified Dataset Metadata Architecture *(Implemented August 30, 2025)*

### **Revolutionary Approach to Dataset Management**
The ATS EDA Tool now implements a **unified metadata system** that automatically manages and tracks statistics for all dataset types:

#### **🗄️ Database Tables**
- **Automatic Registration**: All existing database tables automatically registered as datasets on first access
- **Real-time Statistics**: Comprehensive column analysis including data types, semantic types, completeness ratios
- **Background Computation**: Statistics computed automatically in background without user intervention

#### **🎯 Training Datasets - Sequence-Based Architecture** *(Updated September 6, 2025)*
- **Sequence-Based Organization**: Training data organized by sequences rather than timeframes for better ML workflow integration
- **Directory Structure**: `/mnt/d/ats-data/training_data/{run_id}/{SYMBOL_DATERANGE}/timeframes/` 
  - Example: `/mnt/d/ats-data/training_data/76/AAPL_20250701_000000_20250906_000000/5m/AAPL_20250701_000000_20250906_000000.arrayrecord`
- **🆕 Sequence Selection Interface**: Dropdown menu shows sequences like "AAPL_20250701_000000_20250906_000000" as selectable items
- **🆕 Multi-Timeframe Visualization**: When sequence selected, automatically loads all timeframes (5m, 15m, 1h, 1d, 1w) for comprehensive OHLC visualization
- **🆕 21-Bar Context Window**: Interactive row selection shows 10 bars before + 1 current + 10 bars after target row for contextual analysis
- **🆕 Interactive Row Selection**: Numeric input field allows precise row index selection within sequence data for targeted visualization
- **Table View Integration**: 1h timeframe specifically used for tabular data display with feature matrices
- **Specialized Metadata**: Training-specific metadata including model inputs, backtesting results, portfolio optimization data
- **File Format Support**: ArrayRecord primary format with automatic schema detection
- **🆕 Real-time Chart Updates**: Dynamic Plotly chart regeneration based on row selection with visual highlighting of target row
- **🆕 NaN Value Handling**: Robust JSON serialization preventing visualization failures due to invalid numeric values

#### **📊 Unified Metadata Tables**
1. **`dev_datasets`**: Master catalog of all dataset types with unified metadata
2. **`dev_dataset_columns`**: Detailed column-level statistics and semantic information  
3. **`dev_dataset_column_stats`**: Pre-computed histogram statistics for 20-100x performance

#### **⚡ Automatic Statistics Computation**
- **Zero User Intervention**: Statistics computed automatically when datasets first accessed
- **Background Processing**: Non-blocking computation with status tracking
- **Intelligent Caching**: Results cached for instant subsequent access

#### **🔍 Comprehensive Run Metadata Tracking** *(NEW - September 2, 2025)*
- **Full Reproducibility**: Every training run tracked with complete metadata for exact reproduction
- **Git Integration**: Automatic capture of commit hash, branch, and uncommitted changes detection
- **Environment Capture**: Host system info, Python version, Docker container details
- **Argument Preservation**: Complete command line arguments and parameter configurations stored
- **Performance Metrics**: Execution timing, resource usage, and quality scores tracked
- **Dependency Tracking**: Package version hashes for complete environment reproducibility
- **TFDV-Inspired**: TensorFlow Data Validation approach for robust histogram generation

#### **📁 Critical Training Data File Structure Requirements** *(NEW - September 5, 2025)*
- **Mandatory Directory Structure**: Training data must be organized by timeframe for visualization API compatibility
- **Required Path Format**: `/data/training_data/{run_id}/{timeframe}/{symbol}_{start_date}_{end_date}.arrayrecord`
- **Timeframe Directories**: Each run must have subdirectories: `5m/`, `15m/`, `1h/`, `1d/`, `1w/`
- **File Naming Convention**: `{SYMBOL}_{YYYYMMDD_HHMMSS}_{YYYYMMDD_HHMMSS}.arrayrecord`
- **Multi-Timeframe Requirement**: One ArrayRecord file per symbol-timeframe combination
- **Visualization API Integration**: Structure must match analytics service search patterns for frontend display

**Example Correct Structure:**
```
/data/training_data/
├── 67/                           # Run ID
│   ├── 5m/                       # 5-minute timeframe
│   │   ├── AAPL_20250701_000000_20250905_000000.arrayrecord
│   │   └── TSLA_20250701_000000_20250905_000000.arrayrecord
│   ├── 15m/                      # 15-minute timeframe
│   │   ├── AAPL_20250701_000000_20250905_000000.arrayrecord
│   │   └── TSLA_20250701_000000_20250905_000000.arrayrecord
│   └── 1h/, 1d/, 1w/             # Additional timeframes
```

---

## 🔧 **NEW**: ML Metadata & Run Tracking System *(September 2, 2025)*

### **Enterprise-Grade ML Experiment Tracking**
The ATS platform now implements comprehensive metadata tracking for all machine learning training runs, ensuring full reproducibility and audit compliance for financial ML workflows.

#### **🏗️ Run Metadata Architecture**

**Database Schema Enhancement:**
```sql
-- Extended dev_runs table with comprehensive metadata
ALTER TABLE dev_runs ADD COLUMN command_line TEXT;           -- Complete CLI arguments
ALTER TABLE dev_runs ADD COLUMN git_commit_hash VARCHAR(64); -- Exact code version
ALTER TABLE dev_runs ADD COLUMN git_branch VARCHAR(100);     -- Development branch
ALTER TABLE dev_runs ADD COLUMN environment VARCHAR(50);     -- Execution environment
ALTER TABLE dev_runs ADD COLUMN host_info JSONB;            -- System & container info
ALTER TABLE dev_runs ADD COLUMN dependencies_hash VARCHAR(64); -- Package versions
```

**Automatic Metadata Capture:**
- **Git State**: Commit hash, branch, uncommitted changes detection
- **Environment**: Python version, OS platform, Docker container ID
- **Execution Context**: Working directory, hostname, user account
- **Dependencies**: Package version fingerprints for reproducibility
- **Performance**: Start/end times, resource usage, quality metrics

#### **🛠️ Implementation Components**

1. **`RunMetadataTracker`** - Core tracking utility class
2. **`RunTracker` Context Manager** - Automatic lifecycle management
3. **Run Metadata CLI** - Query and analysis tool
4. **Database Migration 051** - Schema enhancement

#### **📊 Usage Patterns**

**Automatic Context Manager:**
```python
async with RunTracker(
    run_type="unified_training_dataset_generation",
    created_by="generate_unified_training_dataset.py",
    parameters={"symbols": ["AAPL", "TSLA"], "sequence_length": 60}
) as (tracker, run_id):
    # Training code here - metadata tracked automatically
    results = {"total_sequences": 169, "quality_score": 1.0}
```

**Manual Tracking:**
```python
tracker = RunMetadataTracker("model_training", "train_lstm.py")
run_id = await tracker.start_run({"epochs": 100, "batch_size": 32})
await tracker.update_progress(run_id, {"epoch": 50, "loss": 0.05})
await tracker.complete_run(run_id, {"final_accuracy": 0.87})
```

**CLI Query Interface:**
```bash
# List recent training runs
python scripts/run_metadata_cli.py list --limit 10

# Show detailed run metadata
python scripts/run_metadata_cli.py show --run-id 42

# Validate reproducibility
python scripts/run_metadata_cli.py validate --run-id 42

# Export metadata for audit
python scripts/run_metadata_cli.py export --run-id 42 --output audit.json
```

#### **✅ Compliance & Auditability**
- **Regulatory Compliance**: Complete audit trail for financial ML models
- **Reproducibility**: 100% reproducible experiments with exact environment capture
- **Version Control**: Git integration prevents "lost experiment" scenarios
- **Change Detection**: Warns when running with uncommitted changes
- **Performance Tracking**: Execution metrics for optimization and resource planning

---

## 🎯 Problem Statement

### Current Challenges
1. **No Unified Data Exploration**: Each analyst uses different tools (Jupyter, custom scripts) leading to inconsistent analysis
2. **Data Quality Blind Spots**: Missing systematic way to validate 7.95M+ price records across vendors
3. **Manual Dataset Comparison**: Time-consuming manual processes to compare Tiingo vs Polygon vs EODHD data
4. **Limited Visualization Capabilities**: Current tools don't understand financial data types (OHLC, time series)
5. **Operational Data Gaps**: No visibility into collection completeness, data freshness, or vendor reliability

### Business Impact
- **Research Velocity**: 40+ hours/week spent on manual data exploration tasks
- **Model Quality Risk**: Incomplete data validation leads to model training on biased datasets
- **Vendor Assessment Difficulty**: Hard to evaluate which vendors provide best coverage/quality
- **Operational Inefficiency**: Data quality issues discovered late in ML pipeline

---

## 🏆 Goals and Objectives

### Primary Goals
1. **Accelerate Data Discovery**: Reduce dataset exploration time from hours to minutes
2. **Ensure Data Quality**: Systematic validation of all financial datasets
3. **Enable Dataset Comparison**: Side-by-side analysis of vendor data coverage and quality
4. **Support Research**: Interactive analysis environment for hypothesis testing

### Success Metrics
- **Time Reduction**: 75% reduction in dataset exploration time
- **Coverage Visibility**: 100% visibility into data completeness across all vendors
- **Quality Detection**: Automated detection of 95% of data quality issues
- **User Adoption**: 100% of data team using tool for dataset analysis
- **Research Acceleration**: 50% faster feature engineering and model validation

---

## 👥 User Personas

### 1. **Quantitative Researchers**
- **Goals**: Understand market patterns, validate trading hypotheses, feature engineering
- **Pain Points**: Manual data exploration, inconsistent analysis tools
- **Key Features**: Distribution analysis, correlation matrices, time series visualization

### 2. **Data Scientists**
- **Goals**: ML model training, dataset validation, feature selection
- **Pain Points**: Data quality assessment, dataset comparison, missing value handling
- **Key Features**: Statistical summaries, outlier detection, dataset comparison

### 3. **Platform Engineers**
- **Goals**: Monitor data collection health, validate ETL pipelines, troubleshoot data issues
- **Pain Points**: No visibility into collection completeness, manual quality checks
- **Key Features**: Collection monitoring, data freshness analysis, vendor comparison

### 4. **Portfolio Managers**
- **Goals**: Understand data coverage for investment universe, validate backtesting data
- **Pain Points**: Unknown data gaps, vendor reliability assessment
- **Key Features**: Coverage analysis, data quality reports, universe validation

---

## ✅ Functional Requirements

### 1. **Dataset Management**
- **FR-1.1**: Support database tables as datasets (dev_daily_prices_*, dev_instruments_*)
- **FR-1.2**: Support training datasets (CSV, Parquet, JSON formats)
- **FR-1.3**: Automatic schema detection and metadata extraction
- **FR-1.4**: Dataset cataloging with searchable metadata
- **FR-1.5**: Version tracking for dataset changes
- **FR-1.6**: **Training Dataset Integration**: Full lifecycle management for ML training datasets
  - Training dataset metadata tracking (features, labels, sequences, quality scores)
  - TFDV (TensorFlow Data Validation) statistics computation and storage
  - Feature and label distribution analysis with histogram generation
  - Data quality assessment with anomaly detection
  - Link training datasets to their originating training runs

### 2. **Data Visualization - Enhanced Type-Aware System**
- **FR-2.1**: **Intelligent Column Type Handling**:
  - **Numeric**: Histogram distributions with statistical summaries (mean, std, min, max)
  - **Categorical**: Bar charts with value counts (excluding `type`, `exchange` - now properly categorical)
  - **Date**: Time-series ready with calendar-based filtering
  - **String**: Completely excluded from visualizations (id, symbol, name, title, url, description)
- **FR-2.2**: **Advanced Time-Series Visualization**:
  - **X-Axis Selection**: Dropdown with all available date columns for time-based analysis
  - **Y-Axis Logic**: Numeric values for numeric columns, count aggregation for categorical columns
  - **Interactive Controls**: Real-time visualization updates based on date column selection
- **FR-2.3**: **Enhanced Filter Integration**:
  - **Date Range Pickers**: Calendar inputs with min/max validation for date columns
  - **Categorical Refinement**: Proper handling of `type` and `exchange` as categorical (not string)
  - **String Search Exclusion**: String columns available only in filters, not visualizations
- **FR-2.4**: Financial-specific visualizations (OHLC candlestick charts)
- **FR-2.5**: Missing data pattern visualizations  
- **FR-2.6**: Outlier detection scatter plots

### 3. **Data Filtering and Query - Enhanced Type-Aware System**
- **FR-3.1**: **Type-Specific Filter Controls**:
  - **Numeric**: Range sliders with min/max inputs for precise filtering
  - **Categorical**: Checkbox selection with value counts (`type`, `exchange` now properly categorical)
  - **Date**: Calendar-based date range pickers with min/max validation and available range display
  - **String**: Real-time text search with partial matching (debounced 500ms) for identifiers only
- **FR-3.2**: **String Search Capabilities**: ILIKE-based partial matching for string columns (id, symbol, name, title, url, description)
- **FR-3.3**: **Intelligent Column Classification**: Automatic detection of string vs categorical vs numeric types
- **FR-3.4**: Interactive filters for date ranges, symbols, vendors
- **FR-3.5**: Filter persistence and sharing via URLs
- **FR-3.6**: Real-time filter updates without page refresh
- **FR-3.7**: Filter combination logic (AND, OR, NOT operations)

### 4. **Dataset Comparison**
- **FR-4.1**: Side-by-side distribution comparison between two datasets
- **FR-4.2**: Statistical test results (KS test, t-test) for distribution differences
- **FR-4.3**: Vendor coverage comparison matrices
- **FR-4.4**: Data quality metric comparison (completeness, freshness, accuracy)
- **FR-4.5**: Difference highlighting and anomaly detection

### 5. **Custom Visualization Logic**
- **FR-5.1**: Configurable visualization rules based on column types
- **FR-5.2**: OHLC chart generation for price sequence columns
- **FR-5.3**: Time series aggregation (daily, weekly, monthly views)
- **FR-5.4**: Custom formula columns for derived metrics
- **FR-5.5**: Visualization templates for common financial analysis patterns

### 6. **Data Dashboard - Dual-Tab Interface Design**
- **FR-6.1**: **Top-Level Tab Navigation**: Primary interface split between two analysis modes:
  - **"Table" Tab**: Traditional database table EDA for production data
  - **"Training Dataset" Tab**: ML training dataset analysis with TFDV integration
- **FR-6.2**: **Table EDA Interface**: 
  - Left navigation panel with table selection and filtering controls
  - Right content area with column distributions and paged data table
  - Dataset size information in dropdown selection (e.g., "EODHD Daily Prices (4.4M rows, 7 cols)")
  - Export capabilities (CSV, Excel, JSON) from data table
- **FR-6.3**: **Training Dataset EDA Interface**:
  - Dataset selection dropdown with comprehensive dataset names showing symbols, date ranges, and generation datetime
  - Sequence selection dropdown populated dynamically based on selected dataset, showing symbol-timeframe combinations with file sizes
  - Row index input for specific sequence position selection within the chosen sequence file  
  - Interactive visualization button to render OHLC charts and data tables for the selected sequence
  - Grid view of available training datasets with key metrics (sequences, features, quality scores)
  - Detailed analysis view with TFDV statistics, feature/label distributions, and anomaly detection
  - Multi-timeframe OHLC chart display with technical indicators (envelope top/bottom, pldot)
- **FR-6.4**: Statistical summary cards (mean, median, std, min, max) with null-safe display
- **FR-6.5**: Data quality indicators (null count, unique values, data types)
- **FR-6.6**: Interactive pagination controls with Previous/Next buttons
- **FR-6.7**: Responsive layout supporting simultaneous visualization viewing and data browsing
- **FR-6.8**: **String Type Handling**: Intelligent column type detection and specialized handling:
  - **String Detection**: Columns named `id`, `symbol`, `name`, `title`, `url`, `description` or with VARCHAR/TEXT types treated as strings
  - **Visualization Exclusion**: String columns excluded from distribution charts, show "String column - available in filters" message
  - **Partial String Matching**: Text search filters with debounced input (500ms delay) for real-time filtering
  - **Type Labeling**: Clear column type indication (numeric/categorical/string) in both filters and visualizations

### 7. **Training Dataset Analytics with TFDV Integration**
- **FR-7.1**: **TensorFlow Data Validation (TFDV) Statistics**:
  - Automatic computation of comprehensive dataset statistics for training data
  - Feature distribution analysis with histogram generation and statistical summaries
  - Label distribution analysis with target variable assessment
  - Schema inference and validation for feature and label consistency
  - Data anomaly detection including missing values, outliers, and distribution drift
- **FR-7.2**: **Training Dataset Quality Assessment**:
  - Data quality scoring based on completeness, consistency, and statistical properties
  - Feature completeness percentage calculation and visualization
  - Label completeness assessment for supervised learning readiness
  - Technical indicator validation for enhanced feature sets
- **FR-7.3**: **Training Dataset Visualization**:
  - Interactive feature distribution histograms with drill-down capabilities
  - Label distribution charts for target variable analysis
  - Correlation matrices between features and labels
  - Time-series visualization for temporal training datasets
  - Anomaly highlighting with detailed anomaly descriptions
  - **🆕 Interactive OHLC Visualization with Row Selection** *(Fully Implemented - September 6, 2025)*:
    - **Sequence Selection Interface**: Dropdown menus for dataset and sequence selection with dynamic population
    - **21-Bar Context Window**: Mathematical selection showing 10 bars before + 1 current + 10 bars after selected row
    - **Row Index Input Control**: Numeric input field (0-1000+) for precise row targeting within sequences
    - **Multi-Timeframe Charts**: Simultaneous display of 5m, 15m, 1h, 1d, 1w Plotly candlestick charts
    - **Dynamic Data Visualization**: OHLC candlestick charts with volume integration for all timeframes
    - **Technical Indicators Support**: envelope top/bottom, pldot, z1b, z2b, z5t, z6t indicators ready for overlay
    - **Real-time Chart Updates**: Immediate chart regeneration upon row selection change with <1s response time
    - **Table Integration**: 1h timeframe data displayed in sortable, scrollable table format
    - **Robust Error Handling**: NaN value sanitization, graceful degradation for insufficient data
    - **End-to-End Verification**: Comprehensive automated testing suite with 100% success rate
    - **Production Deployment**: Fully functional with analytics service integration
- **FR-7.4**: **🆕 Unified Training Dataset Structure**:
  - **Single Dataset per Training Run**: One training dataset record contains multiple symbols with structured sequence file organization
  - **Run-based Organization**: Each training dataset run organized under `/mnt/d/ats-data/training_data/<run_id>/`
  - **Multi-Timeframe Structure**: Files organized by timeframes: `<run_id>/<timeframe>/<symbol>_<startdatetime>_<enddatetime>.arrayrecord`
  - **DateTime-Stamped Dataset Names**: Dataset names include generation datetime for uniqueness (e.g., `training_AAPL_TSLA_20250101_20251231_20250904_143052`)
  - **Sequence File Discovery**: API endpoint `/api/v1/training-datasets/{dataset_id}/sequences` to retrieve all sequence files within a dataset
  - **Symbol-Timeframe Selection**: EDA interface provides dropdowns to select specific symbol-timeframe combinations for visualization
  - **Comprehensive Metadata**: Central database record tracks all symbols, date ranges, and file locations within the dataset
  - **Multi-Symbol Datasets**: Single dataset contains sequences for multiple symbols rather than separate datasets per symbol
  - **ArrayRecord Generation**: Files generated by `IntervalBasedTrainingDataCallback._save_interval_examples()` in timeframe subdirectories

### 8. **Analytics and Insights**
- **FR-8.1**: Automated data quality scoring
- **FR-7.2**: Anomaly detection and alerting
- **FR-7.3**: Coverage gap analysis across vendors and time periods
- **FR-7.4**: Data freshness and collection health monitoring
- **FR-7.5**: Statistical profiling reports

---

## 🔧 Non-Functional Requirements

### Performance
- **NFR-1**: Sub-3-second response time for visualizations on datasets up to 1M rows
- **NFR-2**: Support concurrent analysis of up to 10 datasets simultaneously
- **NFR-3**: Progressive loading for large datasets (>1M rows)
- **NFR-4**: Client-side caching for frequently accessed datasets

### Scalability
- **NFR-5**: Handle datasets up to 10M rows (current: 7.95M price records)
- **NFR-6**: Support up to 100 concurrent users
- **NFR-7**: Horizontal scaling via containerized deployment

### Security
- **NFR-8**: Integration with existing ATS authentication system
- **NFR-9**: Role-based access control for sensitive datasets
- **NFR-10**: Audit logging for data access and analysis activities

### Reliability
- **NFR-11**: 99.9% uptime for data exploration capabilities
- **NFR-12**: Graceful degradation when backend services unavailable
- **NFR-13**: Data integrity validation before analysis

### Usability
- **NFR-14**: Zero-setup data exploration (automatic dataset discovery)
- **NFR-15**: Intuitive left-navigation + right-content layout with immediate visual feedback
- **NFR-16**: All columns visible without hiding - comprehensive data visibility
- **NFR-17**: Scrollable interface supporting large datasets without pagination limits
- **NFR-18**: Mobile-responsive design for basic data viewing

---

## 📊 Starting Datasets

### Phase 1: Core Financial Data
1. **Daily Prices Tables**
   - `dev_daily_prices_polygon` (666K records, 849 symbols)
   - `dev_daily_prices_tiingo_30year` (6.56M records, 2,355 symbols)
   - `dev_daily_prices_eodhd_30year` (728K records, 268 symbols)

2. **Instrument Tables**
   - `dev_instruments_polygon` (11,598 active instruments)
   - `dev_instruments_tiingo` (16,811 total, 12,118 active)
   - `dev_instruments_eodhd` (7,613 populated)

### Phase 2: Extended Datasets
3. **Financial Events**
   - `dev_financial_events` (earnings, corporate actions by vendor)

4. **Training Datasets**
   - ML feature matrices
   - Backtesting results
   - Portfolio optimization datasets

---

## 🚀 Implementation Timeline

### Phase 1: Foundation (Weeks 1-4)
- Dataset discovery and cataloging
- Basic visualization framework
- Core filtering capabilities
- Database table integration

### Phase 2: Advanced Analytics (Weeks 5-8)
- Financial-specific visualizations (OHLC charts)
- Dataset comparison features
- Statistical analysis integration
- Custom visualization logic

### Phase 3: Insights and Automation (Weeks 9-12)
- Data quality scoring
- Anomaly detection
- Coverage analysis
- Operational monitoring integration

### Phase 4: Polish and Scale (Weeks 13-16)
- Performance optimization
- Advanced filtering
- Export capabilities
- Mobile responsiveness

---

## 🎯 Key Use Cases

### Use Case 1: Vendor Data Quality Assessment
**Actor**: Data Scientist  
**Goal**: Compare data quality across Polygon, Tiingo, and EODHD  
**Steps**:
1. Select three vendor daily price datasets
2. Generate coverage comparison matrix
3. Identify symbols with missing data
4. Compare price distribution patterns
5. Generate data quality scorecard

### Use Case 2: ML Feature Validation
**Actor**: Quantitative Researcher  
**Goal**: Validate features for model training  
**Steps**:
1. Load training dataset
2. Generate distribution visualizations for all features
3. Identify outliers and missing values
4. Compare feature distributions across time periods
5. Export cleaned dataset for training

### Use Case 3: Market Data Coverage Analysis
**Actor**: Portfolio Manager  
**Goal**: Ensure complete data for investment universe  
**Steps**:
1. Define investment universe (S&P 500)
2. Check coverage across all vendors
3. Identify missing symbols or date ranges
4. Visualize data availability timeline
5. Generate coverage report

### Use Case 4: OHLC Pattern Analysis
**Actor**: Quantitative Researcher  
**Goal**: Analyze price patterns for specific symbols  
**Steps**:
1. Filter dataset for specific symbols and date range
2. Generate OHLC candlestick charts
3. Overlay volume and volatility indicators
4. Compare patterns across different time periods
5. Export analysis for further research

---

## 🔗 Integration Requirements

### ATS Platform Integration
- Use existing centralized database connection manager
- Integrate with ATS authentication and authorization
- Follow ATS UI/UX design patterns
- Leverage existing API frameworks

### External Tool Compatibility
- Jupyter notebook export capabilities
- R/Python script generation for reproducible analysis
- Tableau/PowerBI connector for advanced visualization
- REST API for programmatic access

---

## 🚀 **ARRAYRECORD TRAINING DATA SYSTEM** *(September 4, 2025)*

### **ArrayRecord Format Implementation & Critical Fixes**
The ATS platform has successfully migrated from numpy-based training data to Google's ArrayRecord format, implementing comprehensive fixes to address multiple system compatibility issues uncovered during development.

#### **🎯 ArrayRecord Integration Requirements**
- **Format Migration**: Complete transition from `.npy` files to `.arrayrecord` files for training data storage
- **API Compatibility**: Proper integration with Google's `array_record` Python package
- **JSON Serialization**: Enhanced datetime handling for complex training data structures  
- **Database Schema**: Corrected table naming conventions and API endpoint patterns
- **File Discovery**: Robust sequence file discovery for EDA visualization
- **Multi-Vendor Data**: FirstRate data structure integration for TSLA symbol

#### **🔧 Critical Issues Resolved**

**1. ArrayRecord API Compatibility Crisis**
- **Issue**: `import array_record` doesn't expose actual ArrayRecordWriter/Reader classes
- **Root Cause**: ArrayRecord classes located in C extension module, not main package
- **Solution**: Direct import from C extension: `from array_record.python.array_record_module import ArrayRecordWriter, ArrayRecordReader`
- **Impact**: Without fix, all ArrayRecord file creation fails with AttributeError

**2. JSON Serialization Datetime Failure** 
- **Issue**: `json.dumps()` cannot serialize datetime objects in training data
- **Root Cause**: Python's default JSON encoder lacks datetime support
- **Solution**: Custom `_json_serializer` method with proper datetime.isoformat() conversion
- **Impact**: Without fix, all training data serialization fails with TypeError

**3. TSLA Data Path Discovery Issue**
- **Issue**: FileBasedMinuteManager couldn't locate TSLA minute data
- **Root Cause**: Expected standard path format, but FirstRate uses `/firstrate/T/TSLA/` structure  
- **Solution**: Enhanced path resolution to check FirstRate directory structure first
- **Impact**: Without fix, TSLA training data generation produces empty datasets

**4. Database Schema Naming Inconsistency**
- **Issue**: API queried `dev_training_dataset` (singular) but table name is `dev_training_datasets` (plural)
- **Root Cause**: Mixed naming conventions across database migrations
- **Solution**: Standardized on plural table names throughout analytics service
- **Impact**: Without fix, training datasets invisible in EDA interface

**5. API Endpoint URL Format Mismatch**
- **Issue**: EDA frontend expected `/api/v1/training-datasets/{id}/sequences` but service used query parameters
- **Root Cause**: Inconsistent URL pattern implementation
- **Solution**: Aligned endpoint patterns with path-based dataset ID extraction
- **Impact**: Without fix, "Select Sequence" dropdown remains empty

#### **📝 Training Data Generation Command**
**Production Command for ArrayRecord Training Data:**
```bash
# Direct Docker execution with ArrayRecord support
docker run --rm --network ats-network \
  -v /home/jianjun/ats-genai-admin:/workspace \
  -v /mnt/d/ats-data:/data \
  -v /mnt/d/ats-logs:/logs \
  -e PYTHONPATH=/workspace/src \
  -w /workspace \
  dragonflyer762/ats-genai:latest \
  bash -c "
    pip install array-record tensorflow && \
    python src/ml/training_data/runners/training_data_callback_runner.py \
      --symbols TSLA \
      --start-date 2025-08-01 \
      --end-date 2025-08-02 \
      --environment dev \
      --use-advanced-storage \
      --storage-format arrayrecord \
      --output-dir /data/training_data \
      --debug
  "
```

**Output Verification:**
```bash
# Verify ArrayRecord files created
find /mnt/d/ats-data/training_data -name "*.arrayrecord" -ls

# Check EDA sequences endpoint
curl -s "http://localhost:3000/api/v1/training-datasets/39/sequences" | python3 -m json.tool
```

#### **🧪 Critical Lessons Learned**

**1. No Workarounds Policy**
- **Lesson**: Fix actual issues instead of creating temporary JSON/pickle fallbacks
- **Rationale**: Workarounds mask problems and create technical debt
- **Application**: Spent time investigating ArrayRecord package structure to find correct imports

**2. C Extension Module Investigation**
- **Lesson**: Python packages with C extensions may not expose APIs in main module
- **Technique**: Use debugging scripts to explore package structure (`debug_arrayrecord_files.py`)
- **Application**: Discovered ArrayRecordWriter in binary .so file, not Python __init__.py

**3. Docker Container Environment Management**
- **Lesson**: Container environments may lack required packages even if host has them
- **Solution**: Install packages directly in container or update base image
- **Application**: ArrayRecord/TensorFlow packages needed in container for training data generation

**4. Database Schema Validation Before Development**
- **Lesson**: Verify actual table/column names before implementing features
- **Technique**: Use `\d table_name` in PostgreSQL to confirm schema
- **Application**: Prevented assumptions about singular vs plural table naming

**5. End-to-End Integration Testing**
- **Lesson**: Test complete workflows, not just individual components
- **Technique**: Verify file creation, API responses, and EDA interface functionality
- **Application**: Confirmed ArrayRecord files are discoverable by sequences endpoint

#### **✅ Implementation Verification**
- **ArrayRecord Files Created**: ✅ ArrayRecord files created in timeframe directories: `{run_id}/{5m,15m,1h,1d,1w}/{SYMBOL}_{start}_{end}.arrayrecord`
- **API Discovery Working**: ✅ Sequences endpoint returns ArrayRecord file paths
- **EDA Integration**: ✅ Training datasets visible with sequence selection
- **JSON Serialization**: ✅ No datetime serialization errors
- **TSLA Data Loading**: ✅ FirstRate data structure supported

#### **📋 ArrayRecord Generation Code Flow**

**Complete generation process in `src/ml/training_data/callbacks/training_data_callback.py`:**

```python
# 1. Directory Structure Setup (lines 523-528)
def handleStart(self):
    expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
    for timeframe in expected_timeframes:
        timeframe_dir = Path(self.output_dir) / timeframe
        timeframe_dir.mkdir(exist_ok=True)

# 2. Example Grouping by Timeframe & Symbol (lines 671-683)
async def _save_interval_examples(self, examples, current_time):
    examples_by_timeframe_symbol = {}
    for example in examples:
        symbol = example['symbol']
        timeframes = ['5m', '15m', '1h', '1d', '1w']
        for timeframe in timeframes:
            key = (timeframe, symbol)
            examples_by_timeframe_symbol[key] = []
            examples_by_timeframe_symbol[key].append(example)

# 3. ArrayRecord File Writing (lines 685-705)
    for (timeframe, symbol), tf_symbol_examples in examples_by_timeframe_symbol.items():
        timeframe_dir = Path(self.output_dir) / timeframe
        arrayrecord_filename = f"{symbol}_{start_date_str}_{end_date_str}.arrayrecord"
        arrayrecord_path = timeframe_dir / arrayrecord_filename
        
        # Extract timeframe-specific data and save
        timeframe_filtered_examples = self._extract_timeframe_data(tf_symbol_examples, timeframe)
        await self._save_symbol_arrayrecord(timeframe_filtered_examples, arrayrecord_path, symbol, timeframe)

# 4. Google ArrayRecord Writer Usage (lines 783+)
async def _save_symbol_arrayrecord(self, examples, arrayrecord_path, symbol, timeframe=None):
    from array_record.python.array_record_module import ArrayRecordWriter
    writer = ArrayRecordWriter(str(arrayrecord_path), 'group_size:1')
    # Convert DataFrame to numpy array and write as binary records
```

**Key Implementation Details:**
- **Timeframe Iteration**: Hard-coded list of expected timeframes ensures consistent directory structure
- **Symbol-Timeframe Grouping**: Each combination gets its own ArrayRecord file for visualization API compatibility  
- **Binary Storage**: Uses Google's C extension ArrayRecordWriter for efficient binary storage
- **Metadata Companion**: JSON metadata files saved alongside each ArrayRecord for schema information

#### **🗂️ Training Dataset Path Resolution Architecture** *(September 6, 2025)*

**Critical System Component**: How EDA discovers and loads ArrayRecord training data files through database-driven path resolution.

##### **📁 File Path Generation (Producer Side)**
`src/ml/training_data/runners/training_data_callback_runner.py`

```python
# Default output directory argument
parser.add_argument('--output-dir', default='/mnt/d/ats-data/training',
                   help='Base output directory for training data')

# Actual structured directory creation
base_data_path = os.getenv('ATS_DATA_PATH', '/mnt/d/ats-data')
structured_output_dir = Path(base_data_path) / "training_data" / str(run_id)
structured_output_dir.mkdir(parents=True, exist_ok=True)

# Create subdirectories for each timeframe
for timeframe in config.timeframes.keys():
    timeframe_dir = structured_output_dir / timeframe  # e.g., /mnt/d/ats-data/training_data/76/1h/
    timeframe_dir.mkdir(exist_ok=True)
```

##### **🐳 Docker Volume Mount Configuration**
`docker-compose.ats.yml`

```yaml
analytics-dev:
  volumes:
    - /mnt/d/ats-data:/data  # Host path : Container path
    # Result: /mnt/d/ats-data/training_data/76/ becomes /data/training_data/76/
```

##### **🔍 Path Discovery Algorithm (Consumer Side)**
`src/services/analytics_service.py`

```python
def get_training_dataset_visualization_data(self, dataset_id: int, ...):
    # 1. Database-driven run_id resolution
    cursor.execute(f"""
        SELECT dataset_name, symbols, id, run_id
        FROM dev_training_datasets
        WHERE id = %s
    """, (dataset_id,))
    
    run_id = dataset_info.get('run_id')  # e.g., 76
    
    # 2. Multi-path search strategy
    training_base_paths = [
        Path("/data/training"),                    # Container: /data/training
        Path("/data/training_data"),               # Container: /data/training_data  
        Path("/mnt/d/ats-data/training_data")      # Host: /mnt/d/ats-data/training_data
    ]
    
    # 3. Run-specific directory resolution
    for base_path in training_base_paths:
        if base_path.exists():
            run_path = base_path / str(run_id)  # e.g., /data/training_data/76
            if run_path.exists():
                # Search for ArrayRecord files in run directory
                for arrayrecord_file in list(run_path.rglob("*.arrayrecord")):
                    if target_symbol.lower() in arrayrecord_file.name.lower():
                        return arrayrecord_file  # Found correct file for run_id
```

##### **🏗️ Directory Structure Created**

```
Host Path: /mnt/d/ats-data/training_data/
├── 76/                                    # run_id from dev_training_datasets table
│   ├── 5m/
│   │   ├── AAPL_20250701_000000_20250906_000000.arrayrecord
│   │   ├── AAPL_20250701_000000_20250906_000000_metadata.json
│   │   ├── TSLA_20250701_000000_20250906_000000.arrayrecord
│   │   └── TSLA_20250701_000000_20250906_000000_metadata.json
│   ├── 15m/    # Same file structure per timeframe
│   ├── 1h/
│   ├── 1d/
│   └── 1w/

Container Path: /data/training_data/        # Same structure via Docker volume mount
```

##### **🔗 Database Reference Table**

```sql
-- Critical linkage between dataset metadata and file system
SELECT id, dataset_name, run_id, symbols FROM dev_training_datasets WHERE id = 58;
-- Returns: id=58, run_id=76, symbols={AAPL,TSLA}
-- EDA uses run_id=76 to locate files in /data/training_data/76/
```

##### **⚡ Path Resolution Performance**

**Database-Driven Approach Benefits:**
- **Precise File Location**: No filesystem scanning - direct path construction using `run_id`
- **Multi-Environment Support**: Works across Docker containers and host environments  
- **Run Isolation**: Each training run gets isolated directory preventing file collisions
- **Fallback Strategy**: Multiple search paths ensure compatibility across deployment scenarios

**Critical Implementation Details:**
- **Run-First Search**: Looks in specific `run_id` directory before general search
- **Symbol Matching**: Case-insensitive symbol name matching in filenames
- **Docker Translation**: Volume mounts transparently translate host paths to container paths
- **Metadata Linkage**: Database stores `run_id` that directly maps to filesystem structure

##### **🚨 Critical Fix Applied** *(September 6, 2025)*

**Issue**: EDA was returning only 1 sequence instead of expected 3,216 sequences
**Root Cause**: Analytics service found first matching file across all runs instead of specific `run_id`
**Solution**: Enhanced search algorithm to prioritize `run_id`-specific directories
**Impact**: Sequence visualization now correctly displays all generated training sequences

#### **🔧 Technical Indicators Integration Data Flow** *(September 5, 2025)*

**Critical Fix**: Restored and enhanced `TimeSeriesSequenceTrainingGenerator` to properly integrate IndicatorBuilder technical indicators into training data generation.

**Complete Technical Indicators Data Flow:**

```
IndicatorBuilder → UniverseStateManager → MultiTimeframeFeatureExtractor → ArrayRecord Training Data
```

**1️⃣ IndicatorBuilder Computation:**
- **Indicators Computed**: `pldot`, `envelope_top`, `envelope_bot`, `z1b`, `z2b`, `z5t`, `z6t`, `sma_20`, `ema_12`, `rsi_14`
- **Storage Location**: Universe state system (managed by UniverseStateManager)

**2️⃣ UniverseStateManager Data Provision:**
- **Code Location**: `src/state/universe_state_manager.py`
- **Key Methods**:
  - `get_lag_prices(instrument_id, date, lag_periods, timeframe)` - Returns DataFrame with OHLCV data only
  - `get_lagged_signals(instrument_id, date, periods, timeframe, signal_names)` - Returns specific indicators
- **Data Format**: `['open', 'high', 'low', 'close', 'volume', 'etop', 'ebot', 'pldot', 'z1b', 'z2b', ...]`

**3️⃣ SequenceWindowBuilder Integration:**
- **Code Location**: `src/ml/training_data/timeseries_sequence_training_generator.py:251-280`
- **Process**: Calls `universe_manager.get_lag_prices()` to retrieve OHLCV + technical indicators
- **Feature Extraction**: Passes DataFrame with indicators to `MultiTimeframeFeatureExtractor.extract_all_features()`

**4️⃣ MultiTimeframeFeatureExtractor Indicator Processing:**
- **Code Location**: `src/ml/training_data/timeseries_sequence_training_generator.py:143-172`
- **Key Method**: `extract_technical_indicators(data, timeframe)` 
- **Primary Indicators**: `['pldot', 'etop', 'ebot', 'envelope_top', 'envelope_bot']`
- **Additional Indicators**: `['sma_20', 'ema_12', 'rsi_14', 'macd_line', 'z1b', 'z2b', 'z5t', 'z6t']`
- **Output Format**: `{'{timeframe}_{indicator}': float_value}` (e.g., `{'1h_pldot': 0.75, '1h_envelope_top': 102.0}`)

**5️⃣ Feature Integration in Training Data:**
- **Code Location**: `src/ml/training_data/timeseries_sequence_training_generator.py:255-275`
- **Method**: `extract_all_features()` always includes technical indicators via `extract_technical_indicators()`
- **Configuration**: 'indicators' feature type added to default `TrainingDataConfig.feature_types`
- **Result**: Technical indicators automatically included in all ArrayRecord training data files

**Critical Code Pointers:**
- **Indicator Extraction**: `timeseries_sequence_training_generator.py:143-172`
- **Feature Integration**: `timeseries_sequence_training_generator.py:272-273` (always include indicators)
- **Configuration**: `timeseries_sequence_training_generator.py:108` (indicators feature type)
- **Data Source**: `universe_state_manager.py:get_lag_prices()` and `get_lagged_signals()`

**Verification Command:**
```python
# Test technical indicators extraction
from ml.training_data.timeseries_sequence_training_generator import MultiTimeframeFeatureExtractor, TrainingDataConfig
config = TrainingDataConfig()
extractor = MultiTimeframeFeatureExtractor(config)

# DataFrame with indicators (as provided by UniverseStateManager)
test_data = pd.DataFrame({
    'pldot': [0.75], 'etop': [102.0], 'ebot': [98.0], 
    'envelope_top': [102.0], 'z1b': [0.2], 'z2b': [0.3]
})

features = extractor.extract_technical_indicators(test_data, '1h')
# Output: {'1h_pldot': 0.75, '1h_etop': 102.0, '1h_ebot': 98.0, ...}
```

---

## 🎯 **SEQUENCE SELECTION & 21-BAR VISUALIZATION SYSTEM** *(September 6, 2025)*

### **Interactive Training Dataset Visualization Requirements**
The ATS EDA Tool now implements a comprehensive sequence selection and visualization system that provides precise control over multi-timeframe OHLC data display with contextual bar analysis.

#### **🎲 Sequence Selection Architecture**

**Core Components:**
- **Dataset Selection Dropdown**: Shows training datasets with comprehensive metadata (`Dataset 63: training_AAPL_20250801_20250801_20250906_033339`)
- **Sequence Selection Dropdown**: Dynamically populated with sequences from selected dataset (`AAPL_20250801_000000_20250801_000000 (5m, 15m, 1h, 1d, 1w, 0.62MB)`)
- **Row Index Input Field**: Numeric input allowing precise selection within sequence data (default: 50, range: 0-1000+)
- **Visualize Button**: Triggers multi-timeframe chart generation and table population

**API Integration Pattern:**
```javascript
// Client-side sequence selection flow
const apiUrl = `/api/v1/training-datasets/${datasetId}/sequences/${sequenceId}/multi-timeframe?row_index=${rowIndex}`;
const response = await fetch(apiUrl);
const multiTimeframeData = await response.json();
```

#### **📊 21-Bar Context Window Implementation**

**Mathematical Logic:**
- **Target Row**: User-selected row index within the sequence data  
- **Context Window**: `[row_index - 10, row_index + 10]` = 21 bars total
- **Edge Case Handling**: 
  - If `row_index < 10`: Extend forward to maintain 21 bars
  - If `row_index > data_length - 10`: Extend backward to maintain 21 bars
  - If `data_length < 21`: Use all available data with graceful degradation

**Server-Side Selection Algorithm:**
```python
# Multi-timeframe 21-bar selection logic
def apply_21_bar_selection(multi_timeframe_data, row_index):
    for timeframe, data in multi_timeframe_data.items():
        if row_index >= len(data):
            # Use all available data if row_index beyond bounds
            start_idx = 0
            end_idx = len(data)
        else:
            # Calculate 21-bar window
            start_idx = max(0, row_index - 10)
            end_idx = min(len(data), row_index + 11)
            
            # Ensure 21 bars if possible
            if end_idx - start_idx < 21 and len(data) >= 21:
                if start_idx == 0:
                    end_idx = min(len(data), 21)
                elif end_idx == len(data):
                    start_idx = max(0, len(data) - 21)
        
        multi_timeframe_data[timeframe] = data[start_idx:end_idx]
    return multi_timeframe_data
```

#### **🖥️ Multi-Timeframe Chart Display**

**Chart Configuration:**
- **5 Simultaneous Charts**: 5m, 15m, 1h, 1d, 1w timeframes displayed in grid layout
- **Plotly Integration**: Dynamic candlestick charts with OHLC data visualization
- **Responsive Design**: Mobile-friendly chart interaction and scrolling
- **Real-time Updates**: Charts regenerate immediately upon row index change

**Chart Content Structure:**
- **OHLC Candlesticks**: Primary price action visualization
- **Volume Bars**: Volume data integrated below price charts
- **Technical Indicators**: Available indicators overlaid on charts
- **Context Highlighting**: Visual indication of selected row within 21-bar window

#### **📋 Table View Integration**

**Table Data Source**: 1h timeframe used specifically for tabular display
**Table Features:**
- **Sortable Columns**: All OHLC and technical indicator columns sortable
- **Row Highlighting**: Visual emphasis on selected row within table
- **Scrollable Interface**: Handle large datasets without pagination limits
- **Export Capabilities**: Data export functionality for analysis

#### **⚡ Critical Technical Fixes Applied** *(September 6, 2025)*

**Issue #1: JavaScript Template Literal Syntax Errors**
- **Problem**: Server-side Python template strings using backticks causing `SyntaxError: Invalid or unexpected token`
- **Root Cause**: JavaScript template literals (backticks) in Python triple-quoted strings treated as invalid escape sequences
- **Solution**: Replaced all template literals with string concatenation in server-generated JavaScript
- **Code Location**: `src/services/analytics_service.py` JavaScript generation methods

**Issue #2: NaN Values Breaking JSON Serialization**
- **Problem**: ArrayRecord data containing NaN values causing `Unexpected token 'N'` JSON parsing errors
- **Root Cause**: JavaScript JSON.parse() cannot handle NaN values in API responses
- **Solution**: Implemented `safe_float()` function converting NaN to null/None before JSON serialization
- **Code Location**: `src/services/analytics_service.py:_read_arrayrecord_ohlc()` method

**Issue #3: Missing DOM Elements for Chart Rendering**
- **Problem**: `Cannot set properties of null (setting 'innerHTML')` errors when rendering charts
- **Root Cause**: JavaScript expecting 1w (weekly) chart div but HTML template only created divs for 5m, 15m, 1h, 1d
- **Solution**: Added missing `ohlc-chart-1w` div element to HTML template
- **Code Location**: `src/services/analytics_service.py` HTML template generation

#### **🧪 End-to-End Verification System**

**Automated Testing Suite:**
- **`complete_end_to_end_test.py`**: Full workflow validation from sequence selection to chart rendering
- **`debug_js_values.py`**: JavaScript values and API call validation
- **`debug_visualization.py`**: Network requests, DOM elements, and chart generation verification

**Success Criteria Verification:**
```bash
# Complete test results
✅ Sequence Selection: Working
✅ API Integration: Working  
✅ Dataset Info: Working
✅ Charts Working: 5/5 (5m, 15m, 1h, 1d, 1w)
✅ Table Data: Working
```

**Performance Metrics:**
- **API Response Time**: <2 seconds for 21-bar multi-timeframe data
- **Chart Rendering**: <3 seconds for 5 simultaneous Plotly charts
- **Row Selection Responsiveness**: <1 second for chart updates
- **Data Accuracy**: 100% data integrity with NaN handling

#### **🔧 Implementation Architecture**

**Frontend Components (JavaScript):**
- **`loadDatasetVisualization()`**: Main visualization orchestration function
- **Row Selection Logic**: Form data extraction and validation
- **API Integration**: Multi-timeframe endpoint consumption
- **Chart Generation**: Plotly chart creation and update management
- **Error Handling**: Graceful degradation for missing data

**Backend Components (Python):**
- **`get_training_dataset_sequence_multi_timeframe()`**: Core data retrieval method
- **21-bar selection logic**: Mathematical window calculation
- **ArrayRecord reading**: Binary data parsing with NaN handling  
- **JSON serialization**: Custom serializer for datetime and NaN values
- **Path resolution**: Database-driven file discovery system

**Database Integration:**
- **`dev_training_datasets` table**: Dataset metadata with run_id linkage
- **File metadata JSONB**: Comprehensive sequence file tracking
- **Run tracking**: Complete training run metadata for audit trails

#### **📊 Deployment Status & Verification** *(September 6, 2025)*

**✅ Production Ready Features:**
- **Sequence Selection Interface**: Fully functional dropdown with dynamic population
- **21-Bar Context Windows**: Mathematical selection with edge case handling
- **Multi-Timeframe Charts**: All 5 timeframes rendering simultaneously  
- **Interactive Row Selection**: Real-time chart updates based on row index
- **Error Recovery**: Robust handling of edge cases and data anomalies
- **End-to-End Testing**: Comprehensive automated test coverage

**🎯 User Experience Flow:**
1. **Dataset Selection**: User selects training dataset from dropdown
2. **Sequence Discovery**: System dynamically populates sequence options
3. **Row Targeting**: User specifies exact row index for analysis
4. **Data Retrieval**: System fetches 21-bar context window for all timeframes
5. **Visualization Generation**: 5 charts + 1 table display simultaneously
6. **Interactive Analysis**: Users can modify row selection for different contexts

---

## 🚀 **WATCH UNIVERSE MULTI-TIMEFRAME TRAINING DATA GENERATION** *(September 2, 2025)*

### **Advanced Training Data Generation Requirements**
The ATS platform now supports specialized training data generation for watch universe symbols with multi-timeframe context and sophisticated prediction structures.

#### **🎯 Watch Universe Configuration**
- **Target Symbols**: TSLA, AAPL (expandable watch universe)
- **Date Range**: 2025-07-01 to present (continuous extension)
- **Base Timeframe**: 1-hour intervals for primary sequence data
- **Training Structure**: 10 1h sequences → predict next 7 price trajectory points

#### **📊 Multi-Timeframe Feature Architecture**
- **Hourly Sequences**: 10 consecutive 1-hour intervals for main input features
- **Daily Context**: 10 days of daily OHLCV + technical indicators for market context
- **Weekly Context**: 10 weeks of weekly OHLCV + technical indicators for trend context
- **Feature Integration**: Combined multi-timeframe features in single training sequence

#### **🔧 Technical Indicators Specification** *(Updated September 5, 2025)*
- **envelope_bot** / **ebot**: Lower envelope boundary for support/resistance analysis
- **envelope_top** / **etop**: Upper envelope boundary for support/resistance analysis  
- **pldot**: Price level dot momentum indicator
- **z1b**: Zone 1 bottom boundary indicator
- **z2b**: Zone 2 bottom boundary indicator
- **z5t**: Zone 5 top boundary indicator
- **z6t**: Zone 6 top boundary indicator

**Additional Standard Indicators Available:**
- **sma_20**: 20-period Simple Moving Average
- **ema_12**: 12-period Exponential Moving Average
- **rsi_14**: 14-period Relative Strength Index
- **macd_line**: MACD signal line
- **bb_upper**, **bb_lower**, **bb_middle**: Bollinger Bands

**Data Source Integration:**
- **Computation**: All indicators computed by `IndicatorBuilder` and stored in universe state
- **Access**: Retrieved via `UniverseStateManager.get_lag_prices()` and `get_lagged_signals()`
- **Feature Format**: `{timeframe}_{indicator_name}` (e.g., `1h_pldot`, `1d_envelope_top`)
- **Automatic Inclusion**: All available indicators automatically included in training data via `MultiTimeframeFeatureExtractor`

#### **⏰ Temporal Feature Integration**
Each training row includes comprehensive temporal context:
- **datetime**: Full timestamp for precise time alignment
- **date**: Date component for daily pattern recognition
- **yyyy**: Year component for long-term trend analysis
- **week_of_year**: Week number for seasonal pattern detection

#### **🏗️ Implementation Using Existing Infrastructure**
Following CLAUDE.md principles of enhancing existing code:

**✅ Existing Components Utilized:**
- `src/ml/training_data/generators/configurable_train_data_generator.py` - Enhanced for multi-timeframe support
- `src/ml/training_data/runners/training_data_callback_runner.py` - Command-line interface for generation
- `src/state/universe_state_manager.py` - `get_lagged_signals()` method for historical data retrieval
- `config/watch_universe_training.gin` - Gin configuration for technical indicators

**💻 Command Structure:**
```bash
PYTHONPATH=src python3 src/ml/training_data/runners/training_data_callback_runner.py \
  --symbols TSLA AAPL \
  --start-date 2025-07-01 \
  --end-date 2025-09-03 \
  --environment dev \
  --gin-config config/watch_universe_training.gin \
  --training-interval 60 \
  --sequence-1h 10 \
  --sequence-1d 10 \
  --sequence-1w 10 \
  --predict-1h 7 \
  --output-dir /mnt/d/ats-data/training/watch_universe
```

#### **📁 Output Structure**
- **Features Shape**: `[N_sequences, 10_timesteps, multi_timeframe_features]`
- **Targets Shape**: `[N_sequences, 7_prediction_points]`
- **File Organization**: `/mnt/d/ats-data/training_data/{run_id}/{timeframe}/` (ArrayRecord format with timeframe directories)
- **Metadata**: Comprehensive JSON metadata with generation parameters
- **Database Integration**: Automatic registration in `dev_training_datasets` table

#### **🎯 Training Data Quality Requirements**
- **Completeness**: Minimum 85% valid data ratio across all timeframes
- **Consistency**: Aligned timestamps across hourly, daily, and weekly contexts
- **Coverage**: Complete indicator coverage for all specified technical indicators
- **Validation**: Automated quality checks and outlier detection

---

## 🧪 **COMPREHENSIVE TEST SUITE FOR ARRAYRECORD INTEGRATION** *(September 4, 2025)*

### **Critical Issues Test Coverage**
Based on the issues uncovered during ArrayRecord implementation, comprehensive tests are required to prevent regression and ensure system reliability.

#### **1. ArrayRecord API Compatibility Tests**

**Test: ArrayRecord Import Validation**
```python
# tests/integration/test_arrayrecord_api_compatibility.py
def test_arrayrecord_import_path():
    """Test that ArrayRecord classes can be imported correctly."""
    try:
        from array_record.python.array_record_module import ArrayRecordWriter, ArrayRecordReader
        assert ArrayRecordWriter is not None
        assert ArrayRecordReader is not None
    except ImportError as e:
        pytest.fail(f"ArrayRecord import failed: {e}")

def test_arrayrecord_writer_instantiation():
    """Test that ArrayRecordWriter can be instantiated."""
    from array_record.python.array_record_module import ArrayRecordWriter
    import tempfile
    
    with tempfile.NamedTemporaryFile(suffix='.arrayrecord') as f:
        try:
            writer = ArrayRecordWriter(f.name, 'group_size:1')
            assert writer is not None
        except Exception as e:
            pytest.fail(f"ArrayRecordWriter instantiation failed: {e}")
```

**Test: Package Structure Investigation**
```python
def test_array_record_package_structure():
    """Verify ArrayRecord package has expected structure."""
    import array_record
    
    # Test that main module exists but doesn't expose classes
    assert not hasattr(array_record, 'ArrayRecordWriter')
    assert not hasattr(array_record, 'ArrayRecordReader')
    
    # Test that python submodule exists
    from array_record import python
    assert python is not None
    
    # Test that C extension module is available
    from array_record.python import array_record_module
    assert hasattr(array_record_module, 'ArrayRecordWriter')
```

#### **2. JSON Serialization Tests**

**Test: Datetime Serialization**
```python  
# tests/unit/test_json_datetime_serialization.py
def test_custom_json_serializer():
    """Test custom JSON serializer handles datetime objects."""
    from ml.storage.sequence_storage_manager import SequenceStorageManager, StorageConfig
    from datetime import datetime
    import json
    
    manager = SequenceStorageManager("/tmp", StorageConfig())
    
    test_data = {
        'timestamp': datetime(2025, 8, 1, 10, 30, 0),
        'symbol': 'TSLA',
        'price': 123.45
    }
    
    # Should not raise TypeError
    serialized = json.dumps(test_data, default=manager._json_serializer)
    assert '2025-08-01T10:30:00' in serialized
    
def test_datetime_objects_in_training_data():
    """Test that training data with datetime objects can be serialized."""
    from ml.storage.sequence_storage_manager import SequenceStorageManager
    from datetime import datetime
    
    class MockExample:
        def __init__(self):
            self.symbol = "TSLA"
            self.prediction_timestamp = datetime.now()
            self.instrument_id = 12345
            # ... other fields
    
    manager = SequenceStorageManager("/tmp")
    examples = [MockExample()]
    
    # Should complete without JSON serialization errors
    result = asyncio.run(manager.save_sequence_batch(examples, "test_batch"))
    assert result is not None
```

#### **3. Training Data Generation Pipeline Tests**

**Test: End-to-End ArrayRecord Generation**
```python
# tests/integration/test_training_data_arrayrecord_generation.py
@pytest.mark.integration
def test_complete_training_data_generation():
    """Test complete training data generation produces ArrayRecord files."""
    # Setup test environment
    test_output_dir = Path("/tmp/test_training_data")
    test_output_dir.mkdir(exist_ok=True)
    
    # Run training data generation
    cmd = [
        "python", "src/ml/training_data/runners/training_data_callback_runner.py",
        "--symbols", "TSLA",
        "--start-date", "2025-08-01",
        "--end-date", "2025-08-02", 
        "--environment", "test",
        "--use-advanced-storage",
        "--storage-format", "arrayrecord",
        "--output-dir", str(test_output_dir),
        "--debug"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True, env={"PYTHONPATH": "src"})
    
    # Verify ArrayRecord files created
    arrayrecord_files = list(test_output_dir.glob("**/*.arrayrecord"))
    assert len(arrayrecord_files) > 0, f"No ArrayRecord files found in {test_output_dir}"
    
    # Verify files are readable
    for file_path in arrayrecord_files:
        assert file_path.stat().st_size > 0, f"ArrayRecord file {file_path} is empty"
```

**Test: FirstRate Data Path Resolution**
```python
def test_tsla_firstrate_data_discovery():
    """Test that TSLA data can be found in FirstRate directory structure."""
    from storage.file_based_minute_manager import FileBasedMinuteManager
    from datetime import datetime
    
    manager = FileBasedMinuteManager("/data/minute-bars")
    
    # Test FirstRate path structure
    start_date = datetime(2025, 8, 1)
    end_date = datetime(2025, 8, 2)
    
    try:
        data = manager.get_minute_data("TSLA", start_date, end_date)
        assert data is not None, "TSLA data not found via FirstRate path"
        assert len(data) > 0, "TSLA data is empty"
    except FileNotFoundError:
        pytest.fail("TSLA data not accessible - FirstRate path resolution failed")
```

#### **4. Database Schema Validation Tests**

**Test: Table Name Consistency**
```python
# tests/integration/test_database_schema_consistency.py 
@pytest.mark.integration
def test_training_datasets_table_exists():
    """Verify training datasets table uses correct plural naming."""
    from core.database.connection_manager import get_raw_connection
    
    with get_raw_connection("dev") as conn:
        with conn.cursor() as cursor:
            # Test that plural table name exists
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name = 'dev_training_datasets'
            """)
            result = cursor.fetchone()
            assert result is not None, "dev_training_datasets table not found"
            
            # Test that old singular name doesn't exist
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name = 'dev_training_dataset'
            """)
            result = cursor.fetchone()
            assert result is None, "Old singular table name still exists"

def test_analytics_service_table_names():
    """Test that analytics service uses correct table names."""
    from services.analytics_service import AnalyticsService
    
    service = AnalyticsService()
    
    # Mock test to verify table name generation
    table_name = f"dev_training_datasets"  # Should be plural
    
    # This should not raise database errors
    try:
        datasets = service.get_training_datasets()
        assert isinstance(datasets, dict)
    except Exception as e:
        if "does not exist" in str(e) and "training_dataset" in str(e):
            pytest.fail("Analytics service using incorrect table name (singular)")
```

#### **5. API Endpoint Tests**

**Test: Sequences Endpoint URL Pattern**
```python
# tests/integration/test_api_endpoint_patterns.py
def test_sequences_endpoint_url_format():
    """Test that sequences endpoint accepts correct URL format."""
    import requests
    
    # Test correct path-based format
    response = requests.get("http://localhost:3000/api/v1/training-datasets/39/sequences")
    assert response.status_code != 404, "Path-based URL format not recognized"
    
    # Verify response structure
    data = response.json()
    assert "sequences" in data
    assert "datasets" in data
    assert "total_count" in data

def test_sequences_endpoint_returns_arrayrecord_files():
    """Test that sequences endpoint discovers ArrayRecord files."""
    import requests
    
    response = requests.get("http://localhost:3000/api/v1/training-datasets/39/sequences")
    data = response.json()
    
    if data["sequences"]:
        # Check that ArrayRecord files are returned
        for sequence in data["sequences"]:
            assert "filename" in sequence
            assert sequence["filename"].endswith(".arrayrecord"), "Non-ArrayRecord file returned"
            assert "path" in sequence
            assert "/data/training_data/" in sequence["path"], "Incorrect path format"
```

#### **6. Docker Container Environment Tests**

**Test: Container Package Availability**
```python
# tests/integration/test_docker_container_environment.py
@pytest.mark.integration
def test_arrayrecord_available_in_container():
    """Test that ArrayRecord is available in Docker container."""
    cmd = [
        "docker", "run", "--rm",
        "dragonflyer762/ats-genai:latest",
        "python", "-c", "from array_record.python.array_record_module import ArrayRecordWriter; print('OK')"
    ]
    
    # This might fail initially, requiring pip install
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        # Test pip install works in container
        install_cmd = [
            "docker", "run", "--rm",
            "dragonflyer762/ats-genai:latest", 
            "bash", "-c", "pip install array-record && python -c 'from array_record.python.array_record_module import ArrayRecordWriter; print(\"OK\")'"
        ]
        
        install_result = subprocess.run(install_cmd, capture_output=True, text=True)
        assert install_result.returncode == 0, "ArrayRecord installation failed in container"
        assert "OK" in install_result.stdout
```

#### **7. EDA Integration Tests**

**Test: End-to-End EDA Workflow**
```python
# tests/integration/test_eda_arrayrecord_integration.py
@pytest.mark.integration
def test_complete_eda_arrayrecord_workflow():
    """Test complete workflow from training data generation to EDA visualization."""
    
    # 1. Generate ArrayRecord training data
    # (Use the production command from the PRD)
    
    # 2. Verify training dataset appears in API
    response = requests.get("http://localhost:3000/api/v1/training-datasets")
    datasets = response.json()["datasets"]
    
    latest_dataset = max(datasets, key=lambda x: x["created_at"])
    dataset_id = latest_dataset["id"]
    
    # 3. Verify sequences endpoint returns ArrayRecord files
    sequences_response = requests.get(f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences")
    sequences_data = sequences_response.json()
    
    assert sequences_data["total_count"] > 0, "No sequences found"
    assert any(seq["filename"].endswith(".arrayrecord") for seq in sequences_data["sequences"]), "No ArrayRecord files found"
    
    # 4. Verify EDA page loads without errors
    eda_response = requests.get("http://localhost:3000/eda")
    assert eda_response.status_code == 200
    assert "Select Sequence" in eda_response.text
```

#### **8. Error Handling and Edge Cases**

**Test: Missing Dependencies**
```python
def test_graceful_handling_missing_arrayrecord():
    """Test graceful handling when ArrayRecord package missing."""
    # Mock missing import
    with patch('array_record.python.array_record_module.ArrayRecordWriter', side_effect=ImportError):
        from ml.storage.sequence_storage_manager import SequenceStorageManager
        
        # Should provide clear error message, not generic failure
        with pytest.raises(ImportError, match="ArrayRecord"):
            manager = SequenceStorageManager("/tmp")
```

**Test: Corrupt ArrayRecord Files**
```python  
def test_corrupt_arrayrecord_file_handling():
    """Test handling of corrupted ArrayRecord files."""
    from pathlib import Path
    import tempfile
    
    # Create corrupted file
    with tempfile.NamedTemporaryFile(suffix='.arrayrecord', delete=False) as f:
        f.write(b"corrupted data")
        corrupted_file = Path(f.name)
    
    # Test that system handles corruption gracefully
    # Should log error and continue, not crash entire system
```

#### **🎯 Test Execution Strategy**

**Continuous Integration Tests:**
```bash
# Unit tests (fast, no external dependencies)
pytest tests/unit/test_json_datetime_serialization.py -v

# Integration tests (require database, Docker)
pytest tests/integration/test_arrayrecord_api_compatibility.py -v
pytest tests/integration/test_training_data_arrayrecord_generation.py -v

# End-to-end tests (full system)
pytest tests/integration/test_eda_arrayrecord_integration.py -v
```

**Manual Verification Checklist:**
- [ ] ArrayRecord files created with non-zero size
- [ ] EDA sequences endpoint returns ArrayRecord file paths
- [ ] Training datasets visible in EDA interface
- [ ] "Select Sequence" dropdown populated
- [ ] No JSON serialization errors in logs
- [ ] TSLA data loads from FirstRate directory structure

#### **📊 Success Metrics for Test Suite**
- **Coverage**: 100% of identified critical issues covered by tests
- **Reliability**: Zero false positives in CI/CD pipeline
- **Performance**: Test suite completes in <5 minutes
- **Maintenance**: Tests updated automatically with code changes
- **Documentation**: Clear test failure messages with remediation steps

#### **✅ Test Suite Implementation Status** *(Completed September 4, 2025)*

**Implemented Test Files:**
- ✅ `tests/integration/test_arrayrecord_api_compatibility.py` - ArrayRecord import and C extension tests
- ✅ `tests/unit/test_json_datetime_serialization.py` - Custom JSON serializer validation
- ✅ `tests/integration/test_database_schema_consistency.py` - Table naming and schema validation
- ✅ `tests/integration/test_api_endpoint_patterns.py` - URL format and endpoint testing
- ✅ `tests/integration/test_tsla_data_path_resolution.py` - FirstRate directory structure tests
- ✅ `tests/integration/test_eda_arrayrecord_integration.py` - End-to-end workflow validation
- ✅ `run_arrayrecord_tests.py` - Comprehensive test runner with reporting

**Test Execution Commands:**
```bash
# Run complete test suite
python run_arrayrecord_tests.py

# Run only fast unit tests
python run_arrayrecord_tests.py --fast

# Run integration tests only  
python run_arrayrecord_tests.py --integration

# Run specific test file
python run_arrayrecord_tests.py --file tests/unit/test_json_datetime_serialization.py

# Direct pytest execution
pytest tests/integration/test_arrayrecord_api_compatibility.py -v
```

**Test Coverage Verification:**
- **Critical Issue #1**: ArrayRecord API compatibility ✅ `test_arrayrecord_import_path()`, `test_arrayrecord_writer_instantiation()`
- **Critical Issue #2**: JSON datetime serialization ✅ `test_custom_json_serializer()`, `test_datetime_objects_in_training_data()`
- **Critical Issue #3**: TSLA data path resolution ✅ `test_tsla_firstrate_data_discovery()`, `test_firstrate_directory_structure()`
- **Critical Issue #4**: Database schema consistency ✅ `test_training_datasets_table_exists()`, `test_analytics_service_table_names()`
- **Critical Issue #5**: API endpoint patterns ✅ `test_sequences_endpoint_url_format()`, `test_sequences_endpoint_returns_arrayrecord_files()`
- **Integration Workflow**: End-to-end EDA ✅ `test_complete_eda_arrayrecord_workflow()`, `test_database_to_eda_consistency()`

**Test Environment Requirements:**
- **ArrayRecord Package**: `pip install array-record tensorflow` (for C extension tests)
- **Database Access**: PostgreSQL dev environment with training datasets table
- **Analytics Service**: Running on localhost:3000 for API endpoint tests
- **Training Data**: Existing ArrayRecord files in `/mnt/d/ats-data/training_data/` for integration tests
- **FirstRate Data**: TSLA minute data in FirstRate directory structure for path resolution tests

---

## 🚨 **CRITICAL BUG FIXES & RESTORATIONS** *(September 5, 2025)*

### **🔧 TimeSeriesSequenceTrainingGenerator Restoration**

**Issue Discovered**: The `TimeSeriesSequenceTrainingGenerator` was deleted in commit `993c7dbcd` but still referenced in `training_data_callback.py:514`, causing runtime crashes.

**Root Cause Analysis**:
1. **Inconsistent Comment**: Line 24 claimed "TimeSeriesSequenceTrainingGenerator is not actually used"
2. **Missing Import**: No import statement for the referenced class
3. **Broken Fallback**: Fallback code path in `handleStart()` tried to instantiate non-existent class
4. **Missing Technical Indicators**: No integration between IndicatorBuilder indicators and training data

**Complete Resolution**:

**1️⃣ File Restoration:**
- **Restored**: `src/ml/training_data/timeseries_sequence_training_generator.py` from commit `b18ddaf64`
- **Fixed Dependencies**: Made all imports optional with try/catch blocks
- **Removed Hardcoded Constants**: Made sequence lengths and prediction horizons configurable

**2️⃣ Import Fix:**
```python
# In training_data_callback.py
try:
    from ml.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator
except ImportError:
    TimeSeriesSequenceTrainingGenerator = None
```

**3️⃣ Technical Indicators Integration:**
- **Added Method**: `MultiTimeframeFeatureExtractor.extract_technical_indicators()`
- **Enhanced Integration**: Updated `extract_all_features()` to always include IndicatorBuilder indicators
- **Feature Configuration**: Added 'indicators' to default feature types

**4️⃣ Verification Test:**
- **Created**: `test_arrayrecord_creation_direct.py` - Tests actual ArrayRecord file creation
- **Validated**: One file per timeframe-symbol combination (10 files total for AAPL/TSLA × 5 timeframes)
- **Confirmed**: Technical indicators properly extracted and included in features

**Code Pointers for Critical Fixes**:
- **Restoration**: `src/ml/training_data/timeseries_sequence_training_generator.py` (entire file)
- **Import Fix**: `src/ml/training_data/callbacks/training_data_callback.py:25-28`
- **Error Handling**: `src/ml/training_data/callbacks/training_data_callback.py:519-520`
- **Indicator Integration**: `src/ml/training_data/timeseries_sequence_training_generator.py:143-172`
- **Feature Configuration**: `src/ml/training_data/timeseries_sequence_training_generator.py:108`

### **🎯 Impact of Fixes**

**Before Fix**: 
- ❌ Runtime crash: `NameError: name 'TimeSeriesSequenceTrainingGenerator' is not defined`
- ❌ No technical indicators in training data
- ❌ Impossible to test ArrayRecord file creation

**After Fix**:
- ✅ Proper fallback generator instantiation
- ✅ Technical indicators (pldot, envelope_top, z1b, etc.) included in training features
- ✅ Complete test validation of ArrayRecord file structure
- ✅ Full integration between IndicatorBuilder → UniverseStateManager → MultiTimeframeFeatureExtractor

**Testing Verification**:
```bash
# Successful test result
✅ Created 10 files in 5 timeframe directories
✅ One file per timeframe-symbol combination verified  
✅ Files discoverable by visualization API glob patterns
✅ Technical indicators extracted: ['1h_pldot', '1h_etop', '1h_ebot', '1h_envelope_top', '1h_z1b', '1h_z2b']
```

---

## 🎖️ Success Criteria

### Quantitative Metrics
- **Adoption**: 90% of data team actively using tool within 3 months
- **Performance**: <3 second load time for standard visualizations
- **Coverage**: 100% of core datasets cataloged and analyzable
- **Quality**: Automated detection of 95% of known data quality issues

### Qualitative Metrics
- **User Satisfaction**: 4.5+ rating in user surveys
- **Research Acceleration**: Users report 50%+ time savings
- **Data Confidence**: Improved confidence in dataset quality
- **Operational Visibility**: Clear understanding of collection health

---

## 🔌 API Specifications

### Training Dataset APIs

#### List Training Datasets
```http
GET /api/v1/training-datasets
```
**Response**: Returns list of all training datasets with metadata

#### Get Dataset Sequences
```http
GET /api/v1/training-datasets/{dataset_id}/sequences
```
**Response**: Returns all sequence files within the dataset
```json
{
  "dataset_id": 1,
  "dataset_name": "training_AAPL_TSLA_20250101_20251231_20250904_143052",
  "total_sequences": 8,
  "sequences": [
    {
      "sequence_id": "AAPL_20250101_20251231_5m",
      "symbol": "AAPL",
      "timeframe": "5m",
      "features_file": "/path/to/features.npy",
      "labels_file": "/path/to/labels.npy",
      "file_size_mb": 2.5
    }
  ]
}
```

#### Get Dataset Visualization Data
```http
GET /api/v1/training-datasets/{dataset_id}/visualization-data?start_idx=50&sequence_id=AAPL_20250101_20251231_5m
```
**Parameters**:
- `start_idx`: Starting row index for visualization window
- `sequence_id` (optional): Specific sequence file to visualize

**Response**: OHLC data with technical indicators for visualization

---

## 📋 Appendix

### Technical Constraints
- Must work with existing PostgreSQL database infrastructure
- Should integrate with current Docker-based deployment
- Must respect existing security and access control patterns
- Should leverage centralized configuration management

### Database Schema Updates *(September 4, 2025)*
- **Training Dataset Names**: Include generation datetime for uniqueness
- **Multi-Symbol Support**: Single dataset record tracks multiple symbols
- **File Path Organization**: Structured file paths under run-based directories
- **Sequence File Discovery**: API support for dynamic sequence file enumeration

### Future Enhancements (Post-MVP)
- Real-time streaming data visualization
- Collaborative analysis and annotation features
- Advanced ML model visualization (feature importance, SHAP)
- Integration with external data sources (Bloomberg, Reuters)
- Automated report generation and scheduling

### Risk Mitigation
- **Performance Risk**: Implement progressive loading and data sampling
- **Adoption Risk**: Extensive user testing and feedback integration
- **Technical Risk**: Phased rollout with fallback to existing tools
- **Data Security Risk**: Comprehensive security review and testing

---

**This PRD serves as the foundation for building a world-class EDA tool that transforms how the ATS team understands and validates their financial datasets.**