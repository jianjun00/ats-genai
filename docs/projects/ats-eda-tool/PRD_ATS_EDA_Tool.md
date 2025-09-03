# PRD: ATS Exploratory Data Analysis (EDA) Tool

**Document Version**: 2.0  
**Date**: August 30, 2025  
**Owner**: Data Infrastructure Team  
**Status**: ✅ **IMPLEMENTED** - Unified Metadata System with Automatic Statistics  

---

## 📋 Executive Summary

The ATS EDA Tool is a comprehensive data exploration and visualization platform designed to provide deep insights into the ATS financial datasets. This tool enables data scientists, quant researchers, and platform engineers to understand data quality, discover patterns, and validate datasets through interactive visualizations and statistical analysis.

### Key Value Propositions ✅ **DELIVERED**
- **✅ Automated Statistics Computation**: Automatic metadata generation and statistics computation on first dataset access
- **✅ Unified Dataset Management**: Single interface for database tables, files, and training datasets with comprehensive metadata tracking  
- **✅ 20-100x Performance Improvement**: TFDV-inspired pre-computed statistics for instant histogram visualization
- **✅ Training Dataset Integration**: Dedicated tab for ML training datasets with specialized metadata handling
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

#### **🎯 Training Datasets**  
- **Dedicated Interface**: Separate tab specifically for ML training datasets and feature matrices
- **Specialized Metadata**: Training-specific metadata including model inputs, backtesting results, portfolio optimization data
- **File Format Support**: CSV, Parquet, JSON with automatic schema detection

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
  - Grid view of available training datasets with key metrics (sequences, features, quality scores)
  - Clickable dataset cards showing dataset overview (date range, symbols, file size, technical indicators)
  - Detailed analysis view with TFDV statistics, feature/label distributions, and anomaly detection
  - Interactive histogram visualizations for features and labels
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
  - **🆕 Interactive OHLC Visualization with Row Selection**:
    - Clickable data table showing training dataset rows with sequence and time step information
    - Dynamic Plotly candlestick charts displaying OHLC data for selected rows
    - Technical indicators visualization: envelope top/bottom, pldot, z1b, z2b, z5t, z6t
    - Context-aware chart display showing 10 bars before and 10 bars after selected row
    - Multi-axis chart layout with price data, technical indicators, and volume
    - Real-time chart updates based on row selection with visual selection highlighting
    - Support for all feature types defined in Protocol Buffer schema (OHLC_INTERVALS, TECHNICAL_INDICATOR, etc.)
    - Responsive design with mobile-friendly table scrolling and chart interaction
- **FR-7.4**: **🆕 Unified Training Dataset Structure**:
  - **Run-based Organization**: Each training dataset run organized under `/mnt/d/ats-data/training/<run_id>/`
  - **Per-Symbol File Structure**: Individual Riegeli files for each symbol: `<symbol>/<startdatetime>_<enddatetime>.riegeli`
  - **Metadata Tracking**: Central metadata tracks all files within a dataset run
  - **Symbol-Specific Queries**: Backend supports querying and loading individual symbol files
  - **Unified Visualization**: EDA interface shows sequences from selected symbol files only
  - **Standardized Naming**: Strict datetime format for consistent file identification
  - **Comprehensive Metadata**: File path tracking, symbol mapping, and dataset completeness validation

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

## 📋 Appendix

### Technical Constraints
- Must work with existing PostgreSQL database infrastructure
- Should integrate with current Docker-based deployment
- Must respect existing security and access control patterns
- Should leverage centralized configuration management

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