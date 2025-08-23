# Product Requirements Document (PRD)
## ATS Analytics Platform

**Document Version:** 4.0  
**Created:** August 2025  
**Last Updated:** August 23, 2025  
**Product Manager:** AI Trading System Team  

---

## 1. Executive Summary

### 1.1 Product Vision
Build a **unified analytics platform** that consolidates all ML workflow management and analysis capabilities into a single, seamless application. Enable ML engineers, researchers, and portfolio managers to manage jobs, analyze training data, and explore model performance through one integrated interface accessible at a single endpoint.

### 1.2 Platform Consolidation (v3.0 Update)
**Major Product Enhancement**: Unified all analytics functionality into a single application:
- **Simplified Access**: Single URL (port 3000) for all analytics features
- **Integrated Experience**: Seamless navigation between job management, dataset analysis, and enhanced visualizations
- **Reduced Complexity**: Eliminated need to switch between multiple applications
- **Enhanced Usability**: All features accessible from one unified dashboard

### 1.3 Comprehensive Coverage Analytics (v4.0 Update)
**Critical Enhancement**: Resolved data coverage visibility issues and added historical analysis:
- **Coverage Issue Resolution**: Fixed "4 symbols vs 10K" discrepancy through comprehensive analytics
- **30-Year Historical Analysis**: Interactive timeline showing coverage evolution from ~50 symbols (2020-2022) to 10,000 symbols (2024-2025)
- **Gap Analysis Tools**: Real-time identification of missing data across symbols and dates
- **Root Cause Explanation**: Clear explanation of why legacy filters show limited symbols despite excellent modern coverage

### 1.4 Problem Statement
Currently, our ML workflow management and analysis capabilities are fragmented:
- **Job Execution**: No centralized dashboard for Flyte job runs, logs, and metadata
- **Training Data Management**: Generated datasets are not tracked or easily discoverable
- **Dataset Comparison**: No ability to compare distributions between different training datasets
- **Backtest Analysis**: Limited visualization and comparison of model performance
- **Workflow Visibility**: Difficulty tracking end-to-end ML pipeline from data generation to backtesting

### 1.5 Success Metrics
- **Job Management**: 100% of ML jobs tracked with metadata and accessible logs
- **Training Data Discovery**: <30 seconds to find and visualize any generated dataset
- **Dataset Comparison**: Enable side-by-side distribution analysis between any two datasets
- **Workflow Efficiency**: 50% reduction in time from job completion to insights
- **User Adoption**: 90% of ML team using platform for job management and analysis

---

## 2. Product Overview

### 2.1 Target Users

#### Primary Users
- **ML Engineers**: Need to manage job runs, monitor training pipelines, analyze model performance
- **Data Scientists**: Require training dataset exploration, comparison, and quality assessment
- **Quantitative Researchers**: Want comprehensive backtest analysis and model comparison

#### Secondary Users
- **Portfolio Managers**: Need high-level performance metrics and risk analysis
- **DevOps Engineers**: Require job monitoring and pipeline health metrics

### 2.2 Core User Stories

#### US1: Job Run Management
- **As an** ML Engineer
- **I want to** see all my Flyte job runs in a centralized dashboard with status, logs, and metadata
- **So that** I can monitor pipeline execution and quickly debug issues

#### US2: Training Dataset Tracking
- **As a** Data Scientist
- **I want to** automatically track all generated training datasets with metadata and visualization
- **So that** I can quickly discover and analyze previously generated datasets

#### US3: Training Dataset Comparison
- **As a** Data Scientist
- **I want to** compare feature distributions between two training datasets side-by-side
- **So that** I can understand data drift and validate dataset quality

#### US4: Job-to-Dataset Navigation
- **As an** ML Engineer
- **I want to** navigate from a training data generation job directly to the dataset visualization
- **So that** I can immediately analyze the results of my data generation jobs

#### US5: Comprehensive Backtest Analysis
- **As a** Quantitative Researcher
- **I want to** analyze backtest results with interactive charts and drill-down capabilities
- **So that** I can evaluate model performance and make data-driven strategy decisions

### 2.3 Enhanced User Stories (v3.0)

#### US6: Row-Level Dataset Analysis
- **As a** Data Scientist
- **I want to** filter and analyze individual sequences within training datasets with detailed metadata
- **So that** I can identify specific patterns and outliers in my training data

#### US7: Dual-Axis OHLC Visualization
- **As a** Quantitative Researcher
- **I want to** view OHLC price data with technical indicators in synchronized dual-axis charts
- **So that** I can analyze price movements and technical signals in one integrated view

#### US8: Unified Analytics Access
- **As an** ML Engineer
- **I want to** access all analytics features (jobs, datasets, visualizations) from a single URL
- **So that** I don't need to switch between multiple applications or remember different ports

### 2.4 Coverage Analytics User Stories (v4.0)

#### US9: Data Coverage Visibility
- **As a** Data Engineer
- **I want to** understand why the coverage dashboard shows only 4 active symbols when we have 10,000 instruments
- **So that** I can properly assess our actual data coverage and quality

#### US10: Historical Coverage Analysis
- **As a** Quantitative Researcher
- **I want to** see a 30-year timeline of data coverage evolution
- **So that** I can understand how our data collection capabilities have grown over time

#### US11: Gap Identification and Analysis
- **As a** Data Engineer
- **I want to** identify specific dates and symbols with missing data through interactive tools
- **So that** I can prioritize data collection efforts and fix coverage gaps

---

## 3. Functional Requirements

### 3.1 Job Management Dashboard

#### F1: Job Run Overview
- **Job List View**: Comprehensive table of all Flyte job runs with filtering and sorting
  - Job ID, Type (training-data-gen, training, backtest), Status, Start/End Time, Duration
  - User who submitted the job, Flyte workflow name, parameters
  - Success/failure indicators with error summaries for failed jobs
- **Status Filtering**: Active, Succeeded, Failed, Pending filters with real-time updates
- **Job Type Categorization**: Training Data Generation, Model Training, Backtesting
- **Search & Filtering**: Text search by job name, user, date range, parameters

#### F2: Job Detail View
- **Execution Details**: Full job metadata, parameters, environment configuration
- **Flyte Integration**: Direct links to Flyte UI for detailed workflow inspection
- **Log Viewer**: Integrated log streaming with syntax highlighting and search
- **Resource Usage**: CPU, memory, GPU utilization during job execution
- **Timeline View**: Visual representation of job execution phases

#### F3: Job Logs Integration
- **Real-Time Streaming**: Live log tailing for running jobs
- **Log Search**: Full-text search across all job logs with highlighting
- **Log Export**: Download logs in various formats (txt, json, csv)
- **Error Detection**: Automatic highlighting of errors, warnings, and exceptions
- **Log Persistence**: Searchable historical logs for all completed jobs

### 3.2 Training Dataset Management

#### F4: Dataset Registry
- **Dataset Catalog**: Comprehensive list of all generated training datasets
  - Dataset name, creation date, size, feature count, symbol coverage
  - Source job information (linking back to data generation job)
  - Dataset quality metrics (completeness, distribution statistics)
  - Tags and labels for easy categorization and discovery
- **Dataset Search**: Full-text search by name, tags, symbols, date ranges
- **Dataset Filtering**: Filter by job type, date range, symbol universe, feature counts

#### F5: Dataset Visualization
- **Feature Distribution Analysis**: Histograms and box plots for all features
- **Time Series Visualization**: OHLC charts with technical indicators overlay
- **Data Quality Dashboard**: Missing values, outliers, statistical summaries
- **Symbol Coverage Analysis**: Heatmaps of data availability across symbols and dates
- **Interactive Filtering**: Dynamic filtering by date ranges, symbols, feature values

#### F6: Training Dataset Metadata
- **Generation Parameters**: All parameters used in data generation (symbols, date ranges, features)
- **Data Schema**: Feature definitions, data types, value ranges
- **Quality Metrics**: Completeness percentages, distribution statistics, anomaly detection
- **Lineage Tracking**: Source data references, transformation steps, dependencies
- **Version Control**: Track dataset versions and changes over time

### 3.3 Dataset Comparison Engine

#### F7: Side-by-Side Dataset Comparison
- **Distribution Comparison**: Overlay histograms for feature distributions
- **Statistical Tests**: Kolmogorov-Smirnov tests, Mann-Whitney U tests for distribution differences
- **Correlation Analysis**: Compare feature correlation matrices between datasets
- **Missing Data Analysis**: Compare data completeness patterns
- **Time Series Comparison**: Aligned OHLC charts with indicator overlays

#### F8: Difference Metrics
- **Distribution Distance**: Jensen-Shannon divergence, Wasserstein distance between distributions
- **Feature Drift Scores**: Quantitative measures of feature drift between datasets
- **Coverage Differences**: Symbol and date coverage comparison with gap analysis
- **Quality Score Comparison**: Side-by-side data quality metrics with delta calculations
- **Recommendation Engine**: Automated suggestions for dataset selection based on quality metrics

#### F9: Comparison Export & Reporting
- **Comparison Reports**: PDF/HTML reports with key findings and visualizations
- **Delta Analysis**: Detailed breakdown of differences with statistical significance
- **Recommendation Summary**: Data-driven suggestions for dataset selection
- **Export Capabilities**: Save comparison results in multiple formats

### 3.4 Job-to-Dataset Navigation

#### F10: Workflow Integration
- **Direct Navigation**: One-click navigation from data generation job to created dataset
- **Related Jobs**: Show all jobs related to a specific dataset (generation, training, backtesting)
- **Dependency Graph**: Visual representation of job dependencies and data flow
- **Impact Analysis**: Show downstream jobs affected by dataset changes

#### F11: Cross-Reference System
- **Bidirectional Linking**: Navigate from dataset back to originating job
- **Related Datasets**: Show similar or related datasets based on parameters
- **Usage Tracking**: Show which training and backtest jobs used specific datasets
- **Lineage Visualization**: End-to-end pipeline visualization from data to results

### 3.5 Backtest Analytics

#### F12: Portfolio Performance Analysis
- **Performance Dashboards**: Comprehensive portfolio metrics with interactive charts
- **Risk Analysis**: VaR, drawdown analysis, volatility clustering
- **Attribution Analysis**: Performance breakdown by stocks, sectors, signals
- **Benchmark Comparison**: Compare against market indices and custom benchmarks

#### F13: Model Performance Tracking
- **Prediction Accuracy**: Track support/resistance prediction accuracy over time
- **Model Comparison**: Side-by-side comparison of different model strategies
- **Confidence Calibration**: Analyze prediction confidence vs actual outcomes
- **Model Drift Detection**: Automated detection of performance degradation

#### F14: Interactive Analysis
- **Drill-Down Capabilities**: Click any metric to see detailed breakdown
- **Time Period Analysis**: Zoom into specific periods for detailed analysis
- **Stock-Level Analysis**: Individual stock performance and prediction accuracy
- **Trade-Level Details**: Analyze individual trade decisions and outcomes

### 3.6 Enhanced Dataset Analysis (v3.0)

#### F15: Advanced Sequence Data Tables
- **Filterable Data Tables**: Real-time filtering of training sequences by any column value
- **Sortable Columns**: Click-to-sort functionality on all data columns (date, OHLC, volume, indicators)
- **Row-Level Visualization**: Mini OHLC charts embedded in each table row
- **Dynamic Data Loading**: Efficient loading of large sequence datasets with pagination
- **Search Functionality**: Text-based search across all sequence data fields

#### F16: Feature Distribution Visualizations
- **Multi-Feature Histograms**: Simultaneous display of distributions for close price, volume, ETOP, EBOT
- **Real-Time Updates**: Distribution charts update automatically when table filters are applied
- **Statistical Overlays**: Mean, median, and standard deviation indicators on distribution charts
- **Interactive Filtering**: Click distribution segments to filter main data table
- **Export Capabilities**: Save distribution charts as PNG images

#### F17: Technical Indicator OHLC Charts
- **Real Indicator Values**: Display actual ETOP, EBOT, PLDOT, and EMA values (not ratios)
- **Row-Level Mini Charts**: Individual OHLC visualizations for each sequence in the data table
- **Multi-Indicator Support**: Simultaneous display of price data with technical indicators
- **Color-Coded Indicators**: Visual distinction between different technical indicators
- **Responsive Design**: Charts scale appropriately for table cell display

#### F18: Enhanced User Interface
- **Single Entry Point**: All functionality accessible from port 3000
- **Integrated Navigation**: Seamless transitions between job management and dataset analysis
- **Modern Web Components**: Chart.js integration for professional data visualizations
- **Responsive Layout**: Grid-based layout that adapts to different screen sizes
- **Enhanced Badges**: Visual indicators for Enhanced Analysis, Filterable Tables, and OHLC Charts

### 3.7 Comprehensive Coverage Analytics (v4.0)

#### F19: Coverage Issue Resolution
- **Root Cause Analysis**: Comprehensive explanation of why legacy coverage filters show 4 symbols vs 10K reality
- **Historical Context**: Clear documentation of system evolution from ~50 symbols (2020-2022) to 10,000 symbols (2024-2025)
- **Filter Explanation**: Interactive demonstration showing difference between historical continuity filters and modern coverage
- **Data Quality Metrics**: Real-time assessment of coverage quality for both legacy and modern periods

#### F20: 30-Year Historical Coverage Timeline
- **Interactive Timeline Chart**: Plotly.js-powered visualization showing coverage evolution from 1995-2025
- **Dual-Axis Display**: Symbol counts and record counts on separate scales for clarity
- **Key Milestone Annotations**: Highlighting major system changes and scale-ups
- **Drill-Down Capabilities**: Click any year to see detailed breakdown of symbols and data quality

#### F21: Advanced Gap Analysis Tools
- **Multi-Dimensional Gap Analysis**: By symbol, date range, data type, and vendor source
- **Gap Severity Classification**: Categorize gaps as Low, Medium, or High based on impact
- **Interactive Heatmaps**: Visual representation of coverage density across symbols and time periods
- **Missing Data Identification**: Specific tools for users to identify which dates/symbols need attention
- **Actionable Recommendations**: Data-driven suggestions for improving coverage based on gap analysis

---

## 4. Technical Requirements

### 4.1 Job Management Integration

#### T1: Flyte Integration
- **Workflow Metadata Extraction**: Automatic ingestion of Flyte job metadata
- **Real-Time Status Updates**: Live synchronization with Flyte job status
- **Log Streaming**: Direct integration with Flyte log APIs
- **Authentication**: Secure integration with Flyte authentication system

#### T2: Database Schema
```sql
-- Job runs tracking
CREATE TABLE job_runs (
    job_id UUID PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    job_type job_type_enum NOT NULL, -- 'training_data_gen', 'training', 'backtest'
    user_id VARCHAR(100) NOT NULL,
    flyte_execution_id VARCHAR(255) UNIQUE,
    status job_status_enum NOT NULL, -- 'pending', 'running', 'succeeded', 'failed'
    parameters JSONB NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    duration_seconds INTEGER,
    error_message TEXT,
    resource_usage JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Training datasets tracking
CREATE TABLE training_datasets (
    dataset_id UUID PRIMARY KEY,
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
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Dataset comparisons
CREATE TABLE dataset_comparisons (
    comparison_id UUID PRIMARY KEY,
    dataset_a_id UUID REFERENCES training_datasets(dataset_id),
    dataset_b_id UUID REFERENCES training_datasets(dataset_id),
    comparison_results JSONB NOT NULL,
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### 4.2 Data Pipeline Integration

#### T3: Automatic Dataset Registration
- **Job Completion Hooks**: Automatically register datasets when training data generation jobs complete
- **Metadata Extraction**: Extract feature information, schema, and quality metrics
- **File System Integration**: Track dataset locations and file metadata
- **Validation Pipeline**: Automated data quality checks on registration

#### T4: Real-Time Updates
- **Job Status Synchronization**: Real-time updates from Flyte workflow status
- **Dataset Availability**: Immediate notification when datasets become available
- **Progress Tracking**: Live progress updates for long-running jobs
- **Error Notifications**: Instant alerts for job failures with error details

### 4.3 Performance Requirements

#### T5: Scalability
- **Job Volume**: Support 1000+ concurrent job tracking
- **Dataset Management**: Handle 10,000+ datasets with efficient search
- **Comparison Performance**: Dataset comparisons complete within 30 seconds
- **Dashboard Load Time**: All dashboards load within 3 seconds

#### T6: Data Processing
- **Large Dataset Support**: Handle training datasets up to 10GB
- **Efficient Comparison**: Optimized algorithms for distribution comparison
- **Caching Strategy**: Intelligent caching of computed metrics and comparisons
- **Background Processing**: Heavy computations run asynchronously

---

## 5. User Interface Requirements

### 5.1 Unified Navigation Structure (v3.0)

```
Unified Analytics Platform (Port 3000)
├── Job Management Dashboard (/)
│   ├── Job Stats Overview
│   ├── Interactive Job Table (filtering, sorting)
│   ├── Job Detail Views with Logs
│   └── Direct Flyte Integration Links
├── Dataset Catalog (/datasets)
│   ├── Dataset List with Search/Filter
│   ├── Feature Distribution Visualizations
│   ├── Quality Metrics Dashboard
│   └── Dataset Comparison Tools
├── Enhanced Dataset Detail (/dataset-detail)
│   ├── Filterable Sequence Data Tables
│   ├── Real-Time Feature Distribution Charts
│   ├── Row-Level Mini OHLC Visualizations
│   ├── Technical Indicator Analysis (ETOP/EBOT/PLDOT/EMA)
│   ├── Dynamic Search and Sorting
│   └── Chart.js Powered Visualizations
└── Unified API Layer (/api/v1/)
    ├── Job Management APIs
    ├── Dataset Catalog APIs
    ├── Sequence Access APIs
    └── OHLC Chart Data APIs
```

**Key Navigation Improvements in v3.0:**
- **Single URL**: All features accessible from http://localhost:3000
- **Integrated Tabs/Sections**: Seamless navigation between job management and dataset analysis
- **Unified API**: All endpoints under single API namespace
- **Enhanced Detail Pages**: Row-level analysis with interactive charts

### 5.2 Key User Flows

#### Flow 1: Job Monitoring
1. User visits Jobs Dashboard
2. Filters jobs by type, status, or date range
3. Clicks on specific job for detailed view
4. Reviews logs, parameters, and execution details
5. Links to Flyte UI for advanced workflow inspection

#### Flow 2: Dataset Discovery & Analysis
1. User completes training data generation job
2. System automatically registers dataset in catalog
3. User navigates from job completion to dataset view
4. Reviews dataset visualizations and quality metrics
5. Optionally compares with other datasets

#### Flow 3: Dataset Comparison
1. User selects two datasets from catalog
2. Initiates comparison analysis
3. Reviews side-by-side distribution comparisons
4. Analyzes statistical differences and recommendations
5. Exports comparison report for documentation

### 5.3 Enhanced User Flows (v3.0)

#### Flow 4: Enhanced Dataset Detail Analysis
1. User navigates to unified analytics platform at port 3000
2. Accesses enhanced dataset detail page from dataset catalog
3. Views feature distribution charts for close price, volume, ETOP, and EBOT
4. Uses filter input to search for specific sequences by any criteria
5. Sorts data table by clicking column headers (date, price, indicators)
6. Examines row-level mini OHLC charts with real technical indicator values
7. Analyzes patterns using filterable data with real-time distribution updates

#### Flow 5: Unified Analytics Workflow
1. User accesses single URL (http://localhost:3000)
2. Reviews job management dashboard for recent activity
3. Navigates to dataset catalog to explore generated data
4. Selects dataset for detailed row-level analysis
5. Uses enhanced visualization tools for comprehensive analysis
6. All actions performed within single application interface

---

## 6. Implementation Phases

### Phase 1: Job Management Foundation (4 weeks)
- **Core Job Tracking**: Basic job run recording and status tracking
- **Flyte Integration**: Connect to Flyte APIs for metadata and status
- **Jobs Dashboard**: Basic job list and detail views
- **Log Integration**: Real-time log viewing and search

### Phase 2: Training Dataset Management (4 weeks)
- **Dataset Registry**: Automatic dataset registration and cataloging
- **Dataset Visualization**: Feature distributions and quality dashboards
- **Job-Dataset Linking**: Navigation between jobs and created datasets
- **Metadata Management**: Comprehensive dataset metadata tracking

### Phase 3: Dataset Comparison Engine (3 weeks)
- **Comparison Framework**: Side-by-side dataset comparison infrastructure
- **Distribution Analysis**: Statistical comparison of feature distributions
- **Difference Metrics**: Quantitative measures of dataset differences
- **Comparison Reporting**: Export and documentation of comparison results

### Phase 4: Advanced Analytics & Integration (3 weeks)
- **Backtest Integration**: Connect backtest results with training data lineage
- **Workflow Visualization**: End-to-end pipeline visualization
- **Advanced Filtering**: Complex search and filtering capabilities
- **Performance Optimization**: Caching and performance improvements

---

## 7. Success Criteria

### 7.1 Acceptance Criteria

#### Job Management
- [ ] All Flyte jobs appear in dashboard within 30 seconds of submission
- [ ] Job logs are accessible and searchable with <2 second response time
- [ ] Failed jobs show clear error messages and debugging information
- [ ] Users can navigate to Flyte UI with single click from job detail view

#### Training Dataset Management
- [ ] Datasets are automatically registered within 60 seconds of job completion
- [ ] Dataset visualizations load within 5 seconds for datasets up to 1GB
- [ ] All dataset metadata is captured and searchable
- [ ] Navigation from job to dataset works within 2 clicks

#### Dataset Comparison
- [ ] Comparison between two datasets completes within 30 seconds
- [ ] Statistical significance tests provide clear recommendations
- [ ] Comparison reports are generated in <60 seconds
- [ ] Visual comparisons clearly highlight differences

### 7.2 Key Performance Indicators (KPIs)
- **Job Visibility**: 100% of ML jobs tracked and visible in dashboard
- **Dataset Discovery**: Average time to find relevant dataset <30 seconds
- **Comparison Usage**: 80% of datasets compared before use in training
- **Error Resolution**: 50% reduction in time to debug failed jobs
- **User Adoption**: 90% of ML team actively using platform within 60 days

### 7.3 Platform Consolidation Success Metrics (v3.0)
- **Access Simplification**: 100% of analytics features accessible from single URL
- **Resource Efficiency**: 75% reduction in deployment complexity (1 vs 4+ applications)
- **User Experience**: <5 seconds to navigate between any two analytics features
- **Maintenance**: 60% reduction in deployment and configuration management overhead
- **Training Time**: 50% reduction in user onboarding time due to unified interface

---

## 8. Future Enhancements

### 8.1 Advanced Features (Post-MVP)
- **Automated Data Quality Alerts**: Proactive notifications for data quality issues
- **ML Experiment Tracking**: Integration with MLflow for comprehensive experiment management
- **Collaborative Features**: Comments, annotations, and shared analysis capabilities
- **Advanced Workflow Orchestration**: Custom pipeline creation and management
- **Predictive Job Analytics**: Predict job failure and performance issues

### 8.2 Integration Opportunities
- **CI/CD Integration**: Automated dataset validation in deployment pipelines
- **Slack/Teams Integration**: Job status notifications and alerts
- **Jupyter Integration**: Direct dataset loading into notebook environments
- **Cloud Storage Integration**: Support for S3, GCS, Azure blob storage

---

*This PRD serves as the foundation for building a comprehensive ML analytics platform that transforms fragmented workflow management into a unified, efficient, and insightful experience for the entire ML team.*