# PRD: ATS Exploratory Data Analysis (EDA) Tool

**Document Version**: 1.0  
**Date**: August 28, 2025  
**Owner**: Data Infrastructure Team  
**Status**: Requirements Definition  

---

## 📋 Executive Summary

The ATS EDA Tool is a comprehensive data exploration and visualization platform designed to provide deep insights into the ATS financial datasets. This tool enables data scientists, quant researchers, and platform engineers to understand data quality, discover patterns, and validate datasets through interactive visualizations and statistical analysis.

### Key Value Propositions
- **Data Quality Assurance**: Identify missing data, outliers, and inconsistencies across 30+ years of financial data
- **Pattern Discovery**: Uncover market trends, correlations, and anomalies in multi-vendor datasets  
- **Dataset Validation**: Compare data quality and coverage across vendors (Polygon, Tiingo, EODHD)
- **Research Acceleration**: Rapid hypothesis testing and feature engineering for ML models
- **Operational Insights**: Monitor data collection health and identify collection gaps

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

### 6. **Data Dashboard - Updated Interface Design**
- **FR-6.1**: Left navigation panel with dataset selection and filtering controls
- **FR-6.2**: Right content area with two scrollable sections:
  - **Top Section**: All column distributions display (no columns hidden)
  - **Bottom Section**: Paged data table with scrollable rows
- **FR-6.3**: Dataset size information in dropdown selection (e.g., "EODHD Daily Prices (4.4M rows, 7 cols)")
- **FR-6.4**: Export capabilities (CSV, Excel, JSON) from data table
- **FR-6.5**: Statistical summary cards (mean, median, std, min, max) with null-safe display
- **FR-6.6**: Data quality indicators (null count, unique values, data types)
- **FR-6.7**: Interactive pagination controls with Previous/Next buttons
- **FR-6.8**: Responsive layout supporting simultaneous visualization viewing and data browsing
- **FR-6.9**: **String Type Handling**: Intelligent column type detection and specialized handling:
  - **String Detection**: Columns named `id`, `symbol`, `name`, `title`, `url`, `description` or with VARCHAR/TEXT types treated as strings
  - **Visualization Exclusion**: String columns excluded from distribution charts, show "String column - available in filters" message
  - **Partial String Matching**: Text search filters with debounced input (500ms delay) for real-time filtering
  - **Type Labeling**: Clear column type indication (numeric/categorical/string) in both filters and visualizations

### 7. **Analytics and Insights**
- **FR-7.1**: Automated data quality scoring
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
   - `dev_daily_prices_polygon_30year` (666K records, 849 symbols)
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