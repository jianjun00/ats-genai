# Product Requirements Document (PRD)
# ATS-INTG Daily 1-Minute Bar Backfill System

**Document Version:** 1.0  
**Last Updated:** 2025-01-18  
**Owner:** ATS Platform Team  
**Status:** Active  

---

## 1. Executive Summary

### 1.1 Product Overview
The ATS-INTG Daily 1-Minute Bar Backfill System is a comprehensive automated infrastructure that downloads, processes, and organizes 1-minute OHLCV (Open, High, Low, Close, Volume) bar data for all active stocks and critical ETFs. The system ensures continuous availability of high-frequency market data for algorithmic trading, technical analysis, and model training workflows.

### 1.2 Business Value
- **Real-time Trading Support**: Enables millisecond-level algorithmic trading decisions with up-to-date minute bar data
- **Model Training Enhancement**: Provides high-quality intraday data for ML model training and backtesting
- **Risk Management**: Supports real-time position monitoring and risk assessment
- **Operational Efficiency**: Automates data collection with 99.9% uptime and comprehensive monitoring

### 1.3 Key Metrics
- **Coverage**: 18,331+ active instruments (stocks + ETFs)
- **Data Retention**: 7-day rolling backfill with historical access
- **Processing Volume**: ~2M+ minute bars daily
- **Storage**: ~50GB daily storage requirement
- **SLA**: 99.9% uptime with <5 minute data latency

---

## 2. Product Scope

### 2.1 Core Features

#### 2.1.1 Automated Daily Backfill
- **Last 7 Days Processing**: Rolling 7-day window ensures fresh data availability
- **All Active Instruments**: Covers entire universe of tradeable stocks and ETFs
- **Overwrite Capability**: Safely overwrites existing files to correct data issues
- **Resumable Operations**: Checkpoint-based processing allows graceful recovery

#### 2.1.2 Intelligent File Organization
- **Hierarchical Structure**: `/mnt/d/ats-data/firstrate-data/daily/yyyy/mm/dd/<first_letter>/<symbol>_YYYYMMDD.parquet`
- **Symbol Grouping**: Files organized by first letter for efficient retrieval
- **Date Partitioning**: Natural date-based partitioning for time-series queries
- **Parquet Format**: Compressed columnar format for optimal storage and query performance

#### 2.1.3 Comprehensive Monitoring & Alerting
- **Prometheus Metrics**: Real-time metrics on processing volume, errors, and performance
- **Slack Notifications**: Daily and weekly summaries with actionable insights
- **Health Checks**: Continuous monitoring of system health and data quality
- **Error Tracking**: Detailed error logs with classification and resolution guidance

#### 2.1.4 Instrument Classification System
- **Critical ETFs**: 30+ high-priority ETFs (SPY, QQQ, VTI, etc.) with enhanced processing
- **Active Stocks**: All NYSE, NASDAQ, and major exchange stocks
- **Type-Based Processing**: Optimized workflows based on instrument characteristics
- **Priority Queuing**: Critical instruments processed first during heavy load

### 2.2 Technical Architecture

#### 2.2.1 Data Flow Architecture
```
FirstRate API → Download Agent → Processing Engine → File System → Monitoring
     ↓                ↓               ↓              ↓           ↓
   Rate Limit    Batch Processing  Validation   Parquet Files  Metrics
   Management      & Parallel       & Quality     Organization  & Alerts
```

#### 2.2.2 Container Orchestration
- **ATS-INTG Environment**: Integration testing and production deployment
- **Docker Compose**: Service orchestration with dependency management
- **Cron Scheduling**: Automated job execution with proper timing
- **Health Monitoring**: Container health checks and auto-restart

#### 2.2.3 Storage Architecture
- **Base Path**: `/mnt/d/ats-data/firstrate-data/daily/`
- **Partition Strategy**: Year/Month/Day/Letter hierarchy
- **File Naming**: `{symbol}_{YYYYMMDD}.parquet` convention
- **Storage Backend**: High-performance SSD with daily backup

---

## 3. Functional Requirements

### 3.1 Data Collection Requirements

#### FR-001: Daily Backfill Execution
**Requirement**: System SHALL execute daily backfill at 4:00 AM EST
**Priority**: P0 (Critical)
**Acceptance Criteria**:
- Backfill starts automatically at 4:00 AM EST daily
- Processes last 7 days of data for all active instruments
- Completes processing within 2 hours (6:00 AM EST deadline)
- Handles weekends and holidays appropriately

#### FR-002: Instrument Universe Coverage
**Requirement**: System SHALL process all active stocks and critical ETFs
**Priority**: P0 (Critical)
**Acceptance Criteria**:
- Processes 18,331+ active instruments from intg_instruments table
- Prioritizes 30+ critical ETFs (defined list in configuration)
- Includes all NYSE, NASDAQ, and major exchange symbols
- Updates instrument universe automatically based on database changes

#### FR-003: Data Quality Validation
**Requirement**: System SHALL validate data quality and completeness
**Priority**: P1 (High)
**Acceptance Criteria**:
- Validates OHLCV data for logical consistency
- Rejects files with <10 minute bars per trading day
- Logs data quality issues with instrument and date context
- Maintains data quality metrics in Prometheus

### 3.2 File Organization Requirements

#### FR-004: Hierarchical File Structure
**Requirement**: System SHALL organize files in yyyy/mm/dd/<letter>/<symbol>_YYYYMMDD.parquet format
**Priority**: P0 (Critical)
**Acceptance Criteria**:
- Creates directory structure: year → month → day → first_letter
- Names files as: `{symbol}_{YYYYMMDD}.parquet`
- Groups symbols by first letter (A-Z directories)
- Maintains consistent naming across all dates

#### FR-005: File Overwrite Safety
**Requirement**: System SHALL safely overwrite existing files when needed
**Priority**: P1 (High)
**Acceptance Criteria**:
- Overwrites files for same symbol/date combination
- Logs file overwrite operations with size/timestamp changes
- Maintains atomic file operations to prevent corruption
- Tracks overwrite statistics in daily reports

### 3.3 Monitoring & Alerting Requirements

#### FR-006: Prometheus Metrics Integration
**Requirement**: System SHALL expose comprehensive metrics via Prometheus
**Priority**: P1 (High)
**Acceptance Criteria**:
- Exposes metrics on http://localhost:4080/metrics endpoint
- Tracks symbols processed by instrument type
- Reports minute bars processed by day and type
- Monitors processing errors and performance metrics

#### FR-007: Slack Notification System
**Requirement**: System SHALL send daily and weekly summary reports via Slack
**Priority**: P2 (Medium)
**Acceptance Criteria**:
- Daily summary at 8:00 AM EST with key metrics
- Weekly comprehensive report on Mondays at 9:00 AM EST
- Error notifications for processing failures
- Interactive buttons for manual operations

---

## 4. Non-Functional Requirements

### 4.1 Performance Requirements

#### NFR-001: Processing Throughput
**Requirement**: System SHALL process 2M+ minute bars within 2-hour window
**Priority**: P0 (Critical)
**Metrics**: 
- Target: 16,000+ minute bars/minute processing rate
- Acceptable: 12,000+ minute bars/minute minimum
- Measurement: Prometheus `ats_daily_minute_backfill_total_minute_bars` metric

#### NFR-002: Storage Performance
**Requirement**: System SHALL maintain <5 second file write times for 99% of files
**Priority**: P1 (High)
**Metrics**:
- Target: <2 second average file write time
- Acceptable: <5 second for 99th percentile
- Measurement: Application logs with timing data

#### NFR-003: Memory Usage
**Requirement**: System SHALL operate within 8GB memory limit per container
**Priority**: P1 (High)
**Metrics**:
- Target: <6GB average memory usage
- Acceptable: <8GB maximum memory usage
- Measurement: Docker container metrics

### 4.2 Reliability Requirements

#### NFR-004: System Uptime
**Requirement**: System SHALL maintain 99.9% uptime
**Priority**: P0 (Critical)
**Metrics**:
- Target: 99.95% uptime
- Acceptable: 99.9% minimum uptime
- Measurement: Health check endpoint availability

#### NFR-005: Data Completeness
**Requirement**: System SHALL achieve 95%+ data completeness for all trading days
**Priority**: P0 (Critical)
**Metrics**:
- Target: 98%+ data completeness
- Acceptable: 95%+ minimum completeness
- Measurement: Files processed vs. expected files ratio

#### NFR-006: Error Recovery
**Requirement**: System SHALL recover from transient failures within 5 minutes
**Priority**: P1 (High)
**Metrics**:
- Target: <2 minute recovery time
- Acceptable: <5 minute maximum recovery
- Measurement: Time between failure detection and service restoration

### 4.3 Scalability Requirements

#### NFR-007: Instrument Scaling
**Requirement**: System SHALL support up to 25,000 instruments without performance degradation
**Priority**: P2 (Medium)
**Metrics**:
- Current: 18,331 instruments
- Target: 25,000 instruments capacity
- Measurement: Processing time per instrument consistency

#### NFR-008: Historical Data Scaling
**Requirement**: System SHALL extend lookback window to 30 days without architectural changes
**Priority**: P2 (Medium)
**Metrics**:
- Current: 7-day lookback window
- Target: 30-day capability
- Measurement: Processing time scaling linearly

---

## 5. User Stories & Use Cases

### 5.1 Primary User Stories

#### US-001: Automated Trading System
**As a** quantitative trading algorithm  
**I want** access to the latest 1-minute bar data for all instruments  
**So that** I can make millisecond-level trading decisions with current market information

**Acceptance Criteria**:
- Data available within 5 minutes of market close
- Complete coverage of tradeable universe
- Data quality suitable for algorithmic trading
- Consistent file format and location

#### US-002: Risk Management System
**As a** risk management system  
**I want** real-time access to intraday price movements  
**So that** I can monitor position risk and trigger alerts for limit breaches

**Acceptance Criteria**:
- Minute-level granularity for all positions
- Historical lookback for trend analysis
- Fast query performance for real-time monitoring
- Integration with existing risk calculation frameworks

#### US-003: Research & Model Development
**As a** quantitative researcher  
**I want** historical minute bar data organized by symbol and date  
**So that** I can develop and backtest trading models with high-quality data

**Acceptance Criteria**:
- Easy access to symbol-specific data
- Consistent data format across time periods
- Ability to query specific date ranges
- Data quality metrics and validation

### 5.2 Secondary Use Cases

#### UC-001: Data Quality Investigation
**Actor**: Data Operations Team  
**Goal**: Investigate and resolve data quality issues  
**Scenario**:
1. Operations team receives alert about data quality degradation
2. Team accesses Prometheus metrics to identify affected instruments/dates
3. Team examines specific parquet files for data anomalies
4. Team triggers manual re-processing for affected date range
5. Team verifies data quality restoration via monitoring dashboard

#### UC-002: System Maintenance & Updates
**Actor**: Platform Engineering Team  
**Goal**: Perform system maintenance without data loss  
**Scenario**:
1. Engineering team schedules maintenance window
2. Team stops automated backfill jobs
3. Team performs system updates and configuration changes
4. Team validates system functionality with test runs
5. Team resumes production jobs and monitors for successful operation

---

## 6. Technical Constraints

### 6.1 Infrastructure Constraints

#### TC-001: FirstRate API Limitations
- **Rate Limits**: 5 API calls per minute per instrument
- **Batch Size**: Maximum 1000 minute bars per request
- **Daily Quota**: 100,000 API calls per day
- **Retry Policy**: Exponential backoff with 3 retry attempts

#### TC-002: Storage Constraints
- **Available Space**: 500GB allocated for minute bar storage
- **I/O Performance**: 1000 IOPS minimum for parallel file writes
- **Backup Requirements**: Daily incremental backups to separate storage
- **Retention Policy**: 90-day retention for minute bar files

#### TC-003: Network Constraints
- **Bandwidth**: 100 Mbps minimum for API data downloads
- **Latency**: <100ms to FirstRate API endpoints
- **Reliability**: 99.9% network uptime requirement
- **Security**: All API communications via HTTPS/TLS 1.2+

### 6.2 Operational Constraints

#### TC-004: Maintenance Windows
- **Scheduled Maintenance**: Sunday 2:00-4:00 AM EST weekly
- **Emergency Maintenance**: <2 hour maximum outage
- **Version Updates**: Monthly update cycle with 1-week testing
- **Configuration Changes**: Change control process required

#### TC-005: Monitoring Constraints
- **Log Retention**: 30-day log retention in /logs volume
- **Metrics Retention**: 90-day Prometheus metrics retention
- **Alert Response**: 15-minute response time for critical alerts
- **Reporting**: Weekly reports required for stakeholder review

---

## 7. Success Metrics & KPIs

### 7.1 Data Quality Metrics

| Metric | Target | Measurement | Frequency |
|--------|--------|-------------|-----------|
| Data Completeness | 98%+ | Files processed / Expected files | Daily |
| Data Accuracy | 99.9%+ | OHLCV validation pass rate | Daily |
| Processing Success Rate | 99.5%+ | Successful jobs / Total jobs | Daily |
| Error Rate | <0.5% | Failed files / Total files | Daily |

### 7.2 Performance Metrics

| Metric | Target | Measurement | Frequency |
|--------|--------|-------------|-----------|
| Processing Time | <2 hours | Job completion time | Daily |
| Throughput | 16K+ bars/min | Minute bars processed / Time | Daily |
| Storage Efficiency | 80%+ utilization | Used space / Allocated space | Weekly |
| Memory Usage | <6GB average | Container memory metrics | Continuous |

### 7.3 Operational Metrics

| Metric | Target | Measurement | Frequency |
|--------|--------|-------------|-----------|
| System Uptime | 99.9%+ | Health check availability | Continuous |
| Alert Response Time | <15 minutes | Time to acknowledge alerts | Per incident |
| Recovery Time | <5 minutes | Time to restore service | Per incident |
| SLA Compliance | 99.5%+ | SLA met / Total SLA periods | Monthly |

---

## 8. Dependencies & Assumptions

### 8.1 External Dependencies

#### DEP-001: FirstRate Data Provider
- **Service**: FirstRate API for 1-minute OHLCV data
- **SLA**: 99.9% API uptime
- **Data Quality**: Real-time market data with <5 minute delay
- **Support**: 24/7 technical support for API issues

#### DEP-002: ATS-INTG Database
- **Service**: PostgreSQL database for instrument universe
- **Performance**: <100ms query response time
- **Availability**: 99.99% uptime requirement
- **Data Consistency**: Real-time instrument updates

#### DEP-003: Docker Infrastructure
- **Platform**: Docker Compose orchestration
- **Network**: ats-intg-network with container communication
- **Storage**: Persistent volumes for data and logs
- **Monitoring**: Container health checks and restart policies

### 8.2 Assumptions

#### ASSUMP-001: Market Schedule
- **Assumption**: US market trading hours remain 9:30 AM - 4:00 PM EST
- **Impact**: Processing schedules and data availability windows
- **Risk**: Schedule changes would require job timing updates

#### ASSUMP-002: Data Format Stability
- **Assumption**: FirstRate API maintains consistent OHLCV data format
- **Impact**: Data parsing and validation logic
- **Risk**: Format changes would require code updates

#### ASSUMP-003: Storage Growth
- **Assumption**: Storage requirements grow linearly with instrument count
- **Impact**: Capacity planning and cost estimation
- **Risk**: Non-linear growth could require architecture changes

---

## 9. Risk Assessment & Mitigation

### 9.1 Technical Risks

#### RISK-001: FirstRate API Failures
**Probability**: Medium | **Impact**: High | **Risk Score**: High  
**Description**: FirstRate API outages or rate limiting affecting data collection  
**Mitigation**: 
- Implement exponential backoff retry logic
- Monitor API health and switch to backup data sources
- Maintain 48-hour data buffer for critical operations
- Establish SLA with FirstRate for guaranteed uptime

#### RISK-002: Storage Capacity Exhaustion
**Probability**: Low | **Impact**: High | **Risk Score**: Medium  
**Description**: Running out of storage space for minute bar files  
**Mitigation**:
- Implement automated storage monitoring with 80% threshold alerts
- Establish data archival process for files >90 days old
- Set up automatic storage expansion triggers
- Monitor storage growth trends with capacity forecasting

#### RISK-003: Processing Performance Degradation
**Probability**: Medium | **Impact**: Medium | **Risk Score**: Medium  
**Description**: Increasing instrument universe causing processing delays  
**Mitigation**:
- Implement horizontal scaling with additional worker containers
- Optimize file I/O with parallel processing
- Monitor processing times with automated performance alerts
- Establish performance baselines with regression testing

### 9.2 Operational Risks

#### RISK-004: Data Quality Degradation
**Probability**: Medium | **Impact**: High | **Risk Score**: High  
**Description**: Poor quality data affecting trading decisions and model accuracy  
**Mitigation**:
- Implement comprehensive data validation checks
- Set up automated data quality monitoring with Prometheus metrics
- Establish data quality SLAs with alerting thresholds
- Create manual data correction procedures for quality issues

#### RISK-005: Critical ETF Processing Delays
**Probability**: Low | **Impact**: High | **Risk Score**: Medium  
**Description**: Delays in processing critical ETFs affecting trading operations  
**Mitigation**:
- Implement priority queuing for critical instruments
- Set up dedicated processing pipelines for critical ETFs
- Monitor critical ETF processing times separately
- Establish escalation procedures for critical instrument delays

---

## 10. Implementation Timeline

### 10.1 Phase 1: Core Infrastructure (Week 1-2)
- **Week 1**: Implement daily backfill script and file organization
- **Week 2**: Set up Docker Compose orchestration and basic monitoring

### 10.2 Phase 2: Monitoring & Alerting (Week 3)
- **Week 3**: Integrate Prometheus metrics and Slack notifications
- **Week 3**: Implement health checks and error reporting

### 10.3 Phase 3: Production Deployment (Week 4)
- **Week 4**: Deploy to ATS-INTG environment and validate operations
- **Week 4**: Complete documentation and handover to operations team

### 10.4 Phase 4: Optimization & Scaling (Week 5-6)
- **Week 5**: Performance tuning and optimization
- **Week 6**: Implement additional monitoring and operational procedures

---

## 11. Acceptance Criteria

### 11.1 Functional Acceptance
- [ ] Daily backfill processes all 18,331+ instruments successfully
- [ ] Files organized in correct yyyy/mm/dd/<letter>/<symbol>_YYYYMMDD.parquet format
- [ ] Prometheus metrics exposed on localhost:4080/metrics endpoint
- [ ] Slack notifications sent daily at 8:00 AM EST
- [ ] System processes 7-day lookback window within 2-hour window
- [ ] Data quality validation rejects invalid files appropriately

### 11.2 Performance Acceptance
- [ ] Processing completes within 2 hours (4:00-6:00 AM EST window)
- [ ] System maintains <6GB memory usage per container
- [ ] 98%+ data completeness for all trading days
- [ ] <5 second file write times for 99% of files
- [ ] 16,000+ minute bars processed per minute throughput

### 11.3 Operational Acceptance  
- [ ] 99.9% system uptime over 30-day period
- [ ] Automated recovery from transient failures within 5 minutes
- [ ] Health check endpoints respond within 1 second
- [ ] Error alerts delivered to operations team within 15 minutes
- [ ] Weekly operational reports generated automatically

---

## 12. Appendices

### Appendix A: Critical ETFs List
```
SPY, QQQ, VTI, IWM, EFA, VWO, GLD, SLV, TLT, HYG,
LQD, EEM, XLF, XLK, XLE, XLI, XLV, XLY, XLP, XLU,
VNQ, EWJ, FXI, EWZ, RSX, ARKK, ARKG, ARKW, JETS, ICLN
```

### Appendix B: File Path Examples
```
/mnt/d/ats-data/firstrate-data/daily/2025/01/18/A/AAPL_20250118.parquet
/mnt/d/ats-data/firstrate-data/daily/2025/01/18/S/SPY_20250118.parquet
/mnt/d/ats-data/firstrate-data/daily/2025/01/18/T/TSLA_20250118.parquet
```

### Appendix C: Prometheus Metrics Reference
```
ats_daily_minute_backfill_instruments_processed
ats_daily_minute_backfill_total_minute_bars
ats_daily_minute_backfill_symbols_by_type{type="stock|critical_etf|other_etf"}
ats_daily_minute_backfill_bars_by_type{type="stock|critical_etf|other_etf"}
ats_daily_minute_backfill_symbols_by_letter{letter="A-Z"}
```

---

**Document Control**  
**Version**: 1.0  
**Approved By**: Platform Engineering Team  
**Next Review**: 2025-02-18  
**Classification**: Internal Use