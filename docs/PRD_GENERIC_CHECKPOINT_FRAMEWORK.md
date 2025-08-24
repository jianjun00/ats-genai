# PRD: Generic Checkpoint Framework for Long-Running Jobs

## Executive Summary

The Generic Checkpoint Framework provides a standardized, fault-tolerant system for running long-duration data processing jobs in the ATS platform. This framework enables automatic job resumption, progress tracking, and failure recovery for any type of iterative processing job.

## Problem Statement

### Current Issues
1. **Job Failures**: Long-running jobs (30-year price population) fail without recovery mechanism
2. **Resource Waste**: Failed jobs restart from beginning, wasting computation time
3. **Vendor-Specific Code**: Each data vendor requires custom job implementation
4. **No Progress Visibility**: Limited insight into job progress and bottlenecks
5. **Manual Intervention**: Failed jobs require manual restart and monitoring

### Business Impact
- **Cost**: Wasted compute resources from job restarts (~$500/month in K8s costs)
- **Time**: 30-year price jobs take 12+ hours; failures mean complete restart
- **Reliability**: Production data pipelines vulnerable to single-point failures
- **Scalability**: Cannot easily add new data vendors or job types

## Solution Overview

The Generic Checkpoint Framework provides:

### Core Capabilities
1. **Automatic Resumption**: Jobs resume from last checkpoint after failure
2. **Progress Tracking**: Real-time visibility into job progress and statistics
3. **Vendor Agnostic**: Single framework supports any data vendor or job type
4. **Fault Tolerance**: Graceful handling of network, API, and database failures
5. **Resource Optimization**: No duplicate work after job restarts

### Key Components
1. **CheckpointManager**: Handles job state persistence and recovery
2. **CheckpointableJob**: Abstract interface for implementing resumable jobs
3. **GenericJobRunner**: Orchestrates job execution with checkpoint support
4. **Database Schema**: Centralized job state and progress tracking

## Requirements

### Functional Requirements

#### FR1: Job State Persistence
- Store job configuration, progress, and current position
- Support multiple iteration types: instrument, date, instrument-date, custom
- Track processed vs pending items with error details
- Maintain job metadata and execution statistics

#### FR2: Automatic Resume Capability  
- Detect incomplete jobs on startup
- Resume from last successful checkpoint position
- Skip already processed items to avoid duplicates
- Support pause/resume operations

#### FR3: Progress Monitoring
- Real-time statistics: completed/failed/pending counts
- Performance metrics: processing rate, error rate
- Individual item status tracking
- Job execution timeline and duration

#### FR4: Error Handling & Retry Logic
- Automatic retry for transient failures
- Configurable retry limits and backoff strategies
- Failed item isolation (doesn't block job progress)
- Comprehensive error logging and categorization

#### FR5: Vendor Abstraction
- Abstract base class for vendor-specific implementations
- Standardized interfaces for data fetching and storage
- Configurable rate limiting per vendor requirements
- Support for different API authentication methods

### Non-Functional Requirements

#### NFR1: Performance
- Process 10,000+ instruments with <1% overhead
- Checkpoint updates must not impact processing throughput
- Batch processing for database efficiency
- Memory usage <4GB for large jobs

#### NFR2: Reliability
- 99.9% checkpoint accuracy (no lost progress)
- Graceful handling of database connection failures
- Atomic checkpoint updates (all-or-nothing)
- Job state consistency under concurrent access

#### NFR3: Scalability
- Support 100+ concurrent jobs
- Handle jobs with 100,000+ items
- Horizontal scaling across multiple pods
- Efficient database schema for large datasets

#### NFR4: Maintainability
- Clear separation between framework and job logic
- Comprehensive logging for debugging
- Configuration-driven job parameters
- Unit and integration test coverage >90%

## Technical Architecture

### Database Schema

#### dev_job_runs Table
```sql
- id: Primary key
- job_id: Unique identifier for job instance
- job_name: Job type/template name
- vendor: Data vendor (tiingo, fmp, polygon, etc.)
- iteration_type: instrument, date, instrument_date, custom
- status: pending, in_progress, completed, failed, paused
- current_position: JSON serialized checkpoint position
- processed_count: Number of successfully processed items
- error_count: Number of failed items
- total_items: Total items to process
- last_successful_item: Last successfully processed item
- configuration: Job configuration as JSONB
- metadata: Custom job metadata
- timestamps: created_at, updated_at, started_at, completed_at
```

#### dev_job_progress Table
```sql
- id: Primary key
- job_id: Reference to job run
- item_key: Identifier for processing item (symbol, date, etc.)
- item_type: Type of item being processed
- status: pending, in_progress, completed, failed
- records_processed: Count of records stored for this item
- error_message: Error details for failed items
- retry_count: Number of retry attempts
- timestamps: created_at, started_at, completed_at
```

### Class Hierarchy

#### CheckpointableJob (Abstract Base Class)
```python
- get_iteration_items() -> List[Any]: Get all items to process
- process_item(item, session) -> (result, error): Process single item
- store_result(item, result) -> int: Store processing result
- serialize_position(position) -> str: Checkpoint serialization
- deserialize_position(str) -> Any: Checkpoint deserialization
```

#### Concrete Implementations
- **TiingoJob**: 30-year price history collection
- **FMPJob**: Financial Modeling Prep data collection
- **PolygonJob**: Real-time and historical data sync
- **CustomJob**: User-defined processing logic

### Iteration Types

#### Instrument-Based Iteration
- Process all symbols from dev_instruments table
- Checkpoint: last processed symbol
- Use case: Price collection, fundamental data updates

#### Date-Based Iteration  
- Process date ranges (daily, monthly, yearly)
- Checkpoint: last processed date
- Use case: Historical backfills, time-series updates

#### Instrument-Date Iteration
- Process symbol-date combinations
- Checkpoint: (symbol, date) tuple
- Use case: Granular historical updates, data quality checks

#### Custom Iteration
- User-defined iteration logic
- Checkpoint: custom position object
- Use case: Complex processing workflows, multi-step pipelines

## User Stories

### US1: Data Engineer - Resume Failed Job
**As a** data engineer  
**I want to** automatically resume a failed 30-year price collection job  
**So that** I don't lose 8 hours of processing progress

**Acceptance Criteria:**
- Job resumes from last successful symbol
- No duplicate data is collected  
- Progress statistics are accurate
- Job completes successfully after resume

### US2: Operations Team - Monitor Job Progress
**As an** operations team member  
**I want to** see real-time progress of long-running jobs  
**So that** I can estimate completion time and identify bottlenecks

**Acceptance Criteria:**
- Live progress percentage and ETA
- Processing rate (items/minute)
- Error rate and failed item details
- Resource utilization metrics

### US3: Platform Developer - Implement New Vendor
**As a** platform developer  
**I want to** easily add a new data vendor job  
**So that** I can leverage existing checkpoint infrastructure

**Acceptance Criteria:**
- Implement only vendor-specific logic (3 methods)
- Automatic checkpoint and retry handling
- Standard configuration options
- Built-in logging and monitoring

### US4: Production Support - Handle Job Failures
**As a** production support engineer  
**I want to** quickly recover from job failures  
**So that** data pipelines remain reliable and up-to-date

**Acceptance Criteria:**
- Jobs auto-resume after pod restarts
- Clear error messages for failed items
- Manual pause/resume capability
- Historical job execution logs

## Implementation Plan

### Phase 1: Core Framework (Week 1)
- [x] Design database schema for job tracking
- [x] Implement CheckpointManager class
- [x] Create CheckpointableJob abstract base class
- [x] Build GenericJobRunner orchestration logic
- [x] Unit tests for core framework components

### Phase 2: Vendor Implementations (Week 2)
- [x] Migrate Tiingo job to new framework
- [x] Migrate FMP job to new framework
- [ ] Create Polygon job implementation
- [ ] Integration tests for all vendor jobs

### Phase 3: Monitoring & Operations (Week 3)
- [ ] Job progress API endpoints
- [ ] Grafana dashboards for job monitoring
- [ ] Alerting for job failures and SLA breaches
- [ ] Operations runbook and troubleshooting guide

### Phase 4: Advanced Features (Week 4)
- [ ] Job scheduling and dependency management
- [ ] Performance optimization and batch processing
- [ ] Multi-pod job distribution
- [ ] Configuration management UI

## Success Metrics

### Reliability Metrics
- **Job Success Rate**: >99% for production jobs
- **Recovery Time**: <5 minutes for automatic resume
- **Data Accuracy**: 100% consistency, zero duplicates
- **Checkpoint Overhead**: <2% impact on job performance

### Operational Metrics  
- **Time to Market**: 50% faster new vendor integration
- **Resource Utilization**: 30% reduction in wasted compute
- **MTTR**: <15 minutes for job failure resolution
- **Developer Productivity**: 80% less custom job code

### Business Metrics
- **Cost Savings**: $200/month in reduced compute waste
- **Pipeline Reliability**: 99.9% data availability SLA
- **Feature Velocity**: 2x faster data vendor additions
- **Operational Overhead**: 50% reduction in manual interventions

## Risk Assessment

### Technical Risks
1. **Database Performance**: Large job tables impact query performance
   - **Mitigation**: Partitioning, archiving, proper indexing
2. **Memory Consumption**: Large job state objects cause OOM errors
   - **Mitigation**: Streaming checkpoints, memory profiling
3. **Concurrency Issues**: Multiple pods accessing same job state
   - **Mitigation**: Database locks, job ownership model

### Operational Risks
1. **Migration Complexity**: Existing jobs need framework migration
   - **Mitigation**: Gradual rollout, backward compatibility
2. **Learning Curve**: Team needs to adopt new patterns
   - **Mitigation**: Documentation, examples, training sessions
3. **Framework Bugs**: Core bugs impact all jobs
   - **Mitigation**: Comprehensive testing, gradual rollout

## Future Enhancements

### Advanced Scheduling
- Cron-like job scheduling
- Job dependencies and chaining
- Resource-aware job placement
- Priority-based job queuing

### Distributed Processing
- Multi-pod job distribution  
- Work stealing for load balancing
- Cross-region job replication
- Auto-scaling based on job queue

### Machine Learning Integration
- Predictive job failure detection
- Optimal batch size recommendations
- Resource requirement estimation
- Performance anomaly detection

### Monitoring & Analytics
- Real-time job performance dashboards
- Historical job execution analysis
- Cost optimization recommendations
- Capacity planning insights

## Conclusion

The Generic Checkpoint Framework transforms the ATS platform's data processing capabilities by providing a robust, scalable foundation for long-running jobs. This investment in infrastructure will pay dividends through improved reliability, reduced operational overhead, and faster feature development.

The framework's vendor-agnostic design ensures it can support current and future data sources, while its comprehensive checkpoint system eliminates the risk of data loss and computation waste. With proper implementation and adoption, this framework will become the backbone of ATS's data ingestion and processing pipelines.

---

**Document Version**: 1.0  
**Last Updated**: 2025-08-23  
**Author**: Claude (ATS Platform Team)  
**Status**: Implementation in Progress