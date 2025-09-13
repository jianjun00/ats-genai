# 🔄 **DATA QUALITY CONSOLIDATION STRATEGY**

## 🎯 **EXECUTIVE SUMMARY**

**Goal**: Consolidate monitoring, issue lifecycle management under unified data quality framework with consistent patterns, shared code, and streamlined operations.

**Approach**: Refactor existing systems into domain-based services under the data quality agent orchestration layer.

---

## 📊 **CURRENT STATE ASSESSMENT**

### **System Inventory:**

| System | Purpose | Strengths | Redundancies |
|--------|---------|-----------|--------------|
| **Data Quality Agent** | Intelligent issue resolution with MCP tools | Agentic patterns, learning, automation | ❌ Limited to basic quality scans |
| **Coverage Monitoring** | Gap detection & backfill prioritization | Real-time dashboards, Slack alerts | ❌ Separate database schema |
| **Service Architecture** | Clean business logic separation | DI container, testing patterns | ❌ Not integrated with monitoring |

### **Key Problems to Solve:**
- 🔴 **Fragmented monitoring** - Multiple systems scanning same data
- 🔴 **Duplicate alerting** - Different notification channels
- 🔴 **Inconsistent resolution** - No unified issue lifecycle
- 🔴 **Code duplication** - Similar patterns across systems

---

## 🏗️ **UNIFIED ARCHITECTURE DESIGN**

### **Three-Layer Consolidation:**

```
┌─────────────────────────────────────────────────────┐
│                🤖 DATA QUALITY AGENT LAYER          │
│  Orchestration, Learning, Decision Making, MCP      │
└─────────────────────────────────────────────────────┘
                           │
┌─────────────────────────────────────────────────────┐
│            📊 DATA QUALITY SERVICE LAYER             │
│    Coverage, Validation, Gaps, Metrics, Alerts     │
└─────────────────────────────────────────────────────┘
                           │
┌─────────────────────────────────────────────────────┐
│               🗄️ UNIFIED DATA LAYER                  │
│        Consolidated Schema, DAOs, Repositories      │
└─────────────────────────────────────────────────────┘
```

### **Core Principle: Coverage Monitoring as Data Quality Domain**

**Coverage = Data Quality Sub-Domain**
- Coverage gaps → Data quality issues
- Priority scoring → Issue classification  
- Backfill orchestration → Issue resolution
- Dashboard metrics → Quality reporting

---

## 🔧 **REFACTORING PLAN: 4-PHASE APPROACH**

### **PHASE 1: SERVICE INTEGRATION (Week 1)**

#### **Create Data Quality Service Container**

```python
# src/domains/data_quality/services/config/data_quality_service_container.py

class DataQualityServiceContainer:
    """Unified DI container for all data quality concerns"""
    
    async def initialize(self):
        # Consolidate database connections
        self.db_pool = await self._create_unified_pool()
        
        # Register core services
        self.coverage_service = await self._create_coverage_service()
        self.validation_service = await self._create_validation_service()
        self.gap_detection_service = await self._create_gap_detection_service()
        self.alert_service = await self._create_alert_service()
        self.metrics_service = await self._create_metrics_service()
        
        # Register MCP tools
        self.mcp_tools = await self._register_mcp_tools()
        
        # Register data quality agent
        self.agent = await self._create_data_quality_agent()
```

#### **Unified Data Quality Service Interface**

```python
# src/domains/data_quality/services/interfaces/data_quality_service_interface.py

class DataQualityServiceInterface(ABC):
    """Single interface for all data quality operations"""
    
    @abstractmethod
    async def scan_coverage(self, request: CoverageScanRequest) -> CoverageScanResult:
        """Unified coverage scanning (replaces separate monitor scans)"""
    
    @abstractmethod
    async def detect_issues(self, request: IssueDetectionRequest) -> List[DataQualityIssue]:
        """Unified issue detection (coverage gaps + validation issues)"""
    
    @abstractmethod
    async def classify_issue(self, issue: DataQualityIssue) -> IssueClassification:
        """Intelligent issue classification with agent patterns"""
    
    @abstractmethod
    async def resolve_issue(self, issue_id: str, strategy: ResolutionStrategy) -> ResolutionResult:
        """Unified resolution workflow (backfill + validation + alerts)"""
    
    @abstractmethod
    async def get_quality_dashboard_data(self) -> QualityDashboardData:
        """Single endpoint for dashboard (coverage + validation + gaps)"""
```

### **PHASE 2: SCHEMA CONSOLIDATION (Week 2)**

#### **Unified Data Quality Schema**

```sql
-- Consolidate coverage_monitoring_schema.sql + existing quality tables

-- 1. Core Issues Table (replaces both dev_coverage_gaps + quality issues)
CREATE TABLE IF NOT EXISTS dev_data_quality_issues (
    id SERIAL PRIMARY KEY,
    issue_type VARCHAR(50) NOT NULL,     -- 'coverage_gap', 'validation_error', 'stale_data', etc.
    issue_category VARCHAR(20) NOT NULL, -- 'coverage', 'validation', 'consistency' 
    vendor VARCHAR(20) NOT NULL,
    data_type VARCHAR(20) NOT NULL,      -- 'daily_prices', 'minute_bars'
    symbol VARCHAR(20) NOT NULL,
    affected_date_start DATE NOT NULL,
    affected_date_end DATE NOT NULL,
    severity VARCHAR(20) NOT NULL,       -- 'critical', 'high', 'medium', 'low'
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'in_progress', 'resolved', 'escalated'
    
    -- Classification fields
    complexity VARCHAR(20),              -- 'simple', 'medium', 'complex'
    priority_score INTEGER NOT NULL,
    estimated_effort_minutes INTEGER,
    
    -- Resolution tracking
    resolution_strategy VARCHAR(30),     -- 'auto_resolve', 'human_assisted', 'escalate'
    assigned_agent VARCHAR(50),
    workflow_id UUID,
    
    -- Metadata
    issue_metadata JSONB,               -- Flexible issue-specific data
    resolution_metadata JSONB,          -- Resolution details
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMP
);

-- 2. Quality Metrics Table (consolidates coverage + validation metrics)
CREATE TABLE IF NOT EXISTS dev_data_quality_metrics (
    id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    vendor VARCHAR(20) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    metric_category VARCHAR(30) NOT NULL, -- 'coverage', 'timeliness', 'consistency', 'accuracy'
    
    -- Unified metrics
    total_expected INTEGER,
    total_actual INTEGER,
    quality_score DECIMAL(5,2),         -- 0-100 unified score
    coverage_percentage DECIMAL(5,2),   -- Coverage-specific
    completeness_score DECIMAL(5,2),    -- Validation-specific
    timeliness_score DECIMAL(5,2),      -- Validation-specific
    
    issues_detected INTEGER DEFAULT 0,
    issues_auto_resolved INTEGER DEFAULT 0,
    issues_escalated INTEGER DEFAULT 0,
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(metric_date, vendor, data_type, metric_category)
);

-- 3. Agent Operations Log (consolidates workflow tracking)
CREATE TABLE IF NOT EXISTS dev_data_quality_agent_operations (
    id SERIAL PRIMARY KEY,
    operation_type VARCHAR(50) NOT NULL, -- 'scan', 'classify', 'resolve', 'escalate'
    agent_id VARCHAR(100) NOT NULL,
    issue_id INTEGER REFERENCES dev_data_quality_issues(id),
    
    operation_input JSONB,               -- Input parameters
    operation_output JSONB,              -- Results/response
    operation_status VARCHAR(20) NOT NULL, -- 'success', 'failure', 'partial'
    
    -- Performance tracking
    execution_time_ms INTEGER,
    confidence_score DECIMAL(5,2),
    
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP
);
```

### **PHASE 3: AGENT INTEGRATION (Week 3)**

#### **Enhanced Data Quality Agent with Coverage Monitoring**

```python
# src/domains/data_quality/services/impl/enhanced_data_quality_agent.py

class EnhancedDataQualityAgent(DataQualityAgent):
    """Enhanced agent with coverage monitoring capabilities"""
    
    def __init__(self, service_container: DataQualityServiceContainer):
        super().__init__()
        self.services = service_container
        
        # Add coverage-specific MCP tools
        self.mcp_tools.update({
            "coverage_scanner": CoverageScannerTool(service_container),
            "gap_detector": GapDetectorTool(service_container),
            "backfill_orchestrator": BackfillOrchestratorTool(service_container),
            "coverage_validator": CoverageValidatorTool(service_container)
        })
    
    async def _detect_quality_issues(self) -> List[DataQualityIssue]:
        """Enhanced detection combining coverage + validation"""
        
        issues = []
        
        # 1. Coverage gap detection (from our monitoring system)
        coverage_issues = await self.services.coverage_service.detect_coverage_gaps(
            lookback_days=1
        )
        
        # 2. Validation issues (from existing quality scans)
        validation_issues = await self.services.validation_service.scan_validation_issues(
            table_names=["dev_daily_prices", "dev_minute_bars"],
            date_range={"days_back": 1}
        )
        
        # 3. Convert to unified issue format
        for coverage_issue in coverage_issues:
            issues.append(DataQualityIssue(
                issue_type="coverage_gap",
                issue_category="coverage",
                vendor=coverage_issue.vendor,
                data_type=coverage_issue.data_type,
                symbol=coverage_issue.symbol,
                affected_date_start=coverage_issue.gap_start_date,
                affected_date_end=coverage_issue.gap_end_date,
                severity=self._calculate_severity(coverage_issue),
                metadata={"gap_days": coverage_issue.gap_days}
            ))
        
        for validation_issue in validation_issues:
            issues.append(DataQualityIssue(
                issue_type=validation_issue.issue_type,
                issue_category="validation",
                vendor=validation_issue.vendor,
                data_type=validation_issue.data_type,
                symbol=validation_issue.symbol,
                severity=validation_issue.severity,
                metadata=validation_issue.details
            ))
        
        return issues
    
    async def _make_resolution_decision(self, issue: DataQualityIssue, classification: IssueClassification) -> AgentDecision:
        """Enhanced decision making for coverage + validation issues"""
        
        if issue.issue_category == "coverage":
            # Use our coverage monitoring logic
            if issue.issue_type == "coverage_gap":
                return AgentDecision(
                    action="trigger_backfill",
                    reasoning=f"Coverage gap for {issue.symbol} can be resolved with backfill",
                    confidence=0.9,
                    alternatives=["manual_investigation", "mark_as_expected"],
                    risk_assessment="low",
                    human_review_required=classification.complexity != IssueComplexity.SIMPLE
                )
        
        elif issue.issue_category == "validation":
            # Use existing validation logic
            return await super()._make_resolution_decision(issue, classification)
        
        # Fallback to base agent logic
        return await super()._make_resolution_decision(issue, classification)
```

### **PHASE 4: DASHBOARD & API CONSOLIDATION (Week 4)**

#### **Unified Data Quality API**

```python
# src/interfaces/rest_api/unified_data_quality_api.py

class UnifiedDataQualityAPI:
    """Single API endpoint for all data quality concerns"""
    
    def __init__(self, service_container: DataQualityServiceContainer):
        self.services = service_container
    
    @router.get("/data-quality/dashboard")
    async def get_dashboard_data(self) -> DataQualityDashboardResponse:
        """Unified dashboard data (coverage + validation + agent status)"""
        
        # Get coverage metrics (from our monitoring system)
        coverage_data = await self.services.coverage_service.get_coverage_summary()
        
        # Get validation metrics (from existing quality system)
        validation_data = await self.services.validation_service.get_validation_summary()
        
        # Get agent status and active workflows
        agent_status = await self.services.agent.get_agent_status()
        
        # Get recent issues across all categories
        recent_issues = await self.services.get_recent_issues(hours=24)
        
        return DataQualityDashboardResponse(
            coverage_metrics=coverage_data,
            validation_metrics=validation_data,
            agent_status=agent_status,
            recent_issues=recent_issues,
            overall_quality_score=self._calculate_unified_score(coverage_data, validation_data)
        )
    
    @router.get("/data-quality/issues")
    async def get_issues(
        self, 
        category: Optional[str] = None,  # 'coverage', 'validation', 'all'
        severity: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[DataQualityIssueResponse]:
        """Unified issue listing with filtering"""
        
        return await self.services.get_issues(
            category=category,
            severity=severity, 
            status=status,
            limit=limit
        )
    
    @router.post("/data-quality/issues/{issue_id}/resolve")
    async def resolve_issue(
        self,
        issue_id: int,
        resolution_request: IssueResolutionRequest
    ) -> IssueResolutionResponse:
        """Unified issue resolution (coverage backfill + validation fixes)"""
        
        return await self.services.resolve_issue(issue_id, resolution_request.strategy)
```

#### **Enhanced Dashboard with Unified View**

```javascript
// Enhanced dashboard combining coverage + validation in single interface

const DataQualityDashboard = {
    data: {
        coverageMetrics: null,
        validationMetrics: null,
        agentStatus: null,
        recentIssues: null,
        overallScore: 0
    },
    
    async fetchUnifiedData() {
        const response = await fetch('/data-quality/dashboard');
        const data = await response.json();
        
        this.data = data;
        this.renderUnifiedDashboard();
    },
    
    renderUnifiedDashboard() {
        // Single dashboard with tabs for coverage/validation/agent
        this.renderQualityScoreCard();      // Overall quality score
        this.renderCoverageMetrics();       // Our coverage monitoring
        this.renderValidationMetrics();     // Existing validation
        this.renderAgentStatus();           // Agent operations
        this.renderIssuesList();           // Unified issues across categories
        this.renderResolutionWorkflows();  // Active agent workflows
    }
};
```

---

## 🔄 **CODE SHARING & REUSE PATTERNS**

### **Shared Data Transfer Objects**

```python
# src/domains/data_quality/shared/dtos.py

@dataclass
class DataQualityIssue:
    """Unified issue DTO for coverage gaps + validation errors"""
    issue_type: str          # 'coverage_gap', 'missing_data', 'stale_data', 'extreme_value'
    issue_category: str      # 'coverage', 'validation', 'consistency'
    vendor: str
    data_type: str
    symbol: str
    severity: str
    metadata: Dict[str, Any]

@dataclass
class QualityMetric:
    """Unified metric DTO for coverage + validation scores"""
    metric_name: str         # 'coverage_percentage', 'completeness_score', 'timeliness_score'
    metric_category: str     # 'coverage', 'validation'
    value: float
    threshold: float
    status: str              # 'healthy', 'warning', 'critical'

@dataclass
class ResolutionWorkflow:
    """Unified workflow DTO for all resolution types"""
    workflow_id: str
    issue_type: str
    resolution_strategy: str # 'auto_backfill', 'validation_fix', 'manual_review'
    status: str
    metadata: Dict[str, Any]
```

### **Shared Database Operations**

```python
# src/domains/data_quality/shared/repositories.py

class UnifiedDataQualityRepository:
    """Single repository for all data quality database operations"""
    
    async def insert_issue(self, issue: DataQualityIssue) -> int:
        """Insert issue into unified issues table"""
        
    async def update_issue_status(self, issue_id: int, status: str, metadata: Dict = None) -> bool:
        """Update issue status with optional metadata"""
    
    async def get_issues(self, filters: IssueFilters) -> List[DataQualityIssue]:
        """Get issues with unified filtering across categories"""
    
    async def record_metric(self, metric: QualityMetric) -> bool:
        """Record metric in unified metrics table"""
    
    async def get_metrics_summary(self, categories: List[str]) -> Dict[str, Any]:
        """Get metrics summary across multiple categories"""
```

### **Shared Alert System**

```python
# src/domains/data_quality/shared/alerting.py

class UnifiedAlertManager:
    """Single alert manager for coverage + validation + agent alerts"""
    
    async def send_issue_alert(self, issue: DataQualityIssue, alert_config: AlertConfig):
        """Send alert for any type of data quality issue"""
        
        if issue.issue_category == "coverage":
            alert_message = self._format_coverage_alert(issue)
        elif issue.issue_category == "validation":
            alert_message = self._format_validation_alert(issue)
        else:
            alert_message = self._format_generic_alert(issue)
        
        await self._send_to_configured_channels(alert_message, alert_config)
    
    def _format_coverage_alert(self, issue: DataQualityIssue) -> AlertMessage:
        """Format coverage gap alert using our existing logic"""
        
    def _format_validation_alert(self, issue: DataQualityIssue) -> AlertMessage:
        """Format validation error alert"""
```

---

## 📊 **UNIFIED MONITORING & METRICS**

### **Single Quality Score Algorithm**

```python
def calculate_unified_quality_score(coverage_metrics: Dict, validation_metrics: Dict) -> float:
    """Calculate single quality score across coverage + validation"""
    
    # Coverage component (40% weight)
    coverage_score = (
        coverage_metrics['coverage_percentage'] * 0.6 +           # Data availability  
        coverage_metrics['timeliness_percentage'] * 0.4           # Data freshness
    ) * 0.4
    
    # Validation component (60% weight) 
    validation_score = (
        validation_metrics['completeness_score'] * 0.3 +          # No missing values
        validation_metrics['consistency_score'] * 0.3 +           # No contradictions
        validation_metrics['accuracy_score'] * 0.4                # Values within range
    ) * 0.6
    
    unified_score = coverage_score + validation_score
    
    return min(100.0, max(0.0, unified_score))
```

### **Shared Prometheus Metrics**

```python
# Consolidate Prometheus exports from both systems
UNIFIED_METRICS = [
    "ats_data_quality_overall_score",           # New unified score
    "ats_data_coverage_percentage",             # From coverage monitoring
    "ats_data_validation_completeness_score",   # From validation system
    "ats_data_quality_issues_total",           # Combined issue count
    "ats_data_quality_agent_operations_total", # Agent activity
    "ats_data_backfill_operations_total",      # Resolution activity
]
```

---

## 🎯 **SUCCESS METRICS & VALIDATION**

### **Consolidation Success Criteria:**

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Code Duplication** | 3 separate monitoring systems | 1 unified service | -67% complexity |
| **Database Tables** | 15+ scattered tables | 5 consolidated tables | -70% schema overhead |
| **API Endpoints** | 25+ monitoring endpoints | 8 unified endpoints | -68% API surface |
| **Alert Channels** | 3 separate alert systems | 1 unified alert manager | -67% operational overhead |
| **Dashboard Maintenance** | 3 separate dashboards | 1 consolidated dashboard | -67% UI maintenance |

### **Operational Benefits:**

- **🔄 Single Monitoring Cycle**: Agent orchestrates all scans (coverage + validation)
- **📊 Unified Metrics**: One quality score combining coverage + validation  
- **🚨 Consolidated Alerts**: Single Slack channel for all data quality issues
- **🎯 Consistent Resolution**: Same workflow for gaps, errors, and validation failures
- **📈 Shared Learning**: Agent learns from coverage AND validation resolution patterns

---

## 🚀 **IMPLEMENTATION TIMELINE**

### **Week 1: Service Foundation**
- ✅ Create DataQualityServiceContainer with DI
- ✅ Define unified interfaces and DTOs
- ✅ Build shared repository patterns
- ✅ Test service integration

### **Week 2: Schema Consolidation** 
- ✅ Create unified data quality schema
- ✅ Migrate existing coverage monitoring data
- ✅ Migrate existing validation data  
- ✅ Test unified database operations

### **Week 3: Agent Enhancement**
- ✅ Enhance agent with coverage monitoring capabilities
- ✅ Integrate coverage MCP tools
- ✅ Test unified issue detection and resolution
- ✅ Validate agent learning across both domains

### **Week 4: API & Dashboard**
- ✅ Build unified data quality API
- ✅ Consolidate dashboard with unified view
- ✅ Test end-to-end workflows
- ✅ Validate Prometheus metrics integration

---

## 🏆 **LONG-TERM VISION: COMPREHENSIVE DATA QUALITY PLATFORM**

### **Post-Consolidation Roadmap:**

1. **Q1**: Extend to market data quality (prices, volumes, corporate actions)
2. **Q2**: Add news data quality monitoring (sentiment, timeliness, duplicates)  
3. **Q3**: Integrate ML model data quality (feature drift, target leakage)
4. **Q4**: Build data lineage tracking across entire platform

### **Enterprise Benefits:**
- **📊 Executive Dashboard**: Single view of platform data health
- **🔄 Automated Operations**: 90% of data issues resolved automatically
- **📈 Predictive Quality**: ML-powered quality forecasting
- **🎯 ROI Measurement**: Data quality ROI tracking and optimization

---

This consolidation strategy transforms our fragmented monitoring systems into a **unified, intelligent, and scalable data quality platform** that leverages the best patterns from each system while eliminating redundancy and operational overhead.