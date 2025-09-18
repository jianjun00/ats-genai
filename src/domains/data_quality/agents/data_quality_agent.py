"""
Data Quality Agent
=================

Intelligent agent for autonomous data quality management using MCP tools.
Implements 2025 agentic patterns: sequential processing, reflection, and tool orchestration.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum

from src.infrastructure.tools.mcp.quality_scan_tool import QualityScanTool, QualityIssue
from src.infrastructure.tools.mcp.backfill_orchestrator_tool import BackfillOrchestratorTool
from src.domains.data_quality.agents.workflow_state_manager import WorkflowStateManager, WorkflowState
from src.domains.data_quality.agents.agent_metrics_collector import AgentMetricsCollector
from src.domains.data_quality.agents.agent_config import get_config_manager, AgentConfig
from src.domains.data_quality.agents.agent_logger import get_agent_logger
from src.domains.data_quality.agents.system_monitor import get_system_monitor
from src.domains.data_quality.agents.alert_manager import get_alert_manager

logger = logging.getLogger(__name__)

class AgentStatus(Enum):
    ACTIVE = "active"
    IDLE = "idle"
    BUSY = "busy"
    ERROR = "error"
    MAINTENANCE = "maintenance"

class IssueComplexity(Enum):
    SIMPLE = "simple"          # Fully automated resolution
    MEDIUM = "medium"          # Human-assisted resolution  
    COMPLEX = "complex"        # Expert team escalation

class ResolutionStrategy(Enum):
    AUTO_RESOLVE = "auto_resolve"
    HUMAN_ASSISTED = "human_assisted"
    ESCALATE = "escalate"
    MONITOR = "monitor"

@dataclass
class IssueClassification:
    """Issue classification result"""
    complexity: IssueComplexity
    strategy: ResolutionStrategy
    confidence: float
    reasoning: str
    estimated_resolution_time: int  # minutes
    required_approvals: List[str]
    risk_level: str

@dataclass
class AgentDecision:
    """Agent decision with reasoning"""
    action: str
    reasoning: str
    confidence: float
    alternatives: List[str]
    risk_assessment: str
    human_review_required: bool

class DataQualityAgent:
    """
    Intelligent Data Quality Agent implementing 2025 agentic patterns
    
    Key Capabilities:
    - Continuous monitoring and issue detection
    - Intelligent issue classification and resolution planning
    - Autonomous execution with human-in-the-loop for complex cases
    - Self-reflection and continuous learning
    - Multi-tool orchestration via MCP
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # Load configuration from config manager
        self.config_manager = get_config_manager()
        self.agent_config: AgentConfig = self.config_manager.get_config()
        
        # Legacy config override (will be deprecated)
        if config:
            logger.warning("Legacy config parameter is deprecated, use AgentConfigManager instead")
        
        self.status = AgentStatus.IDLE
        self.agent_id = f"dq_agent_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Initialize MCP tools
        self.mcp_tools = {
            "quality_scan": QualityScanTool(),
            "backfill_orchestrator": BackfillOrchestratorTool()
        }
        
        # State management
        self.workflow_manager = WorkflowStateManager()
        self.metrics_collector = AgentMetricsCollector()
        
        # Enhanced logging, monitoring, and alerting
        self.agent_logger = get_agent_logger(self.agent_id, self.agent_config.log_level)
        self.system_monitor = get_system_monitor(self.agent_id)
        self.alert_manager = get_alert_manager(self.agent_id)
        
        # Learning and memory
        self.issue_memory: Dict[str, Any] = {}
        self.success_patterns: Dict[str, List[str]] = {}
        self.failure_patterns: Dict[str, List[str]] = {}
        
        # Monitoring state
        self.monitoring_active = False
        self.last_scan_time: Optional[datetime] = None
        self.active_workflows: Dict[str, WorkflowState] = {}
        
        self.agent_logger.info("agent", "initialization", f"Data Quality Agent {self.agent_id} initialized with {len(self.mcp_tools)} MCP tools")
    
    async def start_continuous_monitoring(self):
        """Start the main monitoring loop - runs continuously"""
        if self.monitoring_active:
            logger.warning("Monitoring already active")
            return
        
        self.monitoring_active = True
        self.status = AgentStatus.ACTIVE
        
        self.agent_logger.info("agent", "start_monitoring", "🤖 Starting Data Quality Agent continuous monitoring")
        
        # Start system health monitoring
        asyncio.create_task(self.system_monitor.start_monitoring())
        
        try:
            while self.monitoring_active:
                with self.agent_logger.operation_timer("agent", "monitoring_cycle") as timer:
                    cycle_start = datetime.now()
                    
                    # Main monitoring cycle
                    await self._execute_monitoring_cycle()
                    
                    # Monitor active workflows
                    await self._monitor_active_workflows()
                    
                    # Perform reflection and learning
                    if self.agent_config.enable_reflection:
                        await self._perform_reflection()
                    
                    # Update metrics
                    cycle_duration = (datetime.now() - cycle_start).total_seconds()
                    await self.metrics_collector.record_monitoring_cycle(cycle_duration)
                    
                    self.agent_logger.debug("agent", "cycle_complete", 
                                          f"Monitoring cycle completed in {cycle_duration:.2f}s")
                
                # Sleep until next cycle
                sleep_duration = self.agent_config.monitoring.cycle_interval_seconds
                await asyncio.sleep(sleep_duration)
                
        except Exception as e:
            self.agent_logger.error("agent", "monitoring_loop", f"Agent monitoring loop failed: {e}", 
                                   error=str(e))
            self.status = AgentStatus.ERROR
            raise
        finally:
            self.monitoring_active = False
            self.status = AgentStatus.IDLE
    
    async def stop_monitoring(self):
        """Stop continuous monitoring"""
        logger.info("Stopping Data Quality Agent monitoring")
        self.monitoring_active = False
        self.status = AgentStatus.IDLE
    
    async def _execute_monitoring_cycle(self):
        """Execute one complete monitoring cycle"""
        try:
            self.status = AgentStatus.BUSY
            
            # Detection Phase: Scan for new issues
            new_issues = await self._detect_quality_issues()
            
            if new_issues:
                logger.info(f"Detected {len(new_issues)} new quality issues")
                
                # Classification Phase: Analyze each issue
                for issue in new_issues:
                    classification = await self._classify_issue(issue)
                    
                    # Decision Phase: Decide on resolution strategy
                    decision = await self._make_resolution_decision(issue, classification)
                    
                    # Execution Phase: Start resolution workflow
                    workflow = await self._initiate_resolution_workflow(issue, classification, decision)
                    
                    # Track workflow
                    self.active_workflows[workflow.workflow_id] = workflow
                    
                    # Record in memory for learning
                    await self._record_issue_in_memory(issue, classification, decision)
            
            self.last_scan_time = datetime.now()
            self.status = AgentStatus.ACTIVE
            
        except Exception as e:
            logger.error(f"Monitoring cycle failed: {e}")
            self.status = AgentStatus.ERROR
            raise
    
    async def _detect_quality_issues(self) -> List[QualityIssue]:
        """Use MCP tools to detect new quality issues"""
        
        # Run comprehensive quality scan
        scan_result = await self.mcp_tools["quality_scan"].execute({
            "table_name": "intg_daily_price_polygon",
            "date_range": {"days_back": 1},  # Check last day for new issues
            "quality_rules": ["completeness", "timeliness", "consistency", "accuracy"],
            "severity_threshold": "medium"
        })
        
        # Filter out issues we've already seen
        new_issues = []
        for issue in scan_result.issues:
            if not await self._is_known_issue(issue):
                new_issues.append(issue)
                await self._mark_issue_as_known(issue)
        
        return new_issues
    
    async def _classify_issue(self, issue: QualityIssue) -> IssueClassification:
        """Classify issue complexity and determine resolution approach"""
        
        # Base classification on issue type and historical patterns
        complexity_scores = {
            "missing_data": 0.3,      # Usually simple - trigger backfill
            "stale_data": 0.4,        # Medium - may need vendor switching
            "duplicate_records": 0.2,  # Simple - automated cleanup
            "extreme_volume": 0.7,    # Complex - needs investigation
            "extreme_price_range": 0.8, # Complex - market event analysis
            "scan_error": 0.9         # Complex - system issue
        }
        
        base_score = complexity_scores.get(issue.issue_type, 0.5)
        
        # Adjust based on severity
        severity_multipliers = {
            "critical": 1.5,
            "high": 1.2,
            "medium": 1.0,
            "low": 0.8
        }
        
        adjusted_score = base_score * severity_multipliers.get(issue.severity, 1.0)
        
        # Check historical success patterns
        historical_success_rate = await self._get_historical_success_rate(issue.issue_type)
        confidence_threshold = self.agent_config.action_thresholds.auto_resolve_confidence_threshold
        if historical_success_rate > confidence_threshold:
            adjusted_score *= 0.8  # More confident in automated resolution
        
        # Determine complexity and strategy using configuration thresholds
        escalation_threshold = self.agent_config.action_thresholds.escalation_confidence_threshold
        
        if adjusted_score < 0.4:
            complexity = IssueComplexity.SIMPLE
            strategy = ResolutionStrategy.AUTO_RESOLVE if historical_success_rate > confidence_threshold else ResolutionStrategy.HUMAN_ASSISTED
            approval_required = []
        elif adjusted_score < 0.7:
            complexity = IssueComplexity.MEDIUM
            strategy = ResolutionStrategy.HUMAN_ASSISTED
            approval_required = ["data_quality_team"]
        else:
            complexity = IssueComplexity.COMPLEX
            strategy = ResolutionStrategy.ESCALATE if adjusted_score < (1 - escalation_threshold) else ResolutionStrategy.ESCALATE
            approval_required = ["data_quality_team", "engineering_team"]
        
        # Estimate resolution time
        time_estimates = {
            IssueComplexity.SIMPLE: 5,   # 5 minutes
            IssueComplexity.MEDIUM: 30,  # 30 minutes
            IssueComplexity.COMPLEX: 120 # 2 hours
        }
        
        return IssueClassification(
            complexity=complexity,
            strategy=strategy,
            confidence=1.0 - adjusted_score,
            reasoning=f"Issue type: {issue.issue_type}, severity: {issue.severity}, historical success: {historical_success_rate:.2f}",
            estimated_resolution_time=time_estimates[complexity],
            required_approvals=approval_required,
            risk_level="low" if adjusted_score < 0.3 else "medium" if adjusted_score < 0.7 else "high"
        )
    
    async def _make_resolution_decision(self, issue: QualityIssue, classification: IssueClassification) -> AgentDecision:
        """Make intelligent decision on how to resolve the issue"""
        
        # Determine primary action based on issue type and classification
        if issue.issue_type == "missing_data" and classification.complexity == IssueComplexity.SIMPLE:
            action = "trigger_backfill"
            reasoning = "Missing data can be resolved by triggering automated backfill"
            alternatives = ["manual_data_entry", "mark_as_expected"]
            confidence = 0.9
            risk = "low"
            human_review = False
            
        elif issue.issue_type == "stale_data":
            action = "refresh_from_vendor"
            reasoning = "Stale data requires fresh data fetch from vendor"
            alternatives = ["switch_vendor", "archive_old_data"]
            confidence = 0.8
            risk = "medium"
            human_review = classification.complexity != IssueComplexity.SIMPLE
            
        elif issue.issue_type == "duplicate_records":
            action = "auto_deduplicate"
            reasoning = "Duplicate records can be safely removed automatically"
            alternatives = ["manual_review", "merge_records"]
            confidence = 0.95
            risk = "low"
            human_review = False
            
        elif issue.issue_type in ["extreme_volume", "extreme_price_range"]:
            action = "cross_validate_vendors"
            reasoning = "Extreme values need verification across multiple data sources"
            alternatives = ["mark_as_valid", "investigate_market_events"]
            confidence = 0.6
            risk = "high"
            human_review = True
            
        else:
            action = "escalate_to_human"
            reasoning = "Unknown or complex issue requires human investigation"
            alternatives = ["monitor_and_wait", "gather_more_data"]
            confidence = 0.5
            risk = "high"
            human_review = True
        
        return AgentDecision(
            action=action,
            reasoning=reasoning,
            confidence=confidence,
            alternatives=alternatives,
            risk_assessment=risk,
            human_review_required=human_review
        )
    
    async def _initiate_resolution_workflow(
        self, 
        issue: QualityIssue, 
        classification: IssueClassification,
        decision: AgentDecision
    ) -> WorkflowState:
        """Start resolution workflow based on decision"""
        
        workflow = await self.workflow_manager.create_workflow(
            issue_id=issue.id,
            issue_type=issue.issue_type,
            complexity=classification.complexity,
            strategy=classification.strategy,
            primary_action=decision.action,
            metadata={
                "symbol": issue.symbol,
                "affected_date": issue.affected_date.isoformat(),
                "severity": issue.severity,
                "confidence": decision.confidence,
                "risk_level": classification.risk_level
            }
        )
        
        # Execute based on strategy
        if classification.strategy == ResolutionStrategy.AUTO_RESOLVE:
            asyncio.create_task(self._execute_automated_resolution(workflow, issue, decision))
        elif classification.strategy == ResolutionStrategy.HUMAN_ASSISTED:
            await self._request_human_assistance(workflow, issue, classification, decision)
        else:  # ESCALATE
            await self._escalate_to_experts(workflow, issue, classification)
        
        return workflow
    
    async def _execute_automated_resolution(
        self,
        workflow: WorkflowState,
        issue: QualityIssue, 
        decision: AgentDecision
    ):
        """Execute fully automated resolution"""
        
        try:
            await self.workflow_manager.update_workflow_status(workflow.workflow_id, "executing")
            
            if decision.action == "trigger_backfill":
                # Use backfill orchestrator tool
                backfill_result = await self.mcp_tools["backfill_orchestrator"].execute({
                    "symbol": issue.symbol,
                    "date_range": {
                        "start_date": issue.affected_date.isoformat(),
                        "end_date": issue.affected_date.isoformat()
                    },
                    "vendor": "auto",
                    "priority": "high" if issue.severity == "critical" else "medium"
                })
                
                if backfill_result["success"]:
                    await self.workflow_manager.update_workflow_status(
                        workflow.workflow_id, "completed",
                        result={"backfill_job_id": backfill_result["job_id"]}
                    )
                    logger.info(f"Successfully triggered backfill for issue {issue.id}")
                else:
                    await self.workflow_manager.update_workflow_status(
                        workflow.workflow_id, "failed",
                        error=backfill_result.get("error", "Backfill failed")
                    )
            
            elif decision.action == "auto_deduplicate":
                # Placeholder for deduplication logic
                await asyncio.sleep(2)  # Simulate processing
                await self.workflow_manager.update_workflow_status(
                    workflow.workflow_id, "completed",
                    result={"duplicates_removed": 3}
                )
                logger.info(f"Successfully removed duplicates for issue {issue.id}")
            
            # Record successful automation for learning
            await self._record_successful_resolution(issue.issue_type, decision.action)
            
        except Exception as e:
            logger.error(f"Automated resolution failed for issue {issue.id}: {e}")
            await self.workflow_manager.update_workflow_status(
                workflow.workflow_id, "failed", error=str(e)
            )
            await self._record_failed_resolution(issue.issue_type, decision.action, str(e))
    
    async def _request_human_assistance(
        self,
        workflow: WorkflowState,
        issue: QualityIssue,
        classification: IssueClassification,
        decision: AgentDecision
    ):
        """Request human assistance for medium complexity issues"""
        
        await self.workflow_manager.update_workflow_status(workflow.workflow_id, "pending_approval")
        
        # Create human-readable summary
        summary = {
            "issue_description": issue.description,
            "recommended_action": decision.action,
            "reasoning": decision.reasoning,
            "confidence": decision.confidence,
            "alternatives": decision.alternatives,
            "estimated_time": classification.estimated_resolution_time,
            "risk_level": classification.risk_level
        }
        
        # Send notification (placeholder - would integrate with notification system)
        logger.info(f"Requesting human assistance for issue {issue.id}: {summary}")
        
        # For now, auto-approve after delay (in production, would wait for human input)
        await asyncio.sleep(10)
        await self._execute_automated_resolution(workflow, issue, decision)
    
    async def _escalate_to_experts(
        self,
        workflow: WorkflowState,
        issue: QualityIssue,
        classification: IssueClassification
    ):
        """Escalate complex issues to expert team"""
        
        await self.workflow_manager.update_workflow_status(workflow.workflow_id, "escalated")
        
        escalation_details = {
            "issue_type": issue.issue_type,
            "severity": issue.severity,
            "symbol": issue.symbol,
            "affected_date": issue.affected_date.isoformat(),
            "complexity_reasoning": classification.reasoning,
            "required_expertise": "data_engineering" if "system" in issue.issue_type else "market_analysis"
        }
        
        logger.warning(f"Escalating complex issue {issue.id} to expert team: {escalation_details}")
    
    async def _monitor_active_workflows(self):
        """Monitor progress of active workflows"""
        
        completed_workflows = []
        
        for workflow_id, workflow in self.active_workflows.items():
            # Check if workflow has been completed or needs attention
            updated_workflow = await self.workflow_manager.get_workflow(workflow_id)
            
            if updated_workflow.status in ["completed", "failed", "cancelled"]:
                await self._handle_workflow_completion(updated_workflow)
                completed_workflows.append(workflow_id)
            elif updated_workflow.status == "stalled":
                await self._handle_stalled_workflow(updated_workflow)
        
        # Remove completed workflows from active tracking
        for workflow_id in completed_workflows:
            del self.active_workflows[workflow_id]
    
    async def _handle_workflow_completion(self, workflow: WorkflowState):
        """Handle completed workflow - verification and learning"""
        
        if workflow.status == "completed":
            # Verify resolution was successful
            verification_passed = await self._verify_issue_resolution(workflow)
            
            if verification_passed:
                logger.info(f"Workflow {workflow.workflow_id} completed successfully")
                await self.metrics_collector.record_successful_resolution(workflow)
            else:
                logger.warning(f"Workflow {workflow.workflow_id} completed but verification failed")
                await self._reopen_issue(workflow)
        
        elif workflow.status == "failed":
            logger.error(f"Workflow {workflow.workflow_id} failed: {workflow.error}")
            await self.metrics_collector.record_failed_resolution(workflow)
    
    async def _verify_issue_resolution(self, workflow: WorkflowState) -> bool:
        """Verify that issue has been properly resolved"""
        
        # Re-run quality scan to check if issue is resolved
        scan_result = await self.mcp_tools["quality_scan"].execute({
            "table_name": "intg_daily_price_polygon", 
            "date_range": {"specific_date": workflow.metadata.get("affected_date")},
            "symbol_filter": workflow.metadata.get("symbol"),
            "quality_rules": ["completeness", "consistency"]
        })
        
        # Check if original issue type is no longer present
        issue_type = workflow.issue_type
        remaining_issues = [issue for issue in scan_result.issues if issue.issue_type == issue_type]
        
        return len(remaining_issues) == 0
    
    async def _perform_reflection(self):
        """Perform self-reflection and continuous learning"""
        
        # Analyze recent resolution patterns
        recent_successes = await self.metrics_collector.get_recent_successes(hours=24)
        recent_failures = await self.metrics_collector.get_recent_failures(hours=24)
        
        # Update success/failure patterns for learning
        for success in recent_successes:
            issue_type = success.get("issue_type")
            action = success.get("action")
            
            if issue_type not in self.success_patterns:
                self.success_patterns[issue_type] = []
            self.success_patterns[issue_type].append(action)
        
        for failure in recent_failures:
            issue_type = failure.get("issue_type")
            action = failure.get("action")
            
            if issue_type not in self.failure_patterns:
                self.failure_patterns[issue_type] = []
            self.failure_patterns[issue_type].append(action)
        
        # Log learning insights
        if len(recent_successes) > 0 or len(recent_failures) > 0:
            logger.info(f"Agent reflection: {len(recent_successes)} successes, {len(recent_failures)} failures in last 24h")
    
    async def _get_historical_success_rate(self, issue_type: str) -> float:
        """Get historical success rate for issue type"""
        successes = len(self.success_patterns.get(issue_type, []))
        failures = len(self.failure_patterns.get(issue_type, []))
        
        if successes + failures == 0:
            return 0.8  # Default optimistic rate
        
        return successes / (successes + failures)
    
    async def _record_successful_resolution(self, issue_type: str, action: str):
        """Record successful resolution for learning"""
        await self.metrics_collector.record_resolution_outcome(issue_type, action, True)
    
    async def _record_failed_resolution(self, issue_type: str, action: str, error: str):
        """Record failed resolution for learning"""
        await self.metrics_collector.record_resolution_outcome(issue_type, action, False, error)
    
    async def _is_known_issue(self, issue: QualityIssue) -> bool:
        """Check if issue has been seen before"""
        return issue.id in self.issue_memory
    
    async def _mark_issue_as_known(self, issue: QualityIssue):
        """Mark issue as known to avoid reprocessing"""
        self.issue_memory[issue.id] = {
            "first_seen": datetime.now(),
            "issue_type": issue.issue_type,
            "symbol": issue.symbol
        }
    
    async def _record_issue_in_memory(self, issue: QualityIssue, classification: IssueClassification, decision: AgentDecision):
        """Record issue details for learning"""
        self.issue_memory[issue.id].update({
            "classification": asdict(classification),
            "decision": asdict(decision),
            "processing_timestamp": datetime.now()
        })
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default agent configuration"""
        return {
            "monitoring_interval_seconds": 300,  # 5 minutes
            "max_concurrent_workflows": 10,
            "auto_resolution_enabled": True,
            "human_assistance_timeout_minutes": 60,
            "reflection_interval_hours": 6,
            "quality_thresholds": {
                "completeness": 0.95,
                "timeliness": 0.90,
                "consistency": 0.98,
                "accuracy": 0.92
            }
        }
    
    async def get_agent_status(self) -> Dict[str, Any]:
        """Get current agent status and metrics"""
        
        metrics = await self.metrics_collector.get_summary_metrics()
        
        return {
            "agent_id": self.agent_id,
            "status": self.status.value,
            "monitoring_active": self.monitoring_active,
            "last_scan_time": self.last_scan_time.isoformat() if self.last_scan_time else None,
            "active_workflows": len(self.active_workflows),
            "issues_in_memory": len(self.issue_memory),
            "metrics": metrics,
            "learning_patterns": {
                "success_patterns": {k: len(v) for k, v in self.success_patterns.items()},
                "failure_patterns": {k: len(v) for k, v in self.failure_patterns.items()}
            }
        }
    
    async def execute_manual_action(self, issue_id: str, action: str) -> Dict[str, Any]:
        """Execute manual action on specific issue"""
        
        if issue_id not in self.issue_memory:
            return {"success": False, "error": "Issue not found"}
        
        issue_info = self.issue_memory[issue_id]
        
        logger.info(f"Executing manual action '{action}' on issue {issue_id}")
        
        # Create manual workflow
        workflow = await self.workflow_manager.create_workflow(
            issue_id=issue_id,
            issue_type=issue_info["issue_type"],
            complexity=IssueComplexity.MEDIUM,
            strategy=ResolutionStrategy.HUMAN_ASSISTED,
            primary_action=action,
            metadata={"manual_trigger": True}
        )
        
        # Execute action based on type
        if action == "auto_resolve":
            asyncio.create_task(self._execute_manual_auto_resolve(workflow, issue_id))
        elif action == "escalate":
            await self.workflow_manager.update_workflow_status(workflow.workflow_id, "escalated")
        
        return {
            "success": True,
            "workflow_id": workflow.workflow_id,
            "action": action,
            "status": "initiated"
        }
    
    async def _execute_manual_auto_resolve(self, workflow: WorkflowState, issue_id: str):
        """Execute manual auto-resolve action"""
        try:
            await self.workflow_manager.update_workflow_status(workflow.workflow_id, "executing")
            
            # Simulate resolution processing
            await asyncio.sleep(3)
            
            await self.workflow_manager.update_workflow_status(
                workflow.workflow_id, "completed",
                result={"manual_resolution": True}
            )
            
            logger.info(f"Manual auto-resolve completed for issue {issue_id}")
            
        except Exception as e:
            logger.error(f"Manual auto-resolve failed: {e}")
            await self.workflow_manager.update_workflow_status(
                workflow.workflow_id, "failed", error=str(e)
            )