"""
Workflow State Manager
=====================

Manages workflow states and transitions for data quality issue resolution.
Provides persistence, tracking, and state transition management.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
import uuid
import json

logger = logging.getLogger(__name__)

class WorkflowStatus(Enum):
    CREATED = "created"
    EXECUTING = "executing"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ESCALATED = "escalated"
    STALLED = "stalled"

@dataclass
class WorkflowTransition:
    """Workflow status transition record"""
    from_status: WorkflowStatus
    to_status: WorkflowStatus
    timestamp: datetime
    reason: str
    triggered_by: str  # 'agent', 'human', 'system'
    metadata: Dict[str, Any]

@dataclass 
class WorkflowState:
    """Complete workflow state tracking"""
    workflow_id: str
    issue_id: str
    issue_type: str
    complexity: str  # simple, medium, complex
    strategy: str    # auto_resolve, human_assisted, escalate
    primary_action: str
    status: WorkflowStatus
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error: Optional[str]
    result: Optional[Dict[str, Any]]
    metadata: Dict[str, Any]
    transitions: List[WorkflowTransition]
    assigned_to: Optional[str]
    priority: str
    estimated_completion: Optional[datetime]
    actual_duration_seconds: Optional[float]

class WorkflowStateManager:
    """Manages workflow states with persistence and transition validation"""
    
    def __init__(self):
        self.workflows: Dict[str, WorkflowState] = {}
        self.transition_rules = self._define_transition_rules()
        
    async def create_workflow(
        self,
        issue_id: str,
        issue_type: str,
        complexity: Any,  # IssueComplexity enum
        strategy: Any,    # ResolutionStrategy enum  
        primary_action: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> WorkflowState:
        """Create new workflow with initial state"""
        
        workflow_id = str(uuid.uuid4())
        now = datetime.now()
        
        # Estimate completion time based on complexity
        estimated_duration = self._estimate_workflow_duration(complexity, primary_action)
        estimated_completion = now + estimated_duration
        
        workflow = WorkflowState(
            workflow_id=workflow_id,
            issue_id=issue_id,
            issue_type=issue_type,
            complexity=complexity.value if hasattr(complexity, 'value') else str(complexity),
            strategy=strategy.value if hasattr(strategy, 'value') else str(strategy),
            primary_action=primary_action,
            status=WorkflowStatus.CREATED,
            created_at=now,
            updated_at=now,
            started_at=None,
            completed_at=None,
            error=None,
            result=None,
            metadata=metadata or {},
            transitions=[],
            assigned_to=None,
            priority=self._determine_priority(issue_type, complexity),
            estimated_completion=estimated_completion,
            actual_duration_seconds=None
        )
        
        # Record initial transition
        initial_transition = WorkflowTransition(
            from_status=None,
            to_status=WorkflowStatus.CREATED,
            timestamp=now,
            reason="Workflow created by agent",
            triggered_by="agent",
            metadata={"initial_creation": True}
        )
        workflow.transitions.append(initial_transition)
        
        self.workflows[workflow_id] = workflow
        
        logger.info(f"Created workflow {workflow_id} for issue {issue_id}")
        return workflow
    
    async def update_workflow_status(
        self,
        workflow_id: str,
        new_status: str,
        reason: Optional[str] = None,
        triggered_by: str = "agent",
        error: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update workflow status with validation and transition tracking"""
        
        if workflow_id not in self.workflows:
            logger.error(f"Workflow {workflow_id} not found")
            return False
        
        workflow = self.workflows[workflow_id]
        old_status = workflow.status
        new_status_enum = WorkflowStatus(new_status)
        
        # Validate transition
        if not self._is_valid_transition(old_status, new_status_enum):
            logger.error(f"Invalid transition from {old_status.value} to {new_status} for workflow {workflow_id}")
            return False
        
        now = datetime.now()
        
        # Update workflow state
        workflow.status = new_status_enum
        workflow.updated_at = now
        
        if new_status_enum == WorkflowStatus.EXECUTING and workflow.started_at is None:
            workflow.started_at = now
        
        if new_status_enum in [WorkflowStatus.COMPLETED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED]:
            workflow.completed_at = now
            if workflow.started_at:
                workflow.actual_duration_seconds = (now - workflow.started_at).total_seconds()
        
        if error:
            workflow.error = error
        
        if result:
            workflow.result = result
        
        if metadata:
            workflow.metadata.update(metadata)
        
        # Record transition
        transition = WorkflowTransition(
            from_status=old_status,
            to_status=new_status_enum,
            timestamp=now,
            reason=reason or f"Status changed to {new_status}",
            triggered_by=triggered_by,
            metadata=metadata or {}
        )
        workflow.transitions.append(transition)
        
        logger.info(f"Workflow {workflow_id} transitioned from {old_status.value} to {new_status}")
        
        # Trigger any status-specific actions
        await self._handle_status_change(workflow, old_status, new_status_enum)
        
        return True
    
    async def get_workflow(self, workflow_id: str) -> Optional[WorkflowState]:
        """Get workflow by ID"""
        return self.workflows.get(workflow_id)
    
    async def get_workflows_by_status(self, status: WorkflowStatus) -> List[WorkflowState]:
        """Get all workflows with specific status"""
        return [wf for wf in self.workflows.values() if wf.status == status]
    
    async def get_active_workflows(self) -> List[WorkflowState]:
        """Get all active (non-terminal) workflows"""
        terminal_statuses = {
            WorkflowStatus.COMPLETED, 
            WorkflowStatus.FAILED, 
            WorkflowStatus.CANCELLED
        }
        return [wf for wf in self.workflows.values() if wf.status not in terminal_statuses]
    
    async def get_stalled_workflows(self, stall_threshold_minutes: int = 60) -> List[WorkflowState]:
        """Get workflows that appear to be stalled"""
        cutoff_time = datetime.now() - timedelta(minutes=stall_threshold_minutes)
        
        stalled = []
        for workflow in self.workflows.values():
            if (workflow.status in [WorkflowStatus.EXECUTING, WorkflowStatus.PENDING_APPROVAL] and
                workflow.updated_at < cutoff_time):
                stalled.append(workflow)
        
        return stalled
    
    async def assign_workflow(self, workflow_id: str, assignee: str) -> bool:
        """Assign workflow to specific person/team"""
        if workflow_id not in self.workflows:
            return False
        
        workflow = self.workflows[workflow_id]
        workflow.assigned_to = assignee
        workflow.updated_at = datetime.now()
        
        transition = WorkflowTransition(
            from_status=workflow.status,
            to_status=workflow.status,
            timestamp=datetime.now(),
            reason=f"Assigned to {assignee}",
            triggered_by="system",
            metadata={"assignment": assignee}
        )
        workflow.transitions.append(transition)
        
        logger.info(f"Assigned workflow {workflow_id} to {assignee}")
        return True
    
    async def cancel_workflow(self, workflow_id: str, reason: str = "Cancelled by request") -> bool:
        """Cancel active workflow"""
        return await self.update_workflow_status(
            workflow_id, 
            WorkflowStatus.CANCELLED.value,
            reason=reason,
            triggered_by="human"
        )
    
    async def get_workflow_metrics(self) -> Dict[str, Any]:
        """Get workflow performance metrics"""
        
        total_workflows = len(self.workflows)
        if total_workflows == 0:
            return {"total_workflows": 0}
        
        # Status distribution
        status_counts = {}
        for workflow in self.workflows.values():
            status = workflow.status.value
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Duration analysis for completed workflows
        completed_workflows = [wf for wf in self.workflows.values() 
                             if wf.status == WorkflowStatus.COMPLETED and wf.actual_duration_seconds]
        
        avg_duration = 0
        if completed_workflows:
            avg_duration = sum(wf.actual_duration_seconds for wf in completed_workflows) / len(completed_workflows)
        
        # Success rate
        terminal_workflows = [wf for wf in self.workflows.values() 
                            if wf.status in [WorkflowStatus.COMPLETED, WorkflowStatus.FAILED]]
        
        success_rate = 0
        if terminal_workflows:
            completed_count = len([wf for wf in terminal_workflows if wf.status == WorkflowStatus.COMPLETED])
            success_rate = completed_count / len(terminal_workflows)
        
        # Complexity analysis
        complexity_performance = {}
        for complexity in ["simple", "medium", "complex"]:
            complexity_workflows = [wf for wf in self.workflows.values() if wf.complexity == complexity]
            if complexity_workflows:
                completed = len([wf for wf in complexity_workflows if wf.status == WorkflowStatus.COMPLETED])
                complexity_performance[complexity] = {
                    "total": len(complexity_workflows),
                    "completed": completed,
                    "success_rate": completed / len(complexity_workflows)
                }
        
        return {
            "total_workflows": total_workflows,
            "status_distribution": status_counts,
            "average_duration_seconds": avg_duration,
            "overall_success_rate": success_rate,
            "complexity_performance": complexity_performance,
            "active_workflows": len(await self.get_active_workflows()),
            "stalled_workflows": len(await self.get_stalled_workflows())
        }
    
    def _define_transition_rules(self) -> Dict[WorkflowStatus, List[WorkflowStatus]]:
        """Define valid status transitions"""
        return {
            WorkflowStatus.CREATED: [
                WorkflowStatus.EXECUTING,
                WorkflowStatus.PENDING_APPROVAL, 
                WorkflowStatus.CANCELLED
            ],
            WorkflowStatus.EXECUTING: [
                WorkflowStatus.COMPLETED,
                WorkflowStatus.FAILED,
                WorkflowStatus.PENDING_APPROVAL,
                WorkflowStatus.ESCALATED,
                WorkflowStatus.CANCELLED,
                WorkflowStatus.STALLED
            ],
            WorkflowStatus.PENDING_APPROVAL: [
                WorkflowStatus.APPROVED,
                WorkflowStatus.REJECTED,
                WorkflowStatus.CANCELLED,
                WorkflowStatus.ESCALATED,
                WorkflowStatus.STALLED
            ],
            WorkflowStatus.APPROVED: [
                WorkflowStatus.EXECUTING,
                WorkflowStatus.CANCELLED
            ],
            WorkflowStatus.REJECTED: [
                WorkflowStatus.ESCALATED,
                WorkflowStatus.CANCELLED
            ],
            WorkflowStatus.ESCALATED: [
                WorkflowStatus.EXECUTING,
                WorkflowStatus.CANCELLED,
                WorkflowStatus.COMPLETED
            ],
            WorkflowStatus.STALLED: [
                WorkflowStatus.EXECUTING,
                WorkflowStatus.ESCALATED,
                WorkflowStatus.CANCELLED
            ],
            # Terminal states (no transitions allowed)
            WorkflowStatus.COMPLETED: [],
            WorkflowStatus.FAILED: [],
            WorkflowStatus.CANCELLED: []
        }
    
    def _is_valid_transition(self, from_status: WorkflowStatus, to_status: WorkflowStatus) -> bool:
        """Check if status transition is valid"""
        allowed_transitions = self.transition_rules.get(from_status, [])
        return to_status in allowed_transitions
    
    def _estimate_workflow_duration(self, complexity: Any, action: str) -> timedelta:
        """Estimate workflow completion time"""
        
        # Base durations by complexity
        base_durations = {
            "simple": timedelta(minutes=5),
            "medium": timedelta(minutes=30),
            "complex": timedelta(hours=2)
        }
        
        complexity_str = complexity.value if hasattr(complexity, 'value') else str(complexity)
        base_duration = base_durations.get(complexity_str, timedelta(minutes=30))
        
        # Action-specific multipliers
        action_multipliers = {
            "trigger_backfill": 1.0,
            "auto_deduplicate": 0.5,
            "cross_validate_vendors": 2.0,
            "escalate_to_human": 4.0
        }
        
        multiplier = action_multipliers.get(action, 1.0)
        return base_duration * multiplier
    
    def _determine_priority(self, issue_type: str, complexity: Any) -> str:
        """Determine workflow priority"""
        
        # High priority issue types
        high_priority_types = ["scan_error", "extreme_price_range", "extreme_volume"]
        
        complexity_str = complexity.value if hasattr(complexity, 'value') else str(complexity)
        
        if issue_type in high_priority_types:
            return "high"
        elif complexity_str == "complex":
            return "high"
        elif complexity_str == "simple":
            return "low"
        else:
            return "medium"
    
    async def _handle_status_change(
        self, 
        workflow: WorkflowState, 
        old_status: WorkflowStatus, 
        new_status: WorkflowStatus
    ):
        """Handle status-specific actions"""
        
        if new_status == WorkflowStatus.STALLED:
            logger.warning(f"Workflow {workflow.workflow_id} has stalled - may need intervention")
        
        elif new_status == WorkflowStatus.FAILED:
            logger.error(f"Workflow {workflow.workflow_id} failed: {workflow.error}")
        
        elif new_status == WorkflowStatus.COMPLETED:
            duration = workflow.actual_duration_seconds or 0
            logger.info(f"Workflow {workflow.workflow_id} completed in {duration:.1f} seconds")
        
        elif new_status == WorkflowStatus.ESCALATED:
            logger.warning(f"Workflow {workflow.workflow_id} escalated - requires expert attention")
    
    async def cleanup_old_workflows(self, days_old: int = 30):
        """Clean up old completed workflows"""
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        to_remove = []
        for workflow_id, workflow in self.workflows.items():
            if (workflow.status in [WorkflowStatus.COMPLETED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED] and
                workflow.completed_at and workflow.completed_at < cutoff_date):
                to_remove.append(workflow_id)
        
        for workflow_id in to_remove:
            del self.workflows[workflow_id]
        
        if to_remove:
            logger.info(f"Cleaned up {len(to_remove)} old workflows")
    
    def to_dict(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Convert workflow to dictionary for serialization"""
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            return None
        
        return {
            "workflow_id": workflow.workflow_id,
            "issue_id": workflow.issue_id,
            "issue_type": workflow.issue_type,
            "complexity": workflow.complexity,
            "strategy": workflow.strategy,
            "primary_action": workflow.primary_action,
            "status": workflow.status.value,
            "created_at": workflow.created_at.isoformat(),
            "updated_at": workflow.updated_at.isoformat(),
            "started_at": workflow.started_at.isoformat() if workflow.started_at else None,
            "completed_at": workflow.completed_at.isoformat() if workflow.completed_at else None,
            "error": workflow.error,
            "result": workflow.result,
            "metadata": workflow.metadata,
            "assigned_to": workflow.assigned_to,
            "priority": workflow.priority,
            "estimated_completion": workflow.estimated_completion.isoformat() if workflow.estimated_completion else None,
            "actual_duration_seconds": workflow.actual_duration_seconds,
            "transitions": [
                {
                    "from_status": t.from_status.value if t.from_status else None,
                    "to_status": t.to_status.value,
                    "timestamp": t.timestamp.isoformat(),
                    "reason": t.reason,
                    "triggered_by": t.triggered_by,
                    "metadata": t.metadata
                }
                for t in workflow.transitions
            ]
        }