"""
Agent Metrics Collector
=======================

Collects and analyzes performance metrics for the Data Quality Agent.
Provides insights for continuous improvement and operational monitoring.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import statistics

logger = logging.getLogger(__name__)

@dataclass
class ResolutionOutcome:
    """Record of issue resolution outcome"""
    timestamp: datetime
    issue_type: str
    action: str
    success: bool
    duration_seconds: float
    error_message: Optional[str]
    workflow_id: str
    complexity: str
    confidence: float

@dataclass
class PerformanceMetrics:
    """Agent performance metrics summary"""
    total_issues_processed: int
    successful_resolutions: int
    failed_resolutions: int
    success_rate: float
    average_resolution_time_seconds: float
    issues_by_type: Dict[str, int]
    actions_by_success: Dict[str, Dict[str, int]]
    complexity_performance: Dict[str, Dict[str, Any]]
    time_period_hours: int

class AgentMetricsCollector:
    """Collects and analyzes Data Quality Agent performance metrics"""
    
    def __init__(self, max_history_days: int = 30):
        self.max_history_days = max_history_days
        self.resolution_outcomes: deque = deque(maxlen=10000)  # Rolling window
        self.monitoring_cycles: deque = deque(maxlen=1000)
        self.daily_summaries: Dict[str, Dict[str, Any]] = {}
        
        # Real-time counters
        self.session_stats = {
            "session_start": datetime.now(),
            "cycles_completed": 0,
            "issues_detected": 0,
            "issues_resolved": 0,
            "total_cycle_time": 0.0
        }
    
    async def record_resolution_outcome(
        self,
        issue_type: str,
        action: str, 
        success: bool,
        duration_seconds: float = 0.0,
        error_message: Optional[str] = None,
        workflow_id: str = "",
        complexity: str = "medium",
        confidence: float = 0.8
    ):
        """Record outcome of issue resolution attempt"""
        
        outcome = ResolutionOutcome(
            timestamp=datetime.now(),
            issue_type=issue_type,
            action=action,
            success=success,
            duration_seconds=duration_seconds,
            error_message=error_message,
            workflow_id=workflow_id,
            complexity=complexity,
            confidence=confidence
        )
        
        self.resolution_outcomes.append(outcome)
        
        # Update session stats
        if success:
            self.session_stats["issues_resolved"] += 1
        
        logger.debug(f"Recorded resolution outcome: {issue_type} -> {action} -> {'success' if success else 'failure'}")
    
    async def record_monitoring_cycle(self, cycle_duration_seconds: float):
        """Record completion of monitoring cycle"""
        
        cycle_record = {
            "timestamp": datetime.now(),
            "duration_seconds": cycle_duration_seconds,
            "cycle_number": self.session_stats["cycles_completed"] + 1
        }
        
        self.monitoring_cycles.append(cycle_record)
        
        # Update session stats
        self.session_stats["cycles_completed"] += 1
        self.session_stats["total_cycle_time"] += cycle_duration_seconds
    
    async def record_successful_resolution(self, workflow):
        """Record successful workflow completion"""
        
        duration = workflow.actual_duration_seconds or 0.0
        
        await self.record_resolution_outcome(
            issue_type=workflow.issue_type,
            action=workflow.primary_action,
            success=True,
            duration_seconds=duration,
            workflow_id=workflow.workflow_id,
            complexity=workflow.complexity,
            confidence=workflow.metadata.get("confidence", 0.8)
        )
    
    async def record_failed_resolution(self, workflow):
        """Record failed workflow"""
        
        duration = workflow.actual_duration_seconds or 0.0
        
        await self.record_resolution_outcome(
            issue_type=workflow.issue_type,
            action=workflow.primary_action,
            success=False,
            duration_seconds=duration,
            error_message=workflow.error,
            workflow_id=workflow.workflow_id,
            complexity=workflow.complexity,
            confidence=workflow.metadata.get("confidence", 0.8)
        )
    
    async def get_recent_successes(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent successful resolutions"""
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        recent_successes = [
            {
                "issue_type": outcome.issue_type,
                "action": outcome.action,
                "duration": outcome.duration_seconds,
                "timestamp": outcome.timestamp.isoformat(),
                "workflow_id": outcome.workflow_id,
                "complexity": outcome.complexity
            }
            for outcome in self.resolution_outcomes
            if outcome.success and outcome.timestamp >= cutoff_time
        ]
        
        return recent_successes
    
    async def get_recent_failures(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent failed resolutions"""
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        recent_failures = [
            {
                "issue_type": outcome.issue_type,
                "action": outcome.action,
                "duration": outcome.duration_seconds,
                "error": outcome.error_message,
                "timestamp": outcome.timestamp.isoformat(),
                "workflow_id": outcome.workflow_id,
                "complexity": outcome.complexity
            }
            for outcome in self.resolution_outcomes
            if not outcome.success and outcome.timestamp >= cutoff_time
        ]
        
        return recent_failures
    
    async def get_performance_metrics(self, hours: int = 24) -> PerformanceMetrics:
        """Get comprehensive performance metrics for time period"""
        
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        # Filter outcomes to time period
        period_outcomes = [
            outcome for outcome in self.resolution_outcomes
            if outcome.timestamp >= cutoff_time
        ]
        
        if not period_outcomes:
            return PerformanceMetrics(
                total_issues_processed=0,
                successful_resolutions=0,
                failed_resolutions=0,
                success_rate=0.0,
                average_resolution_time_seconds=0.0,
                issues_by_type={},
                actions_by_success={},
                complexity_performance={},
                time_period_hours=hours
            )
        
        # Calculate basic metrics
        total_issues = len(period_outcomes)
        successful = len([o for o in period_outcomes if o.success])
        failed = total_issues - successful
        success_rate = successful / total_issues if total_issues > 0 else 0.0
        
        # Average resolution time
        resolution_times = [o.duration_seconds for o in period_outcomes if o.duration_seconds > 0]
        avg_resolution_time = statistics.mean(resolution_times) if resolution_times else 0.0
        
        # Issues by type
        issues_by_type = defaultdict(int)
        for outcome in period_outcomes:
            issues_by_type[outcome.issue_type] += 1
        
        # Actions by success
        actions_by_success = defaultdict(lambda: {"success": 0, "failure": 0})
        for outcome in period_outcomes:
            status = "success" if outcome.success else "failure"
            actions_by_success[outcome.action][status] += 1
        
        # Complexity performance analysis
        complexity_performance = {}
        for complexity in ["simple", "medium", "complex"]:
            complexity_outcomes = [o for o in period_outcomes if o.complexity == complexity]
            if complexity_outcomes:
                complexity_successful = len([o for o in complexity_outcomes if o.success])
                complexity_times = [o.duration_seconds for o in complexity_outcomes if o.duration_seconds > 0]
                
                complexity_performance[complexity] = {
                    "total": len(complexity_outcomes),
                    "successful": complexity_successful,
                    "success_rate": complexity_successful / len(complexity_outcomes),
                    "avg_duration": statistics.mean(complexity_times) if complexity_times else 0.0,
                    "median_duration": statistics.median(complexity_times) if complexity_times else 0.0
                }
        
        return PerformanceMetrics(
            total_issues_processed=total_issues,
            successful_resolutions=successful,
            failed_resolutions=failed,
            success_rate=success_rate,
            average_resolution_time_seconds=avg_resolution_time,
            issues_by_type=dict(issues_by_type),
            actions_by_success=dict(actions_by_success),
            complexity_performance=complexity_performance,
            time_period_hours=hours
        )
    
    async def get_summary_metrics(self) -> Dict[str, Any]:
        """Get current session and recent performance summary"""
        
        # Session metrics
        session_duration = (datetime.now() - self.session_stats["session_start"]).total_seconds()
        avg_cycle_time = (self.session_stats["total_cycle_time"] / 
                         max(self.session_stats["cycles_completed"], 1))
        
        # Recent performance (last 24 hours)
        recent_metrics = await self.get_performance_metrics(hours=24)
        
        # Monitoring cycle performance
        recent_cycles = [c for c in self.monitoring_cycles 
                        if c["timestamp"] >= datetime.now() - timedelta(hours=1)]
        avg_recent_cycle_time = (statistics.mean([c["duration_seconds"] for c in recent_cycles]) 
                               if recent_cycles else 0.0)
        
        return {
            "session": {
                "session_duration_seconds": session_duration,
                "cycles_completed": self.session_stats["cycles_completed"],
                "issues_detected": self.session_stats["issues_detected"],
                "issues_resolved": self.session_stats["issues_resolved"],
                "avg_cycle_time_seconds": avg_cycle_time
            },
            "recent_24h": {
                "total_issues": recent_metrics.total_issues_processed,
                "success_rate": recent_metrics.success_rate,
                "avg_resolution_time": recent_metrics.average_resolution_time_seconds,
                "top_issue_types": self._get_top_items(recent_metrics.issues_by_type, 3)
            },
            "monitoring": {
                "recent_avg_cycle_time": avg_recent_cycle_time,
                "total_cycles_recorded": len(self.monitoring_cycles)
            },
            "learning": {
                "total_outcomes_recorded": len(self.resolution_outcomes),
                "data_retention_days": self.max_history_days
            }
        }
    
    async def get_issue_type_analysis(self, issue_type: str, days: int = 7) -> Dict[str, Any]:
        """Get detailed analysis for specific issue type"""
        
        cutoff_time = datetime.now() - timedelta(days=days)
        
        # Filter to specific issue type and time period
        type_outcomes = [
            outcome for outcome in self.resolution_outcomes
            if outcome.issue_type == issue_type and outcome.timestamp >= cutoff_time
        ]
        
        if not type_outcomes:
            return {
                "issue_type": issue_type,
                "total_occurrences": 0,
                "analysis_period_days": days
            }
        
        # Success analysis
        successful_outcomes = [o for o in type_outcomes if o.success]
        success_rate = len(successful_outcomes) / len(type_outcomes)
        
        # Action effectiveness
        action_effectiveness = defaultdict(lambda: {"attempts": 0, "successes": 0})
        for outcome in type_outcomes:
            action_effectiveness[outcome.action]["attempts"] += 1
            if outcome.success:
                action_effectiveness[outcome.action]["successes"] += 1
        
        # Calculate success rates for each action
        for action_data in action_effectiveness.values():
            attempts = action_data["attempts"]
            action_data["success_rate"] = action_data["successes"] / attempts if attempts > 0 else 0.0
        
        # Time analysis
        resolution_times = [o.duration_seconds for o in type_outcomes if o.duration_seconds > 0]
        time_stats = {}
        if resolution_times:
            time_stats = {
                "avg_resolution_time": statistics.mean(resolution_times),
                "median_resolution_time": statistics.median(resolution_times),
                "min_resolution_time": min(resolution_times),
                "max_resolution_time": max(resolution_times)
            }
        
        # Confidence analysis
        confidence_scores = [o.confidence for o in type_outcomes]
        confidence_stats = {}
        if confidence_scores:
            confidence_stats = {
                "avg_confidence": statistics.mean(confidence_scores),
                "median_confidence": statistics.median(confidence_scores)
            }
        
        # Trend analysis (by day)
        daily_counts = defaultdict(int)
        for outcome in type_outcomes:
            day_key = outcome.timestamp.date().isoformat()
            daily_counts[day_key] += 1
        
        return {
            "issue_type": issue_type,
            "analysis_period_days": days,
            "total_occurrences": len(type_outcomes),
            "success_rate": success_rate,
            "successful_resolutions": len(successful_outcomes),
            "failed_resolutions": len(type_outcomes) - len(successful_outcomes),
            "action_effectiveness": dict(action_effectiveness),
            "time_statistics": time_stats,
            "confidence_statistics": confidence_stats,
            "daily_trend": dict(daily_counts),
            "best_action": self._get_best_action(action_effectiveness),
            "recommendations": self._generate_recommendations(issue_type, action_effectiveness, success_rate)
        }
    
    async def get_agent_health_score(self) -> Dict[str, Any]:
        """Calculate overall agent health score"""
        
        # Recent performance (last 24 hours)
        recent_metrics = await self.get_performance_metrics(hours=24)
        
        # Health score components (0-1 scale)
        success_rate_score = recent_metrics.success_rate
        
        # Response time score (faster is better, normalize to 0-1)
        response_time_score = max(0.0, min(1.0, 1.0 - (recent_metrics.average_resolution_time_seconds / 3600)))
        
        # Activity score (based on issues processed)
        activity_score = min(1.0, recent_metrics.total_issues_processed / 10)  # Normalize to 10 issues/day
        
        # Cycle performance score
        recent_cycles = [c for c in self.monitoring_cycles 
                        if c["timestamp"] >= datetime.now() - timedelta(hours=24)]
        cycle_score = min(1.0, len(recent_cycles) / 288)  # Normalize to 5-min cycles (288/day)
        
        # Weighted overall health score
        weights = {
            "success_rate": 0.4,
            "response_time": 0.3,
            "activity": 0.2,
            "cycle_performance": 0.1
        }
        
        overall_score = (
            success_rate_score * weights["success_rate"] +
            response_time_score * weights["response_time"] +
            activity_score * weights["activity"] +
            cycle_score * weights["cycle_performance"]
        )
        
        # Determine health status
        if overall_score >= 0.9:
            health_status = "excellent"
        elif overall_score >= 0.8:
            health_status = "good"
        elif overall_score >= 0.6:
            health_status = "fair"
        elif overall_score >= 0.4:
            health_status = "poor"
        else:
            health_status = "critical"
        
        return {
            "overall_health_score": overall_score,
            "health_status": health_status,
            "component_scores": {
                "success_rate": success_rate_score,
                "response_time": response_time_score,
                "activity": activity_score,
                "cycle_performance": cycle_score
            },
            "recommendations": self._generate_health_recommendations(overall_score, {
                "success_rate": success_rate_score,
                "response_time": response_time_score,
                "activity": activity_score,
                "cycle_performance": cycle_score
            })
        }
    
    async def export_metrics_report(self, hours: int = 24) -> Dict[str, Any]:
        """Export comprehensive metrics report"""
        
        metrics = await self.get_performance_metrics(hours)
        summary = await self.get_summary_metrics()
        health = await self.get_agent_health_score()
        
        # Top performing actions
        best_actions = []
        for action, stats in metrics.actions_by_success.items():
            total = stats["success"] + stats["failure"]
            if total >= 3:  # Minimum sample size
                success_rate = stats["success"] / total
                best_actions.append({
                    "action": action,
                    "success_rate": success_rate,
                    "total_attempts": total
                })
        
        best_actions.sort(key=lambda x: x["success_rate"], reverse=True)
        
        return {
            "report_generated": datetime.now().isoformat(),
            "time_period_hours": hours,
            "performance_metrics": asdict(metrics),
            "session_summary": summary,
            "agent_health": health,
            "top_performing_actions": best_actions[:5],
            "complexity_insights": self._analyze_complexity_patterns(metrics),
            "improvement_opportunities": self._identify_improvement_opportunities(metrics)
        }
    
    def _get_top_items(self, items_dict: Dict[str, int], top_n: int) -> List[Tuple[str, int]]:
        """Get top N items from dictionary by value"""
        return sorted(items_dict.items(), key=lambda x: x[1], reverse=True)[:top_n]
    
    def _get_best_action(self, action_effectiveness: Dict[str, Dict[str, Any]]) -> Optional[str]:
        """Get best performing action for issue type"""
        
        best_action = None
        best_score = 0.0
        
        for action, stats in action_effectiveness.items():
            if stats["attempts"] >= 2:  # Minimum sample size
                score = stats["success_rate"]
                if score > best_score:
                    best_score = score
                    best_action = action
        
        return best_action
    
    def _generate_recommendations(
        self, 
        issue_type: str, 
        action_effectiveness: Dict[str, Dict[str, Any]], 
        success_rate: float
    ) -> List[str]:
        """Generate recommendations for improving issue type handling"""
        
        recommendations = []
        
        if success_rate < 0.7:
            recommendations.append(f"Success rate for {issue_type} is low ({success_rate:.1%}). Consider reviewing resolution strategies.")
        
        # Find best and worst performing actions
        actions_by_performance = sorted(
            [(action, stats["success_rate"]) for action, stats in action_effectiveness.items() 
             if stats["attempts"] >= 2],
            key=lambda x: x[1], reverse=True
        )
        
        if len(actions_by_performance) >= 2:
            best_action, best_rate = actions_by_performance[0]
            worst_action, worst_rate = actions_by_performance[-1]
            
            if best_rate - worst_rate > 0.3:
                recommendations.append(f"Consider preferring '{best_action}' over '{worst_action}' for {issue_type}")
        
        if success_rate > 0.9:
            recommendations.append(f"Excellent performance on {issue_type}. Consider full automation.")
        
        return recommendations
    
    def _generate_health_recommendations(
        self, 
        overall_score: float, 
        component_scores: Dict[str, float]
    ) -> List[str]:
        """Generate health improvement recommendations"""
        
        recommendations = []
        
        if component_scores["success_rate"] < 0.7:
            recommendations.append("Low success rate detected. Review resolution strategies and failure patterns.")
        
        if component_scores["response_time"] < 0.5:
            recommendations.append("High response times detected. Consider optimizing resolution workflows.")
        
        if component_scores["activity"] < 0.3:
            recommendations.append("Low activity level. Check monitoring frequency and issue detection sensitivity.")
        
        if component_scores["cycle_performance"] < 0.5:
            recommendations.append("Monitoring cycles running slowly. Check system resources and cycle intervals.")
        
        if overall_score > 0.9:
            recommendations.append("Agent performing excellently. Consider expanding automation scope.")
        
        return recommendations
    
    def _analyze_complexity_patterns(self, metrics: PerformanceMetrics) -> Dict[str, Any]:
        """Analyze patterns in complexity handling"""
        
        insights = []
        
        for complexity, stats in metrics.complexity_performance.items():
            if stats["total"] >= 3:  # Minimum sample size
                if stats["success_rate"] < 0.5:
                    insights.append(f"Low success rate for {complexity} issues ({stats['success_rate']:.1%})")
                
                if stats["avg_duration"] > 1800:  # 30 minutes
                    insights.append(f"{complexity.title()} issues taking too long (avg: {stats['avg_duration']/60:.1f} min)")
        
        return {
            "complexity_insights": insights,
            "performance_by_complexity": metrics.complexity_performance
        }
    
    def _identify_improvement_opportunities(self, metrics: PerformanceMetrics) -> List[str]:
        """Identify specific improvement opportunities"""
        
        opportunities = []
        
        # Low performing actions
        for action, stats in metrics.actions_by_success.items():
            total = stats["success"] + stats["failure"]
            if total >= 3 and stats["success"] / total < 0.6:
                opportunities.append(f"Action '{action}' has low success rate ({stats['success']/total:.1%})")
        
        # High volume issue types
        total_issues = sum(metrics.issues_by_type.values())
        for issue_type, count in metrics.issues_by_type.items():
            if count / total_issues > 0.4:  # More than 40% of issues
                opportunities.append(f"High volume of '{issue_type}' issues ({count}) - consider preventive measures")
        
        return opportunities