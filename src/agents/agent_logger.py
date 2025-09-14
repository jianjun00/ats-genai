"""
Agent Logging System
===================

Comprehensive logging system for Data Quality Agent with structured logging,
performance metrics, and operational intelligence.
"""

import logging
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import threading
from pathlib import Path

@dataclass
class LogEntry:
    """Structured log entry for agent operations"""
    timestamp: str
    level: str
    agent_id: str
    component: str
    operation: str
    message: str
    duration_ms: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None
    workflow_id: Optional[str] = None
    issue_id: Optional[str] = None
    tool_name: Optional[str] = None
    success: Optional[bool] = None
    error: Optional[str] = None

class AgentLogger:
    """Enhanced logging system for Data Quality Agent operations"""

    def __init__(self, agent_id: str, log_level: str = "INFO"):
        self.agent_id = agent_id
        self.log_level = log_level

        # Setup structured logger
        self.logger = self._setup_logger()

        # Performance tracking
        self.operation_times: Dict[str, List[float]] = {}
        self.error_counts: Dict[str, int] = {}
        self.success_counts: Dict[str, int] = {}

        # Thread safety
        self._lock = threading.Lock()

        # Log buffer for dashboard display
        self.recent_logs: List[LogEntry] = []
        self.max_recent_logs = 100

    def _setup_logger(self) -> logging.Logger:
        """Setup structured logger with file and console handlers"""
        logger = logging.getLogger(f"agent.{self.agent_id}")
        logger.setLevel(getattr(logging, self.log_level.upper()))

        # Prevent duplicate handlers
        if logger.handlers:
            return logger

        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(detailed_formatter)
        logger.addHandler(console_handler)

        # File handler for agent logs
        log_dir = Path("logs/agent")
        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(
            log_dir / f"agent_{self.agent_id}_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)

        # Structured JSON log handler
        json_handler = logging.FileHandler(
            log_dir / f"agent_{self.agent_id}_structured.jsonl"
        )
        json_handler.setLevel(logging.DEBUG)
        json_handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(json_handler)

        return logger

    def _create_log_entry(self, level: str, component: str, operation: str,
                         message: str, **kwargs) -> LogEntry:
        """Create structured log entry"""
        return LogEntry(
            timestamp=datetime.now().isoformat(),
            level=level,
            agent_id=self.agent_id,
            component=component,
            operation=operation,
            message=message,
            **kwargs
        )

    def _log_structured(self, entry: LogEntry):
        """Log structured entry to both regular and JSON logs"""
        with self._lock:
            # Add to recent logs buffer
            self.recent_logs.append(entry)
            if len(self.recent_logs) > self.max_recent_logs:
                self.recent_logs.pop(0)

            # Update performance metrics
            if entry.duration_ms is not None:
                if entry.operation not in self.operation_times:
                    self.operation_times[entry.operation] = []
                self.operation_times[entry.operation].append(entry.duration_ms)

                # Keep only recent 100 measurements
                if len(self.operation_times[entry.operation]) > 100:
                    self.operation_times[entry.operation].pop(0)

            # Update success/error counts
            if entry.success is not None:
                if entry.success:
                    self.success_counts[entry.operation] = self.success_counts.get(entry.operation, 0) + 1
                else:
                    self.error_counts[entry.operation] = self.error_counts.get(entry.operation, 0) + 1

        # Log to regular logger
        log_method = getattr(self.logger, entry.level.lower())
        log_method(entry.message)

        # Log structured data to JSON handler
        json_handler = [h for h in self.logger.handlers if hasattr(h, 'baseFilename') and 'structured' in str(h.baseFilename)]
        if json_handler:
            json_handler[0].emit(
                logging.LogRecord(
                    name=self.logger.name,
                    level=getattr(logging, entry.level.upper()),
                    pathname="",
                    lineno=0,
                    msg=json.dumps(asdict(entry)),
                    args=(),
                    exc_info=None
                )
            )

    def info(self, component: str, operation: str, message: str, **kwargs):
        """Log info level message"""
        entry = self._create_log_entry("INFO", component, operation, message, **kwargs)
        self._log_structured(entry)

    def warning(self, component: str, operation: str, message: str, **kwargs):
        """Log warning level message"""
        entry = self._create_log_entry("WARNING", component, operation, message, **kwargs)
        self._log_structured(entry)

    def error(self, component: str, operation: str, message: str, **kwargs):
        """Log error level message"""
        entry = self._create_log_entry("ERROR", component, operation, message, **kwargs)
        self._log_structured(entry)

    def debug(self, component: str, operation: str, message: str, **kwargs):
        """Log debug level message"""
        entry = self._create_log_entry("DEBUG", component, operation, message, **kwargs)
        self._log_structured(entry)

    @contextmanager
    def operation_timer(self, component: str, operation: str, workflow_id: str = None, **kwargs):
        """Context manager for timing operations"""
        start_time = time.time()
        start_entry = self._create_log_entry(
            "DEBUG", component, f"{operation}_start",
            f"Starting {operation}", workflow_id=workflow_id, **kwargs
        )
        self._log_structured(start_entry)

        success = True
        error_msg = None

        try:
            yield
        except Exception as e:
            success = False
            error_msg = str(e)
            raise
        finally:
            duration_ms = (time.time() - start_time) * 1000

            end_entry = self._create_log_entry(
                "INFO" if success else "ERROR",
                component,
                f"{operation}_complete",
                f"Completed {operation} in {duration_ms:.1f}ms" + (f" - Error: {error_msg}" if not success else ""),
                duration_ms=duration_ms,
                success=success,
                error=error_msg,
                workflow_id=workflow_id,
                **kwargs
            )
            self._log_structured(end_entry)

    def log_tool_execution(self, tool_name: str, operation: str, success: bool,
                          duration_ms: float, metadata: Dict[str, Any] = None):
        """Log MCP tool execution"""
        entry = self._create_log_entry(
            "INFO" if success else "ERROR",
            "mcp_tools",
            f"tool_execution",
            f"Tool {tool_name}.{operation} {'succeeded' if success else 'failed'} in {duration_ms:.1f}ms",
            tool_name=tool_name,
            duration_ms=duration_ms,
            success=success,
            metadata=metadata
        )
        self._log_structured(entry)

    def log_workflow_transition(self, workflow_id: str, from_status: str, to_status: str,
                               reason: str, metadata: Dict[str, Any] = None):
        """Log workflow state transitions"""
        entry = self._create_log_entry(
            "INFO",
            "workflow_manager",
            "state_transition",
            f"Workflow {workflow_id}: {from_status} → {to_status} - {reason}",
            workflow_id=workflow_id,
            metadata={
                "from_status": from_status,
                "to_status": to_status,
                "reason": reason,
                **(metadata or {})
            }
        )
        self._log_structured(entry)

    def log_issue_detection(self, issue_id: str, issue_type: str, severity: str,
                           symbol: str, description: str, metadata: Dict[str, Any] = None):
        """Log data quality issue detection"""
        entry = self._create_log_entry(
            "WARNING" if severity in ["high", "critical"] else "INFO",
            "quality_scanner",
            "issue_detected",
            f"Issue detected: {symbol} - {description} (Severity: {severity})",
            issue_id=issue_id,
            metadata={
                "issue_type": issue_type,
                "severity": severity,
                "symbol": symbol,
                **(metadata or {})
            }
        )
        self._log_structured(entry)

    def log_decision(self, operation: str, decision: str, confidence: float,
                    reasoning: str, metadata: Dict[str, Any] = None):
        """Log agent decision-making"""
        entry = self._create_log_entry(
            "INFO",
            "decision_engine",
            operation,
            f"Decision: {decision} (confidence: {confidence:.2f}) - {reasoning}",
            metadata={
                "decision": decision,
                "confidence": confidence,
                "reasoning": reasoning,
                **(metadata or {})
            }
        )
        self._log_structured(entry)

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary for operations"""
        with self._lock:
            summary = {}

            for operation, times in self.operation_times.items():
                if times:
                    summary[operation] = {
                        "avg_duration_ms": sum(times) / len(times),
                        "min_duration_ms": min(times),
                        "max_duration_ms": max(times),
                        "total_executions": len(times),
                        "success_count": self.success_counts.get(operation, 0),
                        "error_count": self.error_counts.get(operation, 0),
                        "success_rate": self.success_counts.get(operation, 0) / len(times) if times else 0
                    }

            return summary

    def get_recent_logs(self, count: int = 50) -> List[Dict[str, Any]]:
        """Get recent log entries for dashboard display"""
        with self._lock:
            return [asdict(entry) for entry in self.recent_logs[-count:]]

    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of recent errors"""
        with self._lock:
            recent_errors = [
                entry for entry in self.recent_logs
                if entry.level == "ERROR" and entry.timestamp >=
                (datetime.now().replace(hour=datetime.now().hour-1)).isoformat()
            ]

            error_types = {}
            for entry in recent_errors:
                component_op = f"{entry.component}.{entry.operation}"
                if component_op not in error_types:
                    error_types[component_op] = []
                error_types[component_op].append({
                    "timestamp": entry.timestamp,
                    "message": entry.message,
                    "error": entry.error
                })

            return {
                "total_errors_last_hour": len(recent_errors),
                "error_types": error_types,
                "top_error_operations": sorted(
                    self.error_counts.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:10]
            }

# Global logger registry
_agent_loggers: Dict[str, AgentLogger] = {}

def get_agent_logger(agent_id: str, log_level: str = "INFO") -> AgentLogger:
    """Get or create agent logger instance"""
    global _agent_loggers

    if agent_id not in _agent_loggers:
        _agent_loggers[agent_id] = AgentLogger(agent_id, log_level)

    return _agent_loggers[agent_id]

def cleanup_old_logs(days_old: int = 30):
    """Clean up old log files"""
    log_dir = Path("logs/agent")
    if not log_dir.exists():
        return

    from datetime import timedelta
    cutoff_date = datetime.now() - timedelta(days=days_old)

    removed_count = 0
    for log_file in log_dir.glob("*.log"):
        try:
            file_date = datetime.fromtimestamp(log_file.stat().st_mtime)
            if file_date < cutoff_date:
                log_file.unlink()
                removed_count += 1
        except Exception as e:
            logging.warning(f"Failed to remove old log file {log_file}: {e}")

    if removed_count > 0:
        logging.info(f"Cleaned up {removed_count} old log files")