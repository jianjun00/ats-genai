#!/usr/bin/env python3
"""
Prometheus Metrics Collection for ATS Data Quality Agent
Provides comprehensive metrics for monitoring and alerting
"""

import time
from typing import Dict, Any, Optional
from datetime import datetime
import json
import threading

class PrometheusMetricsCollector:
    """Collect and expose metrics in Prometheus format"""
    
    def __init__(self):
        self.metrics = {}
        self.lock = threading.Lock()
        self.start_time = time.time()
        
        # Initialize core metrics
        self._initialize_metrics()
    
    def _initialize_metrics(self):
        """Initialize default metrics"""
        with self.lock:
            self.metrics = {
                # Agent status metrics
                'ats_data_quality_agent_status': 0,  # 0=inactive, 1=active
                'ats_data_quality_agent_restarts_total': 0,
                'ats_data_quality_agent_uptime_seconds': 0,
                'ats_data_quality_agent_memory_usage_bytes': 0,
                
                # Issue detection metrics
                'ats_data_quality_issues_detected_total': 0,
                'ats_data_quality_issues_resolved_total': 0,
                'ats_data_quality_issues_total': 0,
                'ats_data_quality_issues_by_severity_critical': 0,
                'ats_data_quality_issues_by_severity_high': 0,
                'ats_data_quality_issues_by_severity_medium': 0,
                'ats_data_quality_issues_by_severity_low': 0,
                
                # Symbol tracking
                'ats_data_quality_symbols_affected': 0,
                'ats_data_quality_critical_symbols_affected': 0,
                
                # Quality metrics
                'ats_data_quality_overall_score': 100,
                'ats_data_quality_scan_duration_seconds': 0,
                
                # Vendor-specific metrics
                'ats_data_quality_issues_by_vendor_polygon': 0,
                'ats_data_quality_issues_by_vendor_tiingo': 0,
                'ats_data_quality_issues_by_vendor_eodhd': 0,
                'ats_data_quality_vendor_availability_polygon': 1.0,
                'ats_data_quality_vendor_availability_tiingo': 1.0,
                'ats_data_quality_vendor_availability_eodhd': 1.0,
                
                # API metrics
                'ats_data_quality_api_requests_total': 0,
                'ats_data_quality_api_errors_total': 0,
                'ats_data_quality_api_request_duration_seconds': 0,
                
                # Database metrics
                'ats_data_quality_agent_db_errors_total': 0,
                'ats_data_quality_agent_db_connections_active': 0,
            }
    
    def update_agent_status(self, status: str, agent_id: Optional[str] = None):
        """Update agent status metrics"""
        with self.lock:
            self.metrics['ats_data_quality_agent_status'] = 1 if status == 'active' else 0
            self.metrics['ats_data_quality_agent_uptime_seconds'] = time.time() - self.start_time
    
    def record_agent_restart(self):
        """Record agent restart event"""
        with self.lock:
            self.metrics['ats_data_quality_agent_restarts_total'] += 1
    
    def update_memory_usage(self, bytes_used: int):
        """Update memory usage metrics"""
        with self.lock:
            self.metrics['ats_data_quality_agent_memory_usage_bytes'] = bytes_used
    
    def update_issue_metrics(self, issues_summary: Dict[str, Any]):
        """Update issue-related metrics from API response"""
        with self.lock:
            summary = issues_summary.get('summary', {})
            
            # Update total counts
            self.metrics['ats_data_quality_issues_total'] = summary.get('total_issues', 0)
            self.metrics['ats_data_quality_issues_by_severity_critical'] = summary.get('critical', 0)
            self.metrics['ats_data_quality_issues_by_severity_high'] = summary.get('high', 0)
            self.metrics['ats_data_quality_issues_by_severity_medium'] = summary.get('medium', 0)
            self.metrics['ats_data_quality_issues_by_severity_low'] = summary.get('low', 0)
            
            # Calculate quality score based on issues
            total_issues = summary.get('total_issues', 0)
            critical_issues = summary.get('critical', 0)
            high_issues = summary.get('high', 0)
            
            if total_issues == 0:
                quality_score = 100
            else:
                # Weight critical and high issues more heavily
                weighted_issues = critical_issues * 3 + high_issues * 2 + summary.get('medium', 0)
                quality_score = max(0, 100 - (weighted_issues * 0.01))  # Rough scoring algorithm
            
            self.metrics['ats_data_quality_overall_score'] = quality_score
    
    def record_issue_detection(self, count: int = 1):
        """Record new issues detected"""
        with self.lock:
            self.metrics['ats_data_quality_issues_detected_total'] += count
    
    def record_issue_resolution(self, count: int = 1):
        """Record issues resolved"""
        with self.lock:
            self.metrics['ats_data_quality_issues_resolved_total'] += count
    
    def update_symbols_affected(self, symbols_count: int, critical_symbols: int = 0):
        """Update affected symbols metrics"""
        with self.lock:
            self.metrics['ats_data_quality_symbols_affected'] = symbols_count
            self.metrics['ats_data_quality_critical_symbols_affected'] = critical_symbols
    
    def update_scan_duration(self, duration_seconds: float):
        """Update scan duration metrics"""
        with self.lock:
            self.metrics['ats_data_quality_scan_duration_seconds'] = duration_seconds
    
    def update_vendor_metrics(self, vendor: str, issues_count: int, availability: float = 1.0):
        """Update vendor-specific metrics"""
        with self.lock:
            vendor_key = f'ats_data_quality_issues_by_vendor_{vendor.lower()}'
            availability_key = f'ats_data_quality_vendor_availability_{vendor.lower()}'
            
            if vendor_key in self.metrics:
                self.metrics[vendor_key] = issues_count
            if availability_key in self.metrics:
                self.metrics[availability_key] = availability
    
    def record_api_request(self, duration_seconds: float, error: bool = False):
        """Record API request metrics"""
        with self.lock:
            self.metrics['ats_data_quality_api_requests_total'] += 1
            if error:
                self.metrics['ats_data_quality_api_errors_total'] += 1
            # Simple running average for duration
            current_avg = self.metrics['ats_data_quality_api_request_duration_seconds']
            total_requests = self.metrics['ats_data_quality_api_requests_total']
            new_avg = ((current_avg * (total_requests - 1)) + duration_seconds) / total_requests
            self.metrics['ats_data_quality_api_request_duration_seconds'] = new_avg
    
    def record_db_error(self):
        """Record database error"""
        with self.lock:
            self.metrics['ats_data_quality_agent_db_errors_total'] += 1
    
    def update_db_connections(self, active_connections: int):
        """Update active database connections"""
        with self.lock:
            self.metrics['ats_data_quality_agent_db_connections_active'] = active_connections
    
    def get_prometheus_metrics(self) -> str:
        """Return metrics in Prometheus exposition format"""
        with self.lock:
            lines = []
            
            # Add help and type information for key metrics
            metric_info = {
                'ats_data_quality_agent_status': ('gauge', 'Agent status (0=inactive, 1=active)'),
                'ats_data_quality_issues_total': ('gauge', 'Total number of data quality issues'),
                'ats_data_quality_issues_detected_total': ('counter', 'Total issues detected since startup'),
                'ats_data_quality_overall_score': ('gauge', 'Overall data quality score (0-100)'),
                'ats_data_quality_symbols_affected': ('gauge', 'Number of symbols affected by issues'),
                'ats_data_quality_scan_duration_seconds': ('gauge', 'Time taken for last quality scan'),
            }
            
            for metric_name, metric_value in self.metrics.items():
                # Add help and type info for documented metrics
                if metric_name in metric_info:
                    metric_type, help_text = metric_info[metric_name]
                    lines.append(f'# HELP {metric_name} {help_text}')
                    lines.append(f'# TYPE {metric_name} {metric_type}')
                
                # Add the metric value
                lines.append(f'{metric_name} {metric_value}')
                lines.append('')  # Empty line between metrics
            
            return '\n'.join(lines)
    
    def get_health_metrics(self) -> Dict[str, Any]:
        """Return health check metrics"""
        with self.lock:
            return {
                'agent_active': self.metrics['ats_data_quality_agent_status'] == 1,
                'total_issues': self.metrics['ats_data_quality_issues_total'],
                'quality_score': self.metrics['ats_data_quality_overall_score'],
                'symbols_affected': self.metrics['ats_data_quality_symbols_affected'],
                'uptime_seconds': self.metrics['ats_data_quality_agent_uptime_seconds'],
                'last_updated': datetime.now().isoformat()
            }

# Global metrics collector instance
metrics_collector = PrometheusMetricsCollector()

def get_metrics_collector() -> PrometheusMetricsCollector:
    """Get the global metrics collector instance"""
    return metrics_collector