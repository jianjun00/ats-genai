"""
Helper module for recording metrics in mock testing scenarios.
"""
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class MockMetricsHelper:
    """Helper class to record metrics during mock testing"""
    
    @staticmethod
    def record_data_point_metrics(orchestrator: Any, 
                                  symbol: str, 
                                  date: datetime.date, 
                                  sources: Dict[str, bool],
                                  success: bool,
                                  execution_time: float) -> None:
        """
        Record metrics for a data point processing operation
        
        Args:
            orchestrator: The DataAgentOrchestrator instance
            symbol: The stock symbol being processed
            date: The date being processed
            sources: Dict mapping source names to success status
            success: Whether the overall operation was successful
            execution_time: Time taken to process the data point in seconds
        """
        if not hasattr(orchestrator, 'metrics') or not orchestrator.metrics:
            return
            
        # Record overall data point processing
        orchestrator.metrics.record_data_point_processed(success, execution_time)
        
        # Record source-specific metrics
        for source_name, source_success in sources.items():
            # Use a reasonable default time for successful sources
            source_time = 0.1 if source_success else 0.0
            orchestrator.metrics.record_source_result(source_name, source_success, source_time)
        
        # Record reconciliation metrics
        source_count = sum(1 for s in sources.values() if s)
        had_conflict = source_count > 1
        orchestrator.metrics.record_reconciliation(source_count, had_conflict)
        
    @staticmethod
    def record_batch_metrics(orchestrator: Any, 
                            batch_size: int, 
                            success_count: int,
                            execution_time: float) -> None:
        """
        Record metrics for a batch processing operation
        
        Args:
            orchestrator: The DataAgentOrchestrator instance
            batch_size: Size of the batch processed
            success_count: Number of successfully processed items
            execution_time: Time taken to process the batch in seconds
        """
        if not hasattr(orchestrator, 'metrics') or not orchestrator.metrics:
            return
            
        # Record batch metrics
        if hasattr(orchestrator.metrics, 'record_batch_processed'):
            orchestrator.metrics.record_batch_processed(batch_size, execution_time)
