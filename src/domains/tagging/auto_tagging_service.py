"""
Auto-tagging service for automatically applying tags to data quality issues
based on rules and patterns.
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import re

from domains.tagging.models.tag_models import TagSource, ApplyTagRequest
from domains.tagging.services.tag_service import TagService

logger = logging.getLogger(__name__)


class AutoTaggingRule:
    """Represents a single auto-tagging rule"""
    
    def __init__(self, name: str, condition_func, tag_name: str, confidence: float = 0.9, 
                 description: str = "", category: str = "auto"):
        self.name = name
        self.condition_func = condition_func
        self.tag_name = tag_name
        self.confidence = confidence
        self.description = description
        self.category = category


class AutoTaggingService:
    """Service for automatically applying tags to data quality issues"""
    
    def __init__(self, tag_service: TagService):
        self.tag_service = tag_service
        self.rules = []
        self._initialize_rules()
    
    def _initialize_rules(self):
        """Initialize all auto-tagging rules"""
        self.rules = [
            # Severity-based rules
            AutoTaggingRule(
                name="critical_severity_tagging",
                condition_func=lambda issue: issue.get('severity') == 'critical',
                tag_name="Critical",
                confidence=0.95,
                description="Auto-apply Critical tag for critical severity issues"
            ),
            
            AutoTaggingRule(
                name="high_severity_tagging", 
                condition_func=lambda issue: issue.get('severity') == 'high',
                tag_name="High",
                confidence=0.95,
                description="Auto-apply High tag for high severity issues"
            ),
            
            AutoTaggingRule(
                name="medium_severity_tagging",
                condition_func=lambda issue: issue.get('severity') == 'medium',
                tag_name="Medium", 
                confidence=0.95,
                description="Auto-apply Medium tag for medium severity issues"
            ),
            
            AutoTaggingRule(
                name="low_severity_tagging",
                condition_func=lambda issue: issue.get('severity') == 'low',
                tag_name="Low",
                confidence=0.95,
                description="Auto-apply Low tag for low severity issues"
            ),
            
            # Vendor source rules
            AutoTaggingRule(
                name="polygon_source_tagging",
                condition_func=lambda issue: issue.get('vendor_source', '').lower() == 'polygon',
                tag_name="Polygon",
                confidence=0.98,
                description="Auto-apply Polygon tag for Polygon data issues"
            ),
            
            AutoTaggingRule(
                name="tiingo_source_tagging",
                condition_func=lambda issue: issue.get('vendor_source', '').lower() == 'tiingo',
                tag_name="Tiingo",
                confidence=0.98,
                description="Auto-apply Tiingo tag for Tiingo data issues"
            ),
            
            AutoTaggingRule(
                name="eodhd_source_tagging",
                condition_func=lambda issue: issue.get('vendor_source', '').lower() == 'eodhd',
                tag_name="EODHD",
                confidence=0.98,
                description="Auto-apply EODHD tag for EODHD data issues"
            ),
            
            AutoTaggingRule(
                name="firstrate_source_tagging",
                condition_func=lambda issue: issue.get('vendor_source', '').lower() == 'firstrate',
                tag_name="FirstRate",
                confidence=0.98,
                description="Auto-apply FirstRate tag for FirstRate data issues"
            ),
            
            # Issue type rules
            AutoTaggingRule(
                name="missing_data_tagging",
                condition_func=lambda issue: (
                    'missing' in issue.get('issue_type', '').lower() or
                    'gap' in issue.get('issue_type', '').lower() or
                    'missing' in issue.get('description', '').lower()
                ),
                tag_name="Data Gap",
                confidence=0.85,
                description="Auto-apply Data Gap tag for missing data issues"
            ),
            
            AutoTaggingRule(
                name="price_anomaly_tagging",
                condition_func=lambda issue: (
                    'price' in issue.get('issue_type', '').lower() and
                    ('anomaly' in issue.get('issue_type', '').lower() or
                     'unusual' in issue.get('description', '').lower() or
                     'spike' in issue.get('description', '').lower())
                ),
                tag_name="Price Anomaly",
                confidence=0.80,
                description="Auto-apply Price Anomaly tag for price-related anomalies"
            ),
            
            AutoTaggingRule(
                name="volume_anomaly_tagging",
                condition_func=lambda issue: (
                    'volume' in issue.get('issue_type', '').lower() and
                    ('spike' in issue.get('description', '').lower() or
                     'anomaly' in issue.get('issue_type', '').lower() or
                     'unusual' in issue.get('description', '').lower())
                ),
                tag_name="Volume Spike",
                confidence=0.80,
                description="Auto-apply Volume Spike tag for volume anomalies"
            ),
            
            AutoTaggingRule(
                name="duplicate_data_tagging",
                condition_func=lambda issue: (
                    'duplicate' in issue.get('issue_type', '').lower() or
                    'duplicate' in issue.get('description', '').lower() or
                    'redundant' in issue.get('description', '').lower()
                ),
                tag_name="Duplicate Data",
                confidence=0.85,
                description="Auto-apply Duplicate Data tag for duplicate data issues"
            ),
            
            # Impact assessment rules
            AutoTaggingRule(
                name="system_wide_impact_tagging",
                condition_func=lambda issue: (
                    'system' in issue.get('description', '').lower() or
                    issue.get('severity') == 'critical' and 
                    len(issue.get('symbol', '')) == 0  # System-wide issues often have no specific symbol
                ),
                tag_name="System Wide",
                confidence=0.75,
                description="Auto-apply System Wide tag for system-level issues"
            ),
            
            AutoTaggingRule(
                name="trading_halt_impact_tagging",
                condition_func=lambda issue: (
                    'halt' in issue.get('description', '').lower() or
                    'suspended' in issue.get('description', '').lower() or
                    'trading' in issue.get('description', '').lower()
                ),
                tag_name="Trading Halt",
                confidence=0.70,
                description="Auto-apply Trading Halt tag for trading-related issues"
            ),
            
            # Quality aspect rules
            AutoTaggingRule(
                name="accuracy_quality_tagging",
                condition_func=lambda issue: (
                    'accuracy' in issue.get('description', '').lower() or
                    'incorrect' in issue.get('description', '').lower() or
                    'wrong' in issue.get('description', '').lower() or
                    issue.get('expected_value') != issue.get('actual_value')
                ),
                tag_name="Accuracy",
                confidence=0.75,
                description="Auto-apply Accuracy tag for data accuracy issues"
            ),
            
            AutoTaggingRule(
                name="completeness_quality_tagging",
                condition_func=lambda issue: (
                    'complete' in issue.get('description', '').lower() or
                    'missing' in issue.get('description', '').lower() or
                    'incomplete' in issue.get('description', '').lower()
                ),
                tag_name="Completeness",
                confidence=0.75,
                description="Auto-apply Completeness tag for data completeness issues"
            ),
            
            AutoTaggingRule(
                name="timeliness_quality_tagging",
                condition_func=lambda issue: (
                    'late' in issue.get('description', '').lower() or
                    'delay' in issue.get('description', '').lower() or
                    'timing' in issue.get('description', '').lower() or
                    'stale' in issue.get('description', '').lower()
                ),
                tag_name="Timeliness",
                confidence=0.75,
                description="Auto-apply Timeliness tag for data timeliness issues"
            ),
            
            AutoTaggingRule(
                name="consistency_quality_tagging",
                condition_func=lambda issue: (
                    'consistent' in issue.get('description', '').lower() or
                    'mismatch' in issue.get('description', '').lower() or
                    'conflict' in issue.get('description', '').lower()
                ),
                tag_name="Consistency",
                confidence=0.75,
                description="Auto-apply Consistency tag for data consistency issues"
            ),
            
            # Time-based rules
            AutoTaggingRule(
                name="recent_issue_tagging",
                condition_func=lambda issue: self._is_recent_issue(issue),
                tag_name="Open",
                confidence=0.90,
                description="Auto-apply Open tag for recent issues"
            ),
            
            # Advanced pattern rules
            AutoTaggingRule(
                name="high_volume_symbol_tagging",
                condition_func=lambda issue: self._is_high_volume_symbol(issue),
                tag_name="High Volume Symbol",
                confidence=0.60,
                description="Auto-apply High Volume Symbol tag for issues affecting major symbols"
            ),
        ]
        
        logger.info(f"Initialized {len(self.rules)} auto-tagging rules")
    
    def _is_recent_issue(self, issue: Dict[str, Any]) -> bool:
        """Check if issue was created recently (within last 24 hours)"""
        try:
            created_at_str = issue.get('created_at')
            if not created_at_str:
                return False
                
            if isinstance(created_at_str, str):
                # Parse ISO format datetime
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            else:
                created_at = created_at_str
                
            return created_at >= datetime.now() - timedelta(hours=24)
        except Exception as e:
            logger.warning(f"Error parsing date for recent issue check: {e}")
            return False
    
    def _is_high_volume_symbol(self, issue: Dict[str, Any]) -> bool:
        """Check if the symbol is a high-volume/major symbol"""
        major_symbols = {
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
            'SPY', 'QQQ', 'IWM', 'VTI', 'VOO', 'BRK-A', 'BRK-B', 'JPM',
            'JNJ', 'WMT', 'PG', 'UNH', 'HD', 'BAC', 'MA', 'DIS', 'ADBE'
        }
        symbol = issue.get('symbol', '').upper()
        return symbol in major_symbols
    
    async def auto_tag_issue(self, issue_id: int, issue_data: Dict[str, Any]) -> List[str]:
        """
        Apply auto-tagging rules to a single issue.
        Returns list of applied tag names.
        """
        applied_tags = []
        
        try:
            for rule in self.rules:
                try:
                    if rule.condition_func(issue_data):
                        # Check if tag already exists for this issue
                        existing_tags = await self.tag_service.get_entity_tags("data_quality_issues", issue_id)
                        if any(tag.name == rule.tag_name for tag in existing_tags):
                            logger.debug(f"Tag '{rule.tag_name}' already exists for issue {issue_id}")
                            continue
                        
                        # Apply the tag
                        apply_request = ApplyTagRequest(
                            entity_type="data_quality_issues",
                            entity_id=issue_id,
                            tag_id=0,  # Will be resolved by service
                            confidence_score=rule.confidence,
                            source=TagSource.AUTO,
                            metadata={
                                "rule_name": rule.name,
                                "rule_description": rule.description,
                                "auto_tagged_at": datetime.now().isoformat()
                            }
                        )
                        
                        # Find tag by name
                        all_tags = await self.tag_service.get_all_tags()
                        matching_tag = next((tag for tag in all_tags if tag.name == rule.tag_name), None)
                        
                        if matching_tag:
                            apply_request.tag_id = matching_tag.id
                            await self.tag_service.apply_tag_to_entity(apply_request)
                            applied_tags.append(rule.tag_name)
                            logger.info(f"Applied auto-tag '{rule.tag_name}' to issue {issue_id} (rule: {rule.name})")
                        else:
                            logger.warning(f"Tag '{rule.tag_name}' not found for rule '{rule.name}'")
                            
                except Exception as e:
                    logger.error(f"Error applying rule '{rule.name}' to issue {issue_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in auto_tag_issue for issue {issue_id}: {e}")
        
        return applied_tags
    
    async def auto_tag_issues_batch(self, issues: List[Dict[str, Any]]) -> Dict[int, List[str]]:
        """
        Apply auto-tagging rules to a batch of issues.
        Returns dict mapping issue_id to list of applied tag names.
        """
        results = {}
        
        for issue in issues:
            issue_id = issue.get('id')
            if issue_id:
                applied_tags = await self.auto_tag_issue(issue_id, issue)
                if applied_tags:
                    results[issue_id] = applied_tags
        
        logger.info(f"Auto-tagged {len(results)} issues out of {len(issues)} processed")
        return results
    
    async def get_auto_tag_suggestions(self, issue_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get auto-tag suggestions for an issue without applying them.
        Returns list of suggested tags with confidence scores and explanations.
        """
        suggestions = []
        
        for rule in self.rules:
            try:
                if rule.condition_func(issue_data):
                    suggestions.append({
                        "tag_name": rule.tag_name,
                        "confidence_score": rule.confidence,
                        "source": TagSource.AUTO.value,
                        "explanation": f"Auto-suggested by rule: {rule.description}",
                        "rule_name": rule.name
                    })
            except Exception as e:
                logger.error(f"Error evaluating rule '{rule.name}': {e}")
                continue
        
        # Sort by confidence score descending
        suggestions.sort(key=lambda x: x['confidence_score'], reverse=True)
        return suggestions
    
    def add_custom_rule(self, rule: AutoTaggingRule):
        """Add a custom auto-tagging rule"""
        self.rules.append(rule)
        logger.info(f"Added custom auto-tagging rule: {rule.name}")
    
    def remove_rule(self, rule_name: str) -> bool:
        """Remove an auto-tagging rule by name"""
        original_count = len(self.rules)
        self.rules = [rule for rule in self.rules if rule.name != rule_name]
        removed = len(self.rules) < original_count
        
        if removed:
            logger.info(f"Removed auto-tagging rule: {rule_name}")
        else:
            logger.warning(f"Rule not found for removal: {rule_name}")
            
        return removed
    
    def get_all_rules(self) -> List[Dict[str, Any]]:
        """Get information about all auto-tagging rules"""
        return [
            {
                "name": rule.name,
                "tag_name": rule.tag_name,
                "confidence": rule.confidence,
                "description": rule.description,
                "category": rule.category
            }
            for rule in self.rules
        ]
    
    async def run_auto_tagging_job(self, limit: int = 100, min_hours_old: int = 1) -> Dict[str, Any]:
        """
        Run a batch auto-tagging job on recent untagged issues.
        Returns summary of tagging results.
        """
        logger.info(f"Starting auto-tagging job (limit={limit}, min_hours_old={min_hours_old})")
        
        try:
            # Get recent issues that might need auto-tagging
            # This would typically query the database for recent issues
            # For now, we'll return a placeholder implementation
            
            results = {
                "job_started_at": datetime.now().isoformat(),
                "rules_applied": len(self.rules),
                "issues_processed": 0,
                "issues_tagged": 0,
                "tags_applied": 0,
                "errors": 0,
                "summary": "Auto-tagging job completed successfully"
            }
            
            logger.info(f"Auto-tagging job completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error in auto-tagging job: {e}")
            return {
                "job_started_at": datetime.now().isoformat(),
                "error": str(e),
                "status": "failed"
            }