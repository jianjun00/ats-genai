"""
Tag service for managing tags and entity-tag relationships
"""
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from domains.tagging.models.tag_models import (
    Tag, TagCategory, EntityTag, TagFilter, TaggedEntity, TagUsageStats,
    TagAnalytics, CreateTagRequest, ApplyTagRequest, BulkTagRequest,
    TagSuggestion, TagSource
)
from domains.tagging.repositories.tag_repository import TagRepository

logger = logging.getLogger(__name__)


class TagService:
    """Service for managing tags and entity relationships"""
    
    def __init__(self, tag_repository: TagRepository):
        self.repository = tag_repository
        self._auto_tagging_service = None
    
    async def get_all_tags(self, active_only: bool = True) -> List[Tag]:
        """Get all tags with optional filtering"""
        return await self.repository.get_tags(limit=1000)  # Get all tags, already filtered by active
    
    async def get_tags_by_category(self, category_id: int, active_only: bool = True) -> List[Tag]:
        """Get tags filtered by category"""
        return await self.repository.get_tags(category_id=category_id, limit=1000)
    
    async def get_all_categories(self) -> List[TagCategory]:
        """Get all tag categories"""
        return await self.repository.get_tag_categories()
    
    async def search_tags(self, query: str, limit: int = 50) -> List[Tag]:
        """Search tags by name or description"""
        return await self.repository.search_tags(query, limit=limit)
    
    async def create_tag(self, request: CreateTagRequest, user_id: Optional[str] = None) -> Tag:
        """Create a new tag"""
        logger.info(f"Creating new tag: {request.name}")
        
        # Generate slug from name if not provided
        slug = request.name.lower().replace(' ', '-').replace('_', '-')
        
        # Default color if not provided
        color = request.color or '#6c757d'
        
        tag_id = await self.repository.create_tag(
            name=request.name,
            slug=slug,
            description=request.description,
            color=color,
            category_id=request.category_id,
            metadata=request.metadata or {}
        )
        
        tag = await self.repository.get_tag_by_id(tag_id)
        logger.info(f"Created tag {tag.name} with ID {tag_id}")
        return tag
    
    async def apply_tag_to_entity(self, request: ApplyTagRequest, user_id: Optional[str] = None) -> EntityTag:
        """Apply a tag to an entity"""
        logger.info(f"Applying tag {request.tag_id} to {request.entity_type}:{request.entity_id}")
        
        entity_tag_id = await self.repository.apply_tag_to_entity(
            entity_type=request.entity_type,
            entity_id=request.entity_id,
            tag_id=request.tag_id,
            user_id=user_id or request.metadata.get('user_id') if request.metadata else None,
            confidence_score=request.confidence_score,
            source=request.source,
            metadata=request.metadata or {}
        )
        
        return await self.repository.get_entity_tag_by_id(entity_tag_id)
    
    async def bulk_apply_tags(self, request: BulkTagRequest, user_id: Optional[str] = None) -> List[EntityTag]:
        """Apply multiple tags to multiple entities"""
        logger.info(f"Bulk applying {len(request.tag_ids)} tags to {len(request.entity_ids)} entities")
        
        tasks = []
        for entity_id in request.entity_ids:
            for tag_id in request.tag_ids:
                apply_request = ApplyTagRequest(
                    entity_type=request.entity_type,
                    entity_id=entity_id,
                    tag_id=tag_id,
                    confidence_score=request.confidence_score,
                    source=request.source,
                    metadata=request.metadata
                )
                tasks.append(self.apply_tag_to_entity(apply_request, user_id))
        
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def remove_tag_from_entity(self, entity_type: str, entity_id: int, tag_id: int) -> bool:
        """Remove a tag from an entity"""
        logger.info(f"Removing tag {tag_id} from {entity_type}:{entity_id}")
        return await self.repository.remove_tag_from_entity(entity_type, entity_id, tag_id)
    
    async def get_entity_tags(self, entity_type: str, entity_id: int) -> List[Tag]:
        """Get all tags for a specific entity"""
        return await self.repository.get_entity_tags(entity_type, entity_id)
    
    async def get_tagged_entities(self, tag_filter: TagFilter) -> List[TaggedEntity]:
        """Get entities filtered by tags"""
        return await self.repository.get_tagged_entities(tag_filter)
    
    async def get_tag_usage_stats(self, limit: int = 100) -> List[TagUsageStats]:
        """Get tag usage statistics"""
        return await self.repository.get_tag_usage_stats(limit=limit)
    
    async def get_tag_analytics(self) -> TagAnalytics:
        """Get comprehensive tag analytics for dashboard"""
        logger.info("Generating tag analytics")
        
        # Get most used tags
        most_used = await self.repository.get_tag_usage_stats(limit=20)
        
        # Get category distribution
        categories = await self.repository.get_all_categories()
        category_dist = {}
        for category in categories:
            tags = await self.repository.get_tags_by_category(category.id)
            category_dist[category.name] = len(tags)
        
        # Get tagging trends (simplified - last 30 days)
        trends = await self.repository.get_tagging_trends(days=30)
        
        # Calculate entity coverage
        total_entities = await self.repository.get_total_entities_count()
        tagged_entities = await self.repository.get_tagged_entities_count()
        coverage = (tagged_entities / total_entities * 100) if total_entities > 0 else 0
        
        # Calculate average tags per entity
        avg_tags = await self.repository.get_average_tags_per_entity()
        
        # Get co-occurring tags
        co_occurring = await self.repository.get_co_occurring_tags(limit=10)
        
        return TagAnalytics(
            most_used_tags=most_used,
            tag_categories_distribution=category_dist,
            tagging_trends=trends,
            entity_coverage=coverage,
            avg_tags_per_entity=avg_tags,
            top_co_occurring_tags=co_occurring
        )
    
    async def suggest_tags_for_entity(self, entity_type: str, entity_id: int, limit: int = 5) -> List[TagSuggestion]:
        """Generate tag suggestions for an entity using various strategies"""
        logger.info(f"Generating tag suggestions for {entity_type}:{entity_id}")
        
        suggestions = []
        
        # Strategy 1: Rule-based suggestions for data quality issues
        if entity_type == "data_quality_issues":
            rule_suggestions = await self._get_rule_based_suggestions(entity_id)
            suggestions.extend(rule_suggestions)
        
        # Strategy 2: Similar entity tags
        similar_suggestions = await self._get_similar_entity_suggestions(entity_type, entity_id)
        suggestions.extend(similar_suggestions)
        
        # Strategy 3: Popular tags in category
        popular_suggestions = await self._get_popular_tag_suggestions(entity_type)
        suggestions.extend(popular_suggestions)
        
        # Sort by confidence and return top suggestions
        suggestions.sort(key=lambda x: x.confidence_score, reverse=True)
        return suggestions[:limit]
    
    async def _get_rule_based_suggestions(self, issue_id: int) -> List[TagSuggestion]:
        """Get rule-based tag suggestions for data quality issues"""
        suggestions = []
        
        # Get issue details to apply rules
        try:
            issue = await self.repository.get_issue_details(issue_id)
            if not issue:
                return suggestions
            
            # Rule: Suggest priority based on severity
            severity_to_priority = {
                'critical': 'Critical',
                'high': 'High', 
                'medium': 'Medium',
                'low': 'Low'
            }
            
            if issue.get('severity') in severity_to_priority:
                priority_tag = await self.repository.get_tag_by_name(severity_to_priority[issue['severity']])
                if priority_tag:
                    suggestions.append(TagSuggestion(
                        tag_id=priority_tag.id,
                        tag_name=priority_tag.name,
                        confidence_score=0.9,
                        source=TagSource.RULE,
                        explanation=f"Suggested based on issue severity: {issue['severity']}"
                    ))
            
            # Rule: Suggest source tag based on vendor_source
            if issue.get('vendor_source'):
                source_tag = await self.repository.get_tag_by_name(issue['vendor_source'].title())
                if source_tag:
                    suggestions.append(TagSuggestion(
                        tag_id=source_tag.id,
                        tag_name=source_tag.name,
                        confidence_score=0.95,
                        source=TagSource.RULE,
                        explanation=f"Suggested based on data vendor: {issue['vendor_source']}"
                    ))
            
            # Rule: Suggest type tag based on issue_type
            if issue.get('issue_type'):
                type_mapping = {
                    'missing_data': 'Data Gap',
                    'price_anomaly': 'Price Anomaly',
                    'volume_anomaly': 'Volume Spike',
                    'duplicate': 'Duplicate Data'
                }
                
                if issue['issue_type'] in type_mapping:
                    type_tag = await self.repository.get_tag_by_name(type_mapping[issue['issue_type']])
                    if type_tag:
                        suggestions.append(TagSuggestion(
                            tag_id=type_tag.id,
                            tag_name=type_tag.name,
                            confidence_score=0.85,
                            source=TagSource.RULE,
                            explanation=f"Suggested based on issue type: {issue['issue_type']}"
                        ))
                        
        except Exception as e:
            logger.warning(f"Error getting rule-based suggestions: {e}")
        
        return suggestions
    
    async def _get_similar_entity_suggestions(self, entity_type: str, entity_id: int) -> List[TagSuggestion]:
        """Get suggestions based on tags applied to similar entities"""
        suggestions = []
        
        try:
            # Get tags from similar entities (simplified approach)
            similar_tags = await self.repository.get_similar_entity_tags(entity_type, entity_id, limit=10)
            
            for tag_info in similar_tags:
                suggestions.append(TagSuggestion(
                    tag_id=tag_info['tag_id'],
                    tag_name=tag_info['tag_name'],
                    confidence_score=min(tag_info['usage_frequency'], 0.8),  # Cap at 0.8 for similarity
                    source=TagSource.AUTO,
                    explanation=f"Used by {tag_info['usage_count']} similar entities"
                ))
                
        except Exception as e:
            logger.warning(f"Error getting similar entity suggestions: {e}")
        
        return suggestions
    
    async def _get_popular_tag_suggestions(self, entity_type: str) -> List[TagSuggestion]:
        """Get suggestions based on popular tags for this entity type"""
        suggestions = []
        
        try:
            popular_tags = await self.repository.get_popular_tags_for_entity_type(entity_type, limit=5)
            
            for tag_info in popular_tags:
                suggestions.append(TagSuggestion(
                    tag_id=tag_info['tag_id'],
                    tag_name=tag_info['tag_name'],
                    confidence_score=0.3,  # Lower confidence for popularity-based suggestions
                    source=TagSource.AUTO,
                    explanation=f"Popular tag for {entity_type} (used {tag_info['usage_count']} times)"
                ))
                
        except Exception as e:
            logger.warning(f"Error getting popular tag suggestions: {e}")
        
        return suggestions
    
    async def refresh_tag_analytics(self) -> bool:
        """Refresh the materialized view for tag analytics"""
        logger.info("Refreshing tag analytics materialized view")
        return await self.repository.refresh_tag_usage_summary()
    
    async def cleanup_unused_tags(self, dry_run: bool = True) -> List[int]:
        """Clean up tags with zero usage"""
        logger.info(f"Cleaning up unused tags (dry_run={dry_run})")
        unused_tags = await self.repository.get_unused_tags()
        
        if not dry_run:
            for tag in unused_tags:
                await self.repository.delete_tag(tag.id)
                logger.info(f"Deleted unused tag: {tag.name}")
        
        return [tag.id for tag in unused_tags]
    
    def get_auto_tagging_service(self):
        """Get auto-tagging service instance (lazy initialization)"""
        if self._auto_tagging_service is None:
            from domains.tagging.services.auto_tagging_service import AutoTaggingService
            self._auto_tagging_service = AutoTaggingService(self)
        return self._auto_tagging_service
    
    async def auto_tag_issue(self, issue_id: int, issue_data: Dict[str, Any]) -> List[str]:
        """Apply auto-tagging rules to a single issue"""
        auto_tagging = self.get_auto_tagging_service()
        return await auto_tagging.auto_tag_issue(issue_id, issue_data)
    
    async def get_auto_tag_suggestions_enhanced(self, entity_type: str, entity_id: int, limit: int = 5) -> List[TagSuggestion]:
        """Enhanced tag suggestions combining rule-based, ML, and auto-tagging approaches"""
        suggestions = []
        
        # Get original suggestions
        original_suggestions = await self.suggest_tags_for_entity(entity_type, entity_id, limit)
        suggestions.extend(original_suggestions)
        
        # Get auto-tagging suggestions if this is a data quality issue
        if entity_type == "data_quality_issues":
            try:
                # Get issue details for auto-tagging
                issue_details = await self.repository.get_issue_details(entity_id)
                if issue_details:
                    auto_tagging = self.get_auto_tagging_service()
                    auto_suggestions = await auto_tagging.get_auto_tag_suggestions(issue_details)
                    
                    # Convert to TagSuggestion objects
                    for auto_suggestion in auto_suggestions:
                        # Find tag by name
                        all_tags = await self.get_all_tags()
                        matching_tag = next((tag for tag in all_tags if tag.name == auto_suggestion['tag_name']), None)
                        
                        if matching_tag and not any(s.tag_id == matching_tag.id for s in suggestions):
                            suggestions.append(TagSuggestion(
                                tag_id=matching_tag.id,
                                tag_name=matching_tag.name,
                                confidence_score=auto_suggestion['confidence_score'],
                                source=TagSource.AUTO,
                                explanation=auto_suggestion['explanation']
                            ))
            except Exception as e:
                logger.warning(f"Error getting auto-tag suggestions: {e}")
        
        # Sort by confidence and return top suggestions
        suggestions.sort(key=lambda x: x.confidence_score, reverse=True)
        return suggestions[:limit]