"""
FastAPI routes for tag management
"""
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Depends, Body
from pydantic import BaseModel
import logging

from domains.tagging.models.tag_models import (
    Tag, TagCategory, EntityTag, TagFilter, TaggedEntity, TagUsageStats,
    TagAnalytics, CreateTagRequest, ApplyTagRequest, BulkTagRequest,
    TagSuggestion, TagSource
)
from domains.tagging.services.tag_service import TagService
from domains.tagging.repositories.tag_repository import TagRepository
from infrastructure.database.connection_manager import get_database_connection

logger = logging.getLogger(__name__)

# Pydantic models for API requests/responses
class TagResponse(BaseModel):
    id: int
    name: str
    slug: str
    description: Optional[str]
    color: str
    category_id: Optional[int]
    category_name: Optional[str]
    created_at: str
    updated_at: str
    usage_count: int
    is_system_tag: bool
    is_active: bool
    metadata: Dict[str, Any]

class CategoryResponse(BaseModel):
    id: int
    name: str
    slug: str
    description: Optional[str]
    color: str
    icon: str
    parent_id: Optional[int]
    sort_order: int
    created_at: str
    updated_at: str

class EntityTagResponse(BaseModel):
    id: int
    entity_type: str
    entity_id: int
    tag: TagResponse
    tagged_by_user_id: Optional[str]
    tagged_at: str
    confidence_score: float
    source: str
    metadata: Dict[str, Any]

class TaggedEntityResponse(BaseModel):
    entity_type: str
    entity_id: int
    tags: List[TagResponse]
    total_tags: int

class TagFilterRequest(BaseModel):
    entity_type: str
    tag_ids: Optional[List[int]] = None
    categories: Optional[List[int]] = None
    symbols: Optional[List[str]] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    search: Optional[str] = None
    match_mode: str = "ANY"
    limit: int = 50
    offset: int = 0

class CreateTagRequestModel(BaseModel):
    name: str
    description: Optional[str] = None
    category_id: Optional[int] = None
    color: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class ApplyTagRequestModel(BaseModel):
    entity_type: str
    entity_id: int
    tag_id: int
    confidence_score: float = 1.0
    source: str = "manual"
    metadata: Optional[Dict[str, Any]] = None

class BulkTagRequestModel(BaseModel):
    entity_type: str
    entity_ids: List[int]
    tag_ids: List[int]
    confidence_score: float = 1.0
    source: str = "manual"
    metadata: Optional[Dict[str, Any]] = None

# Router setup
tag_router = APIRouter(prefix="/api/tags", tags=["Tags"])

# Dependency to get tag service
async def get_tag_service() -> TagService:
    """Get tag service instance"""
    try:
        connection = await get_database_connection("dev")  # TODO: Make environment configurable
        repository = TagRepository(connection)
        return TagService(repository)
    except Exception as e:
        logger.error(f"Failed to create tag service: {e}")
        raise HTTPException(status_code=500, detail="Failed to initialize tag service")

def _convert_tag_to_response(tag: Tag, category_name: Optional[str] = None) -> TagResponse:
    """Convert Tag model to TagResponse"""
    return TagResponse(
        id=tag.id,
        name=tag.name,
        slug=tag.slug,
        description=tag.description,
        color=tag.color,
        category_id=tag.category_id,
        category_name=category_name or (tag.category.name if tag.category else None),
        created_at=tag.created_at.isoformat(),
        updated_at=tag.updated_at.isoformat(),
        usage_count=tag.usage_count,
        is_system_tag=tag.is_system_tag,
        is_active=tag.is_active,
        metadata=tag.metadata
    )

def _convert_category_to_response(category: TagCategory) -> CategoryResponse:
    """Convert TagCategory model to CategoryResponse"""
    return CategoryResponse(
        id=category.id,
        name=category.name,
        slug=category.slug,
        description=category.description,
        color=category.color,
        icon=category.icon,
        parent_id=category.parent_id,
        sort_order=category.sort_order,
        created_at=category.created_at.isoformat(),
        updated_at=category.updated_at.isoformat()
    )

# Tag Management Endpoints
@tag_router.get("/", response_model=List[TagResponse])
async def get_tags(
    active_only: bool = Query(True, description="Only return active tags"),
    category_id: Optional[int] = Query(None, description="Filter by category ID"),
    search: Optional[str] = Query(None, description="Search tags by name or description"),
    limit: int = Query(100, description="Maximum number of tags to return"),
    tag_service: TagService = Depends(get_tag_service)
) -> List[TagResponse]:
    """Get all tags with optional filtering"""
    try:
        if search:
            tags = await tag_service.search_tags(search, limit=limit)
        elif category_id:
            tags = await tag_service.get_tags_by_category(category_id, active_only=active_only)
        else:
            tags = await tag_service.get_all_tags(active_only=active_only)
        
        return [_convert_tag_to_response(tag) for tag in tags[:limit]]
    except Exception as e:
        logger.error(f"Error getting tags: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@tag_router.get("/categories", response_model=List[CategoryResponse])
async def get_categories(
    tag_service: TagService = Depends(get_tag_service)
) -> List[CategoryResponse]:
    """Get all tag categories"""
    try:
        categories = await tag_service.get_all_categories()
        return [_convert_category_to_response(cat) for cat in categories]
    except Exception as e:
        logger.error(f"Error getting categories: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@tag_router.post("/", response_model=TagResponse)
async def create_tag(
    request: CreateTagRequestModel,
    tag_service: TagService = Depends(get_tag_service)
) -> TagResponse:
    """Create a new tag"""
    try:
        create_request = CreateTagRequest(
            name=request.name,
            description=request.description,
            category_id=request.category_id,
            color=request.color,
            metadata=request.metadata
        )
        tag = await tag_service.create_tag(create_request)
        return _convert_tag_to_response(tag)
    except Exception as e:
        logger.error(f"Error creating tag: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Entity Tag Management
@tag_router.post("/apply", response_model=EntityTagResponse)
async def apply_tag_to_entity(
    request: ApplyTagRequestModel,
    tag_service: TagService = Depends(get_tag_service)
) -> EntityTagResponse:
    """Apply a tag to an entity"""
    try:
        apply_request = ApplyTagRequest(
            entity_type=request.entity_type,
            entity_id=request.entity_id,
            tag_id=request.tag_id,
            confidence_score=request.confidence_score,
            source=TagSource(request.source),
            metadata=request.metadata
        )
        entity_tag = await tag_service.apply_tag_to_entity(apply_request)
        
        # Get tag details for response
        tag = await tag_service.repository.get_tag_by_id(entity_tag.tag_id)
        tag_response = _convert_tag_to_response(tag)
        
        return EntityTagResponse(
            id=entity_tag.id,
            entity_type=request.entity_type,
            entity_id=entity_tag.entity_id,
            tag=tag_response,
            tagged_by_user_id=entity_tag.tagged_by_user_id,
            tagged_at=entity_tag.tagged_at.isoformat(),
            confidence_score=entity_tag.confidence_score,
            source=entity_tag.source.value,
            metadata=entity_tag.metadata
        )
    except Exception as e:
        logger.error(f"Error applying tag: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@tag_router.post("/bulk-apply")
async def bulk_apply_tags(
    request: BulkTagRequestModel,
    tag_service: TagService = Depends(get_tag_service)
):
    """Apply multiple tags to multiple entities"""
    try:
        bulk_request = BulkTagRequest(
            entity_type=request.entity_type,
            entity_ids=request.entity_ids,
            tag_ids=request.tag_ids,
            confidence_score=request.confidence_score,
            source=TagSource(request.source),
            metadata=request.metadata
        )
        results = await tag_service.bulk_apply_tags(bulk_request)
        
        success_count = len([r for r in results if not isinstance(r, Exception)])
        error_count = len(results) - success_count
        
        return {
            "total_operations": len(results),
            "successful": success_count,
            "failed": error_count,
            "message": f"Applied tags: {success_count} successful, {error_count} failed"
        }
    except Exception as e:
        logger.error(f"Error bulk applying tags: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@tag_router.delete("/entity/{entity_type}/{entity_id}/tag/{tag_id}")
async def remove_tag_from_entity(
    entity_type: str,
    entity_id: int,
    tag_id: int,
    tag_service: TagService = Depends(get_tag_service)
):
    """Remove a tag from an entity"""
    try:
        success = await tag_service.remove_tag_from_entity(entity_type, entity_id, tag_id)
        if success:
            return {"message": "Tag removed successfully"}
        else:
            raise HTTPException(status_code=404, detail="Tag relationship not found")
    except Exception as e:
        logger.error(f"Error removing tag: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@tag_router.get("/entity/{entity_type}/{entity_id}", response_model=List[TagResponse])
async def get_entity_tags(
    entity_type: str,
    entity_id: int,
    tag_service: TagService = Depends(get_tag_service)
) -> List[TagResponse]:
    """Get all tags for a specific entity"""
    try:
        tags = await tag_service.get_entity_tags(entity_type, entity_id)
        return [_convert_tag_to_response(tag) for tag in tags]
    except Exception as e:
        logger.error(f"Error getting entity tags: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Entity Filtering and Search
@tag_router.post("/search-entities", response_model=List[TaggedEntityResponse])
async def search_entities_by_tags(
    filter_request: TagFilterRequest,
    tag_service: TagService = Depends(get_tag_service)
) -> List[TaggedEntityResponse]:
    """Search entities by tag filters"""
    try:
        from datetime import datetime
        
        tag_filter = TagFilter(
            entity_type=filter_request.entity_type,
            tag_ids=filter_request.tag_ids,
            categories=filter_request.categories,
            symbols=filter_request.symbols,
            date_from=datetime.fromisoformat(filter_request.date_from) if filter_request.date_from else None,
            date_to=datetime.fromisoformat(filter_request.date_to) if filter_request.date_to else None,
            search=filter_request.search,
            match_mode=filter_request.match_mode,
            limit=filter_request.limit,
            offset=filter_request.offset
        )
        
        tagged_entities = await tag_service.get_tagged_entities(tag_filter)
        
        return [
            TaggedEntityResponse(
                entity_type=entity.entity_type,
                entity_id=entity.entity_id,
                tags=[_convert_tag_to_response(tag) for tag in entity.tags],
                total_tags=entity.total_tags
            )
            for entity in tagged_entities
        ]
    except Exception as e:
        logger.error(f"Error searching entities: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Analytics and Suggestions
@tag_router.get("/analytics")
async def get_tag_analytics(
    tag_service: TagService = Depends(get_tag_service)
):
    """Get tag analytics for dashboard"""
    try:
        analytics = await tag_service.get_tag_analytics()
        
        return {
            "most_used_tags": [
                {
                    "tag_id": stat.tag_id,
                    "tag_name": stat.tag_name,
                    "total_usage": stat.total_usage,
                    "unique_entities": stat.unique_entities,
                    "entity_types_count": stat.entity_types_count,
                    "avg_confidence": float(stat.avg_confidence) if stat.avg_confidence else 0,
                    "last_used": stat.last_used.isoformat() if stat.last_used else None,
                    "active_days_last_90": stat.active_days_last_90
                }
                for stat in analytics.most_used_tags
            ],
            "tag_categories_distribution": analytics.tag_categories_distribution,
            "tagging_trends": analytics.tagging_trends,
            "entity_coverage": round(analytics.entity_coverage, 2),
            "avg_tags_per_entity": round(analytics.avg_tags_per_entity, 2),
            "top_co_occurring_tags": analytics.top_co_occurring_tags
        }
    except Exception as e:
        logger.error(f"Error getting tag analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@tag_router.get("/suggestions/{entity_type}/{entity_id}")
async def get_tag_suggestions(
    entity_type: str,
    entity_id: int,
    limit: int = Query(5, description="Maximum number of suggestions"),
    tag_service: TagService = Depends(get_tag_service)
):
    """Get tag suggestions for an entity"""
    try:
        suggestions = await tag_service.suggest_tags_for_entity(entity_type, entity_id, limit)
        
        return [
            {
                "tag_id": suggestion.tag_id,
                "tag_name": suggestion.tag_name,
                "confidence_score": round(suggestion.confidence_score, 3),
                "source": suggestion.source.value,
                "explanation": suggestion.explanation
            }
            for suggestion in suggestions
        ]
    except Exception as e:
        logger.error(f"Error getting tag suggestions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Utility endpoints
@tag_router.post("/refresh-analytics")
async def refresh_tag_analytics(
    tag_service: TagService = Depends(get_tag_service)
):
    """Refresh tag analytics materialized view"""
    try:
        success = await tag_service.refresh_tag_analytics()
        if success:
            return {"message": "Tag analytics refreshed successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to refresh analytics")
    except Exception as e:
        logger.error(f"Error refreshing analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@tag_router.get("/usage-stats", response_model=List[Dict[str, Any]])
async def get_tag_usage_stats(
    limit: int = Query(100, description="Maximum number of stats to return"),
    tag_service: TagService = Depends(get_tag_service)
):
    """Get tag usage statistics"""
    try:
        stats = await tag_service.get_tag_usage_stats(limit)
        
        return [
            {
                "tag_id": stat.tag_id,
                "tag_name": stat.tag_name,
                "total_usage": stat.total_usage,
                "unique_entities": stat.unique_entities,
                "entity_types_count": stat.entity_types_count,
                "avg_confidence": float(stat.avg_confidence) if stat.avg_confidence else 0,
                "last_used": stat.last_used.isoformat() if stat.last_used else None,
                "active_days_last_90": stat.active_days_last_90
            }
            for stat in stats
        ]
    except Exception as e:
        logger.error(f"Error getting usage stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Auto-tagging endpoints
@tag_router.post("/auto-tag/{entity_type}/{entity_id}")
async def auto_tag_entity(
    entity_type: str,
    entity_id: int,
    tag_service: TagService = Depends(get_tag_service)
):
    """Apply auto-tagging rules to a specific entity"""
    try:
        # Get entity details for auto-tagging
        if entity_type == "data_quality_issues":
            issue_details = await tag_service.repository.get_issue_details(entity_id)
            if not issue_details:
                raise HTTPException(status_code=404, detail="Entity not found")
                
            applied_tags = await tag_service.auto_tag_issue(entity_id, issue_details)
            
            return {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "applied_tags": applied_tags,
                "total_applied": len(applied_tags),
                "message": f"Applied {len(applied_tags)} auto-tags successfully"
            }
        else:
            raise HTTPException(status_code=400, detail=f"Auto-tagging not supported for entity type: {entity_type}")
            
    except Exception as e:
        logger.error(f"Error auto-tagging entity: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@tag_router.get("/auto-rules")
async def get_auto_tagging_rules(
    tag_service: TagService = Depends(get_tag_service)
):
    """Get all auto-tagging rules"""
    try:
        auto_tagging = tag_service.get_auto_tagging_service()
        rules = auto_tagging.get_all_rules()
        
        return {
            "rules": rules,
            "total_rules": len(rules),
            "categories": list(set(rule['category'] for rule in rules))
        }
    except Exception as e:
        logger.error(f"Error getting auto-tagging rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@tag_router.post("/auto-batch")
async def run_auto_tagging_batch(
    limit: int = Body(100, embed=True, description="Maximum number of issues to process"),
    min_hours_old: int = Body(1, embed=True, description="Minimum hours old for issues to process"),
    tag_service: TagService = Depends(get_tag_service)
):
    """Run auto-tagging batch job on recent issues"""
    try:
        auto_tagging = tag_service.get_auto_tagging_service()
        results = await auto_tagging.run_auto_tagging_job(limit=limit, min_hours_old=min_hours_old)
        
        return results
    except Exception as e:
        logger.error(f"Error running auto-tagging batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@tag_router.get("/suggestions-enhanced/{entity_type}/{entity_id}")
async def get_enhanced_tag_suggestions(
    entity_type: str,
    entity_id: int,
    limit: int = Query(5, description="Maximum number of suggestions"),
    tag_service: TagService = Depends(get_tag_service)
):
    """Get enhanced tag suggestions including auto-tagging rules"""
    try:
        suggestions = await tag_service.get_auto_tag_suggestions_enhanced(entity_type, entity_id, limit)
        
        return [
            {
                "tag_id": suggestion.tag_id,
                "tag_name": suggestion.tag_name,
                "confidence_score": round(suggestion.confidence_score, 3),
                "source": suggestion.source.value,
                "explanation": suggestion.explanation
            }
            for suggestion in suggestions
        ]
    except Exception as e:
        logger.error(f"Error getting enhanced suggestions: {e}")
        raise HTTPException(status_code=500, detail=str(e))